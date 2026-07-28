"""Concurrent, fault-isolated scheduler for Obero ProcessApp clients."""

from __future__ import annotations

import argparse
import json
import logging
import logging.handlers
import os
import re
import signal
import socket
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

from dotenv import load_dotenv

from projects.ProcessRunAutomation.refresh_batch import (
    RefreshBatchCoordinator,
    RefreshBatchResult,
    coordination_enabled,
)
from projects.ProcessRunAutomation.service import load_model_snapshot_marker


PROJECT_ROOT = Path(__file__).resolve().parent
REPO_ROOT = PROJECT_ROOT.parents[1]
RUNTIME_ROOT = Path(
    os.getenv(
        "CEA_PROCESS_SCHEDULER_RUNTIME_DIR",
        REPO_ROOT / "data" / "model_monitoring" / "process_scheduler",
    )
)
load_dotenv(REPO_ROOT / ".env")

CLIENTS_FILE = Path(
    os.getenv("CEA_CLIENTS_FILE", PROJECT_ROOT / "clients.json")
)
INTERVAL_SECONDS = int(os.getenv("CEA_RUN_INTERVAL_SECONDS", "300"))
ACTIVE_BATCH_RETRY_SECONDS = int(os.getenv("CEA_ACTIVE_BATCH_RETRY_SECONDS", "10"))
LAUNCH_WAVE_SIZE = int(os.getenv("CEA_CLIENT_LAUNCH_WAVE_SIZE", "10"))
LAUNCH_WAVE_GAP_SECONDS = float(os.getenv("CEA_LAUNCH_WAVE_GAP_SECONDS", "1"))
BROWSER_MEMORY_MB = int(os.getenv("CEA_BROWSER_MEMORY_ESTIMATE_MB", "350"))
HOST_MEMORY_RESERVE_MB = int(os.getenv("CEA_HOST_MEMORY_RESERVE_MB", "2048"))
DEFAULT_TIMEOUT_SECONDS = int(os.getenv("CEA_CLIENT_TIMEOUT_SECONDS", "240"))
LOG_DIR = PROJECT_ROOT / "logs"
LOG_FILE = Path(os.getenv("CEA_LOG_FILE", LOG_DIR / "scheduler.log"))

NAME_PATTERN = re.compile(r"^[a-z][a-z0-9_]{1,63}$")
ENV_PATTERN = re.compile(r"^[A-Z][A-Z0-9_]*$")
SHUTDOWN = threading.Event()
ACTIVE_LOCK = threading.Lock()
ACTIVE_PROCESSES: dict[str, subprocess.Popen] = {}


def memory_aware_wave_size(client_count: int) -> tuple[int, int | None]:
    """Cap browser concurrency using physical RAM while honoring the configured maximum."""
    total_memory_mb = None
    try:
        total_memory_mb = int(os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") / 1024 / 1024)
    except (AttributeError, OSError, ValueError):
        pass
    if total_memory_mb is None:
        return min(client_count, LAUNCH_WAVE_SIZE), None
    browser_budget_mb = max(BROWSER_MEMORY_MB, total_memory_mb - HOST_MEMORY_RESERVE_MB)
    memory_limit = max(1, browser_budget_mb // BROWSER_MEMORY_MB)
    return min(client_count, LAUNCH_WAVE_SIZE, memory_limit), total_memory_mb


@dataclass(frozen=True)
class ClientConfig:
    name: str
    base_url: str
    fs_id_env: str
    fs_id: str
    timeout_seconds: int
    monitoring_client_id: int
    monitoring_client_name: str

    @property
    def session_file(self) -> Path:
        return RUNTIME_ROOT / "sessions" / f"{self.name}.json"


@dataclass(frozen=True)
class ClientResult:
    name: str
    exit_code: int
    duration_seconds: float
    timed_out: bool = False

    @property
    def succeeded(self) -> bool:
        return self.exit_code == 0 and not self.timed_out


def configure_logging() -> logging.Logger:
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("obero_scheduler")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)
    rotating_file = logging.handlers.RotatingFileHandler(
        LOG_FILE,
        maxBytes=5 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    rotating_file.setFormatter(formatter)
    logger.addHandler(console)
    logger.addHandler(rotating_file)
    return logger


LOGGER = configure_logging()


def _positive_int(value: object, label: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} must be an integer") from exc
    if parsed < 1:
        raise ValueError(f"{label} must be at least 1")
    return parsed


def load_clients() -> list[ClientConfig]:
    with CLIENTS_FILE.open(encoding="utf-8") as handle:
        raw_clients = json.load(handle).get("clients", [])
    if not raw_clients:
        raise ValueError(f"No clients configured in {CLIENTS_FILE}")

    clients: list[ClientConfig] = []
    seen_names: set[str] = set()
    seen_urls: set[str] = set()
    for index, raw in enumerate(raw_clients, start=1):
        if raw.get("enabled", True) is False:
            continue
        name = str(raw.get("name", "")).strip()
        base_url = str(raw.get("base_url", "")).strip().rstrip("/")
        fs_id_env = str(raw.get("fs_id_env", "")).strip()

        if not NAME_PATTERN.fullmatch(name):
            raise ValueError(f"Client #{index} has invalid name: {name!r}")
        if name in seen_names:
            raise ValueError(f"Duplicate client name: {name}")
        parsed_url = urlparse(base_url)
        if parsed_url.scheme != "https" or not parsed_url.hostname:
            raise ValueError(f"Client {name} must have a valid HTTPS base_url")
        if parsed_url.path not in ("", "/") or parsed_url.query or parsed_url.fragment:
            raise ValueError(f"Client {name} base_url must not include a path or query")
        if base_url in seen_urls:
            raise ValueError(f"Duplicate client base_url: {base_url}")
        if not ENV_PATTERN.fullmatch(fs_id_env):
            raise ValueError(f"Client {name} has invalid fs_id_env: {fs_id_env!r}")

        fs_id = os.getenv(fs_id_env, "").strip()
        if not fs_id.isdigit() or int(fs_id) < 1:
            raise ValueError(f"{fs_id_env} for client {name} must be a positive integer")
        timeout = _positive_int(
            raw.get("timeout_seconds", DEFAULT_TIMEOUT_SECONDS),
            f"timeout_seconds for {name}",
        )
        monitoring_client_id = _positive_int(
            raw.get("monitoring_client_id"),
            f"monitoring_client_id for {name}",
        )
        monitoring_client_name = str(raw.get("monitoring_client_name", "")).strip()
        if not monitoring_client_name:
            raise ValueError(f"monitoring_client_name for {name} is required")
        clients.append(
            ClientConfig(
                name,
                base_url,
                fs_id_env,
                fs_id,
                timeout,
                monitoring_client_id,
                monitoring_client_name,
            )
        )
        seen_names.add(name)
        seen_urls.add(base_url)

    if not clients:
        raise ValueError("All configured clients are disabled")
    return clients


def _stop_process(name: str, process: subprocess.Popen) -> None:
    if process.poll() is not None:
        LOGGER.info(
            "[%s] Process runner already stopped; pid=%s exit_code=%s",
            name,
            process.pid,
            process.returncode,
        )
        return
    LOGGER.warning("[%s] Terminating process runner; pid=%s", name, process.pid)
    process.terminate()
    try:
        process.wait(timeout=5)
        LOGGER.info(
            "[%s] Process runner terminated; pid=%s exit_code=%s",
            name,
            process.pid,
            process.returncode,
        )
    except subprocess.TimeoutExpired:
        LOGGER.error(
            "[%s] Process runner ignored termination for 5s; killing pid=%s",
            name,
            process.pid,
        )
        process.kill()
        process.wait()
        LOGGER.info(
            "[%s] Process runner killed; pid=%s exit_code=%s",
            name,
            process.pid,
            process.returncode,
        )


def run_client(client: ClientConfig, launch_barrier: threading.Barrier) -> ClientResult:
    started = time.monotonic()
    if SHUTDOWN.is_set():
        LOGGER.warning("[%s] Skipping launch because shutdown is already requested", client.name)
        return ClientResult(client.name, 130, 0)

    environment = os.environ.copy()
    environment.update(
        {
            "OBERO_BASE_URL": client.base_url,
            "CEA_PROCESS_FS_ID": client.fs_id,
            "OBERO_SESSION_FILE": str(client.session_file),
            "OBERO_CLIENT_NAME": client.name,
            "PYTHONUNBUFFERED": "1",
        }
    )
    LOGGER.info(
        "[%s] Ready at parallel launch barrier; url=%s process=%s timeout=%ss "
        "session=%s session_exists=%s",
        client.name,
        client.base_url,
        client.fs_id,
        client.timeout_seconds,
        client.session_file,
        client.session_file.exists(),
    )
    barrier_started = time.monotonic()
    try:
        launch_barrier.wait(timeout=30)
    except threading.BrokenBarrierError:
        LOGGER.error(
            "[%s] Parallel launch barrier failed after %.1fs; waiting=%s parties=%s "
            "shutdown=%s",
            client.name,
            time.monotonic() - barrier_started,
            launch_barrier.n_waiting,
            launch_barrier.parties,
            SHUTDOWN.is_set(),
        )
        return ClientResult(client.name, 1, time.monotonic() - started)
    LOGGER.info(
        "[%s] Launch barrier released after %.3fs; launching authentication flow",
        client.name,
        time.monotonic() - barrier_started,
    )

    command = [sys.executable, str(PROJECT_ROOT / "obero_process_runner.py")]
    try:
        process = subprocess.Popen(
            command,
            cwd=REPO_ROOT,
            env=environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
    except OSError:
        LOGGER.exception(
            "[%s] Could not start process runner; executable=%s cwd=%s",
            client.name,
            sys.executable,
            REPO_ROOT,
        )
        return ClientResult(client.name, 1, time.monotonic() - started)
    with ACTIVE_LOCK:
        ACTIVE_PROCESSES[client.name] = process
    LOGGER.info(
        "[%s] Process runner started; pid=%s executable=%s",
        client.name,
        process.pid,
        sys.executable,
    )

    timed_out = threading.Event()

    def expire() -> None:
        timed_out.set()
        LOGGER.error("[%s] Timed out after %s seconds", client.name, client.timeout_seconds)
        _stop_process(client.name, process)

    timer = threading.Timer(client.timeout_seconds, expire)
    timer.daemon = True
    timer.start()
    output_lines = 0
    try:
        assert process.stdout is not None
        for line in process.stdout:
            output_lines += 1
            LOGGER.info("[%s] %s", client.name, line.rstrip())
            if SHUTDOWN.is_set():
                _stop_process(client.name, process)
                break
        exit_code = process.wait()
    finally:
        timer.cancel()
        with ACTIVE_LOCK:
            ACTIVE_PROCESSES.pop(client.name, None)

    duration = time.monotonic() - started
    result = ClientResult(client.name, exit_code, duration, timed_out.is_set())
    if result.succeeded:
        LOGGER.info(
            "[%s] Completed successfully in %.1fs; pid=%s output_lines=%s",
            client.name,
            duration,
            process.pid,
            output_lines,
        )
    else:
        LOGGER.error(
            "[%s] Failed in %.1fs; exit_code=%s timed_out=%s output_lines=%s "
            "shutdown=%s",
            client.name,
            duration,
            exit_code,
            result.timed_out,
            output_lines,
            SHUTDOWN.is_set(),
        )
    return result


def run_cycle(
    clients: list[ClientConfig],
    cycle_number: int,
    refresh_coordinator: RefreshBatchCoordinator | None = None,
) -> tuple[list[ClientResult], RefreshBatchResult | None]:
    cycle_started = time.monotonic()
    batch_id: str | None = None
    if refresh_coordinator is not None:
        batch_id = refresh_coordinator.prepare_batch(clients)
        if batch_id is None:
            LOGGER.warning("Cycle %s skipped because another refresh batch is active", cycle_number)
            return [], None

    # Small deployments remain a single simultaneous wave. Large deployments
    # use bounded waves so browser startup cannot exhaust the host.
    workers, total_memory_mb = memory_aware_wave_size(len(clients))
    waves = [clients[index : index + workers] for index in range(0, len(clients), workers)]
    LOGGER.info(
        "Cycle %s starting: clients=%s max_parallel_browsers=%s waves=%s "
        "host_memory_mb=%s estimated_browser_mb=%s reserve_mb=%s",
        cycle_number,
        len(clients),
        workers,
        len(waves),
        total_memory_mb,
        BROWSER_MEMORY_MB,
        HOST_MEMORY_RESERVE_MB,
    )
    results: list[ClientResult] = []
    for wave_number, wave_clients in enumerate(waves, start=1):
        if SHUTDOWN.is_set():
            break
        launch_barrier = threading.Barrier(len(wave_clients))
        LOGGER.info(
            "Cycle %s wave %s/%s ready: clients=%s",
            cycle_number,
            wave_number,
            len(waves),
            [client.name for client in wave_clients],
        )
        with ThreadPoolExecutor(
            max_workers=len(wave_clients),
            thread_name_prefix=f"obero-wave-{wave_number}",
        ) as pool:
            futures = {
                pool.submit(run_client, client, launch_barrier): client
                for client in wave_clients
            }
            for future in as_completed(futures):
                client = futures[future]
                try:
                    results.append(future.result())
                except Exception:
                    LOGGER.exception("[%s] Unexpected scheduler worker failure", client.name)
                    results.append(ClientResult(client.name, 1, 0))
        if wave_number < len(waves) and not SHUTDOWN.is_set():
            LOGGER.info(
                "Cycle %s wave %s complete; next wave in %.1f seconds",
                cycle_number,
                wave_number,
                LAUNCH_WAVE_GAP_SECONDS,
            )
            SHUTDOWN.wait(LAUNCH_WAVE_GAP_SECONDS)

    succeeded = sum(result.succeeded for result in results)
    failed = len(results) - succeeded

    batch_result = None
    if refresh_coordinator is not None and batch_id is not None:
        clients_by_name = {client.name: client for client in clients}
        for result in results:
            if result.succeeded:
                continue
            client = clients_by_name[result.name]
            refresh_coordinator.mark_runner_failed(
                batch_id,
                client,
                f"Process launcher failed: exit_code={result.exit_code} timed_out={result.timed_out}",
            )
        batch_result = refresh_coordinator.wait_for_batch(
            batch_id,
            {client.monitoring_client_id for client in clients},
        )
    LOGGER.info(
        "Cycle %s complete: succeeded=%s failed=%s total=%s duration=%.1fs failures=%s",
        cycle_number,
        succeeded,
        failed,
        len(results),
        time.monotonic() - cycle_started,
        [
            {
                "client": result.name,
                "exit_code": result.exit_code,
                "timed_out": result.timed_out,
            }
            for result in results
            if not result.succeeded
        ],
    )
    if batch_result is not None:
        LOGGER.info(
            "Refresh batch %s finished: status=%s clients=%s error=%s",
            batch_result.batch_id,
            batch_result.status,
            batch_result.client_statuses,
            batch_result.error_message or None,
        )
    return results, batch_result


def request_shutdown(signum: int, _frame: object) -> None:
    LOGGER.warning("Shutdown requested by signal %s", signum)
    SHUTDOWN.set()
    with ACTIVE_LOCK:
        active = list(ACTIVE_PROCESSES.items())
    for name, process in active:
        _stop_process(name, process)


def wait_for_model_snapshot(
    cycle_number: int,
    previous_marker: tuple[int | None, str | None],
) -> None:
    """Hold the next ETL until Application Runs acknowledges this cycle."""
    LOGGER.info(
        "Waiting for Application Runs processing to complete for cycle %s",
        cycle_number,
    )
    while not SHUTDOWN.is_set():
        marker = load_model_snapshot_marker()
        acknowledged_cycle, _ = marker
        if acknowledged_cycle == cycle_number and marker != previous_marker:
            LOGGER.info(
                "Application Runs processing completed for cycle %s",
                cycle_number,
            )
            return
        SHUTDOWN.wait(1)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--once", action="store_true", help="Run one cycle and exit")
    parser.add_argument("--validate", action="store_true", help="Validate configuration and exit")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if (
        INTERVAL_SECONDS < 1
        or DEFAULT_TIMEOUT_SECONDS < 1
        or ACTIVE_BATCH_RETRY_SECONDS < 1
        or LAUNCH_WAVE_SIZE < 1
        or LAUNCH_WAVE_GAP_SECONDS < 0
        or BROWSER_MEMORY_MB < 1
        or HOST_MEMORY_RESERVE_MB < 0
    ):
        LOGGER.error("Scheduler timing values and launch-wave size are invalid")
        return 2
    try:
        clients = load_clients()
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        LOGGER.error("Invalid client configuration: %s", exc)
        return 2

    LOGGER.info("Validated %s enabled clients from %s", len(clients), CLIENTS_FILE)
    if args.validate:
        return 0

    refresh_coordinator = (
        RefreshBatchCoordinator(logger=LOGGER)
        if coordination_enabled()
        else None
    )

    sessions_dir = RUNTIME_ROOT / "sessions"
    sessions_dir.mkdir(parents=True, exist_ok=True)
    sessions_dir.chmod(0o700)
    signal.signal(signal.SIGINT, request_shutdown)
    signal.signal(signal.SIGTERM, request_shutdown)
    LOGGER.info(
        "Scheduler started: host=%s pid=%s python=%s interval=%ss "
        "clients=%s launch_wave_size=%s runtime_root=%s clients_file=%s log=%s",
        socket.gethostname(),
        os.getpid(),
        sys.version.split()[0],
        INTERVAL_SECONDS,
        len(clients),
        LAUNCH_WAVE_SIZE,
        RUNTIME_ROOT,
        CLIENTS_FILE,
        LOG_FILE,
    )

    cycle_number = 1
    while not SHUTDOWN.is_set():
        cycle_started = time.monotonic()
        previous_snapshot_marker = load_model_snapshot_marker()
        try:
            results, batch_result = run_cycle(clients, cycle_number, refresh_coordinator)
        except Exception:
            LOGGER.exception(
                "Cycle %s failed unexpectedly; scheduler remains alive and will retry",
                cycle_number,
            )
            results, batch_result = [], None
        if args.once:
            runners_succeeded = bool(results) and all(result.succeeded for result in results)
            batch_succeeded = batch_result is None or batch_result.status == "COMPLETED"
            return 0 if runners_succeeded and batch_succeeded else 1

        if results or batch_result is not None:
            wait_for_model_snapshot(cycle_number, previous_snapshot_marker)

        elapsed = time.monotonic() - cycle_started
        if not results and batch_result is None and refresh_coordinator is not None:
            wait_seconds = float(ACTIVE_BATCH_RETRY_SECONDS)
            LOGGER.info(
                "Active refresh batch is still running; retrying launch in %.1f seconds",
                wait_seconds,
            )
        else:
            wait_seconds = max(0.0, INTERVAL_SECONDS - elapsed)
            LOGGER.info("Next cycle in %.1f seconds", wait_seconds)
        SHUTDOWN.wait(wait_seconds)
        cycle_number += 1

    LOGGER.info("Scheduler stopped")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
