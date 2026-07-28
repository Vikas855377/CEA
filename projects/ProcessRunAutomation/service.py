"""Process-wide lifecycle manager for the background scheduler."""

from __future__ import annotations

import atexit
import logging
import os
import re
import subprocess
import sys
import threading
import json
from datetime import datetime, timedelta
from collections import deque
from dataclasses import dataclass
from pathlib import Path

LOGGER = logging.getLogger(__name__)

@dataclass(frozen=True)
class SchedulerServiceStatus:
    enabled: bool
    running: bool
    pid: int | None
    message: str


@dataclass(frozen=True)
class ProcessRunSummary:
    successful: int
    failed: int
    cycles: int
    last_completed_at: str | None


@dataclass(frozen=True)
class CurrentEtlProgress:
    percent: int
    completed: int
    total: int
    state: str
    cycle: int | None
    started_at: str | None
    next_cycle_at: str | None
    remaining_seconds: int | None


@dataclass(frozen=True)
class EtlRunHistoryItem:
    cycle: int
    completed_at: str
    duration_seconds: float
    successful: int
    failed: int
    total: int


_LOCK = threading.Lock()
_PROCESS: subprocess.Popen | None = None
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_RUNTIME_ROOT = Path(
    os.getenv(
        "CEA_RUNTIME_ROOT",
        os.getenv(
            "CEA_PROCESS_SCHEDULER_RUNTIME_DIR",
            _PROJECT_ROOT / "data" / "model_monitoring" / "process_scheduler",
        ),
    )
)
_MODEL_SNAPSHOT_MARKER = _RUNTIME_ROOT / "model_snapshot_complete.json"
_CYCLE_COMPLETE_PATTERN = re.compile(
    r"^(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*?"
    r"Cycle (?P<cycle>\d+) complete: succeeded=(?P<succeeded>\d+) "
    r"failed=(?P<failed>\d+) total=(?P<total>\d+) duration=(?P<duration>[\d.]+)s"
)
_CYCLE_START_PATTERN = re.compile(
    r"^(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*?"
    r"Cycle (?P<cycle>\d+) starting: clients=(?P<clients>\d+)"
)
_BATCH_STATUS_PATTERN = re.compile(r"statuses=\{(?P<statuses>[^}]*)\}")


def _stats_reset_at() -> datetime | None:
    raw = os.getenv("CEA_ETL_STATS_RESET_AT", "").strip()
    if not raw:
        return None
    try:
        return datetime.strptime(raw, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        LOGGER.error("Ignoring invalid CEA_ETL_STATS_RESET_AT value: %r", raw)
        return None


def _is_after_stats_reset(timestamp: str, reset_at: datetime | None) -> bool:
    if reset_at is None:
        return True
    try:
        return datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S") >= reset_at
    except ValueError:
        return False


def _enabled() -> bool:
    return os.getenv("PROCESS_RUN_AUTOMATION_ENABLED", "true").strip().lower() in {
        "1",
        "true",
        "yes",
        "y",
    }


def publish_model_snapshot_complete(cycle: int, completed_at: str) -> None:
    """Acknowledge that Application Runs processing finished for an ETL cycle."""
    _RUNTIME_ROOT.mkdir(parents=True, exist_ok=True)
    temporary = _MODEL_SNAPSHOT_MARKER.with_suffix(".tmp")
    temporary.write_text(
        json.dumps({"cycle": int(cycle), "completed_at": completed_at}),
        encoding="utf-8",
    )
    temporary.replace(_MODEL_SNAPSHOT_MARKER)


def load_model_snapshot_marker() -> tuple[int | None, str | None]:
    try:
        payload = json.loads(_MODEL_SNAPSHOT_MARKER.read_text(encoding="utf-8"))
        return int(payload["cycle"]), str(payload["completed_at"])
    except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError):
        return None, None


def ensure_scheduler_started() -> SchedulerServiceStatus:
    """Start one scheduler child process for this application server process."""
    global _PROCESS
    if not _enabled():
        return SchedulerServiceStatus(False, False, None, "disabled by configuration")

    with _LOCK:
        if _PROCESS is not None and _PROCESS.poll() is None:
            return SchedulerServiceStatus(True, True, _PROCESS.pid, "running")

        if _PROCESS is not None:
            LOGGER.error(
                "Scheduler process exited unexpectedly with code %s; restarting",
                _PROCESS.returncode,
            )

        try:
            from projects.ProcessRunAutomation.process_scheduler import load_clients

            clients = load_clients()
        except Exception as exc:
            return SchedulerServiceStatus(
                True,
                False,
                None,
                f"configuration error: {exc}",
            )

        try:
            _PROCESS = subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "projects.ProcessRunAutomation.process_scheduler",
                ],
                cwd=_PROJECT_ROOT,
                env=os.environ.copy(),
            )
        except OSError as exc:
            return SchedulerServiceStatus(
                True,
                False,
                None,
                f"could not start scheduler: {exc}",
            )
        LOGGER.info("Started scheduler process pid=%s", _PROCESS.pid)
        return SchedulerServiceStatus(
            True,
            True,
            _PROCESS.pid,
            f"running for {len(clients)} clients",
        )


def load_recent_run_summary(limit_cycles: int = 12) -> ProcessRunSummary:
    """Summarize recent completed client runs from the persistent scheduler log."""
    configured_log = os.getenv("CEA_LOG_FILE", "").strip()
    candidates = [
        Path(configured_log) if configured_log else None,
        _PROJECT_ROOT / "projects" / "ProcessRunAutomation" / "logs" / "scheduler.log",
        _PROJECT_ROOT / "data" / "model_monitoring" / "process_scheduler" / "logs" / "scheduler.log",
    ]
    existing = [path for path in candidates if path is not None and path.is_file()]
    if not existing:
        return ProcessRunSummary(0, 0, 0, None)

    log_file = max(existing, key=lambda path: path.stat().st_mtime)
    recent: deque[tuple[int, int, str]] = deque(maxlen=max(1, limit_cycles))
    reset_at = _stats_reset_at()
    try:
        with log_file.open(encoding="utf-8", errors="replace") as handle:
            for line in handle:
                match = _CYCLE_COMPLETE_PATTERN.search(line)
                if match and _is_after_stats_reset(match.group("timestamp"), reset_at):
                    recent.append(
                        (
                            int(match.group("succeeded")),
                            int(match.group("failed")),
                            match.group("timestamp"),
                        )
                    )
    except OSError:
        return ProcessRunSummary(0, 0, 0, None)

    return ProcessRunSummary(
        successful=sum(item[0] for item in recent),
        failed=sum(item[1] for item in recent),
        cycles=len(recent),
        last_completed_at=recent[-1][2] if recent else None,
    )


def load_current_etl_progress() -> CurrentEtlProgress:
    """Read the latest cycle and client completion progress from the scheduler log."""
    configured_log = os.getenv("CEA_LOG_FILE", "").strip()
    candidates = [
        Path(configured_log) if configured_log else None,
        _PROJECT_ROOT / "projects" / "ProcessRunAutomation" / "logs" / "scheduler.log",
        _PROJECT_ROOT / "data" / "model_monitoring" / "process_scheduler" / "logs" / "scheduler.log",
    ]
    existing = [path for path in candidates if path is not None and path.is_file()]
    if not existing:
        return CurrentEtlProgress(0, 0, 0, "waiting", None, None, None, None)

    cycle = None
    total = completed = 0
    started_at = None
    state = "waiting"
    reset_at = _stats_reset_at()
    try:
        with max(existing, key=lambda path: path.stat().st_mtime).open(encoding="utf-8", errors="replace") as handle:
            for line in handle:
                start = _CYCLE_START_PATTERN.search(line)
                if start and _is_after_stats_reset(start.group("timestamp"), reset_at):
                    cycle = int(start.group("cycle"))
                    total = int(start.group("clients"))
                    completed = 0
                    started_at = start.group("timestamp")
                    state = "running"
                    continue
                if state != "running":
                    continue
                status_match = _BATCH_STATUS_PATTERN.search(line)
                if status_match:
                    values = re.findall(r"'([^']+)'", status_match.group("statuses"))
                    if values:
                        total = max(total, len(values))
                        completed = sum(value in {"COMPLETED", "FAILED"} for value in values)
                if _CYCLE_COMPLETE_PATTERN.search(line):
                    completed = total
                    state = "completed"
    except OSError:
        return CurrentEtlProgress(0, 0, 0, "unavailable", cycle, started_at, None, None)

    percent = round(completed / total * 100) if total else 0
    next_cycle_at = None
    remaining_seconds = None
    if started_at:
        try:
            cycle_started = datetime.strptime(started_at, "%Y-%m-%d %H:%M:%S")
            interval = int(os.getenv("CEA_RUN_INTERVAL_SECONDS", "300"))
            next_cycle = cycle_started + timedelta(seconds=interval)
            next_cycle_at = next_cycle.strftime("%Y-%m-%d %H:%M:%S")
            remaining_seconds = max(0, int((next_cycle - datetime.now()).total_seconds()))
        except (ValueError, OverflowError):
            pass
    return CurrentEtlProgress(
        percent,
        completed,
        total,
        state,
        cycle,
        started_at,
        next_cycle_at,
        remaining_seconds,
    )


def load_etl_run_history(run_date: str | None = None) -> list[EtlRunHistoryItem]:
    """Return completed ETL cycles, optionally filtered by local log date."""
    configured_log = os.getenv("CEA_LOG_FILE", "").strip()
    candidates = [
        Path(configured_log) if configured_log else None,
        _PROJECT_ROOT / "projects" / "ProcessRunAutomation" / "logs" / "scheduler.log",
        _PROJECT_ROOT / "data" / "model_monitoring" / "process_scheduler" / "logs" / "scheduler.log",
    ]
    existing = [path for path in candidates if path is not None and path.is_file()]
    if not existing:
        return []

    history: deque[EtlRunHistoryItem] = deque(maxlen=500)
    reset_at = _stats_reset_at()
    try:
        with max(existing, key=lambda path: path.stat().st_mtime).open(encoding="utf-8", errors="replace") as handle:
            for line in handle:
                match = _CYCLE_COMPLETE_PATTERN.search(line)
                if (
                    not match
                    or not _is_after_stats_reset(match.group("timestamp"), reset_at)
                    or (run_date and not match.group("timestamp").startswith(run_date))
                ):
                    continue
                history.append(
                    EtlRunHistoryItem(
                        cycle=int(match.group("cycle")),
                        completed_at=match.group("timestamp"),
                        duration_seconds=float(match.group("duration")),
                        successful=int(match.group("succeeded")),
                        failed=int(match.group("failed")),
                        total=int(match.group("total")),
                    )
                )
    except OSError:
        return []
    return list(reversed(history))


def stop_scheduler() -> None:
    """Stop the scheduler and allow it to terminate active client runners."""
    global _PROCESS
    with _LOCK:
        process = _PROCESS
        _PROCESS = None
    if process is None or process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


atexit.register(stop_scheduler)
