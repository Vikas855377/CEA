from __future__ import annotations

import logging
import os
import json
import select
import subprocess
import base64
from dataclasses import dataclass
from pathlib import Path
from queue import Empty, Queue
from threading import Lock, RLock
from typing import Any, Sequence

import pandas as pd

from core.config import XactlyJdbcSettings, normalize_xactly_jdbc_url

logger = logging.getLogger(__name__)

DEFAULT_POD_HOSTS = {
    "secure1": "secure1.xactlycorp.com",
    "secure2": "secure2.xactlycorp.com",
    "secure3": "secure3.xactlycorp.com",
    "secure4": "secure4.xactlycorp.com",
    "eu1": "eu1.xactlycorp.com",
}


def build_pod_hosts() -> dict[str, str]:
    supported_pods = [
        pod.strip().lower()
        for pod in os.environ.get("XACTLY_JDBC_SUPPORTED_PODS", ",".join(DEFAULT_POD_HOSTS.keys())).split(",")
    ]
    return {pod: DEFAULT_POD_HOSTS[pod] for pod in supported_pods if pod in DEFAULT_POD_HOSTS}


@dataclass
class _ServerSlot:
    proc: subprocess.Popen[str]


class XactlyJdbcClient:
    def __init__(self, settings: XactlyJdbcSettings | None = None) -> None:
        self.settings = settings or XactlyJdbcSettings.from_env()
        self.pod_hosts = build_pod_hosts()
        self._conn: Any | None = None
        self._lock = RLock()
        self._server_proc: subprocess.Popen[str] | None = None
        self._server_request_id = 0
        self._server_pool: Queue[_ServerSlot] = Queue()
        self._server_pool_lock = Lock()
        self._server_pool_created = 0

    @staticmethod
    def server_response_timeout() -> float:
        try:
            return max(1.0, float(os.getenv("XACTLY_JDBC_RESPONSE_TIMEOUT_SECONDS", "30")))
        except ValueError:
            return 30.0

    @staticmethod
    def resolve_driver_path(driver_path: str) -> str:
        path = Path(driver_path).expanduser()
        candidates = [
            path,
            Path.cwd() / driver_path,
            Path.cwd() / path.name,
        ]
        for candidate in candidates:
            if candidate.exists():
                return str(candidate)
        return str(path)

    @staticmethod
    def helper_java_path() -> Path:
        return Path(__file__).resolve().parents[1] / "scripts" / "XactlyJdbcQuery.java"

    @classmethod
    def helper_class_path(cls) -> Path:
        return cls.helper_java_path().with_suffix(".class")

    def apply_java_home(self) -> None:
        if not self.settings.java_home:
            return

        java_home = Path(self.settings.java_home).expanduser()
        if not java_home.exists():
            logger.warning("Configured XACTLY_JAVA_HOME does not exist: %s", java_home)
            return

        os.environ["JAVA_HOME"] = str(java_home)
        java_bin = java_home / "bin"
        if java_bin.exists():
            os.environ["PATH"] = f"{java_bin}{os.pathsep}{os.environ.get('PATH', '')}"

    def java_bin(self, executable: str) -> str:
        if self.settings.java_home:
            candidate = Path(self.settings.java_home).expanduser() / "bin" / executable
            if candidate.exists():
                return str(candidate)
        return executable

    def compile_helper(self) -> None:
        helper_java = self.helper_java_path()
        helper_class = self.helper_class_path()
        if helper_class.exists() and helper_class.stat().st_mtime >= helper_java.stat().st_mtime:
            return

        completed = subprocess.run(
            [self.java_bin("javac"), str(helper_java)],
            check=False,
            capture_output=True,
            text=True,
        )
        if completed.returncode != 0:
            raise RuntimeError(f"Failed to compile Xactly JDBC helper: {completed.stderr.strip()}")

    def get_jdbc_url(self, pod_name: str | None = None) -> str:
        if self.settings.url:
            return normalize_xactly_jdbc_url(self.settings.url)

        pod = (pod_name or self.settings.pod_name).lower().replace("_", "")
        if pod not in self.pod_hosts:
            expected = ", ".join(sorted(self.pod_hosts))
            raise ValueError(f"Unknown Xactly pod: {pod_name}. Expected one of: {expected}")

        return f"jdbc:xactly://{self.pod_hosts[pod]}:443?useSSL="

    def _connect(self) -> Any:
        if not self.settings.is_complete:
            raise RuntimeError("Xactly JDBC settings are incomplete. Fill the XACTLY_JDBC_* values in .env.")

        driver_path = self.resolve_driver_path(self.settings.jar_path)
        if not Path(driver_path).exists():
            raise FileNotFoundError(f"Xactly JDBC driver not found at {driver_path}")

        self.apply_java_home()

        try:
            import jaydebeapi
        except ImportError as exc:
            raise RuntimeError("JayDeBeApi is not installed. Run `pip install -r requirements.txt`.") from exc

        jdbc_url = self.get_jdbc_url()
        logger.info("Connecting to Xactly JDBC pod %s", self.settings.pod_name)
        return jaydebeapi.connect(
            self.settings.driver_class,
            jdbc_url,
            [self.settings.user, self.settings.password],
            driver_path,
        )

    def _get_connection(self) -> Any:
        if self._conn is None:
            self._conn = self._connect()
        return self._conn

    def execute_query(self, query: str, params: Sequence[Any] | None = None, max_rows: int = 100) -> dict[str, Any]:
        if not query or not query.strip():
            raise ValueError("Query is required")

        if params and self.settings.mode == "subprocess":
            raise ValueError("Parameterized queries are supported only in JPype mode.")

        if self.settings.mode == "subprocess":
            return self.execute_query_subprocess(query, max_rows=max_rows)

        with self._lock:
            cursor = self._get_connection().cursor()
            try:
                cursor.execute(query, params or [])
                if cursor.description:
                    columns = [description[0] for description in cursor.description]
                    rows = cursor.fetchmany(max_rows)
                    return {
                        "columns": columns,
                        "rows": rows,
                        "row_count": len(rows),
                        "max_rows": max_rows,
                    }

                return {
                    "columns": [],
                    "rows": [],
                    "row_count": getattr(cursor, "rowcount", -1),
                    "max_rows": max_rows,
                }
            except Exception:
                self.close()
                raise
            finally:
                cursor.close()

    def execute_query_subprocess(self, query: str, max_rows: int = 100) -> dict[str, Any]:
        if self.use_server_mode():
            return self.execute_query_server(query, max_rows=max_rows)

        if not self.settings.is_complete:
            raise RuntimeError("Xactly JDBC settings are incomplete. Fill the XACTLY_JDBC_* values in .env.")

        driver_path = self.resolve_driver_path(self.settings.jar_path)
        if not Path(driver_path).exists():
            raise FileNotFoundError(f"Xactly JDBC driver not found at {driver_path}")

        self.apply_java_home()
        self.compile_helper()

        helper_dir = str(self.helper_java_path().parent)
        classpath = os.pathsep.join([driver_path, helper_dir])
        env = os.environ.copy()
        env["XACTLY_JDBC_URL"] = self.get_jdbc_url()
        env["XACTLY_JDBC_USER"] = self.settings.user
        env["XACTLY_JDBC_PASSWORD"] = self.settings.password
        env["XACTLY_JDBC_DRIVER_CLASS"] = self.settings.driver_class

        completed = subprocess.run(
            [self.java_bin("java"), "-cp", classpath, "XactlyJdbcQuery", str(max_rows), query],
            check=False,
            capture_output=True,
            text=True,
            env=env,
        )
        if completed.returncode != 0:
            error = completed.stderr.strip() or completed.stdout.strip()
            raise RuntimeError(f"Xactly JDBC query failed: {error}")

        try:
            output = completed.stdout.strip().splitlines()[-1]
            return json.loads(output)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Xactly JDBC helper returned invalid JSON: {completed.stdout}") from exc

    def use_server_mode(self) -> bool:
        return os.getenv("XACTLY_JDBC_SERVER", "true").strip().lower() in {"true", "1", "yes", "y"}

    def server_pool_size(self) -> int:
        try:
            return max(1, int(os.getenv("XACTLY_JDBC_POOL_SIZE", "3")))
        except ValueError:
            return 3

    def _java_env(self) -> dict[str, str]:
        env = os.environ.copy()
        env["XACTLY_JDBC_URL"] = self.get_jdbc_url()
        env["XACTLY_JDBC_USER"] = self.settings.user
        env["XACTLY_JDBC_PASSWORD"] = self.settings.password
        env["XACTLY_JDBC_DRIVER_CLASS"] = self.settings.driver_class
        return env

    def _java_classpath(self) -> str:
        driver_path = self.resolve_driver_path(self.settings.jar_path)
        if not Path(driver_path).exists():
            raise FileNotFoundError(f"Xactly JDBC driver not found at {driver_path}")
        helper_dir = str(self.helper_java_path().parent)
        return os.pathsep.join([driver_path, helper_dir])

    def _start_server(self) -> subprocess.Popen[str]:
        if not self.settings.is_complete:
            raise RuntimeError("Xactly JDBC settings are incomplete. Fill the XACTLY_JDBC_* values in .env.")

        self.apply_java_home()
        self.compile_helper()

        proc = subprocess.Popen(
            [self.java_bin("java"), "-cp", self._java_classpath(), "XactlyJdbcQuery", "--server"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=self._java_env(),
            bufsize=1,
        )
        ready_line = proc.stdout.readline() if proc.stdout is not None else ""
        if proc.poll() is not None:
            stderr = proc.stderr.read().strip() if proc.stderr is not None else ""
            raise RuntimeError(f"Xactly JDBC helper failed to start: {stderr or ready_line}")
        try:
            ready = json.loads(ready_line)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Xactly JDBC helper returned invalid startup output: {ready_line}") from exc
        if not ready.get("ready"):
            raise RuntimeError(f"Xactly JDBC helper did not become ready: {ready_line}")

        return proc

    def _ensure_server(self) -> subprocess.Popen[str]:
        if self._server_proc is not None and self._server_proc.poll() is None:
            return self._server_proc
        self._server_proc = self._start_server()
        return self._server_proc

    def _checkout_server_slot(self) -> _ServerSlot:
        if self.server_pool_size() <= 1:
            return _ServerSlot(self._ensure_server())

        try:
            slot = self._server_pool.get_nowait()
            if slot.proc.poll() is None:
                return slot
            with self._server_pool_lock:
                self._server_pool_created = max(0, self._server_pool_created - 1)
        except Empty:
            pass

        with self._server_pool_lock:
            if self._server_pool_created < self.server_pool_size():
                self._server_pool_created += 1
                try:
                    return _ServerSlot(self._start_server())
                except Exception:
                    self._server_pool_created = max(0, self._server_pool_created - 1)
                    raise

        while True:
            slot = self._server_pool.get()
            if slot.proc.poll() is None:
                return slot

            with self._server_pool_lock:
                self._server_pool_created = max(0, self._server_pool_created - 1)
                if self._server_pool_created < self.server_pool_size():
                    self._server_pool_created += 1
                    try:
                        return _ServerSlot(self._start_server())
                    except Exception:
                        self._server_pool_created = max(0, self._server_pool_created - 1)
                        raise

    def _checkin_server_slot(self, slot: _ServerSlot) -> None:
        if self.server_pool_size() <= 1:
            return
        if slot.proc.poll() is None:
            self._server_pool.put(slot)
            return
        with self._server_pool_lock:
            self._server_pool_created = max(0, self._server_pool_created - 1)

    def execute_query_server(self, query: str, max_rows: int = 100) -> dict[str, Any]:
        if self.server_pool_size() <= 1:
            lock_context = self._lock
        else:
            lock_context = _NullLock()

        with lock_context:
            slot = self._checkout_server_slot()
            proc = slot.proc
            if proc.stdin is None or proc.stdout is None:
                raise RuntimeError("Xactly JDBC helper streams are not available.")

            try:
                with self._server_pool_lock:
                    self._server_request_id += 1
                    request_id = str(self._server_request_id)
                encoded_query = base64.b64encode(query.encode("utf-8")).decode("ascii")
                proc.stdin.write(f"{request_id}\t{int(max_rows)}\t{encoded_query}\n")
                proc.stdin.flush()

                readable, _, _ = select.select(
                    [proc.stdout], [], [], self.server_response_timeout()
                )
                if not readable:
                    proc.kill()
                    if proc is self._server_proc:
                        self._server_proc = None
                    raise TimeoutError(
                        "Xactly JDBC helper did not respond within "
                        f"{self.server_response_timeout():.1f} seconds"
                    )
                response_line = proc.stdout.readline()
                if not response_line:
                    stderr = proc.stderr.read().strip() if proc.stderr is not None else ""
                    if proc is self._server_proc:
                        self._server_proc = None
                    raise RuntimeError(f"Xactly JDBC helper stopped unexpectedly: {stderr}")

                try:
                    response = json.loads(response_line)
                except json.JSONDecodeError as exc:
                    raise RuntimeError(f"Xactly JDBC helper returned invalid JSON: {response_line}") from exc

                if response.get("request_id") != request_id:
                    raise RuntimeError(f"Xactly JDBC helper response mismatch: {response_line}")
                if not response.get("ok"):
                    raise RuntimeError(f"Xactly JDBC query failed: {response.get('error')}")
                return response["result"]
            finally:
                self._checkin_server_slot(slot)

    def query_df(self, query: str, params: Sequence[Any] | None = None, max_rows: int = 100) -> pd.DataFrame:
        result = self.execute_query(query, params=params, max_rows=max_rows)
        return pd.DataFrame(result["rows"], columns=result["columns"])

    def ping(self) -> tuple[bool, str]:
        try:
            self.execute_query("select 1", max_rows=1)
            return True, "Connected to Xactly JDBC"
        except Exception as exc:  # pragma: no cover - external system.
            return False, str(exc)

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            finally:
                self._conn = None
        if self._server_proc is not None:
            try:
                if self._server_proc.stdin is not None:
                    self._server_proc.stdin.close()
                self._server_proc.terminate()
            finally:
                self._server_proc = None
        while not self._server_pool.empty():
            slot = self._server_pool.get_nowait()
            try:
                if slot.proc.stdin is not None:
                    slot.proc.stdin.close()
                slot.proc.terminate()
            except Exception:
                pass
        with self._server_pool_lock:
            self._server_pool_created = 0


class _NullLock:
    def __enter__(self) -> None:
        return None

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> bool:
        return False
