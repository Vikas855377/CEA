"""Explicit Xactly refresh-batch coordination for monitoring data loads."""

from __future__ import annotations

import logging
import os
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Protocol, Sequence

import pandas as pd

from core.xactly_jdbc import XactlyJdbcClient


BATCH_TABLE = "delta.cea_refresh_batch"
CLIENT_TABLE = "delta.cea_refresh_client"
ACTIVE_CLIENT_STATUSES = {"PENDING", "IN_PROGRESS"}
TERMINAL_CLIENT_STATUSES = {"COMPLETED", "FAILED", "TIMED_OUT"}


class RefreshClientConfig(Protocol):
    name: str
    monitoring_client_id: int
    monitoring_client_name: str


@dataclass(frozen=True)
class RefreshBatchResult:
    batch_id: str
    status: str
    client_statuses: dict[int, str]
    error_message: str = ""


def coordination_enabled() -> bool:
    return os.getenv("CEA_REFRESH_COORDINATION_ENABLED", "false").strip().lower() in {
        "1",
        "true",
        "yes",
        "y",
    }


def _sql_string(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _normalized_rows(frame: pd.DataFrame) -> list[dict[str, object]]:
    normalized = frame.copy()
    normalized.columns = [str(column).upper() for column in normalized.columns]
    return normalized.to_dict(orient="records")


class RefreshBatchCoordinator:
    def __init__(
        self,
        *,
        db: XactlyJdbcClient | None = None,
        logger: logging.Logger | None = None,
        timeout_seconds: int | None = None,
        poll_seconds: float | None = None,
    ) -> None:
        self.db = db or XactlyJdbcClient()
        self.logger = logger or logging.getLogger(__name__)
        self.timeout_seconds = timeout_seconds or int(
            os.getenv("CEA_REFRESH_BATCH_TIMEOUT_SECONDS", "900")
        )
        self.poll_seconds = poll_seconds or float(
            os.getenv("CEA_REFRESH_BATCH_POLL_SECONDS", "1")
        )
        if self.timeout_seconds < 1 or self.poll_seconds <= 0:
            raise ValueError("Refresh timeout and poll interval must be positive")

    def _execute(self, sql: str) -> None:
        self.db.query_df(sql, max_rows=1)

    def latest_batch(self) -> dict[str, object] | None:
        frame = self.db.query_df(
            f"select BATCH_ID, STATUS, EXPECTED_CLIENTS, STARTED_AT, "
            f"COMPLETED_AT, ERROR_MESSAGE from {BATCH_TABLE} "
            "order by STARTED_AT desc limit 1",
            max_rows=1,
        )
        rows = _normalized_rows(frame)
        return rows[0] if rows else None

    def client_statuses(self, batch_id: str) -> dict[int, str]:
        frame = self.db.query_df(
            f"select CLIENTID, STATUS from {CLIENT_TABLE} "
            f"where BATCH_ID = {_sql_string(batch_id)}",
            max_rows=10000,
        )
        statuses: dict[int, str] = {}
        for row in _normalized_rows(frame):
            statuses[int(row["CLIENTID"])] = str(row.get("STATUS") or "").upper()
        return statuses

    def _is_stale(self, batch: dict[str, object]) -> bool:
        started_at = pd.to_datetime(batch.get("STARTED_AT"), utc=True, errors="coerce")
        if pd.isna(started_at):
            return True
        age_seconds = (datetime.now(timezone.utc) - started_at.to_pydatetime()).total_seconds()
        return age_seconds >= self.timeout_seconds

    def _timeout_batch(self, batch_id: str, message: str) -> None:
        quoted_batch = _sql_string(batch_id)
        quoted_message = _sql_string(message)
        self._execute(
            f"update {CLIENT_TABLE} set STATUS = 'TIMED_OUT', "
            "COMPLETED_AT = UTCDateTime(), "
            f"ERROR_MESSAGE = {quoted_message} where BATCH_ID = {quoted_batch} "
            "and STATUS in ('PENDING', 'IN_PROGRESS')"
        )
        self._execute(
            f"update {BATCH_TABLE} set STATUS = 'TIMED_OUT', "
            "COMPLETED_AT = UTCDateTime(), "
            f"ERROR_MESSAGE = {quoted_message} where BATCH_ID = {quoted_batch} "
            "and STATUS = 'IN_PROGRESS'"
        )

    def _reconcile_active_batch(self, batch: dict[str, object]) -> str:
        """Repair a parent batch after a scheduler restart or interrupted waiter."""
        batch_id = str(batch["BATCH_ID"])
        try:
            expected_clients = int(batch.get("EXPECTED_CLIENTS") or 0)
        except (TypeError, ValueError):
            expected_clients = 0
        statuses = self.client_statuses(batch_id)
        if expected_clients > 0 and not statuses:
            message = "Recovered orphaned refresh batch with no client rows"
            self._finish_batch(batch_id, "FAILED", message)
            self.logger.error(
                "Reconciled empty orphaned refresh batch %s as FAILED",
                batch_id,
            )
            return "FAILED"
        failures = {
            client_id: status
            for client_id, status in statuses.items()
            if status in {"FAILED", "TIMED_OUT"}
        }
        if failures:
            message = f"Recovered terminal client failure: {failures}"
            self._finish_batch(batch_id, "FAILED", message)
            self.logger.error(
                "Reconciled orphaned refresh batch %s as FAILED: %s",
                batch_id,
                failures,
            )
            return "FAILED"

        if (
            expected_clients > 0
            and len(statuses) == expected_clients
            and all(status == "COMPLETED" for status in statuses.values())
        ):
            self._finish_batch(batch_id, "COMPLETED")
            self.logger.warning(
                "Reconciled orphaned refresh batch %s as COMPLETED from %s child rows",
                batch_id,
                len(statuses),
            )
            return "COMPLETED"
        return "IN_PROGRESS"

    def prepare_batch(self, clients: Sequence[RefreshClientConfig]) -> str | None:
        if not clients:
            raise ValueError("At least one refresh client is required")
        latest = self.latest_batch()
        if latest and str(latest.get("STATUS") or "").upper() == "IN_PROGRESS":
            active_batch_id = str(latest["BATCH_ID"])
            reconciled_status = self._reconcile_active_batch(latest)
            if reconciled_status == "IN_PROGRESS" and not self._is_stale(latest):
                self.logger.warning(
                    "Skipping refresh launch because batch %s is still IN_PROGRESS",
                    active_batch_id,
                )
                return None
            if reconciled_status == "IN_PROGRESS":
                self.logger.error("Timing out stale refresh batch %s", active_batch_id)
                self._timeout_batch(active_batch_id, "Refresh batch exceeded its timeout")

        batch_id = str(uuid.uuid4())
        self._execute(
            f"insert into {BATCH_TABLE} "
            "(BATCH_ID, STATUS, EXPECTED_CLIENTS, STARTED_AT, COMPLETED_AT, ERROR_MESSAGE) "
            f"values ({_sql_string(batch_id)}, 'IN_PROGRESS', {len(clients)}, "
            "UTCDateTime(), NULL, NULL)"
        )
        client_values = ", ".join(
            f"({_sql_string(batch_id)}, {int(client.monitoring_client_id)}, "
            f"{_sql_string(client.monitoring_client_name)}, 'PENDING', NULL, NULL, NULL)"
            for client in clients
        )
        try:
            self._execute(
                f"insert into {CLIENT_TABLE} "
                "(BATCH_ID, CLIENTID, CLIENTNAME, STATUS, STARTED_AT, "
                f"COMPLETED_AT, ERROR_MESSAGE) values {client_values}"
            )
        except Exception:
            self._finish_batch(
                batch_id,
                "FAILED",
                "Could not create refresh client coordination rows",
            )
            raise
        self.logger.info(
            "Created refresh batch %s with %s pending clients",
            batch_id,
            len(clients),
        )
        return batch_id

    def mark_runner_failed(self, batch_id: str, client: RefreshClientConfig, message: str) -> None:
        quoted_message = _sql_string(message)
        self._execute(
            f"update {CLIENT_TABLE} set STATUS = 'FAILED', "
            "COMPLETED_AT = UTCDateTime(), "
            f"ERROR_MESSAGE = {quoted_message} "
            f"where BATCH_ID = {_sql_string(batch_id)} "
            f"and CLIENTID = {int(client.monitoring_client_id)} "
            "and STATUS in ('PENDING', 'IN_PROGRESS')"
        )

    def _finish_batch(self, batch_id: str, status: str, error_message: str = "") -> None:
        error_sql = _sql_string(error_message) if error_message else "NULL"
        self._execute(
            f"update {BATCH_TABLE} set STATUS = {_sql_string(status)}, "
            "COMPLETED_AT = UTCDateTime(), "
            f"ERROR_MESSAGE = {error_sql} where BATCH_ID = {_sql_string(batch_id)} "
            "and STATUS = 'IN_PROGRESS'"
        )

    def wait_for_batch(self, batch_id: str, expected_client_ids: set[int]) -> RefreshBatchResult:
        deadline = time.monotonic() + self.timeout_seconds
        while True:
            statuses = self.client_statuses(batch_id)
            missing = expected_client_ids.difference(statuses)
            failed = {
                client_id: status
                for client_id, status in statuses.items()
                if status in {"FAILED", "TIMED_OUT"}
            }
            if failed:
                message = f"Client refresh failed: {failed}"
                self._finish_batch(batch_id, "FAILED", message)
                return RefreshBatchResult(batch_id, "FAILED", statuses, message)

            if not missing and expected_client_ids and all(
                statuses.get(client_id) == "COMPLETED" for client_id in expected_client_ids
            ):
                self._finish_batch(batch_id, "COMPLETED")
                return RefreshBatchResult(batch_id, "COMPLETED", statuses)

            if time.monotonic() >= deadline:
                message = "Refresh batch exceeded its timeout"
                self._timeout_batch(batch_id, message)
                return RefreshBatchResult(batch_id, "TIMED_OUT", statuses, message)

            self.logger.info(
                "Waiting for refresh batch %s: statuses=%s missing=%s",
                batch_id,
                statuses,
                sorted(missing),
            )
            time.sleep(self.poll_seconds)
