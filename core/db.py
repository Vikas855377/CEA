from __future__ import annotations

from threading import RLock
from typing import Any

import pandas as pd

from core.config import SnowflakeSettings


class SnowflakeClient:
    def __init__(self, settings: SnowflakeSettings | None = None) -> None:
        self.settings = settings or SnowflakeSettings.from_env()
        self._conn: Any | None = None
        self._lock = RLock()

    def _connect(self) -> Any:
        if not self.settings.is_complete:
            raise RuntimeError("Snowflake settings are incomplete. Fill the Snowflake values in .env.")

        import snowflake.connector

        kwargs = {
            "account": self.settings.account,
            "user": self.settings.user,
            "password": self.settings.password,
            "warehouse": self.settings.warehouse,
            "database": self.settings.database,
            "schema": self.settings.schema,
        }
        if self.settings.role:
            kwargs["role"] = self.settings.role
        return snowflake.connector.connect(**kwargs)

    def _get_connection(self) -> Any:
        if self._conn is None or self._conn.is_closed():
            self._conn = self._connect()
        return self._conn

    def query_df(self, sql: str, params: dict[str, Any] | None = None) -> pd.DataFrame:
        with self._lock:
            try:
                return pd.read_sql(sql, self._get_connection(), params=params)
            except Exception:
                if self._conn is not None and not self._conn.is_closed():
                    self._conn.close()
                self._conn = None
                raise

    def execute(self, sql: str, params: dict[str, Any] | None = None) -> None:
        with self._lock:
            conn = self._get_connection()
            try:
                with conn.cursor() as cursor:
                    cursor.execute(sql, params or None)
            except Exception:
                if self._conn is not None and not self._conn.is_closed():
                    self._conn.close()
                self._conn = None
                raise

    def ping(self) -> tuple[bool, str]:
        try:
            with self._connect() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("select current_version()")
                    version = cursor.fetchone()[0]
            return True, f"Connected to Snowflake {version}"
        except Exception as exc:  # pragma: no cover - external system.
            return False, str(exc)
