from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

from core.db import SnowflakeClient
from core.xactly_jdbc import XactlyJdbcClient


@dataclass(frozen=True)
class SnapshotTable:
    source_name: str
    target_name: str


@dataclass(frozen=True)
class SnapshotSyncResult:
    ready: bool
    incoming_counts: dict[str, int]
    snapshot_counts: dict[str, int]
    blocked_tables: tuple[str, ...] = ()


class SnowflakeSnapshotGuard:
    CONTROL_TABLE = "CEA_MONITORING_SNAPSHOT_COUNTS"

    def __init__(
        self,
        *,
        xactly: XactlyJdbcClient,
        snowflake: SnowflakeClient,
        tables: tuple[SnapshotTable, ...],
    ) -> None:
        self.xactly = xactly
        self.snowflake = snowflake
        self.tables = tables
        self._control_table_ready = False

    def sync(self) -> SnapshotSyncResult:
        self._ensure_control_table()
        incoming_counts = self._incoming_counts()
        snapshot_counts = self._snapshot_counts()
        blocked = tuple(
            spec.target_name
            for spec in self.tables
            if incoming_counts[spec.target_name] == 0
            or incoming_counts[spec.target_name] < snapshot_counts[spec.target_name]
        )
        if blocked:
            return SnapshotSyncResult(
                False,
                incoming_counts,
                snapshot_counts,
                blocked,
            )

        self._save_counts(incoming_counts, snapshot_counts)
        return SnapshotSyncResult(True, incoming_counts, incoming_counts)

    def _ensure_control_table(self) -> None:
        if self._control_table_ready:
            return
        self.snowflake.execute(
            f"CREATE TABLE IF NOT EXISTS {self.CONTROL_TABLE} ("
            "TABLE_NAME VARCHAR PRIMARY KEY, "
            "SOURCE_COUNT NUMBER NOT NULL, "
            "ACCEPTED_AT TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP()"
            ")"
        )
        self._control_table_ready = True

    def _incoming_counts(self) -> dict[str, int]:
        def load(spec: SnapshotTable) -> tuple[str, int]:
            result = self.xactly.query_df(
                f"select count(*) as ROW_COUNT from {spec.source_name}",
                max_rows=1,
            )
            if result.empty:
                return spec.target_name, 0
            result.columns = [str(column).upper() for column in result.columns]
            return spec.target_name, int(result.iloc[0]["ROW_COUNT"])

        with ThreadPoolExecutor(max_workers=min(len(self.tables), 4)) as executor:
            return dict(executor.map(load, self.tables))

    def _snapshot_counts(self) -> dict[str, int]:
        result = self.snowflake.query_df(
            f"select TABLE_NAME, SOURCE_COUNT from {self.CONTROL_TABLE}"
        )
        result.columns = [str(column).upper() for column in result.columns]
        stored = {
            str(row["TABLE_NAME"]).lower(): int(row["SOURCE_COUNT"])
            for _, row in result.iterrows()
        }
        counts = {
            spec.target_name: stored.get(spec.target_name.lower(), 0)
            for spec in self.tables
        }
        return counts

    def _save_counts(
        self,
        incoming_counts: dict[str, int],
        snapshot_counts: dict[str, int],
    ) -> None:
        for table_name, incoming_count in incoming_counts.items():
            if incoming_count <= snapshot_counts[table_name]:
                continue
            self.snowflake.execute(
                f"MERGE INTO {self.CONTROL_TABLE} target "
                "USING (SELECT %(table_name)s::VARCHAR AS TABLE_NAME, "
                "%(source_count)s::NUMBER AS SOURCE_COUNT) source "
                "ON target.TABLE_NAME = source.TABLE_NAME "
                "WHEN MATCHED THEN UPDATE SET "
                "target.SOURCE_COUNT = source.SOURCE_COUNT, "
                "target.ACCEPTED_AT = CURRENT_TIMESTAMP() "
                "WHEN NOT MATCHED THEN INSERT (TABLE_NAME, SOURCE_COUNT, ACCEPTED_AT) "
                "VALUES (source.TABLE_NAME, source.SOURCE_COUNT, CURRENT_TIMESTAMP())",
                {"table_name": table_name, "source_count": incoming_count},
            )
