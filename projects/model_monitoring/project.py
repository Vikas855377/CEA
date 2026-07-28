from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any

import pandas as pd

from core.xactly_jdbc import XactlyJdbcClient
from core.db import SnowflakeClient
from core.llm import LLMClient
from projects.model_monitoring.agents import ModelMonitoringAgent
from projects.model_monitoring.agents.model_monitoring_agent import (
    extract_form_metadata,
    metadata_for,
    minute_matches_trigger_window,
    previous_same_type_times,
)
from projects.model_monitoring.logging_config import get_logger
from projects.model_monitoring.snowflake_snapshot import SnapshotTable, SnowflakeSnapshotGuard
from projects.ProcessRunAutomation.refresh_batch import (
    RefreshBatchCoordinator,
    coordination_enabled as refresh_coordination_enabled,
)


LOGGER = get_logger()
TABLES = {
    "cea_cealog": "delta.cea_cealog",
    "cea_formenginelog": "delta.cea_formenginelog",
    "cea_processlog": "delta.cea_processlog",
    "cea_sessionquery": "delta.cea_sessionquery",
}
CSV_TABLE_FILENAMES = {
    "cea_processlog": "processlog.csv",
    "cea_sessionquery": "sessionquery.csv",
    "cea_formenginelog": "formenginelog.csv",
    "cea_cealog": "cealog.csv",
}

ACTIVE_MODEL_WORKFLOW = "SendEmailAfterModelExecution"
REQUIRED_MONITORING_TABLES = tuple(TABLES)
DATA_REFRESH_MARKER = "__data_refresh_in_progress__"
PROCESSLOG_COLUMNS = [
    "CLIENTID",
    "CLIENTNAME",
    "PROCESS_ID",
    "WORKFLOWNAME",
    "ORIGINAL_UTC",
    "STARTEDBY",
    "ISRUNNING",
    "IST_TILL_MINUTES",
]
SESSIONQUERY_COLUMNS = [
    "CLIENTID",
    "CLIENTNAME",
    "SESSION_ID",
    "STATUS",
    "DURATIONINSECONDS",
    "START_TIME",
    "IST_TILL_MINUTES",
]
FORMENGINELOG_COLUMNS = [
    "CLIENTID",
    "CLIENTNAME",
    "FORMENGINELOGID",
    "CREATED_DATE",
    "OPERATION",
    "FILENAME",
    "FILEPATH",
    "FULL_RESULT",
    "CALCULATIONTYPE",
    "IST_TILL_MINUTES",
]
CEALOG_COLUMNS = [
    "CLIENTID",
    "CLIENTNAME",
    "CEALOGID",
    "ID",
    "RUN_ID",
    "CREATED_DATE",
    "DESCRIPTION",
    "STEP_NO",
    "EXECUTION_ID",
    "LEVEL",
    "ROWCOUNT",
    "MODELID",
    "IST_TILL_MINUTES",
]

SNOWFLAKE_SNAPSHOT_TABLES = (
    SnapshotTable(TABLES["cea_cealog"], "cea_cealog"),
    SnapshotTable(TABLES["cea_formenginelog"], "cea_formenginelog"),
    SnapshotTable(TABLES["cea_processlog"], "cea_processlog"),
    SnapshotTable(TABLES["cea_sessionquery"], "cea_sessionquery"),
)


@dataclass(frozen=True)
class ProjectRun:
    result: dict[str, Any]
    payload: dict[str, Any]
    tables: dict[str, pd.DataFrame]


class TableQueryError(RuntimeError):
    def __init__(self, table_name: str, original: Exception) -> None:
        super().__init__(f"Failed to load {table_name}: {original}")
        self.table_name = table_name
        self.original = original


def env_truthy(name: str, *, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"true", "1", "yes", "y"}


def use_csv_source() -> bool:
    return env_truthy("MODEL_MONITORING_USE_CSV", default=False)


def csv_paths_from_env() -> dict[str, str]:
    data_dir = os.getenv(
        "MODEL_MONITORING_CSV_DIR",
        "data/model_monitoring/load_test",
    ).strip()
    return {
        table_name: os.getenv(
            f"MODEL_MONITORING_{table_name.upper()}_CSV",
            os.path.join(data_dir, filename),
        ).strip()
        for table_name, filename in CSV_TABLE_FILENAMES.items()
    }


def csv_client_dir() -> str:
    return os.getenv(
        "MODEL_MONITORING_CSV_CLIENT_DIR",
        os.path.join(os.getenv("MODEL_MONITORING_CSV_DIR", "data/model_monitoring/load_test"), "by_client"),
    ).strip()


def csv_client_slug(*, clientid: str = "", clientname: str = "") -> str:
    raw = "_".join(value for value in [clientid.strip(), clientname.strip()] if value)
    return "".join(char if char.isalnum() or char in {"-", "_"} else "_" for char in raw).strip("_")


def csv_path_for_table(
    table_name: str,
    *,
    clientid: str = "",
    clientname: str = "",
) -> str:
    if clientid.strip() or clientname.strip():
        client_path = os.path.join(csv_client_dir(), csv_client_slug(clientid=clientid, clientname=clientname), CSV_TABLE_FILENAMES[table_name])
        if os.path.exists(client_path):
            return client_path
    return csv_paths_from_env().get(table_name, "")


def read_monitoring_csv(path: str) -> pd.DataFrame:
    if not path or not os.path.exists(path):
        return pd.DataFrame()
    return normalize_jdbc_columns(pd.read_csv(path, dtype=str, encoding="utf-8-sig"))


def read_monitoring_csv_filtered(
    table_name: str,
    *,
    clientid: str = "",
    clientname: str = "",
    minutes: list[str] | None = None,
    limit: int = 5000,
) -> pd.DataFrame:
    path = csv_path_for_table(table_name, clientid=clientid, clientname=clientname)
    if not path or not os.path.exists(path):
        return pd.DataFrame()

    minute_values = set(minutes or [])
    chunks: list[pd.DataFrame] = []
    chunksize = int(os.getenv("MODEL_MONITORING_CSV_CHUNK_SIZE", "50000"))
    for chunk in pd.read_csv(path, dtype=str, encoding="utf-8-sig", chunksize=chunksize):
        filtered = normalize_jdbc_columns(chunk)
        if clientid.strip() and "CLIENTID" in filtered.columns:
            filtered = filtered[filtered["CLIENTID"].astype(str) == clientid.strip()]
        if clientname.strip() and "CLIENTNAME" in filtered.columns:
            filtered = filtered[filtered["CLIENTNAME"].astype(str) == clientname.strip()]
        if minute_values:
            if "IST_TILL_MINUTES" not in filtered.columns:
                continue
            filtered = filtered[minute_labels(filtered["IST_TILL_MINUTES"]).isin(minute_values)]
        if filtered.empty:
            continue
        chunks.append(filtered)

    if not chunks:
        return pd.DataFrame()
    combined = pd.concat(chunks, ignore_index=True)
    return sort_monitoring_table(table_name, combined).head(limit).reset_index(drop=True)


def minute_labels(values: pd.Series) -> pd.Series:
    labels = values.astype(str).str.replace("T", " ", regex=False).str.slice(0, 16)
    return labels.where(labels.str.len() == 16, "")


def filter_monitoring_csv(
    df: pd.DataFrame,
    *,
    clientid: str = "",
    clientname: str = "",
    limit: int = 5000,
    table_name: str,
) -> pd.DataFrame:
    filtered = normalize_jdbc_columns(df) if not df.empty else df.copy()
    if filtered.empty:
        return filtered

    if clientid.strip() and "CLIENTID" in filtered.columns:
        filtered = filtered[filtered["CLIENTID"].astype(str) == clientid.strip()]
    if clientname.strip() and "CLIENTNAME" in filtered.columns:
        filtered = filtered[filtered["CLIENTNAME"].astype(str) == clientname.strip()]

    return sort_monitoring_table(table_name, filtered).head(limit).reset_index(drop=True)


def filter_by_minutes(
    df: pd.DataFrame,
    minutes: list[str],
    *,
    limit: int,
    table_name: str,
) -> pd.DataFrame:
    if df.empty or "IST_TILL_MINUTES" not in df.columns:
        return df.head(0).copy()
    minute_values = set(minutes)
    if not minute_values:
        return df.head(0).copy()
    normalized_minutes = minute_labels(df["IST_TILL_MINUTES"])
    filtered = df[normalized_minutes.isin(minute_values)]
    return sort_monitoring_table(table_name, filtered).head(limit).reset_index(drop=True)


def filter_by_relevant_form_rows(
    formenginelog: pd.DataFrame,
    *,
    processlog: pd.DataFrame,
    limit: int,
) -> pd.DataFrame:
    trigger_windows = trigger_minute_windows(processlog)
    if not trigger_windows:
        return formenginelog.head(0).copy()

    filtered = filter_by_minutes(
        formenginelog,
        trigger_windows,
        limit=limit,
        table_name="cea_formenginelog",
    )
    if not filtered.empty:
        return filtered

    # Keep a small historical sample for inactive or incomplete fixture scenarios.
    return sort_monitoring_table("cea_formenginelog", formenginelog).head(limit).reset_index(drop=True)


def sort_monitoring_table(table_name: str, df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    sorted_df = df.copy()
    if table_name == "cea_cealog" and "ID" in sorted_df.columns:
        sorted_df["_MONITOR_SORT_ID"] = pd.to_numeric(sorted_df["ID"], errors="coerce")
        return (
            sorted_df.sort_values("_MONITOR_SORT_ID", ascending=False, na_position="last")
            .drop(columns=["_MONITOR_SORT_ID"])
            .reset_index(drop=True)
        )
    if "IST_TILL_MINUTES" in sorted_df.columns:
        sorted_df["_MONITOR_SORT_TIME"] = pd.to_datetime(sorted_df["IST_TILL_MINUTES"], errors="coerce")
        return (
            sorted_df.sort_values("_MONITOR_SORT_TIME", ascending=False, na_position="last")
            .drop(columns=["_MONITOR_SORT_TIME"])
            .reset_index(drop=True)
        )
    return sorted_df.reset_index(drop=True)


class ModelMonitoringProject:
    def __init__(
        self,
        db: XactlyJdbcClient | None = None,
        llm: LLMClient | None = None,
    ) -> None:
        self.db = db or XactlyJdbcClient()
        self.agent = ModelMonitoringAgent(llm=llm or LLMClient())
        self._snapshot_guard: SnowflakeSnapshotGuard | None = None
        self._refresh_coordinator: RefreshBatchCoordinator | None = None
        LOGGER.info("ModelMonitoringProject initialized")

    def load_client_options(self, *, limit: int | None = None) -> list[dict[str, Any]]:
        if use_csv_source():
            return self.load_client_options_from_csv(limit=limit)

        limit = limit or int(os.getenv("MODEL_MONITORING_CLIENT_OPTION_LIMIT", "1000"))
        active_only = os.getenv("MODEL_MONITORING_ACTIVE_CLIENTS_ONLY", "true").strip().lower() in {
            "true",
            "1",
            "yes",
            "y",
        }
        filters = [
            f"WORKFLOWNAME = {sql_literal(ACTIVE_MODEL_WORKFLOW)}",
            "CLIENTNAME is not null",
        ]
        if active_only:
            filters.append("ISRUNNING = 'true'")

        rows = normalize_jdbc_columns(
            self.db.query_df(
                f"""
                select {", ".join(PROCESSLOG_COLUMNS)}
                from {TABLES["cea_processlog"]}
                where {" and ".join(filters)}
                order by IST_TILL_MINUTES desc
                """,
                max_rows=limit,
            )
        )
        if rows.empty or "CLIENTNAME" not in rows.columns:
            return []

        options: list[dict[str, Any]] = []
        for clientname, client_rows in rows.groupby("CLIENTNAME", dropna=True):
            name = str(clientname).strip()
            if not name:
                continue
            is_running = any(
                str(row.get("WORKFLOWNAME", "")) == ACTIVE_MODEL_WORKFLOW
                and truthy(row.get("ISRUNNING"))
                for _, row in client_rows.iterrows()
            )
            active_processlog = None
            if is_running:
                active_rows = client_rows[
                    (client_rows["WORKFLOWNAME"].astype(str) == ACTIVE_MODEL_WORKFLOW)
                    & (client_rows["ISRUNNING"].map(truthy))
                ]
                if not active_rows.empty:
                    active_processlog = active_rows.iloc[0].where(pd.notna(active_rows.iloc[0]), None).to_dict()
            options.append(
                {
                    "clientname": name,
                    "is_running": is_running,
                    "active_processlog": active_processlog,
                }
            )
        return sorted(options, key=lambda row: (not row["is_running"], row["clientname"].lower()))

    def load_client_options_from_csv(self, *, limit: int | None = None) -> list[dict[str, Any]]:
        limit = limit or int(os.getenv("MODEL_MONITORING_CLIENT_OPTION_LIMIT", "1000"))
        active_only = env_truthy("MODEL_MONITORING_ACTIVE_CLIENTS_ONLY", default=True)
        rows = self.load_csv_table("cea_processlog")
        if rows.empty or "CLIENTNAME" not in rows.columns:
            return []

        if "WORKFLOWNAME" in rows.columns:
            rows = rows[rows["WORKFLOWNAME"].astype(str) == ACTIVE_MODEL_WORKFLOW]
        rows = rows[rows["CLIENTNAME"].notna()]
        if active_only and "ISRUNNING" in rows.columns:
            rows = rows[rows["ISRUNNING"].map(truthy)]
        rows = sort_monitoring_table("cea_processlog", rows).head(limit)

        options: list[dict[str, Any]] = []
        for clientname, client_rows in rows.groupby("CLIENTNAME", dropna=True):
            name = str(clientname).strip()
            if not name:
                continue
            is_running = any(
                str(row.get("WORKFLOWNAME", "")) == ACTIVE_MODEL_WORKFLOW
                and truthy(row.get("ISRUNNING"))
                for _, row in client_rows.iterrows()
            )
            active_processlog = None
            if is_running:
                active_rows = client_rows[
                    (client_rows["WORKFLOWNAME"].astype(str) == ACTIVE_MODEL_WORKFLOW)
                    & (client_rows["ISRUNNING"].map(truthy))
                ]
                if not active_rows.empty:
                    active_processlog = active_rows.iloc[0].where(pd.notna(active_rows.iloc[0]), None).to_dict()
            options.append(
                {
                    "clientname": name,
                    "is_running": is_running,
                    "active_processlog": active_processlog,
                }
            )
        return sorted(options, key=lambda row: (not row["is_running"], row["clientname"].lower()))

    def load_from_snowflake(
        self,
        *,
        clientid: str = "",
        clientname: str = "",
        limit_per_table: int = 5000,
    ) -> dict[str, pd.DataFrame]:
        return self.load_from_xactly(
            clientid=clientid,
            clientname=clientname,
            limit_per_table=limit_per_table,
        )
        params: dict[str, Any] = {
            "limit_per_table": int(limit_per_table),
        }
        client_filter = []
        if clientid.strip():
            client_filter.append("CLIENTID = %(clientid)s")
            params["clientid"] = clientid.strip()
        if clientname.strip():
            client_filter.append("CLIENTNAME = %(clientname)s")
            params["clientname"] = clientname.strip()

        where_clause = f"where {' and '.join(client_filter)}" if client_filter else ""

        queries = {
            "cea_processlog": build_distinct_query(
                TABLES["cea_processlog"],
                where_clause,
                "try_to_timestamp_ntz(IST_TILL_MINUTES) desc",
            ),
            "cea_sessionquery": build_distinct_query(
                TABLES["cea_sessionquery"],
                where_clause,
                "try_to_timestamp_ntz(IST_TILL_MINUTES) desc",
            ),
            "cea_formenginelog": build_distinct_query(
                TABLES["cea_formenginelog"],
                where_clause,
                "try_to_timestamp_ntz(IST_TILL_MINUTES) desc",
            ),
            "cea_cealog": build_distinct_query(
                TABLES["cea_cealog"],
                where_clause,
                "ID desc",
            ),
        }
        return query_tables(self.db, queries, params=params)

    def load_from_xactly(
        self,
        *,
        clientid: str = "",
        clientname: str = "",
        limit_per_table: int = 5000,
    ) -> dict[str, pd.DataFrame]:
        queries = {
            "cea_processlog": build_xactly_select_query(
                TABLES["cea_processlog"],
                clientid=clientid,
                clientname=clientname,
                order_by="IST_TILL_MINUTES desc",
            ),
            "cea_sessionquery": build_xactly_select_query(
                TABLES["cea_sessionquery"],
                clientid=clientid,
                clientname=clientname,
                order_by="IST_TILL_MINUTES desc",
            ),
            "cea_formenginelog": build_xactly_select_query(
                TABLES["cea_formenginelog"],
                clientid=clientid,
                clientname=clientname,
                order_by="IST_TILL_MINUTES desc",
            ),
            "cea_cealog": build_xactly_select_query(
                TABLES["cea_cealog"],
                clientid=clientid,
                clientname=clientname,
                order_by="ID desc",
            ),
        }
        return query_xactly_tables(self.db, queries, max_rows=limit_per_table)

    def load_active_model_context_from_snowflake(
        self,
        *,
        limit_per_table: int = 5000,
    ) -> dict[str, pd.DataFrame]:
        return self.load_selected_model_context_from_xactly(limit_per_table=limit_per_table)
        params: dict[str, Any] = {
            "limit_per_table": int(limit_per_table),
            "workflow_name": ACTIVE_MODEL_WORKFLOW,
        }
        queries = {
            "cea_processlog": build_active_processlog_query(),
            "cea_sessionquery": build_active_sessionquery_query(),
            "cea_formenginelog": build_active_formenginelog_query(),
            "cea_cealog": build_active_cealog_query(),
        }
        return query_tables(self.db, queries, params=params)

    def load_selected_model_context_from_snowflake(
        self,
        *,
        clientid: str = "",
        clientname: str = "",
        limit_per_table: int = 5000,
        assume_active: bool | None = None,
    ) -> dict[str, pd.DataFrame]:
        return self.load_selected_model_context_from_xactly(
            clientid=clientid,
            clientname=clientname,
            limit_per_table=limit_per_table,
            assume_active=assume_active,
        )
        params: dict[str, Any] = {
            "limit_per_table": int(limit_per_table),
            "workflow_name": ACTIVE_MODEL_WORKFLOW,
        }
        client_scope = []
        if clientid.strip():
            client_scope.append("CLIENTID = %(clientid)s")
            params["clientid"] = clientid.strip()
        if clientname.strip():
            client_scope.append("CLIENTNAME = %(clientname)s")
            params["clientname"] = clientname.strip()

        active_clause = ""
        if client_scope:
            active_clause = " and " + " and ".join(client_scope)

        if assume_active is False:
            return {
                "cea_processlog": pd.DataFrame(),
                "cea_sessionquery": pd.DataFrame(),
                "cea_formenginelog": pd.DataFrame(),
                "cea_cealog": pd.DataFrame(),
            }

        queries = {
            "cea_processlog": build_active_processlog_query(active_clause=active_clause),
            "cea_sessionquery": build_active_sessionquery_query(active_clause=active_clause),
            "cea_formenginelog": build_active_formenginelog_query(active_clause=active_clause),
            "cea_cealog": build_active_cealog_query(active_clause=active_clause),
        }
        if assume_active is True:
            return query_tables_sequential(self.db, queries, params=params)

        processlog = self.db.query_df(queries["cea_processlog"], params=params)
        if processlog.empty:
            return {
                "cea_processlog": processlog,
                "cea_sessionquery": pd.DataFrame(),
                "cea_formenginelog": pd.DataFrame(),
                "cea_cealog": pd.DataFrame(),
            }

        queries = {
            "cea_sessionquery": build_active_sessionquery_query(active_clause=active_clause),
            "cea_formenginelog": build_active_formenginelog_query(active_clause=active_clause),
            "cea_cealog": build_active_cealog_query(active_clause=active_clause),
        }
        tables = query_tables(self.db, queries, params=params)
        return {"cea_processlog": processlog, **tables}

    def load_selected_model_context_from_xactly(
        self,
        *,
        clientid: str = "",
        clientname: str = "",
        limit_per_table: int = 5000,
        assume_active: bool | None = None,
        processlog_rows: list[dict[str, Any]] | None = None,
    ) -> dict[str, pd.DataFrame]:
        if assume_active is False:
            return empty_monitoring_tables()

        if processlog_rows:
            processlog = normalize_jdbc_columns(pd.DataFrame(processlog_rows))
        else:
            active_filters = [
                f"WORKFLOWNAME = {sql_literal(ACTIVE_MODEL_WORKFLOW)}",
                "ISRUNNING = 'true'",
            ]
            processlog_query = build_xactly_select_query(
                TABLES["cea_processlog"],
                columns=PROCESSLOG_COLUMNS,
                clientid=clientid,
                clientname=clientname,
                filters=active_filters,
                order_by="IST_TILL_MINUTES desc",
            )
            processlog = normalize_jdbc_columns(
                self.db.query_df(processlog_query, max_rows=limit_per_table)
            )
        if processlog.empty:
            return {"cea_processlog": processlog, **empty_monitoring_tables(exclude=("cea_processlog",))}

        trigger_windows = trigger_minute_windows(processlog)
        client_scope = sql_client_pair_filter(processlog)
        session_filters = [sql_in_filter("IST_TILL_MINUTES", trigger_windows)] if trigger_windows else []
        if client_scope:
            session_filters.append(client_scope)
        form_filters = [client_scope] if client_scope else []
        context_tables = query_xactly_tables(
            self.db,
            {
                "cea_sessionquery": build_xactly_select_query(
                    TABLES["cea_sessionquery"],
                    columns=SESSIONQUERY_COLUMNS,
                    clientid=clientid,
                    clientname=clientname,
                    filters=session_filters,
                    order_by="IST_TILL_MINUTES desc",
                ),
                "cea_formenginelog": build_xactly_select_query(
                    TABLES["cea_formenginelog"],
                    columns=FORMENGINELOG_COLUMNS,
                    clientid=clientid,
                    clientname=clientname,
                    filters=form_filters,
                    order_by="IST_TILL_MINUTES desc",
                ),
            },
            max_rows=limit_per_table,
        )
        sessionquery = context_tables["cea_sessionquery"]
        if sessionquery.empty:
            return {
                "cea_processlog": processlog,
                "cea_sessionquery": sessionquery,
                "cea_formenginelog": pd.DataFrame(),
                "cea_cealog": pd.DataFrame(),
            }

        formenginelog = context_tables["cea_formenginelog"]
        if not has_current_form_metadata(processlog, formenginelog):
            return {
                "cea_processlog": processlog,
                "cea_sessionquery": sessionquery,
                "cea_formenginelog": formenginelog,
                "cea_cealog": pd.DataFrame(),
            }

        tables = {
            "cea_sessionquery": sessionquery,
            "cea_formenginelog": formenginelog,
        }
        cealog_start_time = relevant_cealog_start_time(
            processlog=processlog,
            formenginelog=tables["cea_formenginelog"],
        )
        tables["cea_cealog"] = load_relevant_cealog(
            self.db,
            processlog=processlog,
            formenginelog=tables["cea_formenginelog"],
            clientid=clientid,
            clientname=clientname,
            limit_per_table=limit_per_table,
            fallback_start_time=cealog_start_time,
        )
        return {"cea_processlog": processlog, **tables}

    def load_from_csvs(self, paths: dict[str, str]) -> dict[str, pd.DataFrame]:
        return {name: read_monitoring_csv(path) for name, path in paths.items() if path}

    def load_csv_table(self, table_name: str) -> pd.DataFrame:
        paths = csv_paths_from_env()
        path = paths.get(table_name, "")
        if not path:
            return pd.DataFrame()
        return read_monitoring_csv(path)

    def load_selected_model_context(
        self,
        *,
        clientid: str = "",
        clientname: str = "",
        limit_per_table: int = 5000,
        assume_active: bool | None = None,
        processlog_rows: list[dict[str, Any]] | None = None,
    ) -> dict[str, pd.DataFrame]:
        source = "csv" if use_csv_source() else "xactly"
        LOGGER.info(
            "Loading model context source=%s clientid=%s clientname=%s limit=%s",
            source,
            clientid or "all",
            clientname or "all",
            limit_per_table,
        )
        if refresh_coordination_enabled() and not use_csv_source():
            if self._refresh_coordinator is None:
                self._refresh_coordinator = RefreshBatchCoordinator(db=self.db, logger=LOGGER)
            latest_batch = self._refresh_coordinator.latest_batch()
            refresh_status = str((latest_batch or {}).get("STATUS") or "NO_BATCH").upper()
            if refresh_status != "COMPLETED":
                batch_id = str((latest_batch or {}).get("BATCH_ID") or "")
                error_message = str((latest_batch or {}).get("ERROR_MESSAGE") or "")
                LOGGER.warning(
                    "Monitoring blocked by explicit refresh batch status=%s batch_id=%s error=%s",
                    refresh_status,
                    batch_id or None,
                    error_message or None,
                )
                return {
                    **empty_monitoring_tables(),
                    DATA_REFRESH_MARKER: pd.DataFrame(
                        [
                            {
                                "EMPTY_TABLES": "",
                                "REFRESH_STATUS": refresh_status,
                                "BATCH_ID": batch_id,
                                "ERROR_MESSAGE": error_message,
                            }
                        ]
                    ),
                }
            empty_tables: list[str] = []
        elif env_truthy("MODEL_MONITORING_SNOWFLAKE_SNAPSHOT_ENABLED", default=False) and not use_csv_source():
            snapshot_result = self.snowflake_snapshot_guard().sync()
            if not snapshot_result.ready:
                LOGGER.warning(
                    "Data refresh detected by Snowflake snapshot counts; blocked tables=%s incoming=%s snapshot=%s",
                    ", ".join(snapshot_result.blocked_tables),
                    snapshot_result.incoming_counts,
                    snapshot_result.snapshot_counts,
                )
                return {
                    **empty_monitoring_tables(),
                    DATA_REFRESH_MARKER: pd.DataFrame(
                        [{"EMPTY_TABLES": ",".join(snapshot_result.blocked_tables)}]
                    ),
                }
            empty_tables: list[str] = []
        else:
            empty_tables = self.find_empty_source_tables()
        if empty_tables:
            LOGGER.warning(
                "Data refresh detected; source tables are empty: %s",
                ", ".join(empty_tables),
            )
            return {
                **empty_monitoring_tables(),
                DATA_REFRESH_MARKER: pd.DataFrame(
                    [{"EMPTY_TABLES": ",".join(empty_tables)}]
                ),
            }
        if use_csv_source():
            return self.load_selected_model_context_from_csv(
                clientid=clientid,
                clientname=clientname,
                limit_per_table=limit_per_table,
                assume_active=assume_active,
                processlog_rows=processlog_rows,
            )
        return self.load_selected_model_context_from_xactly(
            clientid=clientid,
            clientname=clientname,
            limit_per_table=limit_per_table,
            assume_active=assume_active,
            processlog_rows=processlog_rows,
        )

    def snowflake_snapshot_guard(self) -> SnowflakeSnapshotGuard:
        if self._snapshot_guard is None:
            self._snapshot_guard = SnowflakeSnapshotGuard(
                xactly=self.db,
                snowflake=SnowflakeClient(),
                tables=SNOWFLAKE_SNAPSHOT_TABLES,
            )
        return self._snapshot_guard

    def find_empty_source_tables(self) -> list[str]:
        """Return physical source tables with no rows, before scoped filtering."""
        if use_csv_source():
            empty_tables: list[str] = []
            for table_name, path in csv_paths_from_env().items():
                if not path or not os.path.exists(path):
                    empty_tables.append(table_name)
                    continue
                try:
                    if pd.read_csv(path, dtype=str, encoding="utf-8-sig", nrows=1).empty:
                        empty_tables.append(table_name)
                except pd.errors.EmptyDataError:
                    empty_tables.append(table_name)
            return empty_tables

        health_tables = query_xactly_tables(
            self.db,
            {
                name: f"select CLIENTID from {table}"
                for name, table in TABLES.items()
            },
            max_rows=1,
        )
        return [
            name
            for name in REQUIRED_MONITORING_TABLES
            if health_tables.get(name, pd.DataFrame()).empty
        ]

    def load_selected_model_context_from_csv(
        self,
        *,
        clientid: str = "",
        clientname: str = "",
        limit_per_table: int = 5000,
        assume_active: bool | None = None,
        processlog_rows: list[dict[str, Any]] | None = None,
    ) -> dict[str, pd.DataFrame]:
        if assume_active is False:
            return empty_monitoring_tables()

        if processlog_rows:
            processlog = normalize_jdbc_columns(pd.DataFrame(processlog_rows))
            processlog = filter_monitoring_csv(
                processlog,
                clientid=clientid,
                clientname=clientname,
                limit=limit_per_table,
                table_name="cea_processlog",
            )
        else:
            processlog = read_monitoring_csv_filtered(
                "cea_processlog",
                clientid=clientid,
                clientname=clientname,
                limit=limit_per_table,
            )
            if "WORKFLOWNAME" in processlog.columns:
                processlog = processlog[processlog["WORKFLOWNAME"].astype(str) == ACTIVE_MODEL_WORKFLOW]
            if assume_active is not True and "ISRUNNING" in processlog.columns:
                processlog = processlog[processlog["ISRUNNING"].map(truthy)]
            processlog = sort_monitoring_table("cea_processlog", processlog).head(limit_per_table).reset_index(drop=True)

        if processlog.empty:
            return {"cea_processlog": processlog, **empty_monitoring_tables(exclude=("cea_processlog",))}

        selected_clientid = clientid.strip()
        selected_clientname = clientname.strip()
        should_infer_single_client = bool(processlog_rows) or bool(selected_clientid) or bool(selected_clientname)
        if should_infer_single_client and not selected_clientid and "CLIENTID" in processlog.columns:
            selected_clientid = str(processlog.iloc[0].get("CLIENTID") or "").strip()
        if should_infer_single_client and not selected_clientname and "CLIENTNAME" in processlog.columns:
            selected_clientname = str(processlog.iloc[0].get("CLIENTNAME") or "").strip()

        trigger_windows = trigger_minute_windows(processlog)
        filtered_tables = {"cea_processlog": processlog}
        filtered_tables["cea_sessionquery"] = read_monitoring_csv_filtered(
            "cea_sessionquery",
            clientid=selected_clientid,
            clientname=selected_clientname,
            minutes=trigger_windows,
            limit=limit_per_table,
        )

        formenginelog = read_monitoring_csv_filtered(
            "cea_formenginelog",
            clientid=selected_clientid,
            clientname=selected_clientname,
            limit=limit_per_table,
        )
        filtered_tables["cea_formenginelog"] = filter_by_relevant_form_rows(
            formenginelog,
            processlog=processlog,
            limit=limit_per_table,
        )

        cealog_minutes = relevant_cealog_minutes(processlog, filtered_tables["cea_formenginelog"])
        filtered_tables["cea_cealog"] = read_monitoring_csv_filtered(
            "cea_cealog",
            clientid=selected_clientid,
            clientname=selected_clientname,
            minutes=cealog_minutes,
            limit=min(limit_per_table, int(os.getenv("MODEL_MONITORING_CEALOG_LIMIT", "1000"))),
        )
        return filtered_tables

    def run(
        self,
        *,
        tables: dict[str, pd.DataFrame],
        clientid: str = "",
        clientname: str = "",
    ) -> ProjectRun:
        refresh_marker = tables.get(DATA_REFRESH_MARKER, pd.DataFrame())
        monitoring_tables = {
            name: table
            for name, table in tables.items()
            if name in REQUIRED_MONITORING_TABLES
        }
        if not refresh_marker.empty:
            empty_tables = str(refresh_marker.iloc[0].get("EMPTY_TABLES") or "").split(",")
            empty_tables = [name for name in empty_tables if name]
            refresh_status = str(
                refresh_marker.iloc[0].get("REFRESH_STATUS") or "IN_PROGRESS"
            ).upper()
            batch_id = str(refresh_marker.iloc[0].get("BATCH_ID") or "")
            error_message = str(refresh_marker.iloc[0].get("ERROR_MESSAGE") or "")
            failed = refresh_status in {"FAILED", "TIMED_OUT"}
            overall_status = "DATA_REFRESH_FAILED" if failed else "DATA_REFRESH_IN_PROGRESS"
            summary = (
                f"Data refresh {refresh_status.lower()}. {error_message}".strip()
                if failed
                else "Data refresh is in progress. Monitoring calculations are paused."
            )
            result = {
                "overall_status": overall_status,
                "client_results": [],
                "summary": summary,
                "empty_tables": empty_tables,
                "refresh_status": refresh_status,
                "batch_id": batch_id or None,
                "error_message": error_message or None,
            }
            payload = {
                "data_health": {
                    "status": overall_status,
                    "empty_tables": empty_tables,
                    "refresh_status": refresh_status,
                    "batch_id": batch_id or None,
                    "error_message": error_message or None,
                }
            }
            return ProjectRun(result=result, payload=payload, tables=monitoring_tables)

        selected_client = {
            "CLIENTID": clientid.strip() or None,
            "CLIENTNAME": clientname.strip() or None,
        }
        agent_run = self.agent.run(monitoring_tables, selected_client=selected_client)
        LOGGER.info(
            "Model monitoring run completed clientid=%s clientname=%s tables=%s",
            clientid or "all",
            clientname or "all",
            {name: len(table.index) for name, table in monitoring_tables.items()},
        )
        return ProjectRun(result=agent_run.result, payload=agent_run.payload, tables=monitoring_tables)


def normalize_jdbc_columns(df: pd.DataFrame) -> pd.DataFrame:
    normalized = df.copy()
    normalized.columns = [str(column).upper() for column in normalized.columns]
    return normalized


def truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def sql_in_filter(column: str, values: list[str]) -> str:
    unique_values = sorted({value for value in values if value})
    if not unique_values:
        return "1 = 0"
    return f"{column} in ({', '.join(sql_literal(value) for value in unique_values)})"


def sql_client_pair_filter(processlog: pd.DataFrame) -> str:
    """Restrict related tables to clients with active model runs."""
    if not {"CLIENTID", "CLIENTNAME"}.issubset(processlog.columns):
        return ""

    pairs = {
        (str(row.CLIENTID).strip(), str(row.CLIENTNAME).strip())
        for row in processlog[["CLIENTID", "CLIENTNAME"]].itertuples(index=False)
        if pd.notna(row.CLIENTID)
        and pd.notna(row.CLIENTNAME)
        and str(row.CLIENTID).strip()
        and str(row.CLIENTNAME).strip()
    }
    if not pairs:
        return ""
    clientids = [clientid for clientid, _ in pairs]
    clientnames = [clientname for _, clientname in pairs]
    return (
        f"({sql_in_filter('CLIENTID', clientids)} and "
        f"{sql_in_filter('CLIENTNAME', clientnames)})"
    )


def trigger_minute_windows(processlog: pd.DataFrame) -> list[str]:
    values: list[str] = []
    if "IST_TILL_MINUTES" not in processlog.columns:
        return values

    for raw_value in processlog["IST_TILL_MINUTES"].dropna().tolist():
        values.extend(minute_and_next(raw_value))
    return values


def minute_and_next(raw_value: Any) -> list[str]:
    timestamp = pd.to_datetime(raw_value, errors="coerce")
    if pd.isna(timestamp):
        return [str(raw_value)]
    minute = timestamp.floor("min")
    return [
        minute.strftime("%Y-%m-%d %H:%M"),
        (minute + pd.Timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M"),
    ]


def relevant_cealog_minutes(processlog: pd.DataFrame, formenginelog: pd.DataFrame) -> list[str]:
    minutes = trigger_minute_windows(processlog)
    if processlog.empty or formenginelog.empty:
        return minutes

    form_metadata = extract_form_metadata(formenginelog)
    for _, trigger in processlog.iterrows():
        clientid = trigger.get("CLIENTID")
        clientname = trigger.get("CLIENTNAME")
        trigger_time = pd.to_datetime(trigger.get("IST_TILL_MINUTES"), errors="coerce")
        if pd.isna(trigger_time):
            continue

        trigger_label = trigger_time.floor("min").strftime("%Y-%m-%d %H:%M")
        current_metadata = metadata_for(form_metadata, clientid, clientname, trigger_label)
        if current_metadata is None:
            continue

        for historical_time in previous_same_type_times(
            form_metadata=form_metadata,
            current_metadata=current_metadata,
            trigger_time=current_metadata.get("IST_TILL_MINUTES") or trigger_label,
            limit=4,
        ):
            minutes.extend(minute_and_next(historical_time))
    return minutes


def relevant_cealog_start_time(processlog: pd.DataFrame, formenginelog: pd.DataFrame) -> str:
    minutes = relevant_cealog_minutes(processlog, formenginelog)
    return min(minutes) if minutes else ""


def load_relevant_cealog(
    db: XactlyJdbcClient,
    *,
    processlog: pd.DataFrame,
    formenginelog: pd.DataFrame,
    clientid: str = "",
    clientname: str = "",
    limit_per_table: int = 5000,
    fallback_start_time: str = "",
) -> pd.DataFrame:
    targets = relevant_cealog_run_targets(processlog, formenginelog)
    if not targets:
        return load_cealog_fallback(
            db,
            clientid=clientid,
            clientname=clientname,
            limit_per_table=limit_per_table,
            start_time=fallback_start_time,
        )

    start_candidates = load_cealog_start_candidates(
        db,
        targets=targets,
        clientid=clientid,
        clientname=clientname,
    )
    start_records = start_candidates.where(pd.notna(start_candidates), None).to_dict(orient="records")
    run_starts = choose_run_starts(targets, start_records)
    if not run_starts:
        return load_cealog_fallback(
            db,
            clientid=clientid,
            clientname=clientname,
            limit_per_table=limit_per_table,
            start_time=fallback_start_time,
        )

    rows_per_run = min(
        int(limit_per_table),
        int(os.getenv("MODEL_MONITORING_CEALOG_ROWS_PER_RUN", "1000")),
    )
    frames = load_cealog_run_ranges(
        db,
        run_starts=run_starts,
        rows_per_run=rows_per_run,
    )

    if not frames:
        return load_cealog_fallback(
            db,
            clientid=clientid,
            clientname=clientname,
            limit_per_table=limit_per_table,
            start_time=fallback_start_time,
        )

    combined = pd.concat(frames, ignore_index=True)
    if "ID" in combined.columns:
        combined = combined.drop_duplicates(subset=["CLIENTID", "CLIENTNAME", "ID"])
        combined["_MONITOR_ID_SORT"] = pd.to_numeric(combined["ID"], errors="coerce")
        combined = combined.sort_values("_MONITOR_ID_SORT", ascending=False).drop(columns=["_MONITOR_ID_SORT"])
    return combined.head(limit_per_table).reset_index(drop=True)


def load_cealog_fallback(
    db: XactlyJdbcClient,
    *,
    clientid: str,
    clientname: str,
    limit_per_table: int,
    start_time: str,
) -> pd.DataFrame:
    cealog_filters = [f"IST_TILL_MINUTES >= {sql_literal(start_time)}"] if start_time else []
    return normalize_jdbc_columns(
        db.query_df(
            build_xactly_select_query(
                TABLES["cea_cealog"],
                columns=CEALOG_COLUMNS,
                clientid=clientid,
                clientname=clientname,
                filters=cealog_filters,
                order_by="ID desc",
            ),
            max_rows=min(limit_per_table, int(os.getenv("MODEL_MONITORING_CEALOG_LIMIT", "1000"))),
        )
    )


def relevant_cealog_run_targets(processlog: pd.DataFrame, formenginelog: pd.DataFrame) -> list[dict[str, Any]]:
    targets: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, bool]] = set()
    if processlog.empty:
        return targets

    form_metadata = extract_form_metadata(formenginelog)
    for _, trigger in processlog.iterrows():
        clientid = trigger.get("CLIENTID")
        clientname = trigger.get("CLIENTNAME")
        trigger_time = pd.to_datetime(trigger.get("IST_TILL_MINUTES"), errors="coerce")
        if pd.isna(trigger_time):
            continue

        trigger_label = trigger_time.floor("min").strftime("%Y-%m-%d %H:%M")
        add_cealog_run_target(
            targets,
            seen,
            clientid=clientid,
            clientname=clientname,
            trigger_time=trigger_label,
            match_trigger_window=True,
        )

        current_metadata = metadata_for(form_metadata, clientid, clientname, trigger_label)
        if current_metadata is None:
            continue

        for historical_time in previous_same_type_times(
            form_metadata=form_metadata,
            current_metadata=current_metadata,
            trigger_time=current_metadata.get("IST_TILL_MINUTES") or trigger_label,
            limit=4,
        ):
            add_cealog_run_target(
                targets,
                seen,
                clientid=clientid,
                clientname=clientname,
                trigger_time=historical_time,
                match_trigger_window=False,
            )
    return targets


def add_cealog_run_target(
    targets: list[dict[str, Any]],
    seen: set[tuple[str, str, str, bool]],
    *,
    clientid: Any,
    clientname: Any,
    trigger_time: str,
    match_trigger_window: bool,
) -> None:
    key = (str(clientid), str(clientname), str(trigger_time), match_trigger_window)
    if key in seen:
        return
    seen.add(key)
    targets.append(
        {
            "CLIENTID": clientid,
            "CLIENTNAME": clientname,
            "trigger_time": str(trigger_time),
            "match_trigger_window": match_trigger_window,
        }
    )


def load_cealog_start_candidates(
    db: XactlyJdbcClient,
    *,
    targets: list[dict[str, Any]],
    clientid: str,
    clientname: str,
) -> pd.DataFrame:
    windows: list[str] = []
    for target in targets:
        if target["match_trigger_window"]:
            windows.extend(minute_and_next(target["trigger_time"]))
        else:
            windows.append(target["trigger_time"])

    return normalize_jdbc_columns(
        db.query_df(
            build_xactly_select_query(
                TABLES["cea_cealog"],
                columns=CEALOG_COLUMNS,
                clientid=clientid,
                clientname=clientname,
                filters=[sql_in_filter("IST_TILL_MINUTES", windows)],
                order_by="ID asc",
            ),
            max_rows=int(os.getenv("MODEL_MONITORING_CEALOG_START_SCAN_LIMIT", "1000")),
        )
    )


def choose_run_starts(
    targets: list[dict[str, Any]],
    start_records: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    run_starts: list[dict[str, Any]] = []
    seen_ids: set[int] = set()
    for target in targets:
        matches = [
            row
            for row in start_records
            if is_same_client_row(row, target.get("CLIENTID"), target.get("CLIENTNAME"))
            and str(row.get("DESCRIPTION", "")).strip() == "-START-"
            and cealog_target_time_matches(row, target)
        ]
        if not matches:
            continue

        start = min(matches, key=lambda row: numeric_id(row.get("ID")) or float("inf"))
        start_id = numeric_id(start.get("ID"))
        if start_id is None or start_id in seen_ids:
            continue
        seen_ids.add(start_id)
        run_starts.append(
            {
                **start,
                "_MONITOR_CURRENT": bool(target.get("match_trigger_window")),
            }
        )
    return run_starts


def cealog_target_time_matches(row: dict[str, Any], target: dict[str, Any]) -> bool:
    if target.get("match_trigger_window"):
        return minute_matches_trigger_window(row.get("IST_TILL_MINUTES"), target.get("trigger_time"))
    row_time = pd.to_datetime(row.get("IST_TILL_MINUTES"), errors="coerce")
    target_time = pd.to_datetime(target.get("trigger_time"), errors="coerce")
    if pd.isna(row_time) or pd.isna(target_time):
        return str(row.get("IST_TILL_MINUTES")) == str(target.get("trigger_time"))
    return row_time.floor("min") == target_time.floor("min")


def load_cealog_run_ranges(
    db: XactlyJdbcClient,
    *,
    run_starts: list[dict[str, Any]],
    rows_per_run: int,
) -> list[pd.DataFrame]:
    client_groups = group_run_starts_by_client(run_starts)
    with ThreadPoolExecutor(max_workers=min(len(client_groups), 3)) as executor:
        futures = [
            executor.submit(
                load_cealog_ranges_for_client,
                db,
                clientid=clientid,
                clientname=clientname,
                starts=starts,
                rows_per_run=rows_per_run,
            )
            for (clientid, clientname), starts in client_groups.items()
        ]
        frames = [future.result() for future in futures]
        return [frame for frame in frames if not frame.empty]


def load_cealog_ranges_for_client(
    db: XactlyJdbcClient,
    *,
    clientid: str,
    clientname: str,
    starts: list[dict[str, Any]],
    rows_per_run: int,
) -> pd.DataFrame:
    ranges = cealog_run_ranges_for_client(
        db,
        clientid=clientid,
        clientname=clientname,
        starts=starts,
    )
    if not ranges:
        return pd.DataFrame()
    range_by_start = {start_id: (start_id, end_id) for start_id, end_id in ranges}
    current_ranges = [
        range_by_start[start_id]
        for start in starts
        if start.get("_MONITOR_CURRENT")
        and (start_id := numeric_id(start.get("ID"))) in range_by_start
    ]
    historical_ranges = [
        range_by_start[start_id]
        for start in starts
        if not start.get("_MONITOR_CURRENT")
        and (start_id := numeric_id(start.get("ID"))) in range_by_start
    ]
    historical_rows_per_run = max(
        4,
        int(os.getenv("MODEL_MONITORING_HISTORICAL_BOUNDARY_ROWS_PER_RUN", "100")),
    )
    return normalize_jdbc_columns(
        db.query_df(
            build_xactly_select_query(
                TABLES["cea_cealog"],
                columns=CEALOG_COLUMNS,
                clientid=clientid,
                clientname=clientname,
                filters=[
                    sql_targeted_range_filter(
                        "ID",
                        current_ranges=current_ranges,
                        historical_ranges=historical_ranges,
                    )
                ],
                order_by="ID asc",
            ),
            max_rows=(
                rows_per_run * len(current_ranges)
                + historical_rows_per_run * len(historical_ranges)
            ),
        )
    )


def group_run_starts_by_client(run_starts: list[dict[str, Any]]) -> dict[tuple[str, str], list[dict[str, Any]]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for run_start in run_starts:
        client_key = (str(run_start.get("CLIENTID") or ""), str(run_start.get("CLIENTNAME") or ""))
        grouped.setdefault(client_key, []).append(run_start)
    return grouped


def cealog_run_ranges_for_client(
    db: XactlyJdbcClient,
    *,
    clientid: str,
    clientname: str,
    starts: list[dict[str, Any]],
) -> list[tuple[int, int]]:
    start_ids = sorted(
        start_id
        for start in starts
        if (start_id := numeric_id(start.get("ID"))) is not None
    )
    if not start_ids:
        return []

    min_start_id = min(start_ids)
    with ThreadPoolExecutor(max_workers=2) as executor:
        end_candidates_future = executor.submit(
            load_cealog_end_candidates,
            db,
            clientid=clientid,
            clientname=clientname,
            min_start_id=min_start_id,
        )
        latest_id_future = executor.submit(
            latest_cealog_id,
            db,
            clientid=clientid,
            clientname=clientname,
            min_start_id=min_start_id,
        )
        end_candidates = end_candidates_future.result()
        latest_id = latest_id_future.result()

    ranges: list[tuple[int, int]] = []
    for start_id in start_ids:
        end_id = next((candidate for candidate in end_candidates if candidate > start_id), None)
        if end_id is None:
            end_id = latest_id or start_id
        ranges.append((start_id, end_id))
    return ranges


def load_cealog_end_candidates(
    db: XactlyJdbcClient,
    *,
    clientid: str,
    clientname: str,
    min_start_id: int,
) -> list[int]:
    end_rows = normalize_jdbc_columns(
        db.query_df(
            build_xactly_select_query(
                TABLES["cea_cealog"],
                columns=["ID"],
                clientid=clientid,
                clientname=clientname,
                filters=[
                    f"ID > {min_start_id:g}",
                    "DESCRIPTION like '%-END-%'",
                ],
                order_by="ID asc",
            ),
            max_rows=int(os.getenv("MODEL_MONITORING_CEALOG_END_SCAN_LIMIT", "1000")),
        )
    )
    if end_rows.empty:
        return []
    return sorted(
        row_id
        for row_id in (numeric_id(value) for value in end_rows["ID"].tolist())
        if row_id is not None
    )


def latest_cealog_id(
    db: XactlyJdbcClient,
    *,
    clientid: str,
    clientname: str,
    min_start_id: int,
) -> int | None:
    latest_rows = normalize_jdbc_columns(
        db.query_df(
            build_xactly_select_query(
                TABLES["cea_cealog"],
                columns=["ID"],
                clientid=clientid,
                clientname=clientname,
                filters=[f"ID >= {min_start_id:g}"],
                order_by="ID desc",
            ),
            max_rows=1,
        )
    )
    if latest_rows.empty:
        return None
    return numeric_id(latest_rows.iloc[0].get("ID"))


def sql_range_filter(column: str, ranges: list[tuple[int, int]]) -> str:
    if not ranges:
        return "1 = 0"
    return "(" + " or ".join(
        f"({column} >= {start_id:g} and {column} <= {end_id:g})"
        for start_id, end_id in ranges
    ) + ")"


def sql_targeted_range_filter(
    column: str,
    *,
    current_ranges: list[tuple[int, int]],
    historical_ranges: list[tuple[int, int]],
) -> str:
    """Load full current runs but only timing boundaries from historical runs."""
    clauses: list[str] = []
    if current_ranges:
        clauses.append(sql_range_filter(column, current_ranges))
    if historical_ranges:
        historical_boundaries = (
            "(DESCRIPTION in ('-START-', '-END-') "
            "or DESCRIPTION like '% before %' "
            "or DESCRIPTION like '% after %')"
        )
        clauses.append(
            f"({sql_range_filter(column, historical_ranges)} and {historical_boundaries})"
        )
    return "(" + " or ".join(clauses) + ")" if clauses else "1 = 0"


def is_same_client_row(row: dict[str, Any], clientid: Any, clientname: Any) -> bool:
    return str(row.get("CLIENTID")) == str(clientid) and str(row.get("CLIENTNAME")) == str(clientname)


def numeric_id(value: Any) -> int | None:
    parsed = pd.to_numeric(value, errors="coerce")
    if pd.isna(parsed):
        return None
    return int(parsed)


def has_current_form_metadata(processlog: pd.DataFrame, formenginelog: pd.DataFrame) -> bool:
    if processlog.empty or formenginelog.empty:
        return False

    form_metadata = extract_form_metadata(formenginelog)
    for _, trigger in processlog.iterrows():
        trigger_time = pd.to_datetime(trigger.get("IST_TILL_MINUTES"), errors="coerce")
        if pd.isna(trigger_time):
            continue
        trigger_label = trigger_time.floor("min").strftime("%Y-%m-%d %H:%M")
        if metadata_for(
            form_metadata,
            trigger.get("CLIENTID"),
            trigger.get("CLIENTNAME"),
            trigger_label,
        ):
            return True
    return False


def build_xactly_select_query(
    table: str,
    *,
    columns: list[str] | None = None,
    clientid: str = "",
    clientname: str = "",
    filters: list[str] | None = None,
    order_by: str = "",
) -> str:
    where_parts = list(filters or [])
    if clientid.strip():
        where_parts.append(f"CLIENTID = {sql_literal(clientid.strip())}")
    if clientname.strip():
        where_parts.append(f"CLIENTNAME = {sql_literal(clientname.strip())}")

    query = f"select {', '.join(columns or ['*'])} from {table}"
    if where_parts:
        query += " where " + " and ".join(where_parts)
    if order_by:
        query += f" order by {order_by}"
    return query


def query_xactly_tables(
    db: XactlyJdbcClient,
    queries: dict[str, str],
    *,
    max_rows: int,
) -> dict[str, pd.DataFrame]:
    with ThreadPoolExecutor(max_workers=min(len(queries), 4)) as executor:
        futures = {
            name: executor.submit(db.query_df, sql, max_rows=max_rows)
            for name, sql in queries.items()
        }
        results: dict[str, pd.DataFrame] = {}
        for name, future in futures.items():
            try:
                results[name] = normalize_jdbc_columns(future.result())
            except Exception as exc:
                raise TableQueryError(name, exc) from exc
        return results


def empty_monitoring_tables(exclude: tuple[str, ...] = ()) -> dict[str, pd.DataFrame]:
    return {
        name: pd.DataFrame()
        for name in ("cea_processlog", "cea_sessionquery", "cea_formenginelog", "cea_cealog")
        if name not in exclude
    }


def build_distinct_query(table: str, where_clause: str, order_expression: str) -> str:
    return f"""
        with filtered_rows as (
            select *
            from {table}
            {where_clause}
        ),
        deduped_rows as (
            select distinct *
            from filtered_rows
        )
        select *
        from deduped_rows
        order by CLIENTID, CLIENTNAME, {order_expression}
        limit %(limit_per_table)s
    """


def query_tables(
    db: Any,
    queries: dict[str, str],
    *,
    params: dict[str, Any],
) -> dict[str, pd.DataFrame]:
    with ThreadPoolExecutor(max_workers=min(len(queries), 4)) as executor:
        futures = {
            name: executor.submit(db.query_df, sql, params=params)
            for name, sql in queries.items()
        }
        results: dict[str, pd.DataFrame] = {}
        for name, future in futures.items():
            try:
                results[name] = future.result()
            except Exception as exc:
                raise TableQueryError(name, exc) from exc
        return results


def query_tables_sequential(
    db: Any,
    queries: dict[str, str],
    *,
    params: dict[str, Any],
) -> dict[str, pd.DataFrame]:
    results: dict[str, pd.DataFrame] = {}
    for name, sql in queries.items():
        try:
            results[name] = db.query_df(sql, params=params)
        except Exception as exc:
            raise TableQueryError(name, exc) from exc
    return results


def active_clients_cte(active_clause: str = "") -> str:
    return f"""
        active_clients as (
            select distinct CLIENTID, CLIENTNAME
            from {TABLES["cea_processlog"]}
            where WORKFLOWNAME = %(workflow_name)s
              and try_to_boolean(ISRUNNING::varchar) = true
              {active_clause}
        )
    """


def active_triggers_cte(active_clause: str = "") -> str:
    return f"""
        active_triggers as (
            select distinct CLIENTID, CLIENTNAME, IST_TILL_MINUTES
            from {TABLES["cea_processlog"]}
            where WORKFLOWNAME = %(workflow_name)s
              and try_to_boolean(ISRUNNING::varchar) = true
              {active_clause}
        )
    """


def active_trigger_minute_match(source_alias: str, trigger_alias: str = "active_triggers") -> str:
    return f"""
                  and date_trunc('minute', try_to_timestamp_ntz({source_alias}.IST_TILL_MINUTES))
                      between date_trunc('minute', try_to_timestamp_ntz({trigger_alias}.IST_TILL_MINUTES))
                      and dateadd(
                          minute,
                          1,
                          date_trunc('minute', try_to_timestamp_ntz({trigger_alias}.IST_TILL_MINUTES))
                      )
    """


def build_active_processlog_query(active_clause: str = "") -> str:
    return f"""
        with active_rows as (
            select *
            from {TABLES["cea_processlog"]}
            where WORKFLOWNAME = %(workflow_name)s
              and try_to_boolean(ISRUNNING::varchar) = true
              {active_clause}
            qualify row_number() over (
                partition by CLIENTID, CLIENTNAME, PROCESS_ID, IST_TILL_MINUTES
                order by ORIGINAL_UTC desc
            ) = 1
        )
        select *
        from active_rows
        order by CLIENTID, CLIENTNAME, try_to_timestamp_ntz(IST_TILL_MINUTES) desc
        limit %(limit_per_table)s
    """


def build_active_sessionquery_query(active_clause: str = "") -> str:
    return f"""
        with {active_triggers_cte(active_clause=active_clause)},
        matching_rows as (
            select source_rows.*
            from {TABLES["cea_sessionquery"]} source_rows
            where exists (
                select 1
                from active_triggers
                where active_triggers.CLIENTID = source_rows.CLIENTID
                  and active_triggers.CLIENTNAME = source_rows.CLIENTNAME
{active_trigger_minute_match("source_rows")}
            )
            qualify row_number() over (
                partition by CLIENTID, CLIENTNAME, SESSION_ID, START_TIME, IST_TILL_MINUTES
                order by try_to_number(DURATIONINSECONDS) desc
            ) = 1
        )
        select *
        from matching_rows
        order by CLIENTID, CLIENTNAME, try_to_timestamp_ntz(IST_TILL_MINUTES) desc
        limit %(limit_per_table)s
    """


def current_form_metadata_cte(active_clause: str = "") -> str:
    return f"""
        current_forms as (
            select source_rows.*
            from {TABLES["cea_formenginelog"]} source_rows
            where exists (
                select 1
                from active_triggers
                where active_triggers.CLIENTID = source_rows.CLIENTID
                  and active_triggers.CLIENTNAME = source_rows.CLIENTNAME
{active_trigger_minute_match("source_rows")}
            )
        ),
        current_metadata as (
            select distinct
                CLIENTID,
                CLIENTNAME,
                IST_TILL_MINUTES as TRIGGER_TIME,
                try_parse_json(FULL_RESULT):Model.Key::string as MODEL_KEY,
                try_parse_json(FULL_RESULT):CalculationType.Key::string as CALCULATION_TYPE_KEY,
                try_parse_json(FULL_RESULT):RefreshReportingLayer.Key::string as REFRESH_REPORTING_LAYER_KEY
            from current_forms
            where try_parse_json(FULL_RESULT) is not null
        )
    """


def build_active_formenginelog_query(active_clause: str = "") -> str:
    return f"""
        with {active_triggers_cte(active_clause=active_clause)},
        {current_form_metadata_cte(active_clause=active_clause)},
        current_rows as (
            select source_rows.*, 1 as _MONITOR_PRIORITY
            from current_forms source_rows
        ),
        historical_rows as (
            select source_rows.*, 0 as _MONITOR_PRIORITY
            from {TABLES["cea_formenginelog"]} source_rows
            join current_metadata
              on current_metadata.CLIENTID = source_rows.CLIENTID
             and current_metadata.CLIENTNAME = source_rows.CLIENTNAME
             and current_metadata.MODEL_KEY =
                 try_parse_json(source_rows.FULL_RESULT):Model.Key::string
             and current_metadata.CALCULATION_TYPE_KEY =
                 try_parse_json(source_rows.FULL_RESULT):CalculationType.Key::string
             and current_metadata.REFRESH_REPORTING_LAYER_KEY =
                 try_parse_json(source_rows.FULL_RESULT):RefreshReportingLayer.Key::string
             and try_to_timestamp_ntz(source_rows.IST_TILL_MINUTES)
                 < try_to_timestamp_ntz(current_metadata.TRIGGER_TIME)
            qualify row_number() over (
                partition by current_metadata.CLIENTID,
                             current_metadata.CLIENTNAME,
                             current_metadata.TRIGGER_TIME,
                             current_metadata.MODEL_KEY,
                             current_metadata.CALCULATION_TYPE_KEY,
                             current_metadata.REFRESH_REPORTING_LAYER_KEY
                order by try_to_timestamp_ntz(source_rows.IST_TILL_MINUTES) desc
            ) <= 4
        ),
        deduped_rows as (
            select *
            from (
                select *
                from current_rows
                union all
                select *
                from historical_rows
            )
            qualify row_number() over (
                partition by CLIENTID, CLIENTNAME, CREATED_DATE, IST_TILL_MINUTES, FILEPATH
                order by _MONITOR_PRIORITY desc
            ) = 1
        )
        select * exclude _MONITOR_PRIORITY
        from deduped_rows
        order by CLIENTID, CLIENTNAME, try_to_timestamp_ntz(IST_TILL_MINUTES) desc
        limit %(limit_per_table)s
    """


def build_active_client_context_query(
    table: str,
    order_expression: str,
    active_clause: str = "",
) -> str:
    return f"""
        with {active_clients_cte(active_clause=active_clause)},
        filtered_rows as (
            select source_rows.*
            from {table} source_rows
            where exists (
                select 1
                from active_clients
                where active_clients.CLIENTID = source_rows.CLIENTID
                  and active_clients.CLIENTNAME = source_rows.CLIENTNAME
            )
        ),
        deduped_rows as (
            select distinct *
            from filtered_rows
        )
        select *
        from deduped_rows
        order by CLIENTID, CLIENTNAME, {order_expression}
        limit %(limit_per_table)s
    """


def build_active_cealog_query(active_clause: str = "") -> str:
    return f"""
        with {active_triggers_cte(active_clause=active_clause)},
        {current_form_metadata_cte(active_clause=active_clause)},
        historical_form_times as (
            select
                source_rows.CLIENTID,
                source_rows.CLIENTNAME,
                source_rows.IST_TILL_MINUTES
            from {TABLES["cea_formenginelog"]} source_rows
            join current_metadata
              on current_metadata.CLIENTID = source_rows.CLIENTID
             and current_metadata.CLIENTNAME = source_rows.CLIENTNAME
             and current_metadata.MODEL_KEY =
                 try_parse_json(source_rows.FULL_RESULT):Model.Key::string
             and current_metadata.CALCULATION_TYPE_KEY =
                 try_parse_json(source_rows.FULL_RESULT):CalculationType.Key::string
             and current_metadata.REFRESH_REPORTING_LAYER_KEY =
                 try_parse_json(source_rows.FULL_RESULT):RefreshReportingLayer.Key::string
             and try_to_timestamp_ntz(source_rows.IST_TILL_MINUTES)
                 < try_to_timestamp_ntz(current_metadata.TRIGGER_TIME)
            qualify row_number() over (
                partition by current_metadata.CLIENTID,
                             current_metadata.CLIENTNAME,
                             current_metadata.TRIGGER_TIME,
                             current_metadata.MODEL_KEY,
                             current_metadata.CALCULATION_TYPE_KEY,
                             current_metadata.REFRESH_REPORTING_LAYER_KEY
                order by try_to_timestamp_ntz(source_rows.IST_TILL_MINUTES) desc
            ) <= 4
        ),
        relevant_run_times as (
            select
                CLIENTID,
                CLIENTNAME,
                IST_TILL_MINUTES as MATCH_FROM_TIME,
                dateadd(
                    minute,
                    1,
                    date_trunc('minute', try_to_timestamp_ntz(IST_TILL_MINUTES))
                ) as MATCH_TO_TIME
            from active_triggers
            union
            select
                CLIENTID,
                CLIENTNAME,
                IST_TILL_MINUTES as MATCH_FROM_TIME,
                date_trunc('minute', try_to_timestamp_ntz(IST_TILL_MINUTES)) as MATCH_TO_TIME
            from historical_form_times
        ),
        start_rows as (
            select
                source_rows.CLIENTID,
                source_rows.CLIENTNAME,
                source_rows.IST_TILL_MINUTES,
                min(source_rows.ID) as START_ID
            from {TABLES["cea_cealog"]} source_rows
            where exists (
                select 1
                from relevant_run_times
                where relevant_run_times.CLIENTID = source_rows.CLIENTID
                  and relevant_run_times.CLIENTNAME = source_rows.CLIENTNAME
                  and date_trunc('minute', try_to_timestamp_ntz(source_rows.IST_TILL_MINUTES))
                      between date_trunc('minute', try_to_timestamp_ntz(relevant_run_times.MATCH_FROM_TIME))
                      and relevant_run_times.MATCH_TO_TIME
            )
              and trim(source_rows.DESCRIPTION) = '-START-'
            group by source_rows.CLIENTID, source_rows.CLIENTNAME, source_rows.IST_TILL_MINUTES
        ),
        run_bounds as (
            select
                start_rows.*,
                coalesce(
                    (
                        select min(end_rows.ID)
                        from {TABLES["cea_cealog"]} end_rows
                        where end_rows.CLIENTID = start_rows.CLIENTID
                          and end_rows.CLIENTNAME = start_rows.CLIENTNAME
                          and end_rows.ID > start_rows.START_ID
                          and trim(end_rows.DESCRIPTION) = '-END-'
                    ),
                    (
                        select max(latest_rows.ID)
                        from {TABLES["cea_cealog"]} latest_rows
                        where latest_rows.CLIENTID = start_rows.CLIENTID
                          and latest_rows.CLIENTNAME = start_rows.CLIENTNAME
                    )
                ) as END_ID
            from start_rows
        ),
        exact_trigger_rows as (
            select source_rows.*, 1 as _MONITOR_PRIORITY
            from {TABLES["cea_cealog"]} source_rows
            where exists (
                select 1
                from active_triggers
                where active_triggers.CLIENTID = source_rows.CLIENTID
                  and active_triggers.CLIENTNAME = source_rows.CLIENTNAME
{active_trigger_minute_match("source_rows")}
            )
        ),
        bounded_run_rows as (
            select source_rows.*, 0 as _MONITOR_PRIORITY
            from {TABLES["cea_cealog"]} source_rows
            where exists (
                select 1
                from run_bounds
                where run_bounds.CLIENTID = source_rows.CLIENTID
                  and run_bounds.CLIENTNAME = source_rows.CLIENTNAME
                  and source_rows.ID between run_bounds.START_ID and run_bounds.END_ID
            )
        ),
        deduped_rows as (
            select *
            from (
                select *
                from exact_trigger_rows
                union all
                select *
                from bounded_run_rows
            )
            qualify row_number() over (
                partition by CLIENTID, CLIENTNAME, ID
                order by _MONITOR_PRIORITY desc
            ) = 1
        ),
        limited_rows as (
            select *
            from deduped_rows
            order by _MONITOR_PRIORITY desc, CLIENTID, CLIENTNAME, ID desc
            limit %(limit_per_table)s
        )
        select * exclude _MONITOR_PRIORITY
        from limited_rows
        order by CLIENTID, CLIENTNAME, ID desc
    """
