from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any

import pandas as pd

from core.llm import LLMClient
from projects.model_monitoring.prompts import MODEL_MONITORING_SYSTEM_PROMPT


@dataclass(frozen=True)
class AgentRun:
    result: dict[str, Any]
    payload: dict[str, Any]


class ModelMonitoringAgent:
    def __init__(self, llm: LLMClient | None = None) -> None:
        self.llm = llm or LLMClient()

    def run(self, tables: dict[str, pd.DataFrame], selected_client: dict[str, Any] | None = None) -> AgentRun:
        payload = build_agent_payload(tables, selected_client=selected_client)
        deterministic_result = build_deterministic_result(payload)
        if deterministic_result is not None:
            return AgentRun(result=deterministic_result, payload=payload)

        result = self.llm.analyze_json(
            system_prompt=MODEL_MONITORING_SYSTEM_PROMPT,
            user_payload=payload,
        )
        return AgentRun(result=result, payload=payload)


def build_agent_payload(
    tables: dict[str, pd.DataFrame],
    selected_client: dict[str, Any] | None = None,
) -> dict[str, Any]:
    prepared = {name: prepare_table(name, df) for name, df in tables.items()}
    metadata_by_time = extract_form_metadata(prepared.get("cea_formenginelog", pd.DataFrame()))

    return {
        "selected_client": selected_client,
        "table_rules": {
            "client_scope_columns": ["CLIENTID", "CLIENTNAME"],
            "datetime_column": "IST_TILL_MINUTES",
            "current_run_match_window": "same minute as processlog trigger or one minute later",
            "cealog_order_column": "ID desc",
            "process_trigger": {
                "WORKFLOWNAME": "SendEmailAfterModelExecution",
                "ISRUNNING": True,
            },
            "long_run_threshold_minutes_over_average": 20,
            "historical_same_type_run_count": 4,
        },
        "tables": {
            name: dataframe_records(df)
            for name, df in prepared.items()
        },
        "form_metadata_by_client_time": metadata_by_time,
    }


def prepare_table(name: str, df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    prepared = normalize_columns(df)
    if "IST_TILL_MINUTES" in prepared.columns:
        prepared["IST_TILL_MINUTES"] = pd.to_datetime(
            prepared["IST_TILL_MINUTES"],
            errors="coerce",
        ).dt.strftime("%Y-%m-%d %H:%M")

    if name == "cea_cealog" and "ID" in prepared.columns:
        return prepared.sort_values(["CLIENTID", "CLIENTNAME", "ID"], ascending=[True, True, False])

    if {"CLIENTID", "CLIENTNAME", "IST_TILL_MINUTES"}.issubset(prepared.columns):
        return prepared.sort_values(
            ["CLIENTID", "CLIENTNAME", "IST_TILL_MINUTES"],
            ascending=[True, True, False],
            na_position="last",
        )
    return prepared


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    normalized = df.copy()
    normalized.columns = [str(col).upper() for col in normalized.columns]
    return normalized


def dataframe_records(df: pd.DataFrame, max_rows: int = 4000) -> list[dict[str, Any]]:
    if df.empty:
        return []
    trimmed = df.head(max_rows).where(pd.notna(df.head(max_rows)), None)
    return trimmed.to_dict(orient="records")


def extract_form_metadata(form_df: pd.DataFrame) -> list[dict[str, Any]]:
    if form_df.empty or "FULL_RESULT" not in form_df.columns:
        return []

    rows: list[dict[str, Any]] = []
    for _, row in form_df.iterrows():
        raw = row.get("FULL_RESULT")
        if not raw:
            continue
        try:
            parsed = json.loads(raw)
        except (TypeError, json.JSONDecodeError):
            continue

        model = parsed.get("Model") or {}
        calculation = parsed.get("CalculationType") or {}
        refresh = parsed.get("RefreshReportingLayer") or {}
        rows.append(
            {
                "CLIENTID": row.get("CLIENTID"),
                "CLIENTNAME": row.get("CLIENTNAME"),
                "IST_TILL_MINUTES": row.get("IST_TILL_MINUTES"),
                "model_key": model.get("Key"),
                "model_caption": model.get("Caption"),
                "calculation_type_key": calculation.get("Key"),
                "calculation_type_caption": calculation.get("Caption"),
                "refresh_reporting_layer_key": refresh.get("Key"),
                "refresh_reporting_layer_caption": refresh.get("Caption"),
            }
        )
    return rows


def build_deterministic_result(payload: dict[str, Any]) -> dict[str, Any] | None:
    tables = payload.get("tables", {})
    process_rows = tables.get("cea_processlog", [])
    session_rows = tables.get("cea_sessionquery", [])
    form_rows = tables.get("cea_formenginelog", [])
    cealog_rows = tables.get("cea_cealog", [])

    active_triggers = [
        row
        for row in process_rows
        if str(row.get("WORKFLOWNAME", "")) == "SendEmailAfterModelExecution"
        and truthy(row.get("ISRUNNING"))
    ]
    if not active_triggers:
        return {
            "overall_status": "NO_RUNNING_MODEL",
            "client_results": [],
            "summary": "No active SendEmailAfterModelExecution runs were found.",
        }

    session_by_client = group_rows_by_client(session_rows)
    form_by_client = group_rows_by_client(form_rows)
    cealog_by_client = group_rows_by_client(cealog_rows)
    metadata_by_client = group_rows_by_client(payload.get("form_metadata_by_client_time", []))

    client_results = []
    for trigger in active_triggers:
        key = client_key(trigger.get("CLIENTID"), trigger.get("CLIENTNAME"))
        result = analyze_trigger(
            trigger=trigger,
            session_rows=session_by_client.get(key, []),
            form_rows=form_by_client.get(key, []),
            cealog_rows=cealog_by_client.get(key, []),
            form_metadata=metadata_by_client.get(key, []),
        )
        client_results.append(result)

    statuses = [row["status"] for row in client_results]
    overall_status = highest_status(statuses)
    return {
        "overall_status": overall_status,
        "client_results": client_results,
        "summary": summarize_results(client_results),
    }


def analyze_trigger(
    *,
    trigger: dict[str, Any],
    session_rows: list[dict[str, Any]],
    form_rows: list[dict[str, Any]],
    cealog_rows: list[dict[str, Any]],
    form_metadata: list[dict[str, Any]],
) -> dict[str, Any]:
    clientid = trigger.get("CLIENTID")
    clientname = trigger.get("CLIENTNAME")
    trigger_time = trigger.get("IST_TILL_MINUTES")
    evidence = [
        f"Processlog trigger exists for CLIENTID={clientid}, CLIENTNAME={clientname}, "
        f"IST_TILL_MINUTES={trigger_time}.",
    ]

    matching_sessions = same_client_minute(session_rows, clientid, clientname, trigger_time)
    if not matching_sessions:
        return client_result(
            clientid,
            clientname,
            trigger_time,
            "STUCK_NOT_PROGRESSING",
            evidence
            + [
                "No matching sessionquery row exists for the same client in the trigger minute "
                "or one minute later."
            ],
            recommendation="Check why the triggered model did not create matching session activity.",
        )

    session_duration_minutes = max(
        (number(row.get("DURATIONINSECONDS")) or 0) / 60
        for row in matching_sessions
    )
    evidence.append(
        f"Matching sessionquery found; max DURATIONINSECONDS is "
        f"{session_duration_minutes:.1f} minutes."
    )

    matching_forms = same_client_minute(form_rows, clientid, clientname, trigger_time)
    metadata = metadata_for(form_metadata, clientid, clientname, trigger_time)
    if not matching_forms or metadata is None:
        return client_result(
            clientid,
            clientname,
            trigger_time,
            "NEEDS_INVESTIGATION",
            evidence
            + [
                "No matching formenginelog metadata could be parsed for the same client in the "
                "trigger minute or one minute later."
            ],
            recommendation="Check formenginelog FULL_RESULT for the triggered model run.",
        )

    evidence.append(
        "Formenginelog metadata parsed: "
        f"model_key={metadata.get('model_key')}, "
        f"calculation_type={metadata.get('calculation_type_caption')}, "
        f"refresh_reporting_layer={metadata.get('refresh_reporting_layer_caption')}."
    )

    if not cealog_rows:
        return client_result(
            clientid,
            clientname,
            trigger_time,
            "LOG_DATA_UNAVAILABLE",
            evidence
            + [
                "No cea_cealog rows were loaded for monitoring. The log table may be empty "
                "temporarily while the ETL refresh deletes and reloads data."
            ],
            metadata=metadata,
            recommendation="Wait for the ETL refresh to finish, then rerun monitoring.",
        )

    client_cealog = same_client(cealog_rows, clientid, clientname)
    current_minute_cealog = same_client_minute(cealog_rows, clientid, clientname, trigger_time)
    start_rows = [
        row for row in current_minute_cealog if str(row.get("DESCRIPTION", "")).strip() == "-START-"
    ]
    if not start_rows:
        return client_result(
            clientid,
            clientname,
            trigger_time,
            "TRIGGERED_NOT_STARTED",
            evidence
            + [
                "No cea_cealog DESCRIPTION exactly equal to -START- was found for the same client "
                "in the trigger minute or one minute later."
            ],
            metadata=metadata,
            recommendation="Check why the execution log did not record the -START- marker.",
        )

    start_row = min(start_rows, key=row_id)
    start_id = row_id(start_row)
    run_rows = sorted(
        [row for row in client_cealog if row_id(row) >= start_id],
        key=row_id,
    )
    evidence.append(f"cea_cealog -START- found at ID {start_row.get('ID')}.")

    current_step = find_open_step(run_rows)
    if current_step is None:
        evidence.append("No open before/after execution boundary was found in the current run.")
        return client_result(
            clientid,
            clientname,
            trigger_time,
            "RUNNING_NORMAL",
            evidence,
            metadata=metadata,
            current_step=None,
            recommendation="No open long-running step was detected. Continue monitoring.",
        )

    historical_times = previous_same_type_times(
        form_metadata=form_metadata,
        current_metadata=metadata,
        trigger_time=metadata.get("IST_TILL_MINUTES") or trigger_time,
        limit=4,
    )
    historical_durations = [
        duration
        for historical_time in historical_times
        if (duration := historical_step_duration(
            cealog_rows=client_cealog,
            trigger_time=historical_time,
            step=current_step,
        )) is not None
    ]
    historical_average = (
        sum(historical_durations) / len(historical_durations)
        if historical_durations
        else None
    )

    evidence.append(f"Current open step is {current_step}.")
    evidence.append(
        f"Current session duration is {session_duration_minutes:.1f} minutes."
    )
    if historical_average is not None:
        minutes_over_average = round(session_duration_minutes - historical_average, 1)
        evidence.append(
            f"Historical average for {current_step} across {len(historical_durations)} "
            f"same-type runs is {historical_average:.1f} minutes."
        )
        if minutes_over_average > 20:
            return client_result(
                clientid,
                clientname,
                trigger_time,
                "LONG_RUNNING",
                evidence
                + [
                    f"Current duration is {minutes_over_average:.1f} minutes over the "
                    "historical average, which exceeds the 20 minute threshold."
                ],
                metadata=metadata,
                current_step=current_step,
                long_running_step=current_step,
                minutes_over_average=minutes_over_average,
                current_step_duration_minutes=session_duration_minutes,
                historical_step_average_minutes=historical_average,
                comparison_sample_count=len(historical_durations),
                recommendation=f"Investigate {current_step}; the active session is much slower than prior same-type runs.",
            )
        return client_result(
            clientid,
            clientname,
            trigger_time,
            "RUNNING_NORMAL",
            evidence
            + [
                f"Current duration is {minutes_over_average:.1f} minutes over the historical average, "
                "which does not exceed the 20 minute threshold."
            ],
            metadata=metadata,
            current_step=current_step,
            minutes_over_average=minutes_over_average,
            current_step_duration_minutes=session_duration_minutes,
            historical_step_average_minutes=historical_average,
            comparison_sample_count=len(historical_durations),
            recommendation="No action required yet. Continue monitoring.",
        )

    if session_duration_minutes > 20:
        return client_result(
            clientid,
            clientname,
            trigger_time,
            "LONG_RUNNING",
            evidence
            + [
                "No historical same-step average could be computed, but the open session already "
                "exceeds 20 minutes."
            ],
            metadata=metadata,
            current_step=current_step,
            long_running_step=current_step,
            minutes_over_average=round(session_duration_minutes, 1),
            current_step_duration_minutes=session_duration_minutes,
            historical_step_average_minutes=None,
            comparison_sample_count=0,
            recommendation=f"Investigate {current_step}; no completed matching historical step was available for comparison.",
        )

    return client_result(
        clientid,
        clientname,
        trigger_time,
        "RUNNING_NORMAL",
        evidence + ["No historical average was available and current duration is below 20 minutes."],
        metadata=metadata,
        current_step=current_step,
        recommendation="Continue monitoring until enough historical comparison data is available.",
    )


def client_result(
    clientid: Any,
    clientname: Any,
    trigger_time: Any,
    status: str,
    evidence: list[str],
    *,
    metadata: dict[str, Any] | None = None,
    current_step: str | None = None,
    long_running_step: str | None = None,
    minutes_over_average: float = 0,
    current_step_duration_minutes: float | None = None,
    historical_step_average_minutes: float | None = None,
    comparison_sample_count: int = 0,
    recommendation: str,
) -> dict[str, Any]:
    metadata = metadata or {}
    return {
        "clientid": str(clientid) if clientid is not None else None,
        "clientname": clientname,
        "trigger_time": trigger_time,
        "status": status,
        "model_key": metadata.get("model_key"),
        "model_caption": metadata.get("model_caption"),
        "calculation_type_caption": metadata.get("calculation_type_caption"),
        "refresh_reporting_layer_caption": metadata.get("refresh_reporting_layer_caption"),
        "evidence": evidence,
        "current_step": current_step,
        "long_running_step": long_running_step,
        "minutes_over_average": minutes_over_average,
        "current_step_duration_minutes": current_step_duration_minutes,
        "historical_step_average_minutes": historical_step_average_minutes,
        "comparison_sample_count": comparison_sample_count,
        "recommendation": recommendation,
    }


def same_client(rows: list[dict[str, Any]], clientid: Any, clientname: Any) -> list[dict[str, Any]]:
    return [
        row
        for row in rows
        if str(row.get("CLIENTID")) == str(clientid)
        and str(row.get("CLIENTNAME")) == str(clientname)
    ]


def client_key(clientid: Any, clientname: Any) -> tuple[str, str]:
    return str(clientid), str(clientname)


def group_rows_by_client(
    rows: list[dict[str, Any]],
) -> dict[tuple[str, str], list[dict[str, Any]]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in rows:
        key = client_key(row.get("CLIENTID"), row.get("CLIENTNAME"))
        grouped.setdefault(key, []).append(row)
    return grouped


def same_client_minute(
    rows: list[dict[str, Any]],
    clientid: Any,
    clientname: Any,
    minute: Any,
) -> list[dict[str, Any]]:
    return [
        row
        for row in same_client(rows, clientid, clientname)
        if minute_matches_trigger_window(row.get("IST_TILL_MINUTES"), minute)
    ]


def minute_matches_trigger_window(row_minute: Any, trigger_minute: Any) -> bool:
    row_dt = pd.to_datetime(row_minute, errors="coerce")
    trigger_dt = pd.to_datetime(trigger_minute, errors="coerce")
    if pd.isna(row_dt) or pd.isna(trigger_dt):
        return str(row_minute) == str(trigger_minute)

    row_dt = row_dt.floor("min")
    trigger_dt = trigger_dt.floor("min")
    delta_minutes = (row_dt - trigger_dt).total_seconds() / 60
    return delta_minutes in {0, 1}


def metadata_for(
    rows: list[dict[str, Any]],
    clientid: Any,
    clientname: Any,
    minute: Any,
) -> dict[str, Any] | None:
    matches = same_client_minute(rows, clientid, clientname, minute)
    return matches[0] if matches else None


def previous_same_type_times(
    *,
    form_metadata: list[dict[str, Any]],
    current_metadata: dict[str, Any],
    trigger_time: str,
    limit: int,
) -> list[str]:
    trigger_dt = pd.to_datetime(trigger_time, errors="coerce")
    if pd.isna(trigger_dt):
        return []

    matches: list[tuple[pd.Timestamp, str]] = []
    for row in form_metadata:
        row_time = row.get("IST_TILL_MINUTES")
        row_dt = pd.to_datetime(row_time, errors="coerce")
        if pd.isna(row_dt) or row_dt >= trigger_dt:
            continue
        if not is_same_run_type(row, current_metadata):
            continue
        matches.append((row_dt, row_dt.floor("min").strftime("%Y-%m-%d %H:%M")))

    return [row_time for _, row_time in sorted(matches, reverse=True)[:limit]]


def is_same_run_type(row: dict[str, Any], current_metadata: dict[str, Any]) -> bool:
    return (
        str(row.get("CLIENTID")) == str(current_metadata.get("CLIENTID"))
        and str(row.get("CLIENTNAME")) == str(current_metadata.get("CLIENTNAME"))
        and row.get("model_key") == current_metadata.get("model_key")
        and row.get("calculation_type_key") == current_metadata.get("calculation_type_key")
        and row.get("refresh_reporting_layer_key") == current_metadata.get("refresh_reporting_layer_key")
    )


def historical_step_duration(
    *,
    cealog_rows: list[dict[str, Any]],
    trigger_time: str,
    step: str,
) -> float | None:
    start_rows = [
        row
        for row in cealog_rows
        if str(row.get("IST_TILL_MINUTES")) == str(trigger_time)
        and str(row.get("DESCRIPTION", "")).strip() == "-START-"
    ]
    if not start_rows:
        return None

    start_id = row_id(min(start_rows, key=row_id))
    run_rows = sorted(
        [row for row in cealog_rows if row_id(row) >= start_id],
        key=row_id,
    )
    next_end_index = next(
        (
            index
            for index, row in enumerate(run_rows)
            if row_id(row) > start_id and str(row.get("DESCRIPTION", "")).strip() == "-END-"
        ),
        None,
    )
    if next_end_index is not None:
        run_rows = run_rows[: next_end_index + 1]

    before_row = next(
        (row for row in run_rows if boundary_step(row.get("DESCRIPTION"), "before") == step),
        None,
    )
    after_row = next(
        (
            row
            for row in run_rows
            if before_row is not None
            and row_id(row) > row_id(before_row)
            and boundary_step(row.get("DESCRIPTION"), "after") == step
        ),
        None,
    )
    if before_row is None or after_row is None:
        return None

    before_dt = pd.to_datetime(before_row.get("CREATED_DATE"), errors="coerce")
    after_dt = pd.to_datetime(after_row.get("CREATED_DATE"), errors="coerce")
    if pd.isna(before_dt) or pd.isna(after_dt):
        return None
    return max((after_dt - before_dt).total_seconds() / 60, 0)


def find_open_step(run_rows: list[dict[str, Any]]) -> str | None:
    if any(str(row.get("DESCRIPTION", "")).strip() == "-END-" for row in run_rows):
        return None

    open_steps: list[tuple[int, str]] = []
    for row in run_rows:
        description = row.get("DESCRIPTION")
        before_step = boundary_step(description, "before")
        after_step = boundary_step(description, "after")
        if before_step:
            open_steps.append((row_id(row), before_step))
        if after_step:
            open_steps = [
                (before_id, step)
                for before_id, step in open_steps
                if step != after_step
            ]
    if not open_steps:
        return None
    return max(open_steps, key=lambda item: item[0])[1]


def boundary_step(description: Any, boundary: str) -> str | None:
    text = str(description or "").strip()
    pattern = rf"\b{boundary}\s+(?:exec\s+)?(?P<step>[A-Za-z0-9_.$\[\]-]+)(?:\s+\d+)?\b"
    match = re.search(pattern, text, flags=re.IGNORECASE)
    if not match:
        return None
    return match.group("step")


def truthy(value: Any) -> bool:
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def number(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def row_id(row: dict[str, Any]) -> int:
    value = number(row.get("ID"))
    return int(value) if value is not None else -1


def highest_status(statuses: list[str]) -> str:
    priority = [
        "NO_RUNNING_MODEL",
        "RUNNING_NORMAL",
        "TRIGGERED_NOT_STARTED",
        "STUCK_NOT_PROGRESSING",
        "LOG_DATA_UNAVAILABLE",
        "NEEDS_INVESTIGATION",
        "LONG_RUNNING",
    ]
    return max(statuses, key=lambda status: priority.index(status) if status in priority else -1)


def summarize_results(results: list[dict[str, Any]]) -> str:
    long_running = [row for row in results if row["status"] == "LONG_RUNNING"]
    if long_running:
        clients = ", ".join(str(row["clientname"]) for row in long_running)
        return f"{len(long_running)} model run(s) are long running: {clients}."
    abnormal = [row for row in results if row["status"] != "RUNNING_NORMAL"]
    if abnormal:
        clients = ", ".join(f"{row['clientname']}={row['status']}" for row in abnormal)
        return f"{len(abnormal)} model run(s) need attention: {clients}."
    return "All active model runs are within deterministic monitoring thresholds."
