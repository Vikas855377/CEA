from __future__ import annotations

import asyncio
import io
import importlib.util
import json
import logging
import os
import re
import subprocess
import sys
import threading
import time
from collections import deque
from contextlib import asynccontextmanager, suppress
from datetime import date
from pathlib import Path
from typing import Any, Optional


def _bootstrap_dependencies() -> None:
    """Install Python and browser dependencies when app.py runs on a clean host."""
    if os.getenv("CEA_AUTO_INSTALL_DEPENDENCIES", "true").strip().lower() not in {
        "1",
        "true",
        "yes",
        "y",
    }:
        return

    requirements = Path(__file__).resolve().parent / "requirements.txt"
    required_modules = (
        "fastapi",
        "uvicorn",
        "pandas",
        "dotenv",
        "playwright",
        "snowflake",
        "openai",
        "jaydebeapi",
        "jpype",
    )
    missing = [
        module
        for module in required_modules
        if importlib.util.find_spec(module) is None
    ]
    if missing:
        if not requirements.is_file():
            raise RuntimeError(f"Dependency file not found: {requirements}")
        print(
            f"Installing missing dependencies: {', '.join(missing)}",
            flush=True,
        )
        try:
            subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "pip",
                    "install",
                    "--disable-pip-version-check",
                    "-r",
                    str(requirements),
                ],
                check=True,
            )
        except subprocess.CalledProcessError as exc:
            raise RuntimeError(
                "Automatic dependency installation failed. Ensure the host has "
                "internet access and permits Python package installation."
            ) from exc
        os.execve(sys.executable, [sys.executable, *sys.argv], os.environ.copy())

    try:
        from playwright.sync_api import sync_playwright

        with sync_playwright() as playwright:
            chromium_path = Path(playwright.chromium.executable_path)
        if not chromium_path.is_file():
            print("Installing the Playwright Chromium browser...", flush=True)
            subprocess.run(
                [sys.executable, "-m", "playwright", "install", "chromium"],
                check=True,
            )
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(
            "Playwright Chromium installation failed. Use the supplied Dockerfile "
            "when the host also needs browser system libraries."
        ) from exc


_bootstrap_dependencies()

import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from projects.ProcessRunAutomation.service import (
    ensure_scheduler_started,
    load_current_etl_progress,
    load_etl_run_history,
    publish_model_snapshot_complete,
    load_recent_run_summary,
    stop_scheduler,
)
from projects.ProcessRunAutomation.spm_package import PackageConfig, generate_package
from projects.model_monitoring.project import ModelMonitoringProject, use_csv_source
from projects.model_monitoring.agents.model_monitoring_agent import (
    minute_matches_trigger_window,
)
from projects.model_monitoring.model_run_history import ModelRunHistoryStore


ROOT = Path(__file__).resolve().parent
UI_DIR = ROOT / "ui"
LOGGER = logging.getLogger(__name__)
RUN_CACHE_TTL = int(os.getenv("MODEL_MONITORING_RUN_CACHE_TTL", "300"))
MODEL_REFRESH_POLL_SECONDS = max(
    5, int(os.getenv("MODEL_MONITORING_REFRESH_POLL_SECONDS", "15"))
)
MODEL_ON_DEMAND_REFRESH = os.getenv(
    "MODEL_MONITORING_ON_DEMAND_REFRESH", "true"
).strip().lower() in {"1", "true", "yes", "y"}
DEFAULT_LIMIT = int(os.getenv("MODEL_MONITORING_LIMIT_PER_TABLE", "5000"))

SCHEDULER_WATCHDOG_SECONDS = max(
    5, int(os.getenv("CEA_SCHEDULER_WATCHDOG_SECONDS", "15"))
)


async def _scheduler_watchdog() -> None:
    while True:
        ensure_scheduler_started()
        await asyncio.sleep(SCHEDULER_WATCHDOG_SECONDS)


async def _model_snapshot_watchdog() -> None:
    """Refresh model telemetry after each completed ETL cycle."""
    last_completed_at: str | None = None
    while True:
        latest_runs = await asyncio.to_thread(load_etl_run_history)
        latest_run = latest_runs[0] if latest_runs else None
        completed_at = latest_run.completed_at if latest_run else None
        if completed_at and completed_at != last_completed_at:
            refreshed = await asyncio.to_thread(
                refresh_model_snapshot, DEFAULT_LIMIT, True
            )
            if refreshed:
                await asyncio.to_thread(
                    publish_model_snapshot_complete,
                    latest_run.cycle,
                    latest_run.completed_at,
                )
                last_completed_at = completed_at
        await asyncio.sleep(MODEL_REFRESH_POLL_SECONDS)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    ensure_scheduler_started()
    watchdog = asyncio.create_task(_scheduler_watchdog())
    model_watchdog = asyncio.create_task(_model_snapshot_watchdog())
    try:
        yield
    finally:
        watchdog.cancel()
        model_watchdog.cancel()
        with suppress(asyncio.CancelledError):
            await watchdog
        with suppress(asyncio.CancelledError):
            await model_watchdog
        stop_scheduler()


app = FastAPI(
    title="CEA Ops Monitor",
    version="2.1.0",
    docs_url="/api/docs",
    lifespan=lifespan,
)
app.mount("/assets", StaticFiles(directory=UI_DIR), name="assets")

_project: ModelMonitoringProject | None = None
_project_lock = threading.Lock()
_run_cache: dict[str, Any] = {}
_cache_lock = threading.Lock()
_model_refresh_lock = threading.Lock()
_history_store: ModelRunHistoryStore | None = None


def get_project() -> ModelMonitoringProject:
    global _project
    if _project is None:
        with _project_lock:
            if _project is None:
                _project = ModelMonitoringProject()
    return _project

def get_history_store() -> ModelRunHistoryStore:
    global _history_store
    if _history_store is None:
        _history_store = ModelRunHistoryStore()
    return _history_store


def _current_run_logs(
    logs: pd.DataFrame,
    *,
    client_id: str,
    client_name: str,
    trigger_time: Any,
) -> pd.DataFrame:
    """Return only the log rows belonging to one active model execution."""
    if logs.empty or "ID" not in logs or "DESCRIPTION" not in logs:
        return pd.DataFrame(columns=logs.columns)
    matching = logs[
        (logs.get("CLIENTID", "").astype(str) == client_id)
        & (logs.get("CLIENTNAME", "").astype(str) == client_name)
    ].copy()
    if matching.empty:
        return matching
    matching["_RUN_ID"] = pd.to_numeric(matching["ID"], errors="coerce")
    starts = matching[
        matching["DESCRIPTION"].astype(str).str.strip().eq("-START-")
        & matching.get("IST_TILL_MINUTES", pd.Series(index=matching.index, dtype=object)).map(
            lambda value: minute_matches_trigger_window(value, trigger_time)
        )
    ]
    if starts.empty:
        return matching.iloc[0:0].drop(columns=["_RUN_ID"])
    start_id = starts["_RUN_ID"].min()
    run_rows = matching[matching["_RUN_ID"] >= start_id].sort_values("_RUN_ID")
    ends = run_rows[
        (run_rows["_RUN_ID"] > start_id)
        & run_rows["DESCRIPTION"].astype(str).str.strip().eq("-END-")
    ]
    if not ends.empty:
        run_rows = run_rows[run_rows["_RUN_ID"] <= ends["_RUN_ID"].min()]
    return run_rows.drop(columns=["_RUN_ID"])


def _build_model_snapshot(limit: int) -> dict[str, Any]:
    started = time.perf_counter()
    project = get_project()
    tables = project.load_selected_model_context(limit_per_table=limit)
    loaded = time.perf_counter()
    run = project.run(tables=tables)
    # Preserve the full observed execution trail alongside the agent summary.
    for item in run.result.get("client_results", []):
        client_id, client_name = str(item.get("clientid") or ""), str(item.get("clientname") or "")
        session_rows = tables.get("cea_sessionquery")
        if session_rows is not None and not session_rows.empty:
            matching = session_rows[(session_rows.get("CLIENTID", "").astype(str) == client_id) & (session_rows.get("CLIENTNAME", "").astype(str) == client_name)]
            if not matching.empty and "DURATIONINSECONDS" in matching:
                item["run_duration_seconds"] = float(matching.iloc[0]["DURATIONINSECONDS"] or 0)
        logs = tables.get("cea_cealog")
        steps = []
        if logs is not None and not logs.empty:
            matching = _current_run_logs(
                logs,
                client_id=client_id,
                client_name=client_name,
                trigger_time=item.get("trigger_time"),
            )
            if not matching.empty:
                start_rows = matching[
                    matching["DESCRIPTION"].astype(str).str.strip().eq("-START-")
                ]
                if not start_rows.empty:
                    start_row = start_rows.sort_values("ID").iloc[0]
                    item["model_run_id"] = str(
                        start_row.get("RUN_ID")
                        or start_row.get("EXECUTION_ID")
                        or start_row.get("ID")
                    )
                    run_started_at = start_row.get("CREATED_DATE")
                    item["run_started_at"] = (
                        run_started_at.isoformat()
                        if hasattr(run_started_at, "isoformat")
                        else run_started_at
                    )
                matching = matching.copy()
                matching["_STEP_TIME"] = pd.to_datetime(
                    matching.get("CREATED_DATE"), errors="coerce"
                )
                matching = matching.sort_values(["_STEP_TIME", "ID"], na_position="last")
                records = list(matching.iterrows())
                for position, (_, row) in enumerate(records):
                    step = {
                        k.lower(): (None if str(row.get(k, "")) == "nan" else row.get(k))
                        for k in ("DESCRIPTION", "STEP_NO", "CREATED_DATE", "LEVEL", "ROWCOUNT")
                    }
                    if hasattr(step.get("created_date"), "isoformat"):
                        step["created_date"] = step["created_date"].isoformat()
                    for numeric_field in ("step_no", "rowcount"):
                        if hasattr(step.get(numeric_field), "item"):
                            step[numeric_field] = step[numeric_field].item()
                    current_time = row.get("_STEP_TIME")
                    next_time = records[position + 1][1].get("_STEP_TIME") if position + 1 < len(records) else None
                    step["duration_seconds"] = (
                        max(0.0, float((next_time - current_time).total_seconds()))
                        if current_time is not None
                        and next_time is not None
                        and not pd.isna(current_time)
                        and not pd.isna(next_time)
                        else 0.0
                    )
                    steps.append(step)
        item["steps"] = steps
        item["average_duration_seconds"] = max(float(item.get("run_duration_seconds") or 0) - float(item.get("minutes_over_average") or 0) * 60, 0)
        minutes_over_average = float(item.get("minutes_over_average") or 0)
        item["notified_by_agent"] = 1 if minutes_over_average > 20 else 0
        for step in steps:
            step["notified_by_agent"] = 0
        if item["notified_by_agent"] and steps:
            offending_step = str(
                item.get("long_running_step") or item.get("current_step") or ""
            ).strip().lower()
            boundary = re.compile(
                rf"\bbefore(?:\s+exec)?\s+{re.escape(offending_step)}(?:\s|$)",
                re.IGNORECASE,
            )
            matching_positions = [
                position
                for position, step in enumerate(steps)
                if offending_step
                and boundary.search(str(step.get("description") or ""))
            ]
            if matching_positions:
                flagged_step = steps[matching_positions[-1]]
                current_duration_seconds = float(
                    item.get("current_step_duration_minutes")
                    or float(item.get("run_duration_seconds") or 0) / 60
                ) * 60
                historical_minutes = item.get("historical_step_average_minutes")
                historical_average_seconds = (
                    float(historical_minutes) * 60
                    if historical_minutes is not None
                    else max(0.0, current_duration_seconds - minutes_over_average * 60)
                )
                flagged_step.update(
                    {
                        "duration_seconds": current_duration_seconds,
                        "average_duration_seconds": historical_average_seconds,
                        "minutes_over_average": minutes_over_average,
                        "notification_threshold_minutes": 20,
                        "comparison_sample_count": int(
                            item.get("comparison_sample_count") or 0
                        ),
                        "alert_basis": "exact_open_step_boundary",
                        "notified_by_agent": 1,
                    }
                )
            else:
                steps.append(
                    {
                        "step_no": "ALERT",
                        "description": offending_step,
                        "created_date": item.get("trigger_time"),
                        "duration_seconds": float(
                            item.get("current_step_duration_minutes")
                            or float(item.get("run_duration_seconds") or 0) / 60
                        )
                        * 60,
                        "average_duration_seconds": (
                            float(item["historical_step_average_minutes"]) * 60
                            if item.get("historical_step_average_minutes") is not None
                            else 0.0
                        ),
                        "minutes_over_average": minutes_over_average,
                        "notification_threshold_minutes": 20,
                        "comparison_sample_count": int(
                            item.get("comparison_sample_count") or 0
                        ),
                        "alert_basis": "agent_detected_open_step",
                        "notified_by_agent": 1,
                    }
                )
    if run.result.get("client_results"):
        try:
            get_history_store().save_runs(run.result["client_results"])
        except Exception:
            LOGGER.exception("Unable to save model run history")
    finished = time.perf_counter()
    return {
        "result": run.result,
        "metrics": {
            "data_load_seconds": round(loaded - started, 2),
            "agent_seconds": round(finished - loaded, 2),
            "total_seconds": round(finished - started, 2),
            "limit_per_table": limit,
            "data_source": "csv" if use_csv_source() else "xactly",
        },
        "row_counts": {name: len(frame) for name, frame in run.tables.items()},
        "generated_at": time.time(),
        "cached": False,
    }


def refresh_model_snapshot(limit: int = DEFAULT_LIMIT, force: bool = False) -> bool:
    """Refresh the cache in one background worker; never overlap Xactly reads."""
    if not _model_refresh_lock.acquire(blocking=False):
        return False
    try:
        cache_key = f"all:{limit}"
        with _cache_lock:
            cached = _run_cache.get(cache_key)
        if not force and cached and time.time() - cached[0] < RUN_CACHE_TTL:
            return False

        payload = _build_model_snapshot(limit)
        status = str(payload.get("result", {}).get("overall_status") or "")
        # A refresh-in-progress response is transient and must not hide the first
        # complete post-ETL snapshot for the full cache lifetime.
        if status == "DATA_REFRESH_IN_PROGRESS":
            LOGGER.info(
                "Model analysis is waiting for refreshed ETL data; completion "
                "acknowledgement will not be published yet"
            )
            return False
        with _cache_lock:
            _run_cache[cache_key] = (time.time(), payload)
        return True
    except Exception:
        LOGGER.exception("Background model telemetry refresh failed")
        return False
    finally:
        _model_refresh_lock.release()


def _start_model_refresh(limit: int) -> None:
    threading.Thread(
        target=refresh_model_snapshot,
        args=(limit, True),
        name="model-telemetry-refresh",
        daemon=True,
    ).start()


def model_snapshot(*, force: bool = False, limit: int = DEFAULT_LIMIT) -> dict[str, Any]:
    cache_key = f"all:{limit}"
    with _cache_lock:
        cached = _run_cache.get(cache_key)
    stale = not cached or time.time() - cached[0] >= RUN_CACHE_TTL
    if force or (stale and MODEL_ON_DEMAND_REFRESH):
        _start_model_refresh(limit)

    if cached:
        return {
            **cached[1],
            "cached": True,
            "refreshing": force or stale,
        }

    return {
        "result": {
            "overall_status": "TELEMETRY_INITIALIZING",
            "client_results": [],
            "summary": "Preparing the first model telemetry snapshot in the background.",
        },
        "metrics": {
            "data_load_seconds": 0,
            "agent_seconds": 0,
            "total_seconds": 0,
            "limit_per_table": limit,
            "data_source": "csv" if use_csv_source() else "xactly",
        },
        "row_counts": {},
        "generated_at": None,
        "cached": False,
        "refreshing": True,
    }


def _dashboard_page() -> HTMLResponse:
    page = (UI_DIR / "index.html").read_text(encoding="utf-8")
    page = page.replace(
        "window.__CONFIGURED_CLIENT_IDS__=null",
        f"window.__CONFIGURED_CLIENT_IDS__={json.dumps(sorted(_configured_client_ids()))}",
    )
    return HTMLResponse(page)


@app.get("/", include_in_schema=False)
def index() -> HTMLResponse:
    return _dashboard_page()


@app.get("/logs", include_in_schema=False)
def logs_page() -> HTMLResponse:
    return _dashboard_page()


class ClientPackageRequest(BaseModel):
    client_id: int = Field(ge=1)
    client_name: str = Field(min_length=1, max_length=100)
    staging_prefix: str = Field(min_length=2, max_length=63)
    process_id: int = Field(default=143, ge=1)
    package_name: str = Field(default="Monitoring", min_length=1, max_length=100)


def _configured_client_ids() -> set[int]:
    clients_file = ROOT / "projects" / "ProcessRunAutomation" / "clients.json"
    with clients_file.open(encoding="utf-8") as handle:
        clients = json.load(handle).get("clients", [])
    return {
        int(client["monitoring_client_id"])
        for client in clients
        if str(client.get("monitoring_client_id", "")).isdigit()
    }


@app.get("/api/configure/clients")
async def configured_clients() -> dict[str, Any]:
    return {"client_ids": sorted(_configured_client_ids())}


def _tail_log(path: Path, lines: int) -> list[str]:
    if not path.is_file():
        return []
    with path.open(encoding="utf-8", errors="replace") as handle:
        return list(deque(handle, maxlen=lines))


@app.get("/api/logs")
async def application_logs(lines: int = Query(500, ge=10, le=5000)) -> dict[str, Any]:
    scheduler_log = ROOT / "projects" / "ProcessRunAutomation" / "logs" / "scheduler.log"
    monitoring_log = ROOT / "projects" / "model_monitoring" / "logs" / "model_monitoring.log"
    return {
        "scheduler": _tail_log(scheduler_log, lines),
        "model_monitoring": _tail_log(monitoring_log, lines),
    }


@app.post("/api/configure/package")
def create_client_package(request: ClientPackageRequest) -> StreamingResponse:
    if request.client_id in _configured_client_ids():
        raise HTTPException(
            status_code=409,
            detail=f"Client ID {request.client_id} already exists",
        )
    try:
        payload, filename = generate_package(
            PackageConfig(
                client_id=request.client_id,
                client_name=request.client_name.strip(),
                staging_prefix=request.staging_prefix.strip(),
                process_id=request.process_id,
                package_name=request.package_name.strip(),
            )
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return StreamingResponse(
        io.BytesIO(payload),
        media_type="application/octet-stream",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/api/health")
def health() -> dict[str, Any]:
    scheduler = ensure_scheduler_started()
    return {
        "status": "healthy" if scheduler.running or not scheduler.enabled else "degraded",
        "service": "cea-ops-monitor",
        "scheduler": {
            "enabled": scheduler.enabled,
            "running": scheduler.running,
            "pid": scheduler.pid,
            "message": scheduler.message,
        },
    }


@app.get("/api/etl")
def etl_status() -> dict[str, Any]:
    scheduler = ensure_scheduler_started()
    summary = load_recent_run_summary()
    progress = load_current_etl_progress()
    total = summary.successful + summary.failed
    return {
        "scheduler": {
            "enabled": scheduler.enabled,
            "running": scheduler.running,
            "pid": scheduler.pid,
            "message": scheduler.message,
        },
        "summary": {
            "successful": summary.successful,
            "failed": summary.failed,
            "cycles": summary.cycles,
            "success_rate": round(summary.successful / total * 100, 1) if total else 0,
            "last_completed_at": summary.last_completed_at,
        },
        "current_progress": {
            "percent": progress.percent,
            "completed": progress.completed,
            "total": progress.total,
            "state": progress.state,
            "cycle": progress.cycle,
            "started_at": progress.started_at,
            "next_cycle_at": progress.next_cycle_at,
            "remaining_seconds": progress.remaining_seconds,
        },
    }


@app.get("/api/models")
def models(
    refresh: bool = Query(False),
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=25000),
) -> dict[str, Any]:
    try:
        return model_snapshot(force=refresh, limit=limit)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@app.get("/api/models/stats")
def model_stats() -> dict[str, int]:
    try:
        return get_history_store().status_summary()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Model statistics unavailable: {exc}") from exc


@app.get("/api/etl/history")
def etl_history(run_date: Optional[date] = Query(None)) -> dict[str, Any]:
    runs = load_etl_run_history(run_date.isoformat() if run_date else None)
    return {
        "date": run_date.isoformat() if run_date else None,
        "runs": [
            {
                "cycle": run.cycle,
                "completed_at": run.completed_at,
                "duration_seconds": run.duration_seconds,
                "successful": run.successful,
                "failed": run.failed,
                "total": run.total,
            }
            for run in runs
        ],
    }

@app.get("/api/models/history")
def model_history(
    run_date: Optional[date] = Query(None),
    client_id: str = Query(""),
    model_key: str = Query(""),
    notified_only: bool = Query(False),
    category: str = Query(""),
    limit: int = Query(1000, ge=1, le=5000),
) -> dict[str, Any]:
    try:
        return {
            "date": run_date.isoformat() if run_date else None,
            "runs": get_history_store().load_runs(
                run_date,
                limit,
                client_id=client_id,
                model_key=model_key,
                notified_only=notified_only,
                category=category,
            ),
        }
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Model run history unavailable: {exc}") from exc


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        app,
        host=os.getenv("CEA_APP_HOST", "127.0.0.1"),
        port=int(os.getenv("PORT", "8080")),
        log_level=os.getenv("CEA_APP_LOG_LEVEL", "info"),
    )
