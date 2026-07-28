from __future__ import annotations

import hashlib
import json
import os
import re
from datetime import date
from typing import Any

from core.db import SnowflakeClient

TABLE = "CEA_Modelrun_History"
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*(?:\.[A-Za-z_][A-Za-z0-9_$]*){0,2}$")


class ModelRunHistoryStore:
    def __init__(self, db: SnowflakeClient | None = None) -> None:
        self.db = db or SnowflakeClient()
        self.table = os.getenv("MODEL_RUN_HISTORY_TABLE", TABLE).strip()
        if not _IDENTIFIER.fullmatch(self.table):
            raise ValueError("MODEL_RUN_HISTORY_TABLE is not a valid identifier")
        self.ready = False

    def ensure_table(self) -> None:
        if self.ready:
            return
        self.db.execute(f"""create table if not exists {self.table} (
            HISTORY_ID varchar(64), CLIENT_ID varchar, CLIENT_NAME varchar,
            MODEL_KEY varchar, MODEL_CAPTION varchar, TRIGGER_TIME timestamp_ntz,
            MODEL_RUN_ID varchar, RUN_STARTED_AT timestamp_ntz,
            STATUS varchar, CURRENT_STEP varchar, LONG_RUNNING_STEP varchar,
            MINUTES_OVER_AVERAGE float, RECOMMENDATION varchar, EVIDENCE variant,
            FIRST_SEEN_AT timestamp_ntz default current_timestamp(),
            LAST_SEEN_AT timestamp_ntz default current_timestamp(),
            constraint CEA_MODELRUN_HISTORY_PK primary key (HISTORY_ID))""")
        self.db.execute(f"alter table {self.table} add column if not exists RUN_DURATION_SECONDS float")
        self.db.execute(f"alter table {self.table} add column if not exists AVERAGE_DURATION_SECONDS float")
        self.db.execute(f"alter table {self.table} add column if not exists STEPS variant")
        self.db.execute(f"alter table {self.table} add column if not exists RAW_DATA variant")
        self.db.execute(f"alter table {self.table} add column if not exists NOTIFIED_BY_AGENT number")
        self.db.execute(f"alter table {self.table} add column if not exists MODEL_RUN_ID varchar")
        self.db.execute(f"alter table {self.table} add column if not exists RUN_STARTED_AT timestamp_ntz")
        self.ready = True

    @staticmethod
    def run_id(run: dict[str, Any]) -> str:
        stable_run_id = str(run.get("model_run_id") or "").strip()
        value = (
            f"xactly-run|{stable_run_id}"
            if stable_run_id
            else "|".join(
                str(run.get(k) or "").strip()
                for k in ("clientid", "clientname", "model_key", "trigger_time")
            )
        )
        return hashlib.sha256(value.encode()).hexdigest()

    def save_runs(self, runs: list[dict[str, Any]]) -> None:
        if not runs:
            return
        self.ensure_table()
        for run in runs:
            p = {"id": self.run_id(run), "client": str(run.get("clientid") or ""), "name": str(run.get("clientname") or ""), "key": str(run.get("model_key") or ""), "caption": str(run.get("model_caption") or ""), "trigger": str(run.get("trigger_time") or "") or None, "model_run_id": str(run.get("model_run_id") or ""), "run_started": str(run.get("run_started_at") or "") or None, "status": str(run.get("status") or "UNKNOWN"), "step": str(run.get("current_step") or ""), "long_step": str(run.get("long_running_step") or ""), "minutes": float(run.get("minutes_over_average") or 0), "duration": float(run.get("run_duration_seconds") or 0), "average": float(run.get("average_duration_seconds") or 0), "notified": int(run["notified_by_agent"] if "notified_by_agent" in run else float(run.get("minutes_over_average") or 0) > 20), "recommendation": str(run.get("recommendation") or ""), "evidence": json.dumps(run.get("evidence") or []), "steps": json.dumps(run.get("steps") or []), "raw": json.dumps(run)}
            self.db.execute(f"""merge into {self.table} t using (select %(id)s HISTORY_ID, %(client)s CLIENT_ID, %(name)s CLIENT_NAME, %(key)s MODEL_KEY, %(caption)s MODEL_CAPTION, try_to_timestamp_ntz(%(trigger)s) TRIGGER_TIME, %(model_run_id)s MODEL_RUN_ID, try_to_timestamp_ntz(%(run_started)s) RUN_STARTED_AT, %(status)s STATUS, %(step)s CURRENT_STEP, %(long_step)s LONG_RUNNING_STEP, %(minutes)s MINUTES_OVER_AVERAGE, %(duration)s RUN_DURATION_SECONDS, %(average)s AVERAGE_DURATION_SECONDS, %(notified)s NEW_NOTIFIED_BY_AGENT, %(recommendation)s RECOMMENDATION, parse_json(%(evidence)s) EVIDENCE, parse_json(%(steps)s) STEPS, parse_json(%(raw)s) RAW_DATA) s
                on t.HISTORY_ID=s.HISTORY_ID when matched then update set TRIGGER_TIME=s.TRIGGER_TIME,MODEL_RUN_ID=s.MODEL_RUN_ID,RUN_STARTED_AT=s.RUN_STARTED_AT,STATUS=s.STATUS,CURRENT_STEP=s.CURRENT_STEP,LONG_RUNNING_STEP=s.LONG_RUNNING_STEP,MINUTES_OVER_AVERAGE=s.MINUTES_OVER_AVERAGE,RUN_DURATION_SECONDS=s.RUN_DURATION_SECONDS,AVERAGE_DURATION_SECONDS=s.AVERAGE_DURATION_SECONDS,NOTIFIED_BY_AGENT=greatest(coalesce(t.NOTIFIED_BY_AGENT,0),s.NEW_NOTIFIED_BY_AGENT),RECOMMENDATION=s.RECOMMENDATION,EVIDENCE=s.EVIDENCE,STEPS=s.STEPS,RAW_DATA=s.RAW_DATA,LAST_SEEN_AT=current_timestamp()
                when not matched then insert (HISTORY_ID,CLIENT_ID,CLIENT_NAME,MODEL_KEY,MODEL_CAPTION,TRIGGER_TIME,MODEL_RUN_ID,RUN_STARTED_AT,STATUS,CURRENT_STEP,LONG_RUNNING_STEP,MINUTES_OVER_AVERAGE,RUN_DURATION_SECONDS,AVERAGE_DURATION_SECONDS,NOTIFIED_BY_AGENT,RECOMMENDATION,EVIDENCE,STEPS,RAW_DATA) values (s.HISTORY_ID,s.CLIENT_ID,s.CLIENT_NAME,s.MODEL_KEY,s.MODEL_CAPTION,s.TRIGGER_TIME,s.MODEL_RUN_ID,s.RUN_STARTED_AT,s.STATUS,s.CURRENT_STEP,s.LONG_RUNNING_STEP,s.MINUTES_OVER_AVERAGE,s.RUN_DURATION_SECONDS,s.AVERAGE_DURATION_SECONDS,s.NEW_NOTIFIED_BY_AGENT,s.RECOMMENDATION,s.EVIDENCE,s.STEPS,s.RAW_DATA)""", p)

    def load_runs(
        self,
        run_date: date | None = None,
        limit: int = 1000,
        *,
        client_id: str = "",
        model_key: str = "",
        notified_only: bool = False,
        category: str = "",
    ) -> list[dict[str, Any]]:
        self.ensure_table()
        conditions, params = [], {}
        if run_date:
            conditions.append("to_date(coalesce(h.TRIGGER_TIME,h.FIRST_SEEN_AT))=%(run_date)s")
            params["run_date"] = run_date.isoformat()
        if client_id:
            conditions.append("h.CLIENT_ID=%(client_id)s")
            params["client_id"] = client_id
        if model_key:
            conditions.append("h.MODEL_KEY=%(model_key)s")
            params["model_key"] = model_key
        if notified_only:
            conditions.append("coalesce(h.NOTIFIED_BY_AGENT,0)=1")
        normalized_category = category.strip().lower()
        if normalized_category == "successful":
            conditions.append("upper(coalesce(h.STATUS,'UNKNOWN')) not in ('LONG_RUNNING','STUCK_NOT_PROGRESSING','FAILED')")
        elif normalized_category == "long_running":
            conditions.append("upper(coalesce(h.STATUS,'UNKNOWN'))='LONG_RUNNING'")
        elif normalized_category == "stuck":
            conditions.append("upper(coalesce(h.STATUS,'UNKNOWN'))='STUCK_NOT_PROGRESSING'")
        elif normalized_category not in ("", "total"):
            raise ValueError(f"Unknown model history category: {category}")
        where = f"where {' and '.join(conditions)}" if conditions else ""
        frame = self.db.query_df(f"select h.* from {self.table} h {where} order by coalesce(h.TRIGGER_TIME,h.FIRST_SEEN_AT) desc limit {int(limit)}", params or None)
        frame.columns = [str(c).lower() for c in frame.columns]
        rows = frame.where(frame.notna(), None).to_dict("records")
        for row in rows:
            for key in ("trigger_time", "run_started_at", "first_seen_at", "last_seen_at"):
                if row.get(key) is not None and hasattr(row[key], "isoformat"): row[key] = row[key].isoformat()
            for field in ("evidence", "steps", "raw_data"):
                if isinstance(row.get(field), str):
                    try: row[field] = json.loads(row[field])
                    except json.JSONDecodeError: pass
        durations: dict[str, list[float]] = {}
        for row in rows:
            for step in row.get("steps") or []:
                key = str(step.get("description") or "").strip().lower()
                if key:
                    durations.setdefault(key, []).append(float(step.get("duration_seconds") or 0))
        for row in rows:
            for step in row.get("steps") or []:
                values = durations.get(str(step.get("description") or "").strip().lower(), [])
                if step.get("average_duration_seconds") is None:
                    step["average_duration_seconds"] = sum(values) / len(values) if values else 0.0
        return rows

    def status_summary(self) -> dict[str, int]:
        """Return cumulative counts for the Application Runs summary cards."""
        self.ensure_table()
        frame = self.db.query_df(
            f"select STATUS, count(*) as RUN_COUNT from {self.table} group by STATUS"
        )
        counts = {
            str(row.get("STATUS") or "UNKNOWN").upper(): int(row.get("RUN_COUNT") or 0)
            for row in frame.to_dict("records")
        }
        total = sum(counts.values())
        long_running = counts.get("LONG_RUNNING", 0)
        stuck = counts.get("STUCK_NOT_PROGRESSING", 0)
        failed = counts.get("FAILED", 0)
        return {
            "total": total,
            "successful": max(0, total - long_running - stuck - failed),
            "long_running": long_running,
            "stuck": stuck,
        }
