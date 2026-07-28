from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timedelta
from pathlib import Path


PROCESSLOG_COLUMNS = [
    "clientid",
    "clientname",
    "Process_id",
    "WorkflowName",
    "Original_UTC",
    "StartedBy",
    "IsRunning",
    "IST_Till_Minutes",
]
SESSIONQUERY_COLUMNS = [
    "clientid",
    "clientname",
    "DatabaseName",
    "session_id",
    "status",
    "SqlStatement",
    "ClientHost",
    "ClientProgram",
    "ClientProcessId",
    "SqlLoginUser",
    "DurationInSeconds",
    "start_time",
    "IST_Till_Minutes",
    "cpu_time",
    "logical_reads",
    "writes",
    "ParentStatement",
    "wait_type",
    "BlockingSessionId",
    "BlockingHostname",
    "BlockingProgram",
    "BlockingClientProcessId",
    "BlockingSql",
]
FORMENGINELOG_COLUMNS = [
    "clientid",
    "clientname",
    "formenginelogid",
    "created_date",
    "Operation",
    "FileName",
    "FilePath",
    "full_result",
    "CalculationType",
    "IST_Till_Minutes",
]
CEALOG_COLUMNS = [
    "clientid",
    "clientname",
    "cealogid",
    "id",
    "run_id",
    "created_date",
    "description",
    "step_no",
    "execution_id",
    "level",
    "rowcount",
    "modelid",
    "IST_Till_Minutes",
]

WORKFLOW_NAME = "SendEmailAfterModelExecution"
TOTAL_PROCESS_ROWS = 100_000
TOTAL_SESSION_ROWS = 100_000
TOTAL_FORM_ROWS = 100_000
CEALOG_ROWS_PER_RUN = 7


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate model monitoring load-test CSVs.")
    parser.add_argument(
        "--output-dir",
        default="data/model_monitoring/load_test",
        help="Directory where processlog/sessionquery/formenginelog/cealog CSVs will be written.",
    )
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    clients = [
        {
            "clientid": str(1000 + index),
            "clientname": f"LoadTestClient{index:02d}",
            "active": index <= 10,
            "long_running": index in {1, 3, 5, 7, 9},
        }
        for index in range(1, 21)
    ]
    base_time = datetime(2026, 7, 2, 10, 0)

    write_processlog(output_dir / "processlog.csv", clients, base_time)
    write_sessionquery(output_dir / "sessionquery.csv", clients, base_time)
    write_formenginelog(output_dir / "formenginelog.csv", clients, base_time)
    write_cealog(output_dir / "cealog.csv", clients, base_time)

    print(f"Wrote 1,000,000 rows to {output_dir}")
    print("Clients: 20 total, 10 active, 10 inactive")
    print("Rows: processlog=100000, sessionquery=100000, formenginelog=100000, cealog=700000")


def write_processlog(path: Path, clients: list[dict[str, object]], base_time: datetime) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=PROCESSLOG_COLUMNS)
        writer.writeheader()
        for row_index in range(TOTAL_PROCESS_ROWS):
            client, run_number, event_time = event_context(row_index, clients, base_time)
            writer.writerow(
                {
                    "clientid": client["clientid"],
                    "clientname": client["clientname"],
                    "Process_id": deterministic_uuid("process", row_index),
                    "WorkflowName": WORKFLOW_NAME,
                    "Original_UTC": iso_millis(event_time - timedelta(hours=5, minutes=30)),
                    "StartedBy": f"monitor{int(client['clientid']) % 5}@loadtest.local",
                    "IsRunning": str(bool(client["active"]) and run_number == 0).lower(),
                    "IST_Till_Minutes": minute_label(event_time),
                }
            )


def write_sessionquery(path: Path, clients: list[dict[str, object]], base_time: datetime) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=SESSIONQUERY_COLUMNS)
        writer.writeheader()
        for row_index in range(TOTAL_SESSION_ROWS):
            client, run_number, event_time = event_context(row_index, clients, base_time)
            active_run = bool(client["active"]) and run_number == 0
            duration = active_duration_seconds(client) if active_run else historical_duration_seconds(client, run_number)
            writer.writerow(
                {
                    "clientid": client["clientid"],
                    "clientname": client["clientname"],
                    "DatabaseName": f"LoadDb{int(client['clientid'])}",
                    "session_id": str(50_000 + row_index),
                    "status": "running" if active_run else "completed",
                    "SqlStatement": "NA",
                    "ClientHost": f"load-host-{int(client['clientid']) % 4}",
                    "ClientProgram": "EntityFramework",
                    "ClientProcessId": str(4000 + (row_index % 500)),
                    "SqlLoginUser": f"LoadDb{int(client['clientid'])}",
                    "DurationInSeconds": str(duration),
                    "start_time": iso_millis(event_time - timedelta(hours=5, minutes=30)),
                    "IST_Till_Minutes": minute_label(event_time),
                    "cpu_time": str(duration * 20),
                    "logical_reads": str(100_000 + row_index),
                    "writes": str(row_index % 37),
                    "ParentStatement": "NA",
                    "wait_type": "CXPACKET" if active_run else "",
                    "BlockingSessionId": "0",
                    "BlockingHostname": "",
                    "BlockingProgram": "",
                    "BlockingClientProcessId": "0",
                    "BlockingSql": "",
                }
            )


def write_formenginelog(path: Path, clients: list[dict[str, object]], base_time: datetime) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FORMENGINELOG_COLUMNS)
        writer.writeheader()
        for row_index in range(TOTAL_FORM_ROWS):
            client, run_number, event_time = event_context(row_index, clients, base_time)
            model_key = str((int(client["clientid"]) % 5) + 1)
            calculation_type = "Full" if run_number % 3 == 0 else "Incremental"
            calculation_key = "1" if calculation_type == "Full" else "0"
            refresh = "Yes" if run_number % 2 == 0 else "No"
            refresh_key = "1" if refresh == "Yes" else "0"
            writer.writerow(
                {
                    "clientid": client["clientid"],
                    "clientname": client["clientname"],
                    "formenginelogid": str(90_000 + row_index),
                    "created_date": iso_millis(event_time - timedelta(hours=5, minutes=30)),
                    "Operation": "ProcessForm",
                    "FileName": "Execute",
                    "FilePath": "/CEA/Administrator/Forms/Execute",
                    "full_result": json.dumps(
                        {
                            "Model": {
                                "Caption": f"Load Test Model {model_key}",
                                "IsSelected": False,
                                "Key": model_key,
                                "Name": f"Load Test Model {model_key}",
                                "Captions": None,
                                "Keys": None,
                                "Names": None,
                            },
                            "CalculationType": {
                                "Caption": calculation_type,
                                "IsSelected": False,
                                "Key": calculation_key,
                                "Name": calculation_type,
                                "Captions": None,
                                "Keys": None,
                                "Names": None,
                            },
                            "RefreshReportingLayer": {
                                "Caption": refresh,
                                "IsSelected": False,
                                "Key": refresh_key,
                                "Name": refresh,
                                "Captions": None,
                                "Keys": None,
                                "Names": None,
                            },
                        },
                        separators=(",", ":"),
                    ),
                    "CalculationType": calculation_type,
                    "IST_Till_Minutes": minute_label(event_time),
                }
            )


def write_cealog(path: Path, clients: list[dict[str, object]], base_time: datetime) -> None:
    total_cealog_rows = TOTAL_PROCESS_ROWS * CEALOG_ROWS_PER_RUN
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CEALOG_COLUMNS)
        writer.writeheader()
        for row_index in range(TOTAL_PROCESS_ROWS):
            client, run_number, event_time = event_context(row_index, clients, base_time)
            active_run = bool(client["active"]) and run_number == 0
            run_id = deterministic_uuid("run", row_index)
            execution_id = deterministic_uuid("execution", row_index)
            start_id = total_cealog_rows - (row_index * CEALOG_ROWS_PER_RUN)
            for offset, description in enumerate(cealog_descriptions(active_run)):
                created_time = event_time + cealog_offset(offset, client, run_number, active_run)
                writer.writerow(
                    {
                        "clientid": client["clientid"],
                        "clientname": client["clientname"],
                        "cealogid": "",
                        "id": str(start_id + offset),
                        "run_id": run_id,
                        "created_date": iso_millis(created_time - timedelta(hours=5, minutes=30)),
                        "description": description,
                        "step_no": f"{offset + 1}/{CEALOG_ROWS_PER_RUN}",
                        "execution_id": execution_id,
                        "level": "1" if offset in {0, 6} else "3",
                        "rowcount": str((int(client["clientid"]) + row_index + offset) % 2000),
                        "modelid": str((int(client["clientid"]) % 5) + 1),
                        "IST_Till_Minutes": minute_label(event_time),
                    }
                )


def event_context(
    row_index: int,
    clients: list[dict[str, object]],
    base_time: datetime,
) -> tuple[dict[str, object], int, datetime]:
    client = clients[row_index % len(clients)]
    run_number = row_index // len(clients)
    event_time = base_time - timedelta(minutes=(run_number * 6) + (row_index % len(clients)))
    return client, run_number, event_time


def cealog_descriptions(active_run: bool) -> list[str]:
    if active_run:
        return [
            "-START-",
            "before exec app_sp_prepare_model 1",
            "after exec app_sp_prepare_model 1",
            "before exec app_sp_calc_main 1",
            "running app_sp_calc_main batch 1",
            "running app_sp_calc_main batch 2",
            "running app_sp_calc_main batch 3",
        ]
    return [
        "-START-",
        "before exec app_sp_prepare_model 1",
        "after exec app_sp_prepare_model 1",
        "before exec app_sp_calc_main 1",
        "after exec app_sp_calc_main 1",
        "app_sp_reporting_layer completed",
        "-END-",
    ]


def cealog_offset(
    offset: int,
    client: dict[str, object],
    run_number: int,
    active_run: bool,
) -> timedelta:
    if active_run:
        return timedelta(minutes=[0, 1, 2, 3, 4, 5, 6][offset])
    calc_minutes = 8 + (int(client["clientid"]) % 5) + (run_number % 3)
    offsets = [0, 1, 2, 3, 3 + calc_minutes, 4 + calc_minutes, 5 + calc_minutes]
    return timedelta(minutes=offsets[offset])


def active_duration_seconds(client: dict[str, object]) -> int:
    return 3_600 if bool(client["long_running"]) else 900


def historical_duration_seconds(client: dict[str, object], run_number: int) -> int:
    return (10 + (int(client["clientid"]) % 5) + (run_number % 4)) * 60


def deterministic_uuid(prefix: str, value: int) -> str:
    prefix_code = "AAAAAAAA" if prefix == "process" else "BBBBBBBB" if prefix == "run" else "CCCCCCCC"
    suffix = f"{value:012X}"[-12:]
    return f"{prefix_code}-0000-4000-8000-{suffix}"


def minute_label(value: datetime) -> str:
    return value.strftime("%Y-%m-%d %H:%M")


def iso_millis(value: datetime) -> str:
    return value.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]


if __name__ == "__main__":
    main()
