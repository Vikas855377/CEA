# CEA Agentic

CEA Agentic is a FastAPI-powered operations monitoring console with a responsive web UI.

The current project is the **Model Monitoring Agent**. It monitors active CEA model executions from Xactly Connect through JDBC and classifies each active run as started, stuck, running normally, long running, or needing investigation.

## What The Model Monitoring Agent Does

The agent watches the `SendEmailAfterModelExecution` workflow.

It reads these Xactly JDBC `delta` schema tables:

```text
delta.cea_processlog
delta.cea_sessionquery
delta.cea_formenginelog
delta.cea_cealog
```

For every active processlog trigger, it checks:

- `cea_processlog` has `WORKFLOWNAME = SendEmailAfterModelExecution` and `ISRUNNING = true`.
- `cea_sessionquery` has a same-client session row in the trigger minute or one minute later.
- `cea_formenginelog` has same-client `FULL_RESULT` metadata in the trigger minute or one minute later.
- `cea_cealog` has a same-client start marker in the trigger minute or one minute later where `DESCRIPTION` equals `-START-`.
- The current execution has an open step if `-START-` exists but no later `-END-` exists.
- The current open step is detected from unmatched `before exec <step>` or `before <step>` log boundaries.
- Current step duration is compared with previous 4 same-type historical runs.

The same-type historical comparison uses:

- `CLIENTID`
- `CLIENTNAME`
- `FULL_RESULT.Model.Key`
- `FULL_RESULT.CalculationType.Key`
- `FULL_RESULT.RefreshReportingLayer.Key`

## Status Values

The app returns one of these statuses:

```text
NO_RUNNING_MODEL
STUCK_NOT_PROGRESSING
TRIGGERED_NOT_STARTED
LOG_DATA_UNAVAILABLE
DATA_REFRESH_IN_PROGRESS
RUNNING_NORMAL
LONG_RUNNING
NEEDS_INVESTIGATION
```

Status meaning:

- `NO_RUNNING_MODEL`: no active `SendEmailAfterModelExecution` rows were found.
- `STUCK_NOT_PROGRESSING`: processlog has a running trigger, but no matching sessionquery row exists.
- `TRIGGERED_NOT_STARTED`: session and form rows exist, but no `cea_cealog.DESCRIPTION = -START-` exists in the trigger minute or one minute later.
- `LOG_DATA_UNAVAILABLE`: session and form rows exist, but no `cea_cealog` rows were loaded, usually because the ETL refresh is temporarily deleting and reloading the table.
- `DATA_REFRESH_IN_PROGRESS`: at least one required source table is completely empty; monitoring calculations pause until all four tables contain data again.
- `RUNNING_NORMAL`: the run has started and the current open step is within the historical threshold.
- `LONG_RUNNING`: the current open step is more than 20 minutes above the previous same-type historical average.
- `NEEDS_INVESTIGATION`: required metadata is missing or could not be parsed.

## Important Logic Details

The start marker is strict:

```text
DESCRIPTION must equal -START- after trimming whitespace.
```

These do not count as the model start marker:

```text
starting [app_sp_cam_execute_model]
started by: user
start: app_sp_cam_update_transaction_model
```

Open-step detection:

```text
[app_sp_cam_execute_model] before exec app_sp_cam_generate_reporting_layer 1
```

with no later matching:

```text
[app_sp_cam_execute_model] after exec app_sp_cam_generate_reporting_layer 1
```

or no later:

```text
-END-
```

means the current open step is:

```text
app_sp_cam_generate_reporting_layer
```

When `cea_sessionquery.DURATIONINSECONDS` exists, that duration is used for the active open step.

## Xactly JDBC Data Loading

For active model monitoring, `project.py` now reads directly from the Xactly JDBC `delta` schema:

- Active `delta.cea_processlog` trigger rows for `SendEmailAfterModelExecution`.
- Same-client rows from `delta.cea_sessionquery`.
- Same-client rows from `delta.cea_formenginelog`.
- Same-client rows from `delta.cea_cealog`.

The selected-client Xactly JDBC table queries run in parallel to reduce page wait time.

This is still app-side monitoring. Xactly JDBC is used only as the data source; the classification logic runs in Python.

## Project Structure

```text
cea_agentic/
├── app.py
├── README.md
├── requirements.txt
├── core/
│   ├── config.py
│   ├── db.py
│   └── llm.py
└── projects/
    └── model_monitoring/
        ├── AGENT_PROMPT.md
        ├── project.py
        ├── prompts.py
        └── agents/
            └── model_monitoring_agent.py
```

Key files:

- `app.py`: FastAPI endpoints, monitoring cache, and application entry point.
- `ui/`: Responsive monitoring console frontend.
- `core/config.py`: environment variable loading.
- `core/db.py`: Snowflake connection wrapper.
- `core/xactly_jdbc.py`: Xactly JDBC connection/query wrapper.
- `core/llm.py`: OpenAI or Azure OpenAI client wrapper.
- `projects/model_monitoring/project.py`: Xactly JDBC loading and project orchestration.
- `projects/model_monitoring/agents/model_monitoring_agent.py`: deterministic monitoring logic and optional LLM fallback.
- `projects/model_monitoring/prompts.py`: LLM prompt used if deterministic logic cannot decide.
- `projects/model_monitoring/AGENT_PROMPT.md`: readable prompt documentation.
- `projects/ProcessRunAutomation/`: independent concurrent Obero ProcessApp
  automation project with isolated per-client browser sessions.

## Process Run Automation

Configure shared Xactly credentials and per-client process IDs in the root
`.env`, then validate or run the scheduler:

```bash
python -m projects.ProcessRunAutomation.process_scheduler --validate
python -m projects.ProcessRunAutomation.process_scheduler --once
python -m projects.ProcessRunAutomation.process_scheduler
```

Client URLs are defined in `projects/ProcessRunAutomation/clients.json`. All enabled
clients authenticate and execute concurrently. Runtime session state and logs
are kept under `data/model_monitoring/process_scheduler/`.

## Setup

Create and activate a virtual environment:

```bash
cd /Users/vr/Documents/Automation/cea_agentic
python3 -m venv venv
source venv/bin/activate
```

Install dependencies:

```bash
pip install -r requirements.txt
```

## Environment Variables

Create a `.env` file in the repo root.

Snowflake values are no longer used for model monitoring. Xactly JDBC is the active source for the `delta` tables.

Snowflake snapshot validation is optional and disabled by default. When enabled,
the app compares Xactly table counts with lightweight checkpoints in the
`CEA_MONITORING_SNAPSHOT_COUNTS` Snowflake control table. Empty or reduced
incoming tables pause monitoring; accepted counts are updated without copying
the source tables.

Snowflake connection and snapshot toggle:

```env
SNOWFLAKE_ACCOUNT=
SNOWFLAKE_USER=
SNOWFLAKE_PASSWORD=
SNOWFLAKE_WAREHOUSE=CS_BOT_WH
SNOWFLAKE_DATABASE=CUSTOMER_SUPPORT_BOT_LOGS
SNOWFLAKE_SCHEMA=CHAT_DATA
SNOWFLAKE_ROLE=
MODEL_MONITORING_SNOWFLAKE_SNAPSHOT_ENABLED=false
```

OpenAI:

```env
OPENAI_PROVIDER=openai
OPENAI_API_KEY=
OPENAI_MODEL=gpt-4.1
```

Azure OpenAI:

```env
OPENAI_PROVIDER=azure
AZURE_OPENAI_API_KEY=
AZURE_OPENAI_ENDPOINT=
AZURE_OPENAI_API_VERSION=2024-10-21
AZURE_OPENAI_DEPLOYMENT=
```

Optional row limit:

```env
MODEL_MONITORING_LIMIT_PER_TABLE=5000
MODEL_MONITORING_CEALOG_LIMIT=1000
MODEL_MONITORING_CEALOG_ROWS_PER_RUN=1000
MODEL_MONITORING_HISTORICAL_BOUNDARY_ROWS_PER_RUN=100
MODEL_MONITORING_CEALOG_START_SCAN_LIMIT=1000
MODEL_MONITORING_CEALOG_END_SCAN_LIMIT=1000
MODEL_MONITORING_CLIENT_OPTION_LIMIT=1000
MODEL_MONITORING_CLIENT_OPTION_CACHE_TTL=300
MODEL_MONITORING_ACTIVE_CLIENTS_ONLY=true
MODEL_MONITORING_RUN_CACHE_TTL=60
```

Xactly JDBC:

```env
XACTLY_JDBC_URL=xactly://secure3.xactlycorp.com:443?useSSL=
XACTLY_JDBC_POD=secure3
XACTLY_JDBC_USER=
XACTLY_JDBC_PASSWORD=
XACTLY_JDBC_JAR_PATH=drivers/xjdbc-2.2.3-RELEASE-jar-with-dependencies.jar
XACTLY_JDBC_DRIVER_CLASS=com.xactly.connect.jdbc.Driver
XACTLY_JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
XACTLY_JDBC_MODE=subprocess
XACTLY_JDBC_SERVER=true
XACTLY_JDBC_POOL_SIZE=3
```

## Run

From the repo root:

```bash
python app.py
```

`app.py` checks `requirements.txt` on startup. On a clean Python host it installs
missing Python packages and the Playwright Chromium browser, restarts itself,
and launches the web application. Set `CEA_AUTO_INSTALL_DEPENDENCIES=false` to
disable this behavior in immutable or pre-built environments.

The host must still provide Java 17, environment credentials, and outbound
network access. The licensed Xactly JDBC driver is included under `drivers/`
and configured by `XACTLY_JDBC_JAR_PATH`.
For a completely reproducible host image, use the supplied `Dockerfile`; it also
installs Java and Chromium's operating-system libraries.

The app usually opens at:

```text
http://localhost:8080
```

## Test Xactly JDBC

Install Java and Python dependencies first:

```bash
pip install -r requirements.txt
```

Then run:

```bash
python scripts/test_xactly_jdbc.py
```

Or pass a specific query:

```bash
python scripts/test_xactly_jdbc.py --sql "select 1" --max-rows 20
```

## Monitoring Console

The console includes ETL operations and application-run views, live status cards,
manual refresh, responsive layouts, and an evidence drawer for each model run.

The result screen shows:

- agent JSON result
- Xactly JDBC load time
- agent execution time
- total run time
- row counts sent to the agent
- form metadata rows sent to the agent

## Expected Example Results

Triggered but not started:

```json
{
  "overall_status": "TRIGGERED_NOT_STARTED",
  "current_step": null,
  "long_running_step": null
}
```

Running normally:

```json
{
  "overall_status": "RUNNING_NORMAL",
  "current_step": "appg_cam_generate_schedule_1",
  "long_running_step": null,
  "minutes_over_average": 0
}
```

Long running:

```json
{
  "overall_status": "LONG_RUNNING",
  "current_step": "app_sp_cam_generate_reporting_layer",
  "long_running_step": "app_sp_cam_generate_reporting_layer",
  "minutes_over_average": 51.2
}
```

## Local CSV Testing

Set `MODEL_MONITORING_USE_CSV=true` to make the application read local CSV files instead of Xactly JDBC. Set it to `false` to switch back to the Xactly tables.

```dotenv
MODEL_MONITORING_USE_CSV=true
MODEL_MONITORING_CSV_DIR=data/model_monitoring/load_test
MODEL_MONITORING_CEA_PROCESSLOG_CSV=data/model_monitoring/load_test/processlog.csv
MODEL_MONITORING_CEA_SESSIONQUERY_CSV=data/model_monitoring/load_test/sessionquery.csv
MODEL_MONITORING_CEA_FORMENGINELOG_CSV=data/model_monitoring/load_test/formenginelog.csv
MODEL_MONITORING_CEA_CEALOG_CSV=data/model_monitoring/load_test/cealog.csv
```

Generate a 1,000,000-row local fixture with 20 clients, including active and inactive clients:

```bash
python scripts/generate_model_monitoring_load_csv.py
```

The project class can also load CSV files directly:

```python
from projects.model_monitoring.project import ModelMonitoringProject

project = ModelMonitoringProject()
tables = project.load_from_csvs({
    "cea_processlog": "/path/to/processlog.csv",
    "cea_sessionquery": "/path/to/sessionquery.csv",
    "cea_formenginelog": "/path/to/formenginelog.csv",
    "cea_cealog": "/path/to/cealog.csv",
})
run = project.run(tables=tables)
print(run.result)
```

This is useful for debugging exact scenarios without hitting Xactly JDBC.

## Troubleshooting

If the app says `TRIGGERED_NOT_STARTED` but you believe the run started, check whether the payload includes a row for the trigger minute or one minute later:

```text
CLIENTID = active CLIENTID
CLIENTNAME = active CLIENTNAME
IST_TILL_MINUTES = trigger minute or trigger minute + 1
DESCRIPTION = -START-
```

If that row is missing from the app payload, the issue is data loading, not classification.

If the app says `RUNNING_NORMAL` for a test that should be long running, inspect the evidence fields:

- `trigger_time`
- `Current open step`
- `Current session duration`
- `Historical average`
- `Current duration is ... over the historical average`

If a Xactly JDBC query fails, the app wraps the error with the table name, for example:

```text
Failed to load cea_processlog: ...
```

## Development Checks

Compile the main files:

```bash
python3 -c "compile(open('app.py').read(), 'app.py', 'exec'); compile(open('projects/model_monitoring/project.py').read(), 'projects/model_monitoring/project.py', 'exec'); compile(open('projects/model_monitoring/agents/model_monitoring_agent.py').read(), 'projects/model_monitoring/agents/model_monitoring_agent.py', 'exec'); compile(open('projects/model_monitoring/prompts.py').read(), 'projects/model_monitoring/prompts.py', 'exec')"
```

Run the app:

```bash
uvicorn app:app --reload --port 8080
```
