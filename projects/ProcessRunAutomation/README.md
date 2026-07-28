# Process Run Automation

This agent authenticates to every enabled Obero client with shared Xactly
credentials, runs each configured ProcessApp workflow concurrently, isolates
sessions per client, and repeats on a fixed interval.

## Configuration

Client URLs live in `clients.json`. Process IDs and shared credentials live in
the repository root `.env`:

```env
XACTLY_USERNAME=
XACTLY_PASSWORD=
CEA_SUPPORT_FS_ID=143
WORKIVA_SANDBOX_FS_ID=202068
KNOWBE4_TEMP_FS_ID=50991
CEA_RUN_INTERVAL_SECONDS=300
CEA_CLIENT_TIMEOUT_SECONDS=240
CEA_REFRESH_COORDINATION_ENABLED=false
CEA_REFRESH_BATCH_TIMEOUT_SECONDS=900
CEA_REFRESH_BATCH_POLL_SECONDS=1
CEA_ACTIVE_BATCH_RETRY_SECONDS=10
CEA_CLIENT_LAUNCH_WAVE_SIZE=10
CEA_LAUNCH_WAVE_GAP_SECONDS=1
CEA_BROWSER_MEMORY_ESTIMATE_MB=350
CEA_HOST_MEMORY_RESERVE_MB=2048
OBERO_SESSION_CHECK_TIMEOUT_MS=12000
OBERO_APP_READY_TIMEOUT_MS=6000
XACTLY_APPROVAL_WAIT_MS=20000
```

Each launch wave gives every client a dedicated worker and releases the group
through one barrier. Deployments with up to 10 clients remain one simultaneous
wave. Larger deployments use groups of 10 by default, preventing 50 Chromium
instances from exhausting the host. Adjust `CEA_CLIENT_LAUNCH_WAVE_SIZE` for
the host's measured CPU and memory capacity. Saved browser sessions are reused
and nonessential images, media, and fonts are skipped to reduce startup time.
The scheduler also reduces the effective wave size automatically when physical
RAM cannot safely support the configured maximum, reserving 2 GB for the API,
Java/JDBC, and the operating system. Session directories use owner-only access
and each saved authentication state is written with `0600` permissions.

Each enabled client in `clients.json` must also define the exact monitoring
identity written by its XML process:

```json
{
  "monitoring_client_id": 1,
  "monitoring_client_name": "Test"
}
```

## Refresh batch coordination

When `CEA_REFRESH_COORDINATION_ENABLED=true`, the scheduler creates an
`IN_PROGRESS` row in `delta.cea_refresh_batch` and one `PENDING` row per client
in `delta.cea_refresh_client` before launching any process. It then waits for
the XML processes to mark every client `COMPLETED`. Failed launchers are marked
`FAILED`, and batches that do not finish within
`CEA_REFRESH_BATCH_TIMEOUT_SECONDS` are marked `TIMED_OUT`.

Enable this only after every client XML uses its current control row instead of
the temporary `TEST-BATCH-001` value. The XML commands should identify the row
with `CLIENTID = <client id>` and the current status:

```sql
-- Mark Refresh Started
WHERE CLIENTID = 1 AND STATUS = 'PENDING'

-- Record Loaded Counts / Validate And Complete Client
WHERE CLIENTID = 1 AND STATUS = 'IN_PROGRESS'
```

Overlapping batches are prevented, so only one pending/in-progress row can
exist for a client. When coordination is enabled, Application Monitoring uses
the explicit Xactly batch status and bypasses the optional Snowflake
high-water-count guard.

## Commands

```sh
python -m projects.ProcessRunAutomation.process_scheduler --validate
python -m projects.ProcessRunAutomation.process_scheduler --once
python -m projects.ProcessRunAutomation.process_scheduler
```

Install the Playwright browser once:

```sh
playwright install chromium
```

Runtime sessions are stored below `data/model_monitoring/process_scheduler/sessions/`.
Rotating logs for this project are isolated in
`projects/ProcessRunAutomation/logs/` and generated log files are not committed.
