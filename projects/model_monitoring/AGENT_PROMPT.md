# CEA Model Monitoring Agent Prompt

Use the prompt in `projects/model_monitoring/prompts.py` as the system prompt for the AI agent.

The prompt enforces these business rules:

- Scope every check by `CLIENTID` and `CLIENTNAME`.
- Treat `IST_TILL_MINUTES` as the datetime minute column.
- Sort `cea_processlog`, `cea_sessionquery`, and `cea_formenginelog` by `IST_TILL_MINUTES desc` per client before analysis.
- Sort `cea_cealog` by `ID desc` per client before analysis.
- Start from `cea_processlog` rows where `ISRUNNING = 1` and `WORKFLOWNAME = SendEmailAfterModelExecution`.
- If there is no same-client match in `cea_sessionquery` for the trigger minute or one minute later, classify the run as stuck or not progressing.
- If session query matches, parse `cea_formenginelog.FULL_RESULT` and extract:
  - `Model.Key`
  - `CalculationType.Caption`
  - `RefreshReportingLayer.Caption`
- Match `cea_cealog` by client in the trigger minute or one minute later and require `DESCRIPTION` to equal `-START-` after trimming whitespace.
- If no `cea_cealog` rows are available at all, classify as `LOG_DATA_UNAVAILABLE` because the ETL refresh may be temporarily deleting and reloading the table.
- If the current run has `-START-` but no later `-END-`, treat it as open and evaluate the currently open step.
- If the latest execution boundary is `before exec <step>` or `before <step>` with no later matching `after exec <step>`, `after <step>`, or `-END-`, use `<step>` as the current step.
- For an open current step, use `cea_sessionquery.DURATIONINSECONDS` when present as the active duration, then compare that duration with historical same-step durations.
- Compare current run step/substep timing against the previous 4 historical runs with the same model key, calculation type, and refresh reporting layer.
- Classify as long running when the current step/substep is more than 20 minutes above the historical average.
