# runner-nightly-batch: `run_nightly_batch.sh`

Source: fixture `batch/run_nightly_batch.sh` (bash + `isql`, cron `0 2 * * *`). Converted: `converted.yml` (Lakeflow Jobs resource, Databricks Asset Bundle shape per `databricks-jobs`).

## Constructs exercised
- cron scheduler edge -> `schedule.quartz_cron_expression` + `timezone_id` + `pause_status: PAUSED` (SKILL §2b "Scheduler edges", §6 "isql runner"; `databricks-jobs` references/triggers-schedules.md). cron has no timezone: the host's zone is recorded in `.migration/00_context.md`.
- Three sequential `isql` heredoc steps with `if ! isql ...; then exit 1` -> three `sql_task` tasks chained with `depends_on` + `run_if: ALL_SUCCESS` (`databricks-jobs` references/task-types.md, SKILL.md `run_if` values).
- `echo "FAILED ..."` + log tail -> `run_if: AT_LEAST_ONE_FAILED` audit task and `email_notifications.on_failure` (`databricks-jobs` references/notifications-monitoring.md).
- Shell variables `${SYBASE_SERVER:-LOAN_PROD}`, `${SYBASE_DB}` -> job `parameters` (`catalog`, `schema`) consumed through `IDENTIFIER(:catalog || ...)` (SKILL §2b "Parameter files / shell variables"; `[docs:identifier]`).
- `-P ${SYBASE_PWD}` -> nothing: the job runs as its `run_as` principal; the secret name is recorded, never the value (AGENTS.md).
- `-S LOAN_PROD` resolves through `config/interfaces` to a host:port (FACT lineage for the estate-map server node; SKILL §1 "ASE interfaces file").
- `EXEC ... GETDATE()` evaluated per `isql` session -> `current_timestamp()` per task; documented as a parity choice.

## Lineage (FACT)
Calls: `sp_nightly_accrual`, `sp_apply_late_fees`, `sp_end_of_day_reconciliation` (each a separate procedure unit). Runner unit = this graph + the three procedure units as `depends_on` in the wave manifest; it is the last unit of the wave.

## Recon tier that catches a wrong conversion
- **Tier 1** on `audit_trail` for one run date: exactly the rows the three procedures write (`LATE_FEE`, `BATCH_START`/accrual rows, recon rows) and no `BATCH_FAILED` row. A wrong `run_if` (e.g. `ALL_DONE` on step 2) lets late fees run after a failed accrual: the source never did that, so the audit row sequence differs.
- **Tier 4 (replay)**: the end-to-end nightly run, compared procedure-by-procedure with the individual units' Tier 2 checks; the runner itself computes nothing, so its own signal is ordering and presence.
- Schedule correctness (02:00 in the right zone, one run per day, no overlap) is not a recon tier: it is verified by `max_concurrent_runs: 1` and the first three unpaused runs after cutover.

## INFERRED edges
The cron line lives in a comment in the script header; the live crontab (`crontab -l` on the batch host) was not read. Marked INFERRED until the operations team confirms it.

## Lakebridge
`mssql` flag rejects shell files entirely (SKILL §10 "SQL Agent job scripts / isql runners"). Hand-converted.
