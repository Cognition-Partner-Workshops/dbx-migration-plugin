# 03 — BTEQ control-flow script -> Lakeflow Job with run-if branches

Source: fixture `dml/scripts/bteq_daily_load.btq` (verbatim). Target: `converted/daily_load.job.yml` (bundle
resource) plus one SQL file per task under `converted/sql/`.

## Constructs exercised
- `.LOGON TDPROD/etl_svc_acct,;` -> job runs as the migration service principal (target-routing "Auth"); no
  credential in the artifact.
- `.SET WIDTH / .SET ERROROUT / .SET ERRORLEVEL ... SEVERITY` -> dropped (BTEQ session formatting/severity map;
  the severity semantics survive as which tasks are allowed to fail).
- `.IF ACTIVITYCOUNT = 0 THEN .GOTO NOSTAGING` -> `nostaging_warning` task with `run_if: AT_LEAST_ONE_FAILED` on
  `staging_check`, and **no** count-based branch. Step 1 is an aggregate `SEL COUNT(*) ...` without `GROUP BY`: it
  returns one row whatever the table holds, so `ACTIVITYCOUNT` is 1 and the source branch is reachable only when the
  request errors (skill §7 trap "ACTIVITYCOUNT after an aggregate"). A failed task is that condition on the target.
  Turning "0 staged rows" into a stop is what the script author probably intended, but it is a business-logic
  correction that needs a `.migration/06_decisions.md` row; the first pass is like-for-like (`01_staging_check.sql`
  header says where the guard would go).
- `.IF ERRORCODE <> 0 THEN .GOTO ERRORHANDLER` (repeated after every step) -> default `run_if: ALL_SUCCESS` on the
  main chain and one `errorhandler_mark_failed` task with `run_if: AT_LEAST_ONE_FAILED` over every step.
- `.LABEL` / `.GOTO` -> task keys and `depends_on` edges; `.QUIT 0/4/8` -> job run state + `BATCH_STATUS`; the
  numeric exit code is not reproduced (scheduler contract changes: record in the unit's runbook).
- `CREATE VOLATILE TABLE VT_BATCH ... ON COMMIT PRESERVE ROWS` -> one row in a unit-owned `ETL_JOB_RUN` table keyed
  by the job run; tasks have no shared session (skill §6 "Volatile/global temp tables"). The session identity the
  volatile table had becomes the job-level parameter `run_id` (default `{{job.run_id}}`), pushed to every task and read
  in each SQL file as `:run_id`; `02_new_batch_id` writes `(RUN_ID, BATCH_ID, BATCH_DATE, BATCH_START_TS)` for it and
  tasks 03-07/91 read `WHERE RUN_ID = :run_id` -- never "the open batch", so nothing this job does depends on what
  other runs or other processes have in flight. `ETL_BATCH_CONTROL` keeps its legacy contract exactly: the job only
  `INSERT`s the final `COMPLETED` (07) or `FAILED` (91) row from its own `ETL_JOB_RUN` row, as the BTEQ did from
  `VT_BATCH`, and never updates or closes rows it did not write (an earlier revision failed every `STARTED` row in
  the table, which would have hit any other job sharing it). A run that dies before 07/91 leaves an `ETL_JOB_RUN` row
  and no control row -- the BTEQ's footprint when its session died -- and is invisible to later runs. Allocation stays
  `MAX(BATCH_ID) + 1`, taken over both tables so a died run's id (already stamped on `FACT_TRANSACTION.ETL_BATCH_ID`)
  is never reused; `max_concurrent_runs: 1` + `queue.enabled` serialise this job's allocations. A repaired task of the
  same job run keeps its `BATCH_ID` (`NOT EXISTS` on `RUN_ID`); a new run allocates a new one.
- `CALL proc(..., out1, out2, rc)` -> `CALL` inside a compound with `DECLARE`d OUT variables; `IF rc <> 0 THEN SIGNAL`.
- `EXEC macro(...)` -> `CALL` of the procedure the macro became (example 05).
- `.EXPORT REPORT FILE=... / .EXPORT RESET` -> report rows written to a Delta table; file export (if still
  needed) is a downstream consumer task.
- `TRIM(x (FORMAT 'YYYY-MM-DD'))`, `TRIM(ERRORCODE (FORMAT '-9(5)'))` -> explicit `CAST(... AS STRING)`.
- `MAX(BATCH_ID) + 1` -> `GREATEST(COALESCE(MAX ctrl, 0), COALESCE(MAX run, 0)) + 1` (NULL on an empty control table
  on both engines; made explicit, and widened to the run table as above).

## Recon tier that catches a wrong conversion
- Missing error branch (a step fails but `ETL_BATCH_CONTROL` never gets `FAILED`): **Tier 1** row count on
  `ETL_BATCH_CONTROL` per `BATCH_DATE` grouped by `BATCH_STATUS` (a `FAILED` row short); `ETL_JOB_RUN` rows with no
  control row are the died-run audit trail.
- Batch state keyed on status instead of on the run ("the open batch" lookup): a second process with its own open row
  in `ETL_BATCH_CONTROL` gets closed as `FAILED` by this job, or tasks 03-06 fail on a multi-row scalar subquery --
  **Tier 1** on `ETL_BATCH_CONTROL` grouped by `BATCH_STATUS` for the *other* process's rows (its `FAILED` count up,
  `COMPLETED` down against legacy), plus this job's `FACT_TRANSACTION.ETL_BATCH_ID` missing for the day.
- `run_id` not passed / `:run_id` unresolved: every task after 02 fails on a NULL batch id -> **Tier 1** on
  `FACT_TRANSACTION` for the batch, and `ETL_JOB_RUN` gets a row whose `RUN_ID` is the literal placeholder.
- Batch id reused after a died run (allocation taken over `ETL_BATCH_CONTROL` only): two runs share an `ETL_BATCH_ID`
  on `FACT_TRANSACTION` -> **Tier 1** row count per `ETL_BATCH_ID` and **Tier 2** `sum(amount)` per batch both high.
- Note on the fixture: `STG_TRANSACTIONS`, `ETL_BATCH_CONTROL`, `ETL_LOG`, `RPT_*` have no DDL under `ddl/`; they are
  reached only through this script and the procedures, so lineage marks them INFERRED (skill §2) and the census must
  pull their DDL from `DBC.TablesV`/`SHOW TABLE` on a live engine (PR "Not verified live").
- `nostaging_warning` wired to `ALL_SUCCESS` instead of `AT_LEAST_ONE_FAILED` (branch inverted): **Tier 1** row
  count on `ETL_LOG` where `LOG_LEVEL = 'WARN'` on a day when `staging_check` succeeded.
- Empty-staging parity case: on a day with 0 rows in `STG_TRANSACTIONS` the source runs every step and writes a
  `COMPLETED` batch (0 loaded rows) and no `WARN` row. A converted `staging_check` that SIGNALs on `staged = 0`
  shows as **Tier 1** on `ETL_BATCH_CONTROL` (`COMPLETED` count 0 vs 1 for that `BATCH_DATE`) and **Tier 1** excess
  on `ETL_LOG` `WARN` rows. This case must be in the shadow-run calendar, not just the busy days.
- Emulating `VT_BATCH` with a temporary table (scoped to one task) -> later tasks fail to find the batch: **Tier 1**
  on `FACT_TRANSACTION` for the batch (`ETL_BATCH_ID` never populated).
- Writing a `COMPLETED` row twice (task 07 repaired without its `NOT EXISTS`): **Tier 1** row-count excess on
  `ETL_BATCH_CONTROL` (2 vs 1 per batch).
- Report table (`RPT_DAILY_RECONCILIATION`) is a D4 derived consumer output: **Tier 2** on `STAGED_ROWS`,
  `LOADED_ROWS`, `ERROR_ROWS` against the legacy report file parsed once during shadow-run.

## Citations
- `depends_on`, `run_if` values: `databricks-jobs` `SKILL.md` "Core Concepts / Multi-Task Workflows".
- `sql_task.file`: `databricks-jobs` `references/task-types.md` "SQL Task / Run SQL File".
- `timeout_seconds`, `max_retries`: `databricks-jobs` `references/notifications-monitoring.md` "Timeout
  Configuration", "Retry Configuration"; `max_concurrent_runs`, `queue.enabled`: same file, "Run Queue Settings".
- `${var.*}` substitution: `databricks-dabs` `references/bundle-structure.md` (variables table).
- `BEGIN ... END`, `DECLARE`, `SET var = (SELECT ...)`, `IF`, `SIGNAL SQLSTATE`, `CALL` with OUT variables:
  `databricks-dbsql` `references/sql-scripting.md` "Compound Statements", "Variable Assignment", "Control Flow",
  "SIGNAL and RESIGNAL", "CALL (Invoke a Procedure)".
- Temporary tables are session-scoped: `databricks-dbsql` `references/materialized-views-pipes.md`
  "Temporary Tables and Temporary Views".
- Job-level `parameters` pushed to every task: `databricks-jobs` `SKILL.md` "Job Parameters"; `{{job.run_id}}` ("the
  unique identifier assigned to the job run"): docs.databricks.com/aws/en/jobs/dynamic-value-references; SQL tasks
  read parameters with the named-parameter syntax `:name`: docs.databricks.com/aws/en/jobs/parameter-use ("Use named
  parameters in SQL" and the SQL row of "Details by task type").

## Not verified live
- BTEQ `ACTIVITYCOUNT` after a *failed* request is 0 (which is what makes `.GOTO NOSTAGING` reachable on an error);
  the conversion depends only on the aggregate-returns-one-row half, which holds by SQL semantics.
- That a `sql_task` running a `.sql` file accepts a multi-statement `BEGIN ... END` compound (the official skill shows
  the file form but not a scripting body inside it). If it does not, each file becomes a `CALL` of a small procedure.
- Actual job-run behaviour of `AT_LEAST_ONE_FAILED` fan-in when an upstream task was skipped rather than failed, and
  whether it runs at all on job cancel / `timeout_seconds` expiry (the design assumes it may not: a died run leaves an
  `ETL_JOB_RUN` row without a control row and nothing depends on closing it).
- That `:run_id` resolves inside a `BEGIN ... END` compound in a `sql_task` file (the docs show it in a plain
  `SELECT`), and that a *repaired* run resolves `{{job.run_id}}` to the original run's id (the `NOT EXISTS` in 02 and
  07/91 makes either answer safe: same id -> same batch resumed; new id -> new batch, old one stays without a control
  row).
