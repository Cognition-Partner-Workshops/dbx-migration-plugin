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
  other runs or other processes have in flight. The allocated id is also *reserved* in the shared `ETL_BATCH_CONTROL`
  at once: `02_new_batch_id` inserts a `STARTED` row for it in the same task, so every other `MAX(BATCH_ID) + 1`
  allocator sharing the table sees the id as taken for the whole run (the BTEQ only inserted at the end, so its id was
  invisible to other writers for the entire script; a private reservation in `ETL_JOB_RUN` alone would have been
  invisible too). Tasks 07/91 then close *this run's own row* with a `MERGE` matched on the `BATCH_ID` *and*
  `START_TS` from `ETL_JOB_RUN` (the pair 02 wrote) -- never on status alone, never on the id alone -- and the orphan
  step at the top of 02 closes as `FAILED` only `STARTED` rows whose `(BATCH_ID, START_TS)` has an `ETL_JOB_RUN` row
  with a different `RUN_ID`, i.e. died runs of *this job* (the job is serialised, so any other `RUN_ID` is no longer
  executing); `STARTED` rows of other processes are never touched (an earlier revision failed every `STARTED` row). Consumer-contract delta: legacy rows were only ever
  `COMPLETED`/`FAILED`; readers of the table now also see `STARTED` for an in-flight run -- record in the unit's
  runbook. Allocation stays `MAX(BATCH_ID) + 1`, taken over both tables so a run that died between the two inserts is
  still never reused; `max_concurrent_runs: 1` + `queue.enabled` serialise this job's allocations. Residual race:
  another writer computing `MAX + 1` between the two `INSERT`s of task 02 takes the same id -- the legacy exposure
  narrowed from a whole script to one task. Closing it needs an allocator every writer of the table adopts (identity
  column / sequence table), which is a contract change for those writers: **decision item**, not done here. What 02
  does instead is *fail loudly when it lost*: after the reservation it `SIGNAL`s unless the run's `(BATCH_ID,
  START_TS)` is present in `ETL_BATCH_CONTROL`, so a foreign row holding the id stops the run before any fact row is
  loaded under a shared id; 91 then records `FAILED` for the run without touching the foreign row. A foreign writer
  appending the same id *after* this run's reservation is that writer's allocation to check; recon's duplicate-id
  signature below is the net. A repaired task of the same job run keeps its `BATCH_ID` (`NOT EXISTS` on `RUN_ID`) and
  passes the guard on its own row; a new run allocates a new one.
- `CALL proc(..., out1, out2, rc)` -> `CALL` inside a compound with `DECLARE`d OUT variables; `IF rc <> 0 THEN SIGNAL`.
- `EXEC macro(...)` -> `CALL` of the procedure the macro became (example 05).
- `.EXPORT REPORT FILE=... / .EXPORT RESET` -> report rows written to a Delta table; file export (if still
  needed) is a downstream consumer task.
- `TRIM(x (FORMAT 'YYYY-MM-DD'))`, `TRIM(ERRORCODE (FORMAT '-9(5)'))` -> explicit `CAST(... AS STRING)`.
- `MAX(BATCH_ID) + 1` -> `GREATEST(COALESCE(MAX ctrl, 0), COALESCE(MAX run, 0)) + 1` (NULL on an empty control table
  on both engines; made explicit, and widened to the run table as above).

## Recon tier that catches a wrong conversion
- Missing error branch (a step fails but `ETL_BATCH_CONTROL` never gets `FAILED`): **Tier 1** row count on
  `ETL_BATCH_CONTROL` per `BATCH_DATE` grouped by `BATCH_STATUS` (a `FAILED` row short, a `STARTED` row left open
  until the next run's orphan step); `ETL_LOG` WARN rows `Orphaned STARTED batch of run ...` are the died-run audit
  trail.
- Reservation missing from the shared table (id held only in `ETL_JOB_RUN` until the end of the run): another
  `MAX + 1` writer takes the same `BATCH_ID` during the run -> **Tier 1** row count per `BATCH_ID` on
  `ETL_BATCH_CONTROL` (> 1) and on `FACT_TRANSACTION.ETL_BATCH_ID` (foreign rows mixed into this batch), **Tier 2**
  `sum(amount)` per batch high. The shadow-run should include one concurrent foreign allocation while this job is
  between tasks 02 and 07.
- Batch state keyed on status instead of on the run ("the open batch" lookup): a second process with its own open row
  in `ETL_BATCH_CONTROL` gets closed as `FAILED` by this job, or tasks 03-06 fail on a multi-row scalar subquery --
  **Tier 1** on `ETL_BATCH_CONTROL` grouped by `BATCH_STATUS` for the *other* process's rows (its `FAILED` count up,
  `COMPLETED` down against legacy), plus this job's `FACT_TRANSACTION.ETL_BATCH_ID` missing for the day.
- `run_id` not passed / `:run_id` unresolved: every task after 02 fails on a NULL batch id -> **Tier 1** on
  `FACT_TRANSACTION` for the batch, and `ETL_JOB_RUN` gets a row whose `RUN_ID` is the literal placeholder.
- Batch id reused after a run died between the two inserts of task 02 (allocation taken over `ETL_BATCH_CONTROL`
  only): two runs share an `ETL_BATCH_ID` on `FACT_TRANSACTION` -> **Tier 1** row count per `ETL_BATCH_ID` and
  **Tier 2** `sum(amount)` per batch both high.
- Orphan step keyed on status alone (closing every `STARTED` row) instead of on this job's `ETL_JOB_RUN` rows: same
  signature as the "keyed on status" row above, on the other process's rows.
- 07/91/orphan step matched on `BATCH_ID` alone (no `START_TS`): after a lost allocation race the job closes the
  *foreign* row that holds the id -> same "other process's rows" signature; and without the post-reservation guard the
  run loads under the shared id -> the "reservation missing" signature. The shadow-run's concurrent foreign allocation
  should be placed once between the two `INSERT`s of 02 (expected: this run `FAILED` at 02, foreign row untouched, no
  fact rows for the run) as well as once between 02 and 07.
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
- Writing a `COMPLETED` row twice (task 07 as a plain `INSERT` instead of the `MERGE` on the reservation row): **Tier
  1** row-count excess on `ETL_BATCH_CONTROL` (2 vs 1 per batch).
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
- `MERGE INTO ... WHEN MATCHED AND ... THEN UPDATE ... WHEN NOT MATCHED THEN INSERT`:
  docs.databricks.com/aws/en/sql/language-manual/delta-merge-into (syntax, `matched_condition`); `databricks-dbsql`
  `references/best-practices.md` "SCD Type 2 with MERGE" (shape).
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
  whether it runs at all on job cancel / `timeout_seconds` expiry (the design assumes it may not: a died run leaves its
  `STARTED` reservation open and the next run's orphan step in 02 closes it as `FAILED`).
- That one `sql_task` file may hold plain statements followed by a `BEGIN ... END` compound (02: DDL, DML, then the
  post-reservation `IF ... SIGNAL` guard). If not, the guard becomes its own task between `new_batch_id` and
  `customer_scd2`, or the whole of 02 becomes a compound.
- That `:run_id` resolves inside a `BEGIN ... END` compound in a `sql_task` file (the docs show it in a plain
  `SELECT`), and that a *repaired* run resolves `{{job.run_id}}` to the original run's id (the `NOT EXISTS` in 02 and
  the `MERGE` in 07/91 make either answer safe: same id -> same batch resumed; new id -> new batch, and the old one is
  closed as `FAILED` by the orphan step).
