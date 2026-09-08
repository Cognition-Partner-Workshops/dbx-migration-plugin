-- Step 2 + CREATE VOLATILE TABLE VT_BATCH ... ON COMMIT PRESERVE ROWS.
-- A volatile table lives for one BTEQ session; a job has no shared session across tasks, so VT_BATCH becomes one row
-- in ETL_JOB_RUN keyed by the job run: the job-level parameter run_id defaults to {{job.run_id}} ("the unique
-- identifier assigned to the job run", docs.databricks.com/aws/en/jobs/dynamic-value-references), job parameters are
-- pushed to every task (databricks-jobs SKILL.md "Job Parameters"), and a SQL task reads them with the named-parameter
-- syntax :run_id (docs.databricks.com/aws/en/jobs/parameter-use, "SQL: use named parameters").
-- (A temporary table would be session-scoped to this task only: databricks-dbsql
--  references/materialized-views-pipes.md "Temporary Tables and Temporary Views".)
--
-- Two tables, two roles:
--   * ETL_JOB_RUN (unit-owned) is VT_BATCH: which BATCH_ID belongs to which job run. Every later task reads it
--     WHERE RUN_ID = :run_id, never "the open batch".
--   * ETL_BATCH_CONTROL (shared, legacy contract) gets the reservation *immediately*: a STARTED row for this run's
--     BATCH_ID is written here, in the same task as the allocation, so every other MAX(BATCH_ID) + 1 allocator sharing
--     the table sees the id as taken for the whole run. (The BTEQ only inserted at the end, so its id was invisible to
--     other writers for the entire script -- the target closes that window to the two statements below and fails the
--     run if it lost the race.) Tasks 07/91 then UPDATE *this run's own row* -- matched on BATCH_ID *and* START_TS =
--     the run's BATCH_START_TS, the pair this task wrote -- to COMPLETED/FAILED; no row this job did not write is ever
--     touched. The STARTED value is new to the table's readers (legacy rows were only ever COMPLETED/FAILED): NOTE.md
--     records it as a consumer-contract delta.
--
-- Orphans: a run that dies before 07/91 (job cancel, timeout) leaves its own STARTED row open. The next run closes
-- it as FAILED first -- identified as *this job's* by its ETL_JOB_RUN row (a different RUN_ID, same BATCH_ID and
-- START_TS), never by status alone, so STARTED rows of other processes are left as they are. The job is serialised
-- (daily_load.job.yml: max_concurrent_runs 1 + queue), so any other RUN_ID with a STARTED row is a run that is no
-- longer executing.
CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.ETL_JOB_RUN (
  RUN_ID          STRING    NOT NULL,     -- {{job.run_id}}
  BATCH_ID        BIGINT    NOT NULL,
  BATCH_DATE      DATE      NOT NULL,
  BATCH_START_TS  TIMESTAMP NOT NULL
)
COMMENT 'VT_BATCH of bteq_daily_load, one row per job run; read by the tasks of that run only';

INSERT INTO ${catalog}.${schema}.ETL_LOG
  (PROCEDURE_NAME, BATCH_ID, LOG_LEVEL, LOG_MESSAGE, LOG_TS)
SELECT 'BTEQ_DAILY_LOAD', c.BATCH_ID, 'WARN',
       'Orphaned STARTED batch of run ' || r.RUN_ID || ' (' || CAST(c.BATCH_DATE AS STRING)
       || ') closed as FAILED before new allocation',
       current_timestamp()
FROM ${catalog}.${schema}.ETL_BATCH_CONTROL c
JOIN ${catalog}.${schema}.ETL_JOB_RUN r ON r.BATCH_ID = c.BATCH_ID AND r.BATCH_START_TS = c.START_TS
WHERE c.BATCH_STATUS = 'STARTED'
  AND r.RUN_ID <> :run_id;

UPDATE ${catalog}.${schema}.ETL_BATCH_CONTROL c
SET BATCH_STATUS = 'FAILED',
    END_TS = current_timestamp()
WHERE c.BATCH_STATUS = 'STARTED'
  AND EXISTS (SELECT 1 FROM ${catalog}.${schema}.ETL_JOB_RUN r
              WHERE r.BATCH_ID = c.BATCH_ID AND r.BATCH_START_TS = c.START_TS AND r.RUN_ID <> :run_id);

-- Allocation: MAX(BATCH_ID) + 1 as on the source. The reservation row below makes the id visible in
-- ETL_BATCH_CONTROL; ETL_JOB_RUN is included for the case where a run died between these two statements.
INSERT INTO ${catalog}.${schema}.ETL_JOB_RUN
  (RUN_ID, BATCH_ID, BATCH_DATE, BATCH_START_TS)
SELECT :run_id,
       GREATEST(
         (SELECT COALESCE(MAX(BATCH_ID), 0) FROM ${catalog}.${schema}.ETL_BATCH_CONTROL),
         (SELECT COALESCE(MAX(BATCH_ID), 0) FROM ${catalog}.${schema}.ETL_JOB_RUN)
       ) + 1,                                  -- MAX(BATCH_ID) + 1 on an empty table is NULL on both engines; made explicit
       current_date(),                         -- CURRENT_DATE
       current_timestamp()                     -- CURRENT_TIMESTAMP(0): second precision on source; recon truncates to ms
WHERE NOT EXISTS (SELECT 1 FROM ${catalog}.${schema}.ETL_JOB_RUN WHERE RUN_ID = :run_id);
-- The NOT EXISTS keeps a repaired/re-run task of the *same* job run on its existing BATCH_ID (one row per RUN_ID, so
-- the scalar lookups in 03-07/91 stay single-valued); a new job run has a new run_id and allocates a new batch.

-- Reservation: the shared table learns the id now, not at the end of the run.
INSERT INTO ${catalog}.${schema}.ETL_BATCH_CONTROL
  (BATCH_ID, BATCH_DATE, BATCH_STATUS, START_TS, END_TS)
SELECT r.BATCH_ID, r.BATCH_DATE, 'STARTED', r.BATCH_START_TS, NULL
FROM ${catalog}.${schema}.ETL_JOB_RUN r
WHERE r.RUN_ID = :run_id
  AND NOT EXISTS (SELECT 1 FROM ${catalog}.${schema}.ETL_BATCH_CONTROL c
                  WHERE c.BATCH_ID = r.BATCH_ID);
-- Residual exposure: another writer computing MAX + 1 between the two INSERTs above takes the same id. That is the
-- legacy race narrowed from a whole script to one task; removing it needs an allocator shared by *every* writer of
-- ETL_BATCH_CONTROL (an identity column or sequence table all of them adopt), which is a contract change for those
-- writers and is recorded as a decision item in NOTE.md rather than done unilaterally here. What this task does do
-- is refuse to continue when it lost: the run's reservation must be present as *its own* row (BATCH_ID and START_TS
-- both from ETL_JOB_RUN); if the id is present with a different START_TS another writer owns it, this task fails, 91
-- records FAILED for the run without touching the other writer's row (same two-column match), and no fact rows are
-- loaded under a shared BATCH_ID. A writer that appends the same id *after* this run's reservation is not caught here
-- (that is its allocation to check); recon's duplicate-BATCH_ID signature in NOTE.md is the net for it. A re-run of
-- this task inside the same job run (repair) finds its own row and passes. SIGNAL in a compound: databricks-dbsql
-- references/sql-scripting.md "SIGNAL and RESIGNAL" (same pattern as 03/04); a compound after plain statements in one
-- sql_task file is Not verified live (NOTE.md).
BEGIN
  IF NOT EXISTS (SELECT 1
                 FROM ${catalog}.${schema}.ETL_BATCH_CONTROL c
                 JOIN ${catalog}.${schema}.ETL_JOB_RUN r
                   ON r.BATCH_ID = c.BATCH_ID AND r.BATCH_START_TS = c.START_TS
                 WHERE r.RUN_ID = :run_id) THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = 'BATCH_ID allocation lost to a concurrent writer of ETL_BATCH_CONTROL; run '
                         || :run_id || ' stopped before loading';
  END IF;
END;
