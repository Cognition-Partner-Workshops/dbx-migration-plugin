-- Step 2 + CREATE VOLATILE TABLE VT_BATCH ... ON COMMIT PRESERVE ROWS.
-- A volatile table lives for one BTEQ session; a job has no shared session across tasks, so VT_BATCH becomes one row
-- in ETL_JOB_RUN keyed by the job run: the job-level parameter run_id defaults to {{job.run_id}} ("the unique
-- identifier assigned to the job run", docs.databricks.com/aws/en/jobs/dynamic-value-references), job parameters are
-- pushed to every task (databricks-jobs SKILL.md "Job Parameters"), and a SQL task reads them with the named-parameter
-- syntax :run_id (docs.databricks.com/aws/en/jobs/parameter-use, "SQL: use named parameters").
-- (A temporary table would be session-scoped to this task only: databricks-dbsql
--  references/materialized-views-pipes.md "Temporary Tables and Temporary Views".)
--
-- ETL_JOB_RUN is owned by this unit and is the *only* target-side state the conversion adds. ETL_BATCH_CONTROL keeps
-- its legacy contract untouched: like the BTEQ, this job INSERTs exactly one COMPLETED or FAILED row per run at the
-- end (07 / 91) and never updates rows it did not write, so any other process sharing that table is unaffected. A run
-- that dies before 07/91 (job cancel, timeout) leaves an ETL_JOB_RUN row and no ETL_BATCH_CONTROL row -- the same
-- footprint the BTEQ left when its session died with VT_BATCH -- and later tasks never see it because every lookup is
-- WHERE RUN_ID = :run_id, not "the open batch".
--
-- Allocation: MAX(BATCH_ID) + 1 as on the source, but over both tables, because a died run's BATCH_ID reached
-- FACT_TRANSACTION.ETL_BATCH_ID without ever reaching ETL_BATCH_CONTROL and must not be reused. The job is serialised
-- (daily_load.job.yml: max_concurrent_runs 1 + queue), so two runs of *this* job cannot allocate concurrently; other
-- writers of ETL_BATCH_CONTROL allocating MAX + 1 at the same instant is the legacy exposure, unchanged.
CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.ETL_JOB_RUN (
  RUN_ID          STRING    NOT NULL,     -- {{job.run_id}}
  BATCH_ID        BIGINT    NOT NULL,
  BATCH_DATE      DATE      NOT NULL,
  BATCH_START_TS  TIMESTAMP NOT NULL
)
COMMENT 'VT_BATCH of bteq_daily_load, one row per job run; read by the tasks of that run only';

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
