-- Step 2 + CREATE VOLATILE TABLE VT_BATCH ... ON COMMIT PRESERVE ROWS.
-- A volatile table lives for one BTEQ session; a job has no shared session across tasks, so the batch row is
-- persisted in ETL_BATCH_CONTROL with status 'STARTED' and re-read by later tasks as "the one open batch".
-- (A temporary table would be session-scoped to this task only: databricks-dbsql
--  references/materialized-views-pipes.md "Temporary Tables and Temporary Views".)
--
-- Invariant this task establishes: at most one STARTED row exists once it finishes.
--   * The job is serialised (daily_load.job.yml: max_concurrent_runs 1 + queue), so no other run can allocate
--     MAX(BATCH_ID) + 1 concurrently; the allocation below is therefore race-free without a table lock.
--   * A run interrupted before mark_batch_completed / errorhandler_mark_failed leaves an orphaned STARTED row
--     (job cancel or timeout does not run the AT_LEAST_ONE_FAILED branch). The BTEQ never met this case because
--     VT_BATCH died with the session; here the orphan is closed as FAILED first, with an ETL_LOG WARN row per orphan,
--     so the same-day retry gets exactly one open batch and every later "WHERE BATCH_STATUS = 'STARTED'" lookup is
--     single-valued.
BEGIN
  INSERT INTO ${catalog}.${schema}.ETL_LOG
    (PROCEDURE_NAME, BATCH_ID, LOG_LEVEL, LOG_MESSAGE, LOG_TS)
  SELECT 'BTEQ_DAILY_LOAD', BATCH_ID, 'WARN',
         'Orphaned STARTED batch from ' || CAST(BATCH_DATE AS STRING) || ' closed as FAILED before new allocation',
         current_timestamp()
  FROM ${catalog}.${schema}.ETL_BATCH_CONTROL
  WHERE BATCH_STATUS = 'STARTED';

  UPDATE ${catalog}.${schema}.ETL_BATCH_CONTROL
  SET BATCH_STATUS = 'FAILED',
      END_TS = current_timestamp()
  WHERE BATCH_STATUS = 'STARTED';

  INSERT INTO ${catalog}.${schema}.ETL_BATCH_CONTROL
    (BATCH_ID, BATCH_DATE, BATCH_STATUS, START_TS, END_TS)
  SELECT COALESCE(MAX(BATCH_ID), 0) + 1,        -- MAX(BATCH_ID) + 1 on an empty table is NULL on both engines; made explicit
         current_date(),
         'STARTED',
         current_timestamp(),                    -- CURRENT_TIMESTAMP(0): second precision on source; recon truncates to ms
         NULL
  FROM ${catalog}.${schema}.ETL_BATCH_CONTROL;
END;
