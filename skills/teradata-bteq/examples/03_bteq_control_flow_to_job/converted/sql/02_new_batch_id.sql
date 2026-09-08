-- Step 2 + CREATE VOLATILE TABLE VT_BATCH ... ON COMMIT PRESERVE ROWS.
-- A volatile table lives for one BTEQ session; a job has no shared session across tasks, so the batch row is
-- persisted in ETL_BATCH_CONTROL with status 'STARTED' and re-read by later tasks keyed on batch_date.
-- (A temporary table would be session-scoped to this task only: databricks-dbsql
--  references/materialized-views-pipes.md "Temporary Tables and Temporary Views".)
INSERT INTO ${catalog}.${schema}.ETL_BATCH_CONTROL
  (BATCH_ID, BATCH_DATE, BATCH_STATUS, START_TS, END_TS)
SELECT COALESCE(MAX(BATCH_ID), 0) + 1,        -- MAX(BATCH_ID) + 1 on an empty table is NULL on both engines; made explicit
       current_date(),
       'STARTED',
       current_timestamp(),                    -- CURRENT_TIMESTAMP(0): second precision on source; recon truncates to ms
       NULL
FROM ${catalog}.${schema}.ETL_BATCH_CONTROL;
