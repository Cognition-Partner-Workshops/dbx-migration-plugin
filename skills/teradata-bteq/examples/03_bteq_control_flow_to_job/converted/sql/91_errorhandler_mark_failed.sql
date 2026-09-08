-- .LABEL ERRORHANDLER ... .QUIT 8  (run_if AT_LEAST_ONE_FAILED over every step after the batch row exists)
-- Legacy: SEL 'ERROR: ETL pipeline failed. ERRORCODE=' || TRIM(ERRORCODE (FORMAT '-9(5)')); then INSERT a FAILED row
-- from VT_BATCH. The failing task's own SIGNAL message is the ERRORCODE surrogate and is visible in the job run; here
-- only the batch state is written, from this run's ETL_JOB_RUN row, so ETL_BATCH_CONTROL matches the legacy content
-- (one FAILED row per failed batch) and no row belonging to another run or another process is touched.
INSERT INTO ${catalog}.${schema}.ETL_BATCH_CONTROL
  (BATCH_ID, BATCH_DATE, BATCH_STATUS, START_TS, END_TS)
SELECT BATCH_ID, BATCH_DATE, 'FAILED', BATCH_START_TS, current_timestamp()
FROM ${catalog}.${schema}.ETL_JOB_RUN r
WHERE r.RUN_ID = :run_id
  AND NOT EXISTS (SELECT 1 FROM ${catalog}.${schema}.ETL_BATCH_CONTROL c
                  WHERE c.BATCH_ID = r.BATCH_ID);
-- If new_batch_id itself failed there is no ETL_JOB_RUN row for :run_id and this INSERTs nothing -- exactly what the
-- BTEQ's `SEL ... FROM VT_BATCH` did when the volatile table was never created.
