-- .LABEL ERRORHANDLER ... .QUIT 8  (run_if AT_LEAST_ONE_FAILED over every step after the batch row exists)
-- Legacy: SEL 'ERROR: ETL pipeline failed. ERRORCODE=' || TRIM(ERRORCODE (FORMAT '-9(5)')); then INSERT a FAILED row.
-- The failing task's own SIGNAL message is the ERRORCODE surrogate and is visible in the job run; here only the
-- batch state is written, so ETL_BATCH_CONTROL matches the legacy content (one FAILED row per failed batch).
UPDATE ${catalog}.${schema}.ETL_BATCH_CONTROL
SET BATCH_STATUS = 'FAILED',
    END_TS = current_timestamp()
WHERE BATCH_STATUS = 'STARTED';
