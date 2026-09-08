-- .LABEL NOSTAGING ... .QUIT 4  (runs only when staging_check failed: run_if AT_LEAST_ONE_FAILED)
-- Return code 4 was a BTEQ process exit code read by the scheduler; here the job run is already failed by
-- staging_check, and the warning is recorded where the operators will look.
INSERT INTO ${catalog}.${schema}.ETL_LOG
  (PROCEDURE_NAME, BATCH_ID, LOG_LEVEL, LOG_MESSAGE, LOG_TS)
VALUES ('BTEQ_DAILY_LOAD', NULL, 'WARN',
        'WARNING: No staging data found for ' || CAST(current_date() AS STRING) || ' (legacy rc=4)',
        current_timestamp());
