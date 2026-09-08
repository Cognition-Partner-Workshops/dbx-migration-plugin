-- .LABEL ERRORHANDLER ... .QUIT 8  (run_if AT_LEAST_ONE_FAILED over every step after the batch row exists)
-- Legacy: SEL 'ERROR: ETL pipeline failed. ERRORCODE=' || TRIM(ERRORCODE (FORMAT '-9(5)')); then INSERT a FAILED row
-- from VT_BATCH. The failing task's own SIGNAL message is the ERRORCODE surrogate and is visible in the job run; here
-- only the batch state is written: this run's own STARTED reservation row (02_new_batch_id) is closed as FAILED,
-- matched on the BATCH_ID *and* START_TS held in ETL_JOB_RUN for :run_id, so ETL_BATCH_CONTROL ends with the legacy
-- content (one FAILED row per failed batch) and no row belonging to another run or another process is touched. Same
-- MERGE shape as 07_mark_batch_completed: idempotent on repair, and still writes the row if the reservation INSERT
-- never landed -- including the lost-race stop in 02, where the FAILED row then shares its BATCH_ID with the other
-- writer's row (the run's failure is on record; the duplicate id is recon's duplicate-BATCH_ID signature).
MERGE INTO ${catalog}.${schema}.ETL_BATCH_CONTROL c
USING (SELECT BATCH_ID, BATCH_DATE, BATCH_START_TS
       FROM ${catalog}.${schema}.ETL_JOB_RUN
       WHERE RUN_ID = :run_id) r                                        -- (SEL ... FROM VT_BATCH)
ON c.BATCH_ID = r.BATCH_ID AND c.START_TS = r.BATCH_START_TS    -- the pair 02 wrote: this run's row, nobody else's
WHEN MATCHED AND c.BATCH_STATUS = 'STARTED' THEN
  UPDATE SET BATCH_STATUS = 'FAILED', END_TS = current_timestamp()
WHEN NOT MATCHED THEN
  INSERT (BATCH_ID, BATCH_DATE, BATCH_STATUS, START_TS, END_TS)
  VALUES (r.BATCH_ID, r.BATCH_DATE, 'FAILED', r.BATCH_START_TS, current_timestamp());
-- If new_batch_id itself failed there is no ETL_JOB_RUN row for :run_id and this writes nothing -- exactly what the
-- BTEQ's `SEL ... FROM VT_BATCH` did when the volatile table was never created.
