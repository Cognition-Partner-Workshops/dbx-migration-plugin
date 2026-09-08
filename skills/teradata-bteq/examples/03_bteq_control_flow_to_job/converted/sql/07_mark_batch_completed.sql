-- Step 7 + .QUIT 0: INSERT the COMPLETED row from VT_BATCH -> INSERT it from this run's ETL_JOB_RUN row.
-- Same statement shape and the same single row per batch as the legacy script; ETL_BATCH_CONTROL is only ever
-- INSERTed by this job, never UPDATEd, so rows written by any other process sharing the table are never touched.
INSERT INTO ${catalog}.${schema}.ETL_BATCH_CONTROL
  (BATCH_ID, BATCH_DATE, BATCH_STATUS, START_TS, END_TS)
SELECT BATCH_ID, BATCH_DATE, 'COMPLETED', BATCH_START_TS, current_timestamp()
FROM ${catalog}.${schema}.ETL_JOB_RUN r
WHERE r.RUN_ID = :run_id
  AND NOT EXISTS (SELECT 1 FROM ${catalog}.${schema}.ETL_BATCH_CONTROL c
                  WHERE c.BATCH_ID = r.BATCH_ID);   -- a repaired run of this task must not write a second row
-- DROP TABLE VT_BATCH: the ETL_JOB_RUN row is kept as the run's audit trail (nothing session-scoped to drop).
