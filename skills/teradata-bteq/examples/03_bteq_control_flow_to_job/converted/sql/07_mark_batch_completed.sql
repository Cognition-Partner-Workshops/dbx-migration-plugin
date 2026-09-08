-- Step 7 + .QUIT 0: INSERT the COMPLETED row from VT_BATCH -> close this run's own reservation row.
-- 02_new_batch_id already wrote the STARTED row for this run's BATCH_ID, so the legacy INSERT becomes an UPDATE of
-- that one row, matched on the BATCH_ID *and* START_TS held in ETL_JOB_RUN for :run_id -- never on status alone or
-- on the id alone, so a row written by any other process sharing ETL_BATCH_CONTROL (even one that took the same id;
-- 02 fails the run in that case) is never touched. MERGE (docs.databricks.com/aws/en/sql/language-manual/
-- delta-merge-into; databricks-dbsql references/best-practices.md "SCD Type 2 with MERGE") so a repaired run of this
-- task is a no-op once the row is closed, and the row is still written if the reservation INSERT in 02 never landed.
MERGE INTO ${catalog}.${schema}.ETL_BATCH_CONTROL c
USING (SELECT BATCH_ID, BATCH_DATE, BATCH_START_TS
       FROM ${catalog}.${schema}.ETL_JOB_RUN
       WHERE RUN_ID = :run_id) r                                        -- (SEL ... FROM VT_BATCH)
ON c.BATCH_ID = r.BATCH_ID AND c.START_TS = r.BATCH_START_TS    -- the pair 02 wrote: this run's row, nobody else's
WHEN MATCHED AND c.BATCH_STATUS = 'STARTED' THEN
  UPDATE SET BATCH_STATUS = 'COMPLETED', END_TS = current_timestamp()
WHEN NOT MATCHED THEN
  INSERT (BATCH_ID, BATCH_DATE, BATCH_STATUS, START_TS, END_TS)
  VALUES (r.BATCH_ID, r.BATCH_DATE, 'COMPLETED', r.BATCH_START_TS, current_timestamp());
-- DROP TABLE VT_BATCH: the ETL_JOB_RUN row is kept as the run's audit trail (nothing session-scoped to drop).
