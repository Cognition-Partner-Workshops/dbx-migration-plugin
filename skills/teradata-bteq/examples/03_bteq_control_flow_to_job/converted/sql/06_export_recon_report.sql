-- Step 6: .EXPORT REPORT FILE=/etl/reports/daily_recon_YYYYMMDD.txt ... .EXPORT RESET
-- The flat-file report becomes a Delta table the report consumer reads (D4/D6 consumer decision decides whether a
-- file export is still needed; if so it is a downstream task, not part of the load unit).
INSERT INTO ${catalog}.${schema}.RPT_DAILY_RECONCILIATION
  (REPORT_TYPE, REPORT_DATE, BATCH_ID, STAGED_ROWS, LOADED_ROWS, ERROR_ROWS, GENERATED_TS)
WITH b AS (
  SELECT BATCH_ID
  FROM ${catalog}.${schema}.ETL_JOB_RUN
  WHERE RUN_ID = :run_id                                   -- (SEL BATCH_ID FROM VT_BATCH)
)
SELECT
  'DAILY_RECONCILIATION',
  current_date(),                                          -- (FORMAT 'YYYY-MM-DD') dropped: DATE, not text
  b.BATCH_ID,
  (SELECT COUNT(*) FROM ${catalog}.${schema}.STG_TRANSACTIONS
    WHERE LOAD_DATE = current_date()),
  (SELECT COUNT(*) FROM ${catalog}.${schema}.FACT_TRANSACTION
    WHERE ETL_BATCH_ID = b.BATCH_ID),
  (SELECT COUNT(*) FROM ${catalog}.${schema}.STG_TRANSACTION_ERRORS
    WHERE BATCH_ID = b.BATCH_ID),
  current_timestamp()
FROM b;
