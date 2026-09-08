-- Step 1 + ".IF ACTIVITYCOUNT = 0 THEN .GOTO NOSTAGING".
-- SQL scripting compound: databricks-dbsql references/sql-scripting.md "Compound Statements", "SIGNAL and RESIGNAL".
-- ${catalog}.${schema} are substituted by the unit's build step from .migration/allowed_targets.json (factory convention,
-- same as every other example here); the batch date is current_date(), exactly as the BTEQ used CURRENT_DATE.
BEGIN
  DECLARE staged INT;
  SET staged = (SELECT COUNT(*) FROM ${catalog}.${schema}.STG_TRANSACTIONS WHERE LOAD_DATE = current_date());

  IF staged = 0 THEN
    -- BTEQ branched to .LABEL NOSTAGING; here the task fails and nostaging_warning runs (run_if AT_LEAST_ONE_FAILED)
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'WARNING: No staging data found for ' || CAST(current_date() AS STRING);
  END IF;

  -- The original SEL printed a report line; keep it as a queryable audit row instead of stdout.
  INSERT INTO ${catalog}.${schema}.ETL_LOG
    (PROCEDURE_NAME, BATCH_ID, LOG_LEVEL, LOG_MESSAGE, LOG_TS)
  SELECT 'BTEQ_DAILY_LOAD', NULL, 'INFO',
         'STAGING_CHECK rows=' || CAST(COUNT(*) AS STRING)
         || ' min=' || CAST(MIN(LOAD_DATE) AS STRING)          -- (FORMAT 'YYYY-MM-DD') -> explicit cast
         || ' max=' || CAST(MAX(LOAD_DATE) AS STRING)
         || ' accounts=' || CAST(COUNT(DISTINCT ACCOUNT_ID) AS STRING),
         current_timestamp()
  FROM ${catalog}.${schema}.STG_TRANSACTIONS
  WHERE LOAD_DATE = current_date();
END;
