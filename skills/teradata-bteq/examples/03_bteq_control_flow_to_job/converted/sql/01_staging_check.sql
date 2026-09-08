-- Step 1 + ".IF ACTIVITYCOUNT = 0 THEN .GOTO NOSTAGING".
-- SQL scripting compound: databricks-dbsql references/sql-scripting.md "Compound Statements".
-- ${catalog}.${schema} are substituted by the unit's build step from .migration/allowed_targets.json (factory convention,
-- same as every other example here); the batch date is current_date(), exactly as the BTEQ used CURRENT_DATE.
--
-- Like-for-like: Step 1 is an aggregate SELECT with no GROUP BY, so it always returns exactly one row and BTEQ's
-- ACTIVITYCOUNT is 1 even when STG_TRANSACTIONS has no rows for today. The `.GOTO NOSTAGING` branch is therefore
-- reachable only when the request itself fails (ACTIVITYCOUNT = 0 after an error; Not verified live), never on
-- "no staging data". The converted task reproduces that: it does not branch on the count, and `nostaging_warning`
-- (run_if AT_LEAST_ONE_FAILED on this task) runs only when this SQL errors -- the same condition the BTEQ had.
-- Making "0 staged rows" stop the load is the behaviour the author probably *meant* (skill §7 trap "ACTIVITYCOUNT
-- after an aggregate"), but it is a business-logic correction, not a conversion: it needs a row in
-- .migration/06_decisions.md before this block gains an `IF staged = 0 THEN SIGNAL ...` guard, and the fixture's
-- empty-staging parity case (source completes the batch with 0 rows) is what recon asserts until then.
BEGIN
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
