-- Step 5: EXEC BANKING_DW.DAILY_BALANCE_CHECK(CURRENT_DATE);
-- A Teradata macro that only returns result sets becomes a procedure that materialises them (example 05 pattern);
-- EXEC <macro> -> CALL <procedure>.
BEGIN
  DECLARE v_batch_id BIGINT;
  SET v_batch_id = (SELECT BATCH_ID FROM ${catalog}.${schema}.ETL_BATCH_CONTROL
                    WHERE BATCH_DATE = current_date() AND BATCH_STATUS = 'STARTED');

  CALL ${catalog}.${schema}.DAILY_BALANCE_CHECK(current_date(), v_batch_id);
END;
