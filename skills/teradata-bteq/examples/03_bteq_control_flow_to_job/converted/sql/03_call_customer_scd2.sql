-- Step 3: CALL BANKING_DW.SP_CUSTOMER_SCD2((SEL BATCH_ID FROM VT_BATCH), new_rows, changed_rows, return_code);
-- CALL with OUT arguments bound to local variables: databricks-dbsql references/sql-scripting.md "CALL (Invoke a Procedure)".
BEGIN
  DECLARE v_batch_id BIGINT;
  DECLARE new_rows INT;
  DECLARE changed_rows INT;
  DECLARE return_code INT;

  SET v_batch_id = (SELECT BATCH_ID FROM ${catalog}.${schema}.ETL_BATCH_CONTROL
                    WHERE BATCH_STATUS = 'STARTED');

  CALL ${catalog}.${schema}.SP_CUSTOMER_SCD2(v_batch_id, new_rows, changed_rows, return_code);

  -- .IF ERRORCODE <> 0: the procedure's own EXIT HANDLER sets return_code; surface it as a task failure.
  IF return_code <> 0 THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'SP_CUSTOMER_SCD2 return_code=' || CAST(return_code AS STRING);
  END IF;
END;
