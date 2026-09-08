-- Step 4: CALL BANKING_DW.SP_LOAD_DAILY_TRANSACTIONS(CURRENT_DATE, (SEL BATCH_ID FROM VT_BATCH), rows_inserted, rows_rejected, return_code);
-- Procedure body: examples/04_spl_exit_handler_out_params.
BEGIN
  DECLARE v_batch_id BIGINT;
  DECLARE rows_inserted INT;
  DECLARE rows_rejected INT;
  DECLARE return_code INT;

  SET v_batch_id = (SELECT BATCH_ID FROM ${catalog}.${schema}.ETL_BATCH_CONTROL
                    WHERE BATCH_STATUS = 'STARTED');

  CALL ${catalog}.${schema}.SP_LOAD_DAILY_TRANSACTIONS(
    current_date(), v_batch_id, rows_inserted, rows_rejected, return_code);

  IF return_code <> 0 THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'SP_LOAD_DAILY_TRANSACTIONS return_code=' || CAST(return_code AS STRING);
  END IF;
END;
