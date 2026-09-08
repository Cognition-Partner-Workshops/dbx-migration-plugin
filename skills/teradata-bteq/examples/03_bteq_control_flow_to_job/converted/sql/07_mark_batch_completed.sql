-- Step 7 + .QUIT 0: the BTEQ script INSERTed a COMPLETED row from VT_BATCH; the STARTED row (02_new_batch_id)
-- is updated in place so ETL_BATCH_CONTROL keeps one row per batch, matching the legacy table's content.
UPDATE ${catalog}.${schema}.ETL_BATCH_CONTROL
SET BATCH_STATUS = 'COMPLETED',
    END_TS = current_timestamp()
WHERE BATCH_STATUS = 'STARTED';
-- DROP TABLE VT_BATCH: nothing to drop, the volatile table did not exist on the target.
