-- Target: Unity Catalog stored procedure (databricks-dbsql references/sql-scripting.md "Stored Procedures / CREATE PROCEDURE",
-- "Exception Handling / Handler Declaration", "Variable Assignment (SET)"). Public Preview, Runtime 17.0+ per that section.
-- OUT parameters keep the legacy names so the BTEQ->job caller (example 03, 04_call_load_daily_transactions.sql) is a
-- mechanical CALL. DEFAULT is not allowed on OUT parameters (same section), so none is declared.
CREATE OR REPLACE PROCEDURE ${catalog}.${schema}.SP_LOAD_DAILY_TRANSACTIONS(
    IN  p_batch_date     DATE,          -- FORMAT 'YYYY-MM-DD' dropped: display-only
    IN  p_batch_id       BIGINT,
    OUT p_rows_inserted  INT,
    OUT p_rows_rejected  INT,
    OUT p_return_code    INT
)
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
COMMENT 'Loads daily transaction data from staging to fact table'
AS BEGIN
    DECLARE v_error_count INT DEFAULT 0;
    DECLARE v_start_ts TIMESTAMP;

    -- SQLCODE has no cited equivalent; the handler records a fixed non-zero code (legacy consumers only test <> 0,
    -- see bteq_daily_load.btq) and the message. Skill §7 trap "SQLCODE/SQLSTATE". Not verified live: reading the
    -- caught SQLSTATE inside the handler body.
    -- No ROLLBACK: the source has no BT/ET either (each statement commits), and a compound here has no cited
    -- multi-statement transaction (skill §7 "BT/ET"). Rows written before a failure therefore stay committed under
    -- the failed batch id, and example 03 marks that batch FAILED and re-runs the date under a new one. The two
    -- INSERTs below are keyed on TRANSACTION_ID (stable identity: fixture PI/COLLECT STATS column, example 07
    -- quarantines duplicates before they reach STG_TRANSACTIONS) so the re-run adds only what is missing, and the
    -- completing batch adopts the rows of *incomplete* earlier batches for the date -- batches with no COMPLETED row
    -- in ETL_BATCH_CONTROL (marked FAILED by example 03's error branch, or died before writing any row). Rows owned
    -- by a batch that did complete are never re-stamped: a successful load is history, and a later run for the same
    -- date only adds what that load did not. This keeps the OUT counts and example 03's per-batch recon report whole
    -- for the attempt chain without rewriting other batches' ownership.
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        SET p_return_code = -1;
        INSERT INTO ${catalog}.${schema}.ETL_LOG
            (PROCEDURE_NAME, BATCH_ID, LOG_LEVEL, LOG_MESSAGE, LOG_TS)
        VALUES ('SP_LOAD_DAILY_TRANSACTIONS', p_batch_id, 'ERROR',
                'SQLEXCEPTION at ' || CAST(current_timestamp() AS STRING),
                current_timestamp());
    END;

    SET v_start_ts = current_timestamp();
    SET p_rows_inserted = 0;
    SET p_rows_rejected = 0;
    SET p_return_code = 0;

    INSERT INTO ${catalog}.${schema}.ETL_LOG
        (PROCEDURE_NAME, BATCH_ID, LOG_LEVEL, LOG_MESSAGE, LOG_TS)
    VALUES ('SP_LOAD_DAILY_TRANSACTIONS', p_batch_id, 'INFO',
            'Started loading transactions for date: ' || CAST(p_batch_date AS STRING),
            v_start_ts);

    -- Reject bad records to error table. `stg.*` expanded implicitly on Teradata; Delta target column order is not
    -- guaranteed to match, so the column list is explicit (skill §7 trap "INSERT ... SEL *").
    -- NOT IN against a nullable ACCOUNT_ID behaves the same (three-valued) on both engines; DIM_ACCOUNT.ACCOUNT_ID is NOT NULL.
    INSERT INTO ${catalog}.${schema}.STG_TRANSACTION_ERRORS
        (TRANSACTION_ID, TRANSACTION_DATE, TRANSACTION_TIME, ACCOUNT_ID, TRANSACTION_TYPE, TRANSACTION_SUBTYPE,
         CHANNEL, TRANSACTION_AMOUNT, CURRENCY_CODE, MERCHANT_ID, MERCHANT_NAME, MERCHANT_CATEGORY,
         COUNTERPARTY_ACCT, REFERENCE_NUMBER, DESCRIPTION_TEXT, POSTING_DATE, VALUE_DATE, LOAD_DATE,
         ERROR_REASON, BATCH_ID)
    SELECT stg.TRANSACTION_ID, stg.TRANSACTION_DATE, stg.TRANSACTION_TIME, stg.ACCOUNT_ID, stg.TRANSACTION_TYPE,
           stg.TRANSACTION_SUBTYPE, stg.CHANNEL, stg.TRANSACTION_AMOUNT, stg.CURRENCY_CODE, stg.MERCHANT_ID,
           stg.MERCHANT_NAME, stg.MERCHANT_CATEGORY, stg.COUNTERPARTY_ACCT, stg.REFERENCE_NUMBER,
           stg.DESCRIPTION_TEXT, stg.POSTING_DATE, stg.VALUE_DATE, stg.LOAD_DATE,
           'INVALID_ACCOUNT' AS ERROR_REASON, p_batch_id AS BATCH_ID
    FROM ${catalog}.${schema}.STG_TRANSACTIONS stg
    WHERE stg.LOAD_DATE = p_batch_date
      AND stg.ACCOUNT_ID NOT IN (SELECT ACCOUNT_ID FROM ${catalog}.${schema}.DIM_ACCOUNT WHERE CURRENT_FLAG = 'Y')
      AND NOT EXISTS (SELECT 1 FROM ${catalog}.${schema}.STG_TRANSACTION_ERRORS e
                      WHERE e.TRANSACTION_ID = stg.TRANSACTION_ID
                        AND e.LOAD_DATE = stg.LOAD_DATE
                        AND e.ERROR_REASON = 'INVALID_ACCOUNT');

    -- Re-run of the date: rejections written by an incomplete (FAILED or died) batch now belong to this one; rows of
    -- a batch that COMPLETED keep their owner.
    UPDATE ${catalog}.${schema}.STG_TRANSACTION_ERRORS e
    SET BATCH_ID = p_batch_id
    WHERE e.LOAD_DATE = p_batch_date
      AND e.ERROR_REASON = 'INVALID_ACCOUNT'
      AND e.BATCH_ID <> p_batch_id
      AND NOT EXISTS (SELECT 1 FROM ${catalog}.${schema}.ETL_BATCH_CONTROL c
                      WHERE c.BATCH_ID = e.BATCH_ID AND c.BATCH_STATUS = 'COMPLETED');

    -- ACTIVITY_COUNT: no cited row-count register in the official scripting reference; count the rows this batch owns.
    SET v_error_count = (SELECT COUNT(*) FROM ${catalog}.${schema}.STG_TRANSACTION_ERRORS
                         WHERE BATCH_ID = p_batch_id AND ERROR_REASON = 'INVALID_ACCOUNT');

    INSERT INTO ${catalog}.${schema}.FACT_TRANSACTION (
        TRANSACTION_ID, TRANSACTION_DATE, TRANSACTION_TIME, TRANSACTION_TS,
        ACCOUNT_KEY, CUSTOMER_KEY, PRODUCT_ID, BRANCH_ID, DATE_KEY,
        TRANSACTION_TYPE, TRANSACTION_SUBTYPE, CHANNEL,
        TRANSACTION_AMOUNT, TRANSACTION_CURRENCY, BASE_CURRENCY_AMOUNT,
        EXCHANGE_RATE, MERCHANT_ID, MERCHANT_NAME, MERCHANT_CATEGORY,
        COUNTERPARTY_ACCT, REFERENCE_NUMBER, DESCRIPTION_TEXT,
        IS_INTERNATIONAL, POSTING_DATE, VALUE_DATE,
        ETL_BATCH_ID, ETL_INSERT_TS
    )
    SELECT
        stg.TRANSACTION_ID,
        stg.TRANSACTION_DATE,
        stg.TRANSACTION_TIME,                                        -- TIME(0) -> STRING 'HH:MM:SS' per type map (no TIME type)
        -- DATE (TIMESTAMP(6)) + (TIME - TIME '00:00:00' HOUR TO SECOND): interval arithmetic -> timestamp build
        CAST(CAST(stg.TRANSACTION_DATE AS STRING) || ' ' || stg.TRANSACTION_TIME AS TIMESTAMP),
        a.ACCOUNT_KEY,
        c.CUSTOMER_KEY,
        a.PRODUCT_ID,
        a.BRANCH_ID,
        -- CAST(CAST(d AS DATE FORMAT 'YYYYMMDD') AS INTEGER): FORMAT-driven cast -> explicit date_format
        CAST(date_format(stg.TRANSACTION_DATE, 'yyyyMMdd') AS INT),
        stg.TRANSACTION_TYPE,
        stg.TRANSACTION_SUBTYPE,
        stg.CHANNEL,
        stg.TRANSACTION_AMOUNT,
        stg.CURRENCY_CODE,
        CASE WHEN stg.CURRENCY_CODE <> 'NOK'
             THEN stg.TRANSACTION_AMOUNT * COALESCE(fx.EXCHANGE_RATE, 0)     -- ZEROIFNULL
             ELSE stg.TRANSACTION_AMOUNT
        END,
        COALESCE(fx.EXCHANGE_RATE, 0),
        stg.MERCHANT_ID,
        stg.MERCHANT_NAME,
        stg.MERCHANT_CATEGORY,
        stg.COUNTERPARTY_ACCT,
        stg.REFERENCE_NUMBER,
        stg.DESCRIPTION_TEXT,
        CASE WHEN stg.CURRENCY_CODE <> 'NOK' THEN 1 ELSE 0 END,
        stg.POSTING_DATE,
        stg.VALUE_DATE,
        p_batch_id,
        current_timestamp()
    FROM ${catalog}.${schema}.STG_TRANSACTIONS stg
    INNER JOIN ${catalog}.${schema}.DIM_ACCOUNT a
        ON stg.ACCOUNT_ID = a.ACCOUNT_ID
       AND a.CURRENT_FLAG = 'Y'
    INNER JOIN ${catalog}.${schema}.DIM_CUSTOMER c
        ON a.CUSTOMER_ID = c.CUSTOMER_ID
       AND c.CURRENT_FLAG = 'Y'
    LEFT JOIN ${catalog}.${schema}.DIM_EXCHANGE_RATES fx
        ON stg.CURRENCY_CODE = fx.FROM_CURRENCY
       AND fx.TO_CURRENCY = 'NOK'
       AND stg.TRANSACTION_DATE = fx.RATE_DATE
    WHERE stg.LOAD_DATE = p_batch_date
      AND stg.ACCOUNT_ID IN (SELECT ACCOUNT_ID FROM ${catalog}.${schema}.DIM_ACCOUNT WHERE CURRENT_FLAG = 'Y')
      AND NOT EXISTS (SELECT 1 FROM ${catalog}.${schema}.FACT_TRANSACTION f
                      WHERE f.TRANSACTION_ID = stg.TRANSACTION_ID);

    -- Re-run of the date: fact rows an incomplete batch committed are adopted by the completing batch, so
    -- ETL_BATCH_ID means "the batch that completed this load" and the per-batch counts below (and example 03's
    -- LOADED_ROWS) cover the whole attempt chain, not just this attempt's remainder. Ownership is the gate, not the
    -- date: a row whose batch has a COMPLETED control row belongs to that batch for good, even if its TRANSACTION_ID
    -- is still in staging for the date (the NOT EXISTS above already skipped re-inserting it).
    UPDATE ${catalog}.${schema}.FACT_TRANSACTION f
    SET ETL_BATCH_ID = p_batch_id
    WHERE f.ETL_BATCH_ID <> p_batch_id
      AND EXISTS (SELECT 1 FROM ${catalog}.${schema}.STG_TRANSACTIONS stg
                  WHERE stg.TRANSACTION_ID = f.TRANSACTION_ID AND stg.LOAD_DATE = p_batch_date)
      AND NOT EXISTS (SELECT 1 FROM ${catalog}.${schema}.ETL_BATCH_CONTROL c
                      WHERE c.BATCH_ID = f.ETL_BATCH_ID AND c.BATCH_STATUS = 'COMPLETED');

    SET p_rows_inserted = (SELECT COUNT(*) FROM ${catalog}.${schema}.FACT_TRANSACTION WHERE ETL_BATCH_ID = p_batch_id);
    SET p_rows_rejected = v_error_count;

    -- COLLECT STATISTICS -> target-profile maintenance (databricks-dbsql references/best-practices.md
    -- "OPTIMIZE, VACUUM, and ANALYZE"); left out of the procedure body so the unit's job owns table maintenance.

    INSERT INTO ${catalog}.${schema}.ETL_LOG
        (PROCEDURE_NAME, BATCH_ID, LOG_LEVEL, LOG_MESSAGE, LOG_TS)
    VALUES ('SP_LOAD_DAILY_TRANSACTIONS', p_batch_id, 'INFO',
            'Completed. Inserted: ' || CAST(p_rows_inserted AS STRING) ||
            ', Rejected: ' || CAST(p_rows_rejected AS STRING) ||
            -- (ts - ts) SECOND(4) interval -> seconds as integer
            ', Duration: ' || CAST(unix_timestamp(current_timestamp()) - unix_timestamp(v_start_ts) AS STRING),
            current_timestamp());
END;
