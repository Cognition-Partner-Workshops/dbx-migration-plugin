-- Target: UC stored procedure (databricks-dbsql references/sql-scripting.md: "FOR Loop", "LEAVE and ITERATE",
-- "CASE Statement", "Handler Declaration" (EXIT only), "SIGNAL and RESIGNAL", "EXECUTE IMMEDIATE", "CREATE PROCEDURE").
-- BT/ET has no drop-in (NOTE.md). Deployed with the procedure: one-row campaign lock, seeded idempotently by MERGE.
CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.ARCHIVE_CAMPAIGN_LOCK
    (LOCK_NAME STRING NOT NULL, OWNER_RUN_ID STRING, LOCKED_TS TIMESTAMP);
MERGE INTO ${catalog}.${schema}.ARCHIVE_CAMPAIGN_LOCK l USING (SELECT 'SP_ARCHIVE_CLOSED_ACCOUNTS' AS LOCK_NAME) s
    ON l.LOCK_NAME = s.LOCK_NAME
    WHEN NOT MATCHED THEN INSERT (LOCK_NAME, OWNER_RUN_ID, LOCKED_TS) VALUES (s.LOCK_NAME, NULL, NULL);
CREATE OR REPLACE PROCEDURE ${catalog}.${schema}.SP_ARCHIVE_CLOSED_ACCOUNTS(
    IN    p_closed_before   DATE,
    IN    p_archive_schema  STRING,          -- was p_archive_db (Teradata database == UC schema)
    INOUT p_max_batch       INT,
    OUT   p_accounts_done   INT,
    OUT   p_return_code     INT
)
LANGUAGE SQL SQL SECURITY INVOKER MODIFIES SQL DATA
AS BEGIN
    DECLARE v_run_id     STRING DEFAULT uuid();
    DECLARE v_txn_count  INT;
    DECLARE v_arch_table STRING;
    -- ROLLBACK has no equivalent: partial work is kept, the re-run is idempotent, and the counter increments only after
    -- an account's status UPDATE committed, so the returned INOUT budget charges exactly the ARCHIVED rows. Fixed return
    -- code: no cited read of SQLCODE/SQLSTATE inside a handler. Lock released only if this call owns it.
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        SET p_return_code = -1;
        SET p_max_batch = p_max_batch - p_accounts_done;
        UPDATE ${catalog}.${schema}.ARCHIVE_CAMPAIGN_LOCK SET OWNER_RUN_ID = NULL, LOCKED_TS = NULL
        WHERE LOCK_NAME = 'SP_ARCHIVE_CLOSED_ACCOUNTS' AND OWNER_RUN_ID = v_run_id;
        INSERT INTO ${catalog}.${schema}.ETL_LOG (PROCEDURE_NAME, BATCH_ID, LOG_LEVEL, LOG_MESSAGE, LOG_TS)
        VALUES ('SP_ARCHIVE_CLOSED_ACCOUNTS', p_max_batch, 'ERROR', 'SQLEXCEPTION in run ' || v_run_id || ' after '
                || CAST(p_accounts_done AS STRING) || ' accounts (partial batch kept; re-run with the returned budget)',
                current_timestamp());
    END;
    SET p_return_code = 0; SET p_accounts_done = 0;
    IF p_max_batch IS NULL OR p_max_batch <= 0 THEN
        SIGNAL SQLSTATE '75001' SET MESSAGE_TEXT = 'p_max_batch must be positive';
    END IF;
    -- Identifier spliced into dynamic SQL (a ? marker cannot carry one): build-time allowlist, never caller text.
    IF p_archive_schema IS NULL OR p_archive_schema NOT IN ('${schema}', '${archive_schema}') THEN
        SIGNAL SQLSTATE '75002' SET MESSAGE_TEXT = 'p_archive_schema is not a declared archive target for this unit';
    END IF;
    -- BT held write locks until ET, serialising callers. Claim the seeded row, read back, SIGNAL if not the owner (held
    -- by another run, or row missing: read-back NULL). A loser may instead hit a write conflict; every path -> handler.
    UPDATE ${catalog}.${schema}.ARCHIVE_CAMPAIGN_LOCK SET OWNER_RUN_ID = v_run_id, LOCKED_TS = current_timestamp()
    WHERE LOCK_NAME = 'SP_ARCHIVE_CLOSED_ACCOUNTS' AND OWNER_RUN_ID IS NULL;
    IF (SELECT OWNER_RUN_ID FROM ${catalog}.${schema}.ARCHIVE_CAMPAIGN_LOCK
        WHERE LOCK_NAME = 'SP_ARCHIVE_CLOSED_ACCOUNTS') IS DISTINCT FROM v_run_id THEN
        SIGNAL SQLSTATE '75003' SET MESSAGE_TEXT = 'SP_ARCHIVE_CLOSED_ACCOUNTS campaign lock missing or held by another run';
    END IF;
    SET v_arch_table = '${catalog}.' || p_archive_schema || '.FACT_TRANSACTION_ARCH_'
                       || CAST(year(p_closed_before) AS STRING);            -- EXTRACT(YEAR ...) (FORMAT '9999')
    EXECUTE IMMEDIATE 'CREATE TABLE IF NOT EXISTS ' || v_arch_table
                   || ' AS SELECT * FROM ${catalog}.${schema}.FACT_TRANSACTION WHERE 1 = 0';   -- ... WITH NO DATA
    archive_loop: FOR acct AS
        SELECT ACCOUNT_KEY, ACCOUNT_TYPE FROM ${catalog}.${schema}.DIM_ACCOUNT
        WHERE ACCOUNT_STATUS = 'CLOSED' AND CLOSE_DATE < p_closed_before AND CURRENT_FLAG = 'Y'
        ORDER BY ACCOUNT_KEY
    DO
        IF p_accounts_done >= p_max_batch THEN
            LEAVE archive_loop;                                             -- WHILE ... p_accounts_done < p_max_batch
        END IF;
        CASE acct.ACCOUNT_TYPE
            WHEN 'LOAN' THEN
                SET v_txn_count = 0;
            ELSE
                EXECUTE IMMEDIATE 'INSERT INTO ' || v_arch_table              -- value via USING, not string-spliced
                               || ' SELECT ft.* FROM ${catalog}.${schema}.FACT_TRANSACTION ft WHERE ft.ACCOUNT_KEY = ?'
                               || ' AND NOT EXISTS (SELECT 1 FROM ' || v_arch_table
                               || ' a WHERE a.TRANSACTION_ID = ft.TRANSACTION_ID)'
                    USING acct.ACCOUNT_KEY;
                EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || v_arch_table || ' WHERE ACCOUNT_KEY = ?'
                    INTO v_txn_count USING acct.ACCOUNT_KEY;                -- ACTIVITY_COUNT: no row-count register
                DELETE FROM ${catalog}.${schema}.FACT_TRANSACTION WHERE ACCOUNT_KEY = acct.ACCOUNT_KEY;
        END CASE;
        UPDATE ${catalog}.${schema}.DIM_ACCOUNT SET ACCOUNT_STATUS = 'ARCHIVED', ETL_UPDATE_TS = current_timestamp()
        WHERE ACCOUNT_KEY = acct.ACCOUNT_KEY;
        SET p_accounts_done = p_accounts_done + 1;                          -- after the commit that leaves the account
    END FOR archive_loop;
    SET p_max_batch = p_max_batch - p_accounts_done;
    UPDATE ${catalog}.${schema}.ARCHIVE_CAMPAIGN_LOCK SET OWNER_RUN_ID = NULL, LOCKED_TS = NULL
    WHERE LOCK_NAME = 'SP_ARCHIVE_CLOSED_ACCOUNTS' AND OWNER_RUN_ID = v_run_id;
END;
