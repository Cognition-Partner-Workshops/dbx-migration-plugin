-- Target: UC stored procedure (databricks-dbsql references/sql-scripting.md: "FOR Loop", "LEAVE and ITERATE",
-- "CASE Statement", "Handler Declaration" (EXIT only), "SIGNAL and RESIGNAL", "EXECUTE IMMEDIATE",
-- "CREATE PROCEDURE" (INOUT), "Multi-Statement Transactions": BT/ET has no drop-in, see NOTE.md).
-- Row-at-a-time loop kept like-for-like; flagged for a set-based rewrite once recon is green.
CREATE OR REPLACE PROCEDURE ${catalog}.${schema}.SP_ARCHIVE_CLOSED_ACCOUNTS(
    IN    p_closed_before   DATE,
    IN    p_archive_schema  STRING,          -- was p_archive_db (Teradata database == UC schema)
    INOUT p_max_batch       INT,
    OUT   p_accounts_done   INT,
    OUT   p_return_code     INT
)
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
AS BEGIN
    DECLARE v_txn_count  INT;
    DECLARE v_arch_table STRING;
    -- CONTINUE HANDLER FOR NOT FOUND + FETCH loop: not needed, FOR ... DO ends on cursor exhaustion.
    -- ROLLBACK has no equivalent without BT/ET: partial work is kept and the re-run is idempotent, so the INOUT
    -- budget is charged with the accounts whose status UPDATE committed (counter increments after the UPDATE;
    -- an account interrupted mid-triple is still CLOSED, uncounted, and redone by the retry).
    -- Fixed return code: no cited read of SQLCODE/SQLSTATE inside a handler.
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        SET p_return_code = -1;
        SET p_max_batch = p_max_batch - p_accounts_done;
        INSERT INTO ${catalog}.${schema}.ETL_LOG (PROCEDURE_NAME, BATCH_ID, LOG_LEVEL, LOG_MESSAGE, LOG_TS)
        VALUES ('SP_ARCHIVE_CLOSED_ACCOUNTS', p_max_batch, 'ERROR',
                'SQLEXCEPTION after ' || CAST(p_accounts_done AS STRING)
                || ' accounts (partial batch kept; re-run with the returned budget)', current_timestamp());
    END;

    SET p_return_code = 0;
    SET p_accounts_done = 0;
    IF p_max_batch IS NULL OR p_max_batch <= 0 THEN
        SIGNAL SQLSTATE '75001' SET MESSAGE_TEXT = 'p_max_batch must be positive';
    END IF;
    -- Identifier spliced into dynamic SQL (a ? marker cannot carry one): allowlist of build-time constants from the
    -- unit mapping, never the caller's free text. The source had the full exposure through TRIM(p_archive_db).
    IF p_archive_schema IS NULL OR p_archive_schema NOT IN ('${schema}', '${archive_schema}') THEN
        SIGNAL SQLSTATE '75002' SET MESSAGE_TEXT = 'p_archive_schema is not a declared archive target for this unit';
    END IF;

    SET v_arch_table = '${catalog}.' || p_archive_schema || '.FACT_TRANSACTION_ARCH_'
                       || CAST(year(p_closed_before) AS STRING);            -- EXTRACT(YEAR ...) (FORMAT '9999')
    EXECUTE IMMEDIATE 'CREATE TABLE IF NOT EXISTS ' || v_arch_table
                   || ' AS SELECT * FROM ${catalog}.${schema}.FACT_TRANSACTION WHERE 1 = 0';   -- ... WITH NO DATA

    -- BT; ... ET; -> each DML is its own Delta commit; the (archive, delete, mark) triple is made idempotent
    -- instead, so every intermediate state has each TRANSACTION_ID in the fact, the archive, or transiently both.
    archive_loop: FOR acct AS
        SELECT ACCOUNT_KEY, ACCOUNT_TYPE
        FROM ${catalog}.${schema}.DIM_ACCOUNT
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
        UPDATE ${catalog}.${schema}.DIM_ACCOUNT
        SET ACCOUNT_STATUS = 'ARCHIVED', ETL_UPDATE_TS = current_timestamp()
        WHERE ACCOUNT_KEY = acct.ACCOUNT_KEY;
        SET p_accounts_done = p_accounts_done + 1;
    END FOR archive_loop;

    SET p_max_batch = p_max_batch - p_accounts_done;
END;
