/* Skill-authored minimal fixture (the estate has no cursor / dynamic-SQL procedure); fixture schema
   BANKING_DW.DIM_ACCOUNT, FACT_TRANSACTION, ETL_LOG. Constructs: DECLARE CURSOR / OPEN / FETCH / CLOSE,
   CONTINUE HANDLER FOR NOT FOUND, WHILE + LEAVE, CASE statement, DBC.SysExecSQL, BT/ET, SIGNAL, INOUT. */

REPLACE PROCEDURE BANKING_DW.SP_ARCHIVE_CLOSED_ACCOUNTS(
    IN    p_closed_before   DATE,
    IN    p_archive_db      VARCHAR(30),
    INOUT p_max_batch       INTEGER,
    OUT   p_accounts_done   INTEGER,
    OUT   p_return_code     INTEGER
)
BEGIN
    DECLARE v_account_key   BIGINT;
    DECLARE v_account_type  VARCHAR(20);
    DECLARE v_txn_count     INTEGER;
    DECLARE v_sql           VARCHAR(4000);
    DECLARE v_done          INTEGER DEFAULT 0;

    DECLARE acct_cur CURSOR FOR
        SEL ACCOUNT_KEY, ACCOUNT_TYPE
        FROM BANKING_DW.DIM_ACCOUNT
        WHERE ACCOUNT_STATUS = 'CLOSED'
          AND CLOSE_DATE < p_closed_before
          AND CURRENT_FLAG = 'Y'
        ORDER BY ACCOUNT_KEY;

    DECLARE CONTINUE HANDLER FOR NOT FOUND
        SET v_done = 1;

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        SET p_return_code = SQLCODE;
        INSERT INTO BANKING_DW.ETL_LOG (PROCEDURE_NAME, BATCH_ID, LOG_LEVEL, LOG_MESSAGE, LOG_TS)
        VALUES ('SP_ARCHIVE_CLOSED_ACCOUNTS', p_max_batch, 'ERROR',
                'SQLSTATE ' || SQLSTATE || ' SQLCODE ' || TRIM(SQLCODE (FORMAT '-999999')), CURRENT_TIMESTAMP(0));
    END;

    SET p_return_code = 0;
    SET p_accounts_done = 0;

    IF p_max_batch IS NULL OR p_max_batch <= 0 THEN
        SIGNAL SQLSTATE '75001' SET MESSAGE_TEXT = 'p_max_batch must be positive';
    END IF;
    -- Dynamic DDL: archive table per calendar year, name built at run time
    SET v_sql = 'CREATE MULTISET TABLE ' || TRIM(p_archive_db) || '.FACT_TRANSACTION_ARCH_' ||
                TRIM(EXTRACT(YEAR FROM p_closed_before) (FORMAT '9999')) ||
                ' AS BANKING_DW.FACT_TRANSACTION WITH NO DATA;';
    CALL DBC.SysExecSQL(v_sql);
    BT;
    OPEN acct_cur;
    fetch_loop:
    WHILE v_done = 0 AND p_accounts_done < p_max_batch DO
        FETCH acct_cur INTO v_account_key, v_account_type;
        IF v_done = 1 THEN
            LEAVE fetch_loop;
        END IF;
        CASE v_account_type
            WHEN 'LOAN' THEN
                SET v_txn_count = 0;    -- loans archived by a different process
            ELSE
                SET v_sql = 'INSERT INTO ' || TRIM(p_archive_db) || '.FACT_TRANSACTION_ARCH_' ||
                            TRIM(EXTRACT(YEAR FROM p_closed_before) (FORMAT '9999')) ||
                            ' SEL * FROM BANKING_DW.FACT_TRANSACTION WHERE ACCOUNT_KEY = ' ||
                            TRIM(v_account_key (FORMAT '-(18)9')) || ';';
                CALL DBC.SysExecSQL(v_sql);
                SET v_txn_count = ACTIVITY_COUNT;
                DELETE FROM BANKING_DW.FACT_TRANSACTION WHERE ACCOUNT_KEY = v_account_key;
        END CASE;
        UPDATE BANKING_DW.DIM_ACCOUNT
        SET ACCOUNT_STATUS = 'ARCHIVED', ETL_UPDATE_TS = CURRENT_TIMESTAMP(0)
        WHERE ACCOUNT_KEY = v_account_key;

        SET p_accounts_done = p_accounts_done + 1;
    END WHILE fetch_loop;
    CLOSE acct_cur;
    ET;

    SET p_max_batch = p_max_batch - p_accounts_done;   -- INOUT: remaining budget for the caller
END;
