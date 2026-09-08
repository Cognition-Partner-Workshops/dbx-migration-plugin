-- Target: UC stored procedure (databricks-dbsql references/sql-scripting.md). Sections used:
--   "Control Flow / FOR Loop" (cursor loops), "WHILE Loop", "LEAVE and ITERATE", "CASE Statement",
--   "Exception Handling / Handler Declaration" (EXIT only; NOT FOUND is a condition value, not a CONTINUE handler),
--   "SIGNAL and RESIGNAL", "EXECUTE IMMEDIATE (Dynamic SQL)", "Stored Procedures / CREATE PROCEDURE" (INOUT),
--   "Multi-Statement Transactions" (BT/ET has no drop-in; see body).
-- Row-at-a-time cursor + per-row DELETE is the Teradata shape; the target keeps the loop (first pass) so behaviour is
-- traceable to the source, and the skill §11 risk heuristic flags it for a set-based rewrite once recon is green.

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
    DECLARE v_year       STRING;

    -- CONTINUE HANDLER FOR NOT FOUND + FETCH loop: not needed; FOR ... DO iterates the cursor and ends on exhaustion.
    -- EXIT HANDLER: ROLLBACK has no cited equivalent for a multi-statement compound (no BT/ET); the handler records
    -- the failure and leaves partial work to be repaired by the idempotent re-run (accounts already ARCHIVED are
    -- skipped by the cursor predicate). Because that partial work is *kept* (the source rolled it back), the INOUT
    -- budget must be reduced by the accounts already completed, otherwise a retry with the caller's unchanged
    -- p_max_batch archives a full second batch on top of the partial one. p_accounts_done is incremented only after
    -- an account's status UPDATE, so an account interrupted mid-triple is not counted here and is redone (and then
    -- counted) by the retry. Fixed return code: no cited SQLCODE/SQLSTATE read (example 04).
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        SET p_return_code = -1;
        SET p_max_batch = p_max_batch - p_accounts_done;                    -- budget consumed by the kept partial work
        INSERT INTO ${catalog}.${schema}.ETL_LOG (PROCEDURE_NAME, BATCH_ID, LOG_LEVEL, LOG_MESSAGE, LOG_TS)
        VALUES ('SP_ARCHIVE_CLOSED_ACCOUNTS', p_max_batch, 'ERROR',
                'SQLEXCEPTION during archive after ' || CAST(p_accounts_done AS STRING)
                || ' accounts (partial batch kept; re-run with the returned budget is idempotent)', current_timestamp());
    END;

    SET p_return_code = 0;
    SET p_accounts_done = 0;

    IF p_max_batch IS NULL OR p_max_batch <= 0 THEN
        SIGNAL SQLSTATE '75001' SET MESSAGE_TEXT = 'p_max_batch must be positive';
    END IF;

    -- p_archive_schema is spliced into dynamic SQL as an identifier (a `?` marker cannot carry an identifier), so it is
    -- checked against the unit's declared archive schemas before any EXECUTE IMMEDIATE runs: ${schema} and
    -- ${archive_schema} are build-time constants substituted from the unit mapping (the same place ${catalog}.${schema}
    -- come from), so the set of schemas this procedure can write is fixed at build time, not chosen by the caller.
    -- An allowlist is stricter than an identifier-shape check: a well-formed name for some other schema the invoker
    -- happens to own is rejected too. The source had the full exposure through TRIM(p_archive_db).
    IF p_archive_schema IS NULL OR p_archive_schema NOT IN ('${schema}', '${archive_schema}') THEN
        SIGNAL SQLSTATE '75002' SET MESSAGE_TEXT = 'p_archive_schema is not a declared archive target for this unit';
    END IF;

    -- Dynamic DDL via EXECUTE IMMEDIATE. ${catalog} is a literal, so the catalog cannot move either; the factory's
    -- write-scope hook (.migration/allowed_targets.json) is the outer gate on the catalog.
    SET v_year = CAST(year(p_closed_before) AS STRING);                     -- EXTRACT(YEAR ...) (FORMAT '9999')
    SET v_arch_table = '${catalog}.' || p_archive_schema || '.FACT_TRANSACTION_ARCH_' || v_year;
    EXECUTE IMMEDIATE 'CREATE TABLE IF NOT EXISTS ' || v_arch_table
                   || ' AS SELECT * FROM ${catalog}.${schema}.FACT_TRANSACTION WHERE 1 = 0';   -- ... WITH NO DATA

    -- BT; ... ET;  -> no multi-statement transaction around the loop (see header). Each DML statement is its own
    -- atomic Delta commit; the (archive, delete, mark) triple per account is made idempotent instead: the archive
    -- INSERT skips TRANSACTION_IDs already present in the archive table, so a re-run after a failure between the
    -- INSERT and the DELETE copies nothing twice, and the DELETE/UPDATE that follow are naturally repeatable.
    -- Every intermediate state therefore satisfies: each TRANSACTION_ID is in FACT_TRANSACTION, in the archive, or
    -- (transiently) in both -- never lost, and never twice in the archive.

    archive_loop: FOR acct AS
        SELECT ACCOUNT_KEY, ACCOUNT_TYPE
        FROM ${catalog}.${schema}.DIM_ACCOUNT
        WHERE ACCOUNT_STATUS = 'CLOSED'
          AND CLOSE_DATE < p_closed_before
          AND CURRENT_FLAG = 'Y'
        ORDER BY ACCOUNT_KEY
    DO
        IF p_accounts_done >= p_max_batch THEN
            LEAVE archive_loop;                                             -- WHILE ... p_accounts_done < p_max_batch
        END IF;

        CASE acct.ACCOUNT_TYPE
            WHEN 'LOAN' THEN
                SET v_txn_count = 0;
            ELSE
                -- Parameter marker instead of string-splicing the key (EXECUTE IMMEDIATE ... USING);
                -- NOT EXISTS on TRANSACTION_ID makes the copy idempotent (retry-safe).
                EXECUTE IMMEDIATE 'INSERT INTO ' || v_arch_table
                               || ' SELECT ft.* FROM ${catalog}.${schema}.FACT_TRANSACTION ft WHERE ft.ACCOUNT_KEY = ?'
                               || ' AND NOT EXISTS (SELECT 1 FROM ' || v_arch_table
                               || ' a WHERE a.TRANSACTION_ID = ft.TRANSACTION_ID)'
                    USING acct.ACCOUNT_KEY;
                -- ACTIVITY_COUNT -> count what was archived for this key (no cited row-count register)
                EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || v_arch_table || ' WHERE ACCOUNT_KEY = ?'
                    INTO v_txn_count USING acct.ACCOUNT_KEY;

                DELETE FROM ${catalog}.${schema}.FACT_TRANSACTION WHERE ACCOUNT_KEY = acct.ACCOUNT_KEY;
        END CASE;

        -- Charge the budget *before* the account leaves the cursor predicate. The budget is an upper bound: if the
        -- UPDATE below fails, the account is still CLOSED, the retry redoes it (idempotently) and it is counted twice
        -- (one short of the cap). The other order lets a retry skip an ARCHIVED-but-uncounted account and archive one
        -- past the cap.
        SET p_accounts_done = p_accounts_done + 1;

        UPDATE ${catalog}.${schema}.DIM_ACCOUNT
        SET ACCOUNT_STATUS = 'ARCHIVED', ETL_UPDATE_TS = current_timestamp()
        WHERE ACCOUNT_KEY = acct.ACCOUNT_KEY;
    END FOR archive_loop;

    -- Second pass: FOR cursor loop maps 1:1
    FOR typ AS
        SELECT ACCOUNT_TYPE, COUNT(*) AS N
        FROM ${catalog}.${schema}.DIM_ACCOUNT
        WHERE ACCOUNT_STATUS = 'ARCHIVED' AND CAST(ETL_UPDATE_TS AS DATE) = current_date()
        GROUP BY ACCOUNT_TYPE
    DO
        INSERT INTO ${catalog}.${schema}.ETL_LOG (PROCEDURE_NAME, BATCH_ID, LOG_LEVEL, LOG_MESSAGE, LOG_TS)
        VALUES ('SP_ARCHIVE_CLOSED_ACCOUNTS', p_max_batch, 'INFO',
                'Archived ' || CAST(typ.N AS STRING) || ' accounts of type ' || typ.ACCOUNT_TYPE,
                current_timestamp());
    END FOR;

    SET p_max_batch = p_max_batch - p_accounts_done;
END;
