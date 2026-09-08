-- Converted under skills/tsql-ssis/SKILL.md §6 (procedure, cursor -> set-based, GOTO -> EXIT HANDLER,
-- RAISERROR -> SIGNAL, @@identity -> business-key read-back, #temp -> CREATE TEMP TABLE,
-- audit trigger trg_audit_payment folded into the writer).
-- Syntax per databricks-dbsql references/sql-scripting.md ("Stored Procedures", "Exception Handling",
-- "SIGNAL", "Variables") and references/materialized-views-pipes.md §2 (temporary tables).
-- Stored procedures are Public Preview (DBR 17.0+) per that reference; the sql_task fallback is the
-- same body without the CREATE PROCEDURE wrapper.

CREATE OR REPLACE PROCEDURE ${catalog}.${schema}.sp_process_monthly_payments(
    IN  processing_date TIMESTAMP_NTZ,
    IN  servicer_id     INT,
    OUT batch_id        BIGINT,
    OUT rc              INT                      -- T-SQL RETURN code (0 ok, 1 failed)
)
LANGUAGE SQL
SQL SECURITY INVOKER
AS BEGIN
    DECLARE run_key        STRING DEFAULT uuid();      -- replaces @@identity read-back
    DECLARE eligible_rows  INT    DEFAULT 0;
    DECLARE total_applied  DECIMAL(19,4) DEFAULT 0;
    DECLARE payment_failed CONDITION FOR SQLSTATE '45001';   -- source RAISERROR 50001

    -- error_handler: (source used @@error + GOTO). Runs on any SQL error, cleans up, re-raises 50001.
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        DROP TABLE IF EXISTS eligible_loans;
        SET rc = 1;
        SIGNAL payment_failed
            SET MESSAGE_TEXT = 'Payment processing failed for batch ' || coalesce(cast(batch_id AS STRING), 'NULL');
    END;

    SET rc = 0;

    -- Step 1: batch record. audit_id is GENERATED ALWAYS AS IDENTITY in the converted table;
    -- read it back by the run_key business key instead of @@identity (trigger-hijackable in ASE).
    -- (SET var = (scalar subquery): databricks-dbsql sql-scripting.md "Variable Assignment (SET)".)
    INSERT INTO ${catalog}.${schema}.audit_trail
        (action_type, action_date, table_name, record_count, user_name, new_value)
    VALUES ('BATCH_START', processing_date, 'payments', 0, current_user(), run_key);

    SET batch_id = (SELECT audit_id
                    FROM ${catalog}.${schema}.audit_trail
                    WHERE action_type = 'BATCH_START' AND new_value = run_key);

    -- Step 2: eligible loans snapshot (SELECT INTO #eligible_loans -> session-scoped temp table).
    CREATE TEMP TABLE eligible_loans AS
    SELECT l.loan_id,
           l.current_balance,
           l.interest_rate,
           l.term_months,
           coalesce(e.total_escrow, CAST(0 AS DECIMAL(19,4))) AS escrow_monthly   -- ISNULL(..., $0.00)
    FROM ${catalog}.${schema}.loans l
    LEFT JOIN (
        SELECT loan_id, SUM(monthly_amount) AS total_escrow
        FROM ${catalog}.${schema}.escrow_accounts
        GROUP BY loan_id
    ) e ON l.loan_id = e.loan_id
    WHERE l.loan_status = 'AC'
      AND l.servicer_id = servicer_id;

    SET eligible_rows = (SELECT count(*) FROM eligible_loans);   -- @@rowcount has no equivalent

    -- Step 3: the cursor loop becomes one set-based INSERT ... SELECT.
    -- MONEY arithmetic in ASE rounds to 4 places at each step: reproduce with round(..., 4)
    -- (§7 "MONEY arithmetic"; recon rule decimal_round places=4 covers residual scale noise).
    CREATE TEMP TABLE waterfall AS
    SELECT loan_id,
           current_balance,
           escrow_monthly,
           interest_due,
           CASE WHEN monthly_pi - interest_due > current_balance
                THEN current_balance
                ELSE round(monthly_pi - interest_due, 4)
           END AS principal_due
    FROM (
        SELECT el.loan_id,
               el.current_balance,
               el.escrow_monthly,
               ${catalog}.${schema}.fn_calculate_amortization(el.current_balance, el.interest_rate, el.term_months) AS monthly_pi,
               round(el.current_balance * (el.interest_rate / 12.0 / 100.0), 4) AS interest_due
        FROM eligible_loans el
    );

    INSERT INTO ${catalog}.${schema}.payments
        (loan_id, payment_date, effective_date,
         principal_amt, interest_amt, escrow_amt, late_fee_amt,
         total_amt, payment_type, batch_id, reversal_flag, created_date)
    SELECT loan_id, processing_date, processing_date,
           principal_due, interest_due, escrow_monthly, 0,
           principal_due + interest_due + escrow_monthly, 'REG', batch_id, 'N', current_timestamp()
    FROM waterfall;

    -- trg_audit_payment (FOR INSERT on payments) folded into the writer: one audit row per payment.
    -- ASE concatenation treats NULL as '' (delta list item 14); payment_type/total_amt are NOT NULL here.
    INSERT INTO ${catalog}.${schema}.audit_trail
        (action_type, action_date, table_name, record_id, loan_id, new_value, user_name)
    SELECT 'PAYMENT_INS', current_timestamp(), 'payments', p.payment_id, p.loan_id,
           'type=' || p.payment_type || ' amt=' || cast(p.total_amt AS STRING), current_user()
    FROM ${catalog}.${schema}.payments p
    WHERE p.batch_id = batch_id;

    -- UPDATE dbo.loans per cursor row -> one MERGE keyed on loan_id.
    MERGE INTO ${catalog}.${schema}.loans t
    USING waterfall w
    ON t.loan_id = w.loan_id
    WHEN MATCHED THEN UPDATE SET
        t.current_balance = t.current_balance - w.principal_due,
        t.modified_date   = current_timestamp();

    SET total_applied = (SELECT coalesce(sum(principal_due + interest_due + escrow_monthly), 0) FROM waterfall);

    -- Step 4: batch record update (CONVERT(VARCHAR(20), money) -> cast to STRING).
    UPDATE ${catalog}.${schema}.audit_trail
    SET record_count = eligible_rows,
        new_value    = cast(total_applied AS STRING)
    WHERE audit_id = batch_id;

    DROP TABLE IF EXISTS waterfall;
    DROP TABLE IF EXISTS eligible_loans;
END;

-- Caller shape (replaces EXEC ... @batch_id OUTPUT / RETURN code check):
--   DECLARE b BIGINT; DECLARE rc INT;
--   CALL ${catalog}.${schema}.sp_process_monthly_payments(TIMESTAMP_NTZ '2026-09-01 00:00:00', 7, b, rc);
