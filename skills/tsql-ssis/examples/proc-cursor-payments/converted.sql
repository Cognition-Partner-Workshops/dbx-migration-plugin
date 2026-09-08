-- Converted under skills/tsql-ssis/SKILL.md §6 (procedure, cursor -> set-based, GOTO -> EXIT HANDLER,
-- RAISERROR -> SIGNAL, @@identity -> business-key read-back, #temp -> CREATE TEMP TABLE,
-- audit trigger trg_audit_payment folded into the writer, per-loan transaction -> compensating handler).
-- Syntax per databricks-dbsql references/sql-scripting.md ("Stored Procedures", "Exception Handling",
-- "SIGNAL", "Variables", "Multi-Statement Transactions") and references/materialized-views-pipes.md §2
-- (temporary tables). Stored procedures are Public Preview (DBR 17.0+) per that reference; the sql_task
-- fallback is the same body without the CREATE PROCEDURE wrapper.
--
-- Parameter naming: Databricks resolves an unqualified name as a COLUMN before a parameter or local
-- variable (docs sql-ref-name-resolution: "Columns and parameter win over fields and keys", column
-- references are tried before variables). The source's @servicer_id / @batch_id have no such ambiguity
-- because of the @ sigil, so every parameter is prefixed p_ here and no p_/local name may equal a column
-- of any table the body touches (SKILL §7 "parameter shadowing").

CREATE OR REPLACE PROCEDURE ${catalog}.${schema}.sp_process_monthly_payments(
    IN  p_processing_date TIMESTAMP_NTZ,
    IN  p_servicer_id     INT,
    OUT p_batch_id        BIGINT,
    OUT p_rc              INT                    -- T-SQL RETURN code (0 ok, 1 failed)
)
LANGUAGE SQL
SQL SECURITY INVOKER
AS BEGIN
    DECLARE run_key        STRING DEFAULT uuid();      -- replaces @@identity read-back
    DECLARE eligible_rows  INT    DEFAULT 0;
    DECLARE total_applied  DECIMAL(19,4) DEFAULT 0;
    DECLARE payment_failed CONDITION FOR SQLSTATE '45001';   -- source RAISERROR 50001
    DECLARE negative_balance CONDITION FOR SQLSTATE '45050'; -- trg_validate_loan_amount RAISERROR 50050
    DECLARE balances_applied BOOLEAN DEFAULT false;          -- set after the loans MERGE commits

    -- error_handler (source: @@error + GOTO, with a per-loan BEGIN TRAN ... COMMIT / ROLLBACK).
    -- Commit unit: the source commits loan by loan, so a failure at loan k leaves loans 1..k-1 applied
    -- (and a rerun would pay them twice, since eligibility is not batch-aware). The set-based conversion
    -- makes the BATCH the commit unit: on failure nothing is applied, which is the source's own state
    -- when loan 1 fails and the only state a rerun can safely start from. Recorded as a per-unit
    -- decision (SKILL §6 transactions row), not silently assumed.
    -- The compound statement is NOT ATOMIC by default (sql-scripting.md "Key rules"), so a failure after
    -- the payments INSERT would otherwise leave payments with no balance update, a state the source can
    -- never produce. The handler therefore undoes ONLY this invocation's writes, in reverse order, each
    -- guarded by an ownership predicate so concurrent writers are never overwritten:
    --   * audit rows: owned through payments.batch_id (= this invocation's audit_id),
    --   * payments: owned by batch_id,
    --   * loans: the MERGE is one atomic statement, so balances_applied tells whether it ran; if so the
    --     reversal ADDS BACK w.principal_due per loan (a delta, not an absolute restore from the
    --     eligible_loans snapshot, which would erase any balance change a concurrent writer made in
    --     between). modified_date is left as written; NOTE.md excludes it from the failure-replay Tier 3.
    --   * the BATCH_START row is KEPT: the source inserts it outside every transaction and error_handler
    --     never removes it, so a failed source run always leaves one BATCH_START row with record_count 0
    --     and new_value NULL (Step 4 never ran). Deleting it would produce a state the source cannot
    --     produce and lose the batch id the SIGNAL message reports. The handler only clears the run_key
    --     from new_value so the row matches the source's failed-batch row column for column.
    -- Alternative when payments, audit_trail and loans are all created with
    -- TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported'): wrap the four writes below in
    -- BEGIN ATOMIC ... END (sql-scripting.md "SQL Scripting Atomic Blocks", Preview) and drop the
    -- compensation. Not verified live: BEGIN ATOMIC nested inside a procedure body.
    -- Statement order matches the source: BATCH_START first (Step 1), then the snapshot. A failure in
    -- the snapshot or waterfall CREATE therefore still leaves the source-shaped BATCH_START row and a
    -- reportable p_batch_id, exactly like the source's GOTO error_handler from Step 2. Only a failure of
    -- the BATCH_START INSERT itself leaves nothing permanent (p_batch_id NULL, every compensating
    -- statement matches no rows), which is also the source's state when its Step 1 fails.
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        DELETE FROM ${catalog}.${schema}.audit_trail
        WHERE action_type = 'PAYMENT_INS'
          AND table_name  = 'payments'
          AND record_id IN (SELECT payment_id FROM ${catalog}.${schema}.payments WHERE batch_id = p_batch_id);
        DELETE FROM ${catalog}.${schema}.payments WHERE batch_id = p_batch_id;
        UPDATE ${catalog}.${schema}.audit_trail
        SET new_value = NULL, record_count = 0
        WHERE action_type = 'BATCH_START' AND new_value = run_key;   -- keep the row, source-shaped
        IF balances_applied THEN
            MERGE INTO ${catalog}.${schema}.loans t
            USING waterfall w
            ON t.loan_id = w.loan_id
            WHEN MATCHED THEN UPDATE SET
                t.current_balance = t.current_balance + w.principal_due;
        END IF;
        DROP TABLE IF EXISTS waterfall;
        DROP TABLE IF EXISTS eligible_loans;
        SET p_rc = 1;
        SIGNAL payment_failed
            SET MESSAGE_TEXT = 'Payment processing failed for batch ' || coalesce(cast(p_batch_id AS STRING), 'NULL');
    END;

    SET p_rc = 0;

    -- Step 1: batch record. audit_id is GENERATED ALWAYS AS IDENTITY in the converted table;
    -- read it back by the run_key business key instead of @@identity (trigger-hijackable in ASE).
    -- (SET var = (scalar subquery): databricks-dbsql sql-scripting.md "Variable Assignment (SET)".)
    -- Written BEFORE the snapshot, as in the source, so every later failure is attributable to a batch.
    INSERT INTO ${catalog}.${schema}.audit_trail
        (action_type, action_date, table_name, record_count, user_name, new_value)
    VALUES ('BATCH_START', p_processing_date, 'payments', 0, current_user(), run_key);

    SET p_batch_id = (SELECT audit_id
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
      AND l.servicer_id = p_servicer_id;                -- column vs parameter: no shadowing possible

    SET eligible_rows = (SELECT count(*) FROM eligible_loans);   -- @@rowcount has no equivalent

    -- Step 3: the cursor loop becomes one set-based INSERT ... SELECT.
    -- MONEY arithmetic in ASE rounds to 4 places at each step: reproduce with round(..., 4)
    -- (§7 "MONEY arithmetic"; recon rule decimal_round places=4 half_up covers residual scale noise).
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

    SET total_applied = (SELECT coalesce(sum(principal_due + interest_due + escrow_monthly), 0) FROM waterfall);

    -- Writes 1-4 (compensated by the handler above).
    INSERT INTO ${catalog}.${schema}.payments
        (loan_id, payment_date, effective_date,
         principal_amt, interest_amt, escrow_amt, late_fee_amt,
         total_amt, payment_type, batch_id, reversal_flag, created_date)
    SELECT w.loan_id, p_processing_date, p_processing_date,
           w.principal_due, w.interest_due, w.escrow_monthly, 0,
           w.principal_due + w.interest_due + w.escrow_monthly, 'REG', p_batch_id, 'N', current_timestamp()
    FROM waterfall w;

    -- trg_audit_payment (FOR INSERT on payments) folded into the writer: one audit row per payment.
    -- ASE concatenation treats NULL as '' (delta list item 14); payment_type/total_amt are NOT NULL here.
    INSERT INTO ${catalog}.${schema}.audit_trail
        (action_type, action_date, table_name, record_id, loan_id, new_value, user_name)
    SELECT 'PAYMENT_INS', current_timestamp(), 'payments', p.payment_id, p.loan_id,
           'type=' || p.payment_type || ' amt=' || cast(p.total_amt AS STRING), current_user()
    FROM ${catalog}.${schema}.payments p
    WHERE p.batch_id = p_batch_id;                       -- was `p.batch_id = batch_id`: column = itself

    -- trg_validate_loan_amount (FOR UPDATE on loans, IF UPDATE(current_balance)) folded into the writer as
    -- the pre-check from examples/trigger-validate/converted.sql, Shape A. `inserted` is the post-image the
    -- MERGE below would write (t.current_balance - w.principal_due), `deleted` the current row. On a hit the
    -- source trigger logged BALANCE_VIOLATION, RAISERROR 50050 and ROLLBACK'd the per-loan transaction (the
    -- UPDATE, that loan's payment INSERT and the trigger's own audit row), and the caller's @@error check
    -- jumped to error_handler; here SIGNAL raises before any balance changes and the EXIT HANDLER removes
    -- the whole batch's payments / PAYMENT_INS rows (balances_applied is still false, so no reversal). The
    -- BALANCE_VIOLATION row survives on the converted side: the tolerance-record difference recorded in
    -- trigger-validate/NOTE.md. The waterfall already clamps principal_due to the snapshot balance, so in
    -- practice this fires only when a concurrent writer lowered current_balance after the snapshot.
    IF EXISTS (
        SELECT 1
        FROM ${catalog}.${schema}.loans t
        JOIN waterfall w ON t.loan_id = w.loan_id
        WHERE t.current_balance - w.principal_due < 0
          AND t.loan_status NOT IN ('CO', 'PO')
    ) THEN
        INSERT INTO ${catalog}.${schema}.audit_trail
            (action_type, action_date, table_name, loan_id, old_value, new_value, user_name)
        SELECT 'BALANCE_VIOLATION', current_timestamp(), 'loans', t.loan_id,
               cast(t.current_balance AS STRING),
               cast(t.current_balance - w.principal_due AS STRING),
               current_user()
        FROM ${catalog}.${schema}.loans t
        JOIN waterfall w ON t.loan_id = w.loan_id
        WHERE t.current_balance - w.principal_due < 0
          AND t.loan_status NOT IN ('CO', 'PO');
        SIGNAL negative_balance SET MESSAGE_TEXT = 'Negative balance not allowed for active loans';
    END IF;

    -- UPDATE dbo.loans per cursor row -> one MERGE keyed on loan_id.
    MERGE INTO ${catalog}.${schema}.loans t
    USING waterfall w
    ON t.loan_id = w.loan_id
    WHEN MATCHED THEN UPDATE SET
        t.current_balance = t.current_balance - w.principal_due,
        t.modified_date   = current_timestamp();
    SET balances_applied = true;

    -- Step 4: batch record update (CONVERT(VARCHAR(20), money) -> cast to STRING).
    UPDATE ${catalog}.${schema}.audit_trail
    SET record_count = eligible_rows,
        new_value    = cast(total_applied AS STRING)
    WHERE audit_id = p_batch_id;

    DROP TABLE IF EXISTS waterfall;
    DROP TABLE IF EXISTS eligible_loans;
END;

-- Caller shape (replaces EXEC ... @batch_id OUTPUT / RETURN code check):
--   DECLARE b BIGINT; DECLARE rc INT;
--   CALL ${catalog}.${schema}.sp_process_monthly_payments(TIMESTAMP_NTZ '2026-09-01 00:00:00', 7, b, rc);
