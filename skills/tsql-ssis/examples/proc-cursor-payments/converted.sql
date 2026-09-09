-- Converted under skills/tsql-ssis/SKILL.md §6 (procedure, cursor -> set-based, GOTO -> EXIT HANDLER,
-- RAISERROR -> SIGNAL, @@identity -> business-key read-back, #temp -> CREATE TEMP TABLE,
-- audit trigger trg_audit_payment folded into the writer, per-loan transaction -> one BEGIN ATOMIC block
-- around every write of the batch).
-- Syntax per databricks-dbsql references/sql-scripting.md ("Stored Procedures", "Exception Handling",
-- "SIGNAL", "Variables", "Multi-Statement Transactions", "SQL Scripting Atomic Blocks"),
-- references/materialized-views-pipes.md §2 (temporary tables) and docs.databricks.com/aws/en/transactions/
-- + /aws/en/transactions/transaction-modes (non-interactive transactions: "BEGIN ATOMIC ... END", automatic
-- commit / automatic rollback, "Use SIGNAL to throw an exception and trigger automatic rollback", IF ... SIGNAL
-- ... END IF + MERGE inside the block in the "Use in scheduled jobs" example, optimistic concurrency control:
-- "conflicts are detected at commit time ... If conflicts exist, your transaction fails", "Transactions can be
-- used with stored procedures and SQL Scripting"). Stored procedures are Public Preview (DBR 17.0+) per
-- sql-scripting.md; the sql_task fallback is the same body without the CREATE PROCEDURE wrapper.
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

    -- error_handler (source: @@error + GOTO, with a per-loan BEGIN TRAN ... COMMIT / ROLLBACK).
    -- Commit unit: the source commits loan by loan, so a failure at loan k leaves loans 1..k-1 applied
    -- (and a rerun would pay them twice, since eligibility is not batch-aware). The set-based conversion
    -- makes the BATCH the commit unit: on failure nothing is applied, which is the source's own state
    -- when loan 1 fails and the only state a rerun can safely start from. Recorded as a per-unit
    -- decision (SKILL §6 transactions row), not silently assumed.
    -- The compound statement is NOT ATOMIC by default (sql-scripting.md "Key rules"), so every write of
    -- the batch (payments, PAYMENT_INS audit rows, the balance guard, the loans MERGE, the batch-record
    -- update) sits in ONE nested BEGIN ATOMIC ... END block below (docs /transactions/: all statements
    -- "succeed together or roll back together"). Consequences the handler relies on:
    --   * nothing from the block is ever visible to it: a statement failure, the SIGNAL from the balance
    --     guard, or a commit-time conflict with a concurrent writer all roll the whole block back
    --     automatically, so there is no compensating DELETE and no balance reversal here. A reversal
    --     (`balance + principal_due`) would be wrong under Databricks' optimistic concurrency: it could
    --     run against a balance another writer committed after this block started (docs /transactions/
    --     "Conflict detection and concurrency"). The transaction does that job instead: a concurrent
    --     UPDATE of any loan row the MERGE touches is a write-write conflict, the commit fails, and the
    --     caller retries with fresh data (docs /transactions/ "Best practices": "Build retry logic").
    --   * the BATCH_START row is KEPT: the source inserts it outside every transaction and error_handler
    --     never removes it, so a failed source run always leaves one BATCH_START row with record_count 0
    --     and new_value NULL (Step 4 never ran). It is written outside the atomic block here for the same
    --     reason. Deleting it would produce a state the source cannot produce and lose the batch id the
    --     SIGNAL message reports. The handler only clears the run_key from new_value so the row matches
    --     the source's failed-batch row column for column.
    -- Requirements (docs /transactions/ "Requirements"): payments, audit_trail and loans must be Unity
    -- Catalog managed tables with catalog commits enabled (sql-scripting.md: created with
    -- TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported'); existing tables cannot be upgraded in
    -- place), on a SQL warehouse, serverless compute, or DBR 18.0+. Multi-statement transactions are
    -- Preview per sql-scripting.md "Multi-Statement Transactions".
    -- If a table cannot meet them, do NOT fall back to the delta-reversal handler: use the compare-and-set
    -- form instead (the MERGE's WHEN MATCHED carries `AND t.current_balance = w.current_balance` against
    -- the snapshot pre-image and stamps t.modified_date with a run-owned timestamp; a matched-row count
    -- below count(waterfall) is a conflict that SIGNALs; the handler reverses only rows still carrying the
    -- run's timestamp and writes a BALANCE_CONFLICT audit row for any it left alone). NOTE.md records
    -- both shapes; only the atomic one is executable here.
    -- Statement order matches the source: BATCH_START first (Step 1), then the snapshot. A failure in
    -- the snapshot or waterfall CREATE therefore still leaves the source-shaped BATCH_START row and a
    -- reportable p_batch_id, exactly like the source's GOTO error_handler from Step 2. Only a failure of
    -- the BATCH_START INSERT itself leaves nothing permanent (p_batch_id NULL, the UPDATE below matches
    -- no rows), which is also the source's state when its Step 1 fails.
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        UPDATE ${catalog}.${schema}.audit_trail
        SET new_value = NULL, record_count = 0
        WHERE action_type = 'BATCH_START' AND new_value = run_key;   -- keep the row, source-shaped
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

    -- Writes 1-5: one non-interactive transaction (docs /transactions/transaction-modes "Non-interactive
    -- transactions"). Every table read inside the block is read from one snapshot taken at first access
    -- (docs /transactions/ "Transaction isolation": repeatable reads), so the balance guard and the MERGE
    -- see the SAME loans rows: the guard validates exactly the post-image the MERGE writes, and a balance
    -- another writer commits in between cannot slip past it. If such a writer commits before this block
    -- does, the commit fails with a write-write conflict on the shared loan rows and the block rolls back
    -- (docs /transactions/ "Conflict scenarios"); the handler then reports SQLSTATE 45001 and the caller
    -- re-runs the procedure, which re-snapshots eligible_loans from fresh data.
    -- Not verified live: session-scoped temp tables (waterfall) as a read source inside BEGIN ATOMIC (the
    -- docs list Unity Catalog tables, streaming tables, views and materialized views). If rejected, define
    -- waterfall / eligible_loans as CREATE TEMPORARY VIEW: read inside the block they resolve against the
    -- block's loans snapshot, which is what the source's SELECT INTO #eligible_loans captured anyway.
    BEGIN ATOMIC
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
        WHERE p.batch_id = p_batch_id;                   -- was `p.batch_id = batch_id`: column = itself

        -- trg_validate_loan_amount (FOR UPDATE on loans, IF UPDATE(current_balance)) folded into the writer
        -- as the pre-check from examples/trigger-validate/converted.sql, Shape A. `inserted` is the post-image
        -- the MERGE below writes (t.current_balance - w.principal_due), `deleted` the current row. On a hit
        -- the source trigger logged BALANCE_VIOLATION, RAISERROR 50050 and ROLLBACK'd the per-loan
        -- transaction (the UPDATE, that loan's payment INSERT and the trigger's own audit row), and the
        -- caller's @@error check jumped to error_handler. Here SIGNAL fails the atomic block, which rolls
        -- back the batch's payments, PAYMENT_INS rows AND this BALANCE_VIOLATION row (docs
        -- /transactions/ "Error handling and rollback": SIGNAL triggers automatic rollback), the same
        -- end state as the source's rollback; then the EXIT HANDLER runs. The audit row is still written
        -- so a writer that runs this block outside a transaction (NOT ATOMIC) keeps the source trigger's
        -- logging intent; trigger-validate/NOTE.md records both outcomes. The waterfall already clamps
        -- principal_due to the snapshot balance, so within the block's own snapshot this can fire only
        -- when the snapshot itself was taken before a concurrent writer lowered current_balance.
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

        -- UPDATE dbo.loans per cursor row -> one MERGE keyed on loan_id, over the same snapshot the guard read.
        MERGE INTO ${catalog}.${schema}.loans t
        USING waterfall w
        ON t.loan_id = w.loan_id
        WHEN MATCHED THEN UPDATE SET
            t.current_balance = t.current_balance - w.principal_due,
            t.modified_date   = current_timestamp();

        -- Step 4: batch record update (CONVERT(VARCHAR(20), money) -> cast to STRING). Inside the block so
        -- a failed batch never reports a record_count / total (source: Step 4 is skipped by the GOTO).
        UPDATE ${catalog}.${schema}.audit_trail
        SET record_count = eligible_rows,
            new_value    = cast(total_applied AS STRING)
        WHERE audit_id = p_batch_id;
    END;

    DROP TABLE IF EXISTS waterfall;
    DROP TABLE IF EXISTS eligible_loans;
END;

-- Caller shape (replaces EXEC ... @batch_id OUTPUT / RETURN code check):
--   DECLARE b BIGINT; DECLARE rc INT;
--   CALL ${catalog}.${schema}.sp_process_monthly_payments(TIMESTAMP_NTZ '2026-09-01 00:00:00', 7, b, rc);
