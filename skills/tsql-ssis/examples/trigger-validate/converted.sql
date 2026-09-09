-- Converted under skills/tsql-ssis/SKILL.md §6 "Triggers" (Databricks has no row triggers;
-- the trigger body attaches to every converted writer of the table, or becomes a pipeline
-- expectation for the SDP track: databricks-pipelines references/expectations-sql.md).
-- Two triggers from the fixture, two conversion shapes.

-----------------------------------------------------------------------------------------------
-- 1. trg_validate_loan_amount (FOR UPDATE on loans, RAISERROR 50050 + ROLLBACK)
--    Shape A, analytical/DBSQL track: a pre-check statement that every writer of loans.current_balance
--    runs BEFORE its UPDATE/MERGE, inside the same BEGIN ATOMIC ... END block as that DML (docs
--    /aws/en/transactions/transaction-modes: IF ... SIGNAL ... END IF + MERGE in one non-interactive
--    transaction). The block reads loans from one snapshot, so the pre-check validates exactly the
--    post-image the DML writes and a balance committed by another writer in between cannot bypass it:
--    it either predates the snapshot (and is validated) or fails the block's commit with a write-write
--    conflict (docs /aws/en/transactions/ "Conflict detection and concurrency"). SIGNAL rolls the
--    block back, so the DML never commits (source: rollback after the fact). Run as two statements
--    outside a transaction, the check and the DML race: never ship that form.
-----------------------------------------------------------------------------------------------
-- Inlined into every writer whose SET list names current_balance (IF UPDATE(current_balance) gate;
-- fixture: sp_process_monthly_payments and sp_loan_modification, not sp_apply_late_fees / sp_nightly_accrual,
-- which touch other loans columns) immediately before the UPDATE / MERGE INTO ${catalog}.${schema}.loans
-- (worked instance: examples/proc-cursor-payments/converted.sql, inside its BEGIN ATOMIC block, before
-- the loans MERGE):
--
--   DECLARE negative_balance CONDITION FOR SQLSTATE '45050';  -- source RAISERROR 50050 (outside the block)
--   BEGIN ATOMIC
--   ... the writer's other statements ...
--   IF EXISTS (
--        SELECT 1
--        FROM   ${catalog}.${schema}.loans t
--        JOIN   waterfall w ON t.loan_id = w.loan_id          -- the staged "inserted" image
--        WHERE  t.current_balance - w.principal_due < 0
--          AND  t.loan_status NOT IN ('CO', 'PO')
--   ) THEN
--        INSERT INTO ${catalog}.${schema}.audit_trail
--            (action_type, action_date, table_name, loan_id, old_value, new_value, user_name)
--        SELECT 'BALANCE_VIOLATION', current_timestamp(), 'loans', t.loan_id,
--               cast(t.current_balance AS STRING),
--               cast(t.current_balance - w.principal_due AS STRING),
--               current_user()
--        FROM   ${catalog}.${schema}.loans t JOIN waterfall w ON t.loan_id = w.loan_id
--        WHERE  t.current_balance - w.principal_due < 0 AND t.loan_status NOT IN ('CO', 'PO');
--        SIGNAL negative_balance SET MESSAGE_TEXT = 'Negative balance not allowed for active loans';
--   END IF;
--   MERGE INTO ${catalog}.${schema}.loans t USING waterfall w ON ... ;   -- the guarded DML
--   END;
--
-- Audit-row outcome to record in the tolerance record: ASE logged the violation AND rolled back the
-- whole statement (the audit INSERT inside the trigger is rolled back too unless the caller committed
-- it separately; in the fixture it is inside the caller's transaction, so it is lost). Inside the
-- atomic block the SIGNAL rolls the converted audit row back as well, so BALANCE_VIOLATION counts
-- match the source (0 for a rolled-back attempt). Only a writer that runs the pre-check outside a
-- transaction commits the row; that writer's tolerance record must then allow >= source rows for
-- BALANCE_VIOLATION, and it must also accept the check/DML race described above, so the atomic form
-- is the one to ship.

--    Shape B, SDP track (loans maintained as a streaming table / materialized view):
CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.${schema}.loans_validated (
    CONSTRAINT no_negative_active_balance
        EXPECT (current_balance >= 0 OR loan_status IN ('CO', 'PO')) ON VIOLATION FAIL UPDATE
)
AS SELECT * FROM ${catalog}.${schema}.loans;

-----------------------------------------------------------------------------------------------
-- 2. trg_audit_payment (FOR INSERT on payments): audit row per inserted payment.
--    Shape A: appended to every writer of payments (see proc-cursor-payments/converted.sql).
--    Shape B: a Change Data Feed reader so the audit rows are produced by the platform, not the writer.
-----------------------------------------------------------------------------------------------
-- Writers set the batch predicate they just wrote; the trigger's "inserted" is that predicate.
--   INSERT INTO ${catalog}.${schema}.audit_trail
--       (action_type, action_date, table_name, record_id, loan_id, new_value, user_name)
--   SELECT 'PAYMENT_INS', current_timestamp(), 'payments', p.payment_id, p.loan_id,
--          'type=' || p.payment_type || ' amt=' || cast(p.total_amt AS STRING), current_user()
--   FROM   ${catalog}.${schema}.payments p
--   WHERE  p.batch_id = batch_id;
--
-- Shape B (SDP streaming table fed by CDF; syntax per databricks-pipelines references/streaming-table-sql.md):
CREATE OR REFRESH STREAMING TABLE ${catalog}.${schema}.payment_audit_stream AS
SELECT 'PAYMENT_INS'                                            AS action_type,
       current_timestamp()                                      AS action_date,
       'payments'                                               AS table_name,
       payment_id                                               AS record_id,
       loan_id,
       'type=' || payment_type || ' amt=' || cast(total_amt AS STRING) AS new_value
FROM STREAM(${catalog}.${schema}.payments);
-- Shape B is append-only and observes commits, not statements; it cannot see rows from a rolled-back
-- source statement (neither could the ASE trigger), and it does not share the writer's user_name.
