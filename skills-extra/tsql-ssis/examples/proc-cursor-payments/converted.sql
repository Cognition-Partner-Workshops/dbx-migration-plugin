-- SKILL.md canonical DBSQL shape: cursor -> set-based, GOTO -> EXIT HANDLER, RAISERROR -> SIGNAL,
-- @@identity -> business-key read-back, #temp -> TEMP TABLE, triggers folded into the writer, per-loan
-- transactions -> one BEGIN ATOMIC block. p_ prefix: a bare name resolves as a column before a variable.
CREATE OR REPLACE PROCEDURE ${catalog}.${schema}.sp_process_monthly_payments(
    IN  p_processing_date TIMESTAMP_NTZ,
    IN  p_servicer_id     INT,
    OUT p_batch_id        BIGINT,
    OUT p_rc              INT                                   -- T-SQL RETURN code
)
LANGUAGE SQL SQL SECURITY INVOKER
AS BEGIN
    DECLARE run_key          STRING        DEFAULT uuid();
    DECLARE eligible_rows    INT           DEFAULT 0;
    DECLARE total_applied    DECIMAL(19,4) DEFAULT 0;
    DECLARE payment_failed   CONDITION FOR SQLSTATE '45001';   -- RAISERROR 50001
    DECLARE negative_balance CONDITION FOR SQLSTATE '45050';   -- trg_validate_loan_amount 50050

    DECLARE EXIT HANDLER FOR SQLEXCEPTION                      -- error_handler:
    BEGIN
        UPDATE ${catalog}.${schema}.audit_trail                -- keep the row, as the source does
        SET new_value = NULL, record_count = 0
        WHERE action_type = 'BATCH_START' AND new_value = run_key;
        DROP TABLE IF EXISTS waterfall; DROP TABLE IF EXISTS eligible_loans;
        SET p_rc = 1;
        SIGNAL payment_failed SET MESSAGE_TEXT =
            'Payment processing failed for batch ' || coalesce(cast(p_batch_id AS STRING), 'NULL');
    END;
    SET p_rc = 0;

    INSERT INTO ${catalog}.${schema}.audit_trail
        (action_type, action_date, table_name, record_count, user_name, new_value)
    VALUES ('BATCH_START', p_processing_date, 'payments', 0, current_user(), run_key);
    SET p_batch_id = (SELECT audit_id FROM ${catalog}.${schema}.audit_trail
                      WHERE action_type = 'BATCH_START' AND new_value = run_key);

    CREATE TEMP TABLE eligible_loans AS
    SELECT l.loan_id, l.current_balance, l.interest_rate, l.term_months,
           coalesce(e.total_escrow, CAST(0 AS DECIMAL(19,4))) AS escrow_monthly
    FROM ${catalog}.${schema}.loans l
    LEFT JOIN (SELECT loan_id, SUM(monthly_amount) AS total_escrow
               FROM ${catalog}.${schema}.escrow_accounts GROUP BY loan_id) e ON l.loan_id = e.loan_id
    WHERE l.loan_status = 'AC' AND l.servicer_id = p_servicer_id;
    SET eligible_rows = (SELECT count(*) FROM eligible_loans);   -- @@rowcount
    CREATE TEMP TABLE waterfall AS                               -- the cursor loop, MONEY rounds to 4
    SELECT loan_id, escrow_monthly, interest_due,
           CASE WHEN monthly_pi - interest_due > current_balance THEN current_balance
                ELSE round(monthly_pi - interest_due, 4) END AS principal_due
    FROM (SELECT el.*,
                 ${catalog}.${schema}.fn_calculate_amortization(el.current_balance, el.interest_rate, el.term_months) AS monthly_pi,
                 round(el.current_balance * (el.interest_rate / 12.0 / 100.0), 4) AS interest_due
          FROM eligible_loans el);
    SET total_applied = (SELECT coalesce(sum(principal_due + interest_due + escrow_monthly), 0) FROM waterfall);

    BEGIN ATOMIC                                                 -- all writes commit or roll back together
        INSERT INTO ${catalog}.${schema}.payments
            (loan_id, payment_date, effective_date, principal_amt, interest_amt, escrow_amt,
             late_fee_amt, total_amt, payment_type, batch_id, reversal_flag, created_date)
        SELECT w.loan_id, p_processing_date, p_processing_date, w.principal_due, w.interest_due, w.escrow_monthly,
               0, w.principal_due + w.interest_due + w.escrow_monthly, 'REG', p_batch_id, 'N', current_timestamp()
        FROM waterfall w;
        INSERT INTO ${catalog}.${schema}.audit_trail             -- trg_audit_payment folded in
            (action_type, action_date, table_name, record_id, loan_id, new_value, user_name)
        SELECT 'PAYMENT_INS', current_timestamp(), 'payments', p.payment_id, p.loan_id,
               'type=' || p.payment_type || ' amt=' || cast(p.total_amt AS STRING), current_user()
        FROM ${catalog}.${schema}.payments p WHERE p.batch_id = p_batch_id;
        IF EXISTS (SELECT 1 FROM ${catalog}.${schema}.loans t   -- trg_validate_loan_amount folded in
                   JOIN waterfall w ON t.loan_id = w.loan_id
                   WHERE t.current_balance - w.principal_due < 0 AND t.loan_status NOT IN ('CO', 'PO')) THEN
            SIGNAL negative_balance SET MESSAGE_TEXT = 'Negative balance not allowed for active loans';
        END IF;
        MERGE INTO ${catalog}.${schema}.loans t USING waterfall w ON t.loan_id = w.loan_id
        WHEN MATCHED THEN UPDATE SET t.current_balance = t.current_balance - w.principal_due,
                                     t.modified_date   = current_timestamp();
        UPDATE ${catalog}.${schema}.audit_trail
        SET record_count = eligible_rows, new_value = cast(total_applied AS STRING)
        WHERE audit_id = p_batch_id;
    END;
    DROP TABLE IF EXISTS waterfall; DROP TABLE IF EXISTS eligible_loans;
END;
