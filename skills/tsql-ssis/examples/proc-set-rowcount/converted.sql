-- Converted under skills/tsql-ssis/SKILL.md §6 "WHILE 1=1 ... SET ROWCOUNT" and §7
-- "@@ROWCOUNT-driven logic and SET ROWCOUNT batching": the batching loop collapses to one
-- atomic UPDATE (every Delta DML statement is atomic: databricks-dbsql references/sql-scripting.md
-- "Multi-Statement Transactions"). $ money literals -> DECIMAL(19,4) literals (§5 row 84).
-- @@identity is not used for anything in the source (the comment only warns about it); the audit
-- row is a plain INSERT.

CREATE OR REPLACE PROCEDURE ${catalog}.${schema}.sp_apply_late_fees(
    IN  p_cutoff_date TIMESTAMP_NTZ,
    OUT p_rc          INT
)
LANGUAGE SQL
SQL SECURITY INVOKER
AS BEGIN
    DECLARE total_rows INT DEFAULT 0;

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        DROP TABLE IF EXISTS fee_targets;   -- session-scoped: a retry in the same session must be able to recreate it
        SET p_rc = 1;
        RESIGNAL;
    END;

    SET p_rc = 0;

    -- Fee schedule computed once, before the update, from the same predicate the UPDATE uses.
    -- The source also accumulates @total_fees (and @batch_count) from a second query keyed on
    -- late_fee_assessed = MAX(...), but never emits either: not an output, not audited, not
    -- returned. They are dropped here rather than written anywhere the source does not write.
    CREATE TEMP TABLE fee_targets AS
    SELECT loan_id,
           CASE rtrim(loan_type)              -- CHAR(4): 'FHA ' / 'VA  ' padding (§7)
               WHEN 'CONV' THEN CAST(45.00 AS DECIMAL(19,4))
               WHEN 'FHA'  THEN CAST(35.00 AS DECIMAL(19,4))
               WHEN 'VA'   THEN CAST(0.00  AS DECIMAL(19,4))   -- VA loans: no late fees (regulatory)
               WHEN 'USDA' THEN CAST(30.00 AS DECIMAL(19,4))
               ELSE             CAST(50.00 AS DECIMAL(19,4))
           END AS fee_amt
    FROM ${catalog}.${schema}.loans
    WHERE loan_status = 'DL'
      AND days_past_due >= 15
      AND (last_fee_date IS NULL OR last_fee_date < p_cutoff_date)
      AND rtrim(loan_type) != 'VA';       -- defense in depth, as in the source

    SET total_rows = (SELECT count(*) FROM fee_targets);   -- sum of per-batch @@rowcount

    -- SET ROWCOUNT 1000 / WHILE 1=1 / IF @@rowcount = 0 BREAK / SET ROWCOUNT 0  ->  one MERGE.
    MERGE INTO ${catalog}.${schema}.loans t
    USING fee_targets f
    ON t.loan_id = f.loan_id
    WHEN MATCHED THEN UPDATE SET
        t.late_fee_balance  = t.late_fee_balance + f.fee_amt,
        t.late_fee_assessed = t.late_fee_assessed + 1,
        t.last_fee_date     = p_cutoff_date,
        t.modified_date     = current_timestamp();

    -- Same column list as the source: new_value stays NULL (the source never records the fee total
    -- anywhere) and user_name comes from the converted table's default, the port of the source's
    -- DEFAULT SUSER_NAME() on dbo.audit_trail.
    INSERT INTO ${catalog}.${schema}.audit_trail
        (action_type, action_date, table_name, record_count)
    VALUES ('LATE_FEE', p_cutoff_date, 'loans', total_rows);

    DROP TABLE IF EXISTS fee_targets;
END;
