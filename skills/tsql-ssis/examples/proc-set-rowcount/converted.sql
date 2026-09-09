-- Converted under skills/tsql-ssis/SKILL.md §6 "WHILE 1=1 ... SET ROWCOUNT" and §7
-- "@@ROWCOUNT-driven logic and SET ROWCOUNT batching": the batching loop collapses to one
-- atomic UPDATE (every Delta DML statement is atomic: databricks-dbsql references/sql-scripting.md
-- "Multi-Statement Transactions"). $ money literals -> DECIMAL(19,4) literals (§5 row 84).
-- @@identity is not used for anything in the source (the comment only warns about it); the audit
-- row is a plain INSERT.
--
-- Qualification and fee derivation live in the ONE UPDATE, exactly as in the source's batched
-- UPDATE: the WHERE is the source predicate, the SET derives the fee from the row being updated.
-- A pre-materialised target list (CREATE TEMP TABLE ... AS SELECT, then MERGE on loan_id) is a
-- second snapshot: between the two statements a loan can leave delinquency (loan_status, days_past_due,
-- last_fee_date, loan_type all move in the fixture's other procs), and the MERGE would still charge it
-- the stale fee and re-stamp last_fee_date. The source never had that gap; neither does this.
--
-- @@rowcount (summed over the batches) -> GET DIAGNOSTICS total_rows = ROW_COUNT immediately after the
-- UPDATE (docs sql/language-manual/control-flow/get-diagnostics-stmt: "the number of rows affected by
-- the most recently executed DML statement as a BIGINT"; "Built-in Delta Lake writes (INSERT, UPDATE,
-- DELETE, MERGE INTO ...) populate ROW_COUNT"; the variable must be a BIGINT; ROW_COUNT: Databricks SQL /
-- DBR 18 LTS and above). A count(*) over the predicate before or after the UPDATE is a different
-- snapshot and can disagree with the rows actually updated; the audit row's record_count is the
-- source's @@rowcount sum, so it must come from the DML itself.
-- Runtime prerequisite recorded in NOTE.md (§6 row 75). Where ROW_COUNT is unavailable, SQL scripting
-- has no DML-tied row count: the unit is then a notebook task (PySpark) that reads the DML's
-- affected-row result, not a SQL procedure with a count(*) stand-in.

CREATE OR REPLACE PROCEDURE ${catalog}.${schema}.sp_apply_late_fees(
    IN  p_cutoff_date TIMESTAMP_NTZ,
    OUT p_rc          INT
)
LANGUAGE SQL
SQL SECURITY INVOKER
AS BEGIN
    DECLARE total_rows BIGINT DEFAULT 0;

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        SET p_rc = 1;
        RESIGNAL;
    END;

    SET p_rc = 0;

    -- The source also accumulates @total_fees (and @batch_count) from a second query keyed on
    -- late_fee_assessed = MAX(...), but never emits either: not an output, not audited, not
    -- returned. They are dropped here rather than written anywhere the source does not write.

    -- SET ROWCOUNT 1000 / WHILE 1=1 / UPDATE ... / IF @@rowcount = 0 BREAK / SET ROWCOUNT 0
    --   -> one UPDATE with the same WHERE and the same SET.
    UPDATE ${catalog}.${schema}.loans
    SET late_fee_balance  = late_fee_balance +
            CASE rtrim(loan_type)              -- CHAR(4): 'FHA ' / 'VA  ' padding (§7)
                WHEN 'CONV' THEN CAST(45.00 AS DECIMAL(19,4))
                WHEN 'FHA'  THEN CAST(35.00 AS DECIMAL(19,4))
                WHEN 'VA'   THEN CAST(0.00  AS DECIMAL(19,4))   -- VA loans: no late fees (regulatory)
                WHEN 'USDA' THEN CAST(30.00 AS DECIMAL(19,4))
                ELSE             CAST(50.00 AS DECIMAL(19,4))
            END,
        late_fee_assessed = late_fee_assessed + 1,
        last_fee_date     = p_cutoff_date,
        modified_date     = current_timestamp()
    WHERE loan_status = 'DL'
      AND days_past_due >= 15
      AND (last_fee_date IS NULL OR last_fee_date < p_cutoff_date)
      AND rtrim(loan_type) != 'VA';       -- defense in depth, as in the source

    GET DIAGNOSTICS total_rows = ROW_COUNT;   -- sum of the per-batch @@rowcount values

    -- Same column list as the source: new_value stays NULL (the source never records the fee total
    -- anywhere) and user_name comes from the converted table's default, the port of the source's
    -- DEFAULT SUSER_NAME() on dbo.audit_trail.
    INSERT INTO ${catalog}.${schema}.audit_trail
        (action_type, action_date, table_name, record_count)
    VALUES ('LATE_FEE', p_cutoff_date, 'loans', total_rows);
END;
