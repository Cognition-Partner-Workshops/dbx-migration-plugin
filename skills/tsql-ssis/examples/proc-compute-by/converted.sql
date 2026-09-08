-- Converted under skills/tsql-ssis/SKILL.md §5 row 72 and §7 "COMPUTE BY".
-- The ASE result is ONE stream: detail rows ordered by (loan_type, property_state) with a
-- COMPUTE subtotal row emitted after each loan_type group. Databricks has no interleaved
-- compute rows; GROUPING SETS (opened: docs.databricks.com sql-ref-syntax-qry-select-groupby)
-- emits the subtotal as an extra row and a grouping() flag marks it. Consumers that parsed the
-- compute rows positionally must be re-pointed at grouping_level.
--
-- Materialized as a view (the source was a procedure only because COMPUTE BY cannot sit in a
-- view). Scheduled snapshotting, if needed, is a databricks-jobs SQL task per target-routing.

CREATE OR REPLACE VIEW ${catalog}.${schema}.vw_delinquency_snapshot AS
SELECT
    loan_type,
    property_state,
    delinq_bucket,
    COUNT(*)              AS n_loans,
    SUM(current_balance)  AS total_balance,
    SUM(past_due_amount)  AS total_past_due,
    -- 0 = detail row; 1 = COMPUTE BY loan_type subtotal (property_state / delinq_bucket are NULL)
    CASE WHEN grouping(property_state) = 1 THEN 1 ELSE 0 END AS grouping_level
FROM ${catalog}.${schema}.vw_active_loan_portfolio
GROUP BY GROUPING SETS (
    (loan_type, property_state, delinq_bucket),   -- detail
    (loan_type)                                   -- COMPUTE SUM(COUNT(*)), SUM(SUM(current_balance)) BY loan_type
)
ORDER BY loan_type, grouping_level, property_state, delinq_bucket;

-- Note on the second COMPUTE column: the source subtotalled COUNT(*) and SUM(current_balance) only.
-- total_past_due is also aggregated on the subtotal row here; consumers that must see NULL there
-- can wrap it: CASE WHEN grouping_level = 1 THEN NULL ELSE total_past_due END.
