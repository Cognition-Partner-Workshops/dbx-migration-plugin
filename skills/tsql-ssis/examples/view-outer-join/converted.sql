-- Converted under skills/tsql-ssis/SKILL.md §5 rows 31-32, 71; §7 "ASE *= with WHERE predicates".
-- Target objects live in the migration catalog recorded in .migration/00_context.md
-- (shown here as ${catalog}.${schema}; the mapping file substitutes them).
--
-- Scalar UDFs are inlined (SKILL §6 "Scalar UDF"): fn_format_loan_type and
-- fn_get_delinquency_bucket are single CASE expressions in the fixture.

CREATE OR REPLACE VIEW ${catalog}.${schema}.vw_active_loan_portfolio AS
WITH last_payment AS (
    -- Correlated "latest payment" subquery rewritten as a window; same rows, deterministic.
    SELECT loan_id,
           MAX(payment_date) AS last_payment_date,
           SUM(total_amt)    AS last_payment_amt
    FROM (
        SELECT p.loan_id, p.payment_date, p.total_amt,
               MAX(p.payment_date) OVER (PARTITION BY p.loan_id) AS max_payment_date
        FROM ${catalog}.${schema}.payments p
        WHERE p.reversal_flag = 'N'
    ) x
    WHERE payment_date = max_payment_date
    GROUP BY loan_id
)
SELECT
    l.loan_id,
    l.loan_number,
    l.loan_type,
    CASE rtrim(l.loan_type)                          -- CHAR(4) padding: §7 "Trailing-space padding"
        WHEN 'CONV' THEN 'Conventional'
        WHEN 'FHA'  THEN 'FHA Government'
        WHEN 'VA'   THEN 'VA Government'
        WHEN 'USDA' THEN 'USDA Rural'
        ELSE 'Unknown (' || rtrim(l.loan_type) || ')'
    END                                              AS loan_type_desc,
    l.original_balance,
    l.current_balance,
    l.interest_rate,
    l.term_months,
    l.origination_date,
    l.maturity_date,
    l.loan_status,
    l.days_past_due,
    l.past_due_amount,
    l.escrow_balance,
    l.servicer_id,
    l.investor_code,
    l.property_state,
    l.property_value,
    l.ltv,
    b.borrower_id,
    b.first_name,
    b.last_name,
    b.credit_score,
    b.borrower_type,
    b.state_code,
    m.modification_id,
    m.modification_type,
    m.effective_date                                 AS mod_effective_date,
    m.status                                         AS mod_status,
    p.last_payment_date,
    p.last_payment_amt,
    CASE
        WHEN l.days_past_due IS NULL            THEN 'Unknown'
        WHEN l.days_past_due = 0                THEN 'Current'
        WHEN l.days_past_due BETWEEN 1  AND 29  THEN '1-29'
        WHEN l.days_past_due BETWEEN 30 AND 59  THEN '30-59'
        WHEN l.days_past_due BETWEEN 60 AND 89  THEN '60-89'
        WHEN l.days_past_due BETWEEN 90 AND 119 THEN '90-119'
        WHEN l.days_past_due >= 120             THEN '120+'
        ELSE 'Unknown'
    END                                              AS delinq_bucket
FROM ${catalog}.${schema}.loans l
JOIN ${catalog}.${schema}.borrowers b
  ON l.borrower_id = b.borrower_id
-- Sybase:  AND l.loan_id *= m.loan_id AND (m.status = 'A' OR m.status IS NULL)
-- The inner-side predicate moves INTO the ON clause, verbatim: a WHERE would turn the
-- outer join back into an inner join for loans whose only modifications are inactive,
-- and dropping the IS NULL arm changes the match set whenever status can be NULL
-- (the fixture declares it NOT NULL, but the conversion rule must not depend on that).
LEFT JOIN ${catalog}.${schema}.loan_modifications m
  ON l.loan_id = m.loan_id
 AND (m.status = 'A' OR m.status IS NULL)
-- Sybase:  AND l.loan_id *= p.loan_id
LEFT JOIN last_payment p
  ON l.loan_id = p.loan_id
WHERE l.loan_status IN ('AC', 'DL');
