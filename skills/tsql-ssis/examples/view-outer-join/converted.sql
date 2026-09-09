-- SKILL.md construct map: `*=` / `=*`, comma FROM list, scalar UDF inlining, CHAR padding.
CREATE OR REPLACE VIEW ${catalog}.${schema}.vw_active_loan_portfolio AS
WITH last_payment AS (                       -- correlated MAX() subquery -> window, same rows
    SELECT loan_id,
           MAX(payment_date) AS last_payment_date,
           SUM(total_amt)    AS last_payment_amt
    FROM (
        SELECT p.loan_id, p.payment_date, p.total_amt,
               MAX(p.payment_date) OVER (PARTITION BY p.loan_id) AS max_payment_date
        FROM ${catalog}.${schema}.payments p
        WHERE p.reversal_flag = 'N'
    )
    WHERE payment_date = max_payment_date
    GROUP BY loan_id
)
SELECT
    l.loan_id,
    l.loan_number,
    l.loan_type,
    CASE rtrim(l.loan_type)                  -- fn_format_loan_type inlined; CHAR(4) pads 'FHA '
        WHEN 'CONV' THEN 'Conventional'
        WHEN 'FHA'  THEN 'FHA Government'
        WHEN 'VA'   THEN 'VA Government'
        WHEN 'USDA' THEN 'USDA Rural'
        ELSE 'Unknown (' || rtrim(l.loan_type) || ')'
    END                                      AS loan_type_desc,
    l.current_balance,
    l.loan_status,
    l.days_past_due,
    b.borrower_id,
    b.last_name,
    m.modification_id,
    m.status                                 AS mod_status,
    p.last_payment_date,
    p.last_payment_amt,
    CASE                                     -- fn_get_delinquency_bucket inlined
        WHEN l.days_past_due IS NULL            THEN 'Unknown'
        WHEN l.days_past_due = 0                THEN 'Current'
        WHEN l.days_past_due BETWEEN 1  AND 29  THEN '1-29'
        WHEN l.days_past_due BETWEEN 30 AND 59  THEN '30-59'
        WHEN l.days_past_due BETWEEN 60 AND 89  THEN '60-89'
        WHEN l.days_past_due BETWEEN 90 AND 119 THEN '90-119'
        WHEN l.days_past_due >= 120             THEN '120+'
        ELSE 'Unknown'
    END                                      AS delinq_bucket
FROM ${catalog}.${schema}.loans l
JOIN ${catalog}.${schema}.borrowers b
  ON l.borrower_id = b.borrower_id
-- `l.loan_id *= m.loan_id AND (m.status = 'A' OR m.status IS NULL)`: ASE applies an inner-table
-- qualification as part of the outer join, so a loan whose modifications are all non-'A' is kept
-- with NULL m.* (not dropped). ON reproduces that; WHERE would drop it and the no-modification loans.
LEFT JOIN ${catalog}.${schema}.loan_modifications m
  ON l.loan_id = m.loan_id
 AND (m.status = 'A' OR m.status IS NULL)
LEFT JOIN last_payment p
  ON l.loan_id = p.loan_id
WHERE l.loan_status IN ('AC', 'DL');
