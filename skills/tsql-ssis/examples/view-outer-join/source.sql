/*=====================================================================
  vw_active_loan_portfolio — Active/delinquent loan portfolio view
  Sybase ASE 16

  ** CRITICAL SYBASE-ISM: *= outer join syntax **

  This view uses the Sybase proprietary *= operator for LEFT OUTER JOIN.
  The *= means "keep all rows from the LEFT table even when no match
  exists on the RIGHT."

  - l.loan_id *= m.loan_id  → LEFT JOIN to loan_modifications
  - l.loan_id *= p.loan_id  → LEFT JOIN to last-payment subquery

  A naive conversion that simply removes the *= and leaves a comma join
  produces an INNER JOIN, silently dropping:
    - Loans with no modification history
    - Loans with no payment history (new originations)

  This is the primary "planted bug" — the reconciliation completeness
  control catches the row-count discrepancy.
=====================================================================*/

CREATE VIEW dbo.vw_active_loan_portfolio
AS
SELECT
    l.loan_id,
    l.loan_number,
    l.loan_type,
    dbo.fn_format_loan_type(l.loan_type) AS loan_type_desc,
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
    m.effective_date    AS mod_effective_date,
    m.status            AS mod_status,
    p.last_payment_date,
    p.last_payment_amt,
    dbo.fn_get_delinquency_bucket(l.days_past_due) AS delinq_bucket
FROM dbo.loans l,
     dbo.borrowers b,
     dbo.loan_modifications m,
     (SELECT p1.loan_id,
             MAX(p1.payment_date)  AS last_payment_date,
             SUM(p1.total_amt)     AS last_payment_amt
      FROM dbo.payments p1
      WHERE p1.reversal_flag = 'N'
        AND p1.payment_date = (
            SELECT MAX(p2.payment_date)
            FROM dbo.payments p2
            WHERE p2.loan_id = p1.loan_id
              AND p2.reversal_flag = 'N'
        )
      GROUP BY p1.loan_id
     ) p
WHERE l.borrower_id = b.borrower_id
  AND l.loan_id *= m.loan_id
  AND (m.status = 'A' OR m.status IS NULL)
  AND l.loan_id *= p.loan_id
  AND l.loan_status IN ('AC', 'DL')
go
