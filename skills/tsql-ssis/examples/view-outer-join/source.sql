/* vw_active_loan_portfolio (Sybase ASE 16), trimmed from fixture schema/views/.
   *= is the ASE LEFT OUTER JOIN; the WHERE predicate on the inner table m belongs to the join. */
CREATE VIEW dbo.vw_active_loan_portfolio
AS
SELECT
    l.loan_id,
    l.loan_number,
    l.loan_type,
    dbo.fn_format_loan_type(l.loan_type) AS loan_type_desc,
    l.current_balance,
    l.loan_status,
    l.days_past_due,
    b.borrower_id,
    b.last_name,
    m.modification_id,
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
