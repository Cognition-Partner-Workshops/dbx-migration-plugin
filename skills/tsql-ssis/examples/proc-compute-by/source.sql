/*=====================================================================
  vw_delinquency_snapshot — Delinquency summary with subtotals
  Sybase ASE 16

  ** CRITICAL SYBASE-ISM: COMPUTE BY **

  COMPUTE BY produces subtotal rows interleaved with detail rows.
  SQL Server removed COMPUTE BY support. Must be converted to
  GROUP BY ... WITH ROLLUP, or window functions, or a UNION ALL
  with explicit subtotal queries.

  Note: this is implemented as a stored procedure that returns a
  result set, not a true view, because COMPUTE BY cannot appear in
  a view definition. Named as vw_ for conceptual clarity in the
  estate map.
=====================================================================*/

CREATE PROCEDURE dbo.sp_delinquency_snapshot
AS
BEGIN
    SELECT
        loan_type,
        property_state,
        delinq_bucket,
        COUNT(*)            AS n_loans,
        SUM(current_balance) AS total_balance,
        SUM(past_due_amount) AS total_past_due
    FROM dbo.vw_active_loan_portfolio
    GROUP BY loan_type, property_state, delinq_bucket
    ORDER BY loan_type, property_state
    COMPUTE SUM(COUNT(*)), SUM(SUM(current_balance))
        BY loan_type
END
go
