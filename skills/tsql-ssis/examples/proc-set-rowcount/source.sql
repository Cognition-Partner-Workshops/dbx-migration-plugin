/*=====================================================================
  sp_apply_late_fees — Assess late fees on delinquent loans
  Sybase ASE 16

  Purpose:
    Apply late fees to loans past the grace period (15 days).
    VA loans are exempt from late fees (regulatory requirement).

  ** CRITICAL SYBASE-ISMS: **
    - SET ROWCOUNT for batched DML (deprecated in SQL Server)
    - @@identity instead of SCOPE_IDENTITY()
    - Business rule: VA loans never get late fees — the reconciliation
      parity check validates this invariant

  Schedule: Daily via cron
=====================================================================*/

CREATE PROCEDURE dbo.sp_apply_late_fees
    @cutoff_date    DATETIME
AS
BEGIN
    DECLARE @rows_affected  INT
    DECLARE @total_fees     MONEY
    DECLARE @batch_count    INT
    DECLARE @total_rows     INT

    SELECT @total_fees = $0.00
    SELECT @batch_count = 0
    SELECT @total_rows = 0

    /* ---------------------------------------------------------------
       Step 1: Batched late fee assessment
       SET ROWCOUNT limits DML to N rows per iteration to avoid
       long-running transactions and lock escalation.
       ** In SQL Server, SET ROWCOUNT is deprecated for DML;
          use UPDATE TOP(1000) instead. **
       --------------------------------------------------------------- */
    SET ROWCOUNT 1000

    WHILE 1 = 1
    BEGIN
        UPDATE dbo.loans
        SET late_fee_balance = late_fee_balance +
            CASE loan_type
                WHEN 'CONV' THEN $45.00
                WHEN 'FHA'  THEN $35.00
                WHEN 'VA'   THEN $0.00
                    /* ** VA loans: no late fees — regulatory requirement ** */
                WHEN 'USDA' THEN $30.00
                ELSE $50.00
            END,
            late_fee_assessed = late_fee_assessed + 1,
            last_fee_date = @cutoff_date,
            modified_date = GETDATE()
        WHERE loan_status = 'DL'
          AND days_past_due >= 15
          AND (last_fee_date IS NULL OR last_fee_date < @cutoff_date)
          AND loan_type != 'VA'
              /* Exclude VA from the WHERE as defense in depth,
                 but the CASE also sets $0.00 as a safety net */

        SELECT @rows_affected = @@rowcount
        IF @rows_affected = 0 BREAK

        SELECT @total_rows = @total_rows + @rows_affected
        SELECT @batch_count = @batch_count + 1
        SELECT @total_fees = @total_fees + (
            SELECT SUM(
                CASE loan_type
                    WHEN 'CONV' THEN $45.00
                    WHEN 'FHA'  THEN $35.00
                    WHEN 'USDA' THEN $30.00
                    ELSE $50.00
                END
            )
            FROM dbo.loans
            WHERE loan_status = 'DL'
              AND last_fee_date = @cutoff_date
              AND late_fee_assessed = (
                  SELECT MAX(late_fee_assessed)
                  FROM dbo.loans
                  WHERE last_fee_date = @cutoff_date
              )
        )
    END

    SET ROWCOUNT 0
    /* ** CRITICAL: must reset SET ROWCOUNT or all subsequent DML
       in this connection is limited to 1000 rows! ** */

    /* ---------------------------------------------------------------
       Step 2: Audit trail using @@identity
       --------------------------------------------------------------- */
    INSERT INTO dbo.audit_trail
        (action_type, action_date, table_name, record_count)
    VALUES
        ('LATE_FEE', @cutoff_date, 'loans', @total_rows)

    /* @@identity returns the LAST identity value inserted in this session,
       but if trg_audit_payment fires on a different table with an IDENTITY
       column, @@identity returns THAT table's identity instead.
       ** In SQL Server, use SCOPE_IDENTITY() to stay trigger-safe. ** */

    RETURN 0
END
go
