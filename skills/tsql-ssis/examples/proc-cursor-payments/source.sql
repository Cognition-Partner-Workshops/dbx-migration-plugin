/*=====================================================================
  sp_process_monthly_payments — Monthly payment waterfall processor
  Sybase ASE 16

  Purpose:
    For a given servicer and processing date, iterate active loans and
    apply the monthly payment waterfall:
      Interest → Principal → Escrow → Late Fees

  ** CRITICAL SYBASE-ISMS: **
    - @@sqlstatus for cursor fetch (not @@FETCH_STATUS)
    - DEALLOCATE CURSOR (not DEALLOCATE)
    - RAISERROR without parentheses, with %1! substitution
    - @@error + GOTO error handling (not TRY/CATCH)
    - SELECT INTO #temp (identical to SQL Server, but cleanup differs)

  Schedule: Monthly 1st business day via cron
  Inputs:  Active loans for @servicer_id
  Outputs: dbo.payments (new rows), dbo.loans (balance updates),
           dbo.audit_trail (batch audit)
=====================================================================*/

CREATE PROCEDURE dbo.sp_process_monthly_payments
    @processing_date    DATETIME,
    @servicer_id        INT,
    @batch_id           INT OUTPUT
AS
BEGIN
    DECLARE @loan_id        INT
    DECLARE @current_bal    MONEY
    DECLARE @interest_rate  DECIMAL(6,4)
    DECLARE @escrow_monthly MONEY
    DECLARE @term_months    INT
    DECLARE @monthly_pi     MONEY
    DECLARE @interest_due   MONEY
    DECLARE @principal_due  MONEY
    DECLARE @err            INT
    DECLARE @row_count      INT
    DECLARE @total_applied  MONEY
    DECLARE @cursor_open    INT

    SELECT @total_applied = $0.00
    SELECT @batch_id = NULL
    SELECT @cursor_open = 0

    /* ---------------------------------------------------------------
       Step 1: Create batch record
       --------------------------------------------------------------- */
    INSERT INTO dbo.audit_trail
        (action_type, action_date, table_name, record_count, user_name)
    VALUES
        ('BATCH_START', @processing_date, 'payments', 0, SUSER_NAME())

    SELECT @err = @@error
    IF @err != 0 GOTO error_handler

    SELECT @batch_id = @@identity
    /* ** @@identity — should be SCOPE_IDENTITY() in SQL Server ** */

    /* ---------------------------------------------------------------
       Step 2: Snapshot eligible loans into temp table
       --------------------------------------------------------------- */
    SELECT
        l.loan_id,
        l.current_balance,
        l.interest_rate,
        l.term_months,
        ISNULL(e.total_escrow, $0.00) AS escrow_monthly
    INTO #eligible_loans
    FROM dbo.loans l
    LEFT JOIN (
        SELECT loan_id, SUM(monthly_amount) AS total_escrow
        FROM dbo.escrow_accounts
        GROUP BY loan_id
    ) e ON l.loan_id = e.loan_id
    WHERE l.loan_status = 'AC'
      AND l.servicer_id = @servicer_id

    SELECT @err = @@error, @row_count = @@rowcount
    IF @err != 0 GOTO error_handler

    /* ---------------------------------------------------------------
       Step 3: Cursor-based payment application
       --------------------------------------------------------------- */
    DECLARE payment_cursor CURSOR FOR
        SELECT loan_id, current_balance, interest_rate,
               term_months, escrow_monthly
        FROM #eligible_loans

    OPEN payment_cursor
    SELECT @cursor_open = 1

    FETCH payment_cursor
        INTO @loan_id, @current_bal, @interest_rate,
             @term_months, @escrow_monthly

    /* ** @@sqlstatus: 0 = success, 1 = error, 2 = no more rows ** */
    WHILE @@sqlstatus = 0
    BEGIN
        BEGIN TRANSACTION

        /* Calculate monthly P&I using the amortization function */
        SELECT @monthly_pi = dbo.fn_calculate_amortization(
            @current_bal, @interest_rate, @term_months
        )

        /* Payment waterfall: Interest first */
        SELECT @interest_due = @current_bal * (@interest_rate / 12.0 / 100.0)
        SELECT @principal_due = @monthly_pi - @interest_due

        /* Prevent negative principal (final payment edge case) */
        IF @principal_due > @current_bal
            SELECT @principal_due = @current_bal

        /* Insert payment record */
        INSERT INTO dbo.payments
            (loan_id, payment_date, effective_date,
             principal_amt, interest_amt, escrow_amt, late_fee_amt,
             total_amt, payment_type, batch_id)
        VALUES
            (@loan_id, @processing_date, @processing_date,
             @principal_due, @interest_due, @escrow_monthly, $0.00,
             @principal_due + @interest_due + @escrow_monthly, 'REG',
             @batch_id)

        SELECT @err = @@error
        IF @err != 0
        BEGIN
            ROLLBACK TRANSACTION
            GOTO error_handler
        END

        /* Update loan balance */
        UPDATE dbo.loans
        SET current_balance = current_balance - @principal_due,
            modified_date = GETDATE()
        WHERE loan_id = @loan_id

        SELECT @err = @@error
        IF @err != 0
        BEGIN
            ROLLBACK TRANSACTION
            GOTO error_handler
        END

        COMMIT TRANSACTION

        SELECT @total_applied = @total_applied
            + @principal_due + @interest_due + @escrow_monthly

        FETCH payment_cursor
            INTO @loan_id, @current_bal, @interest_rate,
                 @term_months, @escrow_monthly
    END

    CLOSE payment_cursor
    DEALLOCATE CURSOR payment_cursor
    /* ** Sybase: DEALLOCATE CURSOR, not just DEALLOCATE ** */

    /* ---------------------------------------------------------------
       Step 4: Update batch record with results
       --------------------------------------------------------------- */
    UPDATE dbo.audit_trail
    SET record_count = @row_count,
        new_value = CONVERT(VARCHAR(20), @total_applied)
    WHERE audit_id = @batch_id

    DROP TABLE #eligible_loans

    RETURN 0

error_handler:
    /* Close cursor if it was opened */
    IF @cursor_open = 1
    BEGIN
        CLOSE payment_cursor
        DEALLOCATE CURSOR payment_cursor
    END

    IF EXISTS (SELECT 1 FROM tempdb..sysobjects
               WHERE name LIKE '#eligible_loans%')
        DROP TABLE #eligible_loans

    RAISERROR 50001 'Payment processing failed for batch %1!', @batch_id
    /* ** Sybase RAISERROR: no parentheses, %1! parameter substitution ** */

    RETURN 1
END
go
