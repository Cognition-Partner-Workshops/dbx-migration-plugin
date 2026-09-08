/*=====================================================================
  trg_validate_loan_amount — Validate loan balance on update
  Sybase ASE 16

  Prevents negative balances and logs the attempt.

  ** SYBASE-ISMS: **
    - FOR UPDATE (not AFTER UPDATE)
    - RAISERROR without parentheses
    - IF UPDATE(column) syntax (same in SQL Server)
=====================================================================*/

CREATE TRIGGER dbo.trg_validate_loan_amount
ON dbo.loans
FOR UPDATE
/* ** Sybase: FOR UPDATE — SQL Server uses AFTER UPDATE ** */
AS
BEGIN
    IF @@rowcount = 0
        RETURN

    /* Only fire if balance columns were modified */
    IF UPDATE(current_balance)
    BEGIN
        IF EXISTS (
            SELECT 1 FROM inserted
            WHERE current_balance < $0
              AND loan_status NOT IN ('CO', 'PO')
        )
        BEGIN
            /* Log the violation */
            INSERT INTO dbo.audit_trail
                (action_type, action_date, table_name, loan_id,
                 old_value, new_value)
            SELECT
                'BALANCE_VIOLATION',
                GETDATE(),
                'loans',
                i.loan_id,
                CONVERT(VARCHAR(20), d.current_balance),
                CONVERT(VARCHAR(20), i.current_balance)
            FROM inserted i
            INNER JOIN deleted d ON i.loan_id = d.loan_id
            WHERE i.current_balance < $0
              AND i.loan_status NOT IN ('CO', 'PO')

            RAISERROR 50050 'Negative balance not allowed for active loans'
            ROLLBACK TRANSACTION
        END
    END
END
go
/*=====================================================================
  trg_audit_payment — Audit trigger for payment inserts
  Sybase ASE 16

  ** SYBASE-ISMS: **
    - FOR INSERT (not AFTER INSERT as in SQL Server)
    - @@rowcount guard at the top (Sybase best practice)
    - Sybase 'inserted' virtual table (same name as SQL Server)
=====================================================================*/

CREATE TRIGGER dbo.trg_audit_payment
ON dbo.payments
FOR INSERT
/* ** Sybase: FOR INSERT — SQL Server uses AFTER INSERT ** */
AS
BEGIN
    /* ** Sybase pattern: early exit if no rows affected ** */
    IF @@rowcount = 0
        RETURN

    INSERT INTO dbo.audit_trail
        (action_type, action_date, table_name, record_id, loan_id,
         new_value)
    SELECT
        'PAYMENT_INS',
        GETDATE(),
        'payments',
        i.payment_id,
        i.loan_id,
        'type=' + i.payment_type
            + ' amt=' + CONVERT(VARCHAR(20), i.total_amt)
    FROM inserted i
END
go
