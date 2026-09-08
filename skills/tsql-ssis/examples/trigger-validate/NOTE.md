# trigger-validate: `trg_validate_loan_amount` + `trg_audit_payment`

Source: fixture `triggers/trg_validate_loan_amount.sql` (FOR UPDATE on `loans`) and `triggers/trg_audit_payment.sql` (FOR INSERT on `payments`), concatenated in `source.sql`. Converted: `converted.sql` (two shapes each: writer-attached SQL scripting block for the DBSQL track; expectation / streaming table for the SDP track).

## Constructs exercised
- ASE `FOR UPDATE` / `FOR INSERT` (= SQL Server `AFTER`) statement-level triggers (SKILL §6 "Triggers"; delta list items 15-16). Databricks has no row/statement triggers: the body attaches to every converted writer, or becomes a pipeline expectation (`databricks-pipelines` references/expectations-sql.md: `CONSTRAINT ... EXPECT (...) ON VIOLATION FAIL UPDATE`).
- `IF @@rowcount = 0 RETURN` guard -> nothing (set-based block over the staged image is a no-op on zero rows).
- `IF UPDATE(current_balance)` column-changed test -> attach only to writers that touch that column (§2b "Trigger-implied writes": the lineage extractor records the column list).
- `inserted` / `deleted` pseudo-tables -> the writer's staged table (`waterfall`) joined to the current row (`old_value` / `new_value`).
- `RAISERROR 50050 '...'` + `ROLLBACK TRANSACTION` -> `DECLARE ... CONDITION FOR SQLSTATE '45050'` + `SIGNAL` **before** the DML (`databricks-dbsql` references/sql-scripting.md "Exception Handling", "SIGNAL"). Order differs: ASE logged then rolled back (losing the audit row inside the caller's transaction); converted logs then refuses. Recorded as a tolerance-record difference.
- Concatenation with `CONVERT(VARCHAR(20), money)` -> `|| cast(... AS STRING)` (§5 rows 7, 39); `$0` -> `0`.
- `GETDATE()` / `SUSER_NAME()` defaults -> `current_timestamp()` / `current_user()`.
- Shape B for the audit trigger: `CREATE OR REFRESH STREAMING TABLE ... FROM STREAM(payments)` (`databricks-pipelines` references/streaming-table-sql.md "STREAM(...) source"); `user_name` is not available there.

## Lineage (FACT)
`trg_validate_loan_amount`: reads `inserted`/`deleted` of `loans`, writes `audit_trail`; implied edge `loans -> audit_trail` for every writer whose SET list names `loans.current_balance` (fixture: `sp_process_monthly_payments`, `sp_loan_modification`; `sp_nightly_accrual`, `sp_apply_late_fees` and the CRUD procs update other `loans` columns, so `IF UPDATE(current_balance)` is false and the pre-check is not attached to them). `trg_audit_payment`: `payments -> audit_trail` for every writer of `payments`. Neither is a standalone unit; both are members of every writer's unit (SKILL §3).

## Recon tier that catches a wrong conversion
- **Tier 1** on `audit_trail` grouped by `action_type`: a writer that forgot to fold `trg_audit_payment` has zero `PAYMENT_INS` rows for its batch; `BALANCE_VIOLATION` counts are expected to be >= source (documented).
- **Tier 3** keyed on `(table_name, record_id)` for `PAYMENT_INS`: `new_value` string must match byte-for-byte after `rstrip_spaces` (`'type=' || payment_type` with a `CHAR(3)` column pads on ASE: `'REG'` is fine, but a two-letter code would carry a trailing space).
- The validation trigger is a business invariant, so the parity control from the fixture README applies at **Tier 2**: `count(*) FROM loans WHERE current_balance < 0 AND loan_status NOT IN ('CO','PO')` is 0 on both sides after every unit run.

## INFERRED edges
Which writers actually change `current_balance` at runtime (dynamic SQL, none in the fixture) — INFERRED for any estate with `EXEC(@sql)` writers.

## Lakebridge
`mssql` flag rejects `CREATE TRIGGER` (no Databricks target). Hand-converted (SKILL §10).
