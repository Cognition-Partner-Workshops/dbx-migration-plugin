# proc-cursor-payments: `sp_process_monthly_payments`

Source: fixture `stored_procs/Servicing/sp_process_monthly_payments.sql` (Sybase ASE 16). Converted: `converted.sql` (DBSQL `CREATE PROCEDURE` + SQL scripting; the same body runs as a `sql_task` if procedures are not enabled on the warehouse).

## Constructs exercised
- `OUTPUT` parameter (`@batch_id`) and `RETURN 0/1` -> `OUT batch_id`, `OUT rc` (SKILL §6 "CREATE PROCEDURE", "RETURN").
- `@@identity` after the audit insert -> `uuid()` run key + `SELECT audit_id INTO` read-back (SKILL §5 row 74, §7 "@@identity hijacked by triggers").
- `SELECT ... INTO #eligible_loans` -> `CREATE TEMP TABLE` (SKILL §5 row 70).
- `DECLARE CURSOR` / `FETCH` / `WHILE @@sqlstatus = 0` / `CLOSE` / `DEALLOCATE CURSOR` -> one set-based `INSERT ... SELECT` + `MERGE` (SKILL §6 "cursor" row; delta list item 4).
- `@@error` + `GOTO error_handler` + cleanup -> `DECLARE EXIT HANDLER FOR SQLEXCEPTION` (SKILL §6 "error handling"; delta item 6).
- `RAISERROR 50001 'msg %1!', @batch_id` -> `DECLARE ... CONDITION FOR SQLSTATE '45001'` + `SIGNAL ... SET MESSAGE_TEXT` (delta item 5).
- Per-loan `BEGIN TRANSACTION ... COMMIT` -> single-statement atomicity per DML; cross-table (payments then loans) atomicity is not claimed (SKILL §6 "transactions": Preview `BEGIN ATOMIC` requires `catalogManaged` tables).
- `MONEY` arithmetic (`@current_bal * (@interest_rate / 12.0 / 100.0)`) -> `round(..., 4)` at each source rounding point (SKILL §7 "MONEY arithmetic").
- `ISNULL(e.total_escrow, $0.00)` -> `coalesce(..., CAST(0 AS DECIMAL(19,4)))` (§5 rows 1, 84).
- `@@rowcount` -> `SELECT count(*) INTO` (§5 row 75).
- Audit trigger fan-out: `trg_audit_payment` (FOR INSERT on `payments`) is folded into the writer (SKILL §2b "Triggers", §6 "audit trigger").
- `SUSER_NAME()` -> `current_user()`; `CONVERT(VARCHAR(20), money)` -> `cast(... AS STRING)`.

## Lineage (FACT)
Reads: `loans`, `escrow_accounts`; calls `fn_calculate_amortization`. Writes: `audit_trail` (direct + via trigger), `payments`, `loans`. Unit members: the procedure, the function, the three written tables, `trg_audit_payment`.

## Recon tier that catches a wrong conversion
- **Tier 2 (aggregates)** on `payments WHERE batch_id = <batch>`: `sum(principal_amt)`, `sum(interest_amt)`, `sum(escrow_amt)`, `count(*)` vs the source batch. Missing `round(..., 4)` shows as cent-level drift in `sum(interest_amt)` once `decimal_round` places=4 has removed scale noise; a wrong final-payment clamp shows in `sum(principal_amt)`.
- **Tier 1** on `audit_trail` rows per batch: 1 (`BATCH_START`) + N (`PAYMENT_INS`) — a conversion that forgets the trigger fold produces exactly 1.
- **Tier 3** keyed on `loans.loan_id`: `current_balance` after the run (the `MERGE` must subtract exactly once per loan; a cursor-faithful loop that double-applied would show here).
- Ordering assumption to record in the tolerance record: `payments` are inserted before `loans` is updated; a failure between them leaves payments without balance updates (source rolled back per loan). Not verifiable on the fixture.

## INFERRED edges
None. `#eligible_loans` and `waterfall` are unit-local temp objects.

## Lakebridge
`mssql` flag converts the DECLARE/IF/WHILE shell but rejects `@@sqlstatus`, `DEALLOCATE CURSOR`, positional `RAISERROR`, and `GOTO`; those rows are hand-converted (SKILL §10).
