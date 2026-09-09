# proc-cursor-payments: `sp_process_monthly_payments`

Source: fixture `stored_procs/Batch/sp_process_monthly_payments.sql` (Sybase ASE 16), trimmed.

Constructs: cursor with `@@sqlstatus` / `DEALLOCATE CURSOR` -> set-based temp table + `MERGE`;
`@@error` + `GOTO error_handler` -> `DECLARE EXIT HANDLER`; ASE `RAISERROR 50001 '... %1!'` ->
`SIGNAL` with a declared condition; `@@identity` -> business-key read-back on a `run_key`;
`SELECT INTO #temp` -> `CREATE TEMP TABLE`; `@@rowcount` -> `count(*)`; `MONEY` -> `DECIMAL(19,4)`
with `round(..., 4)` per step; `SUSER_NAME()` -> `current_user()`; parameters `p_`-prefixed
(a bare name resolves as a column first); triggers `trg_audit_payment` and
`trg_validate_loan_amount` folded into the writer inside the atomic block.

Recorded per-unit decision: the source committed loan by loan (a failure at loan k left 1..k-1
applied and a rerun paid them twice); the conversion makes the batch the commit unit with one
`BEGIN ATOMIC ... END` (`SIGNAL` and commit conflicts roll everything back, the caller retries).
The failed-run `BATCH_START` row is kept, as the source's handler never removed it. Needs UC
managed tables with catalog commits and a warehouse / DBR 18+ (multi-statement transactions).

Lineage: reads `loans`, `escrow_accounts`; calls `fn_calculate_amortization`; writes `payments`,
`loans.current_balance`, `audit_trail`.

Recon: **Tier 1** `count(payments WHERE batch_id = b)` = eligible loans; `audit_trail` gains exactly
one `BATCH_START` and one `PAYMENT_INS` per payment. **Tier 2** `sum(principal_amt)`, `sum(total_amt)`
under `decimal_round` places=4 (MONEY residual). **Tier 3** on `loan_id`: `current_balance` after
the run; a failure-injection run must leave balances and payments unchanged and one `BATCH_START`
row with `record_count = 0`.
