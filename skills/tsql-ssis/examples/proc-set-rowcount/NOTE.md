# proc-set-rowcount: `sp_apply_late_fees`

Source: fixture `stored_procs/Servicing/sp_apply_late_fees.sql` (Sybase ASE 16). Converted: `converted.sql`.

## Constructs exercised
- `SET ROWCOUNT 1000` + `WHILE 1 = 1` + `IF @@rowcount = 0 BREAK` + `SET ROWCOUNT 0` batching loop -> one `MERGE` (SKILL §6 batching row; §7 trap; delta list item 3). The source's "must reset SET ROWCOUNT" hazard disappears entirely.
- `$0.00` / `$45.00` money literals -> `CAST(45.00 AS DECIMAL(19,4))` (§5 row 84; delta item 13).
- `CASE loan_type WHEN 'CONV'` on a `CHAR(4)` column -> `CASE rtrim(loan_type)` (§7 "Trailing-space padding").
- `SELECT @v = expr` variable assignment -> `SET` / `SELECT ... INTO` (§5 row 88).
- Business invariant carried over: VA loans never receive a fee (both in the fee schedule and in the predicate).
- `@@identity` warning in the source comments: nothing to convert (no read-back), documented as the §7 trap.
- `RETURN 0` -> `OUT rc`.

## Lineage (FACT)
Reads: `loans`. Writes: `loans`, `audit_trail`. Scheduler edge: cron daily (fixture comment `Schedule: Daily via cron`; called from `batch/run_nightly_batch.sh` step 2).

## Recon tier that catches a wrong conversion
- **Tier 2 (aggregates)**: `sum(late_fee_balance)`, `sum(late_fee_assessed)`, `count(*) WHERE last_fee_date = <cutoff>` on `loans` after the run vs source. A conversion that kept a batching loop with an off-by-one exit, or that re-applied fees to rows already stamped with `last_fee_date = cutoff`, drifts here. The invariant check `count(*) WHERE rtrim(loan_type) = 'VA' AND late_fee_balance > 0` must be 0 on both sides (fixture README parity control).
- **Tier 1** on `audit_trail WHERE action_type = 'LATE_FEE'`: exactly one row per run; the `record_count` value is checked at Tier 2.
- Tier 3 keyed on `loan_id` catches the `CHAR(4)` padding mistake (a converted `CASE loan_type WHEN 'FHA'` without `rtrim` charges `$50` instead of `$35`).

## INFERRED edges
None.

## Lakebridge
`mssql` flag mangles the `SET ROWCOUNT` loop (keeps the loop, drops or no-ops `SET ROWCOUNT`) and rejects `$` literals (SKILL §10); both hand-converted.
