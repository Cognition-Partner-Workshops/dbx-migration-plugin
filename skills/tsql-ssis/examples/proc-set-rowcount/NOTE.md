# proc-set-rowcount: `sp_apply_late_fees`

Source: fixture `stored_procs/Servicing/sp_apply_late_fees.sql` (Sybase ASE 16). Converted: `converted.sql`.

## Constructs exercised
- `SET ROWCOUNT 1000` + `WHILE 1 = 1` + `IF @@rowcount = 0 BREAK` + `SET ROWCOUNT 0` batching loop -> one `UPDATE` with the source's `WHERE` and `SET` unchanged (SKILL §6 batching row; §7 trap; delta list item 3). The source's "must reset SET ROWCOUNT" hazard disappears entirely. Qualification and fee derivation stay in the one statement: a `CREATE TEMP TABLE fee_targets AS SELECT ...` followed by a `MERGE` on `loan_id` (the previous shape) evaluated eligibility on one snapshot and applied the fee on another, so a loan that left delinquency (or changed `loan_type`, or was fee-stamped by another run) in between was still charged the stale fee. The source's batched `UPDATE` never had that gap.
- `@@rowcount` summed over the batches (the audit row's `record_count`) -> `GET DIAGNOSTICS total_rows = ROW_COUNT` as the statement right after the `UPDATE` (docs `sql/language-manual/control-flow/get-diagnostics-stmt`: "the number of rows affected by the most recently executed DML statement as a BIGINT", populated by Delta `INSERT`/`UPDATE`/`DELETE`/`MERGE INTO`; SKILL §5 row 75). A `count(*)` over the predicate before the DML is a separate snapshot and disagrees with the rows the DML touched under concurrent writers. **Runtime prerequisite**: `ROW_COUNT` needs Databricks SQL or DBR 18 LTS+; the unit's target compute is recorded in the mapping, and on older runtimes the unit becomes a notebook task that reads the DML's affected-row result (there is no DML-tied count in SQL scripting without it).
- `$0.00` / `$45.00` money literals -> `CAST(45.00 AS DECIMAL(19,4))` (§5 row 84; delta item 13).
- `CASE loan_type WHEN 'CONV'` on a `CHAR(4)` column -> `CASE rtrim(loan_type)` (§7 "Trailing-space padding").
- `SELECT @v = expr` variable assignment -> `SET` / `SELECT ... INTO` (§5 row 88).
- Business invariant carried over: VA loans never receive a fee (both in the fee schedule and in the predicate).
- `@@identity` warning in the source comments: nothing to convert (no read-back), documented as the §7 trap.
- `RETURN 0` -> `OUT p_rc`; parameters carry the `p_` prefix (SKILL §7 "parameter shadowing").

## Lineage (FACT)
Reads: `loans`. Writes: `loans`, `audit_trail`. Scheduler edge: cron daily (fixture comment `Schedule: Daily via cron`; called from `batch/run_nightly_batch.sh` step 2).

## Recon tier that catches a wrong conversion
- **Tier 2 (aggregates)**: `sum(late_fee_balance)`, `sum(late_fee_assessed)`, `count(*) WHERE last_fee_date = <cutoff>` on `loans` after the run vs source. A conversion that kept a batching loop with an off-by-one exit, or that re-applied fees to rows already stamped with `last_fee_date = cutoff`, drifts here. The invariant check `count(*) WHERE rtrim(loan_type) = 'VA' AND late_fee_balance > 0` must be 0 on both sides (fixture README parity control).
- **Tier 1** on `audit_trail WHERE action_type = 'LATE_FEE'`: exactly one row per run; `record_count` must equal `count(*) WHERE last_fee_date = <cutoff> AND modified_date >= <run start>` on `loans` (rows the run actually updated), and Tier 3 on that row must show `new_value IS NULL` on both sides (the source never records the fee total; a conversion that "helpfully" writes it there fails here). A two-snapshot conversion fails the `record_count` check under a concurrent `sp_nightly_accrual` (its second `UPDATE` moves cured loans `DL` -> `AC`) or `sp_update_loan_status`: the count is taken from the stale list, not from the update.
- **Concurrency run** (Tier 2/3): start the fee run and, while it executes, cure a qualifying loan (`sp_update_loan_status` to `'AC'`, or the accrual's `DL` -> `AC` update). The loan must end with either the fee and then `'AC'` (fee committed first) or `'AC'` and no fee (cure committed first, the fee run's commit then conflicts and is retried against the new state): never a fee stamped after the cure. The single-statement `UPDATE` cannot produce the third state; the temp-table-then-`MERGE` shape can.
- Tier 3 keyed on `loan_id` catches the `CHAR(4)` padding mistake (a converted `CASE loan_type WHEN 'FHA'` without `rtrim` charges `$50` instead of `$35`).

## INFERRED edges
None.

## Lakebridge
`mssql` flag mangles the `SET ROWCOUNT` loop (keeps the loop, drops or no-ops `SET ROWCOUNT`) and rejects `$` literals (SKILL §10); both hand-converted.
