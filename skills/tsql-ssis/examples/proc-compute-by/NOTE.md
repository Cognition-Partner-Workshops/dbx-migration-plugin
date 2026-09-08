# proc-compute-by: `sp_delinquency_snapshot` (fixture file `vw_delinquency_snapshot.sql`)

Source: fixture `schema/views/vw_delinquency_snapshot.sql` (a procedure, because ASE cannot put `COMPUTE BY` in a view). Converted: `converted.sql` (Databricks SQL view with `GROUPING SETS`).

## Constructs exercised
- `COMPUTE SUM(COUNT(*)), SUM(SUM(current_balance)) BY loan_type` -> `GROUP BY GROUPING SETS ((loan_type, property_state, delinq_bucket), (loan_type))` + `grouping()` flag (SKILL §5 row 72, §7 trap, delta list item 2).
- Interleaved-result-stream contract (detail rows then a subtotal row per group) -> single relation with `grouping_level`; consumer re-pointing is part of the unit (SKILL §3: reporting procedures with result-set contracts).
- Procedure-that-is-really-a-view -> view (§6 "Procedure that returns one result set with no side effects").
- `ORDER BY` inside a view: preserved for the replay tier only; downstream consumers must not rely on it.
- Depends on the converted `vw_active_loan_portfolio` (example `view-outer-join`); the two form one migration unit with `fn_get_delinquency_bucket`.

## Lineage (FACT)
Reads: `vw_active_loan_portfolio` (-> `loans`, `borrowers`, `loan_modifications`, `payments`). Writes: none. Scheduler edge: none in the fixture (reporting procedures are called from `batch/run_monthly_reports.sh`, but this one is not).

## Recon tier that catches a wrong conversion
- **Tier 1 (row count)**: source detail rows + one subtotal row per distinct `loan_type`; a conversion with a plain `GROUP BY` (no grouping set) is short by exactly `count(distinct loan_type)` rows, and one with `ROLLUP` is long by one grand-total row plus the `(loan_type, property_state)` level.
- **Tier 2**: `sum(total_balance) WHERE grouping_level = 1` must equal `sum(total_balance) WHERE grouping_level = 0`; `sum(n_loans)` likewise.
- **Tier 4 (replay)**: the ordered stream is compared row-by-row against a captured ASE `isql` output; `ORDER BY loan_type, grouping_level, property_state` reproduces the "subtotal after its group" position.
- Requires the `view-outer-join` unit to be PASS first; a `*=` regression shows up here as a Tier 1 undercount too.

## INFERRED edges
None.

## Lakebridge
`mssql` flag rejects `COMPUTE BY` (SQL Server dropped it in 2012, so the transpiler has no rule). Hand-converted (SKILL §10).
