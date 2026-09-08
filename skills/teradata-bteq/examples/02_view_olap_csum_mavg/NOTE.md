# 02 — View with Teradata OLAP shorthands (CSUM, MAVG), FORMAT casts, NULLIFZERO, LOCKING modifier

Source: fixture `ddl/views/03_vw_branch_performance.sql` (verbatim). Target: Databricks SQL view.

## Constructs exercised
- `SEL` -> `SELECT`; `REPLACE VIEW` -> `CREATE OR REPLACE VIEW`; `COMMENT ON VIEW` -> `COMMENT` clause.
- `LOCKING ROW FOR ACCESS` -> dropped (Teradata lock modifier, no Delta counterpart; skill §5).
- `CSUM(x, k)` -> `SUM(x) OVER (ORDER BY k ROWS UNBOUNDED PRECEDING)`; `MAVG(x, n, k)` ->
  `AVG(x) OVER (ORDER BY k ROWS BETWEEN n-1 PRECEDING AND CURRENT ROW)` (skill §5 rows CSUM/MAVG/MSUM/MDIFF).
  The source writes both without a partition, and a literal reading would give a running total across all branches.
  The fixture's golden contract (`verify/expected/parity_checksums.csv`: `sum_cumulative_fees 208824.74`,
  `sum_moving_avg_volume 6789480.28`, `verify/checks/20_branch_performance.sql`) is only reproduced with
  `PARTITION BY BRANCH_ID` (an unpartitioned frame gives 2011770.49 / 6839911.08 on the seed data, checked with
  DuckDB), so the conversion keeps the per-branch reset and records the divergence from the literal text here.
- Nested aggregate inside a window (`SUM(SUM(...)) OVER`) -> lifted into a CTE so the window sits over grouped rows.
- `NULLIFZERO` -> `nullif(x, 0)`; `ADD_MONTHS(CURRENT_DATE, -24)` -> `add_months(current_date(), -24)` (same month-end
  clamp on both engines for negative offsets, see skill §5).
- `x (FORMAT '9999')`, `(FORMAT 'ZZZ,ZZZ,ZZ9.99')` display formats -> dropped; `TRIM(x (FORMAT ...))` string coercion
  -> explicit `CAST(... AS STRING)` (skill §7 trap "FORMAT implicit cast").

## Recon tier that catches a wrong conversion
- `MAVG` written with `3 PRECEDING` (4-row average): **Tier 2** `sum(MOVING_AVG_VOLUME_3M)` drift on the view, and
  **Tier 4** replay of the fixture's `verify/checks/20_branch_performance.sql` signature (`sum_moving_avg_volume`,
  expected `6789480.28` in `verify/expected/parity_checksums.csv`).
- Wrong window partition (whole result set vs per branch): **Tier 3** keyed diff on `CUMULATIVE_FEES_YTD` for every
  branch after the first in sort order; Tier 2 sums may coincidentally agree, so the mapping must key this view.
- Keeping `FORMAT`-driven text output (e.g. `'  1,234.50'`) instead of a numeric: **Tier 2** type mismatch on
  `TOTAL_DEPOSITS` (aggregates fail to compute on the target side).
- `PCT_OF_REGION_DEPOSITS` rounding: **Tier 3** last-digit diffs, neutralised by `decimal_round` (half_even, places
  from the tolerance record), never by editing the tolerance file.

## Citations
- Window functions and `QUALIFY` in Databricks SQL: `databricks-dbsql` `references/best-practices.md` "DBSQL
  Performance / Query Optimization Tips" and "Quick Reference: SQL Patterns for AI Agents".
