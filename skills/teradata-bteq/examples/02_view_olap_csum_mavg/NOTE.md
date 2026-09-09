# 02 — View with OLAP shorthands (CSUM, MAVG), FORMAT casts, NULLIFZERO, LOCKING modifier

Source: fixture `ddl/views/03_vw_branch_performance.sql` (verbatim). Target: Databricks SQL view.

## Constructs
- `CSUM(x, k)` -> `SUM(x) OVER (ORDER BY k ROWS UNBOUNDED PRECEDING)`; `MAVG(x, n, k)` ->
  `AVG(x) OVER (ORDER BY k ROWS BETWEEN n-1 PRECEDING AND CURRENT ROW)`.
- Partition: the source writes both without one; the fixture's golden checksums
  (`verify/expected/parity_checksums.csv`: `sum_cumulative_fees 208824.74`, `sum_moving_avg_volume 6789480.28`)
  are only reproduced with `PARTITION BY BRANCH_ID` (unpartitioned gives 2011770.49 / 6839911.08, checked with
  DuckDB), so the per-branch reset is kept and the divergence from the literal text recorded here.
- Aggregate inside a window (`SUM(SUM(...)) OVER`) -> CTE; `NULLIFZERO` -> `nullif(x, 0)`; `ADD_MONTHS` ->
  `add_months`; `LOCKING ROW FOR ACCESS` dropped; `TRIM(x (FORMAT '9999'))` -> explicit `CAST(... AS STRING)`.

## Recon tier that catches a wrong conversion
- `MAVG` with `3 PRECEDING` (4-row frame): **Tier 4** replay of `verify/checks/20_branch_performance.sql`
  (`sum_moving_avg_volume` must be `6789480.28`); **Tier 2** sum drift on `MOVING_AVG_VOLUME_3M`.
- Wrong partition: **Tier 3** keyed diff on `CUMULATIVE_FEES_YTD` for every branch after the first.
- `FORMAT` text kept instead of a numeric: **Tier 2** type mismatch on `TOTAL_DEPOSITS`.
- `PCT_OF_REGION_DEPOSITS` last-digit diffs: neutralised by `decimal_round` (half_even), never by editing tolerances.

Citations: `databricks-dbsql` `references/best-practices.md` "Query Optimization Tips" (window functions, CTEs).
