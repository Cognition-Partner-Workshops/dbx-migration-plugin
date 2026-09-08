# 05 — Macro with defaulted parameters and three result sets

Source: fixture `dml/macros/macro_aml_screening.sql` (verbatim). Target: UC procedure materialising a result table +
one view per result set.

## Constructs exercised
- `REPLACE MACRO name (p TYPE DEFAULT ...) AS ( stmt; stmt; stmt; )` -> `CREATE OR REPLACE PROCEDURE` with `IN ...
  DEFAULT`; `:param` references -> plain parameter names.
- `DEFAULT DATE` (today) -> `DEFAULT NULL` + `COALESCE(p, current_date())` in the body.
- Multiple result sets from one `EXEC` -> rows tagged `RESULT_SET_NO` in a Delta table; per-set views for positional
  consumers (skill §6 "Macros").
- `ORDER BY` on a result set -> `SORT_ORDER` column (`ROW_NUMBER() OVER (ORDER BY ...)`); the view consumer sorts.
- `DATE - INTEGER` -> `date_add(d, -n)`; `DATE + 3` -> `date_add(d, 3)`; `DATE - DATE` -> `datediff` (skill §5).
- `x (FORMAT 'ZZZ,ZZZ,ZZ9.99')`, `(FORMAT 'YYYY-MM-DD')` -> dropped; values stay typed.
- `BETWEEN (:t * 0.8) AND :t` DECIMAL(15,2) * literal -> both engines produce a DECIMAL; boundary rows are the recon
  signature for a scale/rounding mismatch.
- `QUALIFY` kept in the view (supported in Databricks SQL; skill §5).

## Recon tier that catches a wrong conversion
- Off-by-one on `date_add(v_date, -lookback_days)` vs Teradata `BETWEEN (d - n) AND d` (inclusive both ends):
  **Tier 1** row count per `RESULT_SET_NO` on the screening date.
- `amount_threshold * 0.8` boundary (DECIMAL scale differs so `40000.00` vs `40000.000`): **Tier 3** keyed diff on
  `(CUSTOMER_ID, ACCOUNT_ID)` for STRUCTURING rows exactly at the boundary; `decimal_round` places from the tolerance
  record.
- `AVG_AMOUNT` scale (Teradata AVG of DECIMAL(15,2) keeps scale 2; Spark widens): **Tier 2** after `decimal_round`.
- `CUSTOMER_NAME` built from `NOT CASESPECIFIC` columns: joins are on keys here so no drift, but **Tier 3** would flag
  a case-differing `KYC_STATUS` grouping if the target column lost `UTF8_LCASE` (example 01).
- Result sets merged into one table without `RESULT_SET_NO`: **Tier 1** on each per-set view (counts collapse).

## Citations
- `CREATE PROCEDURE` with `IN ... DEFAULT` and the all-subsequent-defaults rule: `databricks-dbsql`
  `references/sql-scripting.md` "Stored Procedures / CREATE PROCEDURE".
- `DECLARE`/`SET`, compound body: same file, "Compound Statements", "Variable Assignment (SET)".
- `QUALIFY`, window functions: `databricks-dbsql` `references/best-practices.md` "Query Optimization Tips" and
  "Quick Reference: SQL Patterns for AI Agents".

## Not verified live
- Whether a non-constant expression (`current_date()`) is accepted as a parameter `DEFAULT`; the NULL-sentinel form is
  used so the example does not depend on it.
- Whether the target exposes procedure result sets to a client (would let the views be dropped).
