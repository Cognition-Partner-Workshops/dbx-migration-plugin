# 04 — SPL procedure with EXIT HANDLER, OUT parameters, ACTIVITY_COUNT, SQLCODE

Source: fixture `dml/stored_procedures/sp_load_daily_transactions.sql` (verbatim). Target: UC stored procedure.

## Constructs exercised
- `REPLACE PROCEDURE` with `IN`/`OUT` parameters -> `CREATE OR REPLACE PROCEDURE ... LANGUAGE SQL SQL SECURITY INVOKER
  MODIFIES SQL DATA`; `IN ... DATE FORMAT 'YYYY-MM-DD'` -> `DATE` (FORMAT dropped).
- `DECLARE ... DEFAULT`, `SET` -> same syntax; `TIMESTAMP(0)` -> `TIMESTAMP` (precision handled by
  `datetime_utc_truncate_ms`).
- `DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ... END` -> same shape; `SET p_return_code = SQLCODE` ->
  fixed non-zero code (no cited SQLCODE/SQLSTATE read in the handler; skill §7).
- `SET v = ACTIVITY_COUNT` (x2) -> counted `SELECT COUNT(*)` keyed on `p_batch_id` (no cited row-count register).
- `INSERT ... SEL stg.*, literal, param` -> explicit column list (positional `*` insert is a silent-mismatch trap).
  The fixture has no DDL for `STG_TRANSACTIONS` / `STG_TRANSACTION_ERRORS`; the list in the converted file is inferred
  from the columns the procedure reads and must be confirmed against `SHOW TABLE` on the live engine.
- `ZEROIFNULL` -> `COALESCE(x, 0)`; `SEL` -> `SELECT`.
- `DATE (TIMESTAMP(6)) + (TIME - TIME '00:00:00' HOUR TO SECOND)` -> timestamp built from date and time string
  (target has no TIME type; type map row TIME(n)).
- `CAST(CAST(d AS DATE FORMAT 'YYYYMMDD') AS INTEGER)` -> `CAST(date_format(d, 'yyyyMMdd') AS INT)`.
- `TRIM(n (FORMAT 'Z(9)9'))`, `CAST(x AS VARCHAR(26))` -> `CAST(... AS STRING)`.
- `(ts - ts) SECOND(4)` interval -> `unix_timestamp` difference.
- `COLLECT STATISTICS` inside the procedure -> moved to the unit's maintenance task.

## Recon tier that catches a wrong conversion
- `ACTIVITY_COUNT` mis-scoped (counting all rows in `FACT_TRANSACTION`, not the batch): the OUT value is only visible
  as text in `ETL_LOG`, so the catching check is **Tier 1** on `FACT_TRANSACTION` per `ETL_BATCH_ID` compared with the
  legacy log's "Inserted:" number parsed once during shadow-run; and the fixture's `verify/checks/10_regulatory.sql`
  (`row_count`, `sum_base_amount` over the transactions the load produced) replayed as **Tier 4**.
- Positional `INSERT ... SELECT stg.*` with a reordered target: **Tier 3** keyed diff on `STG_TRANSACTION_ERRORS`
  (values land in wrong columns; Tier 1/2 may pass).
- `TRANSACTION_TS` built at the wrong precision / timezone: **Tier 3** keyed diff after `datetime_utc_truncate_ms`.
- `BASE_CURRENCY_AMOUNT` rounding (`DECIMAL * DECIMAL` scale): **Tier 2** sum drift; `decimal_round` half_even
  places from the tolerance record.
- Handler that swallows the exception without setting `p_return_code <> 0`: **Tier 1** on `ETL_BATCH_CONTROL`
  (`FAILED` rows missing) via example 03's error branch.

## Citations
- `CREATE PROCEDURE` syntax, parameter modes, required/optional characteristics, "DEFAULT is not supported for OUT
  parameters": `databricks-dbsql` `references/sql-scripting.md` "Stored Procedures / CREATE PROCEDURE".
- `DECLARE EXIT HANDLER FOR SQLEXCEPTION`: same file, "Exception Handling / Handler Declaration".
- `SET var = (SELECT ...)`: same file, "Quick Reference Card / Stored Procedure Skeleton".
- `ANALYZE` for statistics: `databricks-dbsql` `references/best-practices.md` "OPTIMIZE, VACUUM, and ANALYZE".

## Not verified live
- Reading the caught SQLSTATE/message inside an EXIT handler (the official skill shows handlers by SQLSTATE but no
  read of the current diagnostics); if available it replaces the fixed `-1`.
- A row-count register equivalent to `ACTIVITY_COUNT` after `INSERT ... SELECT` inside a compound statement.
