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
- Failure after the `FACT_TRANSACTION` insert. Neither engine rolls back (the source has no BT/ET; the target compound
  has no cited multi-statement transaction), so the committed rows survive under the failed batch id and example 03
  re-runs the date under a new batch. Both `INSERT ... SELECT`s carry `NOT EXISTS` on `TRANSACTION_ID` (the stable
  identity: fixture `COLLECT STATISTICS COLUMN (TRANSACTION_ID)`, example 07 quarantines duplicate ids upstream), and
  the completing batch *adopts* the rows of the date's **incomplete** earlier batches (`UPDATE ... SET ETL_BATCH_ID /
  BATCH_ID ... WHERE NOT EXISTS (control row with BATCH_STATUS = 'COMPLETED' for the row's batch)`). Ownership, not
  the date, is the gate: a batch that completed keeps every row it wrote even when the same `TRANSACTION_ID`s are
  still in staging for that `LOAD_DATE` (the `NOT EXISTS` guard already skipped re-inserting them), so a later run
  for an already-loaded date only adds what is missing and never rewrites history. Incomplete means no `COMPLETED`
  row in `ETL_BATCH_CONTROL`: example 03 writes `FAILED` for a run that reached its error branch and nothing for a
  run that died, and both are adopted. The OUT counts and example 03's per-batch `LOADED_ROWS`/`ERROR_ROWS` then
  describe the whole attempt chain, and a transaction is never in the fact twice. Source parity note: a re-run on
  Teradata *would* duplicate, so the shadow-run compares against the legacy first-attempt result for that date, not
  against a replayed legacy retry.
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
- Re-run without the `NOT EXISTS` guards (failure after the fact insert, then a new batch for the same date):
  **Tier 1** `count(*) > count(distinct TRANSACTION_ID)` on `FACT_TRANSACTION` for that `TRANSACTION_DATE`, **Tier 2**
  doubled `sum(BASE_CURRENCY_AMOUNT)`, and the fixture's `10_regulatory.sql` signature for the date. Without the
  batch adoption `UPDATE`, the guards hold but example 03's report shows `LOADED_ROWS + ERROR_ROWS < STAGED_ROWS` for the
  completing batch (**Tier 1** on the report). The shadow-run must include one injected failure after the fact insert
  (e.g. on the completion log write) followed by the re-run.
- Adoption keyed on the date instead of on ownership (an earlier revision re-stamped every fact/error row whose
  `TRANSACTION_ID` was in staging for the date): a second run for a date that already `COMPLETED` moves that batch's
  rows to the new id -> **Tier 1** on `FACT_TRANSACTION` per `ETL_BATCH_ID` (the completed batch drops to 0, the new
  one is over) and **Tier 1** on example 03's `RPT_DAILY_RECONCILIATION` (`LOADED_ROWS` for the old batch no longer
  matches its legacy report line). The shadow-run calendar therefore needs one deliberate second run of an
  already-completed date with staging left in place.

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
