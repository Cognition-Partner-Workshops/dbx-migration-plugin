# 06 — SPL cursor loop, CONTINUE handler, dynamic SQL, BT/ET transaction, INOUT parameter

Source: skill-authored minimal fixture (`source.sp_archive_closed_accounts.td.sql`) because the estate has no
procedure with cursors, loops, dynamic SQL, or explicit transactions. Uses the fixture's `DIM_ACCOUNT`,
`FACT_TRANSACTION`, `ETL_LOG`. Target: UC stored procedure.

## Constructs exercised
- `DECLARE cur CURSOR FOR ...; OPEN; FETCH ... INTO; CLOSE` + `WHILE ... DO ... END WHILE` + `DECLARE CONTINUE HANDLER
  FOR NOT FOUND` -> one labelled `FOR row AS <query> DO ... END FOR` (cursor exhaustion ends the loop, so the NOT FOUND
  handler disappears); `LEAVE label` for the batch cap (skill §6 rows "Cursors", "Loops", "Exception handling").
- `FOR typ AS type_cur CURSOR FOR ... DO ... END FOR` -> `FOR typ AS ... DO ... END FOR` (1:1).
- `CASE v WHEN ... THEN ... ELSE ... END CASE` -> same syntax.
- `CALL DBC.SysExecSQL(v_sql)` -> `EXECUTE IMMEDIATE`; string-spliced key -> `USING ?` parameter marker; row count via
  `EXECUTE IMMEDIATE ... INTO`.
- `CREATE MULTISET TABLE x AS y WITH NO DATA` -> `CREATE TABLE IF NOT EXISTS x AS SELECT * FROM y WHERE 1 = 0`.
- `BT; ... ET;` + `ROLLBACK` in the handler -> no drop-in multi-statement transaction for a compound (skill §6 row
  "Transactions", §7 trap "BT/ET"); loop body made idempotent instead.
- `SIGNAL SQLSTATE '75001' SET MESSAGE_TEXT` -> same syntax.
- `INOUT` parameter -> `INOUT` (same).
- `EXTRACT(YEAR FROM d) (FORMAT '9999')`, `TRIM(n (FORMAT '-(18)9'))` -> `CAST(year(d) AS STRING)` / parameter marker.
- `SQLSTATE`/`SQLCODE` read in handler -> fixed code + message (no cited read; Not verified live).
- Teradata database name parameter -> UC schema name; archive schema must be in the unit's write scope.

## Recon tier that catches a wrong conversion
- Batch cap off-by-one (`>=` vs `>`) : **Tier 1** row count on `DIM_ACCOUNT WHERE ACCOUNT_STATUS = 'ARCHIVED'` per run.
- Archive INSERT missing the key filter (whole fact copied): **Tier 1** on `FACT_TRANSACTION_ARCH_<yyyy>` vs
  `FACT_TRANSACTION` rows removed; **Tier 2** `sum(BASE_CURRENCY_AMOUNT)` conservation across the two tables.
- Loss of BT/ET atomicity when a mid-loop failure occurs (archived but not deleted, or deleted but not archived):
  **Tier 2** conservation check `sum(amount) fact + sum(amount) archive = legacy total`; a row appearing in both
  tables is a **Tier 3** keyed diff on `TRANSACTION_ID`.
- LOAN branch mis-mapped (archived anyway): **Tier 1** on `FACT_TRANSACTION` rows for loan accounts.
- `INOUT` remaining budget not written back: caught only by the caller's next batch size — **Tier 1** over the whole
  archive campaign (total accounts archived vs legacy).

## Citations
- `FOR ... AS query DO`, `WHILE`, `LEAVE`, `CASE` statement: `databricks-dbsql` `references/sql-scripting.md`
  "Control Flow".
- `DECLARE EXIT HANDLER`, `NOT FOUND` as a condition value, EXIT as the only handler type: same file, "Exception
  Handling / Handler Declaration".
- `SIGNAL SQLSTATE ... SET MESSAGE_TEXT`: same file, "SIGNAL and RESIGNAL".
- `EXECUTE IMMEDIATE ... INTO ... USING`: same file, "EXECUTE IMMEDIATE (Dynamic SQL)".
- `INOUT` parameter mode: same file, "Stored Procedures / CREATE PROCEDURE".
- Transactions: same file, "Multi-Statement Transactions" (status and SQL scripting atomic blocks); routed through
  `target-routing`, not restated here.

## Not verified live
- Whether an atomic block (`BEGIN ATOMIC ... END`, per the "Multi-Statement Transactions" section's preview status)
  can wrap the per-account triple to restore BT/ET semantics; the example does not depend on it.
- Reading SQLSTATE inside a handler; row-count register after DML.
