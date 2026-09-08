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
  `EXECUTE IMMEDIATE ... INTO`. The schema name cannot go through a marker (identifiers are not bind values), so
  `p_archive_schema` is checked against a build-time allowlist (`NOT IN ('${schema}', '${archive_schema}')`, both
  substituted from the unit mapping like `${catalog}.${schema}`) and the procedure `SIGNAL`s before any dynamic
  statement runs. The catalog is a literal, so the caller can move neither catalog nor schema; the write-scope hook
  is the outer gate on the catalog. The source `TRIM(p_archive_db)` splice carried the full injection surface --
  conversion is where it gets closed. (An identifier-shape regex alone would still let a caller pick any well-formed
  schema the invoker can write to.)
- `CREATE MULTISET TABLE x AS y WITH NO DATA` -> `CREATE TABLE IF NOT EXISTS x AS SELECT * FROM y WHERE 1 = 0`.
- `BT; ... ET;` + `ROLLBACK` in the handler -> no drop-in multi-statement transaction for a compound (skill §6 row
  "Transactions", §7 trap "BT/ET"); loop body made idempotent instead: the archive `INSERT` carries
  `NOT EXISTS (... a.TRANSACTION_ID = ft.TRANSACTION_ID)` so a retry after a failure between INSERT and DELETE copies
  nothing twice; DELETE and the status UPDATE are repeatable by construction. Conservation invariant: every
  `TRANSACTION_ID` is in the fact, in the archive, or transiently in both -- never twice in the archive.
  Because partial work is kept rather than rolled back, the EXIT handler also writes back the consumed budget
  (`p_max_batch = p_max_batch - p_accounts_done`): the source's ROLLBACK left the budget untouched *and* the accounts
  untouched; here the accounts stay archived, so the budget must follow. Retry contract: call again with the returned
  `p_max_batch`. The handler charges what is *persisted*, not what the loop counted: it recomputes `p_accounts_done`
  as `COUNT(*) FROM DIM_ACCOUNT WHERE ACCOUNT_STATUS = 'ARCHIVED' AND ETL_UPDATE_TS >= v_start_ts` (the call's start
  timestamp) before deducting. The status `UPDATE` is an account's commit point -- it is what removes the account from
  a retry's cursor -- so an account interrupted anywhere in its triple (still `CLOSED`) is neither counted nor charged
  and the retry redoes it and charges it once; an account whose `UPDATE` landed is charged exactly once whichever
  statement failed next. The campaign therefore ends exactly at the cap: a counter-based deduction (an earlier
  revision incremented before the `UPDATE` and deducted the counter) charged accounts whose `UPDATE` never committed
  and left the retry underfilling the batch. If the session itself dies (no handler, no OUT values), the caller
  applies the same expression with the campaign's start instead of reusing the last returned value.
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
  tables is a **Tier 3** keyed diff on `TRANSACTION_ID`. An archive copy without the `NOT EXISTS` guard shows up as
  **Tier 1** `count(*) > count(distinct TRANSACTION_ID)` on the archive table after any retried run.
- Unvalidated `p_archive_schema` (writes redirected outside the declared scope): not a recon signature at all -- the
  allowlist `SIGNAL` in the procedure is the in-band gate and the write-scope hook is the catalog-level outer gate.
- LOAN branch mis-mapped (archived anyway): **Tier 1** on `FACT_TRANSACTION` rows for loan accounts.
- `INOUT` remaining budget not written back: caught only by the caller's next batch size — **Tier 1** over the whole
  archive campaign (total accounts archived vs legacy).
- Failure after k accounts, then retry with the original budget (handler not reconciling `p_max_batch`): the campaign
  archives up to `k` accounts more than the source did for the same sequence of calls -> **Tier 1** on
  `DIM_ACCOUNT WHERE ACCOUNT_STATUS = 'ARCHIVED'` after the retry (`p_max_batch + k` vs `p_max_batch`). The shadow-run
  must include one injected failure after at least one completed account, followed by the retry.
- Handler deducting the loop counter instead of the persisted count (counter incremented before an `UPDATE` that then
  fails): the k-th account is charged but still `CLOSED`, the retry redoes it inside a budget one too small -> the
  campaign ends one account *short* of the cap: **Tier 1** on `DIM_ACCOUNT WHERE ACCOUNT_STATUS = 'ARCHIVED'` after
  the retry (`p_max_batch - 1` vs `p_max_batch`). The mirror bug (counter after the `UPDATE`, handler trusting it,
  failure between the two) ends one *past* the cap. Both are the same Tier 1 signature with opposite sign, so the
  shadow-run needs the injected failure placed once on the `UPDATE` statement and once on the `SET` after it.

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
- That `INOUT`/`OUT` values assigned inside an EXIT handler are returned to the caller (the budget write-back relies on
  it); if not, the remaining budget must be persisted to a control row from the handler instead.
- That `current_timestamp()` assigned to `v_start_ts` and the one stamped by each `UPDATE` come from the same clock
  such that `ETL_UPDATE_TS >= v_start_ts` holds for every account this call archived (a per-call marker column or a
  ledger table replaces the timestamp predicate if it does not).
