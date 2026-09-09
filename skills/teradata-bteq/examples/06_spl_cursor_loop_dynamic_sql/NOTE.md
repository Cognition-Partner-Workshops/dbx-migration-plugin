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
  `p_max_batch`. The handler charges what is *persisted and this call's*, not what the loop counted: each call mints
  `v_run_id = uuid()` and writes one `ARCHIVE_RUN_LEDGER (RUN_ID, CLOSED_BEFORE, ACCOUNT_KEY, LEDGER_TS)` row per
  account immediately before that account's status `UPDATE`; the handler recomputes `p_accounts_done` as
  `COUNT(*) FROM ARCHIVE_RUN_LEDGER l JOIN DIM_ACCOUNT a ON a.ACCOUNT_KEY = l.ACCOUNT_KEY WHERE l.RUN_ID = v_run_id AND
  a.ACCOUNT_STATUS = 'ARCHIVED'` before deducting. The status `UPDATE` is an account's commit point -- it is what
  removes the account from a retry's cursor -- so ledger-row-and-`ARCHIVED` is exactly "this call's `UPDATE` landed":
  an account interrupted anywhere in its triple (still `CLOSED`) is neither counted nor charged and the retry redoes
  it (under its own `RUN_ID`) and charges it once; an account whose `UPDATE` landed is charged exactly once whichever
  statement failed next. The campaign therefore ends exactly at the cap. Two earlier revisions got this wrong in
  opposite ways: deducting the loop counter charged accounts whose `UPDATE` never committed (retry underfills), and
  counting `ACCOUNT_STATUS = 'ARCHIVED' AND ETL_UPDATE_TS >= v_start_ts` charged accounts a *concurrent* call archived
  in the same window (two campaigns or two schedules overlapping; retry underfills by the other call's work) -- the
  per-call `RUN_ID` is what makes the count exact under concurrency. If the session itself dies (no handler, no OUT
  values), the caller recovers the campaign's consumption as `COUNT(DISTINCT l.ACCOUNT_KEY)` over the same join with
  `l.CLOSED_BEFORE = <campaign date>` (an account interrupted in one call and finished by the retry has a ledger row
  under each `RUN_ID`). The ledger is the second unit-owned table this example adds (the archive table is the first);
  `CREATE TABLE IF NOT EXISTS` ships in the converted file ahead of the procedure, as example 05 does.
- `BT; ... ET;` *lock scope* -> `ARCHIVE_CAMPAIGN_LOCK`, the third unit-owned table (one seeded row). On the source the
  loop's UPDATEs and DELETEs held write locks until `ET`, so a second overlapping call blocked and never saw the same
  `CLOSED` account; without BT/ET two calls whose cursors overlap would each run the triple for the shared accounts,
  each write a ledger row and each charge them, and the campaign would end short by that many (the per-call ledger
  makes the count *exact per call*, it does not stop two calls from claiming one account). The procedure therefore
  takes the lock before its first read: `UPDATE ... SET OWNER_RUN_ID = v_run_id WHERE OWNER_RUN_ID IS NULL`, reads
  the row back, and `SIGNAL`s `75003` unless the owner is `v_run_id`. Two calls racing for the row either serialise
  (the second finds the first's id) or conflict at commit -- Delta `UPDATE + UPDATE` on the same row "can conflict"
  (docs.databricks.com/aws/en/optimizations/isolation/row-level-concurrency, opened), the loser's `UPDATE` raises and
  its handler runs -- so exactly one call owns the row and its cursor is the only one selecting `CLOSED` accounts:
  every account it archives is claimed by it alone. The handler and the normal exit release the lock with
  `WHERE OWNER_RUN_ID = v_run_id`, so a call that failed *on* the lock never frees the owner's. A call refused the lock
  has archived and charged nothing; the caller retries with an unchanged budget once the owner finishes (on the source
  it would have waited on the lock instead of returning). Session death while holding the lock leaves
  `OWNER_RUN_ID` set: the next call is refused with that id in its message; the operator confirms the run is dead
  (no active job run, no `ETL_LOG` row for the id) and frees it with `UPDATE ... SET OWNER_RUN_ID = NULL WHERE
  OWNER_RUN_ID = '<id>'`. Freeing it automatically after a timeout needs the campaign's maximum runtime, which is an
  engagement decision (`.migration/06_decisions.md`), not a default this example can pick.
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
- Handler counting by time window instead of by call (`ARCHIVED AND ETL_UPDATE_TS >= v_start_ts`, no ledger): with
  a second call archiving m accounts in the same window, the failed call charges `k + m`, and its retry archives m
  fewer -> **Tier 1** on `DIM_ACCOUNT WHERE ACCOUNT_STATUS = 'ARCHIVED'` for the campaign (`p_max_batch - m` vs
  `p_max_batch`). The shadow-run therefore needs one run with two overlapping calls (different `p_closed_before`),
  one of them with the injected failure. Ledger row missing or written *after* the `UPDATE` (failure between the two
  leaves an `ARCHIVED` account with no ledger row): one *past* the cap, same signature as the mirror bug above.
- No campaign lock (two overlapping calls both admitted): both cursors select the same `CLOSED` accounts, both run the
  triple, both write a ledger row and both charge each shared account, so the two calls together archive `s` fewer
  accounts than their budgets add up to -> **Tier 1** on `DIM_ACCOUNT WHERE ACCOUNT_STATUS = 'ARCHIVED'` for the two
  campaigns (`b1 + b2 - s` vs `b1 + b2`), and `count(*) > count(distinct ACCOUNT_KEY)` on `ARCHIVE_RUN_LEDGER` with
  *both* rows paired to `ARCHIVED` (a retry after an interrupted triple also leaves two rows, but only the later one
  pairs with a status its call set -- the recon distinguishes them by `LEDGER_TS < DIM_ACCOUNT.ETL_UPDATE_TS` on both
  rows). The shadow-run's overlapping-call scenario above exercises it; the expected outcome with the lock is that
  the second call returns `75003` and archives nothing.
- Lock released in the handler without the `OWNER_RUN_ID = v_run_id` predicate: a call refused the lock frees the
  owner's lock on its way out, and a third call is admitted alongside the owner -> same double-claim signature as
  above, only visible when the shadow-run issues *three* overlapping calls. Lock never released on success: the second
  scheduled campaign is refused forever -> **Tier 1** zero archived rows for every later campaign.

## Citations
- `FOR ... AS query DO`, `WHILE`, `LEAVE`, `CASE` statement: `databricks-dbsql` `references/sql-scripting.md`
  "Control Flow".
- `DECLARE EXIT HANDLER`, `NOT FOUND` as a condition value, EXIT as the only handler type: same file, "Exception
  Handling / Handler Declaration".
- `SIGNAL SQLSTATE ... SET MESSAGE_TEXT`: same file, "SIGNAL and RESIGNAL".
- `EXECUTE IMMEDIATE ... INTO ... USING`: same file, "EXECUTE IMMEDIATE (Dynamic SQL)".
- `INOUT` parameter mode: same file, "Stored Procedures / CREATE PROCEDURE".
- `uuid()` returns a 36-character UUID string, non-deterministic: docs.databricks.com/aws/en/sql/language-manual/
  functions/uuid.
- Transactions: same file, "Multi-Statement Transactions" (status and SQL scripting atomic blocks); routed through
  `target-routing`, not restated here.
- Concurrent `UPDATE + UPDATE` on the same row "can conflict" under both isolation levels; writers "see a consistent
  snapshot view of the table and writes occur in a serial order": https://docs.databricks.com/aws/en/optimizations/
  isolation/row-level-concurrency and https://docs.databricks.com/aws/en/optimizations/isolation-level (opened).

## Not verified live
- Whether an atomic block (`BEGIN ATOMIC ... END`, per the "Multi-Statement Transactions" section's preview status)
  can wrap the per-account triple to restore BT/ET semantics; the example does not depend on it.
- Reading SQLSTATE inside a handler; row-count register after DML.
- That `INOUT`/`OUT` values assigned inside an EXIT handler are returned to the caller (the budget write-back relies on
  it); if not, the remaining budget must be persisted to a control row from the handler instead.
- That a procedure-local `DECLARE`d variable assigned from `uuid()` (docs.databricks.com/aws/en/sql/language-manual/
  functions/uuid, "non-deterministic") is evaluated once per call and stable across the loop and the handler (the
  ledger keys on it); if it were re-evaluated per reference, `v_run_id` would have to be passed in by the caller as an
  `IN` parameter instead.
- That the second-pass INFO summary (`ARCHIVED AND CAST(ETL_UPDATE_TS AS DATE) = current_date()`, kept as on the
  source) is acceptable when two calls run on the same day; it is a log line, not a budget input.
- That a Delta write-conflict raised by the lock `UPDATE` surfaces inside the procedure as a `SQLEXCEPTION` the EXIT
  handler catches (the cited page documents the conflict, not how SQL scripting reports it), and that a `SET v =
  (SELECT ...)` issued right after this call's own committed `UPDATE` reads that commit (Delta snapshot per statement is
  the assumption). If either fails live, the lock still serialises correctly -- the loser stops either way -- but its
  error path would differ from the `75003` message.
