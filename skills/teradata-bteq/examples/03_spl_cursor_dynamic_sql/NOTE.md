# 03 — SPL procedure: cursor loop, dynamic SQL, BT/ET, EXIT handler, INOUT budget

Source: skill-authored (fixture schema `DIM_ACCOUNT`, `FACT_TRANSACTION`, `ETL_LOG`). Target: UC SQL procedure.

## Constructs
- `DECLARE CURSOR`/`OPEN`/`FETCH`/`CLOSE` + `CONTINUE HANDLER FOR NOT FOUND` + `WHILE`/`LEAVE` -> one `FOR ... DO`
  with `LEAVE` for the batch cap (only `EXIT` handlers are cited; `NOT FOUND` is not needed with `FOR`).
- `CALL DBC.SysExecSQL(v)` -> `EXECUTE IMMEDIATE`; values through `USING ?`, the schema identifier through a
  build-time allowlist (`${schema}`, `${archive_schema}`), never the caller's string.
- `ACTIVITY_COUNT` -> explicit `SELECT COUNT(*)` (`INTO`); `SQLCODE`/`SQLSTATE` in the handler -> fixed return code.
- `BT`/`ET` + `ROLLBACK` -> no drop-in. Per-statement Delta commits; the archive `INSERT` is idempotent
  (`NOT EXISTS` on `TRANSACTION_ID`), the handler charges the `INOUT` budget with the accounts whose status `UPDATE`
  committed, and a re-run finishes the batch. `BEGIN ATOMIC` (preview, `catalogManaged` tables) is the alternative
  where the target profile allows it.
- BT/ET also serialised overlapping callers (write locks held to `ET`); nothing does here. Decision item: run the
  procedure from a job with `max_concurrent_runs: 1` (`databricks-jobs` `references/triggers-schedules.md`) or add
  an explicit single-row lock table; recorded in `06_decisions.md`, not assumed.

## Recon tier that catches a wrong conversion
- **Tier 2** conservation: `sum(AMOUNT)` over `FACT_TRANSACTION` + archive must equal the source total; a
  non-idempotent `INSERT` after a mid-triple failure shows as archive excess.
- **Tier 1** `p_accounts_done` vs `count(ACCOUNT_STATUS = 'ARCHIVED')` per campaign; a handler that does not
  deduct from `p_max_batch` over-archives on retry.
- **Tier 3** keyed diff on rows present in both fact and archive (should be empty after a completed run).

## Not verified live
`FOR` with a labelled `LEAVE`; `EXECUTE IMMEDIATE ... INTO ... USING`; `INOUT` write-back from inside an `EXIT`
handler; `BEGIN ATOMIC` as a BT/ET replacement.
