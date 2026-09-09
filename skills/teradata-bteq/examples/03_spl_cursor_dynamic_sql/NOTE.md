# 03 — SPL procedure: cursor loop, dynamic SQL, BT/ET, EXIT handler, INOUT budget

Source: skill-authored (fixture schema `DIM_ACCOUNT`, `FACT_TRANSACTION`, `ETL_LOG`). Target: UC SQL procedure.

## Constructs
- `DECLARE CURSOR`/`OPEN`/`FETCH`/`CLOSE` + `CONTINUE HANDLER FOR NOT FOUND` + `WHILE`/`LEAVE` -> one `FOR ... DO`
  with `LEAVE` for the batch cap (only `EXIT` handlers are cited; `NOT FOUND` is not needed with `FOR`).
- `CALL DBC.SysExecSQL(v)` -> `EXECUTE IMMEDIATE`; values through `USING ?`, the schema identifier through a
  build-time allowlist (`${schema}`, `${archive_schema}`), never the caller's string.
- `ACTIVITY_COUNT` -> explicit `SELECT COUNT(*)` (`INTO`); `SQLCODE`/`SQLSTATE` in the handler -> fixed return code.
- `BT`/`ET` + `ROLLBACK` -> no drop-in. Per-statement Delta commits; the archive `INSERT` is idempotent
  (`NOT EXISTS` on `TRANSACTION_ID`); an account interrupted mid-triple stays `CLOSED` and is finished by the re-run.
  The `INOUT` budget is never taken from the in-memory counter (which only caps the loop): the status `UPDATE` also
  stamps `ETL_BATCH_ID = v_run_id` (`unix_micros(current_timestamp())`, docs.databricks.com `functions/unix_micros`),
  and both exits derive `p_accounts_done` as `count(ACCOUNT_STATUS = 'ARCHIVED' AND ETL_BATCH_ID = v_run_id)`, so
  that one committed row is the progress record and rows archived by any other writer (other id) are never charged.
  `BEGIN ATOMIC` (preview, `catalogManaged` tables) is the alternative where the target profile allows it.
- BT/ET also serialised overlapping callers (write locks held to `ET`). Replaced by a one-row
  `ARCHIVE_CAMPAIGN_LOCK (LOCK_NAME, OWNER_RUN_ID, LOCKED_TS)` whose DDL + idempotent `MERGE` seed ship in the
  converted file ahead of the procedure: claim with `UPDATE ... WHERE OWNER_RUN_ID IS NULL`, read back,
  `SIGNAL 75003` if not the owner (covers a missing row: read-back is NULL); released (owner-checked) on both exits.
  A session killed mid-run reaches neither exit: the stale lock row keeps `OWNER_RUN_ID`, so the operator computes
  the consumed budget with the same count before releasing the row (`max_concurrent_runs: 1` on the calling job,
  `databricks-jobs` `references/triggers-schedules.md`, as belt-and-braces). Decision item: stale-lock timeout.

## Recon tier that catches a wrong conversion
- **Tier 2** conservation: `sum(AMOUNT)` over `FACT_TRANSACTION` + archive must equal the source total; a
  non-idempotent `INSERT` after a mid-triple failure shows as archive excess.
- **Tier 1** `p_accounts_done` vs `count(ACCOUNT_STATUS = 'ARCHIVED')` per campaign; a budget taken from an
  in-memory counter, or two unserialised callers, over- or under-archive on retry.
- **Tier 3** keyed diff on rows present in both fact and archive (should be empty after a completed run).

## Not verified live
`FOR` with a labelled `LEAVE`; `EXECUTE IMMEDIATE ... INTO ... USING`; `INOUT` write-back from inside an `EXIT`
handler; `BEGIN ATOMIC` as a BT/ET replacement; write-conflict behaviour of two `UPDATE`s on the same lock row.
