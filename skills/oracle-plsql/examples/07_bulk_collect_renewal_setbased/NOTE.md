# 07 — Package with cursor + `BULK COLLECT LIMIT` + `FORALL` -> set-based DBSQL stored procedure

**Source**: `fixture/08_pkg_policy_renewal.sql` (spec + body). Package constants and state, `PRAGMA EXCEPTION_INIT`,
associative arrays, explicit cursor `FOR UPDATE ... SKIP LOCKED`, `FETCH ... BULK COLLECT INTO ... LIMIT`, `FORALL`
update + insert with `policy_seq.NEXTVAL`, `SQL%ROWCOUNT`, `SAVEPOINT`/`ROLLBACK TO`, `DUP_VAL_ON_INDEX`,
`NO_DATA_FOUND`/`TOO_MANY_ROWS`, `RAISE_APPLICATION_ERROR`, `WHEN OTHERS` swallow into a status parameter,
`EXECUTE IMMEDIATE ... USING` with a runtime table name, `SYS_REFCURSOR` function, `DBMS_OUTPUT`, `ADD_MONTHS`.

**Profile / track**: analytical DBSQL (routing order §6: SQL Scripting / `CREATE PROCEDURE` first; Jobs control flow
for the caller in example 05; no PySpark needed). Lakebase variant summarised at the end of `converted.sql`.

## Constructs exercised (SKILL.md refs)

| Oracle | Converted | Rule |
|---|---|---|
| package spec/body, constants | schema + literals | §6 packages |
| `g_run_id`, `g_rows_renewed` package state | `run_log` table row | §6 package state; §7 trap 11 |
| explicit cursor + `BULK COLLECT LIMIT` + `FORALL` | temp view of candidates + one `MERGE` + one `INSERT ... SELECT` | §6 cursors, `BULK COLLECT`/`FORALL` |
| `FOR UPDATE SKIP LOCKED` | none (snapshot isolation inside `BEGIN ATOMIC`) | §6 commit/rollback |
| `SAVEPOINT`/`ROLLBACK TO` per batch | whole-run `BEGIN ATOMIC` (decision) | §6 savepoints |
| `EXCEPTION WHEN e_premium_negative` / `WHEN OTHERS` | `DECLARE ... CONDITION` + `DECLARE EXIT HANDLER FOR` | §6 exceptions |
| `RAISE_APPLICATION_ERROR(-20002)` | `SIGNAL <condition> SET MESSAGE_TEXT` | §5 #106 |
| `SQL%ROWCOUNT` | `count(*)` of the candidate set | §5 #104 |
| `OUT` params, `IN` defaults | `OUT` without `DEFAULT`, `IN ... DEFAULT` kept | §6 parameters |
| `SYS_REFCURSOR` function | table function | §6 `SYS_REFCURSOR` |
| `EXECUTE IMMEDIATE l_sql USING p_policy_id` | `EXECUTE IMMEDIATE ... USING` with a census allow-list replacing `DBMS_ASSERT` | §6 dynamic SQL; §2 INFERRED edge `risk=dynamic-sql` |
| `NUMBER` arithmetic, `ROUND(x, 2)` | `DECIMAL(38,10)` then `round(.., 2)` | §7 traps 2, 25 |
| `ADD_MONTHS(expiry_dt, 12)` | `add_months` | §5 #57 |
| `d + n` days | `+ make_interval(0,0,0,n)` | §5 #56 |
| `DBMS_OUTPUT.PUT_LINE` | dropped | §6 external calls |

## Recon tier that catches a wrong conversion

- **Tier 1** on `poladm.premium_txn` (`'RN'` rows per run date) and on `run_log`: the set-based rewrite must
  produce exactly one `'RN'` row per renewed policy; a `MERGE` that also matches non-`LIVE` rows or a `BETWEEN`
  that lost the time component (Oracle compares `DATE` with time) changes the count.
- **Tier 2** `SUM(annual_premium)` per `product_cd` with `decimal_round(2)`: catches the uplift being applied twice,
  `broker_uplift` computed at `DECIMAL(38,10)` vs Oracle exact `NUMBER` (agreed tolerance), and `add_months`
  month-end drift.
- **Tier 3** keyed on `policy_no`: catches `row_version` not incremented (trigger fold-in forgotten), `expiry_dt`
  shifted by a day for month-end policies, and `policy_status` not reset.
- **Tier 1 on `policy_audit_log` `ERROR` rows**: the Oracle `WHEN OTHERS` path logs through the autonomous
  logger and *still* reports a status; the converted handler does the same in-transaction, so on a failure run Oracle
  has one more `ERROR` row than the target (the audit insert inside the exit handler is rolled back only if the
  handler itself is inside the atomic block, which it is not). The expected delta is recorded up front.
- The swallowed-exception contract (`p_status_out` instead of a raised error) is checked at **Tier 4**: the
  scheduler wrapper must fail the run when `status_out <> 'OK'`; if the converted caller ignores it, a failed
  renewal looks green, exactly the Oracle failure mode §7 trap 13 describes.

## Canonicalization used

`decimal_round` (`annual_premium`, `amount`, `tax_amount`), `datetime_utc_truncate_ms` (`expiry_dt`, `txn_dt`),
`null_missing_equiv` (`broker_id`).

## Open decisions

1. Per-batch `SAVEPOINT` semantics (partial success of 500-row batches) are not reproduced; the run is all-or-nothing.
2. `SKIP LOCKED` concurrency (two Oracle sessions splitting the sweep) is not reproduced; `max_concurrent_runs: 1`.
3. `archive_to` allow-list contents come from the census; a table added later needs a code change.

## Not verified live

`CREATE PROCEDURE` with `OUT` params, `DECLARE ... CONDITION`/`EXIT HANDLER`, `BEGIN ATOMIC` with `catalogManaged`
tables, `CREATE OR REPLACE TEMPORARY VIEW` inside a procedure body, `EXECUTE IMMEDIATE ... USING` positional
binding, and calling a scalar SQL UDF inside a `MERGE` source on DBSQL; the Postgres variant on Lakebase.
