# 03 — Package with cursor + `BULK COLLECT LIMIT` + `FORALL` -> set-based DBSQL procedure

**Track**: analytical DBSQL (SQL scripting first; the Jobs caller `SIGNAL`s on `status_out <> 'OK'`; no PySpark).
Lakebase variant summarised at the end of `converted.sql`.

| Oracle | Converted | SKILL.md |
|---|---|---|
| package spec/body, constants; `g_run_id` state | schema + literals; `run_log` table row | trap 11 |
| cursor `FOR UPDATE SKIP LOCKED` + `BULK COLLECT LIMIT` + `FORALL` | candidate `TEMPORARY TABLE` (drop-first; a temp *view* is re-executed on every access) + one `MERGE` + `INSERT ... SELECT`; snapshot isolation inside `BEGIN ATOMIC` replaces row locks | construct map |
| `BEFORE UPDATE` trigger firing on the `FORALL UPDATE` | `row_version`/`updated_*`/`active_policy_flag` folded into `UPDATE SET`; `policy_audit_log` `INSERT` for rows whose premium changed | trap 10, example 02 |
| `NO_DATA_FOUND -> default`, `NVL(commission_pct, 0)` | `coalesce((SELECT 1 + coalesce(pct, 0)/100 ...), 1.035)`: outer = no row (1.035), inner = NULL commission (1.0) | construct map |
| `TOO_MANY_ROWS -> RAISE_APPLICATION_ERROR` | duplicate-broker `IF EXISTS` + `SIGNAL` before any write (a scalar UDF cannot raise it) | construct map |
| `SAVEPOINT`/`ROLLBACK TO` per batch | whole-run `BEGIN ATOMIC` (decision: no partial batches) | construct map |
| `EXCEPTION WHEN <named>` / `WHEN OTHERS` swallow | `DECLARE ... CONDITION` + `DECLARE EXIT HANDLER FOR`; status still swallowed | trap 13 |
| `SQL%ROWCOUNT`; `OUT` params with `IN` defaults | `count(*)` of the candidate set; `OUT` without `DEFAULT` | construct map |
| `EXECUTE IMMEDIATE ... USING` + `DBMS_ASSERT` | `EXECUTE IMMEDIATE ... USING` behind a census allow-list | construct map |
| `NUMBER` arithmetic, `ROUND(x, 2)`; `ADD_MONTHS`; `d + n` | `DECIMAL(38,10)` then `round(.., 2)`; `add_months`; `+ make_interval(0,0,0,n)` | traps 2, 3 |
| `DBMS_OUTPUT.PUT_LINE`; `COMMIT` | dropped (`run_log` is the trace); implicit at `END` of the atomic block | construct map |

**Recon**: Tier 1 on `premium_txn` `'RN'` rows and `run_log` per run date (a `BETWEEN` that lost the time component
changes the count); Tier 2 `SUM(annual_premium)` per `product_cd` with `decimal_round(2)` (double uplift, `DECIMAL`
vs exact `NUMBER`, `add_months` month-end); Tier 3 keyed on `policy_no` (`row_version` not incremented, `expiry_dt`
off by a day, `active_policy_flag` stale; no-broker policy uplifted 1.035 vs NULL-commission 1.0); Tier 1 on
`policy_audit_log` `'UPDATE'` rows (missing fold-in = 0, unconditional logging over-counts uplift-of-1.0 rows). The
swallowed-exception contract is Tier 4: the caller must fail the run on `status_out <> 'OK'`.

**Canonicalization**: `decimal_round` (`annual_premium`, `amount`), `datetime_utc_truncate_ms` (`expiry_dt`, `txn_dt`),
`null_missing_equiv` (`broker_id`).

**Open decisions**: per-batch `SAVEPOINT` partial success not reproduced; `SKIP LOCKED` concurrency not reproduced
(`max_concurrent_runs: 1`); `archive_to` allow-list is census-derived; audit rows inside the atomic block are lost on
a failed run where Oracle's autonomous logger kept them (accepted difference, example 02).

**Not verified live**: `CREATE PROCEDURE` with `OUT` params, `DECLARE ... CONDITION`/`EXIT HANDLER`, `BEGIN ATOMIC`
with `catalogManaged` tables, temp-table `CREATE`/`UPDATE` inside a procedure body, `EXECUTE IMMEDIATE ... USING`
positional binding, scalar SQL UDF with a scalar subquery inside an `UPDATE`; the Postgres variant on Lakebase.
