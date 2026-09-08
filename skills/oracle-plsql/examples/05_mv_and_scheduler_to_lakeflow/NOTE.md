# 05 — Fast-refresh materialized view + `DBMS_SCHEDULER` job -> Lakeflow Pipelines MV + Lakeflow Jobs

**Source**: `fixture/12_mv_policy_premium_summary.sql` (two MV logs `WITH ROWID, SEQUENCE ... INCLUDING NEW VALUES`,
aggregate MV `REFRESH FAST ON DEMAND ENABLE QUERY REWRITE`) and `fixture/13_job_nightly_renewal.sql`
(`CREATE_PROGRAM` PL/SQL block calling the renewal package then `DBMS_MVIEW.REFRESH`, `CREATE_JOB` with an
`Europe/London` calendar string, `max_failures`, `max_run_duration`, `restartable`, e-mail notification).

**Profile / track**: analytical. MV -> Lakeflow Declarative Pipelines; job -> Lakeflow Jobs (DABs YAML embedded as a
comment block so the file stays a single `.sql`).

## Constructs exercised (SKILL.md refs)

| Oracle | Converted | Rule |
|---|---|---|
| `CREATE MATERIALIZED VIEW LOG` x2 | none (absorbed: Delta row tracking replaces MV logs) | §6 MV row; §7 trap 15 |
| `REFRESH FAST ON DEMAND` aggregate MV with `COUNT(*)`/`COUNT(col)` guards | `CREATE OR REFRESH MATERIALIZED VIEW` with `CLUSTER BY`, expectation | §6 MV row |
| `TRUNC(effective_dt,'MM')` group key | `date_trunc('MONTH', ...)::DATE` | §5 #61; §7 trap 3 |
| `SUM(NUMBER)` -> `NUMBER` | `DECIMAL(38,10)` declared | §7 trap 2, trap 25 |
| `SUM` over all-NULL group = NULL | pipeline MV returns 0 | §7 trap 25 (decision) |
| `ENABLE QUERY REWRITE` | none (no rewrite on Databricks; consumers query the MV directly) | §6 MV row, GAP |
| `repeat_interval` calendar string + `start_date` TZ | `quartz_cron_expression` + `timezone_id: Europe/London` | §6 scheduler row; §7 trap 16 |
| program = two PL/SQL steps | two tasks with `depends_on` + `run_if: ALL_SUCCESS` | §6 scheduler row |
| `max_failures 3`, `restartable TRUE` | task `max_retries: 3` | §7 trap 16 |
| `max_run_duration 2h`, `JOB_OVER_MAX_DUR` | `timeout_seconds: 7200` + `RUN_DURATION_SECONDS` health rule | §7 trap 16 |
| `ADD_JOB_EMAIL_NOTIFICATION` | `email_notifications.on_failure` (recipients from deployment vars) | §6 scheduler row |
| `DBMS_MVIEW.REFRESH(..., METHOD => 'F')` | `pipeline_task` with `full_refresh: false` | §6 MV row |
| `enabled => FALSE` then `ENABLE` | `pause_status: PAUSED` until STOP E | §11 R6 |

## Recon tier that catches a wrong conversion

- **Tier 2** on the MV (`SUM(amount_sum)`, `SUM(premium_sum)`, `SUM(row_cnt)` grouped by
  `product_cd, policy_status, effective_month`) with `decimal_round(10)`: catches a dropped join predicate, a
  `date_trunc('DAY')`/`'MONTH'` mix-up (month buckets split), and scale loss on `premium_sum`. Run with
  `null_missing_equiv` **off** for `amount_sum`/`tax_sum` so the `NULL` vs `0` all-null-group difference surfaces.
- **Tier 1** on the MV: group count. Wrong when `effective_month` keeps a time component (one group per distinct
  timestamp instead of per month).
- **Tier 4 (freshness)**: the Oracle job runs 02:40 Europe/London Mon-Sat. With `timezone_id` left at UTC the MV
  lands an hour early in BST and the "as of" comparison window (`06_decisions.md`) shows a day boundary shift on
  Monday-morning reads; the recon batch window check flags it before any consumer does.
- **Tier 1 on the renewal outputs** (`poladm.premium_txn` count of `'RN'` rows per run date): a job whose second
  task runs on `ALL_DONE` instead of `ALL_SUCCESS` refreshes the MV from a failed sweep; the count for that run
  date is zero on Oracle and non-zero on the target.

## Canonicalization used

`decimal_round` (all `*_sum` columns), `datetime_utc_truncate_ms` if `effective_month` is left as timestamp,
`null_missing_equiv` (selectively off, see above).

## Not verified live

Pipeline MV incremental refresh actually engaging (depends on row tracking on `policy`/`premium_txn`); the
`EXPECT ... ON VIOLATION FAIL UPDATE` clause on an MV; Jobs cron acceptance of `MON-SAT`; DABs field placement of
`max_retries` under a `sql_task`; e-mail delivery.
