-- Converted: fixture 12_mv_policy_premium_summary.sql + 13_job_nightly_renewal.sql
--   -> Lakeflow Declarative Pipelines materialized view + Lakeflow Jobs orchestration (DABs YAML, in the comment block).
-- Track: analytical. Rules: SKILL.md §6 MV row, scheduler row; §7 traps 15, 16, 25; §11 R6.
-- Routing (target-routing): MV definition -> [pipelines:references/materialized-view-sql.md]; the DBMS_SCHEDULER job
-- (call package, then refresh MV, 02:40 Europe/London Mon-Sat, 3 failures, 2h max duration, failure e-mail) ->
-- [jobs:SKILL.md#Multi-Task Workflows] + [jobs:references/triggers-schedules.md#Cron Schedule] +
-- [jobs:references/notifications-monitoring.md].

-- ---------- 1. materialized view (pipeline SQL file: src/mv_policy_premium_summary.sql) ----------
-- Oracle: REFRESH FAST ON DEMAND over two MV logs (WITH ROWID, SEQUENCE ... INCLUDING NEW VALUES), COUNT(*) and
-- COUNT(col) next to every SUM so the aggregate MV is fast-refreshable (§7 trap 15).
-- Databricks: the MV is refreshed incrementally automatically when the sources are Delta with row tracking enabled
-- [pipelines:references/materialized-view-sql.md#Incremental refresh]; the MV logs have no counterpart and are
-- dropped from the target inventory (recorded as "absorbed" in the census, not as a gap). The COUNT(col) helper
-- columns are kept because downstream consumers may read them (Tier 4), not because the target needs them.
CREATE OR REFRESH MATERIALIZED VIEW ods.mv_policy_premium_summary (
  product_cd        STRING         NOT NULL,
  policy_status     STRING         NOT NULL,
  effective_month   DATE           NOT NULL COMMENT 'TRUNC(effective_dt, ''MM'') in Oracle; DATE here (Oracle returned a DATE at 00:00:00)',
  row_cnt           BIGINT,
  amount_cnt        BIGINT,
  amount_sum        DECIMAL(38,10) COMMENT 'Oracle SUM(NUMBER(12,2)) -> NUMBER; DECIMAL(38,10) here (§7 trap 25)',
  tax_sum           DECIMAL(38,10),
  premium_sum       DECIMAL(38,10) COMMENT 'SUM over NUMBER without scale (§7 trap 2)',
  premium_cnt       BIGINT,
  CONSTRAINT month_present EXPECT (effective_month IS NOT NULL) ON VIOLATION FAIL UPDATE
)
CLUSTER BY (product_cd, effective_month)
COMMENT 'Oracle ODS.MV_POLICY_PREMIUM_SUMMARY (REFRESH FAST ON DEMAND, refreshed by JOB_NIGHTLY_RENEWAL)'
AS
SELECT p.product_cd,
       p.policy_status,
       date_trunc('MONTH', t.effective_dt)::DATE      AS effective_month,   -- §5 #61
       count(*)                                        AS row_cnt,
       count(t.amount)                                 AS amount_cnt,
       sum(t.amount)                                   AS amount_sum,
       sum(t.tax_amount)                               AS tax_sum,
       sum(p.annual_premium)                           AS premium_sum,
       count(p.annual_premium)                         AS premium_cnt
  FROM ${catalog}.poladm.policy p
  JOIN ${catalog}.poladm.premium_txn t ON t.policy_id = p.policy_id
 GROUP BY p.product_cd, p.policy_status, date_trunc('MONTH', t.effective_dt)::DATE;
-- Pipeline MV SUM over an all-NULL group returns 0, Oracle returns NULL
-- [pipelines:references/materialized-view-sql.md#Syntax]: Tier 2 compares with null_missing_equiv OFF for
-- amount_sum/tax_sum so the difference is visible, and the decision (accept 0 / wrap in NULLIF(amount_cnt,0)) goes to
-- 06_decisions.md.

-- ---------- 2. scheduler job -> Lakeflow Jobs (resources/job_nightly_renewal.yml) ----------
-- Read from DBA_SCHEDULER_JOBS / DBA_SCHEDULER_PROGRAMS / DBA_SCHEDULER_NOTIFICATIONS only; never enabled/run at source.
--
-- resources:
--   jobs:
--     poladm_job_nightly_renewal:
--       name: "[${bundle.target}] POLADM.JOB_NIGHTLY_RENEWAL"
--       description: "Nightly renewal sweep + ODS summary refresh (Oracle DBMS_SCHEDULER job, converted)"
--       # repeat_interval 'FREQ=DAILY; BYHOUR=2; BYMINUTE=40; BYSECOND=0; BYDAY=MON,TUE,WED,THU,FRI,SAT'
--       # start_date TZ = Europe/London  -> timezone_id must be the Oracle calendar TZ, NOT the workspace default (§7 trap 16)
--       schedule:
--         quartz_cron_expression: "0 40 2 ? * MON-SAT"
--         timezone_id: "Europe/London"
--         pause_status: PAUSED                     # Oracle job is created enabled=FALSE then ENABLEd; deploy paused, enable at STOP E
--       max_concurrent_runs: 1                     # DBMS_SCHEDULER never overlaps runs of one job
--       # max_run_duration INTERVAL '0 02:00:00' -> timeout_seconds; JOB_OVER_MAX_DUR e-mail -> health rule
--       timeout_seconds: 7200
--       health:
--         rules:
--           - metric: RUN_DURATION_SECONDS
--             op: GREATER_THAN
--             value: 7200
--       email_notifications:                       # ADD_JOB_EMAIL_NOTIFICATION(JOB_FAILED, JOB_OVER_MAX_DUR, ...)
--         on_failure:
--           - ${var.ops_alias}                     # recipient list is deployment config, not copied from the source
--         on_duration_warning_threshold_exceeded:
--           - ${var.ops_alias}
--       tasks:
--         # PRG_NIGHTLY_RENEWAL step 1: pkg_policy_renewal.renew_expiring(...) with status check -> RAISE_APPLICATION_ERROR
--         - task_key: renew_expiring
--           sql_task:
--             warehouse_id: ${var.warehouse_id}
--             file:
--               path: ../src/sql/call_renew_expiring.sql   # see examples/07: CALL ... ; IF status <> 'OK' THEN SIGNAL ...
--               source: WORKSPACE
--           # restartable TRUE -> Oracle restarts a failed run with back-off and counts the exhausted restarts as ONE
--           # failure; the nearest field is a task retry. Retries are per run and the count resets every run
--           # (#Retry Behavior), so this is NOT max_failures. Keep the retry count at 1 (renew_expiring is idempotent
--           # through the MERGE in example 07); the exact Oracle restart count is a live-verify item.
--           max_retries: 1
--           min_retry_interval_millis: 300000
--           retry_on_timeout: false
--         # PRG_NIGHTLY_RENEWAL step 2: DBMS_MVIEW.REFRESH('ODS.MV_POLICY_PREMIUM_SUMMARY', METHOD => 'F')
--         - task_key: refresh_mv_policy_premium_summary
--           depends_on:
--             - task_key: renew_expiring
--           run_if: ALL_SUCCESS                    # Oracle: refresh only reached if step 1 did not raise
--           pipeline_task:
--             pipeline_id: ${resources.pipelines.ods_summary_pipeline.id}
--             full_refresh: false                  # METHOD => 'F' (fast); a 'C' complete refresh maps to full_refresh: true
--
-- GAP: max_failures 3 (§7 trap 16). Oracle counts consecutive *failed scheduled runs* and sets the job to
-- state = 'BROKEN' after 3, so no fourth night runs until a DBA re-enables it. Lakeflow has no job-level failure
-- counter and never pauses a schedule on its own; max_retries above is orthogonal (attempts inside one run).
-- Mitigation chosen for this unit (recorded in 06_decisions.md):
--   1. email_notifications.on_failure above + the ops runbook step "after 3 consecutive failed runs set
--      schedule.pause_status: PAUSED" [jobs:references/triggers-schedules.md#Pause and Resume]; and
--   2. an optional guard task, first in the chain, that reads the last 3 runs of this job
--      (`databricks jobs get-run`, [jobs:SKILL.md#Common Operations]) and, if all failed, pauses the schedule via
--      `databricks jobs update <job_id> --json '{"new_settings":{"schedule":{"pause_status":"PAUSED"}}}'` and
--      fails the run, so behaviour matches Oracle (no further work is attempted). Not implemented here: it needs the
--      job's own id at run time and a principal allowed to update the job, both deployment facts, not conversion facts.
-- Until one of these is live the target keeps scheduling after Oracle would have stopped (Tier 4 outcome drift).
--
-- Not mapped (recorded as GAP in the census, §10): job_class DEFAULT_JOB_CLASS (resource-consumer group),
-- logging_level LOGGING_FULL (Jobs run history is always on), ATOMIC_REFRESH => TRUE (a pipeline update is atomic per
-- MV; no cross-MV atomicity to emulate here because there is a single MV), JOB_SCH_LIM_REACHED event.
