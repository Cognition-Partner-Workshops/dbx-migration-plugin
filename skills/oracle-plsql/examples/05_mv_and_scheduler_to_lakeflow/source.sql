-- Object class: MATERIALIZED VIEW LOG + MATERIALIZED VIEW. Census key: ODS.MV_POLICY_PREMIUM_SUMMARY
-- Fast (incremental) refresh on demand, aggregate MV over a join. Fast-refresh eligibility
-- depends on the MV logs including ROWID, SEQUENCE and the aggregated columns, and on
-- COUNT(*) / COUNT(col) being present next to every SUM (Oracle rule for aggregate MVs).
-- Reads: POLADM.POLICY, POLADM.PREMIUM_TXN. Refreshed by JOB_NIGHTLY_RENEWAL.

CREATE MATERIALIZED VIEW LOG ON poladm.policy
  WITH ROWID, SEQUENCE (policy_id, product_cd, policy_status, annual_premium)
  INCLUDING NEW VALUES;

CREATE MATERIALIZED VIEW LOG ON poladm.premium_txn
  WITH ROWID, SEQUENCE (policy_id, txn_type_cd, effective_dt, amount, tax_amount)
  INCLUDING NEW VALUES;

CREATE MATERIALIZED VIEW ods.mv_policy_premium_summary
  BUILD IMMEDIATE
  REFRESH FAST ON DEMAND
  ENABLE QUERY REWRITE
AS
SELECT p.product_cd,
       p.policy_status,
       TRUNC(t.effective_dt, 'MM')      AS effective_month,
       COUNT(*)                         AS row_cnt,
       COUNT(t.amount)                  AS amount_cnt,
       SUM(t.amount)                    AS amount_sum,        -- NUMBER(12,2) input, NUMBER output
       SUM(t.tax_amount)                AS tax_sum,
       SUM(p.annual_premium)            AS premium_sum,       -- NUMBER without scale input
       COUNT(p.annual_premium)          AS premium_cnt
  FROM poladm.policy p
  JOIN poladm.premium_txn t ON t.policy_id = p.policy_id
 GROUP BY p.product_cd, p.policy_status, TRUNC(t.effective_dt, 'MM');

-- Refresh call used by the scheduler job (read here for lineage only; it is not run by the factory)
-- EXEC DBMS_MVIEW.REFRESH('ODS.MV_POLICY_PREMIUM_SUMMARY', METHOD => 'F', ATOMIC_REFRESH => TRUE);

-- Object class: SCHEDULER JOB (+ PROGRAM). Census key: POLADM.JOB_NIGHTLY_RENEWAL
-- DBMS_SCHEDULER job that runs the renewal package then refreshes the ODS MV. Calendar string
-- is in the database time zone (Europe/London here), retries via max_failures, and a
-- job-completed email notification. The factory only READS this definition (DBA_SCHEDULER_JOBS);
-- it never creates, enables, or runs it.

BEGIN
  DBMS_SCHEDULER.CREATE_PROGRAM(
    program_name        => 'POLADM.PRG_NIGHTLY_RENEWAL',
    program_type        => 'PLSQL_BLOCK',
    program_action      => q'[
      DECLARE
        l_rows   NUMBER;
        l_status VARCHAR2(200);
      BEGIN
        poladm.pkg_policy_renewal.renew_expiring(
          p_as_of_dt => TRUNC(SYSDATE), p_horizon_days => 30,
          p_rows_out => l_rows, p_status_out => l_status);
        IF l_status <> 'OK' THEN
          RAISE_APPLICATION_ERROR(-20010, 'Renewal failed: ' || l_status);
        END IF;
        DBMS_MVIEW.REFRESH('ODS.MV_POLICY_PREMIUM_SUMMARY', METHOD => 'F', ATOMIC_REFRESH => TRUE);
      END;]',
    enabled             => TRUE,
    comments            => 'Nightly renewal sweep + ODS summary refresh');

  DBMS_SCHEDULER.CREATE_JOB(
    job_name            => 'POLADM.JOB_NIGHTLY_RENEWAL',
    program_name        => 'POLADM.PRG_NIGHTLY_RENEWAL',
    start_date          => TO_TIMESTAMP_TZ('2019-04-01 02:40:00 Europe/London', 'YYYY-MM-DD HH24:MI:SS TZR'),
    repeat_interval     => 'FREQ=DAILY; BYHOUR=2; BYMINUTE=40; BYSECOND=0; BYDAY=MON,TUE,WED,THU,FRI,SAT',
    job_class           => 'DEFAULT_JOB_CLASS',
    enabled             => FALSE,
    auto_drop           => FALSE,
    comments            => 'Weeknight + Saturday renewal sweep; Sunday reserved for GoldenGate resync');

  DBMS_SCHEDULER.SET_ATTRIBUTE('POLADM.JOB_NIGHTLY_RENEWAL', 'max_failures',     3);
  DBMS_SCHEDULER.SET_ATTRIBUTE('POLADM.JOB_NIGHTLY_RENEWAL', 'max_run_duration', INTERVAL '0 02:00:00' DAY TO SECOND);
  DBMS_SCHEDULER.SET_ATTRIBUTE('POLADM.JOB_NIGHTLY_RENEWAL', 'restartable',      TRUE);
  DBMS_SCHEDULER.SET_ATTRIBUTE('POLADM.JOB_NIGHTLY_RENEWAL', 'logging_level',    DBMS_SCHEDULER.LOGGING_FULL);

  DBMS_SCHEDULER.ADD_JOB_EMAIL_NOTIFICATION(
    job_name   => 'POLADM.JOB_NIGHTLY_RENEWAL',
    recipients => 'ops-alias@example.invalid',
    events     => 'JOB_FAILED, JOB_OVER_MAX_DUR, JOB_SCH_LIM_REACHED');

  DBMS_SCHEDULER.ENABLE('POLADM.JOB_NIGHTLY_RENEWAL');
END;
/
