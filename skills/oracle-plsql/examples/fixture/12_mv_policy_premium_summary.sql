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
