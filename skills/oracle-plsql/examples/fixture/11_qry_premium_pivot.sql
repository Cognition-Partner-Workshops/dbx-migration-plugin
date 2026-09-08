-- Object class: VIEW (PIVOT). Census key: ODS.V_PREMIUM_BY_TXN_TYPE
-- Fixed IN list PIVOT with two aggregates (generated column names like NB_AMT, RN_CNT),
-- TRUNC(txn_dt,'MM') on a DATE that carries time, and NVL over the sparse cells.

CREATE OR REPLACE VIEW ods.v_premium_by_txn_type AS
SELECT policy_id,
       txn_month,
       NVL(nb_amt, 0)   AS nb_amt,   NVL(nb_cnt, 0)   AS nb_cnt,
       NVL(rn_amt, 0)   AS rn_amt,   NVL(rn_cnt, 0)   AS rn_cnt,
       NVL(mta_amt, 0)  AS mta_amt,  NVL(mta_cnt, 0)  AS mta_cnt,
       NVL(canc_amt, 0) AS canc_amt, NVL(canc_cnt, 0) AS canc_cnt
  FROM (
        SELECT t.policy_id,
               TRUNC(t.txn_dt, 'MM') AS txn_month,       -- DATE truncated to month start (still a DATE)
               t.txn_type_cd,
               t.amount
          FROM poladm.premium_txn t
         WHERE t.txn_type_cd <> 'REFD'
       )
 PIVOT (
        SUM(amount) AS amt,
        COUNT(*)    AS cnt
        FOR txn_type_cd IN ('NB' AS nb, 'RN' AS rn, 'MTA' AS mta, 'CANC' AS canc)
       );
