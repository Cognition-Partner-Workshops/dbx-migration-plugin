-- SKILL.md canonical Lakeflow shape for a parameter-driven SSIS Data Flow: a Jobs sql_task
-- bounded batch (converted.yml), not a streaming table. Job parameters arrive as :name;
-- the two connection managers become two namespace pairs (src_* read-only, tgt_* written).

-- SRC payments (? = User::LoadDate) + LKP loans (Full cache, ? = $Package::ServicerId):
-- the window and its Match / No Match verdict are read ONCE, so both destinations see the
-- same loans snapshot exactly as the Full-cache Lookup did.
DROP TABLE IF EXISTS lkp_window;
CREATE TEMP TABLE lkp_window AS
SELECT p.payment_id, p.loan_id, p.payment_date,
       p.principal_amt, p.interest_amt, p.escrow_amt, p.late_fee_amt, p.total_amt,
       p.reversal_flag, p.batch_id,
       l.loan_type, l.investor_code, l.property_state,
       l.loan_id IS NOT NULL      AS matched,
       CAST(:servicer_id AS INT)  AS servicer_id,
       CAST(:load_date AS DATE)   AS load_date
FROM IDENTIFIER(:src_catalog || '.' || :src_schema || '.payments') p
LEFT JOIN IDENTIFIER(:src_catalog || '.' || :src_schema || '.loans') l
  ON l.loan_id = p.loan_id
 AND l.servicer_id = CAST(:servicer_id AS INT)
WHERE p.payment_date >= CAST(:load_date AS TIMESTAMP_NTZ)
  AND p.payment_date <  CAST(date_add(CAST(:load_date AS DATE), 1) AS TIMESTAMP_NTZ);

-- DER payment attrs + DST fact_payment (fast load, append). Idempotent on payment_id: the
-- source appended the day twice on a rerun (recorded per-unit decision in NOTE.md).
-- FastLoadKeepNulls=false: a NULL total_amt took the DW column DEFAULT 0 -> coalesce.
MERGE INTO IDENTIFIER(:tgt_catalog || '.' || :tgt_schema || '.fact_payment') t
USING (
    SELECT w.payment_id, w.loan_id,
           year(w.payment_date) * 100 + month(w.payment_date) AS payment_month,  -- YEAR()*100+MONTH()
           w.payment_date,
           trim(w.loan_type)                                   AS loan_type,      -- (DT_WSTR,4)TRIM()
           w.investor_code, w.property_state,
           w.principal_amt, w.interest_amt, w.escrow_amt, w.late_fee_amt,
           coalesce(w.total_amt, CAST(0 AS DECIMAL(19,4)))     AS total_amt,
           w.reversal_flag = 'Y'                               AS is_reversal,    -- x == "Y" ? TRUE : FALSE
           CASE WHEN w.total_amt < 500  THEN 'LT500'                               -- nested ? :
                WHEN w.total_amt < 2000 THEN '500-2K'
                ELSE 'GT2K' END                                AS amt_bucket,
           w.batch_id, w.load_date
    FROM lkp_window w
    WHERE w.matched
) s
ON t.payment_id = s.payment_id
WHEN NOT MATCHED THEN INSERT
    (payment_id, loan_id, payment_month, payment_date, loan_type, investor_code, property_state,
     principal_amt, interest_amt, escrow_amt, late_fee_amt, total_amt, is_reversal, amt_bucket,
     batch_id, load_date)
VALUES
    (s.payment_id, s.loan_id, s.payment_month, s.payment_date, s.loan_type, s.investor_code, s.property_state,
     s.principal_amt, s.interest_amt, s.escrow_amt, s.late_fee_amt, s.total_amt, s.is_reversal, s.amt_bucket,
     s.batch_id, s.load_date);

-- DST err_payment_no_loan (Lookup No Match Output). servicer_id is an added column: "payment X
-- has no loan under servicer S" is one fact per servicer, one row per source execution.
MERGE INTO IDENTIFIER(:tgt_catalog || '.' || :tgt_schema || '.err_payment_no_loan') t
USING (
    SELECT w.payment_id, w.loan_id, w.payment_date, w.total_amt, w.batch_id, w.servicer_id,
           'LKP loans: no match' AS error_desc, w.load_date
    FROM lkp_window w
    WHERE NOT w.matched
) s
ON  t.payment_id  = s.payment_id
AND t.servicer_id = s.servicer_id
WHEN NOT MATCHED THEN INSERT
    (payment_id, loan_id, payment_date, total_amt, batch_id, servicer_id, error_desc, load_date)
VALUES
    (s.payment_id, s.loan_id, s.payment_date, s.total_amt, s.batch_id, s.servicer_id, s.error_desc, s.load_date);

DROP TABLE IF EXISTS lkp_window;
