-- Converted under skills/tsql-ssis/SKILL.md §6 "SSIS Data Flow Task" rows (Source / Lookup /
-- Derived Column / OLE DB Destination / error output / OnError handler).
-- Lakeflow Spark Declarative Pipelines SQL, syntax per databricks-pipelines references:
--   temporary-view-sql.md   (CREATE TEMPORARY VIEW for shared intermediates)
--   materialized-view-sql.md (CREATE OR REFRESH MATERIALIZED VIEW, CLUSTER BY, CONSTRAINT ... EXPECT)
--   expectations-sql.md     (warn / DROP ROW / FAIL UPDATE)
-- Batch semantics chosen (databricks-pipelines SKILL.md "batch vs streaming": the source is a
-- daily-windowed full read, not an append stream). The package parameter ServicerId and variable
-- LoadDate become pipeline `configuration` keys read as ${key} in SQL (databricks-pipelines
-- references/pipeline-configuration.md "configuration", kafka.md "Pipeline Configuration"; SKILL §6 "SSIS Variables").
-- Runs as a pipeline_task inside the converted job graph (databricks-jobs task-types.md).

-- SRC payments (OLE DB Source, SQL command with two ? parameters bound to User::LoadDate)
CREATE TEMPORARY VIEW src_payments AS
SELECT p.payment_id, p.loan_id, p.payment_date, p.effective_date,
       p.principal_amt, p.interest_amt, p.escrow_amt, p.late_fee_amt,
       p.total_amt, p.payment_type, p.reversal_flag, p.batch_id
FROM ${catalog}.${schema}.payments p
WHERE p.payment_date >= CAST('${load_date}' AS TIMESTAMP_NTZ)
  AND p.payment_date <  CAST('${load_date}' AS TIMESTAMP_NTZ) + INTERVAL 1 DAY;   -- DATEADD(dd, 1, ?)

-- LKP loans (Lookup, Full cache, parameterised reference query, no-match -> redirect)
-- Full cache == one point-in-time read of the reference set == a plain LEFT JOIN here.
-- (Partial/No cache would have re-queried per row and seen mid-run changes: SKILL §7 "SSIS Lookup cache".)
-- Lookup joins are case-INsensitive on a CI database only when CacheType != Full; with Full cache SSIS
-- compares in .NET (case- and trailing-space-SENSITIVE), so no collation folding is added for this package.
CREATE TEMPORARY VIEW lkp_payments_loans AS
SELECT s.*,
       l.loan_type, l.investor_code, l.property_state,
       l.loan_id AS lkp_loan_id
FROM src_payments s
LEFT JOIN (SELECT loan_id, loan_type, servicer_id, investor_code, property_state
           FROM ${catalog}.${schema}.loans
           WHERE servicer_id = ${servicer_id}) l
  ON s.loan_id = l.loan_id;

-- DER payment attrs (Derived Column). SSIS expression -> SQL (SKILL §6 "SSIS Derived Column"):
--   YEAR(d) * 100 + MONTH(d)              -> year(d) * 100 + month(d)
--   flag == "Y" ? TRUE : FALSE            -> flag = 'Y'
--   a < 500 ? "LT500" : (...)             -> CASE
--   (DT_WSTR,4)TRIM(loan_type)            -> trim(loan_type)  (SSIS TRIM strips spaces only)
-- Lookup Match Output -> fact; Lookup No Match Output -> error table.
CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.${schema}.fact_payment (
    CONSTRAINT loan_exists  EXPECT (lkp_loan_id IS NOT NULL) ON VIOLATION DROP ROW,   -- rows go to err_payment_no_loan below
    CONSTRAINT amt_present  EXPECT (total_amt IS NOT NULL)                            -- FastLoadKeepNulls=false: DW default applied; warn only
)
CLUSTER BY (payment_month)
COMMENT 'Converted from LoadPaymentFact.dtsx / DFT Load fact_payment'
AS
SELECT payment_id,
       loan_id,
       year(payment_date) * 100 + month(payment_date)         AS payment_month,
       payment_date,
       trim(loan_type)                                        AS loan_type,       -- DER loan_type_trim
       investor_code,
       property_state,
       principal_amt, interest_amt, escrow_amt, late_fee_amt,
       coalesce(total_amt, CAST(0 AS DECIMAL(19,4)))          AS total_amt,       -- KeepNulls=false: DW column DEFAULT 0
       reversal_flag = 'Y'                                    AS is_reversal,
       CASE WHEN total_amt < 500  THEN 'LT500'
            WHEN total_amt < 2000 THEN '500-2K'
            ELSE 'GT2K' END                                   AS amt_bucket,
       batch_id,
       CAST('${load_date}' AS DATE)                           AS load_date,
       lkp_loan_id
FROM lkp_payments_loans;

-- DST err_payment_no_loan (Lookup No Match Output). SSIS wrote the unmatched rows to a second table;
-- expectations only drop or fail, they do not redirect, so the redirect is a second dataset over the
-- same view with the inverse predicate (SKILL §6 "error output redirection").
CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.${schema}.err_payment_no_loan
COMMENT 'Lookup No Match Output of LoadPaymentFact.dtsx'
AS
SELECT payment_id, loan_id, payment_date, total_amt, batch_id,
       'LKP loans: no match'            AS error_desc,
       CAST('${load_date}' AS DATE)     AS load_date
FROM lkp_payments_loans
WHERE lkp_loan_id IS NULL;

-- OnError event handler ("SQL Log OnError" -> dw.etl_log): pipeline events are already recorded in the
-- event log; the equivalent user-visible signal is the job's on_failure notification on the pipeline_task
-- (databricks-jobs notifications-monitoring.md). No SQL is emitted for it here.
