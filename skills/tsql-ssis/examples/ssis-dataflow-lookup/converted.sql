-- Converted under skills/tsql-ssis/SKILL.md §6 "SSIS Data Flow Task" rows (Source / Lookup /
-- Derived Column / OLE DB Destination / error output / OnError handler).
-- Lakeflow Spark Declarative Pipelines SQL, syntax per databricks-pipelines references:
--   temporary-view-sql.md   (CREATE TEMPORARY VIEW over STREAM(...); downstream STs read FROM STREAM(view_name))
--   streaming-table-sql.md  (CREATE OR REFRESH STREAMING TABLE, STREAM(table) source, stream-static join,
--                            CLUSTER BY, CONSTRAINT ... EXPECT, read_stream(..., skipChangeCommits => true))
--   expectations-sql.md     (warn / DROP ROW / FAIL UPDATE)
--
-- Target form: STREAMING TABLE, not MATERIALIZED VIEW. The package is an APPEND package: OLE DB Destination
-- fast load with no TRUNCATE / DELETE step, run once per LoadDate, so dw.fact_payment accumulates one day
-- per run and keeps every earlier day. A materialized view over a single ${load_date} window would be
-- recomputed for the new day on every refresh and REPLACE the previous day's rows (materialized-view-sql.md:
-- "batch processing with full refresh or incremental computation" of the defining query, and the defining
-- query only ever covers one window). databricks-pipelines SKILL.md "Streaming Table":
-- "incremental processing, exactly-once, append-only" is the SSIS destination's contract, so the SKILL §6
-- decision table (append/incremental -> ST) applies.
--
-- Idempotency key: the streaming checkpoint. Each payments row is processed exactly once, so re-triggering
-- the pipeline for the same day appends nothing (the source package had no key: re-running it for the same
-- LoadDate appended the day twice; that duplicate is a source defect, recorded as a per-unit decision in
-- NOTE.md, not reproduced). Re-loading one historical day on purpose is a full refresh of the table (the
-- streaming table rebuilds from the whole payments history), never a partial re-run.
--
-- The package parameter ServicerId becomes a pipeline `configuration` key read as ${servicer_id}
-- (databricks-pipelines references/pipeline-configuration.md "configuration"; SKILL §6 "SSIS Variables").
-- User::LoadDate has no configuration key any more: the daily window was the package's own incremental
-- bookmark, and the checkpoint replaces it; load_date is derived per row below.
-- Runs as a pipeline_task inside converted.yml (databricks-jobs task-types.md), whose failure task keeps
-- the OnError handler's dw.etl_log write.

-- SRC payments (OLE DB Source, SQL command with two ? parameters bound to User::LoadDate).
-- Incremental read of the same column list. payments is insert-only in the fixture (no UPDATE or DELETE
-- against dbo.payments anywhere in schema/, stored_procs/, triggers/ or batch/; reversal_flag is set at
-- insert time), so a plain STREAM() read is valid. If the real estate updates payment rows, read
--   FROM STREAM read_stream('${catalog}.${schema}.payments', skipChangeCommits => true)
-- instead (streaming-table-sql.md): the SSIS window only ever saw a row's state on its load day too.
CREATE TEMPORARY VIEW src_payments AS
SELECT p.payment_id, p.loan_id, p.payment_date, p.effective_date,
       p.principal_amt, p.interest_amt, p.escrow_amt, p.late_fee_amt,
       p.total_amt, p.payment_type, p.reversal_flag, p.batch_id
FROM STREAM(${catalog}.${schema}.payments) p;

-- LKP loans (Lookup, Full cache, parameterised reference query, no-match -> redirect)
-- Full cache == one point-in-time read of the reference set per package execution == a stream-static join:
-- loans is read as a static snapshot at stream start, payments incrementally (streaming-table-sql.md
-- "Stream-static join"). One triggered pipeline update per day == one SSIS execution == one snapshot.
-- (Partial/No cache would have re-queried per row and seen mid-run changes: SKILL §7 "SSIS Lookup cache".)
-- Lookup joins are case-INsensitive on a CI database only when CacheType != Full; with Full cache SSIS
-- compares in .NET (case- and trailing-space-SENSITIVE), so no collation folding is added for this package.
CREATE TEMPORARY VIEW lkp_payments_loans AS
SELECT s.*,
       l.loan_type, l.investor_code, l.property_state,
       l.loan_id AS lkp_loan_id
FROM STREAM(src_payments) s
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
-- DST fact_payment (OLE DB Destination, fast load, append). Persistent streaming table: every processed
-- day stays; each pipeline update appends only the payments rows not yet seen by the checkpoint.
CREATE OR REFRESH STREAMING TABLE ${catalog}.${schema}.fact_payment (
    CONSTRAINT loan_exists  EXPECT (lkp_loan_id IS NOT NULL) ON VIOLATION DROP ROW,   -- rows go to err_payment_no_loan below
    CONSTRAINT amt_present  EXPECT (total_amt IS NOT NULL)                            -- FastLoadKeepNulls=false: DW default applied; warn only
)
CLUSTER BY (payment_month)
COMMENT 'Converted from LoadPaymentFact.dtsx / DFT Load fact_payment (append destination; history retained)'
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
       CAST(payment_date AS DATE)                             AS load_date,       -- == User::LoadDate for every row the source window admitted
       lkp_loan_id
FROM STREAM(lkp_payments_loans);

-- DST err_payment_no_loan (Lookup No Match Output). SSIS wrote the unmatched rows to a second table;
-- expectations only drop or fail, they do not redirect, so the redirect is a second streaming table over the
-- same streaming view with the inverse predicate (SKILL §6 "error output redirection"). It is persistent
-- too: the source error table was also append-only, and its history is what the ops team reconciles.
-- Each streaming table keeps its own checkpoint over lkp_payments_loans, so the two outputs partition every
-- processed row exactly once between them.
CREATE OR REFRESH STREAMING TABLE ${catalog}.${schema}.err_payment_no_loan
COMMENT 'Lookup No Match Output of LoadPaymentFact.dtsx (append destination; history retained)'
AS
SELECT payment_id, loan_id, payment_date, total_amt, batch_id,
       'LKP loans: no match'            AS error_desc,
       CAST(payment_date AS DATE)       AS load_date
FROM STREAM(lkp_payments_loans)
WHERE lkp_loan_id IS NULL;

-- OnError event handler ("SQL Log OnError" -> dw.etl_log): a package-scope table write, part of the
-- customer's data contract (ops reads dw.etl_log). It is not emitted here because a pipeline update
-- cannot run SQL after its own failure; it lives in converted.yml as the sql_task log_onerror
-- (run_if: AT_LEAST_ONE_FAILED after the pipeline_task), which inserts the etl_log row:
--   INSERT INTO IDENTIFIER(:catalog||'.'||:schema||'.etl_log') (package_name, event, message, logged_at)
--   VALUES ('LoadPaymentFact', 'OnError', :message, current_timestamp());
-- The pipeline event log and the job's on_failure notification are additional signals, not replacements.
