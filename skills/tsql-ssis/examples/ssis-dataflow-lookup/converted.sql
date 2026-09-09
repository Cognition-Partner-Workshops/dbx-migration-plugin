-- Converted under skills/tsql-ssis/SKILL.md §6 "SSIS Data Flow Task" rows (Source / Lookup /
-- Derived Column / OLE DB Destination / error output / OnError handler) and "SSIS Variables".
-- Runs as the Lakeflow Jobs sql_task `load_payment_fact` in converted.yml (databricks-jobs
-- references/task-types.md "SQL Task": sql_task.file + warehouse_id; docs /aws/en/jobs/sql: "The file can
-- contain multiple SQL statements separated by semicolons"). Job parameters are pushed down to SQL tasks
-- and read with the :name syntax (docs /aws/en/jobs/parameter-use "SQL: Use named parameters to retrieve parameter
-- values"; converted.yml also passes them explicitly as sql_task.parameters); table names are built with IDENTIFIER()
-- (docs sql-ref-names-identifier-clause: "The target table name of a MERGE ... A column, table or view
-- referenced in a query").
--
-- Two namespaces, as in the package's two connection managers (SKILL §6 "SSIS Connection Managers"):
--   OLTP_LoanServicing (LOANSQL01 / loan_servicing, dbo)  -> :src_catalog.:src_schema   (payments, loans: READ ONLY)
--   DW_LoanMart        (LOANDW01  / loan_mart, dw)        -> :tgt_catalog.:tgt_schema   (fact_payment, err_payment_no_loan, etl_log)
-- Every payments/loans reference uses the src pair, every MERGE target the tgt pair; a single catalog/schema
-- pair would read the fact's own namespace for its source and either fail or read unrelated tables.
--
-- Execution model: a PARAMETERISED BOUNDED BATCH, the same as the package. One SSIS execution =
-- (User::LoadDate window, $Package::ServicerId): it re-read the whole window from dbo.payments and evaluated
-- every row of it against the loans of THAT servicer. Both parameters are per execution (ServicerId is
-- Required="True"; LoadDate is bound to the two ? markers), so the conversion is a job with two job parameters
-- (:load_date, :servicer_id), one materialisation of the window with its lookup verdict, and two set-based,
-- idempotent MERGE statements fed from that materialisation.
--
-- Why NOT a Lakeflow Spark Declarative Pipelines streaming table (an earlier shape of this file): a streaming
-- table checkpoints the payments rows it has consumed once, globally for the table. A per-run filter parameter
-- cannot be applied retroactively: payments consumed under servicer 7 are never re-evaluated when the next run
-- passes servicer 12, so that run's matches are lost and the first run's "no match" rows for servicer 12's
-- payments stay wrong forever. The source re-evaluated the window on every execution; only a bounded batch
-- reproduces that. A pipeline `configuration` key has the opposite defect (frozen at deploy time). SKILL §6
-- "SSIS Data Flow" row: a Data Flow driven by a per-execution parameter is a bounded batch (sql_task + MERGE),
-- never a streaming table; the streaming-table shape is for flows whose only variable is their own bookmark.
--
-- ONE lookup snapshot for both outputs. The SSIS Lookup (Full cache) loaded the reference set once per
-- execution and split every row of the window into exactly one of Match / No Match against that one snapshot.
-- Two MERGEs that each read loans themselves read two snapshots: a loan inserted, deleted or re-serviced
-- between them puts a payment in both outputs or in neither. So the window is read ONCE, together with its
-- verdict, into a session-scoped temporary table (databricks-dbsql references/materialized-views-pipes.md
-- "Temporary Tables": session-scoped physical Delta tables, CTAS, no catalog privileges, dropped with the
-- session; `CREATE OR REPLACE TEMP TABLE` is not supported, so DROP first). One statement = one snapshot of
-- payments and one of loans (docs /optimizations/isolation/isolation-levels: readers see a consistent
-- snapshot); both MERGEs then partition that fixed result. A concurrent loans write while the batch runs
-- therefore has the same effect it had on SSIS: it is either in the cache or not, never half in.
-- Alternative for warehouses/tables that support it: both MERGEs inside one BEGIN ATOMIC ... END block
-- (docs /transactions: reads inside the block are repeatable) instead of the temp table; same verdict property.
--
-- Idempotency (the source had none: re-running a LoadDate appended the day twice, recorded in NOTE.md as a
-- per-unit decision, not reproduced): MERGE ... WHEN NOT MATCHED THEN INSERT on the destination's business key
-- (docs delta-merge-into). fact_payment is keyed on payment_id (a payment's loan belongs to exactly one servicer,
-- so exactly one (window, servicer) execution can ever match it); err_payment_no_loan is keyed on
-- (payment_id, servicer_id): "payment X has no loan under servicer S" is one fact per servicer, exactly what
-- the source wrote once per execution. servicer_id is an ADDED column on the error table (the source table had
-- no key at all), recorded in NOTE.md. A rerun of the same (load_date, servicer_id) therefore inserts nothing,
-- a run for another servicer over the same window adds its own matches and its own no-match rows, and a
-- deliberate reload of one day is the same statement again (never a full refresh of the table).
--
-- Window bounds: DATEADD(dd, 1, ?) -> date_add(d, 1) (docs functions/date_add: returns a DATE).
-- Concurrency: converted.yml sets max_concurrent_runs: 1 (an SSIS package is not re-entrant), which serialises
-- runs of THIS job only; it does not stop writers to loan_servicing (handled by the single snapshot above) or
-- to the dw tables. A concurrent writer to fact_payment / err_payment_no_loan outside the job makes a MERGE
-- fail at commit (docs /optimizations/isolation/row-level-concurrency: MERGE + MERGE "can conflict") and the
-- job retries the task, which rebuilds the temp table and re-runs both MERGEs (idempotent).
-- Failure between the two MERGEs leaves the fact loaded and the error rows missing, exactly the
-- non-transactional state a mid-flow SSIS failure left (TransactionOption=Supported, no enclosing transaction
-- in the package); the rerun repairs it because both statements are idempotent.
--
-- The OnError event handler's dw.etl_log write lives in converted.yml as the sql_task log_onerror
-- (run_if: AT_LEAST_ONE_FAILED after this task), into :tgt_catalog.:tgt_schema.etl_log.

-- SRC payments (OLE DB Source, SQL command, ? = User::LoadDate twice) + LKP loans (Full cache, reference query
-- on ? = $Package::ServicerId): the window with its match verdict, read once.
-- Lookup joins are case-INsensitive on a CI database only when CacheType != Full; with Full cache SSIS compares
-- in .NET (case- and trailing-space-SENSITIVE), so no collation folding is added for this package (SKILL §7).
-- loans.loan_id is the primary key, so the LEFT JOIN yields exactly one row per payment.
DROP TABLE IF EXISTS lkp_window;
CREATE TEMP TABLE lkp_window AS
SELECT p.payment_id,
       p.loan_id,
       p.payment_date,
       p.principal_amt, p.interest_amt, p.escrow_amt, p.late_fee_amt,
       p.total_amt,
       p.reversal_flag,
       p.batch_id,
       l.loan_type, l.investor_code, l.property_state,   -- NULL on the No Match rows
       l.loan_id IS NOT NULL                 AS matched,  -- Lookup Match Output / No Match Output
       CAST(:servicer_id AS INT)             AS servicer_id,
       CAST(:load_date AS DATE)              AS load_date  -- == User::LoadDate
FROM IDENTIFIER(:src_catalog || '.' || :src_schema || '.payments') p
LEFT JOIN IDENTIFIER(:src_catalog || '.' || :src_schema || '.loans') l          -- reference query: SELECT ... FROM dbo.loans
  ON l.loan_id = p.loan_id
 AND l.servicer_id = CAST(:servicer_id AS INT)                                  --   WHERE servicer_id = ?
WHERE p.payment_date >= CAST(:load_date AS TIMESTAMP_NTZ)                        -- p.payment_date >= ?
  AND p.payment_date <  CAST(date_add(CAST(:load_date AS DATE), 1) AS TIMESTAMP_NTZ);   -- < DATEADD(dd, 1, ?)

-- DER payment attrs + DST fact_payment (fast load, append) on the Match Output.
-- SSIS expression -> SQL (SKILL §6 "SSIS Derived Column"):
--   YEAR(d) * 100 + MONTH(d)              -> year(d) * 100 + month(d)
--   flag == "Y" ? TRUE : FALSE            -> flag = 'Y'
--   a < 500 ? "LT500" : (...)             -> CASE
--   (DT_WSTR,4)TRIM(loan_type)            -> trim(loan_type)  (SSIS TRIM strips spaces only)
-- FastLoadKeepNulls=false: a NULL total_amt took the DW column DEFAULT (0) -> coalesce.
MERGE INTO IDENTIFIER(:tgt_catalog || '.' || :tgt_schema || '.fact_payment') t
USING (
    SELECT w.payment_id,
           w.loan_id,
           year(w.payment_date) * 100 + month(w.payment_date)   AS payment_month,
           w.payment_date,
           trim(w.loan_type)                                     AS loan_type,       -- DER loan_type_trim
           w.investor_code,
           w.property_state,
           w.principal_amt, w.interest_amt, w.escrow_amt, w.late_fee_amt,
           coalesce(w.total_amt, CAST(0 AS DECIMAL(19,4)))       AS total_amt,
           w.reversal_flag = 'Y'                                 AS is_reversal,
           CASE WHEN w.total_amt < 500  THEN 'LT500'
                WHEN w.total_amt < 2000 THEN '500-2K'
                ELSE 'GT2K' END                                  AS amt_bucket,
           w.batch_id,
           w.load_date
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

-- DST err_payment_no_loan (Lookup No Match Output): the same materialised window, the rows with no loan under
-- this servicer as of the same snapshot. The two statements partition lkp_window exactly the way the Lookup's
-- two outputs did (a payment is either matched or not, for this servicer, decided once).
MERGE INTO IDENTIFIER(:tgt_catalog || '.' || :tgt_schema || '.err_payment_no_loan') t
USING (
    SELECT w.payment_id, w.loan_id, w.payment_date, w.total_amt, w.batch_id,
           w.servicer_id,                    -- added column: the run's $Package::ServicerId
           'LKP loans: no match'  AS error_desc,
           w.load_date
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
