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
-- Execution model: a PARAMETERISED BOUNDED BATCH, the same as the package. One SSIS execution =
-- (User::LoadDate window, $Package::ServicerId): it re-read the whole window from dbo.payments and evaluated
-- every row of it against the loans of THAT servicer. Both parameters are per execution (ServicerId is
-- Required="True"; LoadDate is bound to the two ? markers), so the conversion is a job with two job parameters
-- (:load_date, :servicer_id) and two set-based, idempotent MERGE statements over the same window.
--
-- Why NOT a Lakeflow Spark Declarative Pipelines streaming table (the previous shape of this file): a streaming
-- table checkpoints the payments rows it has consumed once, globally for the table. A per-run filter parameter
-- cannot be applied retroactively: payments consumed under servicer 7 are never re-evaluated when the next run
-- passes servicer 12, so that run's matches are lost and the first run's "no match" rows for servicer 12's
-- payments stay wrong forever. The source re-evaluated the window on every execution; only a bounded batch
-- reproduces that. A pipeline `configuration` key has the opposite defect (frozen at deploy time). SKILL §6
-- "SSIS Data Flow" row: a Data Flow driven by a per-execution parameter is a bounded batch (sql_task + MERGE),
-- never a streaming table; the streaming-table shape is for flows whose only variable is their own bookmark.
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
-- Snapshot per statement: each MERGE reads payments and loans once, as of its own Delta snapshot
-- (docs /optimizations/isolation/isolation-levels: readers see a consistent snapshot); that is the SSIS Full
-- cache Lookup (one point-in-time load of the reference set per execution). converted.yml sets
-- max_concurrent_runs: 1 (an SSIS package is not re-entrant) so two runs never race on the same window; a
-- concurrent writer to fact_payment / err_payment_no_loan outside the job makes the MERGE fail at commit
-- (docs /optimizations/isolation/row-level-concurrency: MERGE + MERGE "can conflict") and the job retries it.
-- Failure between the two statements leaves the fact loaded and the error rows missing, exactly the
-- non-transactional state a mid-flow SSIS failure left (TransactionOption=Supported, no enclosing transaction
-- in the package); the rerun repairs it because both statements are idempotent.
--
-- The OnError event handler's dw.etl_log write lives in converted.yml as the sql_task log_onerror
-- (run_if: AT_LEAST_ONE_FAILED after this task).

-- SRC payments (OLE DB Source, SQL command, ? = User::LoadDate twice) + LKP loans (Full cache, reference query
-- on ? = $Package::ServicerId, Lookup Match Output) + DER payment attrs + DST fact_payment (fast load, append).
-- SSIS expression -> SQL (SKILL §6 "SSIS Derived Column"):
--   YEAR(d) * 100 + MONTH(d)              -> year(d) * 100 + month(d)
--   flag == "Y" ? TRUE : FALSE            -> flag = 'Y'
--   a < 500 ? "LT500" : (...)             -> CASE
--   (DT_WSTR,4)TRIM(loan_type)            -> trim(loan_type)  (SSIS TRIM strips spaces only)
-- Lookup joins are case-INsensitive on a CI database only when CacheType != Full; with Full cache SSIS compares
-- in .NET (case- and trailing-space-SENSITIVE), so no collation folding is added for this package (SKILL §7).
-- FastLoadKeepNulls=false: a NULL total_amt took the DW column DEFAULT (0) -> coalesce.
MERGE INTO IDENTIFIER(:catalog || '.' || :schema || '.fact_payment') t
USING (
    SELECT p.payment_id,
           p.loan_id,
           year(p.payment_date) * 100 + month(p.payment_date)   AS payment_month,
           p.payment_date,
           trim(l.loan_type)                                     AS loan_type,       -- DER loan_type_trim
           l.investor_code,
           l.property_state,
           p.principal_amt, p.interest_amt, p.escrow_amt, p.late_fee_amt,
           coalesce(p.total_amt, CAST(0 AS DECIMAL(19,4)))       AS total_amt,
           p.reversal_flag = 'Y'                                 AS is_reversal,
           CASE WHEN p.total_amt < 500  THEN 'LT500'
                WHEN p.total_amt < 2000 THEN '500-2K'
                ELSE 'GT2K' END                                  AS amt_bucket,
           p.batch_id,
           CAST(:load_date AS DATE)                              AS load_date        -- == User::LoadDate
    FROM IDENTIFIER(:catalog || '.' || :schema || '.payments') p
    JOIN IDENTIFIER(:catalog || '.' || :schema || '.loans') l                          -- Lookup Match Output
      ON l.loan_id = p.loan_id
     AND l.servicer_id = CAST(:servicer_id AS INT)                                     -- reference query WHERE servicer_id = ?
    WHERE p.payment_date >= CAST(:load_date AS TIMESTAMP_NTZ)                         -- p.payment_date >= ?
      AND p.payment_date <  CAST(date_add(CAST(:load_date AS DATE), 1) AS TIMESTAMP_NTZ)   -- < DATEADD(dd, 1, ?)
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

-- DST err_payment_no_loan (Lookup No Match Output): the same window, the rows with no loan under this
-- servicer. Same snapshot semantics as above; the two statements partition the window exactly the way the
-- Lookup's two outputs did (a payment is either matched or not, for this servicer).
MERGE INTO IDENTIFIER(:catalog || '.' || :schema || '.err_payment_no_loan') t
USING (
    SELECT p.payment_id, p.loan_id, p.payment_date, p.total_amt, p.batch_id,
           CAST(:servicer_id AS INT)        AS servicer_id,        -- added column: the run's $Package::ServicerId
           'LKP loans: no match'            AS error_desc,
           CAST(:load_date AS DATE)         AS load_date
    FROM IDENTIFIER(:catalog || '.' || :schema || '.payments') p
    WHERE p.payment_date >= CAST(:load_date AS TIMESTAMP_NTZ)
      AND p.payment_date <  CAST(date_add(CAST(:load_date AS DATE), 1) AS TIMESTAMP_NTZ)
      AND NOT EXISTS (SELECT 1
                      FROM IDENTIFIER(:catalog || '.' || :schema || '.loans') l
                      WHERE l.loan_id = p.loan_id
                        AND l.servicer_id = CAST(:servicer_id AS INT))
) s
ON  t.payment_id  = s.payment_id
AND t.servicer_id = s.servicer_id
WHEN NOT MATCHED THEN INSERT
    (payment_id, loan_id, payment_date, total_amt, batch_id, servicer_id, error_desc, load_date)
VALUES
    (s.payment_id, s.loan_id, s.payment_date, s.total_amt, s.batch_id, s.servicer_id, s.error_desc, s.load_date);
