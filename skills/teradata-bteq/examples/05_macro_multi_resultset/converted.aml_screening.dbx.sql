-- Target: a Teradata macro with parameters and three result sets. Databricks has no macro object; the cited procedure
-- syntax (databricks-dbsql references/sql-scripting.md "Stored Procedures") documents IN/OUT parameters and
-- DEFAULTs but not client-visible result sets, so the macro becomes:
--   (a) two new tables this unit owns: AML_SCREENING_RUN (one row per CALL) and AML_SCREENING_RESULT (the rows of all
--       three patterns, tagged RESULT_SET_NO + RUN_ID). Neither existed on the legacy estate -- the macro's output only
--       ever lived in the client's spool -- so their DDL is part of the conversion, not something to discover.
--   (b) a procedure that writes one run and *publishes* it by stamping COMPLETED_TS on its run row (single-row UPDATE,
--       atomic on Delta), then retires earlier published runs for the same date;
--   (c) one view per pattern for the consumers that read `EXEC AML_SCREENING(...)` output positionally; each view sees
--       only the latest *published* run per SCREENING_DATE, so a run in flight (or one that died) is invisible, which is
--       what the source's single-request macro gave the consumer (no partial result set is ever observable).
-- If the macro had been a single SELECT it would have become a SQL table-valued function instead (skill §6).
-- Parameter DEFAULTs: "Once a parameter has a DEFAULT, all subsequent parameters must also have defaults"; all three do.
-- Teradata `DEFAULT DATE` = today; the DEFAULT expression cannot be current_date() if the target rejects
-- non-constant defaults (Not verified live), so NULL is the sentinel and the body resolves it.

-- Run ledger. RUN_ID is uuid() (docs.databricks.com/aws/en/sql/language-manual/functions/uuid: STRING, canonical
-- 36-char form), so two same-date CALLs can never share an id even when they start in the same microsecond.
CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.AML_SCREENING_RUN (
    RUN_ID            STRING        NOT NULL,
    SCREENING_DATE    DATE          NOT NULL,
    LOOKBACK_DAYS     INT           NOT NULL,
    AMOUNT_THRESHOLD  DECIMAL(15,2) NOT NULL,
    STARTED_TS        TIMESTAMP     NOT NULL,
    COMPLETED_TS      TIMESTAMP                 -- NULL while in flight; set once, last, by the run itself
)
COMMENT 'One row per AML_SCREENING call; COMPLETED_TS IS NOT NULL = published';

-- Superset of the three result-set shapes. Columns a pattern does not produce stay NULL for its rows; the per-pattern
-- views below project only the columns that result set had, in the source's column order. Amount widths are left wide
-- enough for SUM/AVG of DECIMAL(15,2); the AVG scale difference to Teradata is a recon concern (NOTE.md), not a DDL one.
CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.AML_SCREENING_RESULT (
    RUN_ID             STRING        NOT NULL,
    SCREENING_DATE     DATE          NOT NULL,
    RESULT_SET_NO      INT           NOT NULL,   -- 1 STRUCTURING, 2 RAPID_MOVEMENT, 3 NEW_CUSTOMER_INTL (EXEC output order)
    PATTERN_TYPE       STRING        NOT NULL,
    SORT_ORDER         INT           NOT NULL,   -- the result set's ORDER BY, materialised
    CUSTOMER_ID        INT           NOT NULL,   -- DIM_CUSTOMER.CUSTOMER_ID INTEGER
    CUSTOMER_NAME      STRING        NOT NULL,
    ACCOUNT_ID         STRING,                   -- DIM_ACCOUNT.ACCOUNT_ID VARCHAR(20); sets 1, 2
    KYC_STATUS         STRING,                   -- sets 1, 3
    TXN_COUNT          BIGINT,                   -- sets 1, 3
    TOTAL_AMOUNT       DECIMAL(25,2),            -- sets 1, 3
    AVG_AMOUNT         DECIMAL(25,6),            -- set 1
    LAST_TXN_DATE      DATE,                     -- set 1
    CREDIT_DATE        DATE,                     -- set 2
    CREDIT_AMOUNT      DECIMAL(15,2),            -- set 2
    DEBIT_DATE         DATE,                     -- set 2
    DEBIT_AMOUNT       DECIMAL(15,2),            -- set 2
    DAYS_BETWEEN       INT,                      -- set 2
    ONBOARDING_DATE    DATE,                     -- set 3
    DAYS_SINCE_ONBOARD INT                       -- set 3
)
COMMENT 'Materialised result sets of AML_SCREENING, one run per RUN_ID; read through VW_AML_* only';

CREATE OR REPLACE PROCEDURE ${catalog}.${schema}.AML_SCREENING(
    IN screening_date    DATE          DEFAULT NULL,        -- NULL -> current_date(), see body
    IN lookback_days     INT           DEFAULT 30,
    IN amount_threshold  DECIMAL(15,2) DEFAULT 50000.00
)
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
COMMENT 'Anti-Money Laundering screening: structuring, rapid movement, new-customer international'
AS BEGIN
    DECLARE v_date   DATE;
    DECLARE v_from   DATE;
    DECLARE v_run_id STRING;

    -- The source macro is one request: a failure leaves no result set behind. Here the unpublished run is already
    -- invisible to the views; the handler removes its rows so nothing is left for housekeeping, then re-raises so the
    -- caller sees the failure exactly as EXEC did ("SIGNAL and RESIGNAL", sql-scripting.md).
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        DELETE FROM ${catalog}.${schema}.AML_SCREENING_RESULT WHERE RUN_ID = v_run_id;
        DELETE FROM ${catalog}.${schema}.AML_SCREENING_RUN    WHERE RUN_ID = v_run_id;
        RESIGNAL;
    END;

    SET v_date   = COALESCE(screening_date, current_date());
    SET v_from   = date_add(v_date, -lookback_days);        -- :screening_date - :lookback_days (DATE - INTEGER = days)
    SET v_run_id = uuid();

    INSERT INTO ${catalog}.${schema}.AML_SCREENING_RUN
        (RUN_ID, SCREENING_DATE, LOOKBACK_DAYS, AMOUNT_THRESHOLD, STARTED_TS, COMPLETED_TS)
    VALUES (v_run_id, v_date, lookback_days, amount_threshold, current_timestamp(), NULL);

    -- Pattern 1: Structuring
    INSERT INTO ${catalog}.${schema}.AML_SCREENING_RESULT
        (RUN_ID, SCREENING_DATE, RESULT_SET_NO, PATTERN_TYPE, CUSTOMER_ID, CUSTOMER_NAME, KYC_STATUS, ACCOUNT_ID,
         TXN_COUNT, TOTAL_AMOUNT, AVG_AMOUNT, LAST_TXN_DATE, SORT_ORDER)
    SELECT
        v_run_id, v_date, 1, 'STRUCTURING',
        c.CUSTOMER_ID,
        c.FIRST_NAME || ' ' || c.LAST_NAME,
        c.KYC_STATUS,
        a.ACCOUNT_ID,
        COUNT(*),
        SUM(ft.BASE_CURRENCY_AMOUNT),                         -- FORMAT dropped; DECIMAL(15,2) preserved
        AVG(ft.BASE_CURRENCY_AMOUNT),                         -- AVG scale differs: recon decimal_round
        MAX(ft.TRANSACTION_DATE),
        ROW_NUMBER() OVER (ORDER BY SUM(ft.BASE_CURRENCY_AMOUNT) DESC)   -- ORDER BY of the result set, kept as a column
    FROM ${catalog}.${schema}.FACT_TRANSACTION ft
    INNER JOIN ${catalog}.${schema}.DIM_ACCOUNT a
        ON ft.ACCOUNT_KEY = a.ACCOUNT_KEY AND a.CURRENT_FLAG = 'Y'
    INNER JOIN ${catalog}.${schema}.DIM_CUSTOMER c
        ON ft.CUSTOMER_KEY = c.CUSTOMER_KEY AND c.CURRENT_FLAG = 'Y'
    WHERE ft.TRANSACTION_DATE BETWEEN v_from AND v_date
      AND ft.TRANSACTION_TYPE IN ('CREDIT', 'DEBIT')
      AND ft.BASE_CURRENCY_AMOUNT BETWEEN (amount_threshold * 0.8) AND amount_threshold
    GROUP BY c.CUSTOMER_ID, c.FIRST_NAME, c.LAST_NAME, c.KYC_STATUS, a.ACCOUNT_ID
    HAVING COUNT(*) >= 3;

    -- Pattern 2: Rapid movement
    INSERT INTO ${catalog}.${schema}.AML_SCREENING_RESULT
        (RUN_ID, SCREENING_DATE, RESULT_SET_NO, PATTERN_TYPE, CUSTOMER_ID, CUSTOMER_NAME, ACCOUNT_ID,
         CREDIT_DATE, CREDIT_AMOUNT, DEBIT_DATE, DEBIT_AMOUNT, DAYS_BETWEEN, SORT_ORDER)
    SELECT
        v_run_id, v_date, 2, 'RAPID_MOVEMENT',
        c.CUSTOMER_ID,
        c.FIRST_NAME || ' ' || c.LAST_NAME,
        a.ACCOUNT_ID,
        cr.CREDIT_DATE,
        cr.CREDIT_AMOUNT,
        dr.DEBIT_DATE,
        dr.DEBIT_AMOUNT,
        datediff(dr.DEBIT_DATE, cr.CREDIT_DATE),              -- DATE - DATE = INTEGER days
        ROW_NUMBER() OVER (ORDER BY cr.CREDIT_AMOUNT DESC)
    FROM ${catalog}.${schema}.DIM_CUSTOMER c
    INNER JOIN ${catalog}.${schema}.DIM_ACCOUNT a
        ON c.CUSTOMER_ID = a.CUSTOMER_ID AND a.CURRENT_FLAG = 'Y'
    INNER JOIN (
        SELECT ACCOUNT_KEY, TRANSACTION_DATE AS CREDIT_DATE, BASE_CURRENCY_AMOUNT AS CREDIT_AMOUNT
        FROM ${catalog}.${schema}.FACT_TRANSACTION
        WHERE TRANSACTION_TYPE = 'CREDIT'
          AND BASE_CURRENCY_AMOUNT >= amount_threshold
          AND TRANSACTION_DATE BETWEEN v_from AND v_date
    ) cr ON a.ACCOUNT_KEY = cr.ACCOUNT_KEY
    INNER JOIN (
        SELECT ACCOUNT_KEY, TRANSACTION_DATE AS DEBIT_DATE, BASE_CURRENCY_AMOUNT AS DEBIT_AMOUNT
        FROM ${catalog}.${schema}.FACT_TRANSACTION
        WHERE TRANSACTION_TYPE IN ('DEBIT', 'TRANSFER')
          AND BASE_CURRENCY_AMOUNT >= amount_threshold * 0.9
          AND TRANSACTION_DATE BETWEEN v_from AND v_date
    ) dr ON cr.ACCOUNT_KEY = dr.ACCOUNT_KEY
        AND dr.DEBIT_DATE BETWEEN cr.CREDIT_DATE AND date_add(cr.CREDIT_DATE, 3)   -- DATE + 3
    WHERE c.CURRENT_FLAG = 'Y';

    -- Pattern 3: New customer international
    INSERT INTO ${catalog}.${schema}.AML_SCREENING_RESULT
        (RUN_ID, SCREENING_DATE, RESULT_SET_NO, PATTERN_TYPE, CUSTOMER_ID, CUSTOMER_NAME, ONBOARDING_DATE,
         DAYS_SINCE_ONBOARD, KYC_STATUS, TXN_COUNT, TOTAL_AMOUNT, SORT_ORDER)
    SELECT
        v_run_id, v_date, 3, 'NEW_CUSTOMER_INTL',
        c.CUSTOMER_ID,
        c.FIRST_NAME || ' ' || c.LAST_NAME,
        c.ONBOARDING_DATE,
        datediff(v_date, c.ONBOARDING_DATE),
        c.KYC_STATUS,
        COUNT(*),
        SUM(ft.BASE_CURRENCY_AMOUNT),
        ROW_NUMBER() OVER (ORDER BY SUM(ft.BASE_CURRENCY_AMOUNT) DESC)
    FROM ${catalog}.${schema}.FACT_TRANSACTION ft
    INNER JOIN ${catalog}.${schema}.DIM_CUSTOMER c
        ON ft.CUSTOMER_KEY = c.CUSTOMER_KEY AND c.CURRENT_FLAG = 'Y'
    WHERE ft.IS_INTERNATIONAL = 1
      AND ft.TRANSACTION_DATE BETWEEN v_from AND v_date
      AND c.ONBOARDING_DATE >= date_add(v_date, -90)
    GROUP BY c.CUSTOMER_ID, c.FIRST_NAME, c.LAST_NAME, c.ONBOARDING_DATE, c.KYC_STATUS
    HAVING SUM(ft.BASE_CURRENCY_AMOUNT) >= amount_threshold;

    -- Publish: the single-row UPDATE is the atomic switch that makes this run the one VW_AML_* resolve for v_date.
    -- Until it commits, consumers still see the previously published run (or nothing); after it, only this one.
    UPDATE ${catalog}.${schema}.AML_SCREENING_RUN
    SET COMPLETED_TS = current_timestamp()
    WHERE RUN_ID = v_run_id;

    -- Retire runs for this date that were published *before* this one. In-flight runs (COMPLETED_TS IS NULL) are never
    -- touched, so a concurrent same-date CALL keeps its rows and, when it publishes later, becomes the visible run and
    -- retires this one -- "last EXEC wins", as on the source, with no interleaving of two runs' rows.
    DELETE FROM ${catalog}.${schema}.AML_SCREENING_RESULT
    WHERE SCREENING_DATE = v_date
      AND RUN_ID IN (SELECT r.RUN_ID
                     FROM ${catalog}.${schema}.AML_SCREENING_RUN r
                     WHERE r.SCREENING_DATE = v_date
                       AND r.COMPLETED_TS IS NOT NULL
                       AND r.COMPLETED_TS < (SELECT COMPLETED_TS FROM ${catalog}.${schema}.AML_SCREENING_RUN
                                             WHERE RUN_ID = v_run_id));
    DELETE FROM ${catalog}.${schema}.AML_SCREENING_RUN
    WHERE SCREENING_DATE = v_date
      AND COMPLETED_TS IS NOT NULL
      AND COMPLETED_TS < (SELECT COMPLETED_TS FROM ${catalog}.${schema}.AML_SCREENING_RUN WHERE RUN_ID = v_run_id);
END;

-- Positional consumers of result set N read these instead of EXEC output. The macro's :screening_date parameter
-- becomes the consumer's predicate, and the result set's ORDER BY becomes the consumer's ORDER BY SORT_ORDER:
--   EXEC AML_SCREENING(DATE '2024-03-31')  ->  CALL ${catalog}.${schema}.AML_SCREENING(DATE '2024-03-31');
--                                             SELECT ... FROM VW_AML_<n> WHERE SCREENING_DATE = DATE '2024-03-31'
--                                             ORDER BY SORT_ORDER;
-- and the defaulted EXEC AML_SCREENING() -> WHERE SCREENING_DATE = current_date() ORDER BY SORT_ORDER. The ORDER BY
-- is the consumer's, not the view's: a view (like a table) has no row order, so an ORDER BY inside the view would
-- promise nothing to a SELECT over it. SORT_ORDER is the source ORDER BY materialised as ROW_NUMBER() at run time,
-- so a consumer that orders by it gets the rows in the order the macro's spool had them (ties in the source key,
-- e.g. equal TOTAL_AMOUNT, are arbitrary on both engines: Tier 4 compares ordered output only up to ties). A
-- positional consumer that does not add ORDER BY SORT_ORDER is a conversion defect, not a target property.
-- No view picks "the latest date": a backdated run would otherwise be
-- invisible behind a newer one, and two runs for different dates never see each other's rows. For one date the views
-- resolve exactly one run -- the latest *published* one (QUALIFY: databricks-dbsql references/best-practices.md
-- "Query Optimization Tips") -- so a consumer reading after its own CALL returned sees that CALL's rows or a later
-- completed run's, never a mixture, never a run still in flight.
CREATE OR REPLACE VIEW ${catalog}.${schema}.VW_AML_PUBLISHED_RUN AS
SELECT RUN_ID, SCREENING_DATE, LOOKBACK_DAYS, AMOUNT_THRESHOLD, COMPLETED_TS
FROM ${catalog}.${schema}.AML_SCREENING_RUN
WHERE COMPLETED_TS IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY SCREENING_DATE ORDER BY COMPLETED_TS DESC, RUN_ID) = 1;

CREATE OR REPLACE VIEW ${catalog}.${schema}.VW_AML_STRUCTURING AS
SELECT r.SCREENING_DATE, r.PATTERN_TYPE, r.CUSTOMER_ID, r.CUSTOMER_NAME, r.KYC_STATUS, r.ACCOUNT_ID, r.TXN_COUNT,
       r.TOTAL_AMOUNT, r.AVG_AMOUNT, r.LAST_TXN_DATE, r.SORT_ORDER
FROM ${catalog}.${schema}.AML_SCREENING_RESULT r
INNER JOIN ${catalog}.${schema}.VW_AML_PUBLISHED_RUN p ON r.RUN_ID = p.RUN_ID
WHERE r.RESULT_SET_NO = 1;

CREATE OR REPLACE VIEW ${catalog}.${schema}.VW_AML_RAPID_MOVEMENT AS
SELECT r.SCREENING_DATE, r.PATTERN_TYPE, r.CUSTOMER_ID, r.CUSTOMER_NAME, r.ACCOUNT_ID, r.CREDIT_DATE, r.CREDIT_AMOUNT,
       r.DEBIT_DATE, r.DEBIT_AMOUNT, r.DAYS_BETWEEN, r.SORT_ORDER
FROM ${catalog}.${schema}.AML_SCREENING_RESULT r
INNER JOIN ${catalog}.${schema}.VW_AML_PUBLISHED_RUN p ON r.RUN_ID = p.RUN_ID
WHERE r.RESULT_SET_NO = 2;

CREATE OR REPLACE VIEW ${catalog}.${schema}.VW_AML_NEW_CUSTOMER_INTL AS
SELECT r.SCREENING_DATE, r.PATTERN_TYPE, r.CUSTOMER_ID, r.CUSTOMER_NAME, r.ONBOARDING_DATE, r.DAYS_SINCE_ONBOARD,
       r.KYC_STATUS, r.TXN_COUNT AS INTL_TXN_COUNT, r.TOTAL_AMOUNT AS TOTAL_INTL_AMOUNT, r.SORT_ORDER
FROM ${catalog}.${schema}.AML_SCREENING_RESULT r
INNER JOIN ${catalog}.${schema}.VW_AML_PUBLISHED_RUN p ON r.RUN_ID = p.RUN_ID
WHERE r.RESULT_SET_NO = 3;
