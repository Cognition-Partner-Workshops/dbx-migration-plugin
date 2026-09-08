-- Target: a Teradata macro with parameters and three result sets. Databricks has no macro object; the cited procedure
-- syntax (databricks-dbsql references/sql-scripting.md "Stored Procedures") documents IN/OUT parameters and
-- DEFAULTs but not client-visible result sets, so the macro becomes:
--   (a) a procedure that materialises all three patterns into one screening table (RESULT_SET_NO keeps the order),
--   (b) one view per pattern for the consumers that read `EXEC AML_SCREENING(...)` output positionally.
-- If the macro had been a single SELECT it would have become a SQL table-valued function instead (skill §6).
-- Parameter DEFAULTs: "Once a parameter has a DEFAULT, all subsequent parameters must also have defaults"; all three do.
-- Teradata `DEFAULT DATE` = today; the DEFAULT expression cannot be current_date() if the target rejects
-- non-constant defaults (Not verified live), so NULL is the sentinel and the body resolves it.

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
    DECLARE v_date DATE;
    DECLARE v_from DATE;
    SET v_date = COALESCE(screening_date, current_date());
    SET v_from = date_add(v_date, -lookback_days);          -- :screening_date - :lookback_days (DATE - INTEGER = days)

    DELETE FROM ${catalog}.${schema}.AML_SCREENING_RESULT WHERE SCREENING_DATE = v_date;

    -- Pattern 1: Structuring
    INSERT INTO ${catalog}.${schema}.AML_SCREENING_RESULT
        (SCREENING_DATE, RESULT_SET_NO, PATTERN_TYPE, CUSTOMER_ID, CUSTOMER_NAME, KYC_STATUS, ACCOUNT_ID,
         TXN_COUNT, TOTAL_AMOUNT, AVG_AMOUNT, LAST_TXN_DATE, SORT_ORDER)
    SELECT
        v_date, 1, 'STRUCTURING',
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
        (SCREENING_DATE, RESULT_SET_NO, PATTERN_TYPE, CUSTOMER_ID, CUSTOMER_NAME, ACCOUNT_ID,
         CREDIT_DATE, CREDIT_AMOUNT, DEBIT_DATE, DEBIT_AMOUNT, DAYS_BETWEEN, SORT_ORDER)
    SELECT
        v_date, 2, 'RAPID_MOVEMENT',
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
        (SCREENING_DATE, RESULT_SET_NO, PATTERN_TYPE, CUSTOMER_ID, CUSTOMER_NAME, ONBOARDING_DATE,
         DAYS_SINCE_ONBOARD, KYC_STATUS, TXN_COUNT, TOTAL_AMOUNT, SORT_ORDER)
    SELECT
        v_date, 3, 'NEW_CUSTOMER_INTL',
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
END;

-- Positional consumers of result set N read these instead of EXEC output.
CREATE OR REPLACE VIEW ${catalog}.${schema}.VW_AML_STRUCTURING AS
SELECT PATTERN_TYPE, CUSTOMER_ID, CUSTOMER_NAME, KYC_STATUS, ACCOUNT_ID, TXN_COUNT, TOTAL_AMOUNT, AVG_AMOUNT, LAST_TXN_DATE
FROM ${catalog}.${schema}.AML_SCREENING_RESULT
WHERE RESULT_SET_NO = 1
  AND SCREENING_DATE = (SELECT MAX(SCREENING_DATE) FROM ${catalog}.${schema}.AML_SCREENING_RESULT);   -- latest run only

CREATE OR REPLACE VIEW ${catalog}.${schema}.VW_AML_RAPID_MOVEMENT AS
SELECT PATTERN_TYPE, CUSTOMER_ID, CUSTOMER_NAME, ACCOUNT_ID, CREDIT_DATE, CREDIT_AMOUNT, DEBIT_DATE, DEBIT_AMOUNT, DAYS_BETWEEN
FROM ${catalog}.${schema}.AML_SCREENING_RESULT
WHERE RESULT_SET_NO = 2
  AND SCREENING_DATE = (SELECT MAX(SCREENING_DATE) FROM ${catalog}.${schema}.AML_SCREENING_RESULT);

CREATE OR REPLACE VIEW ${catalog}.${schema}.VW_AML_NEW_CUSTOMER_INTL AS
SELECT PATTERN_TYPE, CUSTOMER_ID, CUSTOMER_NAME, ONBOARDING_DATE, DAYS_SINCE_ONBOARD, KYC_STATUS,
       TXN_COUNT AS INTL_TXN_COUNT, TOTAL_AMOUNT AS TOTAL_INTL_AMOUNT
FROM ${catalog}.${schema}.AML_SCREENING_RESULT
WHERE RESULT_SET_NO = 3
  AND SCREENING_DATE = (SELECT MAX(SCREENING_DATE) FROM ${catalog}.${schema}.AML_SCREENING_RESULT);
