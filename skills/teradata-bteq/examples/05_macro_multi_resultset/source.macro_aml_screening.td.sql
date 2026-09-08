/* Source: uc-dw-migration-teradata-to-bigquery dml/macros/macro_aml_screening.sql (verbatim) */
/**********************************************************************
 * MACRO_AML_SCREENING
 * Anti-Money Laundering screening macro
 * Identifies suspicious transaction patterns
 **********************************************************************/

REPLACE MACRO BANKING_DW.AML_SCREENING (
    screening_date DATE FORMAT 'YYYY-MM-DD' DEFAULT DATE,
    lookback_days INTEGER DEFAULT 30,
    amount_threshold DECIMAL(15,2) DEFAULT 50000.00
) AS (
    -- Pattern 1: Structuring — multiple transactions just below threshold
    SEL
        'STRUCTURING' AS PATTERN_TYPE,
        c.CUSTOMER_ID,
        c.FIRST_NAME || ' ' || c.LAST_NAME AS CUSTOMER_NAME,
        c.KYC_STATUS,
        a.ACCOUNT_ID,
        COUNT(*) AS TXN_COUNT,
        SUM(ft.BASE_CURRENCY_AMOUNT) (FORMAT 'ZZZ,ZZZ,ZZ9.99') AS TOTAL_AMOUNT,
        AVG(ft.BASE_CURRENCY_AMOUNT) (FORMAT 'ZZZ,ZZZ,ZZ9.99') AS AVG_AMOUNT,
        MAX(ft.TRANSACTION_DATE) (FORMAT 'YYYY-MM-DD') AS LAST_TXN_DATE
    FROM BANKING_DW.FACT_TRANSACTION ft
    INNER JOIN BANKING_DW.DIM_ACCOUNT a
        ON ft.ACCOUNT_KEY = a.ACCOUNT_KEY AND a.CURRENT_FLAG = 'Y'
    INNER JOIN BANKING_DW.DIM_CUSTOMER c
        ON ft.CUSTOMER_KEY = c.CUSTOMER_KEY AND c.CURRENT_FLAG = 'Y'
    WHERE ft.TRANSACTION_DATE BETWEEN (:screening_date - :lookback_days) AND :screening_date
      AND ft.TRANSACTION_TYPE IN ('CREDIT', 'DEBIT')
      AND ft.BASE_CURRENCY_AMOUNT BETWEEN (:amount_threshold * 0.8) AND :amount_threshold
    GROUP BY c.CUSTOMER_ID, c.FIRST_NAME, c.LAST_NAME, c.KYC_STATUS, a.ACCOUNT_ID
    HAVING COUNT(*) >= 3
    ORDER BY SUM(ft.BASE_CURRENCY_AMOUNT) DESC;

    -- Pattern 2: Rapid movement — large deposits followed by immediate withdrawals
    SEL
        'RAPID_MOVEMENT' AS PATTERN_TYPE,
        c.CUSTOMER_ID,
        c.FIRST_NAME || ' ' || c.LAST_NAME AS CUSTOMER_NAME,
        a.ACCOUNT_ID,
        cr.CREDIT_DATE (FORMAT 'YYYY-MM-DD'),
        cr.CREDIT_AMOUNT (FORMAT 'ZZZ,ZZZ,ZZ9.99'),
        dr.DEBIT_DATE (FORMAT 'YYYY-MM-DD'),
        dr.DEBIT_AMOUNT (FORMAT 'ZZZ,ZZZ,ZZ9.99'),
        (dr.DEBIT_DATE - cr.CREDIT_DATE) AS DAYS_BETWEEN
    FROM BANKING_DW.DIM_CUSTOMER c
    INNER JOIN BANKING_DW.DIM_ACCOUNT a
        ON c.CUSTOMER_ID = a.CUSTOMER_ID AND a.CURRENT_FLAG = 'Y'
    INNER JOIN (
        SEL ACCOUNT_KEY, TRANSACTION_DATE AS CREDIT_DATE,
             BASE_CURRENCY_AMOUNT AS CREDIT_AMOUNT
        FROM BANKING_DW.FACT_TRANSACTION
        WHERE TRANSACTION_TYPE = 'CREDIT'
          AND BASE_CURRENCY_AMOUNT >= :amount_threshold
          AND TRANSACTION_DATE BETWEEN (:screening_date - :lookback_days) AND :screening_date
    ) cr ON a.ACCOUNT_KEY = cr.ACCOUNT_KEY
    INNER JOIN (
        SEL ACCOUNT_KEY, TRANSACTION_DATE AS DEBIT_DATE,
             BASE_CURRENCY_AMOUNT AS DEBIT_AMOUNT
        FROM BANKING_DW.FACT_TRANSACTION
        WHERE TRANSACTION_TYPE IN ('DEBIT', 'TRANSFER')
          AND BASE_CURRENCY_AMOUNT >= :amount_threshold * 0.9
          AND TRANSACTION_DATE BETWEEN (:screening_date - :lookback_days) AND :screening_date
    ) dr ON cr.ACCOUNT_KEY = dr.ACCOUNT_KEY
        AND dr.DEBIT_DATE BETWEEN cr.CREDIT_DATE AND cr.CREDIT_DATE + 3
    WHERE c.CURRENT_FLAG = 'Y'
    ORDER BY cr.CREDIT_AMOUNT DESC;

    -- Pattern 3: International high-value transactions from newly onboarded customers
    SEL
        'NEW_CUSTOMER_INTL' AS PATTERN_TYPE,
        c.CUSTOMER_ID,
        c.FIRST_NAME || ' ' || c.LAST_NAME AS CUSTOMER_NAME,
        c.ONBOARDING_DATE (FORMAT 'YYYY-MM-DD'),
        (:screening_date - c.ONBOARDING_DATE) AS DAYS_SINCE_ONBOARD,
        c.KYC_STATUS,
        COUNT(*) AS INTL_TXN_COUNT,
        SUM(ft.BASE_CURRENCY_AMOUNT) (FORMAT 'ZZZ,ZZZ,ZZ9.99') AS TOTAL_INTL_AMOUNT
    FROM BANKING_DW.FACT_TRANSACTION ft
    INNER JOIN BANKING_DW.DIM_CUSTOMER c
        ON ft.CUSTOMER_KEY = c.CUSTOMER_KEY AND c.CURRENT_FLAG = 'Y'
    WHERE ft.IS_INTERNATIONAL = 1
      AND ft.TRANSACTION_DATE BETWEEN (:screening_date - :lookback_days) AND :screening_date
      AND c.ONBOARDING_DATE >= (:screening_date - 90)
    GROUP BY c.CUSTOMER_ID, c.FIRST_NAME, c.LAST_NAME,
             c.ONBOARDING_DATE, c.KYC_STATUS
    HAVING SUM(ft.BASE_CURRENCY_AMOUNT) >= :amount_threshold
    ORDER BY SUM(ft.BASE_CURRENCY_AMOUNT) DESC;
);
