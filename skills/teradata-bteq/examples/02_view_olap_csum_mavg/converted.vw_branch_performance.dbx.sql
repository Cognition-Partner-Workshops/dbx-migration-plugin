-- Target: Databricks SQL view (databricks-dbsql references/best-practices.md "Query Optimization Tips": window functions,
-- QUALIFY, CTEs).
-- CSUM/MAVG are Teradata OLAP shorthands; both become standard window functions with an explicit ROWS frame.
-- The frame is partitioned by BRANCH_ID because Teradata evaluates CSUM/MAVG over the whole result set ordered by the
-- sort key: verify that reading against the fixture's verify/ signature before assuming a per-branch frame
-- (the fixture's own harness catches the off-by-one on MAVG; skill §7 trap "OLAP frame").

CREATE OR REPLACE VIEW ${catalog}.${schema}.VW_BRANCH_PERFORMANCE
COMMENT 'Monthly branch performance with cumulative sums, moving averages, and regional rankings'
AS
WITH agg AS (
    SELECT
        b.BRANCH_ID,
        b.BRANCH_NAME,
        b.BRANCH_TYPE,
        b.REGION,
        b.CITY,
        snap.SNAPSHOT_MONTH_KEY,
        d.MONTH_NAME,
        d.CALENDAR_YEAR,
        COUNT(DISTINCT snap.ACCOUNT_KEY)                 AS ACCOUNTS_SERVICED,
        COUNT(DISTINCT snap.CUSTOMER_KEY)                AS CUSTOMERS_SERVICED,
        SUM(snap.CLOSING_BALANCE)                        AS TOTAL_DEPOSITS,        -- FORMAT dropped: value, not text
        SUM(snap.TOTAL_DEBITS + snap.TOTAL_CREDITS)      AS TOTAL_VOLUME,
        SUM(snap.FEES_CHARGED)                           AS TOTAL_FEES_EARNED,
        SUM(snap.INTEREST_CHARGED)                       AS TOTAL_INTEREST_INCOME,
        AVG(snap.CLOSING_BALANCE)                        AS AVG_ACCOUNT_BALANCE
    FROM ${catalog}.${schema}.FACT_MONTHLY_ACCOUNT_SNAPSHOT snap
    INNER JOIN ${catalog}.${schema}.DIM_BRANCH b
        ON snap.BRANCH_ID = b.BRANCH_ID
    INNER JOIN ${catalog}.${schema}.DIM_DATE d
        ON snap.SNAPSHOT_DATE = d.CALENDAR_DATE
    WHERE b.IS_ACTIVE = 1
      AND snap.SNAPSHOT_DATE >= add_months(current_date(), -24)   -- LOCKING ROW FOR ACCESS dropped (no lock modes on Delta)
    GROUP BY b.BRANCH_ID, b.BRANCH_NAME, b.BRANCH_TYPE, b.REGION, b.CITY,
             snap.SNAPSHOT_MONTH_KEY, d.MONTH_NAME, d.CALENDAR_YEAR
)
SELECT
    BRANCH_ID,
    BRANCH_NAME,
    BRANCH_TYPE,
    REGION,
    CITY,
    SNAPSHOT_MONTH_KEY,
    -- TRIM(x (FORMAT '9999')) was an implicit FORMAT cast: make it explicit.
    MONTH_NAME || ' ' || CAST(CALENDAR_YEAR AS STRING)                          AS MONTH_LABEL,
    ACCOUNTS_SERVICED,
    CUSTOMERS_SERVICED,
    TOTAL_DEPOSITS,
    TOTAL_VOLUME,
    TOTAL_FEES_EARNED,
    TOTAL_INTEREST_INCOME,
    AVG_ACCOUNT_BALANCE,
    -- CSUM(x, k) -> running SUM ordered by k
    SUM(TOTAL_FEES_EARNED) OVER (PARTITION BY BRANCH_ID ORDER BY SNAPSHOT_MONTH_KEY
                                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)  AS CUMULATIVE_FEES_YTD,
    -- MAVG(x, 3, k) -> 3-ROW moving average: 2 PRECEDING, not 3 (the classic off-by-one)
    AVG(TOTAL_VOLUME) OVER (PARTITION BY BRANCH_ID ORDER BY SNAPSHOT_MONTH_KEY
                            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)               AS MOVING_AVG_VOLUME_3M,
    RANK() OVER (PARTITION BY REGION, SNAPSHOT_MONTH_KEY ORDER BY TOTAL_DEPOSITS DESC) AS REGION_DEPOSIT_RANK,
    -- NULLIFZERO(x) -> nullif(x, 0); DECIMAL/DECIMAL division keeps decimal typing on both engines
    TOTAL_DEPOSITS / nullif(SUM(TOTAL_DEPOSITS) OVER (PARTITION BY REGION, SNAPSHOT_MONTH_KEY), 0) * 100
                                                                                     AS PCT_OF_REGION_DEPOSITS
FROM agg;
