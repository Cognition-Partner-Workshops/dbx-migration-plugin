-- ${catalog}: designated migration catalog. `core.orders` is the landed Hive table; `core.customers` is the
-- ingested (or, during parallel run, federated) copy of the PostgreSQL table, never the source database itself.
SET TIME ZONE 'UTC';

CREATE OR REPLACE TABLE ${catalog}.mart.region_revenue AS
SELECT
    rtrim(c.region)                                                AS region,
    date_format(o.order_ts, 'yyyy-MM')                             AS order_month,
    COUNT(*)                                                       AS orders,
    COUNT(DISTINCT o.customer_id)                                  AS active_customers,
    CAST(SUM(o.order_total) AS DECIMAL(38, 2))                     AS gross_revenue,
    any_value(c.segment)                                           AS sample_segment
FROM ${catalog}.core.orders o
JOIN ${catalog}.core.customers c
  ON c.customer_id = o.customer_id
WHERE o.status <> 'CANCELLED'
GROUP BY rtrim(c.region), date_format(o.order_ts, 'yyyy-MM');
