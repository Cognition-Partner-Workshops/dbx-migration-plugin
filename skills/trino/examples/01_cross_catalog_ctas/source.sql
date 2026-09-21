-- Nightly regional revenue mart: Hive/Parquet orders joined to a PostgreSQL customer table.
-- Region is a PostgreSQL CHAR(4) column surfaced by the JDBC connector.
SET SESSION time_zone = 'UTC';

DROP TABLE IF EXISTS lake.mart.region_revenue;

CREATE TABLE lake.mart.region_revenue
WITH (format = 'PARQUET') AS
SELECT
    c.region,
    format_datetime(o.order_ts, 'yyyy-MM')          AS order_month,
    COUNT(*)                                        AS orders,
    approx_distinct(o.customer_id)                  AS active_customers,
    SUM(o.order_total)                              AS gross_revenue,
    arbitrary(c.segment)                            AS sample_segment
FROM lake.core.orders o
JOIN ops.public.customers c
  ON c.customer_id = o.customer_id
WHERE o.status <> 'CANCELLED'
GROUP BY c.region, format_datetime(o.order_ts, 'yyyy-MM');
