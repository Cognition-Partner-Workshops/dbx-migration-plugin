-- order_total is DECIMAL(12,2) on both sides. Trino result types: AVG -> DECIMAL(12,2),
-- SUM/COUNT -> DECIMAL(31,21) (declared consumer scale 2, see NOTE), COUNT/7 -> BIGINT.
CREATE OR REPLACE TABLE ${catalog}.mart.customer_ltv AS
SELECT
    customer_id,
    COUNT(*)                                                    AS orders,
    CAST(SUM(order_total) AS DECIMAL(38, 2))                    AS lifetime_revenue,
    CAST(AVG(order_total) AS DECIMAL(12, 2))                    AS avg_order_value,
    timestampdiff(DAY, MIN(order_ts), MAX(order_ts))            AS active_days,
    CAST(SUM(order_total) / COUNT(*) AS DECIMAL(38, 2))         AS revenue_per_order,
    COUNT(*) DIV 7                                              AS whole_weeks_of_orders
FROM ${catalog}.core.orders
WHERE status <> 'CANCELLED'
GROUP BY customer_id;
