-- Customer lifetime value mart: elapsed active days, average order value, orders per active day.
DROP TABLE IF EXISTS lake.mart.customer_ltv;

CREATE TABLE lake.mart.customer_ltv AS
SELECT
    customer_id,
    COUNT(*)                                                    AS orders,
    SUM(order_total)                                            AS lifetime_revenue,
    AVG(order_total)                                            AS avg_order_value,
    date_diff('day', MIN(order_ts), MAX(order_ts))              AS active_days,
    SUM(order_total) / COUNT(*)                                 AS revenue_per_order,
    COUNT(*) / 7                                                AS whole_weeks_of_orders
FROM lake.core.orders
WHERE status <> 'CANCELLED'
GROUP BY customer_id;
