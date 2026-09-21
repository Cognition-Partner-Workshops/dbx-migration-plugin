-- Decision-approved exact median variant: cite the applicable D-<id> row in .migration/06_decisions.md.
-- Trino's approx_percentile over DECIMAL returns REAL, so the declared type of median_order_total is FLOAT.
SELECT
    u.attr_value                                                    AS promo,
    COUNT(*)                                                        AS orders,
    CAST(percentile(o.order_total, 0.5) AS FLOAT)                   AS median_order_total,
    CAST(AVG(o.order_total) AS DECIMAL(12, 2))                      AS average_order_total
FROM ${catalog}.core.orders o
LATERAL VIEW EXPLODE(o.attrs) u AS attr_key, attr_value
WHERE u.attr_key = 'promo'
GROUP BY u.attr_value
ORDER BY median_order_total DESC NULLS LAST, promo NULLS LAST;
