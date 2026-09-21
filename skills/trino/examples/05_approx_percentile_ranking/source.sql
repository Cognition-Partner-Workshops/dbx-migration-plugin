-- Promo lift by median order total, ranked. Approximate percentile drives the ranking.
SELECT
    u.attr_value                                    AS promo,
    COUNT(*)                                        AS orders,
    approx_percentile(o.order_total, 0.5)           AS median_order_total,
    AVG(o.order_total)                              AS average_order_total
FROM lake.core.orders o
CROSS JOIN UNNEST(map_entries(o.attrs)) AS u(attr_key, attr_value)
WHERE u.attr_key = 'promo'
GROUP BY u.attr_value
ORDER BY median_order_total DESC;
