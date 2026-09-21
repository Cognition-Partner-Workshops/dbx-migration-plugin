SELECT
    u.attr_value                                    AS promo,
    COUNT(*)                                        AS orders,
    SUM(CASE WHEN size(o.attrs) > 2 THEN 1 ELSE 0 END) AS rich_attr_orders,
    COUNT(DISTINCT element_at(o.attrs, 'channel'))  AS channels
FROM ${catalog}.core.orders o
LATERAL VIEW EXPLODE(o.attrs) u AS attr_key, attr_value
WHERE u.attr_key = 'promo'
GROUP BY u.attr_value
ORDER BY orders DESC NULLS LAST, promo NULLS LAST;
