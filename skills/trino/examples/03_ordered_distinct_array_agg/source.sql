-- Customer tag roll-up: one row per customer with sorted distinct tags, a representative tier and a CSV string.
SELECT
    t.customer_id,
    array_agg(DISTINCT t.tag ORDER BY t.tag)                    AS tags,
    cardinality(array_agg(DISTINCT t.tag))                      AS tag_count,
    arbitrary(t.tier)                                           AS tier,
    listagg(t.tag, ',') WITHIN GROUP (ORDER BY t.tag)           AS tag_csv,
    max_by(t.tag, t.assigned_ts)                                AS latest_tag
FROM ops.public.customer_tags t
GROUP BY t.customer_id;
