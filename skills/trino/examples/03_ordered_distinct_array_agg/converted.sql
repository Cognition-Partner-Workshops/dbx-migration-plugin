-- `core.customer_tags` is the ingested copy of the PostgreSQL table.
SELECT
    t.customer_id,
    array_sort(collect_set(t.tag))                              AS tags,
    size(collect_set(t.tag))                                    AS tag_count,
    any_value(t.tier)                                           AS tier,
    listagg(t.tag, ',') WITHIN GROUP (ORDER BY t.tag)           AS tag_csv,
    max_by(t.tag, t.assigned_ts)                                AS latest_tag
FROM ${catalog}.core.customer_tags t
GROUP BY t.customer_id;
