# 02 — map explode report: `UNNEST(map_entries())`, `cardinality`, `element_at`

| Trino | Converted | SKILL.md |
|---|---|---|
| `CROSS JOIN UNNEST(map_entries(m)) AS u(k, v)` | `LATERAL VIEW EXPLODE(m) u AS k, v` (`explode(map)` yields key/value rows **(probe)**) | row 68 |
| `cardinality(m)` | `size(m)` (`size(NULL)` is `-1` without ANSI: the source column is `NOT NULL` here, else wrap `coalesce`) | row 63 |
| `element_at(m, 'channel')` (missing key -> NULL) | `element_at(m, 'channel')` (same **(probe)**) | row 62 |
| `ORDER BY orders DESC, promo` | `... NULLS LAST` spelled out: Trino defaults NULLs last in both directions, Databricks `ASC` puts them first **(probe)** | row 84 |
| rows with an empty `attrs` map vanish | `EXPLODE` (not `_OUTER`) keeps that behaviour | trap "UNNEST drops empty arrays" |

**Recon**: Tier 1 on the exploded row count (an `EXPLODE_OUTER` or a `map_entries` mis-alias over-counts by the
number of empty-map orders); Tier 3 keyed on `promo` for `orders`, `rich_attr_orders`, `channels`; Tier 4 on the
report order — a NULL `promo` value (present when the key exists with a NULL value) sorts first on Databricks
without `NULLS LAST` and flips the first row.

**Canonicalization**: `identity`; `null_missing_equiv` for `channels` when the `channel` key is absent on some rows.

**Not verified live**: `size(NULL)` under the warehouse's ANSI setting; `LATERAL VIEW` vs `, LATERAL explode()`
plan equivalence on the engagement's DBR.
