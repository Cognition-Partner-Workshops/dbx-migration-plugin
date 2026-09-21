# 03 — ordered distinct `array_agg`, `arbitrary`, `listagg`, `max_by`

| Trino | Converted | SKILL.md |
|---|---|---|
| `array_agg(DISTINCT x ORDER BY x)` | `array_sort(collect_set(x))`: aggregate-level `ORDER BY` is not honoured on Databricks, `array_sort` restores it; equal for non-NULL input **(probe)** | row 58 |
| `cardinality(array_agg(DISTINCT x))` | `size(collect_set(x))` | rows 58, 63 |
| `arbitrary(x)` | `any_value(x)`; non-deterministic on both | row 55 |
| `listagg(x, ',') WITHIN GROUP (ORDER BY x)` | same **(probe)** | row 59 |
| `max_by(x, k)` | `max_by(x, k)` **(probe)**; ties are non-deterministic on both | row 56 |
| NULL tags: kept by Trino `array_agg`, dropped by `collect_set` | not masked: the source column is `NOT NULL` in this shape; if the census shows NULL tags, `filter(tags, x -> x IS NOT NULL)` on the Trino recon side and a decision row | trap "NULLs in array_agg" |

**Recon**: Tier 1 count per customer; Tier 3 keyed on `customer_id` for `tags` (array compare, element-wise) and
`tag_csv` — an unsorted `collect_list` shows as array/text diffs on every customer with more than one tag, a NULL
tag shows as a `tag_count` shortfall; `tier` and `latest_tag` (on ties) are excluded from Tier 3 or replaced by
`min(tier)` on both sides of the recon query.

**Canonicalization**: `identity`; arrays compare element-wise (no text cast), see §8.

**Not verified live**: `collect_set` on a `CHAR(n)`-typed tag column (padding would need `rstrip_spaces` inside the
array, a harness gap); `listagg` overflow behaviour.
