# 01 — cross-catalog CTAS mart with a `CHAR(n)` key, `approx_distinct`, `format_datetime`

| Trino | Converted | SKILL.md |
|---|---|---|
| `lake.core.orders JOIN ops.public.customers` (Hive + PostgreSQL catalogs) | both sides read from the migration catalog; the JDBC side is an ingestion unit (`[lakeflow-connect:2-database-connectors.md]`) or a foreign catalog during parallel run | §3 cross-catalog unit, P24 |
| `DROP TABLE IF EXISTS` + `CREATE TABLE ... WITH (format='PARQUET') AS` | `CREATE OR REPLACE TABLE ... AS` (atomic) `[dbsql:best-practices.md#Fact Table Patterns]` | P3 |
| `SET SESSION time_zone` | `SET TIME ZONE` | P12 |
| `c.region` `CHAR(4)` (padded, e.g. `'NORT'`; value truncated at the source) | `rtrim(c.region)`; landed type `string` | §4 `CHAR(n)`, trap "CHAR padding" |
| `format_datetime(ts, 'yyyy-MM')` | `date_format(ts, 'yyyy-MM')` | row 30 |
| `approx_distinct(x)` | `COUNT(DISTINCT x)` only in a decision-approved exact variant; without a recorded `D-<id>` row naming this consumer, use `approx_count_distinct(x)` | row 51 |
| `SUM(decimal(p,2))` | `CAST(SUM(...) AS DECIMAL(38,2))` so the declared type matches the Trino result | row 4 |
| `arbitrary(x)` | `any_value(x)` | row 55 |

The `COUNT(DISTINCT)` form in `converted.sql` is the decision-approved exact variant; cite the applicable
`D-<id>` row in `.migration/06_decisions.md` before using it. Without that decision, the converted expression is
`approx_count_distinct(o.customer_id)`.

**Recon**: Tier 1 count per `(region, order_month)`; Tier 2 `SUM(gross_revenue)` (`decimal_round` places 2) and
`SUM(orders)`; Tier 3 keyed on `(region, order_month)` for `active_customers` — the recon query recomputes
`COUNT(DISTINCT)` on the Trino side too, otherwise the HLL estimate reads as a Tier 3 miss. A padded key that is not
trimmed shows as a Tier 1 shortfall when the mart is joined to an unpadded region dimension, and as Tier 3 key
misses (`'NORT'` vs `'NORT '`) before `rstrip_spaces`. `sample_segment` is excluded from Tier 3 (non-deterministic).

**Canonicalization**: `rstrip_spaces` (region), `decimal_round` (gross_revenue), `identity`.

**Not verified live**: Lakehouse Federation pushdown of `rtrim` to PostgreSQL; the ingestion unit's cadence relative
to the mart schedule (P24 count drift).
