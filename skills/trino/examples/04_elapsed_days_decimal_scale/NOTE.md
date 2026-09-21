# 04 — elapsed days, decimal `AVG`/division scale, integer division

| Trino | Converted | SKILL.md |
|---|---|---|
| `date_diff('day', a, b)` (elapsed 24 h periods) | `timestampdiff(DAY, a, b)` **(probe: both `0` across a midnight two hours apart)**; `datediff(b, a)` is the wrong translation (`1`) | row 34, trap "Elapsed vs calendar days" |
| `AVG(decimal(12,2))` -> `decimal(12,2)` | `CAST(AVG(x) AS DECIMAL(12,2))` (Databricks widens to `(16,6)` **(probe)**) | row 3 |
| `SUM(decimal)/COUNT(*)` -> `decimal(31,21)` on Trino, `(38,?)` on Databricks | cast to the scale the consumer reads (2 here, recorded in the unit mapping) | row 2 |
| `COUNT(*) / 7` -> `bigint` (truncating) | `COUNT(*) DIV 7` (`/` would return `3.5`-style doubles **(probe)**) | row 1 |
| `DROP` + CTAS | `CREATE OR REPLACE TABLE` | P3 |

**Recon**: Tier 3 keyed on `customer_id` — a `datediff` translation is off by one on roughly half the customers
(`active_days`), an uncast `AVG` differs in the 3rd-6th decimal on every row, a `/` instead of `DIV` puts fractions
in `whole_weeks_of_orders`; Tier 2 `SUM(active_days)` and `SUM(lifetime_revenue)` per cohort catch the same at
aggregate level (sums of `lifetime_revenue` match even when `avg_order_value` does not: that pattern *is* the
scale signature).

**Canonicalization**: `decimal_round` places 2 on `avg_order_value` and `revenue_per_order` (both engines round
half away from zero **(probe)**, so `half_up`), `identity` on `lifetime_revenue` (stored scale), `datetime_utc_truncate_ms`
is not needed (no timestamps in the output).

**Not verified live**: Trino's exact `(31,21)` result scale for `SUM/COUNT` on the engagement's precision settings;
`timestampdiff` on `TIMESTAMP_NTZ` inputs across a DST boundary (no zone, so none expected).
