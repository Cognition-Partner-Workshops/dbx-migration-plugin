# 05 — approximate percentile driving a ranking

| Trino | Converted | SKILL.md |
|---|---|---|
| `approx_percentile(x, 0.5)` (T-Digest; `REAL` for `DECIMAL` input **(probe)**) | decision-approved exact `percentile(x, 0.5)` cast to `FLOAT`; without a recorded `D-<id>` row naming this consumer, use `percentile_approx` | row 52, trap "Approximate aggregates" |
| `ORDER BY median_order_total DESC` (ties/NULLs unspecified) | `... DESC NULLS LAST, promo NULLS LAST` (deterministic tie-break) | row 84 |
| `AVG(decimal(12,2))` | `CAST(AVG(x) AS DECIMAL(12,2))` | row 3 |
| `CROSS JOIN UNNEST(map_entries())` | `LATERAL VIEW EXPLODE` | row 68, example 02 |

**Recon**: Tier 4 on the ranking — when promo medians sit within the estimators' error band, Trino's
`approx_percentile` and Databricks' `percentile_approx` rank groups in *different orders* (observed on a
4-value sample as `3.0` vs `2.0` where the exact answer is `2.5` **(probe)**); Tier 3 keyed on `promo` for
`median_order_total`. The exact form in `converted.sql` is the decision-approved variant; cite the applicable
`D-<id>` row in `.migration/06_decisions.md` before using it. The recon query uses the exact percentile on **both** sides; comparing an approximate source
value against anything is not a like-for-like tier and must not be masked by a tolerance. If the business wants
the source's approximate number preserved, that is a decision row (`06_decisions.md`), and the Tier 3 column is
excluded rather than toleranced.

Without that decision, the converted expression remains approximate:

```sql
percentile_approx(o.order_total, 0.5) AS median_order_total
```

**Canonicalization**: `decimal_round` places 2 on `average_order_total`; `identity` on `median_order_total` after
the exact recompute (a `FLOAT`: compare as a number, not as text — `CAST(double AS STRING)` differs **(probe)**).

**Not verified live**: T-Digest vs exact divergence magnitude on the engagement's distributions; Trino
`approx_percentile(x, w, p)` weighted form; `percentile` performance on large groups (consider `median`).
