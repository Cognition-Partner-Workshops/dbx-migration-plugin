# 04 - m_BILLING_PREMIUM_RECON / wf_BILLING_PREMIUM_RECON

Fixture: `informatica/XML/wf_BILLING_PREMIUM_RECON.xml` (`source.xml`), `informatica/parameter_files/
wf_BILLING_PREMIUM_RECON.par` (`source.par`). Related, not copied: `orchestration/controlm/ALBION_DWH_DAILY.xml`
job `ALB-FIN-0012` (`TIMEFROM="2200" MONTHDAYS="WD1"`).

## Constructs exercised

| Construct | Where in source | Handled by (SKILL.md) |
|---|---|---|
| `DATE_DIFF(SYSDATE, d, 'MM')` (double) assigned to an **integer** variable port -> rounding, not truncation | `v_MONTHS_ON_RISK` | rows 14, 23, 29; trap 23 |
| `LEAST`/`GREATEST` NULL semantics (Informatica returns NULL if any argument is NULL) | `out_EARNED_PREMIUM_24` | row 58 |
| `decimal(12,2)` arithmetic with `* 0.12` and `/ 24` under `Enable high precision` ON vs OFF | both expressions | section 4 (decimal), trap 3, canonicalization `decimal_round` |
| `ROUND(x, 2)` half-away-from-zero | `EXP_IPT_RECALC` | row 16 |
| Hardcoded rate vs an unused `$$IPT_RATE` in the parameter file | `EXP_IPT_RECALC`, `source.par` | section 2 (parameter precedence: a `$$` declared but not referenced is a finding), section 11 |
| Reusable connected Lookup across the Teradata / core-banking boundary, FK by convention only | `LKP_APF_ACCOUNTS` (`REUSABLE=YES`) | row 74, section 3 (shared objects), section 9 (two connections) |
| Reject file `$OutputFile_BAD_APF=.../BAD_APF_MATCH_$$RUNMONTH.csv` -> quarantine table | `source.par` | section 6 "Row error handling" |
| Control-M `MONTHDAYS="WD1"` business-day calendar -> daily Quartz cron + guard task | Control-M (not copied) | section 6 (scheduler edges) |
| SQL-only mapping routed to DBSQL materialized views (first choice in the routing order) | converted.sql | section 6; `databricks-dbsql` SKILL.md "Materialized View with Scheduled Refresh" |

## Recon tier that catches a wrong conversion

- **Tier 2 `sum(EARNED_PREMIUM_24)` and `sum(IPT_EXPECTED)`** with `decimal_round(places=10)`: if the live session
  runs with high precision OFF, the legacy values carry double rounding at the 15th significant digit and the sums
  diverge in the pence; the recon signature is a small but non-zero delta on every run (trap 3). The fix is
  documented: a per-unit `decimal_round(places=2)` under a recorded decision, never a global loosening.
- **Tier 3 on `EARNED_PREMIUM_24`** (key `TRANSACTION_ID`): truncation instead of rounding for
  `v_MONTHS_ON_RISK` moves every policy whose fractional month is >= .5 by one twenty-fourth (`ANNUAL_PREMIUM/12`
  per row) - a large, systematic, easy-to-spot diff.
- **Tier 2 null rate on `EARNED_PREMIUM_24`**: Spark's NULL-skipping `greatest(0, NULL)` returns 0 and produces a
  value where Informatica produced NULL; the null rate on the converted side drops by the share of rows with a NULL
  `INCEPTION_DT`.
- **Tier 1 on the quarantine table vs the legacy `BAD_APF_MATCH_<RUNMONTH>.csv` row count** (~3%): a conversion
  that inner-joins and silently drops orphans passes Tier 1 on the main target only if the reject count is also
  checked.
- **Tier 3 on `IPT_EXPECTED`**: replacing the literal `0.12` with `$$IPT_RATE` gives identical results today
  (both 0.12) - the recon cannot see the change; that is why it is a decision, not a silent improvement.

## Findings recorded, not converted

- Three earned-premium methods across engines (1/24ths here, 1/12ths in BTEQ, 365ths in SAS) and a manual
  "reconciliation of the reconciliation" workbook - assessment finding.
- `$$IPT_RATE` exists in the parameter file, `POLICY.IPT_RATE` exists as a column, neither is used.
- 3% orphans per month are written to a file and never re-processed; the quarantine table preserves that behaviour.
- `SYSDATE` makes the unit non-idempotent across month boundaries: a parallel run must execute both engines on the
  same calendar day (SKILL.md row 29, trap 7).

## Not verified live

- `Enable high precision` for `s_m_BILLING_PREMIUM_RECON` (drives the `decimal_round` decision above).
- Source/target table and column names; the target table is not in the export at all.
- `LKP_APF_ACCOUNTS` lookup condition and multiple-match policy (only the description states the join).
- Control-M WD1 calendar definition (which holidays) - needed to build the guard task's calendar table.
