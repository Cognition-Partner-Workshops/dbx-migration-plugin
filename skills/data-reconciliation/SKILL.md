---
name: data-reconciliation
description: Dual-run data reconciliation harness for Databricks migrations. Use whenever comparing migrated Databricks tables or query outputs against a legacy source (via Lakehouse Federation, dual-run, or snapshots), or when producing recon evidence for a migration PR.
---

# Data Reconciliation Harness

Compare legacy and migrated outputs and emit gate-ready evidence. Every run cites the engagement tolerance record (`.migration/03_recon_tolerances.md`) and states the recon mode (LIVE or DEGRADED) in its header.

## Checks, in order
1. **Row counts**: exact match unless the tolerance record says otherwise.
2. **Per-column aggregates**: COUNT, COUNT(DISTINCT), SUM/AVG/MIN/MAX for numerics (apply the numeric tolerance), MIN/MAX for timestamps and strings, null counts. Aggregates run on the source engine side (pushdown) so they scale to any table size.
3. **Row-level diffs**: keyed FULL OUTER JOIN comparison. Below the size threshold from the tolerance record, diff all rows; above it, keyed stratified sampling plus the full aggregates, with the sampling rule recorded in the evidence.
4. **Duplicates and boundaries**: duplicate-key check both sides; compare first/last rows by key order.
5. **Report outputs**: for report/extract queries, compare complete result sets, normalizing only what the tolerance record permits (e.g. unordered results sorted by declared key).

## Rules
- **Fixture-first, one window**: development and fix rounds run `run_mode: fixture`; live runs happen only in the declared live budget (typically the parent's single uncontended window per wave). Batch the battery into a handful of multi-metric statements (one aggregate statement per table, not one per metric) and execute it in one warehouse window per run; scattered single-metric queries pay per-statement overhead plus an auto-stop idle tail per burst.
- **Declared source-volume assertion**: every table's evidence asserts the row volume the source actually holds against what was processed; green against self-generated rows is invalid by construction.
- Numeric comparison uses the tolerance math from the record, never ad hoc rounding. Timestamp comparison respects the agreed timezone/precision rule.
- Respect the legacy-query concurrency cap from the tolerance record when running checks in parallel.
- In DEGRADED mode, every comparison names its snapshot manifest (source, extraction time, row count) and every verdict carries the caveat that it is a statement about the snapshot, not production.
- On a mismatch: capture the failing evidence first, diagnose root cause, fix converted code only, re-run to green. Never adjust a tolerance to pass; tolerance changes go to the user.

## Evidence format (goes in the PR)
Per object: mode, tolerance record version, each check with legacy value / Databricks value / verdict, any FAIL with diagnosis and fix commit, and the final green run's timestamp. A recon report without the tolerance version is invalid.

Evidence is tiered: the full machine-readable JSON is committed, and a ~30-line `recon.summary.md` beside it carries what a human reads (money lines with quarantine counts, idempotency proof, declared populations, halt basis). The PR renders the summary and links the JSON; never paste a large JSON into a PR body.

## Known traps (append per engagement)
- AVG on integers truncates on some legacy engines and returns decimal on Databricks; the rounding rule must come from the tolerance record.
- NULL ordering differs across engines; sort by key with explicit NULLS FIRST/LAST before diffing.
- Collation-sensitive string comparisons (case, trailing spaces) differ; normalize per the tolerance record only.
