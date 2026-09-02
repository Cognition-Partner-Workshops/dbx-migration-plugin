---
name: data-reconciliation
description: Run the reconciliation harness that gates every Databricks migration unit, wave, parallel-run cycle, and cutover. Use whenever a migration PR needs its recon verdict, a wave needs independent re-verification, or cutover needs final evidence. The harness verdict is the merge authority; nothing else self-certifies.
---

# data-reconciliation

You run the harness in `harness/`. You do not write recon SQL by hand, you do not
reimplement its checks, and you never edit its verdict.

## Why a tool and not a procedure

Every child used to hand-write its own count and aggregate queries. That is where review
rounds, false halts, and "green against my own rows" came from. The harness makes every unit
produce the same `result.json`, so the workflow script and the wave gate can read a verdict
instead of a paragraph.

## Run it

```bash
cd skills/data-reconciliation/harness && pip install -e ".[databricks,<source family>]"   # once
dbx-recon selftest                                                                         # proves the install

dbx-recon run \
  --unit <unit_id> \
  --family redshift|snowflake|teradata|oracle|sqlserver|databricks \
  --mapping .migration/units/<unit_id>/mapping_spec.json \
  --tolerances .migration/03_recon_tolerances.json \
  --canonicalization skills/<source>-sql/canonicalization.json \
  --mode fixture|live|snapshot|continuous \
  --source-dsn-secret <SOURCE_SECRET_NAME> \
  --target-secret DATABRICKS_MIGRATION_SQL --target-catalog <migration catalog> \
  --allowed-targets-file .migration/allowed_targets.json --target-schema <schema> \
  --snapshot-manifest .migration/snapshots/<unit_id>.json \
  --seed 0 [--param from_date=2024-01-01 ...] \
  --out .migration/recon/<unit_id>/
```

Exit code 0 is PASS, 1 is FAIL. Secrets are passed by NAME; the harness reads them from the
environment. Never inline a connection string or token.

`--param name=value` (repeatable) fills `${name}` placeholders in the mapping spec's
`root_where`/`target_where`. Values are validated before any database adapter is constructed.
Unresolved placeholders are refused, and mapping identifiers are validated before execution.
When `root_where` or an embed's `child_where` is set, the corresponding `target_where` is
required so both sides have the same scope.

The allowlist file is trusted setup state, not a caller-controlled CLI list. Use
`--snapshot-manifest` for snapshot mode; it must live under `.migration/snapshots/` and record
`source` (equal to `--family`), `extracted_at`, and `row_counts` keyed by root table. The
harness checks those counts against what Tier 1 observed on the source; any mismatch is a
provenance warning and the run is not merge-eligible.

## What it checks, in cost order

| Tier | Check | What a FAIL means |
|---|---|---|
| 1 | Row counts, source table vs target table (through the mapping, with `root_where`) | Load defect or wrong scope. Nothing else runs. |
| 2 | Per-column aggregates: null rate, min, max, distinct count, sum on numeric columns | Type or conversion drift. |
| 3 | Keyed row diff: full below `full_diff_row_threshold`, seeded stratified sample above | Value-level mismatch; findings name the key and column. |
| 4 | Replay of recorded representative queries on both engines (optional, `--ops`) | Report or extract does not match. SQL ops execute read-only and only `SELECT`/`WITH` queries are allowed. |

Aggregates run natively on each engine, so nothing bulky crosses the wire. Comparisons happen
after the canonicalization rules (trailing spaces, decimal rounding, timestamp precision,
null vs empty string) are applied to BOTH sides.

## Modes

- `fixture`: the source secret points at a small fixture copy. Use for every development and
  fix round. A fixture PASS is stamped "NOT a merge verdict" in every output.
- `live`: the real read-only source, inside the one live window the parent granted this unit.
- `snapshot`: source is a frozen extract; every PASS is scoped to the snapshot watermark.
- `continuous`: Tier 1+2 plus a sampled Tier 3, appended per cycle during parallel-run.

Only PASS results in `live` or `snapshot` mode have `merge_eligible=true`; fixture and
continuous evidence never merges.

## Outputs (in `--out`)

- `result.json`: machine-readable. The workflow gates on `verdict`, and the wave gate reads
  `tiers[*].findings`.
- `report.md`: full human report, read at wave close.
- `recon.summary.md`: about 30 lines, pasted into the unit PR. It cites mode, mapping version,
  tolerance version, and seed, so anyone can re-run it.

## Your obligations

1. Fixture first, one live run. Develop against `--mode fixture`. When it is green, run
   `--mode live` exactly once inside the granted window. If live fails, go back to fixture.
2. Pass the source-dialect skill's canonicalization JSON verbatim. If a rule is missing, that
   goes in the PR under "Skill feedback" for the parent to fold into the dialect skill, not an ad-hoc patch.
3. On FAIL: read `report.md`, fix converted code or the load only. Never touch the source.
   Never change `03_tolerances.json` (that needs a new STOP A approval).
4. Paste `recon.summary.md` into the PR body, link `result.json` and `report.md`. Never paste
   the JSON.
5. Validate `--param` values against the unit brief before running. A parameter is a scope
   choice and must already exist in `.migration/` state.

## Example inputs

`harness/examples/` has a mapping spec, a tolerance record, and Redshift canonicalization
rules. Copy and edit; do not start from a blank file.

## Known traps (append per engagement)
- AVG on integers truncates on some legacy engines and returns decimal on Databricks. Use
  `decimal_round` with the places from the tolerance record.
- Collation-sensitive strings (case, trailing spaces) differ. Enable `collation_casefold` or
  `rstrip_spaces` per the tolerance record only.
- CHAR columns pad with spaces on the source. `rstrip_spaces` is almost always needed.
