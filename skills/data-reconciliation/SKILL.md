---
name: data-reconciliation
description: Run the reconciliation harness that gates every Databricks migration unit, wave, parallel-run cycle, and cutover. Use whenever a migration PR needs its recon verdict, a wave needs independent re-verification, or cutover needs final evidence. The harness verdict is the merge authority; nothing else self-certifies.
---

# data-reconciliation

You run the harness in `harness/`. You do not write recon SQL by hand, you do not
reimplement its checks, and you never edit its verdict.

## Run it

```bash
cd skills/data-reconciliation/harness && pip install -e ".[databricks,<source family>]"   # once
dbx-recon selftest                                                                         # proves the install

dbx-recon run \
  --unit <unit_id> \
  --family sqlserver|postgres|databricks \
  --mapping .migration/units/<unit_id>/mapping_spec.json \
  --tolerances .migration/03_recon_tolerances.json \
  --canonicalization skills/<source>-sql/canonicalization.json \
  --mode fixture|live|snapshot|continuous|transactional \
  --source-dsn-secret <SOURCE_SECRET_NAME> \
  --target-kind databricks|lakebase \
  --target-secret DATABRICKS_MIGRATION_SQL --target-catalog <migration catalog> \
  --allowed-targets-file .migration/allowed_targets.json --target-schema <schema> \
  --snapshot-manifest .migration/snapshots/<unit_id>.json \
  --seed 0 --depth threshold|sampled|full [--param from_date=2024-01-01 ...] \
  --out .migration/recon/<unit_id>/
```

Exit code 0 is PASS, 1 is FAIL. `--depth` sets Tier 3: `threshold` (default; the tolerance
file's `full_diff_row_threshold` decides per table), `sampled` (always the stratified sample; the
independent verifier's default, with a different `--seed` from the child's), `full` (always the
keyed full diff; what the wave manifest's `verify_depth: full` pins for cutover-critical units).
The depth is recorded in `result.json` and the summary.

`--family` names the source adapter. Only families rehearsed against a real engine may run:

| Family | Status |
|---|---|
| `sqlserver` (pyodbc, `[sqlserver]` extra) | live-tested: SQL Server 2022 -> Lakebase rehearsal |
| `postgres` (psycopg, `[lakebase]` extra) | live-tested: Postgres source and Lakebase target |
| `databricks` (`[databricks]` extra) | live-tested: Delta target and Databricks-to-Databricks source |
| `redshift`, `snowflake`, `teradata`, `oracle` | **untested**: the adapter raises `NotImplementedError("<family> source adapter is untested; see SKILL.md")` before any connection. Reconcile these through Lakehouse Federation (`--family databricks`) or land an adapter with a rehearsal first. |

```bash
dbx-recon estimate --mapping <mapping_spec.json> --tolerances .migration/03_recon_tolerances.json \
  --depth sampled --row-counts <{root_table: rows}.json> [--ops-count N]
```

opens no connection and prints statements per side per tier, rows that will cross the wire, and
which Tier 3 mode each table lands in; the plan playbook sums it per wave for the STOP C cost
line. After a run, `result.json["cost"]` holds the actuals (statements, rows fetched per side,
elapsed seconds) so the next estimate is corrected from measurement.

Secrets are passed by NAME; the harness reads them from the environment. Never inline a
connection string or token.

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

## Modes

- `fixture`: the source secret points at a small fixture copy. Use for every development and
  fix round. A fixture PASS is stamped "NOT a merge verdict" in every output.
- `live`: the real read-only source, inside the one live window the parent granted this unit.
- `snapshot`: source is a frozen extract; every PASS is scoped to the snapshot watermark.
- `continuous`: Tier 1+2 plus a sampled Tier 3, appended per cycle during parallel-run.
- `transactional`: both sides live under a consistency window, Lakebase target only. A PASS is
  scoped to the window that held and the target's applied CDC watermark; the summary names the
  isolation each side actually ran under.



### Legal combinations

- `--mode transactional` (operational track, Lakebase target only; refused for `--target-kind databricks`) wraps tiers 1-3 in a consistency window and adds the tiers an OLTP target needs.
- Tiers 5-7 run even when tier 1 fails, so a FAIL names the keys, lag, and schema gaps rather than just a count.
- A table without a watermark is graded strictly (no in-flight allowance).
- Embedded arrays are refused on a Lakebase target: map operational children as separate objects.
- Only PASS results in `live`, `snapshot`, or `transactional` mode have `merge_eligible=true`; fixture and continuous evidence never merges.
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


## References

- [Checks and tier details](references/checks.md) — load when diagnosing tier behavior or verifier findings.
- [Example inputs and known traps](references/traps.md) — load when preparing a new engagement or debugging dialect-specific evidence.
