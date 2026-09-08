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

## What it checks, in cost order

| Tier | Check | What a FAIL means |
|---|---|---|
| 1 | Row counts, source table vs target table (through the mapping, with `root_where`) | Load defect or wrong scope. Nothing else runs. |
| 2 | Per-column aggregates (null rate, min, max, distinct count, sum on numeric columns), one statement per table per side | Type or conversion drift. |
| 3 | Keyed row diff: full below `full_diff_row_threshold`, else server-side stratified sample (equal-count key ranges, seeded positions inside each, every range's first and last key, plus a duplicate-key probe), overridable with `--depth` | Value-level mismatch; findings name the key and column. |
| 4 | Replay of recorded representative queries on both engines (optional, `--ops`) | Report or extract does not match. SQL ops execute read-only and only `SELECT`/`WITH` queries are allowed. |

`--mode transactional` (operational track, Lakebase target only; refused for `--target-kind
databricks`) wraps tiers 1-3 in a consistency window and adds the tiers an OLTP target needs:

| Tier | Check | What a FAIL means |
|---|---|---|
| 0 | Consistency window: both sides pinned (SQL Server `SNAPSHOT` / Postgres `REPEATABLE READ`), open and close markers (count, max watermark) compared. A side whose engine refuses the snapshot records `isolation: none` and its window strength: `change_token` when the engine exposes a per-table write counter (SQL Server `sys.dm_db_index_usage_stats.user_updates`, read before and after the count/max row and accepted only when both reads agree, so a write between the two never enters the baseline; three unsettled brackets record both tokens, which no later marker can equal), else `markers` | `window_unstable`: a side moved during the run; nothing graded in between is evidence. `window_unproven`: a side ran on markers alone, which cannot see an update below the max watermark or a balanced insert+delete, so the run is not merge-eligible unless the tolerance record carries `accept_marker_only_window: true` (a STOP A decision). |
| 1-3 | As above, but rows newer than the target's applied CDC watermark are *in flight*: a count gap within the in-flight count, and in-flight keys, are not defects. The allowance runs one way: inserts and updates the target has not applied yet. Deletes leave no row to carry a watermark, so a target row with no source row is never in flight | Same as the analytical tiers, on applied rows only. A target count above the source count is `root_count` regardless of lag. |
| 5 | PK set diff: equal-count key ranges compared by fingerprint (count, then sum and modular sum of squares of each key column and of the watermark, all as exact decimals) on each side, keys and per-key watermarks streamed only for ranges whose fingerprint differs; string/uuid keys have no portable digest, so every range streams and `stats.fingerprint` says so. Two moments see any one- or two-key change inside a range; `pk_set_stream_every_range: true` in the tolerances trades the fingerprint for a full key stream when a larger engineered substitution must be ruled out | `pk_missing_on_target` older than the watermark = lost change; `pk_extra_on_target` = unapplied delete or stray write; the harness has no tombstone or CDC-position evidence to tell the two apart, so both fail. A key swapped for a stray in the same range is caught even though the count is unchanged. |
| 6 | CDC lag (source max watermark - target max watermark vs `cdc_lag_max_s`), global ordering (`target_ahead_of_source`) and per-key ordering from the tier 5 stream: `row_ahead_of_source` (one applied row newer than its source row while the global max is not), `row_behind_applied_watermark` (a row older than its source row although the target has applied past it) | Pipeline behind, replaying, or skipping changes; cutover cannot be scheduled. |
| 7 | Schema parity through the mapping, in both directions for constraints: PK, unique, FK, NOT NULL and CHECK count must match (`*_missing` / `check_constraint_count_lower`: the target lets in what the source refuses; `*_extra` / `check_constraint_count_higher`: the target refuses a write the legacy application makes today), index coverage one way (a longer target index covers a shorter source one; extra target indexes are free), and identity/sequence headroom (`sequence_behind_source`: the target's next value would collide with rows already loaded). Only indexes the engine enforces over the whole table count: disabled, invalid or still-building ones are ignored, and filtered/partial ones are listed under `stats.partial_indexes_unverified` for a manual check instead of being graded. A target constraint over a column outside the mapping, or an FK to a table outside the spec, is noted in stats, not graded | Constraint or index dropped or added in conversion, or new inserts after cutover would fail. `accept_target_only_constraints: true` in the tolerances (a STOP A decision) demotes the `*_extra` findings to `stats.accepted_target_only_constraints`. |

Tiers 5-7 run even when tier 1 fails, so a FAIL names the keys, lag, and schema gaps rather
than just a count. The mapping needs `watermark` (source/target column) and, for identity
tables, `identity`; the tolerance record needs `cdc_lag_max_s` and optionally `pk_set_ranges`.
A table without a watermark is graded strictly (no in-flight allowance). Embedded arrays are
refused on a Lakebase target: map operational children as separate objects.

**Deletes must be drained before the run.** The in-flight allowance covers inserts and updates
only; a source delete the target has not applied yet fails tier 1 (`root_count`) and tier 5
(`pk_extra_on_target`) exactly like a stray write, because current-row watermarks carry no
tombstone. Run the recon after the CDC feed has applied every delete up to a quiet point, and
treat a persistent `pk_extra_on_target` as a defect.

`--target-catalog` for a Lakebase run is the branch *database* name and must appear in
`.migration/allowed_targets.json`; the adapter reads `current_database()` on connect and refuses
a DSN that lands anywhere else, so the allowlist binds the connection, not just the label.
Tolerance switches (`accept_marker_only_window`, `pk_set_stream_every_range`,
`accept_target_only_constraints`) must be JSON booleans; `"false"`, `0` or `null` are rejected rather than coerced.

`harness/examples/lakebase_rehearsal/` is the rehearsed SQL Server -> Postgres run (mapping,
tolerances, DDL, loader, and three defect scripts with the findings each one must produce; the
third plants defects that keep every count and every max watermark unchanged).

A tier or marker query that raises releases both windows before the error propagates; one
side failing to close never leaves the other pinned.

Aggregates and stratification run natively on each engine, so only chosen keys and their rows
cross the wire; an adapter without stratification support falls back to a streamed key
reservoir and says so in `stats.sampling`. Comparisons happen
after the canonicalization rules (trailing spaces, decimal rounding, timestamp precision,
null vs empty string) are applied to BOTH sides.

## Modes

- `fixture`: the source secret points at a small fixture copy. Use for every development and
  fix round. A fixture PASS is stamped "NOT a merge verdict" in every output.
- `live`: the real read-only source, inside the one live window the parent granted this unit.
- `snapshot`: source is a frozen extract; every PASS is scoped to the snapshot watermark.
- `continuous`: Tier 1+2 plus a sampled Tier 3, appended per cycle during parallel-run.
- `transactional`: both sides live under a consistency window, Lakebase target only. A PASS is
  scoped to the window that held and the target's applied CDC watermark; the summary names the
  isolation each side actually ran under.

Only PASS results in `live`, `snapshot`, or `transactional` mode have `merge_eligible=true`;
fixture and continuous evidence never merges.

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

`harness/examples/` has a mapping spec, a tolerance record, Redshift canonicalization rules,
and the `lakebase_rehearsal/` operational set (mapping with watermark/identity, OLTP
tolerances, target DDL, loader, defect scripts). Copy and edit; do not start from a blank file.

## Known traps (append per engagement)
- AVG on integers truncates on some legacy engines and returns decimal on Databricks. Use
  `decimal_round` with the places from the tolerance record.
- Collation-sensitive strings (case, trailing spaces) differ. Enable `collation_casefold` or
  `rstrip_spaces` per the tolerance record only.
- CHAR columns pad with spaces on the source. `rstrip_spaces` is almost always needed.
