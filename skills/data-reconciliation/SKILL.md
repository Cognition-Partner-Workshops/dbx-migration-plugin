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
| 1-3 | As above, but rows newer than the target's applied CDC watermark are *in flight*: a count gap within the in-flight count, and in-flight keys, are not defects. The allowance runs one way: inserts and updates the target has not applied yet. Deletes leave no row to carry a watermark, so a target row with no source row is never in flight unless the object declares `delete_evidence` and the source's change stream vouches for the delete (below); a trusted in-flight delete then counts toward the tier 1 count gap like an in-flight insert. Tier 2 grades the *applied set* on both sides (source rows at or before the target's applied watermark against the target minus the in-flight keys, one statement each) so a drifted value in an old row is caught even when tier 3's sample never visits it; when the target adapter cannot exclude keys, or more in-flight keys than one statement carries (10 000 rows, or the bind-parameter budget divided by the key width on SQL Server/ODBC targets; Postgres/Lakebase binds one typed array per key column so only the row cap applies), the object is `aggregates_ungraded_in_flight` (a FAIL, never a silent skip; the aggregate is never split across statements). Watermarks compare as instants: an offset-aware value is read in UTC and a naive one *is* UTC (the SQL Server `datetime` -> `timestamptz` contract); a datetime watermark mapped to a numeric one is refused before the window opens | Same as the analytical tiers, on applied rows only. A target count above the source count is `root_count` regardless of lag. |
| 5 | PK set diff: equal-count key ranges compared by fingerprint (count, then sum and modular sum of squares of each key column and of the watermark, all as exact decimals) on each side, keys and per-key watermarks streamed only for ranges whose fingerprint differs; string/uuid keys have no portable digest, so every range streams and `stats.fingerprint` says so. Two moments see any one- or two-key change inside a range; `pk_set_stream_every_range: true` in the tolerances trades the fingerprint for a full key stream when a larger engineered substitution must be ruled out | `pk_missing_on_target` older than the watermark = lost change; `pk_extra_on_target` = unapplied delete or stray write. Without `delete_evidence` the harness cannot tell the two apart, so both fail; with it, a target-only key whose delete the source's change stream records *after* the target's applied position and no longer ago than `cdc_lag_max_s` is an `in_flight_delete` (listed in `stats.in_flight_deletes`, not a finding), anything else stays `pk_extra_on_target`. A key swapped for a stray in the same range is caught even though the count is unchanged. |
| 6 | CDC lag (source max watermark - target max watermark vs `cdc_lag_max_s` for datetime watermarks and for numbers whose mapping declares `watermark.unit` as `epoch_s`/`epoch_ms`/`epoch_us`; a `counter` (rowversion, version column) has no time meaning, so its difference is reported as `lag_units` and graded as unapplied rows against `cdc_in_flight_max_rows`; a number with no declared unit is `cdc_lag_ungraded`, never read as seconds), global ordering (`target_ahead_of_source`) and per-key ordering from the tier 5 stream: `row_ahead_of_source` (one applied row newer than its source row while the global max is not), `row_behind_applied_watermark` (a row older than its source row although the target has applied past it); with `delete_evidence`: `delete_lag_exceeded` (a recorded source delete still present on the target longer than `cdc_lag_max_s`; the key also fails tier 5), `delete_evidence_retention_gap` (the target's applied position is older than the oldest retained change, so deletes in between are unknowable and every target-only key is graded strictly), `delete_evidence_unusable` (declared but the capture retains nothing, the position is missing, or the position kinds cannot be ordered; strict grading applies). When the target's max watermark is carried by a row whose delete is in flight, that max says nothing about lag or order (the source row is gone, not replayed): `stats.target_max_from_in_flight_delete` is true, `lag_s` is null and the global comparison is skipped; per-key ordering still grades | Pipeline behind, replaying, or skipping changes; cutover cannot be scheduled. |
| 7 | Schema parity through the mapping, in both directions for constraints: PK, unique, FK, NOT NULL and CHECK predicates must match (`*_missing`: the target lets in what the source refuses; `*_extra`: the target refuses a write the legacy application makes today). CHECK predicates are compared on a canonical form, never on count alone: quoting and casts dropped, source columns renamed through the mapping, `IN` / `= ANY(ARRAY[...])` / an OR-chain of equalities folded to one sorted list, `len`/`char_length` and similar spellings unified, so `([Balance]>=(0))` on SQL Server is `CHECK ((balance >= (0)::numeric))` on Postgres. A source predicate with nothing left on the target to match is `check_constraint_missing`; when both sides have unmatched predicates (a dialect-specific function, CASE, LIKE, or genuinely different rules) the pair is `check_constraint_unverified` and blocks merge until a human compares them and records `accept_unverified_check_constraints: true`. A reader that can only count CHECKs falls back to `check_constraint_count_lower` / `_higher` and notes `stats.check_predicates_unverified`, index coverage one way (a longer target index covers a shorter source one; extra target indexes are free), and identity/sequence headroom (`sequence_behind_source`: the target's next value would collide with rows already loaded). Only constraints and indexes the engine enforces count: SQL Server FKs and CHECKs under `NOCHECK` (`is_disabled = 1`) and disabled, invalid or still-building indexes are ignored, and filtered/partial ones are listed under `stats.partial_indexes_unverified` for a manual check instead of being graded. Postgres expression indexes are kept as their key text (`lower(email)`, from `pg_get_indexdef`) with source column names rewritten through the mapping: a unique one is a constraint (`expression_unique_missing` / `expression_unique_extra`), a plain or partial one is coverage listed under `stats.expression_indexes_unverified`. A target constraint over a column outside the mapping, or an FK to a table outside the spec, is noted in stats, not graded. Identifiers are compared case-folded (SQL Server returns the DDL's casing, Postgres folds unquoted names), while stats keep each catalog's own spelling | Constraint or index dropped or added in conversion, or new inserts after cutover would fail. `accept_target_only_constraints: true` in the tolerances (a STOP A decision) demotes the `*_extra` findings to `stats.accepted_target_only_constraints`. |

Tiers 5-7 run even when tier 1 fails, so a FAIL names the keys, lag, and schema gaps rather
than just a count. The mapping needs `watermark` (source/target column, plus `unit` when the column is numeric)
and, for identity tables, `identity`; the tolerance record needs `cdc_lag_max_s` (and
`cdc_in_flight_max_rows` for counter watermarks) and optionally `pk_set_ranges`.
A table without a watermark is graded strictly (no in-flight allowance). Embedded arrays are
refused on a Lakebase target: map operational children as separate objects.

**Deletes must be drained before the run unless trusted delete evidence is available.** Current-row
watermarks carry no tombstone, so by default a source delete the target has not applied yet fails
tier 1 (`root_count`) and tier 5 (`pk_extra_on_target`) exactly like a stray write: run after the
CDC feed has applied every delete up to a quiet point and treat a persistent `pk_extra_on_target`
as a defect. An object whose source already has change capture on can declare where the
evidence lives instead:

```json
"delete_evidence": {"kind": "sqlserver_cdc", "capture": "raw_loans",
                    "applied_position": {"table": "cdc_checkpoint", "column": "applied_lsn",
                                         "where": "source_table = 'raw.loans'"}}
```

`kind` names the source mechanism (`sqlserver_cdc`: `cdc.fn_cdc_get_all_changes_<capture>`,
`__$operation = 1`, 10-byte LSN positions); `applied_position` is the target table and column the
feed writes its last applied source position to (a checkpoint row picked by `where`, or a landing
table whose newest position is taken; `bytea` for an LSN, an integer for a numeric version, any
other type refused). Positions are opaque and only ordered within one mechanism: an integer against
an LSN, or LSNs of different widths, is `delete_evidence_unusable`, never a guess. Three statements
per object, all inside the window (applied position on the target, `fn_cdc_get_min_lsn`/`max_lsn`
horizon and the delete rows after the applied position on the source), reported as the
`delete_evidence` cost line; `cost.delete_evidence_statements` carries the actuals. A scoped object
(`root_where`) gets its scope applied to the deleted row's before-image, so a delete outside the
scope never vouches for an in-scope target-only key; a capture that does not carry the scope
columns errors rather than widening. The factory never enables CDC:
`factory-doctor --mapping mapping.json --source-secret NAME` verifies the source has it on, every
declared capture exists and captures each mapped `key.source` column, and the identity can call
that capture's `fn_cdc_get_all_changes_<capture>` with the key columns and scope (one bounded
read-only probe per object; no `SELECT` on the `cdc` schema is required or checked). A red row is a
customer decision to record (or drop the block and drain), not a fix to apply.

`--target-catalog` for a Lakebase run is the branch *database* name and must appear in
`.migration/allowed_targets.json`; the adapter reads `current_database()` on connect and refuses
a DSN that lands anywhere else, so the allowlist binds the connection, not just the label.
Tolerance switches (`accept_marker_only_window`, `pk_set_stream_every_range`,
`accept_target_only_constraints`, `accept_unverified_check_constraints`) must be JSON booleans; `"false"`, `0` or `null` are rejected rather than coerced.
Tolerance bounds (`cdc_lag_max_s`, `numeric_abs_tol`, `aggregate_rel_tol`) must be finite,
non-negative JSON numbers: `NaN` would make every lag comparison false and silently pass tier 6,
so it, `Infinity`, negatives, booleans and numeric strings are refused at load.

`harness/examples/lakebase_rehearsal/` records the rehearsed SQL Server -> Postgres run (OLTP
tolerance record plus the findings each of four defect scripts must produce; the third plants
defects that keep every count and every max watermark unchanged). The fixture itself (mapping,
DDL, loader, defect scripts) lives in `ts-tsql-sybase-legacy-db/rehearsals/lakebase/`.

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
and the `lakebase_rehearsal/` OLTP tolerance record. Copy and edit; do not start from a blank file.

## Known traps (append per engagement)
- AVG on integers truncates on some legacy engines and returns decimal on Databricks. Use
  `decimal_round` with the places from the tolerance record.
- Collation-sensitive strings (case, trailing spaces) differ. Enable `collation_casefold` or
  `rstrip_spaces` per the tolerance record only.
- CHAR columns pad with spaces on the source. `rstrip_spaces` is almost always needed.
