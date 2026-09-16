---
name: data-reconciliation
description: Run the reconciliation harness that gates every Databricks migration unit, wave, parallel-run cycle, and cutover. Use whenever a migration PR needs its recon verdict, a wave needs independent re-verification, or cutover needs final evidence. The harness verdict is the merge authority; nothing else self-certifies. Also owns live-mode source access (Lakehouse Federation) and the materialize load-posture table.
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
  --mode fixture|live|snapshot|continuous|transactional|structural \
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
required so both sides have the same scope. A target table written by units in more than one
wave declares `scope_columns` on the object (and on an embed that reads it) and is read with a
`target_where` pinning one of them to the unit's own partition or run date; the fan-out workflow
refuses to launch otherwise (rule in `skills/migration-fanout/SKILL.md`).

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
- `structural`: Tier 0 `structural_parity` only, both catalogs read and no row read on either
  side — the identity frontier-vs-rows collision check needs source MIN/MAX, so it is not checked
  here (`source_bounds` reports `unread`). The independent verifier's run on a wave the manifest declares `degraded`; never merge
  evidence: a clean run's `merge_block_reasons` is exactly `["mode"]` (no rerun proof applies, no
  row tier ran), and `estimate` counts only Tier 0's catalog statements per adapter
  (`CATALOG_STATEMENTS`), so `--family` is required in structural mode.



### Legal combinations

- `--mode transactional` (operational track, Lakebase target only; refused for `--target-kind databricks`) wraps tiers 1-3 in a consistency window and adds the tiers an OLTP target needs.
- Tier 0 `structural_parity` runs first in every mode except `continuous`/`transactional`, and alone in `structural` (which
  get the same comparison as tier 7 `schema_parity`): primary keys, uniques, foreign keys,
  not-nulls, checks, indexes, triggers (by timing+event, names ignored), identity columns, and
  grants (source grantees mapped through the spec's `principal_map` before comparing). The
  tier's `stats.structural_checks` records each category as `checked`, `direct_only` (grants:
  direct object grants only; role-inherited and schema/database-scope grants are not expanded),
  or `unsupported`, and `stats.structural_diff` the per-object detail; an unsupported category
  is unchecked, not
  clean. `result.json`'s `merge_block_reasons` puts `structural_gap` first when a structural
  tier has findings or unverifiable objects. `--source-dictionary`/`--target-dictionary`
  substitute a fixture JSON (`harness/fixtures/example_<family>/dictionary.json` shows the
  shape per family) for the live catalog read; structure proven from a fixture never merges.
- Rerun proof (schema evolution): `dbx-recon rerun-proof --unit <id> --source <job file>... --prior-proof
  <last committed rerun_proof.json> --fresh <record> --evolved <record> --out <dir>` grades the idempotency job twice
  from two run records (`dbx-recon shape` reads a target's observed columns, read-only; it refuses a
  table the target does not have rather than recording it empty). The catalog is the source of truth,
  never the DDL: the shape the fresh run landed is the expected shape and the evolved run, against the
  table pre-created in its previous committed shape, must land the identical one (`harness/fixtures/example_rerun/`
  is the canonical failing case, a `CREATE TABLE IF NOT EXISTS` that never lands the new column).
  The previous shape is the last committed proof's observed `shape` (`--prior-proof`; it must be this
  unit's proof and its shape must still match its `shape_digest`), or on a unit's first run the
  manifest-declared old shape (`--prior-shape`). `rerun_proof.json` carries
  `{fresh: pass|fail, evolved: pass|fail|unsupported, findings, shape, shape_digest, source_digest}`;
  without an evolved record, without a prior, when the pre-created shape equals the fresh one or
  differs from the prior one, or when the fresh leg failed, `evolved` is `unsupported` with the reason,
  never clean; a reordered column is a `column_order` finding; a fresh run that recorded no table fails
  (`no_tables`). `--ddl` is a hint only: tables its `CREATE TABLE` statements name that the fresh run did
  not record become notes, never findings. `run --rerun-proof <file> --rerun-source <job file>...` copies
  it into `result.json` after checking its `source_digest` against the job's files as committed now (any
  edit to the DDL, notebook or SQL makes the proof stale and refused); a
  failed leg adds `rerun_gap` to `merge_block_reasons`, an unsupported evolved leg adds
  `rerun_unsupported`, a `run` without `--rerun-proof` adds `rerun_missing` (every migrated unit
  writes its tables, so no proof is a missing control), and any of them sets `merge_eligible=false`.
- Fixture shape (wave 0): `dbx-recon fixture-shape --family <f> --mapping <spec>
  --source-dsn-secret <NAME> --fixture-dsn-secret <NAME> --source-statement-cap <n> --out <dir>`
  compares the fixture copy with the real source per mapped table: column names, types (after
  the rerun proof's normalisation), nullability, and a sample cardinality (distinct count and
  null rate per mapped column, within the object's `root_where`), not just that the table exists.
  Mapped columns are the object's comparison keys plus its fields; two objects on one root table
  are each checked.
  `fixture_shape.json` carries
  `{status: pass|fail|unsupported, findings: [{table, check, column?, detail}], tables,
  source_statements}`; checks are `table_missing`, `column_missing`, `column_extra`,
  `type_mismatch`, `nullable_mismatch`, `empty_fixture`, `cardinality_collapsed`,
  `null_profile`. Source reads are catalog queries plus one profile statement per column,
  read-only and counted against the cap on the adapter's statement counter: shapes first, then
  cardinality until the cap; a table past it, whose shape either side could not read, or whose
  source scope has no rows to profile, is `unsupported` with the reason, never clean. The wave 0 manifest declares it
  as a `custom` gate with this file as evidence; a `fail` is a listed finding the child reports as `failed`,
  so wave 0 does not close and wave 1 is not launched on an unproven fixture.
  `harness/fixtures/example_fixture_shape/` is the canonical gap (a column spelled differently,
  a loosened type, one status for every row).
- Tiers 5-7 run even when tier 1 fails, so a FAIL names the keys, lag, and schema gaps rather than just a count.
- A table without a watermark is graded strictly (no in-flight allowance).
- Embedded arrays are refused on a Lakebase target: map operational children as separate objects.
- Only PASS results in `live`, `snapshot`, or `transactional` mode have `merge_eligible=true`; fixture and continuous evidence never merges.
- `result.json` always names `merge_authority: {kind: harness, decision_id: null}`; the harness never writes `human_override`. Merging past `merge_eligible=false` is the workflow's `merge_authority` check against a `merge_override` row in `.migration/06_decisions.md` (see `migration-fanout`).

## Source access

### Live mode prerequisites (Lakehouse Federation)
Federation is a read-only live view of a legacy source through a connection plus a foreign catalog.
It is the default recon and coexistence bridge whenever a JDBC path exists.
1. Fire network path and private-link/VPN requirements as an early D10 dependency; confirm the engine is supported.
2. Put legacy credentials in a Databricks secret scope and reference scope/key names only.
3. Create `CREATE CONNECTION ... OPTIONS (... secret(...))` with the read-only legacy principal, never an admin login.
4. Create `CREATE FOREIGN CATALOG ... USING CONNECTION` and grant `USE` to the migration principal only.
5. Verify a trivial `SELECT COUNT(*)` on an in-scope table, record it in the access checklist, and reuse existing connections/catalogs rather than duplicating them.
- Federation is read-only by policy even where the engine allows writes; aggregates and filters push down, but wide row-level pulls do not, so large comparisons use the size tiers.
- Every federated query loads the legacy production engine and counts against the legacy-query concurrency cap in the tolerance record.
- Small-table backfill uses CTAS from the foreign catalog; federation is never the production consumer path.
- At STOP E consumers point at Delta in the target catalog; federation remains a recon and rollback bridge until decommission.
- If JDBC access is unavailable or security denies it, run DEGRADED recon from customer exports with `--mode snapshot` or an in-perimeter dual-run and record a D10; for an unsupported engine, use export/unload to cloud storage or JDBC via Spark with the same read-only principal.
- Lakeflow Connect keeps a copy current under continuous legacy writes; it is not a read-in-place substitute and its connector row is below.

### Load posture (materialize)
| Class | Signal | Method |
|---|---|---|
| Small and static | below the size threshold, no in-flight writes | CTAS through federation, single shot |
| Large and partitionable | above threshold, natural partition key (date, id range) | partitioned incremental copy with checkpointing and per-partition verification |
| Very large or mutable, engine has a Lakeflow Connect connector | SQL Server (CT/CDC gateway, GA), Postgres/MySQL CDC, or query-based Oracle/Teradata/SQL Server/PG/MySQL | managed initial snapshot plus continuous CDC into the migration catalog; connector is the watermark and catch-up |
| Very large or mutable, no connector | CDC-fed or continuously written | initial copy to a recorded watermark, then CDC catch-up via own extract or Auto Loader over exported change files, ordered against in-flight changes |
| Restricted | no live read access | customer-run export or in-perimeter execution; DEGRADED recon rules apply |
Prefer the connector row when the engine has one and its D10 prerequisites close in time; build it via `target-routing` -> `databricks-lakeflow-connect`, destination the batch's isolated schema in the migration catalog, schedule PAUSED until STOP E, and count query-based polling against the legacy-query cap.
The output is a machine-readable table of object, class, method, partition key, watermark rule, verification rule, and projected legacy-side cost, attached to the plan and each unit handoff.
- Checkpoint each partition in a load ledger with id, row count, and aggregate checksum; resume from it, and drop and recopy any unverified partial partition.
- Verify each partition against the source at copy time with this harness's aggregate check.
- Share partition-copy parallelism with the legacy-query cap used by recon.
- Run loads as checkpointed Databricks jobs: launch, record the run id in the ledger, verify later, and never poll in-session.
- Customer DBA/platform owns CT/CDC enablement, connector DB user, and gateway path as D10 entries; never run `ALTER DATABASE` or `sp_cdc_enable_table`, and fall back to the hand-rolled row if prerequisites are open at wave launch.
- Reconcile connector-fed tables at a recorded Tier 1/2 snapshot; CDC lag is a stated finding, and connector output never self-certifies.
- For mutable tables without a connector, record the exact timestamp/SCN/LSN at copy start and re-verify catch-up in the cutover runbook; if no usable watermark exists and no freeze is possible, say so in the plan.

## Routine parity (writing routines)

A converted routine that writes, itself or through a routine it `calls` (its `dependencies.json` row,
transitively), is proven only by one committed run on a dedicated execution target (Lakebase branch
`mig-<pipeline>-exec`; Unity Catalog schema `<catalog>.<pipeline>_exec`; never the migration target itself)
against a committed fixture snapshot (`snapshot: "fixture:<path in the repo>"`; anything else, a production
or ad hoc snapshot, is `unproven`), with the rows it left in every written table compared to a golden set.
The run record and the snapshot must both be files in the committed tree (`--repo`, default the current
directory), byte-identical to `HEAD`; a run naming a file that is untracked, staged, edited or missing is
`unproven`. The record is its own evidence: `evidence` is the record's path in the repository, and a
record read from any other path (or from outside the repository) is `unproven`, so `--runs` cannot borrow
some other committed file. Record each run as `<routine>.run.json`
(`{routine, target_family, target_branch, snapshot, evidence, golden: {table: [rows]}, observed: {table: [rows]}}`;
fixture: `harness/fixtures/example_routine_parity/`, laid out like a unit's repository) and grade them:

```bash
dbx-recon routine-parity --dependencies .migration/units/<unit>/dependencies.json \
  --runs .migration/recon/<unit>/runs/ --out .migration/recon/<unit>/
```

`routine_parity.json` carries `routine_parity: [{routine, status: proven|unproven|failed, evidence}]`. A
routine with no run, a run off a dedicated target, without evidence or a snapshot, or in a family with no
dedicated-target rule is `unproven` (exit 2), never silently clean; rows that differ, or a written table
absent from either set, are `failed` (exit 1); table names compare case-insensitively. Pass the file to
`run --routine-parity <file>` so `result.json` carries it. `run` reads the unit's dependency analysis
(`.migration/units/<unit>/dependencies.json`, or `--routine-dependencies`), which must be a committed file
of the repository (an outside, untracked or edited file is refused; `result.json` records the path as
`routine_dependencies`), and regrades every row that names evidence from the committed run record: a
`proven`/`failed` claim the run does not support is refused, an unreadable or uncommitted record is
`unproven`, and an `unproven` row whose run grades `failed` is carried as failed. A writing routine the
file lacks is carried as `unproven`; a row for a routine the analysis does not know, or a routine listed twice, is refused. A `failed` routine sets
`merge_eligible=false` with reason `routine_gap`; `unproven` routines are listed in `recon.summary.md` and
become cutover exceptions (`8-cutover_signoff.md`). No analysis, or writers with no parity file, is
`merge_eligible=false` with reason `routine_parity_missing` (only an analysis with zero writers needs no file):
absent parity is never clean parity. The run itself
needs the read-only principal to hold EXECUTE on the routines under test; the intake asks (`14-front_door_oltp.md`).

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
   The same file's `type_map.<family>.<target_kind>` is applied when the spec loads (`--target-kind`
   selects the table; `estimate --family/--canonicalization` applies it too, so the plan counts
   the statements the run will issue): empty `target_type`s are
   filled from it, a declared type it forbids — including a `conditional` alternative the
   field's `evidence` list does not carry (the map names the token, e.g. `census_fits_int64`,
   `census_midnight_only`) — stops the run before any query (`type map:` on
   stderr), and `result.json` records the outcome under `type_map` (`null` when the family has
   no map, so unaudited is never mistaken for clean).
3. On FAIL: read `report.md`, fix converted code or the load only. Never touch the source.
   Never change `03_tolerances.json` (that needs a new STOP A approval).
4. Paste `recon.summary.md` into the PR body, link `result.json` and `report.md`. Never paste
   the JSON.
5. Validate `--param` values against the unit brief before running. A parameter is a scope
   choice and must already exist in `.migration/` state.


## References

- [Checks and tier details](references/checks.md) — load when diagnosing tier behavior or verifier findings.
- [Example inputs and known traps](references/traps.md) — load when preparing a new engagement or debugging dialect-specific evidence.
