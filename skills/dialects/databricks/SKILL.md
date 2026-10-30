---
name: databricks
description: Source-dialect skill for a Databricks-to-Databricks estate (`policy.source.kind: existing` — the source is already a Unity Catalog or hive_metastore catalog, so there is no engine translation; the work is relocation, isolation and parity). Load it when `policy.source.family` is `databricks`: for the analyze census, slice conversion, or recon input authoring.
---

# Databricks dialect (kind: existing)

Same engine both sides: no language translation, so this skill is thin. The source catalog is
read through `source.access` like any other family, and it is listed in
`policy.forbidden_catalogs` because the guard has no lexical handle on a same-platform source —
the read-only fence is the migration principal's `SELECT`-only grants, proven by the doctor's
`source_read_only` and `isolation` checks.

- Lakebridge: no `--source-dialect` value; do not run the transpiler. "Conversion" is
  relocating objects: re-point catalog/schema references, clone DDL, and re-declare jobs and
  pipelines against the target namespaces.
- Canonicalization: no `canonicalization.json` ships yet; same-engine comparison runs `identity`
  plus the rules the traps table names where session settings differ
  (`datetime_utc_truncate_ms`, `collation_casefold`, `uuid_normalize`).

## Dependency scan

Walk every routine in scope (procedures, SQL functions, views, DLT/pipeline datasets, job tasks,
notebook-extracted SQL); names are already three-part `catalog.schema.object`; output one map
keyed by routine to `.migration/recon/<slice>/inputs/dependencies.json`:

```json
{"<schema>.<routine>": {"writes": ["<schema>.<table>"], "reads": ["<schema>.<table>"]}}
```

`writes` = targets of `INSERT`/`UPDATE`/`DELETE`/`MERGE`/`CREATE OR REPLACE TABLE`/`TRUNCATE`;
`reads` = tables and views in `SELECT`/`FROM`/`JOIN`/pipeline dataset dependencies. Both lists
are the transitive closure over the call graph: a routine's row carries what its callees write
and read. Every callee (including notebook jobs whose SQL is extractable) needs its own row, or
`slices.py --check` fails the slice proposal.

## Known traps with recon signature

| Trap | Source vs target | Recon signature | Fix / canonicalization |
|---|---|---|---|
| `TIMESTAMP` session-TZ semantics | `TIMESTAMP` is an instant read in session TZ; `TIMESTAMP_NTZ` a wall time | Tier 3 constant offset after a TZ or workspace change | pin session TZ on both sides; `datetime_utc_truncate_ms` |
| Column/identifier case | case-insensitive compare, case-preserving display; a hand-written lowercase port renames | Tier 4 column-name diffs | keep source spelling; alias at the boundary, never rename |
| Non-UTC `TIMESTAMP_NTZ` written by an older pipeline | wall time baked at write, no instant | Tier 3 skew on one column only | tolerance record names the write TZ; recompute in UTC on the target |
| Collation (`UTF8_LCASE` columns) | equality depends on declared collation | Tier 2 distinct-count drift if the target column loses the collation | carry `COLLATE` in the DDL; `collation_casefold` |
| `GENERATED ALWAYS AS IDENTITY` / auto-increment | surrogate values never match across a re-creation | Tier 3 on surrogate joins | recon on business keys; reseed `START WITH` past the max on the target |
| Streaming tables / MVs as source objects | the object is a live computation, not stored rows | parity compares a moving target | recon against a snapshot (CTAS) or land `mode: snapshot`; freeze the watermark in the spec |
| `hive_metastore` objects | no UC lineage, HMS types (`decimal` unbounded, `string` non-UTF8 edge cases) | Tier 1/3 on exotic columns | census the HMS types explicitly; external locations stay in place or move via volumes |
| Workspace-internal references (mounts, `dbfs:`, job cluster policies) | paths/policies do not exist in the target workspace | deploy fails or reads the wrong volume | remap mounts to volumes; job policies declared in the bundle |
| Row filters / column masks on source | the read principal sees a filtered view | Tier 1 shortfall, Tier 3 masked vs unmasked | recon with a principal that bypasses filters, or record the policy in the tolerance record |
| View chains into other catalogs | a slice's "table" is a view over a foreign catalog | Tier 1 drift when the foreign side moves | foreign objects are separate census objects; never fold into a slice silently |
