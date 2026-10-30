---
name: postgres
description: Source-dialect skill for PostgreSQL estates (procedures/functions, triggers, sequences, partitioned tables, views, pg_cron or external schedulers). Load it when `policy.source.family` is `postgres`: for the analyze census, slice conversion (delta track DBSQL / lakebase track PL/pgSQL — the lowest-drama lift of the four core dialects), or recon input authoring.
---

# PostgreSQL dialect

Source-side only. Everything Databricks-side routes through `skills/target-routing/SKILL.md`;
never restate it. Track **A** is Delta/DBSQL, track **L** is Lakebase Postgres (same engine —
most objects carry over; the work is schema mapping and privilege review, not conversion).

- Lakebridge: `--source-dialect postgresql` — Switch (LLM transpiler) only, no deterministic
  transpiler; treat its output as a draft with the standing of hand conversion, proved by recon.
- Canonicalization: no `canonicalization.json` ships yet; the harness runs `identity` plus the
  rules the traps table names (`uuid_normalize`, `rstrip_spaces`, `datetime_utc_truncate_ms`,
  `collation_casefold`, `null_missing_equiv`). Create the file beside this SKILL.md with the
  first engagement's spec.

## Dependency scan

Walk every routine in scope (procedures, functions, trigger functions, scheduled job bodies);
resolve `search_path`-dependent names, synonyms do not exist (use views/functions) and case
(unquoted folds to lower); output one map keyed by routine to
`.migration/recon/<slice>/inputs/dependencies.json`:

```json
{"<schema>.<routine>": {"writes": ["<schema>.<table>"], "reads": ["<schema>.<table>"]}}
```

`writes` = targets of `INSERT`/`UPDATE`/`DELETE`/`MERGE`/`TRUNCATE`/`COPY ... TO` consumed as a
write; `reads` = tables and views in `SELECT`/`FROM`/`JOIN`/cursor bodies. Both lists are the
transitive closure over the call graph: a routine's row carries what its callees write and read.
Every callee (including trigger-fired functions and the scheduler entry point) needs its own
row, or `slices.py --check` fails the slice proposal.

## Known traps with recon signature

| Trap | Postgres vs target | Recon signature | Fix / canonicalization |
|---|---|---|---|
| `timestamptz` vs `timestamp` | `timestamptz` stores an instant, `timestamp` a wall time; target `TIMESTAMP` vs `TIMESTAMP_NTZ` | Tier 3 constant offset; Tier 2 `min()`/`max()` shifted | pin the session TZ on both sides; compare as UTC; `datetime_utc_truncate_ms` only where ms-truncation is lossless |
| `citext` / non-default collation | case-insensitive equality; Databricks is binary by default | Tier 2 distinct-count drift; joins fan out | `COLLATE UTF8_LCASE` on the column; `collation_casefold` only when the census shows it |
| `uuid` type | stored lowercase-hex; a `text` carry keeps mixed case | Tier 3 key mismatch | `uuid_normalize` |
| `bytea` | hex vs base64 rendering | Tier 3 on binary compare | compare `sha2(col,256)` on both sides; `identity` |
| `serial` / `GENERATED ... IDENTITY` | values assigned per writer; surrogate values never compare across engines | Tier 3 on surrogate joins if business keys missed | recon on business keys; L: `setval` after backfill |
| Arrays, `jsonb`, `hstore` | native vs `ARRAY`/`VARIANT`/`MAP` | Tier 3 key order / member order diffs | canonical `to_json` form both sides; `identity` |
| Advisory locks, `LISTEN`/`NOTIFY`, `pg_advisory_lock` queue jobs | session-scoped concurrency; no Delta equivalent | silent: passes tiers, races in prod | queue pattern → `for_each_task` / `max_concurrent_runs: 1`; flag as a finding, not a silent port |
| Rules / `INSTEAD OF` triggers on views | view writers rewritten to base tables | Tier 1 writer rows land on the wrong object | fold into the writer routine; L keeps native triggers |
| Partitioned tables | partition pruning invisible to recon | Tier 1 fine, Tier 4 consumer plans differ | partition-aware census; map to `liquid` clustering or keep partitions on L |
| `RETURNING`, `ON CONFLICT` | DBSQL `MERGE` covers `ON CONFLICT`; `RETURNING` has no equivalent | Tier 4 consumer diff | `MERGE` + post-state select; record the contract change |
| `dblink` / FDW / `postgres_fdw` | remote objects look local | Tier 1 drift when the foreign side moved | foreign tables are separate census objects, never folded into a slice silently |
| Extension functions (`pgcrypto`, `uuid-ossp`, PostGIS) | no equivalent or a different name | Tier 3 on derived columns | per-function map; PostGIS → `GEOGRAPHY`/`GEOMETRY` or a GAP |
