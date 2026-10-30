---
name: sqlserver
description: Source-dialect skill for SQL Server T-SQL estates (procedures, views, triggers, SQL Agent jobs, isql runners; Sybase ASE deltas marked where they differ). Load it when `policy.source.family` is `sqlserver`: for the analyze census, slice conversion (delta track DBSQL / lakebase track T-SQL to PL/pgSQL), or recon input authoring. SSIS packages are the `tsql-ssis` dialect in skills-extra, never core.
---

# SQL Server (T-SQL) dialect

Source-side only. Everything Databricks-side routes through `skills/target-routing/SKILL.md`;
never restate it. Track **A** is Delta/DBSQL, track **L** is Lakebase Postgres.

- Lakebridge: `--source-dialect mssql` — deterministic transpilers cover DML/DDL and common
  functions; `TRY/CATCH`, cursors, `sp_executesql`, triggers, `OUTPUT` clauses and linked-server
  names are hand work here. Sybase ASE has no flag: transpile as `mssql`, then apply the ASE rows
  by hand.
- Canonicalization: [canonicalization.json](../../../skills-extra/tsql-ssis/canonicalization.json)
  (loaded by `recon.config.load_canon_rules`); rules: `datetime_grid_333` only on recomputed
  `DATETIME` columns, `datetime_utc_truncate_ms` on `DATETIMEOFFSET(<=3)`, `collation_casefold`
  only when the census reports `_CI_`, `rstrip_spaces`, `decimal_round` places=4 half_up on
  `MONEY`-derived columns only, `uuid_normalize`, `null_missing_equiv`, `identity`.
- Full type/construct maps and canonical shape: `skills-extra/tsql-ssis/references/`.

## Dependency scan

Walk every routine in scope (procedures, functions, triggers, SQL Agent job steps); resolve
synonyms, linked-server four-part names and case per the database collation; output one map keyed
by routine to `.migration/recon/<slice>/inputs/dependencies.json`:

```json
{"<schema>.<routine>": {"writes": ["<schema>.<table>"], "reads": ["<schema>.<table>"]}}
```

`writes` = targets of `INSERT`/`UPDATE`/`DELETE`/`MERGE`/`TRUNCATE`/`SELECT INTO`; `reads` =
tables and views in `SELECT`/`FROM`/`JOIN`/`UPDATE ... FROM`/`MERGE USING`/cursor bodies. Both
lists are the transitive closure over the call graph: a routine's row carries what its callees
write and read. Every callee the scan names (including `sp_executesql` targets it can resolve
and the job step action) needs its own row, or `slices.py --check` fails the slice proposal.

## Known traps with recon signature

| Trap | Recon signature | Fix / canonicalization |
|---|---|---|
| ASE `*=` with inner-side `WHERE` predicate | Tier 1 count < source; null-rate on outer columns = 0 | predicate into `ON`, not `WHERE`; Lakebridge mangles `*=` — pre-rewrite to `LEFT`/`RIGHT JOIN` |
| `DATETIME` 1/300 s grid | Tier 3 on recomputed timestamps; Tier 2 `max()` drift ≤ 3 ms | loaded values `identity`; recomputed `datetime_grid_333` |
| CI collation equality/grouping (`_CI_AS`; ASE default is binary, check `sp_helpsort`) | Tier 2 distinct-count drift on string keys; Tier 1 on joins | `COLLATE UTF8_LCASE`, never `lower()` in joins; `collation_casefold` only on census-CI |
| Trailing-space padding (`CHAR(n)`, `ANSI_PADDING`) | Tier 2 distinct-count; joins fan out to 0 | `rtrim()` on load or `UTF8_BINARY_RTRIM`; `rstrip_spaces` |
| `ISNULL` type coercion | Tier 2 `max(length())` drift | reproduce the truncation or record it in the tolerance record |
| `@@ROWCOUNT` / `SET ROWCOUNT` batching | Tier 1 after the run; audit `record_count` ≠ rows updated | single `UPDATE` + `GET DIAGNOSTICS ROW_COUNT`; no pre-materialised list |
| Integer division / `AVG(INT)` | Tier 2 drift on ratio columns; one unit on negatives | `div`; `sum() div count()` |
| `MONEY` arithmetic | Tier 2 sums differ in the 5th+ decimal | `round(...,4)` at each source rounding step; `decimal_round` places=4 on those columns only |
| `DATEDIFF` boundaries | Tier 2 histogram drift on bucketed columns | rewrite per the construct map |
| `CONVERT` style codes (ASE default 100) | Tier 3 on formatted strings | pattern table in the construct map; `datetime_utc_truncate_ms` does not mask it |
| `@@identity` hijacked by triggers | Tier 3 on FKs populated from it | business-key read-back; `identity` |
| NULL concatenation (ASE `NULL + 'x'` = `'x'`) | Tier 2 null-rate rises on derived strings | `coalesce(x,'')` per operand; `null_missing_equiv` is not the fix |
| `SELECT @v = col` over N rows | procedure raises, or Tier 3 on the derived value | deterministic `ORDER BY ... LIMIT 1` |
| Parameter shadowing (`@x` → `x`) | Tier 1 count too high; wrong key in Tier 3 | `p_` prefix; alias every column |
| Multi-table transaction flattened to sequential DML | clean run passes every tier; failure-injection replays fail | `BEGIN ATOMIC` on catalog-managed tables, else compare-and-set; record the commit unit |
| `UNIQUEIDENTIFIER` case | Tier 3 100% key mismatch | lower-case on load; `uuid_normalize` |
| `DATETIME`/`DATETIMEOFFSET` zone | Tier 2 `min()`/`max()` shifted | land as `TIMESTAMP_NTZ`; tolerance record names the server zone |
