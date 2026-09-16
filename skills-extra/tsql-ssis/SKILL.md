---
name: tsql-ssis
description: Source-dialect skill for SQL Server T-SQL estates (with Sybase ASE deltas) and SSIS packages. Load it when converting T-SQL procedures, views, triggers, SQL Agent jobs, isql runners or .dtsx packages to Databricks SQL, Lakeflow Jobs and Lakeflow Spark Declarative Pipelines, or reconciling a T-SQL-sourced unit (analytical track and the Lakebase OLTP front door).
---

# T-SQL + SSIS Dialect (v1)

## When to use / routing

Source-side half of the factory for the Lakebridge `mssql` and `ssis` flags. Sybase ASE 16 is a sub-profile (Lakebridge has no ASE flag): apply the rows marked **ASE** by hand before and after the transpile. Fixture: `Cognition-Partner-Workshops/ts-tsql-sybase-legacy-db` (loan servicing; no SSIS, so the SSIS example is a hand-written `.dtsx`-shaped package).

Everything Databricks-side is a pointer through `skills/target-routing/SKILL.md` to the official plugin; this file never restates it:

| Need | Official skill / doc |
|---|---|
| SQL scripting, `CREATE PROCEDURE`, handlers, `SIGNAL`, `EXECUTE IMMEDIATE`, temp tables, collations, `MERGE` | `databricks-dbsql` (`references/sql-scripting.md`, `materialized-views-pipes.md` §2, `geospatial-collations.md` Part 2) |
| Task graphs (`depends_on`, `run_if`, If/else, `for_each_task`, `sql_task`, job parameters, retries) | `databricks-jobs` (`SKILL.md`, `references/task-types.md`, `notifications-monitoring.md`, `triggers-schedules.md`); docs `/jobs/parameter-use`, `/jobs/dynamic-value-references`, `/jobs/conditional-tasks` |
| Data flows as streaming tables / materialized views / expectations / Auto CDC | `databricks-pipelines` |
| `BEGIN ATOMIC`, transaction requirements, optimistic concurrency | docs `/transactions/`, `/transactions/transaction-modes` |
| Lakebase Postgres column types (`!dbx_migrate_oltp`) | `databricks-lakebase` `references/synced-tables.md` "Type mapping" |
| Grants, RLS, system tables | `databricks-unity-catalog` |
| Name resolution (column beats parameter/variable) | docs `sql-ref-name-resolution` |

## Canonicalization

`canonicalization.json` (loads with `recon.config.load_canon_rules`): `datetime_grid_333` (only `DATETIME` columns the target recomputes; loaded `DATETIME`/`SMALLDATETIME`/`DATETIME2(<=6)`/`BIGDATETIME` are exact in µs and use `identity`), `datetime_utc_truncate_ms` (only `DATETIMEOFFSET(<=3)`, where ms truncation is lossless), `collation_casefold` (only when the census reports `_CI_`), `rstrip_spaces` (`CHAR`), `decimal_round` places=4 `half_up` (only columns recomputed from `MONEY`; `DECIMAL(p,s)` of any scale is `identity`), `uuid_normalize`, `null_missing_equiv`, `identity`. `empty_string_is_null` is deliberately absent (would hide the ASE concatenation trap). The harness applies a rule only to fields whose mapping-spec `rules` list names it (`recon.config` field mapping), so `applies_to` / `enabled_if` are instructions for writing that list, not runtime switches.

## References

| File | Load it when |
|---|---|
| [Type map](references/type-map.md) | mapping SQL Server or ASE data types to Delta or Lakebase |
| [Function and construct map](references/construct-map.md) | converting T-SQL, ASE, SQL Agent, or SSIS constructs |
| [Traps with recon signature](references/traps.md) | diagnosing dialect-specific recon findings |
| [Canonical Lakeflow / DBSQL shape and examples](references/canonical-shape.md) | writing target procedures, jobs, or example conversions |
