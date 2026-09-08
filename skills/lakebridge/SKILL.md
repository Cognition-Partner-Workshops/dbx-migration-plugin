---
name: lakebridge
description: Databricks Labs Lakebridge (analyzer, profiler, BladeBridge/Morpheus/Switch transpilers, reconciler) as a migration accelerator. Use during estate assessment and unit conversion for supported source dialects; carries the invocation, dialect flags, and the seeded per-dialect coverage table. Accelerator only, never the merge gate.
---

# Lakebridge

Lakebridge is the Databricks Labs migration toolkit: an **analyzer** (offline scan of exported code, complexity + inventory + interdependencies), an experimental **profiler** (connects to the source system for metadata and workload metrics), three **transpilers** (BladeBridge and Morpheus deterministic; Switch LLM-based, experimental), and a **reconciler**. Position in this kit: it accelerates `!dbx_estate_inventory` and `!dbx_unit_migration`; its output always passes our own data-reconciliation harness, and the independent recon session remains the merge authority.

Facts below marked **[docs]** come from https://databrickslabs.github.io/lakebridge/ (read 2026-09); rows marked **SEEDED** are expectations from the dialect skills' Known Traps and have not been confirmed on an engagement. Confirm the command surface with `--help` at first use per engagement: the labs CLI evolves.

## Install and verify [docs]
```
databricks labs install lakebridge
databricks labs lakebridge install-transpile      # prompts for transpiler, dialect, paths; Java 21+ for Morpheus
databricks labs lakebridge transpile --help
databricks labs lakebridge analyze --help
```
Network: GitHub, Maven Central, PyPI. The `factory-doctor` reports Lakebridge present/absent; absent is not a blocker, it is a hand-conversion engagement.

## Analyzer (estate inventory) [docs]
```
databricks labs lakebridge analyze --source-directory <export dir> --source-tech <tech> \
  --report-file <path>.xlsx --generate-json true
```
Input is the legacy export on disk (SQL files, ETL repository XML/JSON), never a live connection: it fits the read-only rule by construction. Feed the JSON into the census and complexity ranking, cross-checked against the coverage arithmetic (the analyzer is an input, not the census; a file it cannot parse is still an asset). Run the SQL Splitter first on monolithic dump files.

## Profiler (optional, experimental) [docs]
`configure-database-profiler` then `execute-database-profiler`; supports Synapse, Teradata, Snowflake, SQL Server, Oracle, BigQuery, Redshift. It connects to the source: only with the engagement's read-only principal named in `07_access_checklist.md`, only inside the legacy-query concurrency cap, and only if the census needs workload metrics the export cannot give (query patterns for Tier 4 op selection). Never install its collection objects on the legacy system.

## Transpiler (unit conversion) [docs]
```
databricks labs lakebridge transpile --input-source <unit dir> --output-folder <unit working area> \
  --source-dialect <dialect> [--transpiler-config-path <cfg>] [--error-file-path <log>] \
  --skip-validation false --catalog-name <migration catalog> --schema-name <unit schema>
```
`--catalog-name`/`--schema-name` are used only to validate the output SQL against a warehouse; they must be the batch's declared write target (the PreToolUse guard blocks anything else). Output lands in the unit working area for review, never straight to deploy. Always pass `--error-file-path`: the error log is the raw material of the coverage table.

### Dialect and transpiler matrix [docs]
| `--source-dialect` | BladeBridge | Morpheus | Switch (experimental) | Factory dialect skill |
|---|---|---|---|---|
| `mssql` (SQL Server, Azure SQL, RDS) | DBSQL | DBSQL | SparkSQL | `tsql-ssis` (child session; Sybase ASE has no dialect flag, treat as `mssql` with the ASE delta list) |
| `teradata` | DBSQL | — | SparkSQL | `teradata-bteq` |
| `oracle` | DBSQL | — | SparkSQL | `oracle-plsql` (child session) |
| `redshift` | DBSQL (experimental) | — | SparkSQL | `redshift-sql` |
| `snowflake` | — | DBSQL | SparkSQL | none yet |
| `synapse` | DBSQL | DBSQL | SparkSQL | none yet |
| `netezza` | DBSQL | — | SparkSQL | none yet |
| `postgresql`, `mysql` | — | — | SparkSQL | OLTP front door sources; Switch only |
| `ssis` | SparkSQL (experimental) | — | SDP | `tsql-ssis` |
| `datastage` | SparkSQL, PySpark | — | SDP | none yet |
| Informatica | not listed as a transpile source | | | `informatica-xml` (hand conversion; analyzer may still inventory the XML export, confirm `--source-tech`) |

Selection rule: deterministic first (BladeBridge/Morpheus) for anything they list; Switch only for procedural bodies they reject, and its output is a *draft with the same standing as an LLM-written conversion*: every statement reviewed against the dialect skill, then proved by recon. Never run Switch on a whole estate to save time; it removes the determinism that makes systematic-error detection cheap.

## Seeded coverage table
Three classes per dialect, appended per engagement like Known Traps: **converts** (trust after a read), **mangles silently** (compiles, wrong answer; recon catches it, so make sure the tolerance or op set exercises it), **rejects** (error log; hand-convert). Seed rule [docs + kit]: deterministic transpilers are strongest on set-based DML/DDL and weakest on procedural code and vendor functions with non-obvious semantics.

| Dialect | Converts (SEEDED) | Mangles silently (SEEDED; recon signature) | Rejects / hand-convert (SEEDED) |
|---|---|---|---|
| `teradata` | SELECT/INSERT/UPDATE/DELETE/MERGE, `QUALIFY`, CTEs, volatile tables to temp views, BTEQ SQL bodies | `NOT CASESPECIFIC` comparisons (extra distinct groups, missed joins: Tier 2 count/distinct drift on string keys); `SET` table dedup (row-count excess); half-even decimal rounding (last-digit Tier 3 diffs); `FORMAT`/`TITLE`-driven implicit casts; `ADD_MONTHS` month-end; integer division | BTEQ control flow (`.IF ERRORCODE`, `.LABEL`, `.QUIT`), SPL procedures with cursors/exception handlers, macros with parameters, `PERIOD` types, TPT/MLOAD/FASTLOAD control files |
| `redshift` (experimental) | ANSI DML/DDL, window functions, CTEs, `DISTKEY`/`SORTKEY` dropped to clustering hints | `GETDATE()`/timezone-less timestamps (Tier 3 offset diffs); `VARCHAR` byte vs character length; `NULLS FIRST/LAST` defaults; `LISTAGG` ordering without `WITHIN GROUP`; `::` casts with truncation | `UNLOAD`/`COPY` with IAM roles, Redshift Spectrum externals, `SUPER`/`PartiQL`, stored procedures (plpgsql), Python UDFs |
| `mssql` | DML/DDL, `TOP` to `LIMIT`, CTEs, `MERGE`, `CASE`, common string/date functions | `DATETIME` 3.33ms rounding (Tier 3 timestamp diffs, canonicalize with `datetime_grid_333`); `nvarchar` collation-insensitive joins (Tier 2 distinct drift; fix with `COLLATE UTF8_LCASE`); `ISNULL` vs `COALESCE` typing; `@@ROWCOUNT`-dependent logic; integer division; `CONVERT` style codes | Procedures with `TRY/CATCH`, cursors, `sp_executesql`, temp tables with identity, triggers, `OUTPUT` clauses, linked-server four-part names, Sybase ASE `*=` joins and `COMPUTE BY` |
| `oracle` | DML/DDL, `MERGE`, analytic functions, `DECODE`/`NVL`, `ROWNUM` to `LIMIT` in simple cases | `NUMBER` without scale (float vs decimal; Tier 2 sum drift); `DATE` carrying a time component; `''` is NULL (Tier 1/3 null-vs-empty, canonicalize `empty_string_is_null`); `CONNECT BY` order; `TRUNC(date)` vs `DATE_TRUNC` | PL/SQL packages, cursors/bulk collect, exceptions, autonomous transactions, sequences/`NEXTVAL` in DML, `DBMS_*` calls, external tables, `PIVOT` with XML |
| `snowflake` | DML/DDL, `QUALIFY`, semi-structured `VARIANT` path access to `:` / `get_json_object`, `FLATTEN`, time travel dropped with note | `VARIANT` typing on comparison; `TIMESTAMP_NTZ/LTZ/TZ` mixing (Tier 3 offset diffs); `IFF`/`ZEROIFNULL` typing; identifier case folding (unquoted upper) | JavaScript/Snowflake Scripting procedures, tasks/streams, stages/`COPY INTO` with file formats, external functions, `MATCH_RECOGNIZE` |
| `ssis` (experimental) | Data Flow source/destination/derived-column/lookup to SparkSQL or SDP skeletons | Lookup cache mode semantics (first-match vs all rows: Tier 1 row counts); error-output redirection (silently dropped reject rows); expression-language date/string edge cases | Script Components (C#/VB), Execute SQL Task bodies (hand-convert via `mssql`), package variables/config, event handlers, precedence constraints (to Lakeflow Jobs by hand) |

A row moves from SEEDED to CONFIRMED (or is corrected) when a unit's error log or recon result shows it; record the unit id next to the row. A systematic mangle goes into SKILL FEEDBACK for the dialect skill so the wave learns it once.

## Reconciler [docs]
Lakebridge ships its own reconcile module. It may run as a second opinion on a unit; it never replaces the kit's `dbx-recon` gate and never self-certifies (rule 4 of the guardrails). If both disagree, the kit's harness result stands and the disagreement is a finding.

## Rules
- Transpiled output is a draft: review against the source-dialect skill's conversion rules, then prove it with the data-reconciliation harness like hand-converted code.
- Transpile per unit into the unit's working area with `--error-file-path` set; a unit whose error log is non-empty is a partial conversion and the brief says so.
- Validation catalog/schema are the batch's declared write target; never `remorph`/`transpiler` defaults, never a production catalog.
- Profiler only with the read-only principal, inside the query cap, and only when the export cannot answer the question.
- Every coverage-table change carries a unit id and a date; SEEDED rows are hypotheses, not knowledge.
