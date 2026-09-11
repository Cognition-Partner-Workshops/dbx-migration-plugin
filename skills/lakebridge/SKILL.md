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
| `redshift` | DBSQL (experimental) | — | SparkSQL | `redshift-sql` (stub) |
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
| `teradata` (SEEDED v1, `teradata-bteq` "Traps with recon signature"; fixture `uc-dw-migration-teradata-to-bigquery`, file round-trip only) | SELECT/INSERT/UPDATE/DELETE/MERGE, `QUALIFY`, CTEs, volatile tables to temp views, BTEQ SQL bodies, `SEL`/`INS`/`UPD`/`DEL` abbreviations, `NULLIFZERO`/`ZEROIFNULL`, `ADD_MONTHS`, `CREATE TABLE ... AS ... WITH DATA`, `LOCKING`/`COLLECT STATISTICS`/`COMPRESS` dropped | `NOT CASESPECIFIC` comparisons (extra distinct groups, missed joins: Tier 2 count/distinct drift on string keys); `SET` table dedup (Tier 1 row-count excess); half-even decimal rounding (last-digit Tier 3 diffs); `FORMAT`/`TITLE`-driven implicit casts (Tier 3 on derived keys); `ADD_MONTHS` month-end; integer division (Tier 2 sum drift); `MAVG`/`MSUM` frame width `n` vs `n-1 PRECEDING` (Tier 4 checksum, fixture `verify/checks/20_branch_performance.sql`); `CSUM` inside `GROUP BY` without `PARTITION BY` (Tier 2); `SUBSTR` position 0; `STRTOK` empty tokens; `TD_WEEK_OF_YEAR` week 0; `DATE` integer arithmetic on timestamps; `CHAR` trailing-blank equality (Tier 1 join shortfall) | BTEQ control flow (`.IF ERRORCODE`, `.LABEL`, `.QUIT`, `.EXPORT`/`.IMPORT`, `.OS`), SPL procedures with cursors/exception handlers/`DBC.SysExecSQL`/BT-ET, multi-statement macros (multi result set), `PERIOD` types and `NORMALIZE`/`P_INTERSECT`, `RESET WHEN`, `EXPAND ON`, `HASHROW`-derived columns, TPT/MLOAD/FASTLOAD/FASTEXPORT control files, triggers, join/hash indexes, `DYNAMIC RESULT SETS` |
| `redshift` (experimental) | ANSI DML/DDL, window functions, CTEs, `DISTKEY`/`SORTKEY` dropped to clustering hints | `GETDATE()`/timezone-less timestamps (Tier 3 offset diffs); `VARCHAR` byte vs character length; `NULLS FIRST/LAST` defaults; `LISTAGG` ordering without `WITHIN GROUP`; `::` casts with truncation | `UNLOAD`/`COPY` with IAM roles, Redshift Spectrum externals, `SUPER`/`PartiQL`, stored procedures (plpgsql), Python UDFs |
| `mssql` | DML/DDL, `TOP` to `LIMIT`, CTEs, `MERGE`, `CASE`, common string/date functions | `DATETIME` 3.33ms rounding (Tier 3 timestamp diffs, canonicalize with `datetime_grid_333`); `nvarchar` collation-insensitive joins (Tier 2 distinct drift; fix with `COLLATE UTF8_LCASE`); `ISNULL` vs `COALESCE` typing; `@@ROWCOUNT`-dependent logic; integer division; `CONVERT` style codes | Procedures with `TRY/CATCH`, cursors, `sp_executesql`, temp tables with identity, triggers, `OUTPUT` clauses, linked-server four-part names, Sybase ASE `*=` joins and `COMPUTE BY` |
| `oracle` | DML/DDL, `MERGE`, analytic functions, `DECODE`/`NVL`, `ROWNUM` to `LIMIT` in simple cases | `NUMBER` without scale (float vs decimal; Tier 2 sum drift); `DATE` carrying a time component; `''` is NULL (Tier 1/3 null-vs-empty, canonicalize `empty_string_is_null`); `CONNECT BY` order; `TRUNC(date)` vs `DATE_TRUNC` | PL/SQL packages, cursors/bulk collect, exceptions, autonomous transactions, sequences/`NEXTVAL` in DML, `DBMS_*` calls, external tables, `PIVOT` with XML |
| `snowflake` | DML/DDL, `QUALIFY`, semi-structured `VARIANT` path access to `:` / `get_json_object`, `FLATTEN`, time travel dropped with note | `VARIANT` typing on comparison; `TIMESTAMP_NTZ/LTZ/TZ` mixing (Tier 3 offset diffs); `IFF`/`ZEROIFNULL` typing; identifier case folding (unquoted upper) | JavaScript/Snowflake Scripting procedures, tasks/streams, stages/`COPY INTO` with file formats, external functions, `MATCH_RECOGNIZE` |
| `ssis` (experimental) | Data Flow source/destination/derived-column/lookup to SparkSQL or SDP skeletons | Lookup cache mode semantics (first-match vs all rows: Tier 1 row counts); error-output redirection (silently dropped reject rows); expression-language date/string edge cases | Script Components (C#/VB), Execute SQL Task bodies (hand-convert via `mssql`), package variables/config, event handlers, precedence constraints (to Lakeflow Jobs by hand) |
| Informatica (`informatica-xml`; no transpiler flag; SEEDED 2026-09 from fixture units `m_POLICY_MASTER_DAILY`, `m_PARTY_MDM_SYNC`, `m_RI_BORDEREAUX_MONTHLY`) | analyzer inventory of the XML export only; embedded SQL overrides (`Sql Query`, `Lookup Sql Override`, pre/post SQL) transpile under the connection's dialect (`teradata`, `oracle`) | n/a for mappings (nothing is transpiled); for embedded SQL, the connection dialect's mangles apply plus lost session-level / per-partition overrides (Tier 1 count drift) | all transformation logic (Expression, Aggregator, Lookup, Router, Update Strategy, Sequence Generator, Normalizer, Java/SQL/Stored Procedure transformations), workflows/worklets/Command/Event Wait/Decision tasks, parameter files, mapping variables, lookup cache modes: hand-convert per `skills/informatica-xml/SKILL.md` (function/construct map, traps, canonical shape) |

Delta appended with `skills/tsql-ssis/SKILL.md` (SEEDED 2026-09-08 against the Sybase ASE fixture `ts-tsql-sybase-legacy-db` and the `.dtsx`-shaped package in `skills/tsql-ssis/examples/ssis-dataflow-lookup/`; unit id: none, fixture seed, not an engagement unit). Additive to the `mssql`/`ssis` rows above, not a rewrite:

| Dialect | Converts (SEEDED) | Mangles silently (SEEDED; recon signature) | Rejects / hand-convert (SEEDED) |
|---|---|---|---|
| `mssql` + Sybase ASE delta (`tsql-ssis`) | `HOLDLOCK`/`NOHOLDLOCK`/`NOLOCK` hints dropped; `text`/`image` to `STRING`/`BINARY`; `MONEY` to `DECIMAL(19,4)` (`decimal_round` places=4 only on columns recomputed from `MONEY`); `STRING_AGG`/`PERCENTILE_CONT` (add deterministic `ORDER BY`) | `SET ROWCOUNT n` + `@@rowcount` batching loops (loop kept, limit dropped: Tier 1 count excess or a non-terminating loop); `@@identity`/`SCOPE_IDENTITY()` emitted as unresolved function (Tier 3 diffs on audit/key columns); `CONVERT(..., style)` for styles other than 101/103/112/120 (Tier 3 string-date diffs, `datetime_utc_truncate_ms` does not mask it); `MERGE ... OUTPUT` / `UPDATE ... FROM` (`OUTPUT` dropped: Tier 1 on the audit target); `SELECT @v = col` over many rows (last-row-wins becomes an error or a different row: Tier 3); NULL string concatenation under ASE (`''` vs NULL: Tier 2 null-rate) | ASE `*=`/`=*` (pre-rewrite to `LEFT`/`RIGHT JOIN` with inner-side predicates moved to `ON`), `COMPUTE BY`, `@@sqlstatus` cursor loops, `DEALLOCATE CURSOR`, `@@error` + `GOTO`, positional `RAISERROR 50001 'msg', @arg`, `$45.00` money literals, `FOR INSERT`/`FOR UPDATE` triggers with `ROLLBACK TRIGGER`, `PATINDEX`/`FORMAT`/`QUOTENAME`/`CHOOSE`, SQL Agent `sp_add_job*` scripts, `isql` shell runners and `interfaces` aliases (task graph by hand) |
| `ssis` (`tsql-ssis`) | Conditional Split, Union All, Sort, Aggregate to SparkSQL shape (re-target to SDP datasets, or to a Jobs `sql_task` `MERGE` batch when a per-execution parameter selects the flow's rows: `tsql-ssis` construct map, "SSIS Data Flow driven by a per-execution parameter") | Lookup Full Cache vs Partial/No Cache on a CI database (exact vs collation-insensitive match: Tier 1 match/no-match split, `collation_casefold` only for non-Full cache); `FastLoadKeepNulls=false` destination defaults (Tier 2 null-rate vs default value); `(DT_STR, n, cp)` casts silently truncating (Tier 3 string length); `OnError` handlers dropped (no recon signature: task-graph parity check) | Lookup `NoMatch` redirect to a second destination (expectation `DROP ROW` + quarantine dataset by hand), `ExpressionAndConstraint`/`LogicalAnd="False"` precedence constraints (`run_if` by hand), `ResultSet=SingleRow` variable bindings, Execute Package / For Loop / Foreach Loop, Flat File fixed-width destinations with code pages, FTP/SMTP tasks, `EncryptSensitiveWith*` protection levels (governance finding) |

A row moves from SEEDED to CONFIRMED (or is corrected) when a unit's error log or recon result shows it; record the unit id next to the row. A systematic mangle goes into SKILL FEEDBACK for the dialect skill so the wave learns it once.

### `oracle` delta (SEEDED, 2026-09-08, from `skills/oracle-plsql`; no engagement unit yet)
Additions to the `oracle` row above for `--source-dialect oracle`; the base row is unchanged. Signatures name the trap number in the `oracle-plsql` SKILL.md traps table.

| Class | Construct | Expectation and recon signature |
|---|---|---|
| Converts | `LISTAGG ... WITHIN GROUP`, `NVL2`, `TO_CHAR(d, fmt)` with `YYYY/MM/DD/HH24/MI/SS` only, `ADD_MONTHS`/`LAST_DAY`, `MERGE ... WHEN MATCHED ... DELETE WHERE` | trust after a read; `DELETE WHERE` must come out as a separate `WHEN MATCHED AND <cond> THEN DELETE` ordered before the `UPDATE` (trap 8; `oracle-plsql` example 01) |
| Mangles silently | `CHAR(n)` compares against `VARCHAR2` | blank-padded semantics lost; Tier 3 mismatches on `CHAR` keys, Tier 1 undercount on joins; canonicalize `rstrip_spaces` and `rtrim()` the converted predicate (trap 5) |
| Mangles silently | `ROWNUM` sandwich pagination | rewritten to `LIMIT` without the inner `ORDER BY` preserved as the total key; Tier 4 page contents differ, Tier 1 per-page count is right (trap 7) |
| Mangles silently | `TO_CHAR(n, 'FM...')`, `TO_CHAR(d, 'DD-MON-YYYY')`, `TRUNC(d, 'IW'/'Q')` | `FM` dropped, `MON` lower-cased (Tier 4 byte diffs); `'IW'`/`'Q'` become day truncation or fail typing (Tier 1 group counts) |
| Mangles silently | `a || b` with a nullable operand | `concat()` returns NULL where Oracle returns the non-null side; Tier 3 null-vs-value diffs on derived strings (trap 23) |
| Mangles silently | `CONNECT BY NOCYCLE ... CONNECT_BY_ISCYCLE` | recursive CTE without a cycle guard loops to the recursion limit or drops the cycle row Oracle emits; Tier 1 row count on hierarchy views (trap 6) |
| Mangles silently | `SUM(NUMBER)` over an all-NULL group in an MV | Oracle NULL vs pipeline MV 0; Tier 2 with `null_missing_equiv` **off** on the aggregate columns (traps 15, 25) |
| Rejects / hand-convert | `CREATE MATERIALIZED VIEW LOG`, `REFRESH FAST`, `ENABLE QUERY REWRITE`, `DBMS_MVIEW.REFRESH`; `DBMS_SCHEDULER.*`, calendar strings | Lakeflow Pipelines MV / Lakeflow Jobs by hand; calendar string to quartz cron + `timezone_id`; `max_failures` is a GAP (traps 15, 16) |
| Rejects / hand-convert | SQL*Plus directives (`SET`, `DEFINE`/`&n`, `WHENEVER`, `SPOOL`, `EXIT`) | strip and re-express as job parameters / task outcome (trap 17); a transpiler run on a `.sql` with these produces parse errors for the whole file, so split the SQL body out first |
| Rejects / hand-convert | PL/SQL bodies: packages, `BULK COLLECT`/`FORALL`, exception blocks, `CREATE OR REPLACE TRIGGER` with `:NEW`/`:OLD`, `PRAGMA AUTONOMOUS_TRANSACTION`, `FOR UPDATE SKIP LOCKED`, `SAVEPOINT`/`ROLLBACK TO` | OLTP-profile units go to Lakebase/Postgres by hand (`oracle-plsql` example 02); analytical units become set-based DBSQL procedures with trigger logic folded into the writer (example 03; traps 10, 13, 14) |
| Rejects / hand-convert | `CREATE SYNONYM`, `@dblink` references, `CREATE DATABASE LINK`, VPD (`DBMS_RLS`) / `DBMS_REDACT` policies, `GRANT ... TO PUBLIC` | inventory only; resolve synonyms in lineage, external links stay `INFERRED` edges, policies map through `databricks-unity-catalog` or `GAP` (traps 18, 19) |

## Reconciler [docs]
Lakebridge ships its own reconcile module. It may run as a second opinion on a unit; it never replaces the kit's `dbx-recon` gate and never self-certifies (rule 4 of the guardrails). If both disagree, the kit's harness result stands and the disagreement is a finding.

## Rules
- Transpiled output is a draft: review against the source-dialect skill's conversion rules, then prove it with the data-reconciliation harness like hand-converted code.
- Transpile per unit into the unit's working area with `--error-file-path` set; a unit whose error log is non-empty is a partial conversion and the brief says so.
- Validation catalog/schema are the batch's declared write target; never `remorph`/`transpiler` defaults, never a production catalog.
- Profiler only with the read-only principal, inside the query cap, and only when the export cannot answer the question.
- Every coverage-table change carries a unit id and a date; SEEDED rows are hypotheses, not knowledge.
