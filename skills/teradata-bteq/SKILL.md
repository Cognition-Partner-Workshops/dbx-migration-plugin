---
name: teradata-bteq
description: Source-dialect skill for Teradata estates (Teradata SQL, BTEQ scripts, SPL stored procedures and macros, TPT/MLOAD/FASTLOAD control files). Load it when enumerating a Teradata estate, extracting lineage from BTEQ/SPL, converting Teradata objects to Databricks, or reconciling a converted unit (its `canonicalization.json` feeds the harness). Target-side facts live in the official databricks skills behind `target-routing`.
---

# Teradata / BTEQ Dialect

Source-side half of the factory for Teradata. Every Databricks-side rule below cites the official skill section it
relies on (`databricks-dbsql`, `databricks-jobs`, `databricks-pipelines`, `databricks-unity-catalog`,
`databricks-lakebase`, `databricks-dabs`); load `databricks-core` first via `target-routing`. Fixture estate used to
harden this version: the retail-banking Teradata estate `uc-dw-migration-teradata-to-bigquery` (`ddl/`, `dml/`,
`schemas/`, `verify/`, `docs/`; its BigQuery half is ignored, the target here is Databricks). Constructs the fixture
lacks (cursors, loops, dynamic SQL, BT/ET, load-utility control files) are exercised by skill-authored minimal
fixtures under `examples/` and marked as such.

## 1. Enumeration

Units: BTEQ scripts (the scheduler's script library is usually the real estate), stored procedures and macros
(`DBC.TablesV` where `TableKind` in P/M), views, tables, triggers, and load-utility jobs (MLOAD/FASTLOAD/TPT control
files). Census key: `database.object` for catalog objects; repository path for scripts and control files. Every
query below is read-only, one statement per object class, paginated with `QUALIFY ROW_NUMBER() OVER (ORDER BY
DatabaseName, TableName) BETWEEN :lo AND :hi`; each names the privilege the read-only principal needs so
`07_access_checklist.md` can be filled from it.

| Object class | Catalog query (read-only) | Census key | Size / complexity signal | Minimum privilege |
|---|---|---|---|---|
| Tables (SET/MULTISET, GTT, queue) | `SELECT DatabaseName, TableName, TableKind, CreateTimeStamp, LastAlterTimeStamp, RequestText FROM DBC.TablesV WHERE TableKind IN ('T','O','Q')` | `db.table` | `DBC.TableSizeV` (`CurrentPerm` summed over AMPs), column count from `DBC.ColumnsV`, index count from `DBC.IndicesV` | `SELECT` on `DBC` views (granted to PUBLIC on most sites; confirm) |
| Views | `... WHERE TableKind = 'V'`; body from `SHOW VIEW db.v` or `DBC.TablesV.RequestText` (truncated at 12500 chars: prefer `SHOW`) | `db.view` | body lines, referenced tables (§2), OLAP-function count, `QUALIFY` count | `SELECT` on `DBC`; `SHOW VIEW` needs `SELECT` or `SHOW` on the view |
| Stored procedures (SPL) | `... WHERE TableKind = 'P'`; body via `SHOW PROCEDURE db.p` (source kept only if compiled `WITH SPL`) | `db.proc` | lines, `DECLARE ... CURSOR` count, `DBC.SysExecSQL`/`EXECUTE IMMEDIATE` count, handler count, `CALL` fan-out | `SHOW` needs `DROP PROCEDURE` or ownership on many sites: name it in the checklist |
| Macros | `... WHERE TableKind = 'M'`; body via `SHOW MACRO` | `db.macro` | statement count (multi-statement macro = multi result set, §6), parameter count | `SELECT` on `DBC`; `SHOW MACRO` |
| Triggers | `SELECT DatabaseName, TriggerName, SubjectTableName, ActionTime, Event, Enabled FROM DBC.TriggersV` | `db.trigger` | body lines, subject table fan-out | `SELECT` on `DBC` |
| Columns / types (§4 input) | `SELECT DatabaseName, TableName, ColumnName, ColumnType, ColumnLength, DecimalTotalDigits, DecimalFractionalDigits, UpperCaseFlag, Nullable, ColumnFormat, ColumnTitle, CompressValueList, DefaultValue FROM DBC.ColumnsV` | `db.table.column` | `UpperCaseFlag` (`C` = CASESPECIFIC, `N` = NOT CASESPECIFIC), `ColumnFormat` non-null (implicit-cast risk) | `SELECT` on `DBC` |
| Indexes / partitioning | `DBC.IndicesV` (`IndexType` P/Q/S/K/U, `UniqueFlag`), `DBC.PartitioningConstraintsV` (`ConstraintText`) | `db.table` | PPI level count, join-index presence (`IndexType = 'J'` in `DBC.IndicesV`, kind `J` in `TablesV`) | `SELECT` on `DBC` |
| BTEQ scripts | filesystem: `**/*.btq`, `**/*.bteq`, `**/*.sql` containing lines starting with `.LOGON`, `.RUN FILE`, `.IF ERRORCODE`; scheduler definitions (cron/Control-M/Autosys) for the invocation and the parameter file | repo path | lines, `.IF` count, `.GOTO`/`.LABEL` count, `.EXPORT`/`.IMPORT` count, `.RUN FILE` includes, `.OS` shell escapes | repository read; scheduler export |
| TPT jobs | `**/*.tpt`, `**/*.txt` containing `DEFINE JOB` / `DEFINE OPERATOR`; job-variables files (`-v`) | repo path | operator types (`LOAD`, `UPDATE`, `STREAM`, `EXPORT`, `DDL`, `DATACONNECTOR`), `APPLY` statement count | repository read |
| MLOAD / FASTLOAD / FASTEXPORT | `**/*.mload`, `**/*.ml`, `**/*.fload`, `**/*.fexp`, files containing `.BEGIN IMPORT MLOAD` / `BEGIN LOADING` / `.BEGIN EXPORT` | repo path | `.DML LABEL` count, `.LAYOUT` field count, error-table names | repository read |
| Scheduler | script library index + scheduler export (job -> command line -> script -> parameter file) | job name | dependency edges, run windows | scheduler export |

Complexity signal per object is the input to §11. Cross-check counts (`DBC.TablesV` per kind vs export file counts vs
scheduler job count) as `2-estate_inventory` step 5 requires; when `SHOW PROCEDURE` is denied, the census records the
object with body `UNAVAILABLE` and the checklist asks for it: never guess a body.

Fixture round-trip (file-based, no live engine): 7 table DDL files (`ddl/tables/*.sql`, `CREATE SET|MULTISET TABLE`),
3 views (`ddl/views/*.sql`, `REPLACE VIEW`), 3 procedures (`dml/stored_procedures/*.sql`, `REPLACE PROCEDURE`),
3 macros (`dml/macros/*.sql`, `REPLACE MACRO`), 2 BTEQ scripts (`dml/scripts/*.btq`); no triggers, no control
files. `N = 18 = 7 + 3 + 3 + 3 + 2`.

## 2. Lineage extraction

Reads/writes per object class, in the order the object executes them (BTEQ and SPL are imperative; the order matters
for volatile tables and for `.IF` branches).

| Object class | Reads | Writes | Resolution rules | FACT / INFERRED |
|---|---|---|---|---|
| Table | — | — | PI/PPI/join indexes are properties, not edges | FACT (DDL) |
| View | every table/view in `FROM`/`JOIN`/subquery, plus `LOCKING ROW FOR ACCESS` targets | none | resolve unqualified names against the view's own database (Teradata default database at create time) | FACT |
| Procedure | `SELECT`/`SEL` sources, cursor queries, `CALL` targets (edge to the callee's unit) | `INSERT`/`UPDATE`/`UPD`/`DELETE`/`DEL`/`MERGE`/`CREATE ... AS` targets | volatile tables (`CREATE VOLATILE TABLE`) and GTT materialisations are script-local, not estate edges; record them as internal nodes. `CALL DBC.SysExecSQL(v)` / `EXECUTE IMMEDIATE v`: parse literal fragments of `v` for table names; anything concatenated from a parameter is INFERRED with the parameter named | FACT for static statements, INFERRED for dynamic |
| Macro | body statements | body statements | parameters (`:p`) inside identifiers are impossible in Teradata macros (parameters are values), so macro lineage is FACT; the *caller* is the edge to record (`EXEC db.macro` in BTEQ/SPL) | FACT |
| BTEQ script | each statement, in order; `.RUN FILE` includes parsed inline; `.IMPORT` file -> table; `.EXPORT` table -> file (consumer edge) | as procedure | `.SET`/shell `${VAR}` / `$var` substitution: FACT when the wrapper or parameter file is in the export, otherwise INFERRED naming the variable; `.OS` shell escapes: INFERRED (opaque); `.IF ... .GOTO` branches: include both branches' edges | mixed; list every INFERRED edge |
| Trigger | subject table + body reads | body writes | edge from subject table to every body write; fan-out feeds §11 | FACT |
| TPT / MLOAD / FASTLOAD | file(s) named in `DirectoryPath`/`FileName` / `.IMPORT INFILE` / `DEFINE ... FILE` | `TargetTable`, `TABLES`, `.DML` targets; error/log/work tables (bookkeeping nodes, not estate edges) | job variables (`@Var`) from the job-variables file: FACT when the file is exported; `$tdpid/$user` never carry lineage | FACT / INFERRED per variable |
| Scheduler | job -> script -> parameter file | — | one edge per scheduler dependency; cron ordering by clock time is INFERRED (name the assumption) | as stated |

Rules: Teradata `SEL`, `UPD`, `DEL`, `INS` abbreviations parse as their full keywords; `db.table` and `"db"."table"`
are the same node (identifiers fold to upper case unless quoted); `CREATE TABLE ... AS db.t WITH NO DATA` is a
structural read (edge class `schema-only`). A table that appears only as a write target and has no DDL in the export
(the fixture's `STG_TRANSACTIONS`, `STG_TRANSACTION_ERRORS`, `STG_CUSTOMER`, `ETL_BATCH_CONTROL`, `ETL_LOG`,
`DIM_EXCHANGE_RATES`) is an INFERRED node whose DDL must be fetched live
(`SHOW TABLE`/`DBC.ColumnsV`); it is never UNVERIFIABLE because the write statement is the cite.

Fixture round-trip: `bteq_daily_load.btq` yields FACT edges `STG_TRANSACTIONS -> (SP_LOAD_DAILY_TRANSACTIONS) ->
FACT_TRANSACTION`, `STG_CUSTOMER -> (SP_CUSTOMER_SCD2) -> DIM_CUSTOMER`, `EXEC DAILY_BALANCE_CHECK` (reads only), `.EXPORT REPORT ->
/etl/reports/daily_recon_<date>.txt` (consumer edge); volatile `VT_BATCH` is internal; four `.IF` branches, both
sides walked; INFERRED nodes only for the six tables without DDL in the export (schema, not edge). `bteq_extract_report.btq` yields
three `.EXPORT` consumer edges from the three views. `SP_MONTHLY_SNAPSHOT` has volatile `VT_TXN_AGGREGATES` internal
and `MERGE INTO FACT_MONTHLY_ACCOUNT_SNAPSHOT` FACT. Zero UNVERIFIABLE edges.

## 3. Unit definition

One unit is one *runnable*: a BTEQ script with every procedure and macro it calls and every table those write; a
procedure invoked directly by the scheduler with its write set; a load-utility job with its target and error tables;
a view (or a connected group of views over the same base tables) as a consumer unit. Tables are not units; they belong
to the unit that writes them. A unit is `shared` when two units write the same table (rare and a defect worth
recording) or when a table written by one unit is read by another pipeline (`DIM_CUSTOMER` written by the daily load,
read by every report view: wave 0). Macros called from more than one script are shared code, converted once in wave 0.
Volatile and global temporary tables never make a unit shared.

## 4. Type map

`loss`: none / precision / semantics. Lakebase column applies when the estate feeds the OLTP front door
(`databricks-lakebase` `references/synced-tables.md` "Data Type Mapping" gives the UC -> Postgres side; only UC types
listed there are shown). Canonicalization rule names are the harness's (§8).

| Teradata | Delta / UC type | Lakebase (via UC type) | loss | Canonicalization | Notes |
|---|---|---|---|---|---|
| `BYTEINT` | `TINYINT` | `SMALLINT` | none | `identity` | used as boolean flag (`IS_ACTIVE BYTEINT DEFAULT 1`): keep numeric, do not widen to `BOOLEAN` in like-for-like |
| `SMALLINT` | `SMALLINT` | `SMALLINT` | none | `identity` | |
| `INTEGER` / `INT` | `INT` | `INTEGER` | none | `identity` | |
| `BIGINT` | `BIGINT` | `BIGINT` | none | `identity` | |
| `DECIMAL(p,s)` / `NUMERIC(p,s)`, p <= 38 | `DECIMAL(p,s)` | `NUMERIC` | none (storage); precision in arithmetic | `decimal_round` | Teradata intermediate results keep p <= 38 with different scale rules than Spark; compare after rounding to the tolerance record's places |
| `DECIMAL(38,s)` at the boundary | `DECIMAL(38,s)` | `NUMERIC` | precision | `decimal_round` | `SUM` over `DECIMAL(38,s)` overflows on Spark where Teradata returns `DECIMAL(38,s)`; cast inputs to a lower scale or `DOUBLE` and record it |
| `NUMBER` (no p,s) | `DECIMAL(38,18)` or `DOUBLE` per column profile | `NUMERIC` / `DOUBLE PRECISION` | precision | `decimal_round` | float-like semantics on Teradata; profile the column before choosing |
| `FLOAT` / `REAL` / `DOUBLE PRECISION` | `DOUBLE` | `DOUBLE PRECISION` | none | `decimal_round` (recon only) | |
| `CHAR(n)` | `STRING` (`COLLATE UTF8_LCASE_RTRIM` when also NOT CASESPECIFIC, `databricks-dbsql` `references/geospatial-collations.md` "Collation Modifiers") | `TEXT` | semantics (blank padding) | `rstrip_spaces` | Teradata pads to n and compares ignoring trailing blanks; Delta keeps what is written. Loaders must not pad |
| `VARCHAR(n)` | `STRING` | `TEXT` | none (length not enforced) | `identity` | consumers relying on truncation at n: flag |
| `VARCHAR(n) NOT CASESPECIFIC` (default in Teradata-mode sessions) | `STRING COLLATE UTF8_LCASE` (`geospatial-collations.md` "Collation Types") | `TEXT` | semantics (case folding) | `collation_casefold` | the single most common recon trap (§7). `UpperCaseFlag = 'N'` in `DBC.ColumnsV` is the per-column FACT; ANSI-mode sessions default to CASESPECIFIC, so the session mode is a census fact |
| `... CASESPECIFIC` | `STRING` (`UTF8_BINARY`) | `TEXT` | none | `identity` | per-column exception to the previous row |
| `CHAR/VARCHAR ... CHARACTER SET UNICODE/LATIN/KANJISJIS` | `STRING` | `TEXT` | none for LATIN/UNICODE; semantics for KANJISJIS ordering | `identity` | `ORDER BY` on non-Latin strings differs in collation order; Tier 4 report ordering only |
| `CLOB` | `STRING` | `TEXT` | none | `identity` | |
| `BYTE(n)` / `VARBYTE(n)` / `BLOB` | `BINARY` | `BYTEA` | none | `identity` | `HASHROW` outputs are `BYTE(4)`: recompute, do not migrate (§5) |
| `DATE` | `DATE` | `DATE` | none | `identity` | Teradata `DATE` integer arithmetic (`d + 1`, `d - d`) becomes `date_add`/`datediff` (§5) |
| `DATE FORMAT 'YYYY-MM-DD'` | `DATE` | `DATE` | none (display only) | `identity` | `FORMAT` drops; every place the column was implicitly cast to text via the format needs an explicit `date_format` (§7) |
| `TIME(n)` / `TIME(n) WITH TIME ZONE` | `STRING` (`'HH:mm:ss[.SSSSSS]'`) | `TEXT` | semantics (no TIME type in the cited references) | `identity` | keep as string; arithmetic on it moves to timestamps |
| `TIMESTAMP(0)` | `TIMESTAMP` | `TIMESTAMP WITH TIME ZONE` | none | `datetime_utc_truncate_ms` | session time zone must be pinned in the mapping (Teradata stores without zone unless `WITH TIME ZONE`) |
| `TIMESTAMP(6)` | `TIMESTAMP` | `TIMESTAMP WITH TIME ZONE` | precision (microseconds compared at the engagement's grain) | `datetime_utc_truncate_ms` | engagement rule decides ms vs us; the harness rule truncates to ms on both sides |
| `TIMESTAMP(n) WITH TIME ZONE` | `TIMESTAMP` (UTC-normalised) | `TIMESTAMP WITH TIME ZONE` | semantics (zone offset dropped) | `datetime_utc_truncate_ms` | offset preserved only as a separate column if a consumer reads it |
| `TIMESTAMP(n)` where the estate wants zone-less | `TIMESTAMP_NTZ` | `TIMESTAMP WITHOUT TIME ZONE` | none | `datetime_utc_truncate_ms` | choose once per estate in the STOP A target profile |
| `INTERVAL YEAR/MONTH/DAY/HOUR/... TO ...` | `INTERVAL` (year-month or day-time) | `INTERVAL` | none for the two families; semantics for mixed `DAY TO SECOND` beyond Spark limits | `identity` | stored interval columns are rare; usually only in expressions (§5) |
| `PERIOD(DATE)` / `PERIOD(TIMESTAMP(n))` | two columns `<col>_BEGIN`, `<col>_END` of the element type; the end is exclusive in Teradata, keep it exclusive | two columns | semantics (type lost; `P_INTERSECT`, `OVERLAPS`, `NORMALIZE` become predicates) | **none in harness** (harness gap: a `period_decompose` rule; until then compare the two columns with `datetime_utc_truncate_ms`) | record the decomposition in the unit mapping so recon joins column-by-column |
| `JSON` | `STRING` (or `VARIANT` when the target profile allows it) | `TEXT` | none for storage | `identity` | path access `col.JSONExtractValue(...)` -> `get_json_object` (§5) |
| `XML` | `STRING` | `TEXT` | semantics (XPath functions) | `identity` | hand-convert consumers |
| `ST_GEOMETRY` | `GEOMETRY` (`geospatial-collations.md` "Geospatial Data Types") | unsupported in Lakebase per "Data Type Mapping" | semantics per function | `identity` | out of like-for-like scope; flag |
| `ARRAY`/`VARRAY` (UDT) | `ARRAY<T>` | `JSONB` | none | `identity` | rare |
| `GENERATED ALWAYS AS IDENTITY` | `GENERATED ALWAYS AS IDENTITY (START WITH .. INCREMENT BY ..)` | `BIGINT` | semantics (values differ: identity is not reproducible) | exclude the surrogate from Tier 3 keyed diff; join through the natural key | example 01 |
| `DEFAULT <expr>` on columns | moved to the loader/`INSERT` (not asserted as a target column default in this version) | — | none | `identity` | example 01 |
| `COMPRESS (...)` / `TITLE` / `FORMAT` / `CHECKSUM` / `FALLBACK` / `JOURNAL` / `MERGEBLOCKRATIO` | dropped with a note | — | none | — | display/physical properties |

## 5. Function and operator map

`semantics`: same / edge case / no equivalent. Databricks SQL expressions are those shown in `databricks-dbsql`
`SKILL.md` / `references/best-practices.md` where a pattern is cited; plain Spark SQL builtins otherwise (verify any
builtin not listed there with `databricks-core`'s query tooling before relying on it, per `target-routing`).

| # | Teradata | Databricks SQL | semantics | Edge case |
|---|---|---|---|---|
| 1 | `SEL` / `INS` / `UPD` / `DEL` | `SELECT` / `INSERT` / `UPDATE` / `DELETE` | same | abbreviations only |
| 2 | `QUALIFY <window predicate>` | `QUALIFY` (`best-practices.md` "Query Optimization Tips") | same | keep; Databricks allows `QUALIFY` on aliases defined in the select list |
| 3 | `SAMPLE n` / `SAMPLE .1` | `TABLESAMPLE (n ROWS)` / `TABLESAMPLE (10 PERCENT)` | edge case | non-deterministic on both; `SAMPLE` is AMP-local stratified; never reconcile a sampled output row-for-row |
| 4 | `TOP n [WITH TIES]` | `LIMIT n` (ties: `QUALIFY RANK() OVER (...) <= n`) | edge case | `TOP` without `ORDER BY` is arbitrary on both |
| 5 | `SELECT ... WITH BY ...` (BTEQ totals) | separate aggregate query or `GROUPING SETS` | no equivalent | report formatting; Tier 4 |
| 6 | `NULLIFZERO(x)` | `NULLIF(x, 0)` | same | |
| 7 | `ZEROIFNULL(x)` | `COALESCE(x, 0)` | edge case | result type follows `x` on Teradata; cast explicitly when `x` is `DECIMAL` |
| 8 | `NVL(a, b)` | `COALESCE(a, b)` | same | |
| 9 | `COALESCE` | `COALESCE` | same | |
| 10 | `NULLIF` | `NULLIF` | same | |
| 11 | `CASE ... END`, `CASE WHEN` | same | same | Teradata `CASE` result type is the highest precedence type; Spark same rule |
| 12 | `DECODE(x, a, r1, b, r2, d)` | `CASE x WHEN a THEN r1 WHEN b THEN r2 ELSE d END` or `decode(x, a, r1, ...)` | edge case | Teradata `DECODE` treats `NULL = NULL` as match; write `WHEN x IS NULL` explicitly |
| 13 | `INDEX(s, sub)` | `instr(s, sub)` | same | both 1-based, 0 when not found |
| 14 | `POSITION(sub IN s)` | `position(sub, s)` / `instr(s, sub)` | same | |
| 15 | `SUBSTR(s, pos, len)` / `SUBSTRING(s FROM pos FOR len)` | `substr(s, pos, len)` | edge case | Teradata `pos <= 0` shifts the window (`SUBSTR('abc', 0, 2) = 'a'`); Spark `substr('abc', 0, 2) = 'ab'`. Rewrite negative/zero positions explicitly |
| 16 | `CHAR_LENGTH` / `CHARACTERS(s)` | `length(s)` | edge case | on `CHAR(n)` Teradata counts padding; after `rstrip` semantics use `length(rtrim(s))` |
| 17 | `OCTET_LENGTH` / `BYTES` | `octet_length` | same | |
| 18 | `TRIM(s)` / `TRIM(BOTH FROM s)` | `trim(s)` | same | |
| 19 | `TRIM(LEADING 'x' FROM s)` / `TRIM(TRAILING ...)` | `ltrim('x', s)` / `rtrim('x', s)` | edge case | argument order is (trimStr, str) in Spark; Teradata trims one character, Spark trims any character in the set |
| 20 | `s1 \|\| s2` | `s1 \|\| s2` / `concat` | edge case | Teradata `CHAR` operands carry padding into the result; NULL propagates on both |
| 21 | `UPPER` / `LOWER` | same | same | |
| 22 | `OREPLACE(s, a, b)` | `replace(s, a, b)` | same | |
| 23 | `OTRANSLATE(s, from, to)` | `translate(s, from, to)` | same | |
| 24 | `REGEXP_SUBSTR(s, re, pos, occ, flags)` | `regexp_extract(s, re, 0)` for the first match; `regexp_extract_all` for others | edge case | Teradata `occurrence` and match flags (`'i'`) have no direct argument; fold `(?i)` into the pattern |
| 25 | `REGEXP_REPLACE(s, re, rep, pos, occ, flags)` | `regexp_replace(s, re, rep)` | edge case | positional/occurrence args dropped; Teradata regex dialect is POSIX-ish, Spark is Java: escape `\\d` etc. |
| 26 | `REGEXP_SIMILAR(s, re)` | `s RLIKE re` | edge case | returns 1/0 on Teradata; boolean on Spark |
| 27 | `LIKE ... ESCAPE` | `LIKE ... ESCAPE` | edge case | `LIKE` on NOT CASESPECIFIC columns is case-insensitive on Teradata; use the column collation or `lower()` both sides |
| 28 | `s (CASESPECIFIC)` / `s (NOT CASESPECIFIC)` | `s COLLATE UTF8_BINARY` / `s COLLATE UTF8_LCASE` (`geospatial-collations.md` "Collation Precedence") | same | explicit collation beats implicit column collation on both |
| 29 | `x (FORMAT '...')` / `x (FORMAT '...') (CHAR(n))` | `date_format(x, 'pattern')` / `format_number` / `lpad`/`rpad` | edge case | Teradata format tokens (`YYYY-MM-DD`, `9(5)`, `Z(9)9`, `-(18)9`) map by hand; output type is text |
| 30 | `x (TITLE '...')` | drop | no equivalent | display only |
| 31 | `CAST(x AS type FORMAT '...')` | `to_date(x, 'pattern')` / `to_timestamp` / `cast` | edge case | pattern letters differ (`MM` months, `MI` minutes on Teradata vs `mm` minutes on Spark) |
| 32 | `x (DATE)` / `x (INTEGER)` (Teradata-style cast) | `CAST(x AS DATE)` / `CAST(x AS INT)` | edge case | Teradata `(INTEGER)` on a decimal truncates; Spark `CAST` also truncates toward zero; on strings Teradata errors where Spark returns NULL: use `try_cast` only where the source tolerated bad input via error tables |
| 33 | `TRYCAST(x AS t)` | `try_cast(x AS t)` | same | |
| 34 | `TO_CHAR(x, fmt)` / `TO_DATE` / `TO_NUMBER` / `TO_TIMESTAMP` | `date_format` / `to_date` / `cast` / `to_timestamp` | edge case | Oracle-compatible token set on Teradata; map tokens by hand |
| 35 | `CURRENT_DATE` | `current_date()` | edge case | Teradata evaluates once per request; session time zone decides the day boundary on both |
| 36 | `CURRENT_TIMESTAMP(n)` / `CURRENT_TIME` | `current_timestamp()` (precision from the column) | edge case | `(0)` truncation is a cast on the target; `CURRENT_TIME` -> `date_format(current_timestamp(), 'HH:mm:ss')` |
| 37 | `DATE` (keyword as value) | `current_date()` | same | Teradata `DATE` in a `DEFAULT` or expression means today |
| 38 | `d + n` / `d - n` (DATE integer arithmetic) | `date_add(d, n)` / `date_sub(d, n)` | edge case | Spark `d + n` also works for `DATE + INT` but the explicit form is preferred; `d + 1.5` errors on Spark |
| 39 | `d1 - d2` (DATE) | `datediff(d1, d2)` | same | integer days on both |
| 40 | `ts1 - ts2 DAY(4) TO SECOND` / `(ts1 - ts2) HOUR TO MINUTE` | `ts1 - ts2` (day-time interval) or `unix_timestamp(ts1) - unix_timestamp(ts2)` for seconds | edge case | Teradata errors on overflow of the leading field; Spark does not |
| 41 | `ADD_MONTHS(d, n)` | `add_months(d, n)` | edge case | both clamp to month-end (`ADD_MONTHS('2024-01-31', 1) = 2024-02-29`); Teradata keeps the time part of a timestamp: same |
| 42 | `EXTRACT(YEAR/MONTH/DAY/HOUR/MINUTE/SECOND FROM x)` | `extract(... FROM x)` / `year(x)` / `month(x)` | edge case | `EXTRACT(SECOND ...)` returns `DECIMAL(8,6)` on Teradata, `DECIMAL(8,6)` on Spark: same; `EXTRACT(TIMEZONE_HOUR)` no equivalent |
| 43 | `LAST_DAY(d)` | `last_day(d)` | same | |
| 44 | `NEXT_DAY(d, 'MONDAY')` | `next_day(d, 'MO')` | edge case | day-name token differs |
| 45 | `TRUNC(d, 'MM')` / `TRUNC(d, 'YYYY')` / `TRUNC(d, 'IW')` | `trunc(d, 'MM')` / `trunc(d, 'YYYY')` / `date_trunc('WEEK', d)` | edge case | `'IW'` (ISO week) -> Monday-start week on Spark too |
| 46 | `TD_DAY_OF_WEEK(d)` / `TD_DAY_OF_YEAR` / `TD_WEEK_OF_YEAR` | `dayofweek(d)` / `dayofyear` / `weekofyear` | edge case | `TD_DAY_OF_WEEK` Sunday = 1 (matches `dayofweek`); `TD_WEEK_OF_YEAR` week 0 exists on Teradata, `weekofyear` is ISO: Tier 3 diffs in the first week of January |
| 47 | `DATE '2024-01-31'` / `TIMESTAMP '...'` literals | same | same | |
| 48 | `INTERVAL '1' YEAR` / `INTERVAL '3' DAY` | `INTERVAL 1 YEAR` / `INTERVAL 3 DAY` | same | quoted-number form also accepted on Spark |
| 49 | `x / y` on integers | `x DIV y` (truncating) or `CAST(x AS DECIMAL) / y` | edge case | Teradata integer division truncates toward zero; Spark `/` returns `DOUBLE`. Choose per expression; the fixture's `PCT_OF_REGION_DEPOSITS` needs the decimal form |
| 50 | `x MOD y` | `x % y` / `mod(x, y)` | edge case | sign follows the dividend on both |
| 51 | `x ** y` | `power(x, y)` | same | |
| 52 | `ROUND(x, n)` | `round(x, n)` | edge case | Teradata rounds half-even in ANSI mode unless `RoundHalfwayMagUp` is set; Spark `round` is half-up, `bround` is half-even: pick per the session setting and neutralise with `decimal_round` (§7) |
| 53 | `TRUNC(x, n)` (numeric) | `truncate`-free: `floor(x * pow(10, n)) / pow(10, n)` for positives or `cast(x as decimal(p, n))` semantics | edge case | negative numbers: use `sign(x) * floor(abs(x) * pow(10,n)) / pow(10,n)` |
| 54 | `ABS`, `SQRT`, `EXP`, `LN`, `LOG`, `CEILING`/`CEIL`, `FLOOR`, `SIGN` | same names | same | `LOG` is base 10 on Teradata: `log10` |
| 55 | `RANDOM(lo, hi)` | `floor(rand() * (hi - lo + 1)) + lo` | no equivalent (non-deterministic) | exclude from recon |
| 56 | `HASHROW(cols)` / `HASHBUCKET(HASHROW(...))` / `HASHAMP` | `hash(cols)` / `xxhash64` / none | no equivalent | different hash function: recompute on the target and exclude from Tier 3; `HASHAMP`/`HASHBUCKET` are physical-distribution diagnostics, drop |
| 57 | `CSUM(x, k)` | `SUM(x) OVER (ORDER BY k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)` | edge case | `CSUM` inside a `GROUP BY` query orders by the *group* key; add `PARTITION BY` for the intended reset (example 02) |
| 58 | `MAVG(x, n, k)` | `AVG(x) OVER (ORDER BY k ROWS BETWEEN n-1 PRECEDING AND CURRENT ROW)` | edge case | **n-1** preceding: `MAVG(v, 3, k)` is a 3-row frame (fixture `verify/checks/20_branch_performance.sql`) |
| 59 | `MSUM(x, n, k)` / `MDIFF(x, n, k)` / `MLINREG` | `SUM(x) OVER (... ROWS BETWEEN n-1 PRECEDING AND CURRENT ROW)` / `x - LAG(x, n) OVER (ORDER BY k)` / no equivalent | edge case | `MDIFF` yields NULL for the first n rows on both forms |
| 60 | `RANK(x DESC)` (Teradata OLAP form) | `RANK() OVER (ORDER BY x DESC)` | same | |
| 61 | `RANK() OVER`, `DENSE_RANK`, `ROW_NUMBER`, `PERCENT_RANK`, `NTILE` | same | same | `QUALIFY` on them is the idiom (row 2) |
| 62 | `SUM/AVG/MIN/MAX/COUNT ... OVER (PARTITION BY ... ORDER BY ... ROWS ...)` | same | edge case | default frame with `ORDER BY` is `RANGE UNBOUNDED PRECEDING` on both; Teradata requires `ROWS` for cumulative aggregates on some versions: state frames explicitly always |
| 63 | `RESET WHEN <cond>` in `OVER` | no equivalent; emulate with a running `SUM(CASE WHEN cond THEN 1 ELSE 0 END) OVER (...)` as an extra `PARTITION BY` key | no equivalent | two-pass CTE; Tier 3 |
| 64 | `AVG(int_col)` | `AVG(CAST(int_col AS DECIMAL(...)))` or accept `DOUBLE` | edge case | Teradata returns `INTEGER` (truncated) for integer input; Spark returns `DOUBLE`. Pick per consumer and canonicalise with `decimal_round` |
| 65 | `SUM(decimal)` | `SUM(decimal)` | edge case | result precision rules differ (§4 `DECIMAL(38)` row) |
| 66 | `COUNT(*)` / `COUNT(DISTINCT x)` | same | edge case | `COUNT(DISTINCT s)` on NOT CASESPECIFIC strings counts case variants once; use the column collation (§7) |
| 67 | `MIN/MAX(string)` | same | edge case | collation decides the winner between `'a'` and `'A'` |
| 68 | `STDDEV_SAMP`, `STDDEV_POP`, `VAR_SAMP`, `VAR_POP`, `CORR`, `COVAR_*`, `REGR_*` | same names | same | |
| 69 | `KURTOSIS`, `SKEW` | `kurtosis`, `skewness` | edge case | Teradata `SKEW` is sample-corrected; Spark `skewness` is population: Tier 4 |
| 70 | `GROUPING SETS` / `ROLLUP` / `CUBE` / `GROUPING(x)` | same | same | |
| 71 | `PIVOT` / `UNPIVOT` (TD 16.20+) | `PIVOT` / `UNPIVOT` | edge case | Teradata `PIVOT` requires explicit `IN` list: same shape on Spark |
| 72 | `TD_UNPIVOT`, `TD_SYSFNLIB.*` | `stack` / `UNPIVOT` / hand-convert | edge case | per function |
| 73 | `XMLAGG(x ORDER BY k) (VARCHAR(n))` (string aggregation idiom) | `array_join(array_agg(x) ... )` with ordering via `sort_array` or `collect_list` over an ordered window | edge case | ordering inside `array_agg` is not guaranteed without the window form; trailing separator handling differs |
| 74 | `LISTAGG(x, ',') WITHIN GROUP (ORDER BY k)` (TD 17+) | `listagg(x, ',') WITHIN GROUP (ORDER BY k)` or `array_join(...)` | edge case | verify `listagg` availability on the target runtime with `databricks-core` tooling before relying on it |
| 75 | `STRTOK(s, delim, n)` | `split(s, delimRegex)[n - 1]` | edge case | `STRTOK` treats consecutive delimiters as one and is 1-based; `split` keeps empty tokens and is 0-based |
| 76 | `STRTOK_SPLIT_TO_TABLE` | `explode(split(...))` with `posexplode` for the token number | edge case | table-function form becomes a lateral `explode` |
| 77 | `NORMALIZE` (PERIOD coalescing) / `P_INTERSECT` / `P_NORMALIZE` / `OVERLAPS` | gaps-and-islands rewrite over `_BEGIN`/`_END` columns; `a_begin < b_end AND b_begin < a_end` | no equivalent | §4 PERIOD row; Tier 3 on the decomposed columns |
| 78 | `BEGIN(p)` / `END(p)` / `PERIOD(a, b)` | `p_BEGIN` / `p_END` columns | no equivalent | see above |
| 79 | `EXPAND ON` | `explode(sequence(begin, end, interval))` | no equivalent | row-expansion of periods |
| 80 | `col.JSONExtractValue('$.a')` / `JSON_TABLE` | `get_json_object(col, '$.a')` / `from_json` + lateral view | edge case | Teradata returns NULL on malformed paths; `get_json_object` also returns NULL |
| 81 | `NEW JSON(...)` | `to_json(named_struct(...))` | edge case | key ordering |
| 82 | `MERGE INTO t USING s ON ... WHEN MATCHED THEN UPD ... WHEN NOT MATCHED THEN INS ...` | `MERGE INTO ... WHEN MATCHED THEN UPDATE ... WHEN NOT MATCHED THEN INSERT ...` (`best-practices.md` "Quick Reference") | edge case | Teradata `MERGE` requires the `ON` to include the target PI and rejects duplicate source keys with an error; Delta `MERGE` also errors on multiple matches: pre-dedupe the source with `QUALIFY` |
| 83 | `UPDATE t FROM s SET ... WHERE ...` / `UPD ... ELSE INS ...` (upsert) | `MERGE INTO` | edge case | Teradata `UPDATE ... FROM` with a multi-row match updates with one arbitrary row; Delta errors: dedupe |
| 84 | `DELETE t FROM s WHERE ...` / `DEL t ALL` | `DELETE FROM t WHERE EXISTS (...)` / `DELETE FROM t` (or `TRUNCATE TABLE t`) | same | |
| 85 | `INSERT INTO t SEL ...` (positional) | `INSERT INTO t SELECT ...` (positional) / `INSERT INTO t BY NAME` | edge case | Teradata inserts positionally; Delta positional too, but `BY NAME` is safer after column-order drift |
| 86 | `CREATE TABLE t AS (SELECT ...) WITH DATA [AND STATS]` | `CREATE OR REPLACE TABLE t AS SELECT ...` (`best-practices.md` "Quick Reference") | same | `AND STATS` -> `ANALYZE TABLE ... COMPUTE STATISTICS` in a maintenance task |
| 87 | `CREATE TABLE t AS s WITH NO DATA` | `CREATE TABLE t AS SELECT * FROM s WHERE 1 = 0` (or `LIKE`) | same | PI/PPI not copied: reapply `CLUSTER BY` per §4 |
| 88 | `CREATE VOLATILE TABLE vt ... ON COMMIT PRESERVE ROWS` | `CREATE TEMPORARY TABLE` (`databricks-dbsql` `SKILL.md` table "Temp Tables"; `references/materialized-views-pipes.md` "Temp Tables") or `CREATE TEMPORARY VIEW` when only read once | edge case | session-scoped on both; `ON COMMIT DELETE ROWS` has no equivalent (rows persist for the session): drop rows explicitly if the script relies on it |
| 89 | `CREATE GLOBAL TEMPORARY TABLE` (definition persists, rows per session) | persistent Delta table + session key column, or `CREATE TEMPORARY TABLE` created by the unit | edge case | choose per script; record it |
| 90 | `CREATE [UNIQUE] PRIMARY INDEX (...)` / `PARTITION BY RANGE_N(...)` / `CASE_N` | `CLUSTER BY (cols)` (liquid clustering, `best-practices.md` "Liquid Clustering vs Traditional Partitioning") | no equivalent (physical) | uniqueness of a UPI is *not* enforced on Delta: Tier 1 `count vs count(distinct)` check (example 01) |
| 91 | `CREATE JOIN INDEX` / `CREATE HASH INDEX` | materialized view (`databricks-dbsql` `references/materialized-views-pipes.md`) or drop | no equivalent | optimizer artefacts; keep only when a consumer queries the JI directly |
| 92 | `COLLECT STATISTICS ON t COLUMN (c)` | `ANALYZE TABLE t COMPUTE STATISTICS FOR COLUMNS c` (`best-practices.md` "Quick Reference") | same | move out of procedure bodies into a maintenance job task |
| 93 | `LOCKING ROW FOR ACCESS` / `LOCKING TABLE ... FOR ...` | drop | no equivalent | Delta readers never block |
| 94 | `EXEC db.macro(args)` | `CALL db.proc(args)` (`sql-scripting.md` "CALL") or inline the SQL as a view/CTE | edge case | macro with several `SELECT`s returns several result sets; a procedure returns at most one, so materialise (example 05) |
| 95 | `HELP TABLE` / `HELP COLUMN` / `SHOW TABLE` | `DESCRIBE TABLE EXTENDED` / `SHOW CREATE TABLE` | same | census only |
| 96 | `SELECT ... FROM t1, t2 WHERE t1.k = t2.k (+)`-style / `t1 LEFT OUTER JOIN t2` | `LEFT JOIN` | same | Teradata has no `(+)`; only the ANSI form appears |
| 97 | `SELECT ... WHERE x IN (SEL ...) ` / `NOT IN` with NULLs | same | edge case | `NOT IN` with a NULL in the subquery returns no rows on both |
| 98 | `BETWEEN`, `IS [NOT] NULL`, `EXISTS`, `ANY/SOME/ALL` | same | same | |
| 99 | `x <> y` on strings with trailing blanks | `rtrim(x) <> rtrim(y)` or `COLLATE UTF8_LCASE_RTRIM`/`UTF8_BINARY_RTRIM` | edge case | Teradata ignores trailing blanks in `=`/`<>` on `CHAR` and `VARCHAR`; Delta does not (§7) |
| 100 | `CHARACTER SET`, `TRANSLATE(s USING LATIN_TO_UNICODE)` | drop / `s` | no equivalent | strings are UTF-8 on the target |
| 101 | `ACTIVITY_COUNT` (SPL) / `ACTIVITYCOUNT` (BTEQ) | `SELECT COUNT(*)` on the affected predicate, or a `DECLARE`d count | no equivalent (no cited row-count register) | example 04 |
| 102 | `SQLCODE` / `SQLSTATE` (SPL) | handler on `SQLEXCEPTION` / `SQLSTATE 'xxxxx'` (`sql-scripting.md` "Handler Declaration"); no cited read of the value | edge case | write a fixed code and message in the handler (examples 04, 06) |

## 6. Procedural-construct map

Order of preference: DBSQL SQL scripting / `CREATE PROCEDURE` first, Lakeflow Jobs task control flow second (for
BTEQ-level flow), PySpark last (only where the body leaves SQL: file system, `.OS`, non-SQL parsing). Citations are
to `databricks-dbsql` `references/sql-scripting.md` unless stated.

| Construct | Teradata form | Databricks target | Cite | Notes |
|---|---|---|---|---|
| Procedure | `REPLACE PROCEDURE db.p(IN a T, OUT b T, INOUT c T) BEGIN ... END;` | `CREATE OR REPLACE PROCEDURE cat.sch.p(IN a T, OUT b T, INOUT c T) LANGUAGE SQL SQL SECURITY INVOKER MODIFIES SQL DATA AS BEGIN ... END;` | "Stored Procedures / CREATE PROCEDURE" (public preview; `DEFAULT` not allowed on `OUT`; defaults must be trailing) | only `SQL SECURITY INVOKER` is cited: Teradata definer-rights procedures (`SQL SECURITY OWNER/CREATOR`) need the caller granted the body's privileges (D8); `DYNAMIC RESULT SETS` has no equivalent |
| Output parameters / return codes | `OUT p_return_code INTEGER; SET p_return_code = SQLCODE` | `OUT p_return_code INT` set explicitly in the handler | "CREATE PROCEDURE", "Handler Declaration" | return code convention: 0 / -1 / caller-defined; job task fails on `SIGNAL`, not on a code (example 04) |
| Macro (single statement) | `REPLACE MACRO m(p T DEFAULT v) AS (SEL ... WHERE c = :p;)` | view with the parameter folded in, or a SQL UDF / procedure with `IN p T DEFAULT v` | "CREATE PROCEDURE" | `DEFAULT DATE` (non-constant) -> `DEFAULT NULL` + `COALESCE(p, current_date())` (example 05) |
| Macro (multi statement / multi result set) | several `SEL` in one macro | procedure writing tagged rows to a result table + one view per result set | "CREATE PROCEDURE"; `QUALIFY` per `best-practices.md` | example 05 |
| Compound statement / variables | `BEGIN DECLARE v T DEFAULT x; SET v = ...; END` | `BEGIN DECLARE v T DEFAULT x; SET v = ...; END` | "Compound Statements", "Variable Declaration", "Variable Assignment" | `SET v = (SELECT ...)` scalar-subquery form as in the "Stored Procedure Skeleton" |
| Conditionals | `IF c THEN ... ELSEIF ... ELSE ... END IF;`, `CASE ... END CASE;` | same | "Control Flow / IF", "CASE Statement" | |
| Loops | `WHILE c DO ... END WHILE;`, `LOOP ... END LOOP;`, `REPEAT ... UNTIL c END REPEAT;`, `label: ... LEAVE label; ITERATE label;` | same | "WHILE Loop", "LOOP Statement", "REPEAT Statement", "LEAVE and ITERATE" | example 06 |
| Cursors | `DECLARE c CURSOR FOR q; OPEN c; FETCH c INTO ...; CLOSE c;` with `CONTINUE HANDLER FOR NOT FOUND`; `FOR r AS c CURSOR FOR q DO ... END FOR;` | `FOR r AS q DO ... END FOR;` (no OPEN/FETCH/CLOSE) | "FOR Loop" | scrollable / `FOR UPDATE` cursors: rewrite as set-based DML; §11 flags any row-at-a-time loop for a set-based rewrite after recon (example 06) |
| Exception handling | `DECLARE EXIT\|CONTINUE HANDLER FOR SQLEXCEPTION\|SQLWARNING\|NOT FOUND\|SQLSTATE 'x' ...` | `DECLARE EXIT HANDLER FOR SQLEXCEPTION\|NOT FOUND\|SQLSTATE 'x'\|<condition>`; `DECLARE cond CONDITION FOR SQLSTATE 'x'` | "Condition Declaration", "Handler Declaration" | only `EXIT` handlers cited: a `CONTINUE` handler becomes a nested `BEGIN ... END` around the guarded statement with its own `EXIT` handler; `SQLWARNING` no equivalent |
| Raising errors | `SIGNAL SQLSTATE '75001' SET MESSAGE_TEXT = '...'`; `RESIGNAL` | same | "SIGNAL and RESIGNAL" | user SQLSTATE cannot start with `00`/`01`/`XX` |
| Transactions | `BT; ... ET;` (Teradata mode), `COMMIT`/`ROLLBACK` (ANSI mode), implicit per-request transaction | each statement is atomic; `BEGIN ATOMIC ... END` (preview) for a block; otherwise idempotent re-run design | "Multi-Statement Transactions / Overview and Current Status", "SQL Scripting Atomic Blocks" (requires `catalogManaged` tables, created with the property) | no `ROLLBACK` inside a handler (examples 04, 06); BTEQ `.SET SESSION TRANSACTION ANSI\|BTET` is a census fact |
| Dynamic SQL | `CALL DBC.SysExecSQL(v)`, `EXECUTE IMMEDIATE v`, `PREPARE`/`EXECUTE` with `USING` | `EXECUTE IMMEDIATE v [INTO vars] [USING args]` | "EXECUTE IMMEDIATE (Dynamic SQL)" | identifiers spliced from parameters stay INFERRED lineage; values go through `USING ?` (example 06); targets must be inside the unit's write scope (`target-routing` "Write scope") |
| Volatile / global temporary tables | `CREATE VOLATILE TABLE ... ON COMMIT PRESERVE ROWS` | `CREATE TEMPORARY TABLE` / `CREATE TEMPORARY VIEW` | `databricks-dbsql` `SKILL.md` "Temp Tables" row; `references/materialized-views-pipes.md` "Temp Tables" | §5 row 88; a BTEQ volatile table that carries state across `.IF` branches becomes a persisted control-table row (example 03) |
| Result sets from procedures | `DYNAMIC RESULT SETS n` + open cursor `WITH RETURN` | materialise into a table and read it after `CALL` | "CALL" | example 05 |
| Calling | `CALL db.p(:a, :b)`; `EXEC macro` | `CALL cat.sch.p(a, b)` | "CALL (Invoke a Procedure)" | from a job: SQL file task with the `CALL` (example 03 `03_call_customer_scd2.sql`) |
| BTEQ `.IF ERRORCODE <> 0 THEN .GOTO L` / `.LABEL L` / `.QUIT n` | script-level flow | Lakeflow Jobs task graph: each SQL block a `sql_task` on a file; `depends_on` + `run_if` (`ALL_SUCCESS`, `AT_LEAST_ONE_FAILED`, `ALL_DONE`, `NONE_FAILED`, `AT_LEAST_ONE_SUCCESS`, `ALL_FAILED`); `.QUIT n` -> run state + control-table status row | `databricks-jobs` `SKILL.md` and `references/task-types.md` (SQL task on a workspace file, `depends_on`, `run_if`); retries/timeouts in `references/notifications-monitoring.md` | not emulated line by line (example 03) |
| BTEQ `.IF ACTIVITYCOUNT = 0 THEN ...` | data-dependent branch on the *row count of the last request* | `SIGNAL` in the SQL block when the guard fails, downstream `run_if`; or an `IF` inside one scripting block. First decide what the source branch could actually observe (§7 "ACTIVITYCOUNT after an aggregate") | `databricks-jobs` `references/task-types.md`; `sql-scripting.md` "IF" | example 03 `01_staging_check.sql` (branch is dead on the source, so it is *not* re-created) |
| BTEQ `.SET ERRORLEVEL n SEVERITY m` / `.SET MAXERROR` | error-class tuning | job `timeout_seconds`, task `max_retries`; severities have no equivalent | `databricks-jobs` `references/notifications-monitoring.md` | record dropped severities |
| BTEQ `.EXPORT REPORT\|DATA FILE = f` / `.EXPORT RESET` | file consumer edge | Delta report table (consumers repointed) or a job task writing to a UC volume | `databricks-unity-catalog` `references/6-volumes.md` | example 03 `06_export_recon_report.sql` |
| BTEQ `.IMPORT DATA\|VARTEXT FILE = f` + `USING (...) INSERT` | small file load | `read_files` streaming table or a one-shot `INSERT ... SELECT FROM read_files(...)` | `databricks-pipelines` `references/auto-loader-sql.md` | large feeds go to §6 load-utility row |
| BTEQ `.RUN FILE = f` / `.OS cmd` / `.LOGON` / `.LOGOFF` / `.SET WIDTH` / `.SET SEPARATOR` | includes, shell, session | inline the included file; `.OS` -> a Python task or dropped (INFERRED); logon -> service principal (`target-routing` "Auth"); formatting -> drop | `databricks-jobs` `references/task-types.md` | |
| BTEQ `.REPEAT n` / `USING (...)` parameterised requests | row-by-row import loop | set-based `INSERT ... SELECT` | — | |
| Shell `${VAR}` / `.SET` substitution | environment | DABs variables `${var.x}` at deploy time; job/task parameters at run time only where the cited task type supports them | `databricks-dabs` `references/bundle-structure.md` (variables) | example 03 uses build-time `${catalog}.${schema}` |
| Triggers | `CREATE TRIGGER t AFTER INSERT ON s FOR EACH ROW ... (INSERT INTO audit ...)` | no trigger construct; fold the body into the writing unit's procedure (after the DML) or a downstream pipeline step; `FOR EACH ROW` -> set-based over the affected keys | `databricks-pipelines` `SKILL.md` (downstream step); `sql-scripting.md` (fold into procedure) | trigger fan-out scored in §11; every trigger is an INFERRED consumer edge until converted |
| Load utilities | TPT `LOAD`/`UPDATE`/`STREAM` operators, MLOAD `.DML LABEL ... DO INSERT FOR MISSING UPDATE ROWS`, FASTLOAD `BEGIN LOADING ... ERRORFILES` | `CREATE OR REFRESH STREAMING TABLE ... FROM STREAM read_files(...)`; upsert -> `CREATE FLOW ... AS AUTO CDC INTO ... KEYS ... SEQUENCE BY ... STORED AS SCD TYPE 1`; error tables -> quarantine table on `_rescued_data` + `CONSTRAINT ... EXPECT ... ON VIOLATION DROP ROW\|FAIL UPDATE`; `ErrorLimit` -> `FAIL UPDATE` on the invariant + count check | `databricks-pipelines` `references/auto-loader-sql.md`, `options-csv.md`, `auto-cdc-sql.md`, `expectations-sql.md`, `streaming-patterns.md` "Rescue-Data Quarantine" | backfill vs incremental per the backfill plan (`backfill-planner`); `COPY INTO` named in earlier versions is not documented in the official skills read: route through `target-routing` before using it (example 07) |
| FASTEXPORT / TPT `EXPORT` | table -> file | job task writing to a UC volume, or consumer repoint | `databricks-unity-catalog` `references/6-volumes.md` | consumer edge |
| PySpark fallback | body leaves SQL (`.OS`, file parsing, UDFs) | notebook/Python task in the same job | `databricks-jobs` `references/task-types.md` | last resort, recorded per unit |

## 7. Known traps with recon signature

| Trap | Teradata does | Databricks does | Recon signature | Fix in converted code | Decision before first run |
|---|---|---|---|---|---|
| NOT CASESPECIFIC comparisons | `=`, `IN`, `LIKE`, `GROUP BY`, `DISTINCT`, joins fold case on NOT CASESPECIFIC columns (default in Teradata-mode sessions) | byte comparison under `UTF8_BINARY` | Tier 2 distinct-count drift and count excess on string keys; Tier 1 row-count shortfall on joins | declare affected columns `STRING COLLATE UTF8_LCASE` (`geospatial-collations.md` "Collation Types"); per-column `CASESPECIFIC` exceptions stay `UTF8_BINARY` | collation per column from `DBC.ColumnsV.UpperCaseFlag` + session mode, recorded in the mapping; `collation_casefold` enabled for those columns |
| `ACTIVITYCOUNT` after an aggregate | `SEL COUNT(*) ... ;` returns one row, so `ACTIVITYCOUNT` is 1 even for an empty table; `.IF ACTIVITYCOUNT = 0` after it fires only when the request errored | n/a (no register) | a converted guard on `COUNT(*) = 0` stops runs the source completed: Tier 1 on the control table (`COMPLETED` batch missing) and Tier 1 excess on the warning/log table for empty days | reproduce the reachable condition only (task failure -> `run_if: AT_LEAST_ONE_FAILED`); the intended zero-row guard is a business-logic correction | decision row in `06_decisions.md` before any zero-row guard is added (example 03) |
| Trailing-blank equality | `'ab' = 'ab '` is true for `CHAR` and `VARCHAR` | false | Tier 3 mismatches on padded keys; Tier 1 join shortfall | `rtrim` on load for `CHAR`; `UTF8_LCASE_RTRIM`/`UTF8_BINARY_RTRIM` collation where comparisons must stay padded-insensitive (`geospatial-collations.md` "Collation Modifiers") | `rstrip_spaces` on `CHAR` columns |
| SET table dedup | `SET` tables silently drop full-row duplicates on `INSERT ... SELECT` | Delta keeps every row | Tier 1 row-count excess on the target | `INSERT ... SELECT DISTINCT` or `QUALIFY ROW_NUMBER() OVER (PARTITION BY <all columns>) = 1` for tables declared `SET` | mapping notes `SET` tables; no canonicalization can hide it |
| UPI/USI uniqueness | `UNIQUE PRIMARY INDEX` rejects duplicates (error or error table) | no enforcement | Tier 1 `count(*) > count(distinct key)` | dedupe before `MERGE`; quarantine duplicates (example 07) | which tables had UPIs (from `DBC.IndicesV.UniqueFlag`) |
| Decimal rounding | half-even (ANSI default) or half-up (`RoundHalfwayMagUp`) per DBS Control | `round` half-up, `bround` half-even | Tier 3 last-digit diffs; Tier 2 sum drift by a few units in the last place | use `bround` where the source was half-even | `decimal_round` at the estate's finest scale (§8); `mode` recorded (`half_even` here); per-column slack only via the tolerance record's `numeric_abs_tol` |
| Integer division | `x / y` truncates for integer operands | returns `DOUBLE` | Tier 2 sum drift, Tier 3 fractional values where the source had integers | `DIV` or explicit `CAST` per expression (§5 row 49) | none |
| `AVG` on integers | integer result | double | as above | cast per consumer | `decimal_round` |
| FORMAT-driven implicit casts | `x (FORMAT 'YYYYMMDD') (CHAR(8))` yields text; `WHERE date_col = '2024-01-31'` casts via the column format | no format-aware casting | Tier 3 on derived keys (`TRANSACTION_DATE_KEY`); silent NULLs or errors on string-to-date compares | explicit `date_format`/`to_date` with the pattern translated (§5 rows 29-31, example 04) | none |
| `TIMESTAMP(6)` precision | microseconds | microseconds, but sources feeding via JDBC/CSV often truncate | Tier 3 timestamp diffs below 1 ms | none in code | `datetime_utc_truncate_ms` on both sides |
| Session time zone | timestamps stored without zone, interpreted in session zone | `TIMESTAMP` normalised to UTC in session zone | Tier 3 whole-hour offsets; Tier 1 day-boundary drift on `CURRENT_DATE` filters | pin the session time zone in the unit; consider `TIMESTAMP_NTZ` (§4) | mapping records the legacy session zone |
| `MAVG`/`MSUM` window width | `n` rows including current | frame written as `n PRECEDING` covers `n + 1` rows | Tier 4 checksum mismatch (fixture `20_branch_performance.sum_moving_avg_volume = 6789480.28` with the correct frame) | `ROWS BETWEEN n-1 PRECEDING AND CURRENT ROW` (example 02) | none |
| `CSUM` inside `GROUP BY` | orders by the grouped key, resets per statement only | window needs `PARTITION BY` to reset | Tier 2 sum drift on cumulative columns | CTE + explicit partition (example 02) | none |
| `ADD_MONTHS` month-end | clamps to month-end | `add_months` clamps too | none when mapped directly; Tier 3 if rewritten as `+ INTERVAL 1 MONTH` on a `TIMESTAMP` (does not clamp) | keep `add_months` | none |
| `DATE` integer arithmetic | `d + 1`, `d - d` on `DATE` | `date_add`, `datediff` | Tier 3 off-by-one where `DATE - 1` was written on a timestamp | explicit functions (§5 rows 38-39) | none |
| `SUBSTR` position 0 / negative | window shift | different window | Tier 3 on derived strings | rewrite positions | none |
| `STRTOK` empty tokens | collapses consecutive delimiters | `split` keeps them | Tier 3 on parsed fields | filter empty tokens | none |
| Multi-result-set macros | several result sets to the client | one result | Tier 1 on the consumer: only the last set present | tagged result table + views (example 05) | none |
| `ACTIVITY_COUNT` logic | row count of the previous request | no cited register | Tier 1 on control/log tables (counts 0 or NULL) | scoped `COUNT(*)` (example 04) | none |
| BT/ET atomicity | multi-statement rollback | per-statement atomicity unless `BEGIN ATOMIC` (preview, `catalogManaged`) | Tier 2 conservation drift after a failed run (rows in archive and in source, example 06) | idempotent re-run design; `BEGIN ATOMIC` where the preview is acceptable | mapping states which units rely on rollback |
| Error tables / `ErrorLimit` | rejected rows land in `_ET`/`_UV` tables; job aborts at the limit | rows fail the pipeline or are dropped | Tier 1 `source lines = loaded + quarantined` per file | quarantine table + expectations (example 07) | reject-count reconciliation is part of the unit's recon plan |
| Volatile tables carrying state across `.IF` branches | script-local table survives the whole script | temp table lives in one session/task | Tier 1 on control tables (status never `COMPLETED`/`FAILED`) | persist the state row (example 03) | none |
| `PERIOD` types | typed period with exclusive end | two columns | Tier 3 on `_END` when the loader made it inclusive | keep exclusive end | harness gap: `period_decompose` |
| `HASHROW`-derived columns | Teradata hash | different hash | Tier 3 100% mismatch on the column | recompute on the target; exclude from Tier 3 | mapping lists the column as recomputed |
| `SAMPLE` outputs | AMP-stratified sample | `TABLESAMPLE` | Tier 4 never matches row-for-row | compare distributions only | tolerance record marks the op as distribution-compared |
| Identity values | per-AMP batches (gaps, no order) | per-task batches (gaps, no order) | Tier 3 on the surrogate | join on natural key; exclude surrogate | mapping marks identity columns |
| `COUNT(*)` on views with `LOCKING ROW FOR ACCESS` | dirty reads possible during loads | snapshot isolation | Tier 1 transient drift during live window | schedule recon outside loads | none |

## 8. Canonicalization rules

`canonicalization.json` (loads with `recon.config.load_canon_rules`; six rules, all implemented in
`recon/canon.py`):

| Rule | `applies_to` | params | Why (row in §4/§7) |
|---|---|---|---|
| `collation_casefold` | `CHAR,VARCHAR->STRING` | `enabled_if`: column is NOT CASESPECIFIC on the source; disable per column where DDL says `CASESPECIFIC` and the target stays `UTF8_BINARY` | NOT CASESPECIFIC trap: compares case-folded on both sides so a correctly collated target shows no drift, and a mis-collated one still shows Tier 2 distinct drift because the *source* aggregate is computed by Teradata under its own rule |
| `decimal_round` | `DECIMAL,NUMBER->DECIMAL` | `mode: half_even`, `places: 8` | half-even vs half-up and intermediate-precision differences. The harness keys rules by name (`Canonicalizer._by_name`), so one `places` serves every decimal column of the profile: it is set to the **finest** source scale in the estate (`DECIMAL(18,8)` `EXCHANGE_RATE`), which is a no-op for `DECIMAL(15,2)`/`DECIMAL(18,2)` money and never hides a sub-cent or sub-pip difference. Scale-widening on `AVG`/derived columns is absorbed by the tolerance record's `numeric_abs_tol` (human-approved), not by rounding to cents. `mode` matches the source's DBS Control setting (ANSI default half-even) |
| `rstrip_spaces` | `CHAR->STRING` | — | `CHAR(n)` blank padding; Teradata's own comparisons ignore trailing blanks, so stripping both sides reproduces the source semantics |
| `datetime_utc_truncate_ms` | `TIMESTAMP(6),TIMESTAMP(6) WITH TIME ZONE->TIMESTAMP` | — | `TIMESTAMP(6)` microseconds vs the engagement's millisecond grain and JDBC/CSV truncation; also normalises the zone on `WITH TIME ZONE` columns |
| `null_missing_equiv` | `*` | — | `.EXPORT`/`.IMPORT` round-trips and `NullColumns = 'Y'` loaders turn empty fields into NULL; a column absent from a result set on one side equals NULL |
| `identity` | `*` | — | default for every other column |

Not used and why: `empty_string_is_null` (Teradata distinguishes `''` from NULL; enable only for a unit whose
loader used `NullColumns`/`nullValue => ''` on both sides), `datetime_grid_333` (SQL Server only), `uuid_normalize`
(no UUID type on Teradata).

Harness gaps (filed in the PR body, never faked here): `period_decompose` (compare a `PERIOD` value against its
`_BEGIN`/`_END` pair with exclusive-end semantics); `time_of_day_normalize` (compare Teradata `TIME(n)` against the
string mapping in §4); per-field rule params (a field mapping can name `decimal_round` but cannot override its
`places`, so a profile has one decimal precision). Until they exist, PERIOD columns are compared as two ordinary
timestamp columns, `TIME` columns as strings, and `places` stays at the estate's finest scale, and the unit's recon
plan says so.

## 9. Governance discovery

All read-only, one statement per class, `SELECT` on the `DBC` views (usually PUBLIC; confirm in `07_access_checklist.md`).
Output feeds `.migration/08_governance_inventory.md`, the `governance-mapping` skill, and D8. Target-side
equivalents are cited from `databricks-unity-catalog` `references/1-access-control.md` and `4-fine-grained-access.md`;
routing through `target-routing` "Governance".

| Class | Teradata query | Watch | UC equivalent (cite) |
|---|---|---|---|
| Explicit grants | `SELECT UserName, DatabaseName, TableName, ColumnName, AccessRight, GrantAuthority, GrantorName FROM DBC.AllRightsV` | `AccessRight` codes (`R` select, `I` insert, `U` update, `D` delete, `E` execute, `CT` create table, `DT` drop table, `ST` show, ...); column-level rights (`ColumnName` not `All`) | `GRANT SELECT\|MODIFY\|EXECUTE\|CREATE TABLE ... ON TABLE\|SCHEMA\|FUNCTION ... TO` ("Privilege Reference", "GRANT / REVOKE (SQL)"); column-level rights have no grant equivalent: column masks or views ("Column Masks", "Dynamic Views") |
| Role-held grants | `SELECT RoleName, DatabaseName, TableName, ColumnName, AccessRight, GrantorName FROM DBC.AllRoleRightsV` | rights granted to a role, not a user | grant to a group ("Best Practices": prefer groups) |
| Roles and memberships | `SELECT RoleName, CreatorName, CommentString FROM DBC.RolesV`; `SELECT RoleName, Grantee, WhenGranted, DefaultRole, WithAdmin FROM DBC.RoleMembersV` | `DefaultRole` (active at logon), `WithAdmin` chains, nested roles | account/workspace groups; `MANAGE` for delegated grant admin ("Privilege Reference") |
| Users and profiles | `SELECT UserName, CreatorName, DefaultDataBase, ProfileName, CreateTimeStamp, LastAlterTimeStamp FROM DBC.UsersV`; `SELECT ProfileName, DefaultDB, SpoolSpace, TempSpace FROM DBC.ProfileInfoV` | service accounts (no human owner), `DefaultDataBase` implying unqualified-name resolution | service principals; spool/temp space have no equivalent (warehouse sizing) |
| Ownership-implied rights | `SELECT DatabaseName, OwnerName FROM DBC.DatabasesV`; `SELECT DatabaseName, TableName, CreatorName FROM DBC.TablesV` | owner/creator carry implicit rights `AllRightsV` does not list as grants | UC owner implicitly holds all privileges; `ALTER ... OWNER TO` a group ("Ownership") |
| `WITH GRANT OPTION` chains | `DBC.AllRightsV.GrantAuthority = 'Y'` | re-grant chains that vanish if the grantor is dropped | `MANAGE` privilege, not ownership ("Ownership") |
| PUBLIC grants | `... FROM DBC.AllRightsV WHERE UserName = 'PUBLIC'` | anything readable by all | grant to `account users` only by explicit decision at D8 |
| Row-level security | `SELECT ConstraintName, ConstraintValueName, ConstraintFunction... FROM DBC.SecConstraintsV`; `DBC.ConstraintFunctionsV`; `DBC.ColumnsV.ConstraintId` | RLS constraint functions (UDFs) per statement type | `ROW FILTER` functions ("Row Filters"); constraint UDF logic hand-converted |
| Views as security | views granting `SELECT` without base-table rights (`DBC.AllRightsV` on view vs table) | the common Teradata masking pattern | dynamic views with `is_account_group_member()` ("Dynamic Views", "Identity Functions") or column masks |
| Audit / access logging | `SELECT ... FROM DBC.AccLogRulesV`; `DBC.AccessLogV` (if access logging is on); `DBC.LogOnOffV` | which objects are logged; logon history | `system.access.audit` (`5-system-tables.md` "system.access.audit") |
| Query history (Tier 4 op selection) | `DBC.QryLogV` / `DBC.DBQLogTbl` (`QueryText`, `NumResultRows`, `UserName`, `StartTime`), `DBC.QryLogSQLV` for full text | DBQL may be disabled or purged; `QueryText` truncated at 200 chars in `QryLogV` | `system.query.history` (`5-system-tables.md`) |
| Zones / secure zones (TD 15.10+) | `DBC.ZonesV`, `DBC.ZoneGuestsV` | zone isolation | catalog boundary + grants |
| Object lineage evidence on the target | — | — | `system.access.table_lineage` / `column_lineage` for parity verification ("Access Schema") |

## 10. Lakebridge coverage delta

`--source-dialect teradata` (BladeBridge -> DBSQL; Switch -> SparkSQL for procedural bodies it rejects; see
`skills/lakebridge/SKILL.md` "Dialect and transpiler matrix"). SEEDED rows, mirrored in `skills/lakebridge/SKILL.md`
"Seeded coverage table":

| Class | SEEDED content |
|---|---|
| Converts | `SELECT`/`INSERT`/`UPDATE`/`DELETE`/`MERGE`, `QUALIFY`, CTEs, volatile tables to temp views, BTEQ SQL bodies, `SEL`/`INS`/`UPD`/`DEL` abbreviations, `NULLIFZERO`/`ZEROIFNULL`, `ADD_MONTHS`, `CREATE TABLE ... AS ... WITH DATA`, `LOCKING`/`COLLECT STATISTICS`/`COMPRESS` dropped |
| Mangles silently (recon signature) | `NOT CASESPECIFIC` comparisons (Tier 2 count/distinct drift on string keys); `SET` table dedup (Tier 1 row-count excess); half-even decimal rounding (Tier 3 last-digit); `FORMAT`/`TITLE`-driven implicit casts (Tier 3 derived keys); `ADD_MONTHS` month-end; integer division (Tier 2 sum drift); `MAVG`/`MSUM` frame width (Tier 4 checksum, fixture `20_branch_performance`); `CSUM` inside `GROUP BY` without `PARTITION BY` (Tier 2); `SUBSTR` position 0; `STRTOK` empty tokens; `TD_WEEK_OF_YEAR`; `DATE` integer arithmetic on timestamps; `CHAR` trailing-blank equality (Tier 1 join shortfall) |
| Rejects / hand-convert | BTEQ control flow (`.IF ERRORCODE`, `.LABEL`, `.QUIT`, `.EXPORT`/`.IMPORT`, `.OS`), SPL procedures with cursors/exception handlers/`DBC.SysExecSQL`/BT-ET, multi-statement macros, `PERIOD` types and `NORMALIZE`/`P_INTERSECT`, `RESET WHEN`, `EXPAND ON`, `HASHROW`-derived columns, TPT/MLOAD/FASTLOAD/FASTEXPORT control files, triggers, join/hash indexes, `DYNAMIC RESULT SETS` |

Rows move to CONFIRMED with a unit id and date when a unit's error log or recon result shows it.

## 11. Risk heuristics

Inputs per object (census computes; thresholds are suggestions for the inventory's complexity rank):

| Signal | Source | Low | Medium | High |
|---|---|---|---|---|
| Lines (body) | `SHOW`/file | < 100 | 100-400 | > 400 |
| Procedural depth | nesting of `BEGIN`/`IF`/`WHILE`/`FOR`/handlers | 0-1 | 2-3 | >= 4, or any cursor `FETCH` loop with DML in the body (row-at-a-time: convert as-is, then schedule a set-based rewrite) |
| Dynamic SQL | count of `DBC.SysExecSQL` / `EXECUTE IMMEDIATE` / `PREPARE` | 0 | 1-2 with literal table names | any with identifiers built from parameters (INFERRED lineage) |
| Vendor-function density | §5 rows marked edge case / no equivalent per 100 lines | < 2 | 2-5 | > 5, or any `RESET WHEN`, `NORMALIZE`, `EXPAND ON`, `HASHROW`, `PERIOD` |
| Collation exposure | NOT CASESPECIFIC string columns used as join/group keys | 0 | 1-3 | > 3, or session mode unknown |
| BTEQ flow | `.IF`/`.GOTO`/`.LABEL` count; `.OS`; `.RUN FILE` depth | 0 | 1-3 `.IF`, no `.OS` | > 3 `.IF`, any `.OS`, `.RUN FILE` depth > 1 |
| Result-set shape | macros with > 1 `SELECT`; procedures with `DYNAMIC RESULT SETS` | 0 | 1 | > 1 |
| Transactions | `BT`/`ET`, `ROLLBACK` in handlers | none | one block, idempotent body | rollback semantics relied upon across tables |
| Load utilities | operators per job; `ErrorLimit`; upsert `.DML` | single `LOAD` | `UPDATE`/upsert | `STREAM`, multiple `APPLY`, error-table consumers downstream |
| Trigger fan-out | triggers on the unit's write set x their write targets | 0 | 1 | >= 2, or trigger chains |
| External calls | UDFs (`DBC.TablesV.TableKind = 'F'`), `.OS`, `SYSLIB`, `TD_SYSFNLIB` beyond §5 | 0 | UDFs with SQL bodies | external (C/Java) UDFs |
| Data-type exposure | `PERIOD`, `TIME`, `DECIMAL(38)`, `TIMESTAMP WITH TIME ZONE`, `JSON`, `XML`, `ST_GEOMETRY` columns written | 0 | 1-2 | > 2 |
| Schedule coupling | scheduler predecessors/successors; `.QUIT` codes consumed downstream | 0-1 | 2-3 | > 3, or exit codes consumed by other jobs |

Rank = max signal class, then count of High signals as tiebreak. Any INFERRED lineage edge on the unit raises the
rank one class (`3-pipeline_analysis` batches across INFERRED edges conservatively).

## 12. Worked examples

Index of `examples/` (each directory: source file(s), converted file(s), `NOTE.md` with constructs and the recon tier
that catches a wrong conversion). Fixture-derived sources are verbatim copies from the fixture repository; two
directories are skill-authored minimal fixtures for constructs the fixture lacks.

| Dir | Source | Constructs | Recon tier that catches a wrong conversion |
|---|---|---|---|
| `01_ddl_set_table_casespecific/` | `ddl/tables/01_dim_customer.sql` (fixture) | `CREATE SET TABLE`, `NOT CASESPECIFIC` -> `COLLATE UTF8_LCASE`, `BYTEINT`, `DATE FORMAT`, `TIMESTAMP(0)`, `COMPRESS`, identity, UPI/NUPI/`PARTITION BY RANGE_N` -> `CLUSTER BY`, defaults moved to the loader | Tier 2 distinct drift (collation), Tier 1 count excess (SET dedup, UPI), Tier 3 on `CHAR` padding and identity |
| `02_view_olap_csum_mavg/` | `ddl/views/03_vw_branch_performance.sql` (fixture) | `CSUM`, `MAVG(.., 3, ..)`, `RANK` OLAP form, `NULLIFZERO`, `(FORMAT 'ZZ9.99')`, `LOCKING ROW FOR ACCESS`, integer division | Tier 4 checksum `verify/checks/20_branch_performance.sql` (`sum_moving_avg_volume = 6789480.28`), Tier 2 sum drift on cumulative columns, Tier 3 on rounding |
| `03_bteq_control_flow_to_job/` | `dml/scripts/bteq_daily_load.btq` (fixture) | `.IF ERRORCODE`/`.IF ACTIVITYCOUNT`/`.GOTO`/`.LABEL`/`.QUIT n`, volatile `VT_BATCH`, `.EXPORT`, `CALL`, `EXEC` -> Lakeflow Job with `depends_on`/`run_if`, `SIGNAL` on procedure return codes, control-table state; the dead `ACTIVITYCOUNT` branch is not re-created | Tier 1 on control/log tables and downstream row counts, Tier 2 on batch totals, Tier 4 on the exported report |
| `04_spl_exit_handler_out_params/` | `dml/stored_procedures/sp_load_daily_transactions.sql` (fixture) | `EXIT HANDLER FOR SQLEXCEPTION`, `OUT` parameters, `SQLCODE`, `ACTIVITY_COUNT`, `FORMAT` casts, `ZEROIFNULL`, error-table insert, `COLLECT STATISTICS` | Tier 1 `inserted + rejected = staged`, Tier 2 `sum(BASE_CURRENCY_AMOUNT)`, Tier 3 on `TRANSACTION_DATE_KEY` |
| `05_macro_multi_resultset/` | `dml/macros/macro_aml_screening.sql` (fixture) | multi-statement macro with `DEFAULT DATE` parameters, date arithmetic, `QUALIFY`, three result sets -> procedure + tagged result table + three views | Tier 1 per result set (collapse), Tier 2 on date windows, Tier 3 on decimal thresholds |
| `06_spl_cursor_loop_dynamic_sql/` | skill-authored (fixture schema) | `DECLARE CURSOR`/`OPEN`/`FETCH`/`CLOSE`, `CONTINUE HANDLER FOR NOT FOUND`, `WHILE`/`LEAVE`, `FOR ... CURSOR`, `CASE` statement, `DBC.SysExecSQL` -> `EXECUTE IMMEDIATE ... USING`, `BT`/`ET`, `SIGNAL`, `INOUT` | Tier 1 batch cap and archive counts, Tier 2 amount conservation across fact and archive, Tier 3 keyed diff on rows present in both |
| `07_tpt_mload_control_to_pipeline/` | skill-authored (fixture schema) | TPT `DEFINE SCHEMA`/`DATACONNECTOR`/`LOAD` operator, error tables, `ErrorLimit`, `APPLY` casts; MLOAD `.LAYOUT`/`.FILLER`/`.DML ... DO INSERT FOR MISSING UPDATE ROWS` -> `read_files` streaming tables, quarantine, expectations, `AUTO CDC INTO ... SCD TYPE 1` | Tier 1 `source lines = loaded + quarantined`, Tier 1 `count vs count(distinct)` for the lost UPI, Tier 3 on `EXCHANGE_RATE` ordering/scale |

Not verified live (file-based round-trip only): `DBC.*` column names and privilege requirements in §1 and §9; the
DDL of the fixture's staging/control/log tables; SQL-file `sql_task` executing a compound `BEGIN ... END`; reading
`SQLSTATE` in a handler; `BEGIN ATOMIC` for BT/ET; `_metadata.file_path` and `try_cast` inside pipeline streaming
tables; `listagg` availability. Each example's `NOTE.md` carries its own list.
