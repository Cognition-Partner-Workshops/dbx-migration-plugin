---
name: teradata-bteq
description: Source-dialect skill for Teradata estates (Teradata SQL, BTEQ scripts, SPL stored procedures and macros, TPT/MLOAD/FASTLOAD control files). Load it when converting Teradata objects to Databricks or reconciling a converted unit (its `canonicalization.json` feeds the harness). Target-side facts live in the official databricks skills behind `target-routing`.
---

# Teradata / BTEQ Dialect

## When to use / routing

Source-side half of the factory for Teradata. Use it for the type map, the construct map, the traps and the
canonicalization rules; everything Databricks-side is a pointer through `skills/target-routing/SKILL.md` to the
official skills: `databricks-dbsql` (`references/sql-scripting.md` for procedures, `references/geospatial-collations.md`
for collations, `references/best-practices.md` for DDL/MERGE/QUALIFY), `databricks-jobs` (BTEQ flow ->
`sql_task`/`depends_on`/`run_if`), `databricks-pipelines` (load utilities -> streaming tables / `AUTO CDC`),
`databricks-unity-catalog` (`DBC.*` grants -> UC grants, volumes), `databricks-lakebase` (OLTP column of the type map).
Load `databricks-core` first. Hardened against the fixture estate `uc-dw-migration-teradata-to-bigquery` (`ddl/`,
`dml/`, `verify/`; BigQuery half ignored). Lakebridge (`--source-dialect teradata`) coverage row: `skills/lakebridge/SKILL.md`.

## Type map

`loss`: none / precision / semantics. Lakebase column per `databricks-lakebase` `references/synced-tables.md` "Data Type Mapping".

| Teradata | Delta / UC | Lakebase | loss | Canon rule | Notes |
|---|---|---|---|---|---|
| `BYTEINT` | `TINYINT` | `SMALLINT` | none | `identity` | boolean-flag use stays numeric in like-for-like |
| `SMALLINT` / `INTEGER` / `BIGINT` | same (`INT` for `INTEGER`) | same | none | `identity` | |
| `DECIMAL(p,s)` / `NUMERIC`, p <= 38 | `DECIMAL(p,s)` | `NUMERIC` | precision in arithmetic | `decimal_round` | intermediate scale rules differ; `SUM` over `DECIMAL(38,s)` overflows on Spark |
| `NUMBER` (no p,s) | `DECIMAL(38,18)` or `DOUBLE` per profile | `NUMERIC` / `DOUBLE PRECISION` | precision | `decimal_round` | profile the column first |
| `FLOAT` / `REAL` / `DOUBLE PRECISION` | `DOUBLE` | `DOUBLE PRECISION` | none | `decimal_round` (recon only) | |
| `CHAR(n)` | `STRING COLLATE UTF8_BINARY_RTRIM` (`UTF8_LCASE_RTRIM` if also NOT CASESPECIFIC; "Collation Modifiers") | `TEXT` | semantics (padding) | `rstrip_spaces` | Teradata compares ignoring trailing blanks; loaders must not pad |
| `VARCHAR(n)` (NOT CASESPECIFIC, Teradata-mode default) | `STRING COLLATE UTF8_LCASE` ("Collation Types"); `UTF8_LCASE_RTRIM` where profiling finds trailing blanks | `TEXT` | semantics (case fold, padding) | `collation_casefold` + `rstrip_spaces` | Teradata blank-pads both operands of every character comparison, `VARCHAR` included; `DBC.ColumnsV.UpperCaseFlag = 'N'`; ANSI-mode sessions default to CASESPECIFIC: session mode is a census fact |
| `... CASESPECIFIC` | `STRING` (`UTF8_BINARY`; `UTF8_BINARY_RTRIM` where trailing blanks occur) | `TEXT` | none / padding | `identity` / `rstrip_spaces` | case axis and `RTRIM` axis are independent |
| `CHARACTER SET KANJISJIS` | `STRING` | `TEXT` | ordering | `identity` | Tier 4 report ordering only |
| `CLOB` / `JSON` / `XML` | `STRING` (`VARIANT` for JSON if profile allows) | `TEXT` | none (storage) | `identity` | `JSONExtractValue` -> `get_json_object`; XPath hand-converted |
| `BYTE(n)` / `VARBYTE(n)` / `BLOB` | `BINARY` | `BYTEA` | none | `identity` | `HASHROW` outputs: recompute, never migrate |
| `DATE` [`FORMAT '...'`] | `DATE` | `DATE` | none (display) | `identity` | `FORMAT` drops; every implicit text cast through it becomes explicit `date_format` |
| `TIME(n)` [`WITH TIME ZONE`] | `STRING` `'HH:mm:ss[.SSSSSS]'` | `TEXT` | semantics (no TIME type cited) | `identity` (harness gap `time_of_day_normalize`) | arithmetic moves to timestamps |
| `TIMESTAMP(0)` / `TIMESTAMP(6)` | `TIMESTAMP` (or `TIMESTAMP_NTZ`, chosen once per estate) | `TIMESTAMP WITH[OUT] TIME ZONE` | precision (us vs ms grain) | `datetime_utc_truncate_ms` | pin the legacy session time zone in the mapping |
| `TIMESTAMP(n) WITH TIME ZONE` | `TIMESTAMP` (UTC-normalised) | `TIMESTAMP WITH TIME ZONE` | semantics (offset dropped) | `datetime_utc_truncate_ms` | keep the offset as a column only if a consumer reads it |
| `INTERVAL ... TO ...` | `INTERVAL` (year-month / day-time) | `INTERVAL` | none for the two families | `identity` | |
| `PERIOD(DATE\|TIMESTAMP)` | `<col>_BEGIN`, `<col>_END` (end stays exclusive) | two columns | semantics (type lost) | none: harness gap `period_decompose`; compare the pair with `datetime_utc_truncate_ms` | `P_INTERSECT`/`OVERLAPS`/`NORMALIZE` become predicates |
| `ST_GEOMETRY` | `GEOMETRY` ("Geospatial Data Types") | unsupported | per function | `identity` | out of like-for-like scope |
| `GENERATED ALWAYS AS IDENTITY` | `GENERATED ALWAYS AS IDENTITY (START WITH .. INCREMENT BY ..)` | `BIGINT` | values differ | exclude from Tier 3; join on natural key | example 01 |
| `DEFAULT <expr>` | moved to the loader/`INSERT` | — | none | `identity` | example 01 |
| `COMPRESS` / `TITLE` / `FORMAT` / `FALLBACK` / `JOURNAL` / `CHECKSUM` | dropped with a mapping note | — | none | — | display/physical |

## Function and construct map

`sem`: same / edge / none. Expressions follow `databricks-dbsql` where cited; other builtins are plain Spark SQL
(verify with `databricks-core` tooling before relying on one not listed there).

| # | Teradata | Databricks SQL | sem | Edge case |
|---|---|---|---|---|
| 1 | `SEL` / `INS` / `UPD` / `DEL` | full keywords | same | |
| 2 | `QUALIFY <window predicate>` | `QUALIFY` (`best-practices.md`) | same | aliases from the select list allowed |
| 3 | `SAMPLE n` / `SAMPLE .1` | `TABLESAMPLE (n ROWS)` / `(10 PERCENT)` | edge | never reconcile sampled output row-for-row |
| 4 | `TOP n [WITH TIES]` | `LIMIT n` / `QUALIFY RANK() OVER (...) <= n` | edge | arbitrary without `ORDER BY` on both |
| 5 | `SELECT ... WITH BY` (BTEQ totals) | `GROUPING SETS` or separate query | none | Tier 4 formatting |
| 6 | `NULLIFZERO(x)` / `ZEROIFNULL(x)` / `NVL` | `NULLIF(x, 0)` / `COALESCE(x, 0)` / `COALESCE` | edge | cast `ZEROIFNULL` result when `x` is `DECIMAL` |
| 7 | `DECODE(x, a, r1, ..., d)` | `CASE x WHEN a THEN r1 ... ELSE d END` | edge | Teradata `DECODE` matches `NULL = NULL`: add `WHEN x IS NULL` |
| 8 | `INDEX(s, sub)` / `POSITION(sub IN s)` | `instr(s, sub)` / `position(sub, s)` | same | 1-based, 0 when absent |
| 9 | `SUBSTR(s, pos, len)` | `substr(s, pos, len)` | edge | `pos <= 0` shifts the window on Teradata (`SUBSTR('abc',0,2)='a'`), not on Spark |
| 10 | `CHAR_LENGTH` / `CHARACTERS(s)` | `length(s)` | edge | counts `CHAR(n)` padding on Teradata: `length(rtrim(s))` |
| 11 | `TRIM(s)` / `TRIM(LEADING\|TRAILING 'x' FROM s)` | `trim(s)` / `ltrim('x', s)` / `rtrim('x', s)` | edge | `ltrim([trimStr,] str)` (docs.databricks.com/aws/en/sql/language-manual/functions/ltrim); trims any char of the set |
| 12 | `s1 \|\| s2` | `s1 \|\| s2` / `concat` | edge | `CHAR` operands carry padding into the result |
| 13 | `OREPLACE` / `OTRANSLATE` | `replace` / `translate` | same | |
| 14 | `REGEXP_SUBSTR(s, re, pos, occ, flags)` / `REGEXP_REPLACE(...)` / `REGEXP_SIMILAR` | `regexp_extract(s, re, 0)` / `regexp_replace(s, re, rep)` / `RLIKE` | edge | occurrence/flags args dropped: fold `(?i)` into the pattern; Java regex escaping; `RLIKE` is boolean not 1/0 |
| 15 | `LIKE` on NOT CASESPECIFIC | `LIKE` | edge | case-insensitive on Teradata: column collation or `lower()` both sides |
| 16 | `s (CASESPECIFIC)` / `(NOT CASESPECIFIC)` | `s COLLATE UTF8_BINARY` / `UTF8_LCASE` ("Collation Precedence") | same | |
| 17 | `x (FORMAT '...')` / `(TITLE '...')` / `CAST(x AS t FORMAT '...')` | `date_format` / `format_number` / `lpad`; drop `TITLE`; `to_date(x, 'pattern')` | edge | tokens map by hand (`MI` minutes on Teradata, `mm` on Spark); output type is text |
| 18 | `x (INTEGER)` / `x (DATE)` (Teradata cast) | `CAST(x AS INT)` / `CAST(x AS DATE)` | edge | on bad strings Teradata errors, Spark returns NULL: `try_cast` only where the source used error tables |
| 19 | `TO_CHAR` / `TO_DATE` / `TO_NUMBER` | `date_format` / `to_date` / `cast` | edge | Oracle token set: map by hand |
| 20 | `CURRENT_DATE` / `DATE` keyword / `CURRENT_TIMESTAMP(n)` / `CURRENT_TIME` | `current_date()` / `current_timestamp()` / `date_format(current_timestamp(), 'HH:mm:ss')` | edge | `(0)` truncation is a cast on the target |
| 21 | `d + n` / `d - n` / `d1 - d2` (DATE) | `date_add(d, n)` / `date_sub` / `datediff(d1, d2)` | edge | `d + 1.5` errors on Spark |
| 22 | `ts1 - ts2 DAY(4) TO SECOND` | `ts1 - ts2` (day-time interval) or `unix_timestamp` difference | edge | Teradata errors on leading-field overflow |
| 23 | `ADD_MONTHS(d, n)` | `add_months(d, n)` | same | both clamp to month-end; `+ INTERVAL 1 MONTH` on a timestamp does not |
| 24 | `EXTRACT(part FROM x)` / `LAST_DAY` | same | edge | `EXTRACT(TIMEZONE_HOUR)` none |
| 25 | `NEXT_DAY(d, 'MONDAY')` | `next_day(d, 'MO')` | edge | token differs |
| 26 | `TRUNC(d, 'MM'\|'YYYY'\|'IW')` | `trunc(d, 'MM'\|'YYYY')` / `date_trunc('WEEK', d)` | edge | Monday-start weeks on both |
| 27 | `TD_DAY_OF_WEEK` / `TD_DAY_OF_YEAR` / `TD_WEEK_OF_YEAR` | `dayofweek` / `dayofyear` / `weekofyear` | edge | `TD_WEEK_OF_YEAR` has week 0, `weekofyear` is ISO: Tier 3 in early January |
| 28 | `INTERVAL '3' DAY` | `INTERVAL 3 DAY` | same | quoted form also accepted |
| 29 | `x / y` on integers | `x DIV y` or `CAST(x AS DECIMAL) / y` | edge | Teradata truncates, Spark returns `DOUBLE`; fixture `PCT_OF_REGION_DEPOSITS` needs the decimal form |
| 30 | `x MOD y` / `x ** y` / `LOG(x)` | `x % y` / `power` / `log10` | edge | `LOG` is base 10 on Teradata |
| 31 | `ROUND(x, n)` | `bround(x, n)` (half-even) or `round` (half-up) | edge | per DBS Control `RoundHalfwayMagUp`; neutralise with `decimal_round` |
| 32 | `TRUNC(x, n)` (numeric) | `sign(x) * floor(abs(x) * pow(10, n)) / pow(10, n)`, then cast back | edge | a narrowing decimal cast alone *rounds* the dropped digits |
| 33 | `RANDOM(lo, hi)` | `floor(rand() * (hi - lo + 1)) + lo` | none | exclude from recon |
| 34 | `HASHROW` / `HASHBUCKET` / `HASHAMP` | `hash` / `xxhash64` / drop | none | different hash: recompute, exclude from Tier 3 |
| 35 | `CSUM(x, k)` | `SUM(x) OVER (ORDER BY k ROWS UNBOUNDED PRECEDING)` | edge | inside `GROUP BY` orders by the group key; add `PARTITION BY` for the intended reset (example 02) |
| 36 | `MAVG(x, n, k)` / `MSUM` / `MDIFF` | `AVG/SUM(x) OVER (ORDER BY k ROWS BETWEEN n-1 PRECEDING AND CURRENT ROW)` / `x - LAG(x, n)` | edge | **n-1** preceding (fixture `verify/checks/20_branch_performance.sql`) |
| 37 | `RANK(x DESC)` (OLAP form) / window aggregates | `RANK() OVER (ORDER BY x DESC)` / same | edge | state `ROWS` frames explicitly always |
| 38 | `RESET WHEN <cond>` | running `SUM(CASE WHEN cond THEN 1 ELSE 0 END) OVER (...)` as extra partition key | none | two-pass CTE |
| 39 | `AVG(int_col)` | `AVG(CAST(int_col AS DECIMAL(...)))` | edge | Teradata truncates to integer, Spark returns `DOUBLE` |
| 40 | `COUNT(DISTINCT s)` / `MIN/MAX(string)` | same | edge | collation decides case variants and the winner |
| 41 | `SKEW` / `KURTOSIS` | `skewness` / `kurtosis` | edge | sample vs population: Tier 4 |
| 42 | `XMLAGG(x ORDER BY k)` (`TRIM(... (VARCHAR(n)))`) / `LISTAGG(x, ',') WITHIN GROUP (ORDER BY k)` | grouped `array_join(transform(array_sort(collect_list(struct(k, x))), s -> s.x), '')` / `listagg(x, ',') WITHIN GROUP (ORDER BY k)` | edge | one row per group, never a window; both skip NULLs; `XMLAGG` XML-escapes `<`/`&` and needs the sort key in the struct for a deterministic order; verify `listagg` availability; hand-convert when `x` is not plain text |
| 43 | `STRTOK(s, delim, n)` / `STRTOK_SPLIT_TO_TABLE` | `split(s, re)[n - 1]` / `posexplode(split(...))` | edge | `STRTOK` collapses consecutive delimiters and is 1-based |
| 44 | `NORMALIZE` / `P_INTERSECT` / `OVERLAPS` / `BEGIN(p)` / `END(p)` / `EXPAND ON` | gaps-and-islands over `_BEGIN`/`_END`; `a_begin < b_end AND b_begin < a_end`; `explode(sequence(...))` | none | Tier 3 on the decomposed columns |
| 45 | `MERGE INTO ... WHEN MATCHED THEN UPD ... WHEN NOT MATCHED THEN INS` | `MERGE INTO` (`best-practices.md` "Quick Reference") | edge | both reject duplicate source keys: pre-dedupe with `QUALIFY` |
| 46 | `UPDATE t FROM s ...` / `UPD ... ELSE INS` | `MERGE INTO` | edge | multi-row match picks an arbitrary row on Teradata, errors on Delta |
| 47 | `DELETE t FROM s WHERE ...` / `DEL t ALL` | `DELETE FROM t WHERE EXISTS (...)` / `DELETE FROM t` | same | |
| 48 | `INSERT INTO t SEL ...` | `INSERT INTO t SELECT ...` (`BY NAME` after column drift) | edge | positional on both |
| 49 | `CREATE TABLE t AS (...) WITH DATA [AND STATS]` / `AS s WITH NO DATA` | `CREATE OR REPLACE TABLE t AS SELECT` / `... WHERE 1 = 0` | same | `AND STATS` -> `ANALYZE TABLE` in a maintenance task |
| 50 | `CREATE VOLATILE TABLE ... ON COMMIT PRESERVE ROWS` / `GLOBAL TEMPORARY` | `CREATE TEMPORARY TABLE` / `TEMPORARY VIEW` (`databricks-dbsql` "Temp Tables") | edge | `ON COMMIT DELETE ROWS` none: delete explicitly; state crossing BTEQ `.IF` branches -> persisted row keyed by job run id |
| 51 | `[UNIQUE] PRIMARY INDEX` / `PARTITION BY RANGE_N\|CASE_N` | `CLUSTER BY (cols)` ("Liquid Clustering vs Traditional Partitioning") | none | UPI uniqueness not enforced: Tier 1 `count vs count(distinct)` (example 01) |
| 52 | `CREATE JOIN\|HASH INDEX` | materialized view or drop | none | keep only when queried directly |
| 53 | `COLLECT STATISTICS` / `LOCKING ROW FOR ACCESS` | `ANALYZE TABLE ... COMPUTE STATISTICS FOR COLUMNS` (maintenance job) / drop | same | Delta readers never block |
| 54 | `EXEC db.macro(args)` (single statement) | view with the parameter folded in, or procedure with `IN p T DEFAULT v` | edge | `DEFAULT DATE` -> `DEFAULT NULL` + `COALESCE(p, current_date())` |
| 55 | Multi-statement macro / `DYNAMIC RESULT SETS` | procedure writing a result table with a `RUN_ID`, published last; one view per result set on the latest published run | none | a procedure returns at most one result set |
| 56 | `REPLACE PROCEDURE db.p(IN a T, OUT b T, INOUT c T) BEGIN ... END` | `CREATE OR REPLACE PROCEDURE cat.sch.p(...) LANGUAGE SQL SQL SECURITY INVOKER MODIFIES SQL DATA AS BEGIN ... END` ("CREATE PROCEDURE") | edge | only `SQL SECURITY INVOKER` cited: definer-rights procedures need the caller granted the body's privileges |
| 57 | `DECLARE v T DEFAULT x; SET v = ...;` / `IF` / `CASE ... END CASE` / `WHILE` / `LOOP` / `REPEAT` / `LEAVE` / `ITERATE` | same (`sql-scripting.md` "Compound Statements", "Control Flow") | same | `SET v = (SELECT ...)` scalar form |
| 58 | `DECLARE c CURSOR FOR q; OPEN; FETCH; CLOSE` + `CONTINUE HANDLER FOR NOT FOUND`; `FOR r AS c CURSOR FOR q DO` | `FOR r AS q DO ... END FOR` ("FOR Loop") | edge | row-at-a-time loops flagged for a set-based rewrite after recon (example 03) |
| 59 | `DECLARE EXIT\|CONTINUE HANDLER FOR SQLEXCEPTION\|SQLWARNING\|NOT FOUND\|SQLSTATE 'x'` | `DECLARE EXIT HANDLER FOR SQLEXCEPTION\|NOT FOUND\|SQLSTATE 'x'\|<condition>` ("Handler Declaration") | edge | `CONTINUE` -> nested `BEGIN ... END` with its own `EXIT` handler; `SQLWARNING` none |
| 60 | `SET p_return_code = SQLCODE` / `SQLSTATE` read | fixed code set in the handler | edge | no cited read of the value (example 03) |
| 61 | `SIGNAL SQLSTATE '75001' SET MESSAGE_TEXT = ...` / `RESIGNAL` | same ("SIGNAL and RESIGNAL") | same | user SQLSTATE cannot start with `00`/`01`/`XX` |
| 62 | `BT; ... ET;` / `COMMIT` / `ROLLBACK` in a handler | per-statement atomicity; `BEGIN ATOMIC ... END` (preview, `catalogManaged` tables, "SQL Scripting Atomic Blocks"); otherwise idempotent re-run design | none | no `ROLLBACK` in a handler; lock scope of BT/ET is not replaced either (traps) |
| 63 | `CALL DBC.SysExecSQL(v)` / `EXECUTE IMMEDIATE` / `PREPARE ... USING` | `EXECUTE IMMEDIATE v [INTO vars] [USING args]` ("EXECUTE IMMEDIATE") | edge | identifiers from parameters: allowlist + INFERRED lineage; values via `USING ?` (example 03) |
| 64 | `ACTIVITY_COUNT` (SPL) / `ACTIVITYCOUNT` (BTEQ) | scoped `SELECT COUNT(*)` | none | no row-count register; see trap "ACTIVITYCOUNT after an aggregate" |
| 65 | BTEQ `.IF ERRORCODE <> 0 THEN .GOTO L` / `.LABEL L` / `.QUIT n` | Lakeflow Job: one `sql_task` per SQL block, `depends_on` + `run_if` (`ALL_SUCCESS`, `AT_LEAST_ONE_FAILED`, `ALL_DONE`, ...); `.QUIT n` -> run state + control-table status row (`databricks-jobs` `references/task-types.md`) | none | not emulated line by line |
| 66 | BTEQ `.IF ACTIVITYCOUNT = 0` | `SIGNAL` in the block + downstream `run_if`, or `IF` in one scripting block | none | decide first what the source branch could observe |
| 67 | BTEQ `.SET ERRORLEVEL` / `.SET MAXERROR` | task `max_retries`, job `timeout_seconds` (`references/notifications-monitoring.md`) | none | severities dropped, recorded |
| 68 | BTEQ `.EXPORT REPORT\|DATA FILE` / FASTEXPORT / TPT `EXPORT` | Delta report table or task writing to a UC volume (`databricks-unity-catalog` `references/6-volumes.md`) | none | consumer edge |
| 69 | BTEQ `.IMPORT ... FILE` + `USING (...) INSERT` / `.REPEAT` | `INSERT ... SELECT FROM read_files(...)` (`databricks-pipelines` `references/auto-loader-sql.md`) | none | set-based |
| 70 | BTEQ `.RUN FILE` / `.OS` / `.LOGON` / `.SET WIDTH` | inline the include; `.OS` -> Python task or dropped (INFERRED); logon -> service principal; formatting dropped | none | |
| 71 | Shell `${VAR}` / `.SET` substitution | DABs `${var.x}` at deploy; job `parameters` read as `:name` in a `sql_task` file (docs.databricks.com/aws/en/jobs/parameter-use) | none | |
| 72 | Triggers (`FOR EACH ROW`) | fold the body into the writing procedure after the DML, set-based over affected keys | none | INFERRED consumer edge until converted |
| 73 | TPT `LOAD`/`UPDATE`/`STREAM`, MLOAD `.DML ... DO INSERT FOR MISSING UPDATE ROWS`, FASTLOAD `ERRORFILES`/`ErrorLimit` | `CREATE OR REFRESH STREAMING TABLE ... FROM STREAM read_files(...)`; upsert -> `CREATE FLOW ... AS AUTO CDC INTO ... KEYS ... SEQUENCE BY ... STORED AS SCD TYPE 1`; error tables -> quarantine on `_rescued_data` + `CONSTRAINT ... EXPECT ... ON VIOLATION DROP ROW\|FAIL UPDATE` (`databricks-pipelines` `auto-loader-sql.md`, `auto-cdc-sql.md`, `expectations-sql.md`) | none | recon `source lines = loaded + quarantined`; dedupe keys (lost UPI) before `AUTO CDC` |

## Traps with recon signature

| Trap | Teradata | Databricks | Recon signature | Fix |
|---|---|---|---|---|
| NOT CASESPECIFIC comparisons | `=`, `IN`, `LIKE`, `GROUP BY`, `DISTINCT`, joins fold case | byte comparison | Tier 2 distinct drift / count excess on string keys; Tier 1 join shortfall | `COLLATE UTF8_LCASE` per column from `UpperCaseFlag` + session mode; `collation_casefold` |
| Trailing-blank equality | `'ab' = 'ab '` true for `CHAR` and `VARCHAR` | false | Tier 3 on padded keys; Tier 1 join shortfall | `_RTRIM` collations; `rstrip_spaces` (example 01) |
| SET table dedup / UPI | silent full-row dedup; UPI rejects duplicates | keeps every row; nothing enforced | Tier 1 count excess; `count(*) > count(distinct key)` | `SELECT DISTINCT` / `QUALIFY ROW_NUMBER() = 1`; dedupe before `MERGE` |
| Decimal rounding | half-even (ANSI default) or half-up per DBS Control | `round` half-up, `bround` half-even | Tier 3 last digit; Tier 2 sum drift in ULPs | `bround`; `decimal_round` at the estate's finest scale, slack only via the tolerance record |
| Integer division / `AVG(int)` | truncated integer | `DOUBLE` | Tier 2 sum drift, Tier 3 fractions | `DIV` / explicit `CAST` per expression |
| FORMAT-driven implicit casts | `x (FORMAT 'YYYYMMDD') (CHAR(8))` yields text; `date_col = '2024-01-31'` casts via the format | none | Tier 3 on derived keys; silent NULLs on string-date compares | explicit `date_format` / `to_date` with translated pattern |
| `TIMESTAMP(6)` / session zone | microseconds, zone-less in session zone | JDBC/CSV feeds truncate; UTC-normalised | Tier 3 sub-ms diffs; whole-hour offsets; Tier 1 day-boundary drift | `datetime_utc_truncate_ms`; pin the zone; consider `TIMESTAMP_NTZ` |
| `MAVG`/`MSUM` width, `CSUM` in `GROUP BY` | `n` rows incl. current; resets per statement | `n PRECEDING` is `n+1` rows; needs `PARTITION BY` | Tier 4 checksum (fixture `sum_moving_avg_volume = 6789480.28`); Tier 2 cumulative drift | `n-1 PRECEDING`; CTE + partition (example 02) |
| `ACTIVITYCOUNT` after an aggregate | `SEL COUNT(*)` returns one row, so `.IF ACTIVITYCOUNT = 0` fires only on error | no register | a converted `COUNT(*) = 0` guard stops runs the source completed: Tier 1 on control tables | reproduce only the reachable condition; a zero-row guard is a decision row |
| BT/ET atomicity and lock scope | multi-statement rollback; write locks to `ET` serialise callers | per-statement commits, no lock between statements | Tier 2 conservation drift after a failed run; Tier 1 shortfall when overlapping calls process the same rows | idempotent re-run, budget derived from committed rows stamped since the run's lock time (never an in-memory counter), explicit exclusion (`max_concurrent_runs: 1` or a seeded lock row shipped with the DDL) (example 03) |
| Multi-result-set macros | several result sets | one | Tier 1 on the consumer: only the last set present | result table + run ledger + one view per set |
| Error tables / `ErrorLimit` | rejects land in `_ET`/`_UV`; abort at limit | rows fail or drop | Tier 1 `source lines = loaded + quarantined` | quarantine table + expectations |
| `ADD_MONTHS`, `DATE` arithmetic, `SUBSTR` pos 0, `STRTOK`, `TD_WEEK_OF_YEAR`, `HASHROW`, identity | map rows 9, 21, 23, 27, 34, 43 | | Tier 3 on derived columns | as in the map; exclude hash/identity from Tier 3 |
| `PERIOD` end | exclusive | two columns | Tier 3 on `_END` when loaded inclusive | keep exclusive; harness gap `period_decompose` |

## Canonical DBSQL / Lakeflow shape

Procedure (`sql-scripting.md` "Stored Procedure Skeleton", "Handler Declaration", "EXECUTE IMMEDIATE"):

```sql
CREATE OR REPLACE PROCEDURE ${catalog}.${schema}.SP_X(IN p_date DATE, INOUT p_budget INT, OUT p_rc INT)
LANGUAGE SQL SQL SECURITY INVOKER MODIFIES SQL DATA AS BEGIN
    DECLARE v_n INT DEFAULT 0;
    DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN SET p_rc = -1; SET p_budget = p_budget - v_n; END;  -- no ROLLBACK
    SET p_rc = 0;
    FOR r AS SELECT k FROM ${catalog}.${schema}.T WHERE d < p_date ORDER BY k DO
        IF v_n >= p_budget THEN LEAVE; END IF;
        EXECUTE IMMEDIATE 'INSERT INTO ' || v_target || ' SELECT * FROM ${catalog}.${schema}.T WHERE k = ?' USING r.k;
        SET v_n = v_n + 1;                                       -- each DML is its own commit: make the loop idempotent
    END FOR;
END;
```

BTEQ script -> one job (`databricks-jobs` `references/task-types.md`, `triggers-schedules.md`): one `sql_task` (`file:`)
per SQL block, `depends_on` for order, `run_if: AT_LEAST_ONE_FAILED` for the `.IF ERRORCODE` branch, job
`parameters` read as `:name`, `max_concurrent_runs: 1`, `.QUIT n` -> a status row in the unit's control table.
Load utilities -> `CREATE OR REFRESH STREAMING TABLE ... FROM STREAM read_files(...)` + `AUTO CDC` flow + quarantine
(`databricks-pipelines`). `canonicalization.json` (six harness rules: `collation_casefold`, `rstrip_spaces`,
`datetime_utc_truncate_ms`, `decimal_round` half-even places 8, `null_missing_equiv`, `identity`) loads with
`recon.config.load_canon_rules`; harness gaps `period_decompose`, `time_of_day_normalize` are filed, not faked.

## Examples

| Dir | Source | Constructs | Recon tier that catches a wrong conversion |
|---|---|---|---|
| `examples/01_ddl_collation_set_table/` | fixture `ddl/tables/01_dim_customer.sql` | `SET TABLE`, `NOT CASESPECIFIC` -> `UTF8_LCASE`, `CHAR` -> `_RTRIM`, `BYTEINT`, identity, UPI/PPI -> `CLUSTER BY` | Tier 2 distinct drift, Tier 1 dedup/UPI excess, Tier 3 padding |
| `examples/02_view_olap_csum_mavg/` | fixture `ddl/views/03_vw_branch_performance.sql` | `CSUM`, `MAVG(.., 3, ..)`, `NULLIFZERO`, `FORMAT`, `LOCKING`, decimal division | Tier 4 checksum `verify/checks/20_branch_performance.sql`, Tier 2 cumulative drift |
| `examples/03_spl_cursor_dynamic_sql/` | skill-authored (fixture schema) | cursor/`FETCH` -> `FOR`, `DBC.SysExecSQL` -> `EXECUTE IMMEDIATE ... USING`, `BT`/`ET`, `EXIT HANDLER`, `SIGNAL`, `INOUT` | Tier 2 amount conservation, Tier 1 budget vs archived count |

Not verified live (file round-trip only): `DBC.*` column names; `sql_task file:` running a compound block or `CALL`;
reading `SQLSTATE` in a handler; `BEGIN ATOMIC` for BT/ET; `listagg`; `_metadata.file_path`/`try_cast` in streaming
tables. Each `NOTE.md` carries its own list.
