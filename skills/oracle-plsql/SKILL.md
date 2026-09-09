---
name: oracle-plsql
description: Source-dialect skill for Oracle SQL and PL/SQL estates (packages, procedures, functions, triggers, sequences, materialized views, DBMS_SCHEDULER jobs, SQL*Plus scripts). Load it when converting Oracle units to Databricks SQL / Lakeflow (analytical track) or to Lakebase Postgres (`!dbx_migrate_oltp` OLTP track), or when reconciling Oracle against Databricks.
---

# Oracle SQL / PL-SQL Dialect

## When to use / routing

Source-side half only: what Oracle does, where each construct lands, which recon tier catches a wrong conversion.
Two tracks: **A** = analytical Delta/DBSQL (ODS/warehouse schemas; MVs -> Lakeflow Pipelines, jobs -> Lakeflow Jobs),
**L** = Lakebase Postgres via `!dbx_migrate_oltp` (application schemas; tables, constraints, sequences, triggers must
behave identically). Procedural logic routes DBSQL SQL scripting first, Lakeflow Jobs control flow second, PySpark last.

Everything Databricks-side comes from the official plugin skills through `skills/target-routing/SKILL.md`; do not
restate it here. Citation shorthands used below: `[dbsql:<file>#<section>]` = `databricks-dbsql/references/<file>`;
`[jobs:]`, `[pipelines:]`, `[lakebase:]`, `[uc:]` likewise for `databricks-jobs`, `databricks-pipelines`,
`databricks-lakebase`, `databricks-unity-catalog`; `[docs:<path>]` = `docs.databricks.com/aws/en/sql/language-manual/<path>`;
`[pg17:<page>]` = `postgresql.org/docs/17/<page>` for the Lakebase column (Lakebase product facts only from `[lakebase:]`).
Lakebridge (`--source-dialect oracle`) handles static SQL; its seeded coverage row is in `skills/lakebridge/SKILL.md`.
Anything with `BEGIN`, `DECLARE`, `CREATE OR REPLACE (PACKAGE|PROCEDURE|FUNCTION|TRIGGER)` or `DBMS_` comes straight here.
Recon rules for this dialect: `canonicalization.json` (loads with `recon.config.load_canon_rules`; the `canonicalization`
column below names the rule per type).

## Type map

| Oracle | Delta / DBSQL (A) | Lakebase Postgres (L) | Loss | Canonicalization |
|---|---|---|---|---|
| `NUMBER(p,s)` | `DECIMAL(p,s)` | `NUMERIC(p,s)` | none | `identity` (exact; `decimal_round` places 10 would hide digits when `s > 10`) |
| `NUMBER` (no scale), `NUMBER(*)` | `DECIMAL(38,10)` (raise scale if census `MAX(scale)` > 10; never `DOUBLE` for money) | `NUMERIC` | floating scale fixed | `decimal_round` half_even 10 |
| `NUMBER(p,0)`, `NUMBER(p)` | `BIGINT` for p<=18; `DECIMAL(p,0)` for p>18 | `BIGINT` / `NUMERIC(p,0)` | none | `identity` |
| `INTEGER`, `INT`, `SMALLINT` (= `NUMBER(38)`) | `DECIMAL(38,0)` (`BIGINT` only if census `MAX(ABS(col))` < 2^63) | `NUMERIC(38,0)` / `BIGINT` likewise | none | `identity` |
| `FLOAT(b)` | `DECIMAL(38,10)` if money-like, else `DOUBLE` | `NUMERIC` / `DOUBLE PRECISION` | `FLOAT` is decimal, not IEEE | `decimal_round` for `DECIMAL(38,10)`; `identity` for `DOUBLE` |
| `BINARY_FLOAT` / `BINARY_DOUBLE` | `FLOAT` / `DOUBLE` | `REAL` / `DOUBLE PRECISION` | none | `identity` (same IEEE value; a per-type `places` is a harness gap) |
| `VARCHAR2(n)`, `NVARCHAR2(n)`, `LONG` | `STRING` | `VARCHAR(n)` (`TEXT` for `BYTE` semantics with multibyte data) | `''` is NULL in Oracle only | `empty_string_is_null` |
| `CHAR(n)`, `NCHAR(n)` | `STRING` (`COLLATE UTF8_BINARY_RTRIM` if compares must ignore padding `[dbsql:geospatial-collations.md#Collation Modifiers (DBR 16.2+)]`) | `CHAR(n)` (pads, ignores on compare) | padding, `LENGTH` | `rstrip_spaces`, then `empty_string_is_null` |
| `CLOB`, `NCLOB` | `STRING` (census `MAX(DBMS_LOB.GETLENGTH)` vs target limit) | `TEXT` | none below limit | `empty_string_is_null` |
| `BLOB`, `LONG RAW`, `RAW(n)` | `BINARY`; `RAW(16)` GUID -> `STRING` `lower(hex())` | `BYTEA`; `RAW(16)` from `SYS_GUID()` -> `UUID` | hex case/hyphens | `identity` (hash compare) / `uuid_normalize` |
| `DATE` (always has seconds) | `TIMESTAMP_NTZ`; `DATE` only if census proves `TRUNC(col)=col` on 100% of rows | `TIMESTAMP(0)`; `DATE` under the same proof | time-of-day if mapped to `DATE` | `datetime_utc_truncate_ms` |
| `TIMESTAMP(n)` | `TIMESTAMP_NTZ` (us; Oracle ns) | `TIMESTAMP(n)` (max 6) | ns -> us | `n <= 3`: `datetime_utc_truncate_ms`; `n > 3`: `identity` (drivers deliver us; ms truncation would hide sub-ms errors) |
| `TIMESTAMP WITH [LOCAL] TIME ZONE` | `TIMESTAMP` (instant in session TZ; source offset lost: sibling `STRING` if printed) | `TIMESTAMP WITH TIME ZONE` | original offset/region | `identity` on `SYS_EXTRACT_UTC(col)` / `to_utc_timestamp(col,'UTC')` in the recon queries (a us-precision UTC rule is a harness gap) |
| `INTERVAL YEAR TO MONTH` / `DAY TO SECOND` | `INTERVAL YEAR TO MONTH` / `INTERVAL DAY TO SECOND` (us) | same | ns | `identity` |
| `ROWID`, `UROWID` | drop; PK or `BIGINT` surrogate if used as a key | drop (`ctid` unstable) | row address | `GAP` if consumed |
| `XMLTYPE` | `STRING` + `xpath*`/`from_xml`; `VARIANT` if JSON upstream | `XML` / `TEXT` | schema validation, XMLIndex | `identity` on canonical string |
| `JSON`, `IS JSON` columns | `VARIANT` | `JSONB` | key order (both) | `identity` (`to_json` form) |
| Object type / `VARRAY` / nested table | `STRUCT` / `ARRAY<...>` | `JSONB` `[lakebase:references/synced-tables.md#Data Type Mapping]` | methods, `TABLE()` unnest | `identity` |
| `BOOLEAN` (PL/SQL; 23ai SQL) | `BOOLEAN` (`CHAR(1) 'Y'/'N'` stays string unless decided) | `BOOLEAN` | none | `identity` |
| `BFILE`, `SDO_GEOMETRY`, `ANYDATA` | path `STRING` via Volumes; `GEOMETRY`/`GEOGRAPHY` via WKT/WKB; `STRING`/`VARIANT` | `TEXT`; WKT unless PostGIS is on the Lakebase extension list; `TEXT`/`JSONB` | external file; SRID | `GAP` |

## Function / operator map

`same` = identical for non-NULL inputs; `edge` = spelled-out edge case; `none` = no expression-level equivalent.

| Oracle | Databricks SQL (Lakebase where different) | Sem. | Edge case |
|---|---|---|---|
| `NVL`, `NVL2`, `COALESCE`, `NULLIF`, `LNNVL(c)` | `nvl`, `nvl2`, `coalesce`, `nullif`, `NOT coalesce(c,false)` | same | Oracle evaluates `NVL`'s 2nd arg eagerly |
| `DECODE(x,k,v,...,d)` | `decode(...)`; NULL keys -> `CASE WHEN x IS NULL AND k IS NULL` | edge | Oracle `DECODE(NULL,NULL,1)` = 1 |
| `x = ''`, `LENGTH('')`, `TRIM('  ')` | `x IS NULL`; `length('')`=0; `nullif(trim(x),'')` | edge | `''` is NULL in Oracle only (trap 1) |
| `a \|\| b`, `CONCAT(a,b)` | `nullif(concat_ws('',a,b),'')` or `nullif(coalesce(a,'')\|\|coalesce(b,''),'')` | edge | Oracle concatenates NULL as `''` and an all-NULL result is NULL; Databricks `\|\|` yields NULL on any NULL, `concat_ws` yields `''` on all-NULL |
| `SUBSTR`, `INSTR(s,sub[,pos[,nth]])`, `LENGTH`, `LENGTHB` | `substr`, `instr`/`locate(sub,s,pos)`, `length`, `length(cast(s AS BINARY))` | edge | `nth`/negative `pos` -> `regexp_instr` or UDF; `SUBSTRB` -> `BINARY` |
| `REPLACE`, `TRANSLATE`, `LPAD`, `RPAD`, `SOUNDEX`, `ASCII`, `CHR` | same names | edge | `LPAD(s,0)` NULL vs `''`; non-UTF8 `NLS_CHARACTERSET` code points > 127 |
| `TRIM/LTRIM/RTRIM(s[,set])` | `trim`, `ltrim([set,] s)`, `rtrim([set,] s)` | edge | argument order reversed; trimmed-to-nothing is NULL in Oracle |
| `REGEXP_REPLACE/SUBSTR/INSTR/LIKE/COUNT` | `regexp_replace`, `regexp_substr`/`regexp_extract`, `regexp_instr`, `rlike`, `regexp_count` | edge | Databricks replaces all occurrences; POSIX classes -> Java (`[[:alpha:]]` -> `\p{Alpha}`); `match='i'` -> `(?i)`; `subexpr` -> `regexp_extract` |
| `UPPER`, `LOWER`, `INITCAP`, `NLS_UPPER` | `upper`, `lower`, `initcap`; collation for `NLS_*` | edge | `initcap` splits on whitespace only (`o'neil`); ß / Turkish i differ |
| `CHAR` `=` compare; `NLS_COMP=LINGUISTIC`/`BINARY_CI` | `rtrim()` both sides or `COLLATE UTF8_BINARY_RTRIM`; column `COLLATE UTF8_LCASE` `[dbsql:geospatial-collations.md#Collation Types]` | edge | declare collation on the column, never sprinkle `upper()` |
| `ROUND(n,d)`, `TRUNC(n,d)`, `MOD`, `REMAINDER` | `round` (`bround` for IEEE), `truncate` or `cast(n*10^d AS BIGINT)/10^d`, `mod`/`try_mod`, UDF | edge | `MOD(x,0)` = x in Oracle, raises in Databricks; `ROUND(BINARY_DOUBLE)` is half-even |
| `a / b`, `TRUNC(a/b)`, `POWER`, `SIGN`, `SQRT(-1)` | `/` (`try_divide`), `a div b`, `cast(power() AS DECIMAL)`, `cast(sign())`, NaN | edge | `DECIMAL` division scale = `max(6, s1+p2+1)`; `POWER`/`SIGN` return `DOUBLE`; Oracle raises on `SQRT(-1)` |
| `GREATEST`, `LEAST` | wrap: `CASE WHEN a IS NULL OR b IS NULL THEN NULL ELSE greatest(a,b) END` | edge | Oracle returns NULL on any NULL; Databricks skips NULLs |
| `TO_CHAR(n,fmt)` | `to_char(n,fmt)` (`0 9 , . G D S MI PR L $` only) | edge | `FM`/`RN`/`X`/`EEEE`/`TM` unsupported: `trim(to_char())`, `hex` |
| `TO_CHAR(d,fmt)`, `TO_DATE(s,fmt)`, `TO_TIMESTAMP[_TZ]` | `date_format`/`to_char`, `to_timestamp` (result has time; `to_date` only if time provably zero), zone tokens `xxx`/`VV` | edge | token rewrite `YYYY-MM-DD HH24:MI:SS` -> `yyyy-MM-dd HH:mm:ss`, `MON` -> `MMM`, `FF3` -> `SSS`; `RR` century has no token; `TO_DATE(s)` without fmt depends on `NLS_DATE_FORMAT`: never port blind |
| `TO_NUMBER(s[,fmt])`, `CAST`, implicit `VARCHAR2`->`NUMBER`/`DATE` | `to_number(s,fmt)`/`try_to_number`, `cast`/`try_cast`; cast literals explicitly | edge | `to_number` requires fmt; `CAST(d AS DATE)` keeps time in Oracle, drops it in Databricks; ANSI coercion differs (trap 20) |
| `SYSDATE`, `SYSTIMESTAMP`, `CURRENT_DATE`, `CURRENT_TIMESTAMP`, `DBTIMEZONE` | `cast(from_utc_timestamp(current_timestamp(),'<server TZ>') AS TIMESTAMP_NTZ)`, `current_timestamp()`, `cast(current_timestamp() AS TIMESTAMP_NTZ)` (session TZ; never `current_date()`), `current_timestamp()`, `current_timezone()` (L: `localtimestamp`, `now()`) | edge | `SYSDATE` (server TZ) and `CURRENT_DATE` (session TZ) are both `DATE` with time to the second; `date_trunc('SECOND')` if seconds parity matters; `SYSDATE` advances inside PL/SQL loops, Databricks fixes per query |
| `d + n`, `d1 - d2`, `ADD_MONTHS`, `MONTHS_BETWEEN`, `LAST_DAY`, `NEXT_DAY` | `dateadd(DAY,n,d)`, `datediff` (whole units; `unix_timestamp` diff/86400 for fractional), `add_months` (returns `DATE`: use `+ INTERVAL n MONTH` to keep time), `months_between(...,false)`, `last_day`, `next_day` | edge | `DATE - DATE` is fractional days in Oracle; `add_months`/`last_day` drop time; day names English only |
| `TRUNC(d[,fmt])`, `ROUND(d)`, `EXTRACT`, `FROM_TZ`, `AT TIME ZONE`, `NUMTODSINTERVAL` | `date_trunc(unit,d)` cast `TIMESTAMP_NTZ`, none (arithmetic/UDF), `extract`, `to_utc_timestamp`/`from_utc_timestamp`/`convert_timezone`, `make_dt_interval` | edge | `'IW'` -> `'WEEK'`; `'W'`/`'WW'` no unit; DST-gap instants raise in Oracle, shift in Databricks |
| `ROWNUM <= n`; `ROWNUM` sandwich pagination; `OFFSET/FETCH` | `LIMIT n`; `ORDER BY total_key LIMIT n OFFSET lo` or `row_number()` with a tie-breaker; `LIMIT m OFFSET n` | edge | make the order key total (append PK) or pages overlap; `ROWNUM > 1` is always empty (trap 7) |
| `ROW_NUMBER/RANK/LAG/LEAD/FIRST_VALUE ... OVER`, `KEEP (DENSE_RANK FIRST)`, `RATIO_TO_REPORT` | same names; `max_by`/`min_by` or `first_value ... QUALIFY`; `x / sum(x) OVER ()` | edge | `IGNORE NULLS` supported; default frame identical with `ORDER BY` |
| `LISTAGG(x,',') WITHIN GROUP (ORDER BY k)`, `WM_CONCAT` | `listagg(x,',') WITHIN GROUP (ORDER BY k)` / `string_agg` (DBR 16.4+; else `array_join(transform(array_sort(collect_list(struct(k, pk, x))), s -> s.x), ',')`: sort on the `WITHIN GROUP` key plus a tie-breaker, never on `x`) | edge | both drop NULL `x`; `ON OVERFLOW TRUNCATE` has no clause; `WM_CONCAT` order was never defined |
| `MEDIAN`, `PERCENTILE_CONT`, `STDDEV`, `VARIANCE`, `COUNT(DISTINCT)`, `SUM/AVG(NUMBER)` | same names | edge | `sum(DECIMAL)` adds 10 precision, `avg` adds 4 scale -> `cast(... AS DECIMAL(38,s))` (trap 25) |
| `ROLLUP/CUBE/GROUPING SETS`, `PIVOT`, `UNPIVOT` | same; `PIVOT` column names lower-case; `PIVOT XML` -> collect values + `EXECUTE IMMEDIATE` | edge | alias every pivot column explicitly (trap 9) |
| `CONNECT BY [NOCYCLE] PRIOR ... START WITH`, `LEVEL`, `CONNECT_BY_ROOT`, `SYS_CONNECT_BY_PATH`, `ORDER SIBLINGS BY` | `WITH RECURSIVE` `[dbsql:sql-scripting.md#Recursive CTEs]` with explicit `level`, `path ARRAY` + `NOT array_contains(path,id)`, root column, `concat_ws`, order by the path | edge | depth limit 100 (`MAX RECURSION LEVEL`); `NVL(col, PRIOR col)` reads the parent's **raw** column, not its effective value (trap 6) |
| `MERGE ... WHEN MATCHED THEN UPDATE ... DELETE WHERE ... WHEN NOT MATCHED ... WHERE` | `MERGE` with `WHEN MATCHED AND cond THEN DELETE` **first**, `WHEN NOT MATCHED AND cond` `[docs:delta-merge-into]` | edge | `DELETE WHERE` sees post-update values in Oracle, pre-update in Delta; ORA-30926 / ORA-00001 parity via `assert_true` pre-check (trap 8, example 01) |
| `INSERT ALL/FIRST`, `UPDATE t SET (a,b) = (SELECT ...)`, `DELETE`, `TRUNCATE` | one `INSERT` per target (`BEGIN ATOMIC` for atomicity), `MERGE` (`UPDATE` has no join), `DELETE`, `TRUNCATE` | edge | `TRUNCATE` is DDL in Oracle, transactional in Delta |
| `seq.NEXTVAL`, `seq.CURRVAL` | A: `GENERATED ALWAYS AS IDENTITY` (no `CURRVAL`); L: `nextval('seq')`, `currval('seq')` | none | never compare surrogate values across engines; recon on business keys (trap 10) |
| `SYS_GUID()`, `ORA_HASH`, `STANDARD_HASH`, `RAWTOHEX`/`HEXTORAW`, `DBMS_RANDOM` | `uuid()`, `hash`/`xxhash64` (never persist across), `sha2(x,256)`, `hex`/`unhex`, `rand()` | edge | `ORA_HASH` is Oracle-specific; `uuid_normalize` for GUID recon |
| `USER`, `SYS_CONTEXT('USERENV','SESSION_USER')`; other `SYS_CONTEXT`/application contexts | `session_user()` (L: `session_user`); none: pass as `IN` parameter | edge | VPD predicates on it -> UC row filter with `is_account_group_member` `[uc:references/4-fine-grained-access.md#Identity Functions]`; contexts driving a policy are `GAP` |
| `NOT IN`, `BETWEEN` on `DATE`, `DECODE(a,b,0,1)=0`/`NVL(a,'~')=NVL(b,'~')`, `(+)`, `MINUS`, `FROM DUAL`, `SAMPLE` | same, `>= d1 AND < d2 + INTERVAL 1 DAY`, `a <=> b`, `LEFT JOIN ... ON` (filter `(+)` goes in `ON`), `EXCEPT`, no `FROM`, `TABLESAMPLE` | edge | `BETWEEN` to a midnight `d2` excludes the last day (trap 3) |
| `TABLE(coll)`, `XMLTABLE`/`XMLAGG`/`EXTRACTVALUE`, `JSON_VALUE`/`JSON_TABLE` | `LATERAL VIEW explode`, `to_xml`/`xpath_string`/`from_xml`, `get_json_object`/`variant_get`/`parse_json` | edge | `JSON_VALUE` truncates at `VARCHAR2(4000)`; SOAP payload builders belong to the serving layer |
| `SQL%ROWCOUNT`, `SQLCODE`/`SQLERRM`, `RAISE_APPLICATION_ERROR(-20nnn,msg)` | `count(*)` of the affected set (L: `GET DIAGNOSTICS`), handler-local `SQLSTATE`, `SIGNAL SQLSTATE '45nnn' SET MESSAGE_TEXT` `[dbsql:sql-scripting.md#SIGNAL and RESIGNAL]` (L: `RAISE EXCEPTION USING ERRCODE`) | edge | 1000 app codes collapse to one class unless `'45xxx'` is allocated per code; keep a code table in the unit note |

## Procedural-construct map

Track: **A** analytical DBSQL, **L** Lakebase, **J** Lakeflow Jobs, **P** Lakeflow Pipelines, **Py** PySpark (last), **M** manual.

| Oracle construct | Target | Track | Cites |
|---|---|---|---|
| `CREATE PACKAGE` spec + body | one schema per package, one `CREATE PROCEDURE`/SQL function per public member; private members `_`-prefixed, no grants (L: one Postgres schema, `CREATE PROCEDURE`/`FUNCTION`) | A, L | `[dbsql:sql-scripting.md#CREATE PROCEDURE]`, `[pg17:sql-createprocedure.html]` |
| Package variables / constants (`g_run_id`) | constants -> literals or config table; state -> `IN`/`OUT` params or a run-log table (L: custom GUCs) | A, L | `[dbsql:sql-scripting.md#Variable Declaration (DECLARE)]` |
| `PROCEDURE p(IN, OUT, IN OUT)`, `DEFAULT`s, overloads | `CREATE PROCEDURE p(IN x T, OUT y T, INOUT z T) LANGUAGE SQL SQL SECURITY INVOKER AS BEGIN ... END`; `DEFAULT` on `IN` only; one procedure per signature (L: overloads native) | A, L | `[dbsql:sql-scripting.md#CREATE PROCEDURE]`, `#CALL (Invoke a Procedure)` |
| `FUNCTION f RETURN scalar` (pure SQL body) | `CREATE FUNCTION f(...) RETURNS T RETURN <expr>` (L: `LANGUAGE sql/plpgsql`) | A, L | `[docs:sql-ref-syntax-ddl-create-sql-function]` |
| `FUNCTION f RETURN SYS_REFCURSOR` (static `OPEN c FOR`) | SQL table function `RETURNS TABLE`; cursor contract (names/order/types) pinned in the unit note, Tier 4 | A | `[docs:sql-ref-syntax-ddl-create-sql-function]` |
| `OPEN c FOR l_sql`, `EXECUTE IMMEDIATE ... [USING] [INTO]`, `DBMS_SQL` | `EXECUTE IMMEDIATE sql [INTO var] [USING args]` behind an allow-list; M when identifiers come from user input; `DBMS_SQL` with variable shape -> Py | A, M | `[dbsql:sql-scripting.md#EXECUTE IMMEDIATE (Dynamic SQL)]` |
| Explicit/implicit cursor loops, `BULK COLLECT [LIMIT]` + `FORALL` | set-based: driving `SELECT` into a `TEMPORARY TABLE`, loop body -> one `MERGE`/`INSERT ... SELECT` (`UPDATE` has no join); truly row-wise -> `FOR r AS SELECT ... DO ... END FOR` (L: `FOR r IN SELECT ... LOOP`) | A, L | `[dbsql:sql-scripting.md#FOR Loop]`, `[docs:delta-merge-into]` |
| `SAVE EXCEPTIONS` + `SQL%BULK_EXCEPTIONS` | validating `INSERT ... SELECT` of good rows + reject `INSERT`; or pipeline `EXPECT ... ON VIOLATION DROP ROW` | A, P | `[pipelines:references/expectations-sql.md]` |
| `SELECT ... INTO var` | `SET var = (SELECT ...)`; `NO_DATA_FOUND` -> `HANDLER FOR NOT FOUND`; in a scalar UDF `coalesce((SELECT ...), default)` with the no-row default **outside** and `NVL(col,0)` **inside**; `TOO_MANY_ROWS` -> assert uniqueness in the caller, never `max()` (L: `SELECT INTO STRICT`) | A, L | `[dbsql:sql-scripting.md#Variable Assignment (SET)]`, `#Handler Declaration` |
| `IF/ELSIF`, `CASE`, `LOOP/EXIT WHEN`, `WHILE`, `FOR i IN 1..n`, `GOTO`, `CONTINUE` | `IF/ELSEIF`, `CASE ... END CASE`, `LOOP ... LEAVE`, `WHILE ... DO`, `WHILE` + counter, labelled `LEAVE`, `ITERATE` | A, L | `[dbsql:sql-scripting.md#Control Flow]` |
| `EXCEPTION WHEN NO_DATA_FOUND / DUP_VAL_ON_INDEX / named / OTHERS` | `DECLARE cond CONDITION FOR SQLSTATE '45nnn'`; `DECLARE EXIT HANDLER FOR NOT FOUND / SQLSTATE '23505' / cond / SQLEXCEPTION` (only `EXIT` handlers: handle-and-continue needs a nested `BEGIN...END` per iteration) (L: `EXCEPTION WHEN ...`, a subtransaction) | A, L | `[dbsql:sql-scripting.md#Exception Handling]`, `#Condition Declaration` |
| `WHEN OTHERS THEN NULL` / status-code swallow | do not port the swallow: log, `RESIGNAL` or carry `OUT status` and make the caller fail on it; decision recorded | A, L | `[dbsql:sql-scripting.md#SIGNAL and RESIGNAL]` |
| `COMMIT`/`ROLLBACK` in a procedure; `SAVEPOINT`/`ROLLBACK TO` | delete (each Delta statement is atomic); multi-statement -> `BEGIN ATOMIC ... END` (Preview, `catalogManaged` tables) or one Jobs task per step with `run_if`; savepoints -> validate-then-write (L: native, savepoints via nested `BEGIN ... EXCEPTION`) | A, J, L | `[dbsql:sql-scripting.md#Multi-Statement Transactions]`, `[jobs:SKILL.md#Multi-Task Workflows]` |
| `PRAGMA AUTONOMOUS_TRANSACTION` logger | A: `EXIT HANDLER` log write or a `run_if: ALL_DONE` task; L: none in Postgres -> in-transaction logger (rows lost on rollback, accepted) or outbox written before the business txn; M until the tolerance record accepts | A, J, L, M | `[jobs:SKILL.md#Multi-Task Workflows]`, `[dbsql:sql-scripting.md#Handler Declaration]` |
| `LOCK TABLE`, `SET TRANSACTION`, `FOR UPDATE [SKIP LOCKED]` | none: optimistic concurrency, `max_concurrent_runs: 1`; queue patterns -> `for_each_task` (L: native) | A, J, L | `[dbsql:sql-scripting.md#Isolation Levels]`, `[jobs:references/task-types.md#For Each Task]` |
| Global / private temporary tables | `CREATE TEMPORARY TABLE` (`DROP TABLE IF EXISTS` first; no `CREATE OR REPLACE TEMP TABLE`); not a temp view when re-read after a write it drives (views re-execute) (L: `CREATE TEMPORARY TABLE ... ON COMMIT ...` inside the procedure) | A, L | `[dbsql:materialized-views-pipes.md#Temporary Tables]` |
| Row trigger `BEFORE INSERT/UPDATE FOR EACH ROW` (`:NEW`/`:OLD`, `INSERTING`/`UPDATING`) | A: no triggers; fold the body into **every** writer: `:NEW.x := f()` -> select-list/`UPDATE SET` expr, `:OLD` -> target row in `WHEN MATCHED`, audit cols -> `current_timestamp()`/`current_user()`, keys -> identity, conditional logger -> `INSERT ... SELECT` from a pre-`MERGE` `:OLD` image; only declared events (Oracle `MERGE ... DELETE WHERE` fires UPDATE first). L: trigger function + `CREATE TRIGGER`, `TG_OP`, `NEW`/`OLD` | A, L | `[docs:delta-merge-into]`, `[pg17:plpgsql-trigger.html]` |
| Statement / `AFTER` fan-out / `INSTEAD OF` / compound triggers; DDL and logon triggers | second statement in the same procedure or task (`run_if: ALL_SUCCESS`); view writers rewritten to base tables (L: native); system triggers -> system tables | A, J, L, M | `[jobs:SKILL.md#Multi-Task Workflows]`, `[uc:references/5-system-tables.md]` |
| `CREATE SEQUENCE` (`CACHE`, `NOORDER`, `CYCLE`), identity columns | A: `GENERATED ALWAYS AS IDENTITY (START WITH last_number+1)` per table; L: `CREATE SEQUENCE ... START WITH last_number+1 ... CACHE [CYCLE]`, `setval` after backfill | A, L | `[docs:sql-ref-syntax-ddl-create-table-using]`, `[pg17:sql-createsequence.html]` |
| Constraints (PK/UK/FK/CHECK/NOT NULL, `DEFERRABLE`, `NOVALIDATE`, `DISABLE`) | A: `NOT NULL`/`CHECK` enforced, PK/FK informational (`RELY` only if census proves), uniqueness = Tier 1 check; L: all enforced, `NOVALIDATE` -> `NOT VALID`, disabled constraints not recreated (flag) | A, L | `[docs:sql-ref-syntax-ddl-create-table-constraint]`, `[pg17:ddl-constraints.html]` |
| `CREATE MATERIALIZED VIEW ... REFRESH FAST` + MV logs; `DBMS_MVIEW.REFRESH` | pipeline MV (incremental when the query qualifies; row tracking replaces MV logs); standalone DBSQL MV with `SCHEDULE`/`TRIGGER ON UPDATE`; `REFRESH MATERIALIZED VIEW` as a Jobs SQL task; `ENABLE QUERY REWRITE` has no equivalent | P, J | `[pipelines:references/materialized-view-sql.md#Incremental refresh]`, `[dbsql:materialized-views-pipes.md#Refresh Options]` |
| `DBMS_SCHEDULER.CREATE_JOB/PROGRAM`, chains, `DBMS_JOB`, email notifications, event/file jobs | Lakeflow Job: `quartz_cron_expression` + `timezone_id` (`FREQ=DAILY;BYHOUR=2;BYMINUTE=40;BYDAY=MON,...,SAT` -> `0 40 2 ? * MON-SAT`), `timeout_seconds`, `max_concurrent_runs: 1`; one task per program (`sql_task`/`notebook_task`); chains -> `depends_on` + `run_if`; `email_notifications`, health rules; table-update / file-arrival triggers. `max_failures` (auto-disable after n failed runs) has no field: GAP, mitigate with `on_failure` + runbook or a monitor job that sets `pause_status: PAUSED`; never map it to `max_retries` | J | `[jobs:references/triggers-schedules.md#Cron Schedule]`, `[jobs:references/notifications-monitoring.md#Retry Configuration]`, `[jobs:references/task-types.md#SQL Task]` |
| SQL*Plus `SET/COLUMN/TTITLE/BREAK`, `DEFINE`/`&var`/`&1`, `SPOOL`, `WHENEVER SQLERROR`, `@file`/`START`, `HOST` | drop formatting (consumer's job); `:param` task parameters (`&var` in an identifier -> `EXECUTE IMMEDIATE`); write to a Delta table or Volume; task failure + `max_retries` + `run_if`; separate tasks with `depends_on`; OS lines -> Py or M | J, A | `[jobs:references/task-types.md#SQL Task]`, `[uc:references/6-volumes.md]` |
| `DBMS_OUTPUT`, `DBMS_LOCK.SLEEP`, `DBMS_APPLICATION_INFO`, `DBMS_STATS`, `DBMS_LOB.*` | drop (or `INSERT` into a run-log table when consumed); drop; drop; drop; `substr`/`instr`/`length`/`concat` | A, L | |
| `UTL_FILE`/`sqlldr` staging, `UTL_HTTP`, `UTL_SMTP`, `DBMS_AQ`/`PIPE`/`ALERT`, Java/`EXTPROC`, `@dblink` | Auto Loader / `COPY INTO` from Volumes; Py task; job notifications; M (messaging); `python_wheel_task`; Lakehouse Federation or ingest then local read | P, Py, J, M | `[pipelines:references/auto-loader-sql.md]`, `[jobs:references/task-types.md#Python Wheel Task]` |
| `AUTHID DEFINER`, VPD (`DBMS_RLS`), `DBMS_REDACT`, roles, `PUBLIC` grants, column grants | `SQL SECURITY INVOKER` only -> explicit table grants to callers; row filters / column masks (`RANDOM` redaction -> GAP); UC groups; `PUBLIC` re-granted to named groups; column grants -> mask or projecting view | A, L, M | `[uc:references/1-access-control.md#GRANT / REVOKE (SQL)]`, `[uc:references/4-fine-grained-access.md]` |

## Known traps with recon signature

| # | Trap | Oracle vs target | Recon signature | Fix / canonicalization |
|---|---|---|---|---|
| 1 | `''` is NULL | `''` stored as NULL, `x = ''` never true, `TRIM` to nothing is NULL; targets store `''` | Tier 1 null-count drift; Tier 3 `NULL` vs `''` | `nullif(trim(col),'')` on load; `x = ''` -> `IS NULL`; `empty_string_is_null` on every string column |
| 2 | `NUMBER` without scale | floating scale, 38 digits; `DECIMAL(38,10)` fixes scale; `DOUBLE` drifts | Tier 2 `SUM` drift past the 10th decimal; `0.30000000000000004` diffs | scale from sampled `MAX(SCALE)`; never `DOUBLE` for money; `decimal_round` half_even 10 |
| 3 | `DATE` carries time | seconds precision, `SYSDATE` defaults; lazy `DATE` mapping drops it | Tier 3 mismatch on non-midnight rows; per-day counts off by one day at `BETWEEN` midnight | `TIMESTAMP_NTZ`; `>= d1 AND < d2 + INTERVAL 1 DAY`; `date_trunc('DAY')`; `datetime_utc_truncate_ms` |
| 4 | `TIMESTAMP WITH [LOCAL] TIME ZONE` | offset stored / normalised to DB TZ; target is one instant in session TZ | Tier 3 constant offset on every row (+1h) | compare as UTC instants (`SYS_EXTRACT_UTC` / `to_utc_timestamp`) with `identity`; harness session TZ = UTC both sides |
| 5 | `CHAR(n)` padding | pads and pad-compares; `STRING` does not | `COUNT(DISTINCT)`/join-cardinality drift; join to `VARCHAR2` keys yields 0 rows | `rtrim` on load or `COLLATE UTF8_BINARY_RTRIM`; `rstrip_spaces` then `empty_string_is_null` |
| 6 | `CONNECT BY` order, cycles, `LEVEL` | depth-first, `ORDER SIBLINGS BY`; `NOCYCLE` flags the row whose child is an ancestor; recursive CTE has none of these, depth limit 100 | Tier 1 doubled rows or `RECURSION_LEVEL_LIMIT_EXCEEDED`; Tier 3 path/`is_cycle` on wrong row | carry `path ARRAY`, `NOT array_contains`; compute `is_cycle` after the walk; order by path |
| 7 | `ROWNUM` pagination | assigned before `ORDER BY`; ties arbitrary | equal page counts, Tier 3 rows differ across pages | total order key (append PK); `row_number()` for `rn` |
| 8 | `MERGE` duplicates, `DELETE WHERE`, `NEXTVAL` | ORA-30926 when >1 source row updates one target row; unmatched duplicates insert once each (ORA-00001 if the key is unique); duplicates that reach no clause are a no-op; `DELETE WHERE` sees post-update values; Delta has no unique constraint and evaluates `WHEN MATCHED AND` on pre-update rows | Tier 1 count off by the deleted set / double-inserted duplicates; Tier 4 target succeeded where Oracle failed (`QUALIFY` dedupe hides it) | `assert_true(count(*)=0)` over the duplicate keys Oracle would reject (matched, or unmatched and insert-eligible) before the `MERGE`; `DELETE` clause first with a source predicate; exclude surrogate keys from Tier 3 |
| 9 | `PIVOT` names and nulls | generated names upper-case; `PIVOT XML` dynamic | Tier 4 column names; Tier 2 `SUM` where `NVL(cell,0)` on one side | alias every column; `null_missing_equiv` |
| 10 | Sequences and trigger side effects | every writer gets PK, audit columns, derived flags, logger for free; Delta has no triggers | Tier 1 audit table 0 rows; Tier 3 `row_version`/`active_policy_flag` drift when one writer forgot | A: shared write procedure called by all writers; L: port trigger, synchronous logger; compare audit rows on content, not `audit_id` |
| 11 | Package session state | `g_*` persists across calls in a session; none on either target | Tier 3 on state-derived columns; Tier 4 `run_id` sequences | thread through params or a run-control table keyed by job run id |
| 12 | `SYS_REFCURSOR` contracts | names/order/types fixed at `OPEN`; SOAP stacks bind by position | Tier 4 consumer diff | pin the projection, never `SELECT *`, alias to Oracle upper-case names |
| 13 | Swallowed exceptions | `WHEN OTHERS THEN NULL` / `p_status_out` succeed with partial writes | Tier 4 no failure signal while source logged `ERROR` rows | log then `RESIGNAL`, or `OUT status` the caller fails on; source audit table as Tier 4 evidence |
| 14 | Autonomous transactions | logger commits independently; rows exist for rolled-back business txns | Tier 1 audit count higher on source | accept the delta explicitly in the tolerance record, or log outside the transaction (job task) |
| 15 | MV fast-refresh eligibility | needs MV logs + `COUNT` columns; target picks incremental by cost model; `SUM` over all-NULL group = 0 `[dbsql:materialized-views-pipes.md#Key Limitations]` | Tier 2 `SUM` 0 vs NULL; Tier 1 group count | keep `COUNT` columns; `nullif(sum(x),0)` only if consumed; `null_missing_equiv` on aggregates |
| 16 | Scheduler TZ, retries, auto-disable | `repeat_interval` in `start_date` TZ; `max_failures` disables after n failed runs; `max_retries` retries a task within one run, counter resets per run | Tier 4 1h shift across DST; duplicate batch rows after a retry; target keeps scheduling where Oracle stopped | `timezone_id` = Oracle region; idempotent tasks before `max_retries`; `max_failures` = GAP with `on_failure` + runbook or monitor job |
| 17 | SQL*Plus directives | `&var` substituted before parse; `COLUMN FORMAT` shapes output | Tier 4 formatted-output diff | strip directives; task parameters; formatting to the consumer |
| 18 | Synonyms and `@dblink` | `PUBLIC` synonyms hide owners; links read another DB's TZ/charset | Tier 1 drift when the synonym pointed elsewhere in the enumerated env | resolve every synonym to `OWNER.TABLE`; remote tables are separate units |
| 19 | Roles, VPD, redaction | predicates on `SYS_CONTEXT`; partial masks | Tier 3 masked vs unmasked when principals differ | recon as an unfiltered principal both sides; inexpressible policy = `GAP`, never emulated |
| 20 | Implicit conversions, `NLS_*` | `WHERE id = '00123'` compares numerically; `TO_DATE(s)`/`TO_NUMBER('1,5')` follow session settings | Tier 1 drift on quoted-number filters; Tier 3 `NULL` from `try_cast` | cast literals to the column type; carry every format explicitly |
| 21 | `VARCHAR2(n BYTE)` multibyte | byte semantics truncate (`ORA-12899`) | Tier 3 truncated names; `MAX(LENGTH)` drift | character semantics on target; accept pre-truncated source data in the tolerance record |
| 22 | NULL ordering | `ORDER BY x` puts NULL last ascending; Databricks first | Tier 4 order; Tier 3 pages with NULL keys | always spell `NULLS FIRST/LAST` |
| 23 | `GREATEST`/`LEAST`, `\|\|` with NULL | NULL-propagating vs NULL-skipping; `'a'\|\|NULL` = `'a'` vs NULL | Tier 3 on derived text keys and dates | function-map rows; `empty_string_is_null` |
| 24 | `DECODE(NULL,NULL,...)` | NULL matches NULL | Tier 3 on NULL-key rows | `CASE WHEN x IS NULL ...` |
| 25 | `AVG`/`SUM` scale growth | full precision vs `+4` scale / `+10` precision, overflow -> NULL/error | Tier 2 last-digit rounding | `cast(sum(x) AS DECIMAL(38,s))`; `decimal_round` |

## Canonical Lakeflow / DBSQL shape

One Oracle scheduler job becomes one Lakeflow Job; one package becomes one schema of DBSQL procedures/functions; every
MV becomes a pipeline MV. Each DBSQL procedure follows this shape (example 03 is the full version):

```sql
CREATE OR REPLACE PROCEDURE ${catalog}.<pkg_schema>.<member>(IN ..., OUT p_rows_out BIGINT, OUT p_status_out STRING)
LANGUAGE SQL SQL SECURITY INVOKER MODIFIES SQL DATA
AS BEGIN
  DECLARE <named> CONDITION FOR SQLSTATE '45nnn';                -- one per -20nnn code
  DECLARE EXIT HANDLER FOR <named> BEGIN SET p_status_out = '...'; INSERT INTO ...audit_log ...; END;
  DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN SET p_status_out = 'ERR'; INSERT INTO ...audit_log ...; END;
  DROP TABLE IF EXISTS <candidates>; CREATE TEMPORARY TABLE <candidates> AS SELECT ...;   -- cursor / GTT / BULK COLLECT
  SELECT assert_true(count(*) = 0, '...') FROM (... GROUP BY key HAVING count(*) > 1);   -- ORA-30926 / TOO_MANY_ROWS parity
  BEGIN ATOMIC                                                    -- replaces COMMIT/SAVEPOINT; catalogManaged tables
    MERGE INTO <target> USING <candidates> ...                    -- FORALL body + folded row-trigger columns
      WHEN MATCHED AND <delete cond> THEN DELETE WHEN MATCHED THEN UPDATE SET ... WHEN NOT MATCHED THEN INSERT ...;
    INSERT INTO <audit_log> SELECT ... FROM <pre_image> JOIN <target> ...;   -- trigger's logger branch
    INSERT INTO <run_log> ...;                                    -- package state
  END;
  SET p_rows_out = (SELECT count(*) FROM <candidates>); SET p_status_out = 'OK';
END;
```

Jobs wrapper: `sql_task` `CALL`ing the procedure, `SIGNAL` when `p_status_out <> 'OK'`, `quartz_cron_expression` +
`timezone_id` from `repeat_interval`/`start_date`, `timeout_seconds` from `max_run_duration`, `max_concurrent_runs: 1`,
`depends_on`/`run_if` from chain rules. Lakebase track: the same procedure in PL/pgSQL with the trigger and sequences
kept as objects (example 02), `EXCEPTION WHEN` blocks, `RAISE EXCEPTION USING ERRCODE`, no autonomous transactions.

## Examples

| Dir | Exercises | Track |
|---|---|---|
| `examples/01_merge_delete_where/` | GTT, `MERGE ... DELETE WHERE` ordering, ORA-30926 parity, `NEXTVAL` -> identity, `''`/`TRUNC(date)`/`CHAR`, trigger fold-in (traps 1, 3, 5, 8, 10) | A (L variant noted) |
| `examples/02_trigger_sequence_lakebase/` | `CREATE SEQUENCE`, row trigger `:NEW`/`:OLD`/`INSERTING`, `PRAGMA AUTONOMOUS_TRANSACTION` logger -> Postgres trigger function, `nextval`, in-transaction logger (traps 1, 3, 10, 13, 14) | L |
| `examples/03_bulk_collect_setbased/` | package spec/body, `BULK COLLECT`/`FORALL`, `NO_DATA_FOUND`/`TOO_MANY_ROWS`, `SAVEPOINT`, `EXECUTE IMMEDIATE` allow-list -> set-based DBSQL procedure with handlers, `SIGNAL`, `BEGIN ATOMIC` (traps 2, 3, 10, 11, 13) | A (L variant noted) |

Each `NOTE.md` lists the constructs, the recon tier that catches a wrong conversion, and what was not verified live.
