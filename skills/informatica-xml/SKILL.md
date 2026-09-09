---
name: informatica-xml
description: Source-dialect skill for Informatica PowerCenter/IICS estates. Use when enumerating a PowerCenter repository or its XML exports (workflows, sessions, mappings, mapplets, parameter files, pre/post-session shell and SQL), extracting mapping lineage, converting mappings/sessions/workflows to Databricks (Lakeflow Spark Declarative Pipelines, PySpark, Lakeflow Jobs, DBSQL procedures for pre/post SQL), or reconciling a converted Informatica unit with the harness. Hardened against the Albion insurance fixture estate; DataStage is a separate future skill and is not covered here.
---

# Informatica PowerCenter / IICS Dialect

## 1. When to use / routing

Use for PowerCenter 9.x/10.x (and IICS) repository XML exports: `MAPPING`, `SESSION`, `WORKFLOW`/`WORKLET`, `MAPPLET`, `.par` parameter files, pre/post-session commands and SQL. One unit = a mapping plus the sessions that run it plus their workflow task instances, keyed `<folder>.<mapping>`; reusable transformations and mapplets used by two or more mappings are shared objects (wave 0, convert once). Session-level `Sql Query` / `Lookup Sql Override` / `Source Filter` / `Pre SQL` / `Post SQL` overrides replace the mapping's SQL silently: always convert the session's effective SQL, transpiled with the connection's dialect skill (`teradata-bteq`, `oracle-plsql`).

Everything Databricks-side goes through `skills/target-routing/SKILL.md` to the official skills; this file cites, never restates:

| Legacy construct | Target | Official skill (section) |
|---|---|---|
| Mapping (transformations, ports) | Lakeflow Spark Declarative Pipelines, `from pyspark import pipelines as dp`, never `import dlt` | `databricks-pipelines` `references/python-basics.md` "Setup"; `references/expectations-python.md` "Decorators" (row errors / reject files) |
| Workflow / worklet DAG, links, Event Wait, Email, Command, Control-M / cron edges | Lakeflow Job: one task per session, `depends_on` per link, `run_if`, `trigger.file_arrival`, `email_notifications`, cron `schedule` deployed `PAUSED` | `databricks-jobs` SKILL.md "Multi-Task Workflows", "Job Parameters"; `references/task-types.md`; `references/triggers-schedules.md`; `references/notifications-monitoring.md` |
| Pre/Post SQL, Stored Procedure transformation, SQL-only Decision/Assignment | `CREATE PROCEDURE` + `CALL` from a `sql_task`; `DECLARE`/`SET`/`IF`; `EXECUTE IMMEDIATE` for `~param~` SQL | `databricks-dbsql` `references/sql-scripting.md` "CREATE PROCEDURE", "CALL", "Control Flow", "Exception Handling" |
| Connections, `$DBConnection_*`, file dirs, reject files, credentials | UC catalog/schema, external locations, volumes; secrets by name only | `databricks-unity-catalog` `references/2-external-locations.md`, `references/6-volumes.md`, `references/1-access-control.md` |
| OLTP front-door units only | Lakebase type column below | `databricks-lakebase` `references/synced-tables.md` "Data Type Mapping" |
| Analyzer inventory of the export (no transpiler flag exists) | embedded SQL overrides transpile under `teradata` / `oracle` | `skills/lakebridge/SKILL.md` Informatica row |

## 2. Type map

| Informatica port type | Delta | Lakebase | Handling |
|---|---|---|---|
| `decimal(p,s)`, `Enable high precision = YES` | `DECIMAL(p,s)` (p <= 38) | `NUMERIC` | `decimal_round` at port scale |
| `decimal(p,s)`, `Enable high precision = NO` (default) | `DECIMAL(p,s)`; legacy computed in double (15 digits) | `NUMERIC` | `decimal_round`; last-digit drift expected (trap 3) |
| `decimal` p > 38, Oracle `NUMBER` without scale | `DECIMAL(38,s)` or `DOUBLE` per STOP A | `NUMERIC` / `DOUBLE PRECISION` | Tier 2 sum drift is the signature |
| `integer`, `small integer`, `bigint` | `INT`, `SMALLINT`, `BIGINT` | `INTEGER`, `SMALLINT`, `BIGINT` | double->integer port assignment rounds (row 14) |
| `double`, `real` | `DOUBLE`, `FLOAT` | `DOUBLE PRECISION`, `REAL` | compare with `numeric_abs_tol`, not `decimal_round` |
| `string(n)` from `VARCHAR` | `STRING` | `TEXT` | width dropped; `empty_string_is_null` where an engine folds `''` to NULL (Oracle targets) |
| `string(n)` from `CHAR(n)` / fixed-width with `STRIPTRAILINGBLANKS="NO"` | `STRING` | `TEXT` | `rstrip_spaces`; `IS_SPACES`/`LENGTH` see the padding (trap 4) |
| `nstring`, `ntext`, `text` | `STRING` | `TEXT` | codepage `Latin1`/`MS1252` -> UTF-8 on read (trap 18); `text` excluded from Tier 3, hash-compare |
| `binary` | `BINARY` | `BYTEA` | `uuid_normalize` only for 16-byte GUIDs |
| `date/time` (29,9) from `TIMESTAMP(6)`, Oracle `DATE` | `TIMESTAMP_NTZ` (zone-less legacy); `TIMESTAMP` only if STOP A says UTC | `TIMESTAMP [WITHOUT TIME ZONE]` | `datetime_utc_truncate_ms` (traps 7, 8) |
| `date/time` into a `DATE` target column | `DATE` | `DATE` | `CAST(ts AS DATE)` only after confirming the legacy target truncated |
| `timestamp with time zone` | `TIMESTAMP` (UTC) | `TIMESTAMP WITH TIME ZONE` | `datetime_utc_truncate_ms` |
| Flat-file `PICTURETEXT="9(09)V99"` (implied decimals) | `DECIMAL(11,2)` via `CAST(substr AS DECIMAL(11,0)) / 100` | `NUMERIC` | Tier 2 sum 100x off if missed (trap 21) |
| Flat-file `9(05)` Julian `YYDDD`, `X(n)` codes | `STRING` + derived `DATE` | `TEXT`, `DATE` | pivot year lives in converted code (trap 22) |
| Packed / `COMP-3` (Normalizer input) | `DECIMAL(p,s)` after unpacking | `NUMERIC` | hand conversion |
| Sequence Generator `NEXTVAL` | `BIGINT GENERATED ALWAYS AS IDENTITY` | `BIGINT` | never compared by value (trap 19) |

## 3. Function and construct map

Databricks expressions are SQL, usable verbatim via `F.expr(...)` or in DBSQL. Keep legacy port names as column aliases so Tier 3 field mapping is 1:1.

| # | Informatica | Databricks | Edge case |
|---|---|---|---|
| 1 | `IIF(c, a, b)` | `CASE WHEN c THEN a ELSE b END` | NULL condition -> ELSE in both |
| 2 | `IIF(c, a)` | `CASE WHEN c THEN a ELSE <typed empty> END` | omitted ELSE is 0 / `''` / NULL by type, not NULL for all |
| 3 | `DECODE(v, s1, r1, ..., default)` | `CASE v WHEN s1 THEN r1 ... ELSE default END` | NULL `v` falls to default in both |
| 4 | `ISNULL(x)` | `x IS NULL` | - |
| 5 | `IS_SPACES(s)` | `s RLIKE '^ +$'` | FALSE for `''`; evaluate before any rtrim |
| 6 | `IS_DATE(s, fmt)` | `try_to_timestamp(s, fmt') IS NOT NULL` | tokens per row 9 |
| 7 | `IS_NUMBER(s)` | `try_cast(s AS DOUBLE) IS NOT NULL AND s NOT RLIKE '(?i)^\\s*[+-]?(inf|infinity|nan)\\s*$'` | accepts exponent notation, rejects Inf/NaN; never `try_cast AS DECIMAL` for the test |
| 8 | `IN(port, v1, ..., caseFlag)` | `port IN (...)`; caseFlag 0 -> `lower()` both sides | - |
| 9 | `TO_DATE(s, fmt)` | `to_timestamp(s, fmt')`: `YYYY->yyyy DD->dd HH24->HH HH12->hh MI->mm SS->ss MS->SSS US->SSSSSS MON->MMM MONTH->MMMM DY->EEE DAY->EEEE AM/PM->a` | `RR`, `J`, `Q`, `SSSSS`, `NS` hand-convert; unparsable = row error -> `try_to_timestamp` + `@dp.expect_or_drop` + quarantine |
| 10 | `TO_DATE(s)` / `TO_CHAR(d)` no format | session `DateTime Format String` (default `MM/dd/yyyy HH:mm:ss`) | read the session config |
| 11 | `TO_CHAR(d, fmt)` | `date_format(d, fmt')` | row 9 tokens |
| 12 | `TO_CHAR(n)` | `CAST(n AS STRING)` | trailing zeros differ; compare numerically |
| 13 | `TO_DECIMAL(s[, scale])` | `try_cast(s AS DECIMAL(p, scale))` | non-numeric/empty -> **0** legacy, NULL Spark: `coalesce(..., 0)` if consumers relied on it |
| 14 | `TO_INTEGER(s[, flag])` | flag omitted/FALSE: `CAST(round(CAST(s AS DECIMAL(38,10))) AS INT)`; TRUE: plain `CAST` | legacy **rounds** by default; non-numeric -> 0 |
| 15 | `TO_FLOAT(s)` | `CAST(s AS DOUBLE)` | - |
| 16 | `ROUND(n[, p])` | `round(n, p)` | half away from zero in both; drift only under high precision OFF |
| 17 | `ROUND(d, 'DD'\|'MM'\|'YYYY'\|'HH'\|'MI')` | `CASE WHEN hour(d) >= 12 THEN date_trunc('DAY', d) + INTERVAL 1 DAY ELSE date_trunc('DAY', d) END` (compose per unit) | `MM` rounds up from the 16th, `YYYY` from July 1 |
| 18 | `TRUNC(d, part)` | `date_trunc(part', d)` | - |
| 19 | `TRUNC(n, p)` | `sign(n) * floor(abs(n), p)` | truncates toward zero |
| 20 | `ADD_TO_DATE(d, 'MM'\|'YY', n)` | `add_months(d, n)` / `add_months(d, 12*n)` | month-end clamp identical |
| 21 | `ADD_TO_DATE(d, 'DD'\|'HH'\|'MI'\|'SS', n)` | `date_add` / `timestampadd(unit, n, d)` | - |
| 22 | `DATE_DIFF(d1, d2, 'DD')` | `(unix_timestamp(d1) - unix_timestamp(d2)) / 86400.0` | fractional double; `datediff` is whole days |
| 23 | `DATE_DIFF(d1, d2, 'MM'\|'YY')` | `months_between(d1, d2)` [`/ 12`] | into an `integer` port: `round()` (row 14), record it |
| 24 | `DATE_COMPARE(d1, d2)` | `CASE WHEN d1 IS NULL OR d2 IS NULL THEN NULL WHEN d1 < d2 THEN -1 WHEN d1 = d2 THEN 0 ELSE 1 END` | explicit NULL arm required |
| 25 | `LAST_DAY(d)` | `timestamp(last_day(d)) + make_interval(0,0,0,0,hour(d),minute(d),second(d))` | legacy keeps the time component |
| 26 | `GET_DATE_PART(d, part)` | `year/month/day/hour/minute/second(d)`; `'DAY'` -> `date_format(d, 'EEEE')` | - |
| 27 | `SET_DATE_PART`, `MAKE_DATE_TIME` | `make_timestamp(...)` | Feb 30 = row error legacy, NULL Spark |
| 28 | `SYSDATE`, `SESSSTARTTIME` | `current_timestamp()` in the engagement zone, `TIMESTAMP_NTZ` columns | `SYSDATE` is per-row node local time |
| 29 | `SYSTIMESTAMP('NS')` | `current_timestamp()` (us) | `datetime_utc_truncate_ms` |
| 30 | `LTRIM/RTRIM(s)`; two-arg `LTRIM(s, set)` | `ltrim/rtrim(s)`; `trim(LEADING set FROM s)` | spaces only in one-arg form |
| 31 | `LPAD/RPAD(s, n, pad)` | `lpad/rpad` | legacy NULL when `pad = ''` |
| 32 | `SUBSTR(s, start[, len])` | `substr` | 1-based, negative start from end, both; numeric port implicitly cast legacy |
| 33 | `INSTR(s, x[, start[, occ]])` | `locate(x, s, start)` | `occ > 1` / negative start -> `regexp_instr` |
| 34 | `LENGTH(s)` | `length(s)` | counts CHAR padding (trap 4) |
| 35 | `UPPER/LOWER` | same | - |
| 36 | `INITCAP(s)` | split on `(?<=[^a-z0-9])(?=[a-z0-9])\|(?<=[a-z0-9])(?=[^a-z0-9])` and upper each token's first char (see example 03) | legacy starts a word at **any** non-alphanumeric (`O'Brien`, `Smith-Jones`); `initcap` only at whitespace |
| 37 | `a \|\| b`, `CONCAT` | `concat_ws('', a, b)` | legacy **skips NULL operands**; `concat` returns NULL |
| 38 | `REPLACECHR(caseFlag, s, chars, new)` | `translate(s, chars, repeat(new, length(chars)))`; `new = ''` deletes; caseFlag 0 with letters -> `regexp_replace(s, '(?i)[chars]', new)` | - |
| 39 | `REPLACESTR(caseFlag, s, old..., new)` | `replace(...)` nested in argument order; caseFlag 0 -> `regexp_replace(s, '(?i)' \|\| regexp_quote(old), new)` | caseFlag 0 also replaces `'al/'` for `'AL/'` (example 05) |
| 40 | `REG_MATCH(s, p)` | `s RLIKE p` | POSIX `[:alpha:]` classes rewrite |
| 41 | `REG_EXTRACT(s, p[, n])` | `nullif(regexp_extract(s, p, n), '')` | no match: NULL legacy, `''` Spark |
| 42 | `REG_REPLACE(s, p, new[, n])` | `regexp_replace(s, p, new)` | replacement count limit has no equivalent |
| 43 | `CHR(n)` / `ASCII(s)` | `chr(n)` / `ascii(s)` | `n` is in the session codepage: `MS1252` 0x80-0x9F differ from Unicode |
| 44 | `SOUNDEX(s)` | `CASE WHEN s RLIKE '[A-Za-z]' THEN soundex(regexp_replace(upper(s), '^[^A-Z]+', '')) END` | legacy NULL when no letters; Spark returns the input unchanged |
| 45 | `METAPHONE`, `COMPRESS`, `AES_*` (default modes) | none | hand-convert; `AES` mode/padding verified on a known plaintext |
| 46 | `MD5(s)`, `CRC32(s)` | `md5(s)` / `md5(encode(s, 'ISO-8859-1'))` under Latin1 | hash of non-ASCII and CHAR padding differ |
| 47 | `ABS CEIL FLOOR SQRT EXP LN SIGN POWER LOG MOD %` | same names | `LN(0)`, `SQRT(-1)`, `MOD(a, 0)` are row errors legacy, NULL/NaN Spark |
| 48 | `a / b` into an integer port | `a / b` then row 14 rounding | legacy evaluates in double |
| 49 | `GREATEST/LEAST(a1..aN[, caseFlag])` | `CASE WHEN a1 IS NULL OR ... OR aN IS NULL THEN NULL ELSE greatest(a1..aN) END`, one term per argument | legacy NULL if **any** argument NULL; Spark skips NULLs |
| 50 | `RAND()` | `rand()` | exclude from Tier 3 |
| 51 | `ERROR('msg')` | `@dp.expect_or_drop("name", "<negated cond>")` + quarantine table | row error -> reject file; reproduce the file as a table if anything reads it |
| 52 | `ABORT('msg')` | `@dp.expect_or_fail(...)` / SQL scripting handler | job `on_failure` replaces the Email task |
| 53 | `SETVARIABLE`, `SETMAXVARIABLE`, ... | watermark table `wm_<unit>` written at task end; `dbutils.jobs.taskValues` intra-run | persisted only on success; seed from the `.par` (trap 20) |
| 54 | `$$param`, `$DBConnection_*`, `$InputFile_*`, `$BadFileName` | job parameters / pipeline configuration; catalog.schema; volume paths | resolve: session `.par` section > workflow section > `pmcmd -paramfile` > `DEFAULTVALUE` > repository value (INFERRED) |
| 55 | `:LKP.name(args)` (unconnected) | `LEFT JOIN` a deduplicated lookup view, one return port | NULL when unmatched |
| 56 | `:SP.name(args)` (unconnected) | `CALL` only if the procedure migrated; else SQL UDF / pre-joined result | per-row calls do not exist |
| 57 | Variable port (`LOCAL VARIABLE`) | `withColumn` step before the outputs that use it | self-referencing variable = stateful `lag()` over an INFERRED order |
| 58 | Port `DEFAULTVALUE` | `coalesce(expr, default)` | legacy also applies defaults to transformation errors: pair with `try_*` |
| 59 | Expression transformation | `withColumn` chain on the **same row** | row-preserving: never split into views and re-join on a non-unique key (examples 01, 05) |
| 60 | Filter | `.filter(cond)` | NULL drops in both |
| 61 | Router | one `filter()` **per group**; DEFAULT = `NOT (coalesce(c1, FALSE) OR ...)` | a row enters every matching group; NULL condition is not a match |
| 62 | Union | `unionByName` / `UNION ALL` | never `UNION` distinct |
| 63 | Lookup, connected, static cache, `Lookup policy on multiple match` | join to `row_number() OVER (PARTITION BY keys ORDER BY <order>) = 1` | `Use First/Last Value` = cache build order (INFERRED); `Use All Values` multiplies rows; `Report Error` -> `expect_or_fail` |
| 64 | Lookup `Lookup Sql Override` / `Lookup Source Filter` | the override SQL is the lookup view | may read tables the lookup table name does not |
| 65 | Lookup uncached / persistent cache | same join; snapshot at read time | timing differences, not logic; recon on a frozen snapshot |
| 66 | Lookup dynamic cache (`NewLookupRow`) | `dp.create_auto_cdc_flow(..., stored_as_scd_type=1\|2)` or `MERGE` | `Output Old Value On Update` = pre-image columns |
| 67 | Lookup / Joiner `Case Sensitive String Comparison = NO` | `lower()` keys or `COLLATE UTF8_LCASE` (`databricks-dbsql` SKILL.md "Collation") | `collation_casefold` on those keys only |
| 68 | Aggregator `SUM AVG MIN MAX COUNT(port)`, `SUM(x, cond)` | grouped aggregates; `sum(CASE WHEN cond THEN x END)` | `COUNT(*)` -> `count(*)` |
| 69 | Aggregator `FIRST/LAST(port)`, non-aggregated ports | `min_by(port, <order>)` / `max_by(port, <order>)` in the same `GROUP BY`; never a window in the select list | pipeline order is INFERRED; window keeps every input row (Tier 1) |
| 70 | Aggregator `MEDIAN`, `PERCENTILE(x, p)` | `median(x)`, `percentile(x, p/100)` | `p` is 0-100 legacy |
| 71 | Aggregator `Sorted Input = YES` on unsorted data | `groupBy` | legacy emitted multiple groups per key: legacy defect, decide |
| 72 | Sorter | `ORDER BY` only where order is consumed (files, FIRST/LAST, Rank); `Distinct` -> `DISTINCT` | table targets have no order |
| 73 | Rank (Top/Bottom N) | `row_number() OVER (PARTITION BY grp ORDER BY port DESC) <= N` | ties resolved by pipeline order legacy |
| 74 | Joiner `Normal / Master Outer / Detail Outer / Full Outer` | `INNER / LEFT (keeps detail) / RIGHT (keeps master) / FULL OUTER` | "Master Outer" keeps **detail** rows |
| 75 | Update Strategy `DD_INSERT/UPDATE/DELETE/REJECT` | `op` column -> `dp.create_auto_cdc_flow(apply_as_deletes="op='D'")` or `MERGE` | session `Treat source rows as` != `Data driven` ignores the flags; target checkboxes gate each op; `DD_REJECT` -> quarantine |
| 76 | Sequence Generator | identity column or `row_number() OVER (ORDER BY natural key)` | values never match legacy |
| 77 | Normalizer (`OCCURS`, `GCID_*`) | `posexplode(array(...))` with `pos + 1` as `GCID` | `GCID` is 1-based |
| 78 | Transaction Control, `Commit Interval`, `Rollback on Errors` | none: Delta writes are atomic; multi-table via `BEGIN ATOMIC ... END` | mid-run partial visibility disappears (trap 25) |
| 79 | Stored Procedure transformation (pre/post load) | `CREATE PROCEDURE` + `CALL` in a `sql_task` before/after the pipeline task | procedure still on the source database = cross-system call finding |
| 80 | SQL transformation (`~param~`), Java / Custom / HTTP / XML transformations | `EXECUTE IMMEDIATE ... USING`; PySpark UDF; `http_request`; `from_xml` | dynamic SQL lineage INFERRED; no side effects inside SDP dataset functions |
| 81 | Workflow link `$s.Status = SUCCEEDED` / `= FAILED` / empty / count predicate | `run_if: ALL_SUCCESS` / `AT_LEAST_ONE_FAILED` / `ALL_DONE` / upstream `taskValues.set` + downstream check | no expression-typed `run_if` |
| 82 | Decision / Assignment task, `WORKFLOWVARIABLE` | SQL scripting `IF`/`DECLARE`/`SET` in one script, else a task publishing a task value | - |
| 83 | Event Wait (`File Watch Name`), Event Raise, Timer | `trigger.file_arrival` on the landing location; `depends_on`; cron | `$$RUNDATE`-templated names become a glob; never `sleep` |
| 84 | Command task / pre-post-session command | file moves -> volume ops task; `pmcmd startworkflow` -> `run_job_task`; `mailx` -> `email_notifications`; SFTP pull -> ingestion decision (D3) | other engines' scripts kicked = cross-pipeline edge (D5) |
| 85 | `Recovery Strategy`, `Fail parent if this task fails`, `SUSPEND_ON_ERROR` | `max_retries`, `min_retry_interval_millis`; failure propagates via `run_if` | every load must be rerunnable from scratch |
| 86 | `Truncate target table option`, `Target load type = Bulk` | `TRUNCATE` in the pre procedure or MV full recompute; plain Delta write | bulk loaders diverted duplicates silently (trap 26) |
| 87 | Session partitions with per-partition `Sql Query` | `UNION ALL` the partition variants into one read | the hidden filter most often lost |
| 88 | Scheduler `RECURRING`, Control-M `CYCLIC`/`TIMEFROM`, cron | Quartz cron + `timezone_id`, `PAUSED`; 30-min cadence -> `0 0/30 7-20 * * ?` | `MONTHDAYS="WD1"` = daily cron + business-day check task |
| 89 | Control-M `INCOND/OUTCOND`, duplicate cron owners | `depends_on` / `run_job_task`; one owner under D5 | missing legacy dependencies are findings, not edges |

## 4. Traps (recon signature)

| # | Trap | Legacy | Databricks | Signature | Fix / canon |
|---|---|---|---|---|---|
| 1 | Session / partition SQL overrides | replace mapping SQL silently | mapping SQL converted | Tier 1 count, Tier 3 missing columns | extract effective SQL per session **and** partition |
| 2 | Implicit port conversions | round / 0-on-failure | error or NULL | Tier 3 off-by-one, Tier 2 null rate | explicit `try_cast`/`round` per port; gap `zero_null_equiv` |
| 3 | `Enable high precision = NO` | double arithmetic | exact `DECIMAL` | Tier 3 last digit, Tier 2 sum drift | `decimal_round half_up`; never widen tolerance without a decision |
| 4 | CHAR padding, `IS_SPACES`, `LENGTH` | padded | unbounded `STRING` | Tier 3 strings, Tier 2 distinct | `rstrip_spaces`; test on the untrimmed value |
| 5 | `''` vs NULL (`NULL_CHARACTER`, Oracle targets) | engine-specific | distinct | Tier 2 null rate | `empty_string_is_null` (STOP A); `nullif(x, '')` at Oracle boundaries |
| 6 | `TO_DATE` format on transposed data | day <= 12 transposes, else row error | same, or NULL | Tier 3 dates, Tier 2 null rate | reproduce like-for-like, file the defect; rejects -> quarantine |
| 7 | Timezone | zone-less local time | UTC `TIMESTAMP` | Tier 3 constant offset | `TIMESTAMP_NTZ`; gap `timestamp_offset_shift` |
| 8 | Datetime precision | ns / `TIMESTAMP(6)` / seconds | us | Tier 3 sub-ms | `datetime_utc_truncate_ms` |
| 9 | Lookup multiple match | first/last by cache order | all matches | Tier 1 excess, Tier 3 looked-up cols | `row_number()` over an explicit INFERRED order |
| 10 | Lookup cache staleness | stale persistent / per-row snapshots | one snapshot | Tier 3 on rows changed mid-run | frozen recon snapshot; document |
| 11 | Router | every matching group | `CASE` first-match | Tier 1 per-target lower | one filter per group |
| 12 | Update Strategy vs session flags | flags gate ops, `DD_REJECT` to file | MERGE applies all | Tier 1 excess | gate ops as the session did; quarantine rejects |
| 13 | Aggregator order dependence | pipeline order | order-free | Tier 3 FIRST/LAST cols, Tier 1 groups | explicit order (INFERRED) |
| 14 | `\|\|` with NULL | NULL skipped | NULL | Tier 2 null jump | `concat_ws` |
| 15 | `INITCAP` boundaries | any non-alphanumeric | whitespace | Tier 3 names with `'`/`-` | regex casing (example 03) |
| 16 | `REG_EXTRACT` no match | NULL | `''` | Tier 2 null drop | `nullif` |
| 17 | caseFlag 0 / case-insensitive compares | insensitive | sensitive | Tier 3 unreplaced, Tier 2 key distinct | `(?i)`, `lower()`, `COLLATE UTF8_LCASE`; `collation_casefold` on those columns only |
| 18 | Codepage `Latin1`/`MS1252` | `CHR`, `MD5`, bytes per codepage | Unicode | Tier 3 hashes, 0x80-0x9F strings | read with the codepage; `encode` before hashing; gap `codepage_transcode` |
| 19 | Sequence Generator | gaps, cached blocks | identity | Tier 3 surrogates | exclude; join on natural keys |
| 20 | Persisted mapping variables | repository state | none | Tier 1 on first run | seed watermark from `.par` or live `pmrep` |
| 21 | Fixed-width files | `OFFSET/LENGTH`, `PICTURETEXT V99`, `NULL_CHARACTER` | text + `substr` | Tier 2 sum 100x, shifted fields | generate reader from `SOURCEFIELD`; `/ 10^scale` (example 01) |
| 22 | Two-digit-year pivot | `IIF(yy <= 49, 2000+yy, 1900+yy)` | none | Tier 3 100-year diffs | reproduce literally; cross-engine pivots are findings |
| 23 | Double -> integer port | round | cast truncates | Tier 3 off-by-one | `round()` explicitly (row 14) |
| 24 | Double schedule ownership | Control-M + cron | one schedule | Tier 1 inflated appends | inventory both; D5 owner |
| 25 | Commit-interval visibility | partial loads mid-run | atomic | Tier 1 after legacy mid-run failure | recon completed runs only |
| 26 | Bulk loaders | duplicates diverted to error tables | all rows written | Tier 1 excess | dedupe per legacy unique index under a decision |

## 5. Canonical target shape

- Mapping -> one SDP file: `@dp.table` Auto Loader read for file sources (`_metadata.file_path` supplies `$$RUNDATE`-style values), `@dp.temporary_view` per lookup deduplicated to one row per key, row-preserving Expressions as `withColumn` on the same row, `@dp.materialized_view` target named after the legacy target with port names as aliases, `@dp.expect*` for every legacy row-error/reject condition plus a quarantine MV built from the negated conditions.
- Workflow -> one Lakeflow Job: task key = session name, `pipeline_task` per SDP unit, `depends_on` + `run_if` per link, `trigger.file_arrival` for Event Wait, `email_notifications.on_failure` for the failure Email task, cron `schedule` with `timezone_id`, deployed `PAUSED` via `databricks-dabs`.
- Pre/Post SQL and SQL-only tasks -> `CREATE PROCEDURE <unit>_pre()` / `<unit>_post()` (statements transpiled by the connection's dialect skill, `Continue` -> `DECLARE CONTINUE HANDLER`), `CALL`ed from `sql_task`s before/after the pipeline task; a Pre SQL `TRUNCATE`/`DELETE` on the target makes the unit a truncate-load.
- `canonicalization.json` (list form, `recon.config.load_canon_rules`): `decimal_round` (trap 3), `datetime_utc_truncate_ms` (7, 8), `rstrip_spaces` (4), `empty_string_is_null` (5, 16), `null_missing_equiv`, `collation_casefold` (17, flagged columns only), `identity`. Harness gaps, never faked: `zero_null_equiv`, `timestamp_offset_shift`, `codepage_transcode`, per-field rule parameters.

## 6. Examples

| Example | Fixture object | Exercises |
|---|---|---|
| `examples/01_policy_master_daily/` (`source.xml`, `converted.py`) | `INS_POLICY.m_POLICY_MASTER_DAILY` + `wf_POLICY_MASTER_DAILY` | fixed-width `SOURCEFIELD` reader, `NULL_CHARACTER`, implied `V99` decimals, Julian pivot 49, `TO_INTEGER` 0-on-failure, `INSTR`/`SUBSTR`/`REG_MATCH` postcode DQ, connected Lookup `Use First Value`, `$$RUNDATE` from the arrived file name, Event Wait + failure Email |
| `examples/03_party_mdm_sync/` (`source.xml`, `converted.py`) | `MDM_PARTY.m_PARTY_MDM_SYNC` + shared `mplt_DQ_PARTY_STANDARDISE` | mapplet as a shared function module, `INITCAP` boundaries, `REPLACESTR`/`REPLACECHR`, `\|\|` NULL skipping, `LENGTH` on CHAR, `SOUNDEX` no-letter guard, `TO_DATE('DD/MM/YYYY')` row errors, one-row-per-key fuzzy match, Oracle `''` -> NULL |
| `examples/05_ri_bordereaux_monthly/` (`source.xml`, `converted.py`) | `RI_CESSIONS.m_RI_BORDEREAUX_MONTHLY` | Auto Loader with `ISO-8859-1` encoding, `CHR(163)`, `REPLACESTR` caseFlag 0 as `(?i)`, `TO_DECIMAL` 0-vs-NULL surfaced as an expectation, drifted reference Lookup deduplicated, unexpected-broker rows quarantined not published, warn-only expectations where the legacy had no Filter |
