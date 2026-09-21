---
name: trino
description: Source-dialect skill for Trino (formerly PrestoSQL) estates - federated SQL over Hive/Iceberg/Delta lake catalogs and JDBC catalogs (PostgreSQL, MySQL, ...), CTAS/INSERT marts, views, and the dbt/Airflow wrappers that schedule them. Load it when enumerating a Trino estate, converting Trino SQL units to Databricks SQL / Lakeflow, or reconciling a converted unit (its `canonicalization.json` and `type_map.trino` feed the harness). Target-side facts live in the official databricks skills behind `target-routing`.
---

# Trino Dialect

## When to use / routing

Source-side half of the factory for Trino. Trino stores nothing: every table lives in a connector catalog (Hive
metastore + Parquet/ORC on object storage, Iceberg, Delta Lake, or a JDBC database), and the estate's value is the
SQL that joins across those catalogs. So a Trino migration has three source-side questions this skill answers:
what each catalog *is* (section 1), which SQL crosses catalogs (sections 2-3), and which Trino semantics silently
differ on Databricks (sections 5-8). Everything Databricks-side is a pointer through `skills/target-routing/SKILL.md`
to the official skills. Load `databricks-core` first.

Citations: `[dbsql:<file>#<section>]` = `databricks-dbsql/references/<file>`, likewise `[jobs:]`, `[pipelines:]`,
`[uc:]` (`databricks-unity-catalog/references/`), `[lakeflow-connect:]`; `[docs:<path>]` =
`docs.databricks.com/aws/en/sql/language-manual/<path>`; `[trino:<path>]` = `trino.io/docs/current/<path>`.
Rows marked **(probe 483/2026.36)** were observed on Trino 483 and a Databricks SQL warehouse (DBSQL 2026.36) with
the probe files in `examples/00_probe/`; every other row is read from the cited docs and is a SEEDED expectation
until a unit's recon confirms it.

Landing rule of thumb (the *decision* is the migration plan's, at STOP B):

| Trino catalog connector | Databricks landing | Skill |
|---|---|---|
| `hive` on Parquet/ORC (external metastore) | UC external tables over the same files, then `CONVERT TO DELTA` or CTAS into managed Delta | `[uc:3-securables-ddl.md#Tables — Managed vs External]`, `[uc:2-external-locations.md]` |
| `iceberg` | UC managed Iceberg / external Iceberg tables; keep the REST catalog only during parallel run | `databricks-unity-catalog` SKILL.md |
| `delta_lake` | register the existing Delta location as a UC external table; no data movement | `[uc:3-securables-ddl.md#Tables — Managed vs External]` |
| `postgresql`, `mysql`, `sqlserver`, `oracle`, `redshift`, `bigquery`, `snowflake` (JDBC) | Lakehouse Federation connection + foreign catalog for parallel run; Lakeflow Connect ingestion when the mart must be materialised | `databricks-dbsql` SKILL.md "Lakehouse Federation"; `[lakeflow-connect:2-database-connectors.md]` |
| `kafka`, `elasticsearch`, `mongodb` | streaming table / connector ingestion | `databricks-pipelines` `[pipelines:kafka.md]`, `databricks-lakeflow-connect` |
| `tpch`, `tpcds`, `memory`, `blackhole` | test fixtures: inventory, never migrate | — |

## 1. Enumeration (read-only)

Everything below is a `SELECT` or `SHOW`. Never `CALL system.*` procedures that write (`sync_partition_metadata`,
`register_table`, `flush_metadata_cache`), never `ANALYZE`, never `SET SESSION` on a shared coordinator during census.

Minimum privileges: `SELECT` on `information_schema` of each catalog (implied by any table grant under the
file/`allow-all` access control; under Ranger/OPA request `SELECT` on `<catalog>.information_schema.*`), and
read access to `system.runtime.queries` (own queries by default; all users' queries need the `system` catalog's
`queries` privilege or an event-listener export handed over by the platform team).

| What | Query | Notes |
|---|---|---|
| Catalogs and connectors | `SELECT catalog_name, connector_name FROM system.metadata.catalogs` | connector decides the landing row above; `system` is Trino itself **(probe 483)** |
| Schemas | `SELECT catalog_name, schema_name FROM system.jdbc.schemas` or `<cat>.information_schema.schemata` | one row per catalog.schema |
| Tables and views | `SELECT table_catalog, table_schema, table_name, table_type FROM <cat>.information_schema.tables` | `table_type` = `BASE TABLE` / `VIEW`; materialized views: `system.metadata.materialized_views` |
| Columns | `SELECT ... FROM <cat>.information_schema.columns` | `data_type` is the *Trino* type as mapped by the connector (a PostgreSQL `char(4)` shows as `char(4)`, a `numeric` without scale as `decimal(38,?)` per connector config): keep it as the source type in the type map |
| Table DDL incl. connector properties | `SHOW CREATE TABLE <cat>.<schema>.<table>` | Hive `WITH (format=, partitioned_by=, bucketed_by=, sorted_by=, external_location=)`; Iceberg `partitioning=`, `location=`; JDBC tables have none |
| View SQL | `SHOW CREATE VIEW ...` / `information_schema.views.view_definition` | records `SECURITY DEFINER` vs `INVOKER`; views are Trino-SQL, connectors do not see them |
| Table comments / properties | `system.metadata.table_comments`, `system.metadata.table_properties` | comments carry into UC `COMMENT` |
| Statistics | `SHOW STATS FOR <table>` | row counts and NDV for the census; free on Hive when `ANALYZE` was run before, never run it yourself |
| Recent queries (lineage seed) | `SELECT query_id, "user", source, query, started, "end", state FROM system.runtime.queries` | coordinator memory only (typically the last few hours); ask for the event-listener/query-log export (Starburst Insights, Trino Gateway history, or a Kafka event listener) for a 30-day window |
| Functions incl. UDFs | `SHOW FUNCTIONS` | 900+ builtins on 483 **(probe)**; rows with a non-`system` catalog are SQL UDFs (`CREATE FUNCTION`) or plugin functions: each is a unit |
| Session defaults | `SHOW SESSION` | `time_zone`, `legacy_timestamp`, `insert_existing_partitions_behavior`, `query_max_*`; the coordinator's `config.properties` is the source of truth, request a copy |
| Grants | `SHOW GRANTS ON <table>`, `SHOW ROLES [IN <cat>]`, `SHOW ROLE GRANTS` | only meaningful under system access control that supports SQL-level grants; file-based rules live in `rules.json` (section 9) |

Repository census (the SQL is the estate): glob `**/*.sql`, `**/*.trino`, dbt `models/**/*.sql` with
`profiles.yml` `type: trino`, Airflow DAGs importing `TrinoOperator`/`trino.dbapi`, Superset/Metabase saved queries
exported as JSON, JDBC URLs `jdbc:trino://`/`jdbc:presto://`, Python `trino.dbapi.connect`. Count `SET SESSION`,
`USE`, `CALL`, `EXECUTE`/`PREPARE`, `${var}`/`{{ }}` templating per file: they decide the unit boundary in section 3.

## 2. Lineage extraction

Trino has no server-side lineage table. Build edges from three sources, in this order of trust:

1. **Query log** (`system.runtime.queries` now, event-listener export for history): parse every `INSERT`, `CREATE
   TABLE ... AS`, `CREATE [OR REPLACE] [MATERIALIZED] VIEW`, `MERGE`, `DELETE` for the target, and every
   `FROM`/`JOIN`/`UNNEST(<col>)` reference for readers. Three-part names resolve against the query's `catalog`/
   `schema` columns when a name is one- or two-part. Edges are CONFIRMED.
2. **View definitions**: `information_schema.views.view_definition` gives view -> base-table edges. Views store
   the *expanded* catalog-qualified SQL, so cross-catalog reads are explicit. CONFIRMED.
3. **Repository SQL and dbt manifests**: `target/manifest.json` `depends_on.nodes` when the estate is dbt; otherwise
   static parse of the files from section 1. Edges are INFERRED until seen in the log.

Cross-catalog edges are the migration-relevant ones: a `JOIN` between a `hive` table and a `postgresql` table means
the target unit either federates (foreign catalog) or waits for an ingestion unit that materialises the JDBC side.
Record `source_catalog_connector` on every edge; the dependency resolver (`13-dependency_resolution.md`) orders
JDBC-materialisation units before the marts that read them. Hidden Hive columns (`"$path"`,
`"$file_modified_time"`, `"$partition"`) and `$properties` tables (`<table>$partitions`) are lineage to *files*, not
tables: flag them, they have no like-for-like on Delta (`_metadata.file_path` is the nearest, `[pipelines:auto-loader-sql.md]`).

## 3. Unit definition

| Trino shape | Unit | Target shape (decision at STOP B) |
|---|---|---|
| `CREATE TABLE ... WITH (...)` / `CREATE SCHEMA` (Hive/Iceberg DDL) | DDL unit per table, grouped per schema | Delta DDL, `CLUSTER BY` for `bucketed_by`/`sorted_by`, `PARTITIONED BY` only above ~1 TB `[dbsql:best-practices.md#Liquid Clustering vs Traditional Partitioning]` |
| Seed / one-off `INSERT INTO ... VALUES` | seed unit (reference data) | `INSERT` or a CSV in a volume + `read_files` `[uc:6-volumes.md#Read Files from Volumes]` |
| Nightly `CREATE TABLE <mart> AS SELECT ...` (drop + CTAS) or `INSERT INTO ... SELECT` guarded by `DELETE` | one ETL unit per target table, including its `DELETE`/`DROP` prelude | `CREATE OR REPLACE TABLE ... AS` for full rebuilds; `INSERT OVERWRITE` / `MERGE` for partition or key loads; a Lakeflow materialized view when the SQL is a pure transform `[pipelines:materialized-view-sql.md]` |
| `CREATE [OR REPLACE] VIEW` | view unit; `SECURITY DEFINER` views carry a governance finding | UC view `[uc:3-securables-ddl.md]`; `DEFINER` semantics are the UC default (owner's rights) |
| `CREATE MATERIALIZED VIEW` (Iceberg) + `REFRESH MATERIALIZED VIEW` | MV unit; the refresh call is its schedule | Lakeflow materialized view with a schedule `[pipelines:materialized-view-sql.md]` |
| Report `.sql` (a `SELECT` a person or BI tool runs) | report unit, Tier 4 recon | DBSQL query / dashboard dataset; the SQL is the unit, the tool is out of scope |
| Cross-catalog query (`lake` + JDBC catalog in one statement) | the *reading* unit; add an ingestion or federation unit for the JDBC side as a dependency | foreign catalog for parallel run (`databricks-dbsql` "Lakehouse Federation"), ingestion unit when the mart is materialised |
| `CREATE FUNCTION` (SQL UDF) | function unit, converted before its callers | `CREATE FUNCTION` UC SQL UDF `[docs:sql-ref-syntax-ddl-create-sql-function]` |
| dbt model / Airflow task wrapping the SQL | the wrapper is not a unit: its schedule, retries and `depends_on` become the Lakeflow Job around the SQL unit | `[jobs:task-types.md#SQL Task]`, `[jobs:triggers-schedules.md]` |
| `SET SESSION <prop>` preceding a statement | part of the statement's unit; record the property in the unit's mapping (`insert_existing_partitions_behavior`, `time_zone`, `join_distribution_type` are the ones that change results or are dropped) | see section 6 |

Boundary rule: one unit writes one table (or is one view/function/report). A script that rebuilds three marts is
three units sharing a source file; `PREPARE`/`EXECUTE` with parameters is one unit per prepared statement.

## 4. Type map

`type_map.trino.databricks` in `canonicalization.json` is the machine copy the harness fills targets from and the
doctor audits (`type_map_audit`). Loss: none / precision / semantics. Canon rule = the harness rule that neutralises
the residual difference during recon, never a licence to skip the conversion.

| Trino | Delta / UC | Loss | Canon rule | Notes |
|---|---|---|---|---|
| `BOOLEAN` | `BOOLEAN` | none | `identity` | |
| `TINYINT` / `SMALLINT` / `INTEGER` / `BIGINT` | same (`INT` for `INTEGER`) | none | `identity` | integer literals: `1` is `integer`, `9999999999` is `bigint` on both **(probe)** |
| `REAL` | `FLOAT` | none | `identity` | Trino `approx_percentile` over `DECIMAL` returns `REAL` **(probe 483)**: the *target* type of such a column is `FLOAT`, not decimal |
| `DOUBLE` | `DOUBLE` | none | `identity` (recon `decimal_round` only where the census shows the column is recomputed) | `1.5e0` is double on both; `1.5` is `decimal(2,1)` on both **(probe)** |
| `DECIMAL(p,s)`, p <= 38 | `DECIMAL(p,s)` | none for storage; precision in arithmetic (section 7) | `identity`; `decimal_round` on recomputed columns | Trino `DECIMAL` without args is `decimal(38,0)`; JDBC `numeric` without scale maps per connector `decimal-mapping` |
| `VARCHAR` / `VARCHAR(n)` | `STRING` | none (length not enforced on cast, see trap) | `identity` | Databricks `VARCHAR(n)` exists for DDL; `CAST(x AS VARCHAR(3))` truncates on Trino and does not on Databricks **(probe)** |
| `CHAR(n)` | `STRING COLLATE UTF8_BINARY_RTRIM` when compares must ignore padding `[dbsql:geospatial-collations.md#Collation Modifiers (DBR 16.2+)]`, else `STRING` | semantics (padding, `length`) | `rstrip_spaces` | Trino pads on cast and compares padded (`'ab'` = `'ab  '` is true, `length` = 4); Databricks `CHAR(4)` lands as `string`, length 2, compare false **(probe)** |
| `VARBINARY` | `BINARY` | none | `identity` | `to_hex`/`from_hex` -> `hex`/`unhex`; hash outputs: recompute, never migrate |
| `JSON` | `VARIANT` (or `STRING` when the consumer only does `json_extract_scalar`) | none | `identity` on the extracted scalars | `parse_json` / `:` paths `[docs:functions/parse_json]` |
| `DATE` | `DATE` | none | `identity` | |
| `TIME(p)` / `TIME(p) WITH TIME ZONE` | `STRING` `'HH:mm:ss[.SSS]'` | semantics (no TIME type) | `identity` (harness gap `time_of_day_normalize`) | arithmetic moves to timestamps |
| `TIMESTAMP(p)` (zone-less; literal default p=0..6 from the literal, `localtimestamp` p=3) | `TIMESTAMP_NTZ` (`TIMESTAMP` only when the estate decides once to treat all values as UTC instants) | p <= 3 is millisecond precision; p 4-6 is exact; p > 6 is a declared harness gap | `datetime_utc_truncate_ms` only when effective p <= 3; `identity` otherwise | Trino keeps the literal's precision (`'..00'` is `timestamp(0)`, `'..123456'` is `timestamp(6)`) **(probe)**; Hive/Parquet timestamps are millis unless configured otherwise, so extract p > 6 at microsecond precision and compare with identity |
| `TIMESTAMP(p) WITH TIME ZONE` (`now()`, `current_timestamp`, `from_unixtime`, `AT TIME ZONE`) | `TIMESTAMP` (UTC-normalised instant) | p <= 3 is millisecond precision; p 4-6 is exact; p > 6 is a declared harness gap; offset text dropped | `datetime_utc_truncate_ms` only when effective p <= 3; `identity` otherwise | `now()` is `timestamp(3) with time zone` on Trino **(probe)**; a Databricks `TIMESTAMP` renders in the session zone (`current_timezone()`, `Etc/UTC` on the probe warehouse): pin `spark.sql.session.timeZone` in the mapping |
| `INTERVAL YEAR TO MONTH` / `INTERVAL DAY TO SECOND` | `INTERVAL YEAR TO MONTH` / `INTERVAL DAY TO SECOND` | none | `identity` | literal spelling `INTERVAL '1' MONTH` -> `INTERVAL 1 MONTH` (quoted form also parses on Databricks) |
| `ARRAY(T)` | `ARRAY<T>` | none | `identity` (element rules apply inside) | 1-based on Trino, 0-based `[]` on Databricks (section 5) |
| `MAP(K,V)` | `MAP<K,V>` | key order (unordered on Trino, insertion order on Databricks) | `identity` after `map_entries` sort in the recon query | never compare maps as text |
| `ROW(a T1, b T2)` / anonymous `ROW(...)` | `STRUCT<a:T1, b:T2>` | anonymous field names differ (`field0` vs `col1`) | `identity` on named fields | name every field in the DDL |
| `UUID` | `STRING` | type lost | `uuid_normalize` | `uuid()` returns `string` on Databricks **(probe)** |
| `IPADDRESS` / `IPPREFIX` | `STRING` | type and ordering lost | `identity` | range predicates need `inet_aton`-style UDF: GAP |
| `HyperLogLog` / `P4HyperLogLog` / `SetDigest` | none: recompute with `approx_count_distinct` or store `COUNT(DISTINCT)` | sketch not portable | exclude from Tier 3 | `approx_set`/`merge`/`cardinality` sketch pipelines are re-derived, never migrated |
| `QDigest` / `TDigest` | none: recompute `approx_percentile` / exact `percentile` | sketch not portable | exclude from Tier 3 | see trap "approximate aggregates" |
| `Geometry` / `SphericalGeography` | `GEOMETRY` / `GEOGRAPHY` `[dbsql:geospatial-collations.md#Geospatial Data Types]` | per function | `identity` | out of like-for-like scope |
| `color`, `CodePoints`, `KdbTree`, `JoniRegExp`, `Re2JRegExp`, `LikePattern` | internal: never in a table | — | — | appear only in `SHOW FUNCTIONS` signatures |

Connector-specific: PostgreSQL `char(n)`/`bpchar` arrive as `CHAR(n)` (padding trap applies), `numeric` without
precision follows `decimal-mapping=allow_overflow` + `decimal-default-scale`, `timestamptz` arrives as
`TIMESTAMP(6) WITH TIME ZONE`, `json`/`jsonb` as `JSON`, arrays as `ARRAY` only when `postgresql.array-mapping`
is `AS_ARRAY`; MySQL `TINYINT(1)` arrives as `TINYINT` not boolean; Hive `TIMESTAMP` is zone-less `TIMESTAMP(3)`
(Parquet `INT96`) and `hive.timestamp-precision` can raise it to 9.

## 5. Function / operator map

`sem`: same / edge / none. Expressions follow `databricks-dbsql` where cited; other builtins are plain Spark SQL
(verify with `databricks-core` tooling before relying on one not listed there). Rows marked **(probe)** were run on
both engines; the rest are read from `[trino:functions/*]` and `[docs:functions/*]`.

| # | Trino | Databricks SQL | sem | Edge case |
|---|---|---|---|---|
| 1 | `a / b` on integers | `a DIV b` | edge | Trino truncates to integer (`7/2` = `3`), Databricks `/` returns `3.5` **(probe)** |
| 2 | `dec(p1,s1) / dec(p2,s2)` | `CAST(a / b AS DECIMAL(p,s))` with the *Trino* result scale | edge | Trino `decimal(10,2)/3` is `decimal(21,13)`, Databricks `decimal(14,6)` **(probe)**: cast to the scale the consumer sees, `decimal_round` at that scale |
| 3 | `AVG(dec(p,s))` | `CAST(AVG(x) AS DECIMAL(p,s))` | edge | Trino keeps the input scale (`1.67`), Databricks widens to `decimal(p+4,s+4)` (`1.666667`) **(probe)** |
| 4 | `SUM(dec(p,s))` | `SUM(x)` | same | both widen precision only (`decimal(38,2)` vs `decimal(20,2)`) **(probe)** |
| 5 | `AVG(int)` / `SUM(int)` / `COUNT(*)` | same | same | `double` / `bigint` / `bigint` on both **(probe)** |
| 6 | `a % b`, `mod(a, b)` | same | same | sign follows the dividend on both (`-7 % 3` = `-1`) **(probe)**; `pmod` differs |
| 7 | `round(x)` / `round(x, n)` | `round(x)` / `round(x, n)` | same | half away from zero on both for decimals **(probe)**; `bround` is half-even |
| 8 | `truncate(x)` / `truncate(dec, n)` | `CAST(x AS INT)` is **not** it: use `sign(x)*floor(abs(x)*pow(10,n))/pow(10,n)` cast back | edge | no `truncate` numeric function on Databricks **(probe)**; Trino has no `truncate(double, n)` either |
| 9 | `CAST(dec AS INTEGER)` | `CAST(round(dec) AS INT)` | edge | Trino rounds (`2.9` -> `3`), Databricks truncates (`2`) **(probe)** |
| 10 | `CAST('2.9' AS INTEGER)` / `TRY_CAST` | `try_cast` | same | both reject the fractional string (NULL under `try`) **(probe)** |
| 11 | `TRY(expr)` | `try_divide`, `try_add`, `try_cast`, `try_element_at`, `try_to_timestamp` per expression | edge | Trino `TRY` covers any expression incl. overflow (`9223372036854775807 + 1` errors); Databricks plain `+` wraps silently unless ANSI mode is on **(probe: `try_add` NULL)** |
| 12 | `IF(c, a, b)` / `NULLIF` / `COALESCE` | same | same | **(probe)** |
| 13 | `greatest(a, NULL)` / `least` | `greatest(a, NULL)` | edge | Trino returns NULL when any argument is NULL, Databricks skips NULLs **(probe)**: wrap `CASE WHEN a IS NULL OR b IS NULL THEN NULL ...` |
| 14 | `a \|\| b` | `a \|\| b` / `concat` | same | NULL-propagating on both **(probe)**; `concat_ws` skips NULLs |
| 15 | `length(CHAR(n) col)` | `length(rtrim(col))` or `length(col)` | edge | Trino counts padding (4), Databricks stores unpadded (2) **(probe)** |
| 16 | `substr(s, 0, n)` | `substr(s, 1, n - 1)` | edge | position 0 yields `''` on Trino and behaves like 1 on Databricks **(probe)**; audit every `substr(..., 0` |
| 17 | `strpos(s, sub)` / `position(sub IN s)` | `instr(s, sub)` / `position(sub, s)` | same | 1-based, 0 when absent **(probe)** |
| 18 | `split_part(s, d, n)` | `split_part(s, d, n)` | edge | out-of-range index: Trino NULL, Databricks `''` **(probe)**; `nullif(split_part(...), '')` when the source relies on NULL |
| 19 | `split(s, d)[n]` | `element_at(split(s, d), n)` | edge | 1-based on both for `element_at`; `[n]` is 0-based on Databricks |
| 20 | `regexp_like(s, re)` | `s RLIKE re` / `regexp_like` | same | Java regex on both **(probe)**; Trino uses Joni by default: possessive/atomic groups behave alike, `\p{...}` classes same |
| 21 | `regexp_replace(s, re, rep)` | `regexp_replace` | same | replaces all on both **(probe)**; Trino `$1` back-references -> `$1` also on Spark |
| 22 | `regexp_extract(s, re[, group])` / `regexp_extract_all` | `regexp_extract(s, re, group)` / `regexp_extract_all` | edge | Trino default group is 0 (whole match), Databricks default is 1: always pass the group |
| 23 | `lpad`/`rpad`/`trim`/`ltrim`/`rtrim`/`upper`/`lower`/`reverse`/`replace` | same | same | `lpad('ab', 1, '0')` truncates to `a` on both **(probe)** |
| 24 | `trim(chars FROM s)` | `trim(chars FROM s)` | same | |
| 25 | `codepoint`, `chr`, `from_utf8`, `to_utf8` | `ascii`, `chr`, `decode(b, 'UTF-8')`, `encode(s, 'UTF-8')` | same | |
| 26 | `to_hex(sha256(to_utf8(s)))` / `md5` / `xxhash64` | `sha2(s, 256)` (already hex, lower case) / `md5` / `xxhash64` | edge | Trino hex is upper case **(probe)**; `xxhash64` returns `varbinary` on Trino and `bigint` on Databricks: not comparable, recompute |
| 27 | `'a' = 'A'`, `LIKE` | same | same | both case-sensitive, binary collation **(probe)**; JDBC catalogs with `citext`/CI collations push the compare down: check `information_schema.columns` on the source DB |
| 28 | `s LIKE p ESCAPE '\'` | same | same | |
| 29 | `format('%s-%05d', a, b)` | `format_string('%s-%05d', a, b)` | same | Java `Formatter` on both |
| 30 | `format_datetime(ts, 'yyyy-MM-dd HH:mm E')` (Joda) | `date_format(ts, 'yyyy-MM-dd HH:mm E')` | same | identical output for `yyyy MM dd HH mm ss E` **(probe)**; Joda `Y`/`x` week-year and `e` numeric day-of-week differ: map by hand |
| 31 | `date_format(ts, '%Y-%m-%d %H:%i %W')` (MySQL style) | `date_format(ts, 'yyyy-MM-dd HH:mm EEEE')` | edge | token set differs entirely (`%i` minutes, `%W` weekday name) **(probe)**; every `%` pattern is hand-mapped |
| 32 | `date_parse(s, '%Y-%m-%d')` / `parse_datetime(s, 'yyyy-MM-dd')` | `to_timestamp(s, 'yyyy-MM-dd')` | edge | MySQL vs Joda vs Spark token sets; Trino errors on a bad string, Databricks returns NULL unless ANSI |
| 33 | `from_iso8601_timestamp(s)` / `from_iso8601_date` / `to_iso8601(ts)` | `to_timestamp(s)` / `to_date(s)` / `date_format(ts, "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")` | edge | Trino result carries the offset (`with time zone`) |
| 34 | `date_diff('day', a, b)` | `timestampdiff(DAY, a, b)` | same | both count *elapsed* whole days (`23:00` -> next `01:00` = `0`) **(probe)**; Databricks `datediff(b, a)` counts calendar boundaries (`1`) **(probe)** and is the wrong translation |
| 35 | `date_diff('hour'\|'minute'\|'second'\|'millisecond', a, b)` | `timestampdiff(HOUR\|MINUTE\|SECOND, a, b)`; ms via `unix_millis(b) - unix_millis(a)` | same | |
| 36 | `date_diff('month'\|'year'\|'quarter'\|'week', a, b)` | `floor(months_between(b, a))` / `... / 12` / `... / 3`; week: `timestampdiff(DAY, a, b) DIV 7` | edge | Trino `date_diff('month', Jan 31, Feb 28)` = `1`, Databricks `timestampdiff(MONTH)` = `0`, `months_between` = `1.0` **(probe)**: month-end rule differs, choose `months_between` and record it |
| 37 | `date_add('day', n, d)` / `d + INTERVAL '1' MONTH` | `date_add(d, n)` / `add_months(d, 1)` / `d + INTERVAL 1 MONTH` | same | month-end clamps on both (`Jan 31 + 1 month` = `Feb 28`) **(probe)** |
| 38 | `date_trunc('week', d)` | `date_trunc('WEEK', d)` | edge | Monday start on both **(probe)**; Databricks returns `TIMESTAMP`, cast back to `DATE` |
| 39 | `date_trunc('hour'\|'day'\|'month'\|'quarter'\|'year', ts)` | same | same | result type `TIMESTAMP` on Databricks even for `DATE` input |
| 40 | `day_of_week(d)` / `dow(d)` | `extract(DOW_ISO FROM d)` or `weekday(d) + 1` | edge | ISO (Mon=1..Sun=7) on Trino; `dayofweek` is Sun=1 on Databricks **(probe)** |
| 41 | `day_of_year`, `doy`, `week`, `week_of_year`, `year_of_week`, `yow` | `dayofyear`, `weekofyear`, `extract(YEAROFWEEK FROM d)` | same | ISO weeks on both (`week(2026-01-01)` = `1`) **(probe)** |
| 42 | `last_day_of_month(d)` | `last_day(d)` | same | **(probe)** |
| 43 | `current_date` / `localtimestamp` / `current_timestamp` / `now()` | `current_date()` / `localtimestamp()` / `current_timestamp()` | edge | Trino `localtimestamp` is `timestamp(3)` zone-less; `now()` carries the session zone **(probe)**; Databricks has one zoned `TIMESTAMP` |
| 44 | `ts AT TIME ZONE 'Europe/Paris'` | `from_utc_timestamp(ts, 'Europe/Paris')` (to wall clock) / `to_utc_timestamp` (from wall clock) | edge | Trino keeps the instant and changes the display zone; Databricks shifts the wall-clock value |
| 45 | `with_timezone(ts, tz)` | `to_utc_timestamp(ts, tz)` | edge | interprets a zone-less value *in* `tz` |
| 46 | `from_unixtime(x)` | `timestamp_seconds(x)` | edge | Databricks `from_unixtime` returns a **string** **(probe)** |
| 47 | `to_unixtime(ts)` | `unix_timestamp(ts)` (`bigint`) or `unix_micros(ts) / 1e6` for the fractional part | edge | Trino returns `double` incl. fractions **(probe)** |
| 48 | `from_unixtime(x, 'UTC')` / `from_unixtime_nanos` | `from_utc_timestamp(timestamp_seconds(x), 'UTC')` / `timestamp_micros(x DIV 1000)` | edge | nanos lost |
| 49 | `CAST(ts AS DATE)` / `CAST('2026-01-01' AS TIMESTAMP)` / `date(ts)` | same | same | **(probe)** |
| 50 | `CAST(dec AS VARCHAR)` / `CAST(double AS VARCHAR)` | `CAST(dec AS STRING)` / `CAST(double AS STRING)` | edge | decimals equal (`1.50`); doubles differ (`1.5E0` vs `1.5`) **(probe)**: Tier 4 text diffs, compare as numbers |
| 51 | `approx_distinct(x[, e])` | `approx_count_distinct(x[, rsd])`; `COUNT(DISTINCT x)` only in a decision-approved exact variant | none | Approximate → exact changes observable counts and requires a recorded `D-<id>` row in `.migration/06_decisions.md` naming affected consumers; without one, keep the approximate contract and reconcile estimator drift as a Tier 3 finding, never a tolerance |
| 52 | `approx_percentile(x, p)` / `approx_percentile(x, w, p)` / `approx_percentile(x, ARRAY[...])` | `percentile_approx` by default; exact `percentile(x, p)` / `median` / `percentile_cont(p) WITHIN GROUP (ORDER BY x)` only in a decision-approved exact variant | none | Approximate → exact changes observable values and rankings and requires a recorded `D-<id>` row in `.migration/06_decisions.md` naming affected consumers; without one, keep `percentile_approx` and reconcile estimator drift as a Tier 3 finding, never a tolerance. `{1,2,3,4}` p50: Trino `3.0`, Databricks `percentile_approx` `2.0`, exact `2.5` **(probe)**; Trino returns `REAL` for `DECIMAL` input, Databricks keeps `DECIMAL` **(probe)** |
| 53 | `approx_set` / `merge` / `cardinality(hll)` / `empty_approx_set` | recompute with `approx_count_distinct` or `COUNT(DISTINCT)` | none | sketches are not portable |
| 54 | `count_if(c)` / `bool_or` / `bool_and` / `every` | `count_if` / `bool_or` / `bool_and` / `every` | same | **(probe)** |
| 55 | `arbitrary(x)` / `any_value(x)` | `any_value(x)` | same | non-deterministic on both: exclude from Tier 3, or replace with `min`/`max`/`max_by` |
| 56 | `max_by(x, k)` / `min_by` / `max_by(x, k, n)` | `max_by` / `min_by`; top-n form -> window `ROW_NUMBER` | same / none | **(probe)** for the 2-arg form |
| 57 | `array_agg(x ORDER BY k)` | `array_agg(x)` over a pre-sorted subquery, or `transform(array_sort(collect_list(struct(k, x))), s -> s.x)` | edge | Databricks `array_agg`/`collect_list` **drop NULLs** and Trino keeps them (`2` vs `1` elements) **(probe)**; aggregate `ORDER BY` inside the call is not honoured on Databricks |
| 58 | `array_agg(DISTINCT x ORDER BY x)` | `array_sort(collect_set(x))` | edge | equal for non-NULL input **(probe)**; NULL handling as row 57 |
| 59 | `listagg(x, ',') WITHIN GROUP (ORDER BY x)` | `listagg(x, ',') WITHIN GROUP (ORDER BY x)` | same | **(probe)**; Trino `ON OVERFLOW TRUNCATE` has no equivalent |
| 60 | `array_join(a, sep[, null_repl])` | `array_join(a, sep[, null_repl])` | same | **(probe)** |
| 61 | `a[1]` (array subscript) | `element_at(a, 1)` or `a[0]` | edge | Trino is 1-based and errors out of bounds; Databricks `[]` is 0-based **(probe)**; `element_at` is 1-based on both and NULL out of bounds **(probe)** |
| 62 | `element_at(m, k)` / `m[k]` on a map | `element_at(m, k)` / `m[k]` | edge | missing key: NULL on both for `element_at` **(probe)**; Trino `m[k]` errors, Databricks returns NULL (ANSI off) or errors (ANSI on) |
| 63 | `cardinality(a)` / `cardinality(m)` | `size(a)` / `size(m)` | edge | Databricks `size(NULL)` is `-1` unless `spark.sql.ansi.enabled` or `legacy.sizeOfNull=false`: wrap `coalesce(size(x), 0)` only after checking the source |
| 64 | `contains(a, x)` / `array_position(a, x)` / `array_distinct` / `array_sort` / `array_max` / `array_min` / `array_union` / `array_intersect` / `array_except` / `array_remove` / `flatten` / `reverse` / `slice(a, start, len)` / `sequence(lo, hi[, step])` / `repeat(x, n)` / `shuffle` / `zip` / `zip_with` | `array_contains` / `array_position` / `array_distinct` / `array_sort` / `array_max` / `array_min` / `array_union` / `array_intersect` / `array_except` / `array_remove` / `flatten` / `reverse` / `slice` / `sequence` / `array_repeat` / `shuffle` / `arrays_zip` / `zip_with` | edge | `slice` is 1-based on both; `sequence` over dates needs an `INTERVAL` step on Databricks; `array_sort` NULL placement: both last |
| 65 | `transform(a, x -> ...)` / `filter` / `reduce(a, s0, (s,x) -> ..., s -> s)` / `any_match` / `all_match` / `none_match` | `transform` / `filter` / `reduce` or `aggregate` / `exists` / `forall` / `NOT exists` | same | `reduce` **(probe)**; lambda syntax identical |
| 66 | `map(k_array, v_array)` / `map_from_entries` / `map_entries` / `map_keys` / `map_values` / `map_agg(k, v)` / `map_concat` / `map_filter` / `transform_values` / `transform_keys` / `multimap_agg` / `map_union` | `map_from_arrays` / `map_from_entries` / `map_entries` / `map_keys` / `map_values` / `map_from_entries(collect_list(struct(k, v)))` / `map_concat` / `map_filter` / `transform_values` / `transform_keys` / `map_from_entries(collect_list(...))` then group / `map_concat` | edge | Databricks `map_concat` errors on duplicate keys unless `spark.sql.mapKeyDedupPolicy=LAST_WIN`; Trino `map_concat` keeps the last |
| 67 | `CROSS JOIN UNNEST(arr) AS t(x)` | `LATERAL VIEW EXPLODE(arr) t AS x` or `, LATERAL explode(arr) AS t(x)` | same | rows with an empty or NULL array vanish on both (use `EXPLODE_OUTER` / `LEFT JOIN UNNEST ... ON TRUE`) |
| 68 | `CROSS JOIN UNNEST(map_entries(m)) AS t(k, v)` / `UNNEST(m)` | `LATERAL VIEW EXPLODE(m) t AS k, v` | same | **(probe: `explode(map)` yields `a=1,b=2`)**; see example 02 |
| 69 | `UNNEST(a) WITH ORDINALITY AS t(x, ord)` | `LATERAL VIEW POSEXPLODE(a) t AS pos, x` then `pos + 1` | edge | ordinality is 1-based, `pos` is 0-based **(probe)** |
| 70 | `UNNEST(a, b)` (parallel arrays) | `LATERAL VIEW POSEXPLODE(a) ... ` joined on position, or `arrays_zip(a, b)` then `EXPLODE` | edge | shorter array is NULL-padded on both |
| 71 | `ROW(1, 'a')` / `CAST(ROW(...) AS ROW(id INTEGER, name VARCHAR))` / `r.name` | `struct(1, 'a')` / `named_struct('id', 1, 'name', 'a')` / `r.name` | edge | anonymous field names `field0` vs `col1` **(probe)** |
| 72 | `json_extract_scalar(j, '$.a.b')` / `json_extract` / `json_format` / `json_parse` / `json_array_length` / `json_array_get` | `get_json_object(j, '$.a.b')` or `parse_json(j):a.b` / `variant_get` / `to_json` / `parse_json` / `json_array_length` / `get_json_object(j, '$[i]')` | same | **(probe: both `1`)**; Trino `json_extract_scalar` returns NULL for objects/arrays, `get_json_object` returns their text |
| 73 | `json_query` / `json_value` / `json_exists` (SQL/JSON) | `variant_get` / `:` path / `try_variant_get IS NOT NULL` | edge | SQL/JSON path language vs JSONPath: hand-map filters |
| 74 | `typeof(x)` | `typeof(x)` | same | names differ (`integer` vs `int`, `varchar(1)` vs `string`) **(probe)**: never compare across engines |
| 75 | `uuid()` | `uuid()` | same | type `uuid` vs `string` **(probe)**; `uuid_normalize` |
| 76 | `random()` / `rand()` / `random(n)` | `rand()` / `floor(rand() * n)` | none | exclude from recon |
| 77 | `bitwise_and`/`bitwise_or`/`bitwise_xor`/`bit_count`/`bitwise_left_shift` | `&` / `\|` / `^` / `bit_count` / `shiftleft` | same | |
| 78 | `ln`/`log2`/`log10`/`log(b, x)`/`power`/`sqrt`/`cbrt`/`exp`/`e()`/`pi()`/`abs`/`sign`/`ceil`/`floor`/`width_bucket` | `ln`/`log2`/`log10`/`log(b, x)`/`power`/`sqrt`/`cbrt`/`exp`/`e()`/`pi()`/`abs`/`sign`/`ceil`/`floor`/`width_bucket` | same | |
| 79 | `nan()` / `infinity()` / `is_nan(x)` / `is_finite` | `double('NaN')` / `double('Infinity')` / `isnan(x)` / `NOT isnan(x) AND abs(x) <> double('Infinity')` | same | |
| 80 | `normalize(s)` / `hamming_distance` / `levenshtein_distance` / `soundex` | GAP / GAP / `levenshtein` / `soundex` | none | NFC normalisation needs a UDF |
| 81 | `url_extract_host` / `url_extract_path` / `url_extract_parameter` / `url_encode` / `url_decode` | `parse_url(u, 'HOST')` / `parse_url(u, 'PATH')` / `parse_url(u, 'QUERY', k)` / `url_encode` / `url_decode` | same | |
| 82 | `bar(x, w)` / `color` / `render` | none (terminal art) | none | drop |
| 83 | `ROW_NUMBER`/`RANK`/`DENSE_RANK`/`NTILE`/`LAG`/`LEAD`/`FIRST_VALUE`/`LAST_VALUE`/`NTH_VALUE`/`PERCENT_RANK`/`CUME_DIST` | same | same | frame default `RANGE UNBOUNDED PRECEDING` on both; Trino `LAG(x, n, default)` requires `default` type match |
| 84 | `ORDER BY x` default NULL placement | `ORDER BY x NULLS LAST` / `ORDER BY x DESC NULLS LAST` | edge | Trino puts NULLs **last** for both `ASC` and `DESC` **(probe 483)**; Databricks `ASC` puts NULLs first: always spell `NULLS FIRST/LAST` |
| 85 | `GROUPING SETS` / `ROLLUP` / `CUBE` / `GROUPING(...)` | same | same | **(probe)** |
| 86 | `FILTER (WHERE c)` on any aggregate | `agg(x) FILTER (WHERE c)` | same | supported on Databricks since DBR 12 |
| 87 | `TABLESAMPLE BERNOULLI (10)` / `SYSTEM (10)` | `TABLESAMPLE (10 PERCENT)` | none | never reconcile sampled output row-for-row |
| 88 | `SELECT ... FETCH FIRST n ROWS WITH TIES` / `OFFSET n` / `LIMIT n` | `QUALIFY RANK() OVER (...) <= n` / `OFFSET n` / `LIMIT n` | edge | `WITH TIES` has no keyword on Databricks |
| 89 | `VALUES (1, 'a'), (2, 'b')` as a table / `SELECT * FROM (VALUES ...) t(a, b)` | `VALUES (1, 'a'), (2, 'b') AS t(a, b)` | same | Databricks `VALUES` needs a table alias in a `FROM` |
| 90 | `EXCEPT [DISTINCT]` / `INTERSECT` / `UNION` / `EXCEPT ALL` | `EXCEPT` / `INTERSECT` / `UNION` / `EXCEPT ALL` | same | |
| 91 | `LATERAL (subquery)` | `LATERAL (subquery)` | same | correlated lateral subqueries supported on both |
| 92 | `MATCH_RECOGNIZE` | none: window functions / gaps-and-islands by hand | none | |
| 93 | `WITH FUNCTION f(...) RETURNS ... RETURN ...` (inline SQL UDF) / `CREATE FUNCTION` | `CREATE [TEMPORARY] FUNCTION f(...) RETURNS ... RETURN ...` `[docs:sql-ref-syntax-ddl-create-sql-function]` | edge | Trino inline functions live in the query; Databricks needs a catalog/schema or `TEMPORARY` |
| 94 | `EXECUTE stmt USING 1, 'a'` / `PREPARE` / `?` parameters | `EXECUTE IMMEDIATE ... USING` `[dbsql:sql-scripting.md#EXECUTE IMMEDIATE]`; job `parameters` read as `:name` `[jobs:task-types.md#Parameters]` | edge | positional vs named |
| 95 | `SHOW STATS FOR (SELECT ...)` / `EXPLAIN ANALYZE` | `ANALYZE TABLE ... COMPUTE STATISTICS` (writes stats: target only) / `EXPLAIN` | none | census only |
| 96 | `$path`, `$file_modified_time`, `$file_size`, `$partition` hidden columns | `_metadata.file_path`, `_metadata.file_modification_time`, `_metadata.file_size` on file reads; none on Delta tables | edge | lineage to files (section 2) |

## 6. Procedural / source-construct map

Trino has no stored procedures, cursors or triggers; the "procedural" layer is DDL semantics, session properties
and the orchestrator around the SQL. Every row lands through the cited official skill.

| # | Trino construct | Databricks landing | Notes |
|---|---|---|---|
| P1 | `CREATE TABLE t (...) WITH (format = 'PARQUET', partitioned_by = ARRAY['dt'], bucketed_by = ARRAY['id'], bucket_count = 32, sorted_by = ARRAY['ts'])` | `CREATE TABLE t (...) USING DELTA CLUSTER BY (id, ts)`; `PARTITIONED BY (dt)` only above ~1 TB per table `[dbsql:best-practices.md#Liquid Clustering vs Traditional Partitioning]` | Hive partition columns are *trailing* columns in Trino DDL: keep column order for Tier 3 |
| P2 | `CREATE TABLE t WITH (external_location = 's3://...')` (Hive external) | UC external table over the same location `[uc:2-external-locations.md#Create an External Location]`, `[uc:3-securables-ddl.md#Tables — Managed vs External]`; `CONVERT TO DELTA` is a *write* to that location and needs the target allowlist | Parquet stays readable by Trino during parallel run only if you do **not** convert in place |
| P3 | `CREATE TABLE t AS SELECT ...` (CTAS, nightly rebuild via `DROP TABLE IF EXISTS` first) | `CREATE OR REPLACE TABLE t AS SELECT ...` (atomic, keeps history) `[dbsql:best-practices.md#Fact Table Patterns]` | never translate `DROP` + `CREATE` literally: readers see a gap on Trino and none on Delta (Tier 1 during parallel run is *stricter* on the target) |
| P4 | `INSERT INTO t SELECT ...` with `SET SESSION <cat>.insert_existing_partitions_behavior = 'OVERWRITE'` | `INSERT OVERWRITE t PARTITION (...) SELECT ...` or `INSERT INTO ... REPLACE WHERE dt = ...` | the session property is the only signal that the load is idempotent: capture it in the unit mapping |
| P5 | `INSERT INTO t SELECT ...` (append) | `INSERT INTO t SELECT ...` | re-run doubles rows on both: add a run-ledger guard on the target only if the source wrapper had one |
| P6 | `DELETE FROM t WHERE dt = DATE '...'` (Hive: whole partitions only) / `DELETE` / `UPDATE` / `MERGE` (Iceberg, Delta connectors) | Delta `DELETE` / `UPDATE` / `MERGE INTO` `[dbsql:best-practices.md#Slowly Changing Dimensions (SCD) Patterns]` | Trino `MERGE` requires a unique match per target row and errors otherwise; Delta does the same (`MERGE` multiple-match error) |
| P7 | `CREATE [OR REPLACE] VIEW v [SECURITY DEFINER \| INVOKER] AS ...` | `CREATE OR REPLACE VIEW v AS ...` `[uc:3-securables-ddl.md]`; UC views run with the owner's rights (definer) | `SECURITY INVOKER` views become a governance finding: the consumer's grants on base tables must be re-derived (section 9) |
| P8 | `CREATE MATERIALIZED VIEW mv ... GRACE PERIOD ...` + `REFRESH MATERIALIZED VIEW mv` | Lakeflow materialized view with `SCHEDULE` `[pipelines:materialized-view-sql.md]` | stale reads within the grace period have no equivalent: MV reads are always current on Databricks |
| P9 | `CREATE SCHEMA <cat>.s WITH (location = ...)` | `CREATE SCHEMA c.s [MANAGED LOCATION ...]` `[uc:3-securables-ddl.md#Schemas]` | one UC catalog per Trino *catalog* when the source names must survive three-part (`lake.core.orders` -> `<target>.core.orders` folds the catalog into the migration catalog: record the naming decision) |
| P10 | `CREATE TABLE ... WITH (transactional = true)` (Hive ACID) | Delta table | ACID semantics are default on Delta |
| P11 | `CREATE FUNCTION s.f(x INTEGER) RETURNS ... RETURN ...` (SQL UDF) / language plugins (Python UDF via `LANGUAGE PYTHON`) | UC SQL UDF / UC Python UDF `[docs:sql-ref-syntax-ddl-create-sql-function]` | Trino Python UDFs run in a WASM sandbox with no imports: the body ports directly |
| P12 | `SET SESSION time_zone = 'America/New_York'` | `SET TIME ZONE 'America/New_York'` at the top of the SQL task / `spark.sql.session.timeZone` | changes every `TIMESTAMP WITH TIME ZONE` render and `date_trunc('day')` on zoned values: Tier 1 day-boundary drift if dropped |
| P13 | `SET SESSION query_max_run_time`, `join_distribution_type`, `spill_enabled`, `<cat>.parquet_*` | drop with a mapping note | performance only; a `SET SESSION` that changes *results* is P4/P12 |
| P14 | `USE <cat>.<schema>` | `USE CATALOG c; USE SCHEMA s` | the unit's default schema; resolve every unqualified name before conversion |
| P15 | `CALL system.sync_partition_metadata(...)` / `CALL system.register_table` / `CALL <cat>.system.flush_metadata_cache()` | none: Delta/UC discover files and partitions themselves; for external Parquet, `MSCK REPAIR TABLE` or `REFRESH TABLE` | source-side *writes*: never run during census |
| P16 | `ALTER TABLE ... EXECUTE optimize(file_size_threshold => ...)` / `expire_snapshots` / `remove_orphan_files` (Iceberg) | `OPTIMIZE` / `VACUUM` `[dbsql:best-practices.md#OPTIMIZE, VACUUM, and ANALYZE]` | maintenance jobs are units of the *target* schedule, not of the migration |
| P17 | `ANALYZE t` / `SHOW STATS` | `ANALYZE TABLE t COMPUTE STATISTICS` on the target | never on the source |
| P18 | `EXPLAIN` / `EXPLAIN ANALYZE` / `SHOW CREATE` | `EXPLAIN` / `DESCRIBE TABLE EXTENDED` | census tooling |
| P19 | dbt model (`{{ config(materialized='table') }}`, `profiles.yml type: trino`) | dbt-databricks with the same models; or `sql_task` per model when dbt is being retired `[jobs:task-types.md#dbt Task]` | `{{ ref() }}` graph *is* the lineage; Trino-specific macros (`trino__` dispatch) are the unit's dialect surface |
| P20 | Airflow `TrinoOperator` / `SQLExecuteQueryOperator(conn_id='trino')` DAG | one Lakeflow Job: `sql_task` (`file:`) per statement, `depends_on` from the DAG edges, cron -> `schedule.quartz_cron_expression` + `timezone_id` `[jobs:task-types.md#SQL Task]`, `[jobs:triggers-schedules.md]` | Airflow `retries`/`retry_delay` -> task `max_retries`/`min_retry_interval_millis`; `catchup=True` back-fills need a parameterised `dt` |
| P21 | Trino Gateway / Superset / Metabase saved queries | DBSQL queries, dashboard datasets | report units, Tier 4 |
| P22 | `PREPARE q FROM SELECT ... WHERE dt = ?; EXECUTE q USING DATE '...'` | `EXECUTE IMMEDIATE ... USING` `[dbsql:sql-scripting.md#EXECUTE IMMEDIATE]`; or `:dt` job parameter | positional -> named |
| P23 | JDBC catalog write (`INSERT INTO ops_pg.public.t ...` through the connector) | forbidden during migration: the JDBC database is a legacy source; a Lakebase/OLTP unit only under `!dbx_migrate_oltp` | write-scope guard blocks it; inventory it as a `reverse-write` finding |
| P24 | Cross-catalog read (`lake.x JOIN ops_pg.y`) | parallel run: foreign catalog (Lakehouse Federation, read-only by construction); steady state: ingest the JDBC side (`[lakeflow-connect:2-database-connectors.md]`) and join on Delta | the plan must say which; Tier 1 counts differ when the JDBC side moves between the two reads |

## 7. Known traps with reconciliation signature

| Trap | Trino | Databricks | Recon signature | Fix |
|---|---|---|---|---|
| Elapsed vs calendar days | `date_diff('day', a, b)` counts whole 24 h periods | `datediff(b, a)` counts date boundaries | Tier 3 off-by-one on `*_days` columns for roughly half the rows; Tier 2 `SUM(active_days)` drift | `timestampdiff(DAY, a, b)` (row 34); example 04 |
| Decimal division scale | `decimal(10,2) / 3` -> `decimal(21,13)`; `AVG(decimal)` keeps input scale | `decimal(14,6)`; `AVG` adds 4 to the scale | Tier 3 last-digit diffs on `avg_*`/`*_per_*` columns; Tier 2 sums equal | cast to the Trino result scale (rows 2-3); `decimal_round` at that scale; example 04 |
| Integer division | `7 / 2` = `3` | `3.5` | Tier 3 fractions on ratio columns; Tier 2 sum drift | `DIV` (row 1) |
| `CAST(decimal AS INTEGER)` | rounds | truncates | Tier 3 off-by-one on ~half the rows | `CAST(round(x) AS INT)` (row 9) |
| Approximate aggregates | `approx_percentile` over `DECIMAL` returns `REAL`; T-Digest estimator | `percentile_approx` keeps `DECIMAL`; different estimator | Tier 3 on `median_*`; Tier 4 **ranking flips** when groups are within the error band (`ORDER BY median DESC` puts a different group first) | Approximate → exact changes observable values/rankings and requires a recorded `D-<id>` row in `.migration/06_decisions.md` naming affected consumers; without one, keep `percentile_approx`, reconcile estimator drift as a Tier 3 finding, and never add a tolerance; example 05 |
| `approx_distinct` | HLL, `2.3 %` default error | `approx_count_distinct` HLL++, `5 %` default | Tier 2 count drift within error bands | Approximate → exact changes observable counts and requires a recorded `D-<id>` row in `.migration/06_decisions.md` naming affected consumers; without one, keep `approx_count_distinct`, reconcile estimator drift as a Tier 3 finding, and never add a tolerance |
| NULLs in `array_agg` | kept | dropped by `array_agg`/`collect_list` | Tier 3 `size()` shortfall; `array_join` text diffs | `collect_list(coalesce(x, '<NULL>'))` when NULLs carry meaning, or filter NULLs on the source-side recon query (`filter(arr, x -> x IS NOT NULL)`) and record it |
| Aggregate `ORDER BY` | `array_agg(x ORDER BY k)` honoured | ignored | Tier 3 array text diffs, Tier 4 report order | `array_sort(collect_set)` for distinct, struct-sort for ordered (rows 57-58); example 03 |
| `ORDER BY` NULL placement | NULLs last in both directions | NULLs first for `ASC` | Tier 4 first/last rows differ; `LIMIT n` picks different rows (Tier 1 on top-n marts) | spell `NULLS LAST` |
| Map key order | unordered | insertion order | Tier 4 text diffs when a map is cast to string | compare `map_entries` sorted by key; never `CAST(map AS STRING)` |
| `CHAR(n)` padding (Hive `char`, PostgreSQL `bpchar`) | padded storage and compare; a `CHAR(4)` column holding `'NORT'` was truncated *at the source* | `string`, unpadded, length differs | Tier 3 on `CHAR` keys and `length()` derivations; Tier 1 join shortfall against unpadded lookup tables | `rstrip_spaces`; `_RTRIM` collation when consumers compare padded; source truncation is a **finding**, not a conversion bug |
| `VARCHAR(n)` cast truncation | `CAST(s AS VARCHAR(3))` truncates | no truncation | Tier 3 longer strings on the target | explicit `substr(s, 1, n)` |
| Bounded `VARCHAR(n)` on insert | Trino errors when a value exceeds `n` on Hive | Databricks `VARCHAR(n)` errors too, `STRING` accepts | none (target is looser) | keep `STRING` unless the length was a business rule |
| `substr(s, 0, n)` | `''` | first `n-1` characters | Tier 3 empty-vs-prefix | row 16 |
| `split_part` out of range | NULL | `''` | Tier 3 null-vs-empty; `null_missing_equiv` does **not** mask it (`''` is a value) | `nullif(..., '')` |
| `greatest`/`least` with NULL | NULL | NULL skipped | Tier 3 value-vs-NULL | row 13 |
| Integer overflow | error (`TRY` -> NULL) | wraps silently unless ANSI mode | Tier 2 sum drift with negative outliers; source job failed, target succeeded | `try_add`/`try_multiply`; enable ANSI on the warehouse for the parallel run |
| Zone-less vs zoned timestamps | `timestamp(p)` has no zone; `now()`/`from_unixtime` are zoned; `AT TIME ZONE` changes display only | one zoned `TIMESTAMP` rendered in the session zone; `TIMESTAMP_NTZ` for zone-less | Tier 3 whole-hour offsets; Tier 1 day-boundary drift on `date_trunc('day')`/`CAST(ts AS DATE)` | declare `timestamp_ntz` for zone-less columns, pin the session zone (P12), and use `datetime_utc_truncate_ms` only at effective p <= 3 |
| Timestamp precision | up to `timestamp(12)`, default 3 | microseconds | Tier 3 sub-ms diffs | `datetime_utc_truncate_ms` only for effective p <= 3; p 4-6 uses `identity`; p > 6 is a declared harness gap, so extract at microsecond precision and compare with identity |
| `from_unixtime` type | zoned timestamp | **string** | Tier 3 type mismatch or silent string compare | `timestamp_seconds` (row 46) |
| `day_of_week` | ISO Mon=1 | Sun=1 | Tier 1 group counts on weekday reports | row 40 |
| `date_format` `%` tokens | MySQL-style | Java pattern | Tier 4 text diffs or wrong dates parsing | hand-map every token (rows 31-32) |
| `regexp_extract` default group | 0 (whole match) | 1 | Tier 3 on extracted columns | pass the group (row 22) |
| Hex case | upper | lower | Tier 3 on hash/hex keys | `upper(hex(...))` or `lower()` both sides; `uuid_normalize` for UUIDs |
| `xxhash64`/`murmur3` outputs | `varbinary` | `bigint` | not comparable | recompute on the target; exclude from Tier 3 |
| `CROSS JOIN UNNEST` drops empty arrays | rows with `ARRAY[]`/NULL vanish | `EXPLODE` same; `EXPLODE_OUTER` keeps them | Tier 1 count equal only if both drop | keep `EXPLODE`, not `_OUTER`, for like-for-like; example 02 |
| Hidden `$path` columns | available | none on Delta | conversion error, or a NULL column | lineage finding (section 2) |
| `DROP TABLE` + CTAS rebuild | readers see no table for the CTAS duration | `CREATE OR REPLACE` is atomic | Tier 1 during parallel run: a *source* reader failed, the target did not | P3; the failure is a source-side finding |
| `insert_existing_partitions_behavior` | `APPEND` default: re-runs double partitions | `INSERT OVERWRITE` replaces | Tier 1 excess on the **source** after a re-run | P4; record the property, do not "fix" the target to match a bug |
| JDBC pushdown semantics | `ops_pg.t WHERE lower(name) = ...` runs *in PostgreSQL* (its collation, its `char` rules) | Databricks evaluates or pushes down per foreign catalog rules | Tier 2 distinct drift on collated columns | check the source DB collation; `collation_casefold` only where the DB is CI |
| Views with `SECURITY INVOKER` | consumer's grants | owner's grants | no recon signature: governance finding | section 9 |
| Grace-period MVs | stale within grace | always fresh | Tier 1/2 differences equal to one refresh delta during parallel run | compare at refresh boundaries |

## 8. Canonicalization rules

`canonicalization.json` carries six harness rules plus `type_map.trino.databricks`; all load through
`recon.config.load_canon_rules` and `recon.typemap.load_type_map`. Order of application is the harness's
(`ORDER_PRESERVING_RULES` in `recon/tiers.py`).

| Rule | Applies to | Params | Why |
|---|---|---|---|
| `decimal_round` | recomputed `DECIMAL` columns only (division, `AVG`, ratios); *not* stored decimals | `mode: half_up`, `places` = the Trino result scale the consumer saw (default 2 when the mapping declares none) | section 7 "Decimal division scale"; both engines round half away from zero **(probe)**, so `half_up` is the mode that changes nothing on equal inputs |
| `datetime_utc_truncate_ms` | `TIMESTAMP(p)` / `TIMESTAMP(p) WITH TIME ZONE` only when effective source precision <= 3 (Hive/Parquet millis, PostgreSQL timestamp(3)) | `assume_source_tz_for_NTZ: <coordinator time_zone>` | p 4-6 is exact in Databricks and uses `identity`; p > 6 is a declared harness gap with no microsecond rule: extract at microsecond precision and compare with identity |
| `rstrip_spaces` | `CHAR(n)` -> `STRING` | — | padding |
| `uuid_normalize` | `UUID` -> `STRING` | — | case/hyphen spelling |
| `null_missing_equiv` | `*` | — | JDBC catalogs surface absent columns as NULL |
| `identity` | `*` | — | everything else compares exactly |

### Reconciliation routing

`trino` has no live source adapter yet. Reconcile Hive/Parquet tables through Lakehouse Federation or a landed
snapshot with `--family databricks --mode snapshot` and a manifest; reconcile JDBC-connector catalogs directly
against their engine, such as `--family postgres`. The doctor still audits the Trino type map with
`--source-family trino`, while `dbx-recon --family trino` fails fast by design until a Trino adapter is rehearsed.

Not rules (recon-query shape instead): `ORDER BY ... NULLS LAST` on both sides; `map_entries` sorted by key for
maps; `array_sort` only where the source aggregate was `DISTINCT` (otherwise order is part of the contract);
approximate aggregates replaced by their exact counterpart **on both sides** of the recon query only after a recorded
`D-<id>` decision names the affected consumers; without that decision, retain the approximate contract on both sides.
Harness gaps filed, not faked: `time_of_day_normalize` (TIME columns), `approx_within_error_band` (would let an
approximate aggregate pass within its documented error: rejected for merge authority, a decision row instead).

## 9. Governance discovery (read-only)

Trino's authorisation lives in the coordinator's access-control plugin, not in the catalogs, so SQL sees only part
of it. Ask the platform team for the files; query what SQL exposes.

| Source | How to read | Maps to | Notes |
|---|---|---|---|
| `access-control.properties` (`access-control.name = allow-all \| read-only \| file \| ranger \| opa`) | file from the coordinator | decides whether anything below exists | `allow-all` means every grant is implicit: UC grants must be *designed*, not migrated |
| File-based `rules.json`: `catalogs`, `schemas`, `tables` (with `privileges`, `filter`, `columns[].mask`), `session_properties`, `queries`, `impersonation` | file | `GRANT ... ON CATALOG/SCHEMA/TABLE` `[uc:1-access-control.md#GRANT / REVOKE (SQL)]`; `filter` -> row filter, `columns[].mask` -> column mask `[uc:4-fine-grained-access.md#Row Filters]`, `[uc:4-fine-grained-access.md#Column Masks]` | rules match by regex on user/group/catalog names: expand them against the group list before mapping |
| Ranger / OPA policies | export via their APIs (platform team) | same UC objects | policy expressions in Rego/Ranger conditions are hand-converted to SQL UDF filters |
| `SHOW GRANTS ON <table>` / `information_schema.table_privileges` / `role_authorization_descriptors` / `applicable_roles` / `enabled_roles` | SQL, per catalog | UC grants | populated only under system access control that supports SQL grants (Hive connector `sql-standard`, Iceberg with a REST catalog) |
| `SHOW ROLES [IN <cat>]` / `SHOW ROLE GRANTS` / `SHOW CURRENT ROLES` | SQL | UC groups (account-level) | Trino roles are per-catalog; UC groups are account-wide: flatten `catalog.role` into a group naming decision |
| `SECURITY INVOKER` views (`SHOW CREATE VIEW`) | SQL | dynamic views `[uc:4-fine-grained-access.md#Dynamic Views]` when the view narrows rows per user; plain views otherwise | finding per view |
| `system.runtime.queries."user"` + `source` | SQL | service principals per job/BI tool | the `source` column (`trino-cli`, `dbt`, `airflow`, JDBC driver names) enumerates consumers |
| `http-server.authentication.type` (`PASSWORD`, `OAUTH2`, `KERBEROS`, `JWT`), `user-mapping`, `impersonation` | coordinator config | workspace SSO / SCIM groups; job `run_as` service principal | `impersonation` rules show which services run as users: those become `run_as` decisions |
| Catalog properties with credentials (`connection-password`, `hive.s3.aws-secret-key`, `hive.metastore.thrift.impersonation.enabled`) | files | UC storage credentials `[uc:2-external-locations.md#Create a Storage Credential]`, connection secrets for foreign catalogs | values are secrets: record *names*, never copy the values into the repo |

Minimum privileges for the census identity: `SELECT` on every `information_schema` and on `system.metadata.*`,
`system.runtime.queries` visibility, and read access to the config directory; nothing that can `CALL`, `SET
SESSION` for others, or write. Findings that need a decision at STOP A: `allow-all` estates (no source grants to
migrate), `INVOKER` views, impersonation rules, JDBC catalogs whose credentials are shared by all Trino users
(everyone sees the whole source DB through Trino; UC will not).

## 10. Lakebridge coverage delta

Lakebridge's documented transpiler matrix (`skills-extra/lakebridge/SKILL.md` "Dialect and transpiler matrix")
lists no `trino` or `presto` `--source-dialect` as of the 2026-09 read. Expectation (SEEDED, not confirmed on an
engagement): static conversion for Trino is **hand conversion under this skill**; an operator may *try* the
generic ANSI path once per engagement and record `--help` output and the result in the coverage table, but must not
claim transpiler support because a flag appears in a newer release. The delta appended to
`skills-extra/lakebridge/SKILL.md` lists what a generic ANSI pass would convert, mangle silently and reject for
Trino SQL; each row carries its recon signature from section 7.

## 11. Risk heuristics

Score each unit before wave assignment; anything >= 3 goes to a human-reviewed batch.

| Signal (per unit) | Weight | Why |
|---|---|---|
| Reads from two or more catalogs with different connectors | +2 | landing decision (federate vs ingest) changes counts during parallel run |
| Any `approx_*` aggregate feeding an `ORDER BY`, `LIMIT`, `RANK` or a threshold | +2 | ranking flips (section 7) |
| `date_diff`, `/` on integers or decimals, `AVG(decimal)`, `CAST(decimal AS INTEGER)` | +1 each | silent arithmetic semantics |
| `map`/`array` columns with `UNNEST`, `array_agg`, `map_agg`, lambda functions | +1 | NULL and order semantics |
| `SET SESSION` that changes results (`time_zone`, `insert_existing_partitions_behavior`) | +2 | dropped silently by a naive conversion |
| `date_format`/`date_parse` with `%` tokens, `regexp_extract` without a group | +1 | token/group defaults |
| `CHAR(n)` columns from a JDBC catalog | +1 | padding and source truncation findings |
| `SECURITY INVOKER` view, `impersonation` in the query log | +1 | governance decision needed |
| Hidden `$path`/`$partition` columns, `CALL system.*`, `TABLESAMPLE`, `MATCH_RECOGNIZE` | +3 | no like-for-like; hand design |
| Unit is the target of `DROP` + CTAS with downstream readers in the query log | +1 | parallel-run gap semantics |
| Unit has no query-log evidence (repository only) | +1 | INFERRED lineage; may be dead code |
| dbt/Airflow wrapper with `catchup`, `retries` > 0, or templated `dt` | +1 | schedule semantics to re-express |
| Report unit (`SELECT` only) with no downstream writer | -1 | Tier 4 only, low blast radius |

## 12. Worked examples

`examples/<dir>/source.sql` is Trino SQL, `converted.sql` is Databricks SQL, `NOTE.md` names constructs,
citations and the recon tier that catches a wrong conversion. `${catalog}`/`${schema}` are mapping parameters.
`examples/00_probe/` holds the two probe files behind every **(probe)** row and their captured outputs.

| Dir | Constructs | Recon tier that catches a wrong conversion |
|---|---|---|
| `examples/01_cross_catalog_ctas/` | Hive + JDBC catalog join, `DROP` + CTAS -> `CREATE OR REPLACE TABLE`, `CHAR(n)` region key, decision-approved `approx_distinct` -> `COUNT(DISTINCT)` (otherwise `approx_count_distinct`), `format_datetime` | Tier 1 join shortfall on padded keys, Tier 2 distinct drift |
| `examples/02_map_unnest_report/` | `CROSS JOIN UNNEST(map_entries(m))` -> `LATERAL VIEW EXPLODE`, `element_at`, `cardinality` | Tier 1 exploded row count, Tier 4 report order |
| `examples/03_ordered_distinct_array_agg/` | `array_agg(DISTINCT x ORDER BY x)` -> `array_sort(collect_set)`, `arbitrary` -> `any_value`, `listagg`, NULL handling | Tier 3 array text diffs, `size()` shortfall |
| `examples/04_elapsed_days_decimal_scale/` | `date_diff('day')` -> `timestampdiff(DAY)`, decimal division and `AVG` scale casts, integer `DIV` | Tier 3 off-by-one on `active_days`, last-digit diffs on `avg_order_value`, Tier 2 sum drift |
| `examples/05_approx_percentile_ranking/` | decision-approved `approx_percentile` -> exact `percentile`/`median` (otherwise `percentile_approx`), `ORDER BY ... NULLS LAST`, `REAL` vs `DECIMAL` result type | Tier 4 ranking flip, Tier 3 `median_*` |

Not verified live (docs only): Iceberg/Delta connector `MERGE` multiple-match behaviour, `GRACE PERIOD` MVs, Python
UDF port, Ranger/OPA policy shapes, `json_query`/`json_value` path mapping, `MATCH_RECOGNIZE` rewrites, hidden
`$path` columns on Iceberg. Each `NOTE.md` carries its own list.
