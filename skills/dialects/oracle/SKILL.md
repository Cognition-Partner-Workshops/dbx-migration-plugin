---
name: oracle
description: Source-dialect skill for Oracle SQL and PL/SQL estates (packages, procedures, functions, triggers, sequences, materialized views, DBMS_SCHEDULER jobs, SQL*Plus scripts). Load it when `policy.source.family` is `oracle`: for the analyze census, slice conversion (delta track DBSQL / lakebase track PL/pgSQL), or recon input authoring.
---

# Oracle dialect

Source-side only. Everything Databricks-side routes through `skills/target-routing/SKILL.md`;
never restate it. Track **A** is Delta/DBSQL, track **L** is Lakebase Postgres.

- Lakebridge: `--source-dialect oracle` — handles static SQL/DDL/views; anything with `BEGIN`,
  `DECLARE`, `CREATE OR REPLACE (PACKAGE|PROCEDURE|FUNCTION|TRIGGER)` or `DBMS_` is hand work here.
- Canonicalization: [canonicalization.json](../../oracle-plsql/canonicalization.json) (loaded by
  `recon.config.load_canon_rules`; moves beside this file when the harness component lands).
- Full construct/function/type maps: `skills/oracle-plsql/SKILL.md` until the 0.6.2 files are
  removed.

## Dependency scan

Walk every routine in scope (package members, standalone procedures and functions, triggers,
scheduler job actions); resolve synonyms and case; output one map keyed by routine to
`.migration/recon/<slice>/inputs/dependencies.json`:

```json
{"<schema>.<routine>": {"writes": ["<schema>.<table>"], "reads": ["<schema>.<table>"]}}
```

`writes` = targets of `INSERT`/`UPDATE`/`DELETE`/`MERGE INTO`/`TRUNCATE`; `reads` = tables and
views in `SELECT`/`FROM`/`JOIN`/`MERGE USING`/cursor bodies. Both lists are the transitive
closure over the call graph: a routine's row carries what its callees write and read. Every
callee the scan names must be covered by its own row (trigger-fired routines and the job action
included), or `slices.py --check` fails the slice proposal.

## Known traps with recon signature

| # | Trap | Oracle vs target | Recon signature | Fix / canonicalization |
|---|---|---|---|---|
| 1 | `''` is NULL | `''` stored as NULL, `x = ''` never true; targets store `''` | Tier 1 null-count drift; Tier 3 `NULL` vs `''` | `nullif(trim(col),'')` on load; `x = ''` → `IS NULL`; `empty_string_is_null` on every string column |
| 2 | `NUMBER` without scale | floating scale, 38 digits; `DECIMAL(38,10)` fixes scale; `DOUBLE` drifts | Tier 2 `SUM` drift past the 10th decimal | scale from sampled `MAX(SCALE)`; never `DOUBLE` for money; `decimal_round` half_even 10 |
| 3 | `DATE` carries time | seconds precision; lazy `DATE` mapping drops it | Tier 3 on non-midnight rows; `BETWEEN` off by a day at midnight | `TIMESTAMP_NTZ`; `>= d1 AND < d2 + INTERVAL 1 DAY`; `datetime_utc_truncate_ms` |
| 4 | `TIMESTAMP WITH [LOCAL] TIME ZONE` | offset stored / normalised; target is one instant in session TZ | Tier 3 constant offset on every row | compare UTC instants (`SYS_EXTRACT_UTC` / `to_utc_timestamp`) with `identity`; session TZ = UTC both sides |
| 5 | `CHAR(n)` padding | pads and pad-compares; `STRING` does not | `COUNT(DISTINCT)`/join drift; joins yield 0 rows | `rtrim` on load or `COLLATE UTF8_BINARY_RTRIM`; `rstrip_spaces` then `empty_string_is_null` |
| 6 | `CONNECT BY` order, cycles, `LEVEL` | depth-first `ORDER SIBLINGS BY`; `NOCYCLE` flags the row; recursive CTE has none | Tier 1 doubled rows or recursion-limit error; Tier 3 wrong path/`is_cycle` | carry `path ARRAY` + `NOT array_contains`; compute `is_cycle` after the walk; order by path |
| 7 | `ROWNUM` pagination | assigned before `ORDER BY`; ties arbitrary | equal page counts, Tier 3 rows differ | total order key (append PK); `row_number()` for `rn` |
| 8 | `MERGE` duplicates, `DELETE WHERE`, `NEXTVAL` | ORA-30926 on multi-match; `DELETE WHERE` sees post-update values; Delta has no unique constraint and evaluates on pre-update rows | Tier 1 off by the deleted/double-inserted set; Tier 4 target succeeded where Oracle failed | `assert_true(count(*)=0)` over the keys Oracle would reject before the `MERGE`; `WHEN MATCHED AND` delete clause first; exclude surrogate keys from Tier 3 |
| 9 | `PIVOT` names and nulls | generated names upper-case; `PIVOT XML` dynamic | Tier 4 column names; Tier 2 `SUM` with `NVL` on one side | alias every column; `null_missing_equiv` |
| 10 | Sequences and trigger side effects | every writer gets PK/audit/derived values for free; Delta has no triggers | Tier 1 audit 0 rows; Tier 3 drift when one writer forgot | A: shared write procedure called by all writers; L: port the trigger; compare audit rows on content |
| 11 | Package session state | `g_*` persists across calls; none on either target | Tier 3 on state-derived columns; Tier 4 `run_id` | thread through params or a run-control table keyed by job run id |
| 12 | `SYS_REFCURSOR` contracts | names/order/types fixed at `OPEN` | Tier 4 consumer diff | pin the projection, never `SELECT *`; alias to Oracle upper-case names |
| 13 | Swallowed exceptions | `WHEN OTHERS THEN NULL` / status-out succeed with partial writes | Tier 4 no failure signal while source logged errors | log then `RESIGNAL`, or `OUT status` the caller fails on |
| 14 | Autonomous transactions | logger commits independently of the business txn | Tier 1 audit count higher on source | accept the delta in the tolerance record, or log outside the transaction (job task) |
| 15 | MV fast-refresh eligibility | needs MV logs + `COUNT` columns; `SUM` over all-NULL group = 0 on target vs NULL | Tier 2 `SUM` 0 vs NULL; Tier 1 group count | keep `COUNT` columns; `null_missing_equiv` on aggregates |
| 16 | Scheduler TZ, retries, auto-disable | `repeat_interval` in `start_date` TZ; `max_failures` disables; `max_retries` resets per run | Tier 4 1h DST shift; duplicate batch rows; target keeps scheduling where Oracle stopped | `timezone_id` = Oracle region; idempotent tasks; `max_failures` is a GAP (`on_failure` + runbook or monitor job), never `max_retries` |
| 17 | SQL*Plus directives | `&var` substituted pre-parse; `COLUMN FORMAT` shapes output | Tier 4 formatted-output diff | strip directives; job parameters; formatting to the consumer |
| 18 | Synonyms and `@dblink` | `PUBLIC` synonyms hide owners; links read another DB's TZ/charset | Tier 1 drift when the synonym pointed elsewhere | resolve every synonym to `OWNER.TABLE`; remote tables are separate census objects |
| 19 | Roles, VPD, redaction | predicates on `SYS_CONTEXT`; partial masks | Tier 3 masked vs unmasked by principal | recon as an unfiltered principal both sides; inexpressible policy = `GAP`, never emulated |
| 20 | Implicit conversions, `NLS_*` | `WHERE id = '00123'` compares numerically; `TO_DATE`/`TO_NUMBER` follow session settings | Tier 1 on quoted-number filters; Tier 3 `NULL` from `try_cast` | cast literals to the column type; carry every format explicitly |
| 21 | `VARCHAR2(n BYTE)` multibyte | byte semantics truncate (`ORA-12899`) | Tier 3 truncated names; `MAX(LENGTH)` drift | character semantics on target; pre-truncated source data goes in the tolerance record |
| 22 | NULL ordering | `ORDER BY` puts NULL last ascending; Databricks first | Tier 4 order; Tier 3 pages with NULL keys | spell `NULLS FIRST/LAST` |
| 23 | `GREATEST`/`LEAST`, `\|\|` with NULL | NULL-propagating vs NULL-skipping | Tier 3 on derived text keys/dates | wrap per the function map; `empty_string_is_null` |
| 24 | `DECODE(NULL,NULL,...)` | NULL matches NULL | Tier 3 on NULL-key rows | `CASE WHEN x IS NULL ...` |
| 25 | `AVG`/`SUM` scale growth | full precision vs `+4` scale / `+10` precision | Tier 2 last-digit rounding | `cast(sum(x) AS DECIMAL(38,s))`; `decimal_round` |
