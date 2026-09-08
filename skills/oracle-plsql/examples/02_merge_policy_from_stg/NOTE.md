# 02 — `MERGE` from a global temporary table with `DELETE WHERE` and `NEXTVAL`

**Source**: `fixture/09_mrg_policy_from_stg.sql` (GTT `ON COMMIT PRESERVE ROWS`, `MERGE ... WHEN MATCHED THEN UPDATE ...
DELETE WHERE ... WHEN NOT MATCHED THEN INSERT (policy_seq.NEXTVAL, ...)`, `COMMIT`).

**Profile / track**: both. Converted file is the analytical Delta copy (DBSQL); the Lakebase/Postgres variant is
commented at the bottom because the two differ on sequences and trigger fan-out.

## Constructs exercised (SKILL.md refs)

| Oracle | Converted | Rule |
|---|---|---|
| `CREATE GLOBAL TEMPORARY TABLE ... ON COMMIT PRESERVE ROWS` | `CREATE TEMPORARY TABLE` (session-scoped) | §6 temporary tables; `CREATE OR REPLACE TEMP TABLE` unsupported |
| `MERGE ... WHEN MATCHED THEN UPDATE ... DELETE WHERE (feed_action = 'D')` | `WHEN MATCHED AND action='D' THEN DELETE` **before** the `UPDATE` branch | §5 #83; §7 trap 8 (Oracle deletes after applying the update; Databricks evaluates `WHEN MATCHED` clauses in order) |
| duplicate source keys (ORA-30926, whole `MERGE` rolls back) | `SELECT assert_true(count(*) = 0, ...)` pre-check on the normalised key, then the `MERGE`; **no** dedupe | §7 trap 8; Databricks raises `DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE` only for duplicates that hit a target row and inserts the rest twice, and `QUALIFY ... = 1` would silently pick one row, so neither preserves the failure contract on its own |
| `policy_seq.NEXTVAL` in the `INSERT` | identity column on the analytical copy; `nextval()` on Lakebase | §5 #87; §7 trap 10 |
| `NULLIF(TRIM(cover_note_ref), '')` (Oracle: `''` already NULL) | kept explicitly | §7 trap 1 |
| `TRUNC(inception_dt)` | `date_trunc('DAY', ...)` on `TIMESTAMP_NTZ` | §5 #61; §7 trap 3 |
| `CHAR(1) feed_action` | `rtrim()` at read | §7 trap 5 |
| trigger-maintained `row_version`, `updated_*` | folded into `UPDATE SET` on Delta; left to the trigger on Lakebase | §2 trigger fan-out; example 04 |
| `COMMIT` | dropped (one Delta transaction per statement) or `BEGIN ATOMIC` | §6 commit/rollback |

## Recon tier that catches a wrong conversion

- **Tier 1** row counts on `poladm.policy` after the run: if the `DELETE` branch is placed after `UPDATE` (or its
  predicate dropped), the count differs by the number of `'D'` rows. A feed with duplicate keys fails the unit on
  both sides (ORA-30926 / `USER_RAISED_EXCEPTION`), which is the parity being asserted; if the pre-check is dropped
  the target inserts the unmatched duplicates twice (Tier 1 count high) or applies one arbitrary update to matched
  ones (Tier 3 value drift on `policy_status`/`annual_premium` with no count signal), and if a `QUALIFY` dedupe is
  substituted the run *succeeds* where Oracle failed, which only Tier 4 (job outcome parity) sees.
- **Tier 2** `SUM(annual_premium)` per `product_cd` with `decimal_round(10)`: catches the `'D'` rows surviving as
  updates and any scale loss from `NUMBER` -> `DECIMAL`.
- **Tier 3** keyed on `policy_no` (never `policy_id`, which is sequence/identity-assigned): catches `cover_note_ref`
  `''` vs `NULL` (with `empty_string_is_null` on, the diff disappears, which is the intended canonicalization,
  recorded in `06_decisions.md`), `inception_dt` retaining a time component if `date_trunc` is dropped, and
  `row_version` drift if the trigger fold-in is forgotten.

## Canonicalization used

`empty_string_is_null` (`cover_note_ref`), `decimal_round` (`annual_premium`), `datetime_utc_truncate_ms`
(`inception_dt`, `expiry_dt`), `rstrip_spaces` (`feed_action`, `premium_ccy`).

## Not verified live

`MERGE ... WHEN MATCHED AND ... THEN DELETE` ordering on a real Delta table; `assert_true` inside an aggregate over an
empty `HAVING` result (one row, `count(*) = 0`) as the first statement of a `sql_task`; Postgres 17 `MERGE`
duplicate-source behaviour on Lakebase; `BEGIN ATOMIC` with `catalogManaged` tables.

## Open decision

Whether the feed *should* be deduped (Oracle never was; every duplicate batch failed and was re-sent). If the
business answer is yes, the pre-check is replaced by `QUALIFY row_number() OVER (PARTITION BY key ORDER BY <agreed
columns>) = 1` plus a Tier 1 count of dropped rows, recorded in `06_decisions.md`; the default is failure parity.
