# 01 — `MERGE` from a GTT with `DELETE WHERE`, `NEXTVAL` and duplicate-key parity

**Track**: analytical Delta (converted file); Lakebase variant noted at the bottom of `converted.sql`.

| Oracle | Converted | SKILL.md |
|---|---|---|
| `GLOBAL TEMPORARY TABLE ... ON COMMIT PRESERVE ROWS` | `CREATE TEMPORARY TABLE` (drop-first on re-run) | construct map |
| `WHEN MATCHED THEN UPDATE ... WHERE ... DELETE WHERE` | `WHEN MATCHED AND action='D' THEN DELETE` **before** the `UPDATE` branch | trap 8 |
| duplicate/NULL source keys: ORA-30926 (matched) / ORA-00001 (unmatched, both insert) / ORA-01400 (NULL key inserts); unmatched `'D'` duplicates or NULLs are a no-op | `assert_true` pre-check on the normalised key restricted to matched or insert-eligible duplicates and NULLs (a NULL key could not be re-identified for the audit fold-in either); no `QUALIFY` dedupe | trap 8 |
| `policy_seq.NEXTVAL` in `INSERT` | identity column (Delta) / `nextval()` (Lakebase); recon keys on `policy_no` | trap 10 |
| `NULLIF(TRIM(x), '')`, `TRIM(policy_no)` of blanks (NULL in Oracle), `TRUNC(date)`, `CHAR(1)` | kept explicitly; `nullif(..., '')` on the normalised key so a blank never matches or inserts `''`; `date_trunc('DAY')` on `TIMESTAMP_NTZ`; `rtrim()` | traps 1, 3, 5 |
| `BEFORE INSERT OR UPDATE` trigger firing on the `MERGE` | `row_version`/`updated_*`/`active_policy_flag` folded into the `MERGE`; audit rows from a pre-`MERGE` `:OLD` image | trap 10, example 02 |
| `COMMIT` | dropped (one Delta transaction per statement) or `BEGIN ATOMIC` | construct map |

**Recon**: Tier 1 count on `poladm.policy` (misordered `DELETE` leaves the `'D'` rows); Tier 1 on `policy_audit_log`
per `event_cd` (missing fold-in = 0 rows, logging every matched row = over-count); Tier 2 `SUM(annual_premium)` per
`product_cd` with `decimal_round`; Tier 3 keyed on `policy_no` for `cover_note_ref` `''`/NULL, `inception_dt` time
component, `row_version`/`active_policy_flag` drift. A `QUALIFY` dedupe succeeds where Oracle failed, and a pre-check
over *all* duplicates fails where Oracle succeeded (repeated unmatched `'D'` rows): only Tier 4 (job outcome parity) sees either.

**Canonicalization**: `empty_string_is_null`, `decimal_round`, `datetime_utc_truncate_ms`, `rstrip_spaces`.

**Not verified live**: `WHEN MATCHED AND ... THEN DELETE` ordering; `assert_true` over an empty `HAVING` result; that
`poladm.policy.policy_no` carries a unique index (the pre-check assumes it, as the source `ON` clause implies);
`CREATE TEMPORARY TABLE ... AS` with an `IN (subquery)`; Postgres 17 `MERGE` duplicate-source behaviour on Lakebase.

**Open decision**: whether the feed should be deduped (Oracle never was). Default is failure parity.
