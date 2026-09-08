# 04 — Sequence + `:NEW`/`:OLD` trigger + autonomous logger -> Lakebase (OLTP parity)

**Source**: `fixture/01_seq_policy.sql`, `fixture/07_trg_policy_biu.sql`, `fixture/06_prc_log_event.sql`.
`BEFORE INSERT OR UPDATE ... FOR EACH ROW`, `INSERTING`/`UPDATING`, `:NEW.policy_id := policy_seq.NEXTVAL`,
`SYS_CONTEXT('USERENV','SESSION_USER')`, `PRAGMA AUTONOMOUS_TRANSACTION` + `COMMIT` + `WHEN OTHERS THEN ROLLBACK`.

**Profile / track**: OLTP `!dbx_migrate_oltp` -> Lakebase (PostgreSQL 17). The analytical profile has no
trigger/sequence objects; the note at the end of `converted.sql` says where each side effect lands on Delta.

## Constructs exercised (SKILL.md refs)

| Oracle | Converted | Rule |
|---|---|---|
| `CREATE SEQUENCE ... CACHE 200 NOCYCLE NOORDER` | `CREATE SEQUENCE ... CACHE 200 NO CYCLE` + cutover `RESTART WITH` from `ALL_SEQUENCES.LAST_NUMBER` | §4 sequence row; §7 trap 10 |
| row trigger with `INSERTING`/`UPDATING`, `:NEW`/`:OLD` | trigger function (`TG_OP`, `NEW`/`OLD`, `RETURN NEW`) + `CREATE TRIGGER` | §6 triggers |
| `seq.NEXTVAL` in PL/SQL | `nextval('schema.seq')` | §5 #87 |
| `SYSDATE` into a `DATE` column | `localtimestamp` into `timestamp(0)` | §5 #52; §7 trap 3 |
| `SYS_CONTEXT('USERENV','SESSION_USER')` | `session_user` | §5 #92 |
| `IF :NEW.x = '' THEN` (dead in Oracle) | live branch in Postgres, kept | §7 trap 1 |
| `TRUNC(SYSDATE) BETWEEN TRUNC(a) AND TRUNC(b)` | `current_date BETWEEN a::date AND b::date` | §5 #61 |
| `NVL(:OLD.col, sentinel) <> :NEW.col` | `coalesce(OLD.col, sentinel) <> NEW.col` | §5 #1, #96 |
| `PRAGMA AUTONOMOUS_TRANSACTION` + `COMMIT` | plain procedure, no `COMMIT`; decision recorded | §6 autonomous txn; §7 trap 14 |
| `WHEN OTHERS THEN ROLLBACK` | `WHEN OTHERS THEN NULL` (swallow, parity) | §7 trap 13 |

## Recon tier that catches a wrong conversion

- **Tier 3** on `poladm.policy` keyed by `policy_no`: `row_version`, `active_policy_flag`, `policy_no`
  normalisation and `cover_note_ref` (`''` vs `NULL`) are all trigger outputs. A trigger that returns `NULL`
  instead of `NEW`, or is created `AFTER` instead of `BEFORE`, leaves every one of these wrong or the row
  missing. `policy_id` is excluded from Tier 3 (sequence values are not expected to match) and covered by a
  uniqueness check instead.
- **Tier 1/2** on `poladm.policy_audit_log`: row count per `event_cd` and `SUM(new_premium)` reveal the
  autonomous-transaction difference. On Oracle, `ERROR` rows written just before a caller `ROLLBACK` survive;
  on Lakebase they do not. The expected delta is exactly the count of rolled-back business transactions in the
  replay window, and must be pre-declared in `06_decisions.md` (otherwise the compare fails, which is correct).
- **Tier 4**: the SOAP layer expects `ALB-` prefixed `policy_no`; a dropped `replace()` surfaces there.

## Canonicalization used

`datetime_utc_truncate_ms` (`created_dt`, `updated_dt`: Oracle `DATE` seconds vs Postgres `timestamp(0)`),
`decimal_round` (`old_premium`/`new_premium`, `NUMBER` -> `numeric`), `empty_string_is_null` (`cover_note_ref`).

## Open decisions

1. Autonomous audit rows lost on rollback: accept (default) or add an extension-based out-of-band writer
   (only if the extension is on the Lakebase supported list; not verified here).
2. `localtimestamp` vs `now() AT TIME ZONE 'UTC'` for `created_dt`: depends on the Oracle `DBTIMEZONE`/session
   TZ recorded in the census (§7 trap 3).

## Not verified live

Trigger/sequence creation and firing on Lakebase; `CALL` from a trigger function; `RESTART WITH` at cutover;
the count of rollback-lost audit rows.
