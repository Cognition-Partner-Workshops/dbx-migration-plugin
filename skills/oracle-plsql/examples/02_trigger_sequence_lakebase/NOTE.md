# 02 — Sequences + `:NEW`/`:OLD` trigger + autonomous logger -> Lakebase (OLTP parity)

**Track**: OLTP `!dbx_migrate_oltp` -> Lakebase (PostgreSQL 17). On the analytical track there are no triggers or
sequences: `policy_id` becomes an identity column and the trigger body is folded into each writer (examples 01, 03).

| Oracle | Converted | SKILL.md |
|---|---|---|
| `CREATE SEQUENCE ... CACHE 200 NOCYCLE NOORDER` | `CREATE SEQUENCE ... CACHE 200 NO CYCLE`; cutover `RESTART WITH` from `DBA_SEQUENCES.LAST_NUMBER + CACHE` | trap 10 |
| row trigger, `INSERTING`/`UPDATING`, `:NEW`/`:OLD` | trigger function (`TG_OP`, `NEW`/`OLD`, `RETURN NEW`) + `CREATE TRIGGER` | construct map |
| `seq.NEXTVAL` in PL/SQL | `nextval('schema.seq')` | function map |
| `SYSDATE` into `DATE`; `SYS_CONTEXT('USERENV','SESSION_USER')` | `localtimestamp` into `timestamp(0)`; `session_user` | trap 3 |
| `IF :NEW.x = '' THEN` (dead in Oracle) | live branch in Postgres, kept | trap 1 |
| `TRUNC(SYSDATE) BETWEEN TRUNC(a) AND TRUNC(b)` | `current_date BETWEEN a::date AND b::date` | function map |
| `PRAGMA AUTONOMOUS_TRANSACTION` + `COMMIT` | plain procedure, no `COMMIT`; audit rows lost on caller rollback (decision) | trap 14 |
| `WHEN OTHERS THEN ROLLBACK` | `WHEN OTHERS THEN NULL` (swallow, parity) | trap 13 |

**Recon**: Tier 3 on `poladm.policy` keyed by `policy_no` (`row_version`, `active_policy_flag`, `policy_no`
normalisation, `cover_note_ref` `''`/NULL are all trigger outputs; `policy_id` excluded, uniqueness-checked instead).
Tier 1/2 on `policy_audit_log` per `event_cd`: the expected delta is exactly the count of rolled-back business
transactions in the replay window, pre-declared in `06_decisions.md`. Tier 4: the SOAP layer expects `ALB-` prefixes.

**Canonicalization**: `datetime_utc_truncate_ms` (`created_dt`, `updated_dt`), `decimal_round` (`old_premium`,
`new_premium`), `empty_string_is_null` (`cover_note_ref`).

**Open decisions**: accept audit rows lost on rollback (default) or an extension-based out-of-band writer if it is on
the Lakebase supported list; `localtimestamp` vs `now() AT TIME ZONE 'UTC'` depends on the census `DBTIMEZONE`.

**Not verified live**: trigger/sequence creation and firing on Lakebase; `CALL` from a trigger function; `RESTART
WITH` at cutover.
