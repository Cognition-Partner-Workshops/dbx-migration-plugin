-- Converted: fixture 01_seq_policy.sql + 07_trg_policy_biu.sql + 06_prc_log_event.sql -> Lakebase (PostgreSQL 17).
-- Track: OLTP / `!dbx_migrate_oltp` (SKILL.md §3 OLTP profile, §6 trigger/sequence/autonomous rows, §7 traps 10, 14).
-- Lakebase is Postgres [lakebase:SKILL.md]; syntax below is PostgreSQL 17 [pg17:sql-createsequence],
-- [pg17:sql-createtrigger], [pg17:plpgsql-trigger], [pg17:sql-createprocedure].
-- The analytical (Delta) profile has no triggers: the same logic is folded into the writer (see example 02).

-- ---------- sequences (§4 SEQUENCE row) ----------
-- Oracle CACHE 200 NOORDER -> Postgres CACHE 200 (per-session cache; gaps and non-monotonic values are normal in both)
CREATE SEQUENCE poladm.policy_seq START WITH 1000000 INCREMENT BY 1 CACHE 200 NO CYCLE;
CREATE SEQUENCE poladm.audit_seq  START WITH 1       INCREMENT BY 1 CACHE 1000 NO CYCLE;
-- Cutover: ALTER SEQUENCE poladm.policy_seq RESTART WITH <ALL_SEQUENCES.LAST_NUMBER + CACHE> (read from the census; never lower)

-- ---------- autonomous logger (§7 trap 14) ----------
-- PRAGMA AUTONOMOUS_TRANSACTION has no Postgres equivalent: a PL/pgSQL procedure runs inside the caller's transaction,
-- so a caller ROLLBACK also removes the audit row. Chosen fix (must be recorded in 06_decisions.md): keep the logger
-- as a plain procedure and accept that error-path audit rows are lost on rollback; the surviving rows are identical.
-- Alternative if audit-on-rollback is a hard requirement: dblink/pg_background-style extension, only if it is on the
-- supported list linked from [lakebase:SKILL.md#PostgreSQL Extensions] (verify there).
CREATE OR REPLACE PROCEDURE poladm.prc_log_event(
  p_policy_id   bigint,
  p_event_cd    varchar(20),
  p_old_status  varchar(10)  DEFAULT NULL,
  p_new_status  varchar(10)  DEFAULT NULL,
  p_old_premium numeric      DEFAULT NULL,      -- NUMBER without scale -> numeric (unbounded) §4
  p_new_premium numeric      DEFAULT NULL,
  p_message     text         DEFAULT NULL)
LANGUAGE plpgsql
AS $$
BEGIN
  INSERT INTO poladm.policy_audit_log
    (audit_id, policy_id, event_cd, old_status, new_status, old_premium, new_premium, message)
  VALUES
    (nextval('poladm.audit_seq'), p_policy_id, p_event_cd, p_old_status, p_new_status,
     p_old_premium, p_new_premium, left(p_message, 4000));
  -- no COMMIT: Oracle's COMMIT here committed only the autonomous txn; in Postgres transaction control is only
  -- possible in top-level CALL/DO chains [pg17:plpgsql-transactions], so a COMMIT here fails when called from the
  -- trigger and would in any case commit the caller's work.
EXCEPTION
  WHEN OTHERS THEN
    -- Oracle: WHEN OTHERS THEN ROLLBACK (swallow). Postgres: the EXCEPTION block already rolls back this
    -- sub-transaction; swallowing keeps parity with "logging never breaks the business transaction" (§7 trap 13).
    NULL;
END;
$$;

-- ---------- trigger (§7 trap 10) ----------
-- Oracle: one BEFORE INSERT OR UPDATE ... FOR EACH ROW trigger with INSERTING/UPDATING predicates and :NEW/:OLD.
-- Postgres: a trigger function using TG_OP and NEW/OLD, plus a CREATE TRIGGER binding.
CREATE OR REPLACE FUNCTION poladm.trg_policy_biu_fn() RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  l_event varchar(20);
BEGIN
  IF TG_OP = 'INSERT' THEN
    IF NEW.policy_id IS NULL THEN
      NEW.policy_id := nextval('poladm.policy_seq');
    END IF;
    NEW.created_dt  := localtimestamp;              -- SYSDATE -> localtimestamp (session TZ; Oracle DATE has no TZ) §5 #52
    NEW.created_by  := session_user;                -- SYS_CONTEXT('USERENV','SESSION_USER') §5 #92
    NEW.row_version := 1;
    l_event := 'INSERT';
  ELSIF TG_OP = 'UPDATE' THEN
    NEW.updated_dt  := localtimestamp;
    NEW.updated_by  := session_user;
    NEW.row_version := coalesce(OLD.row_version, 0) + 1;
    l_event := 'UPDATE';
  END IF;

  NEW.policy_no := replace(upper(trim(NEW.policy_no)), 'AL/', 'ALB-');

  -- Oracle: `:NEW.cover_note_ref = ''` is never true because '' IS NULL. In Postgres '' is a value,
  -- so the dead branch becomes live and must be kept to preserve the stored value (§7 trap 1).
  IF NEW.cover_note_ref = '' THEN
    NEW.cover_note_ref := NULL;
  END IF;

  NEW.active_policy_flag :=
    CASE WHEN NEW.policy_status = 'LIVE'
          AND current_date BETWEEN NEW.inception_dt::date AND NEW.expiry_dt::date   -- TRUNC(date) -> ::date §5 #61
         THEN 'Y' ELSE 'N' END;

  IF TG_OP = 'INSERT'
     OR coalesce(OLD.policy_status, '~') <> NEW.policy_status
     OR coalesce(OLD.annual_premium, -1) <> NEW.annual_premium THEN
    CALL poladm.prc_log_event(
      p_policy_id   => NEW.policy_id,
      p_event_cd    => l_event,
      p_old_status  => OLD.policy_status,             -- NULL on INSERT, as :OLD is in Oracle
      p_new_status  => NEW.policy_status,
      p_old_premium => OLD.annual_premium,
      p_new_premium => NEW.annual_premium);
  END IF;

  RETURN NEW;                                          -- BEFORE ROW trigger must return the (possibly modified) row
END;
$$;

CREATE TRIGGER trg_policy_biu
  BEFORE INSERT OR UPDATE ON poladm.policy
  FOR EACH ROW EXECUTE FUNCTION poladm.trg_policy_biu_fn();

-- ---------- analytical (Delta) profile note ----------
-- No triggers/sequences on Delta. policy_id -> BIGINT GENERATED ALWAYS AS IDENTITY
-- [docs:sql-ref-syntax-ddl-create-table-using]; created_*/updated_*/row_version/active_policy_flag/policy_no
-- normalisation are set by the writing MERGE (example 02); the audit INSERT becomes a second statement in the same
-- Lakeflow Jobs task (a failed MERGE then also skips the audit row, mirroring the "lost on rollback" decision above).
