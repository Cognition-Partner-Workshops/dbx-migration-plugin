-- Lakebase / PostgreSQL 17 (OLTP track, !dbx_migrate_oltp). Cites: [lakebase:SKILL.md], [pg17:sql-createsequence],
-- [pg17:sql-createtrigger], [pg17:plpgsql-trigger], [pg17:sql-createprocedure], [pg17:plpgsql-transactions].
-- Analytical (Delta) track has no triggers: the same logic is folded into each writer (example 01).

CREATE SEQUENCE poladm.policy_seq START WITH 1000000 INCREMENT BY 1 CACHE 200 NO CYCLE;
CREATE SEQUENCE poladm.audit_seq  START WITH 1       INCREMENT BY 1 CACHE 1000 NO CYCLE;
-- Cutover: ALTER SEQUENCE ... RESTART WITH <DBA_SEQUENCES.LAST_NUMBER + CACHE>; never lower.

-- PRAGMA AUTONOMOUS_TRANSACTION has no Postgres equivalent: the logger runs inside the caller's transaction, so
-- a caller ROLLBACK also drops the audit row (record in 06_decisions.md). No COMMIT: transaction control is only
-- legal in a top-level CALL chain and would commit the caller's work.
CREATE OR REPLACE PROCEDURE poladm.prc_log_event(
  p_policy_id   bigint,
  p_event_cd    varchar(20),
  p_old_status  varchar(10)  DEFAULT NULL,
  p_new_status  varchar(10)  DEFAULT NULL,
  p_old_premium numeric      DEFAULT NULL,      -- NUMBER without scale -> unbounded numeric
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
EXCEPTION
  WHEN OTHERS THEN
    NULL;                                         -- WHEN OTHERS THEN ROLLBACK: the EXCEPTION block already rolls back
END;
$$;

-- INSERTING/UPDATING -> TG_OP; :NEW/:OLD -> NEW/OLD; the trigger body becomes a function plus a binding.
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
    NEW.created_dt  := localtimestamp;              -- SYSDATE (no TZ)
    NEW.created_by  := session_user;                -- SYS_CONTEXT('USERENV','SESSION_USER')
    NEW.row_version := 1;
    l_event := 'INSERT';
  ELSIF TG_OP = 'UPDATE' THEN
    NEW.updated_dt  := localtimestamp;
    NEW.updated_by  := session_user;
    NEW.row_version := coalesce(OLD.row_version, 0) + 1;
    l_event := 'UPDATE';
  END IF;

  NEW.policy_no := replace(upper(trim(NEW.policy_no)), 'AL/', 'ALB-');

  IF NEW.cover_note_ref = '' THEN                   -- dead in Oracle ('' IS NULL); live here, keep it
    NEW.cover_note_ref := NULL;
  END IF;

  NEW.active_policy_flag :=
    CASE WHEN NEW.policy_status = 'LIVE'
          AND current_date BETWEEN NEW.inception_dt::date AND NEW.expiry_dt::date   -- TRUNC(date) -> ::date
         THEN 'Y' ELSE 'N' END;

  IF TG_OP = 'INSERT'
     OR coalesce(OLD.policy_status, '~') <> NEW.policy_status
     OR coalesce(OLD.annual_premium, -1) <> NEW.annual_premium THEN
    CALL poladm.prc_log_event(NEW.policy_id, l_event, OLD.policy_status, NEW.policy_status,
                              OLD.annual_premium, NEW.annual_premium);
  END IF;

  RETURN NEW;
END;
$$;

CREATE TRIGGER trg_policy_biu
  BEFORE INSERT OR UPDATE ON poladm.policy
  FOR EACH ROW EXECUTE FUNCTION poladm.trg_policy_biu_fn();
