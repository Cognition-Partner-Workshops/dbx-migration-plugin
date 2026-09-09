-- Oracle OLTP schema: sequences, a BEFORE INSERT OR UPDATE row trigger (:NEW/:OLD, INSERTING/UPDATING,
-- NEXTVAL, SYS_CONTEXT, a '' = NULL dead branch) and the autonomous-transaction logger it calls.

CREATE SEQUENCE poladm.policy_seq START WITH 1000000 INCREMENT BY 1 CACHE 200 NOCYCLE NOORDER;
CREATE SEQUENCE poladm.audit_seq  START WITH 1       INCREMENT BY 1 CACHE 1000 NOCYCLE;

CREATE OR REPLACE TRIGGER poladm.trg_policy_biu
  BEFORE INSERT OR UPDATE ON poladm.policy
  FOR EACH ROW
DECLARE
  l_event VARCHAR2(20);
BEGIN
  IF INSERTING THEN
    IF :NEW.policy_id IS NULL THEN
      :NEW.policy_id := poladm.policy_seq.NEXTVAL;
    END IF;
    :NEW.created_dt  := SYSDATE;
    :NEW.created_by  := SYS_CONTEXT('USERENV','SESSION_USER');
    :NEW.row_version := 1;
    l_event := 'INSERT';
  ELSIF UPDATING THEN
    :NEW.updated_dt  := SYSDATE;
    :NEW.updated_by  := SYS_CONTEXT('USERENV','SESSION_USER');
    :NEW.row_version := NVL(:OLD.row_version, 0) + 1;
    l_event := 'UPDATE';
  END IF;

  :NEW.policy_no := REPLACE(UPPER(TRIM(:NEW.policy_no)), 'AL/', 'ALB-');

  IF :NEW.cover_note_ref = '' THEN            -- never true in Oracle: '' IS NULL
    :NEW.cover_note_ref := NULL;
  END IF;

  :NEW.active_policy_flag :=
    CASE WHEN :NEW.policy_status = 'LIVE'
          AND TRUNC(SYSDATE) BETWEEN TRUNC(:NEW.inception_dt) AND TRUNC(:NEW.expiry_dt)
         THEN 'Y' ELSE 'N' END;

  IF INSERTING
     OR NVL(:OLD.policy_status,'~') <> :NEW.policy_status
     OR NVL(:OLD.annual_premium,-1) <> :NEW.annual_premium THEN
    poladm.prc_log_event(
      p_policy_id   => :NEW.policy_id,
      p_event_cd    => l_event,
      p_old_status  => :OLD.policy_status,
      p_new_status  => :NEW.policy_status,
      p_old_premium => :OLD.annual_premium,
      p_new_premium => :NEW.annual_premium);
  END IF;
END trg_policy_biu;
/

CREATE OR REPLACE PROCEDURE poladm.prc_log_event (
  p_policy_id   IN NUMBER,
  p_event_cd    IN VARCHAR2,
  p_old_status  IN VARCHAR2 DEFAULT NULL,
  p_new_status  IN VARCHAR2 DEFAULT NULL,
  p_old_premium IN NUMBER   DEFAULT NULL,
  p_new_premium IN NUMBER   DEFAULT NULL,
  p_message     IN VARCHAR2 DEFAULT NULL
) AS
  PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
  INSERT INTO poladm.policy_audit_log
    (audit_id, policy_id, event_cd, old_status, new_status, old_premium, new_premium, message)
  VALUES
    (poladm.audit_seq.NEXTVAL, p_policy_id, p_event_cd, p_old_status, p_new_status,
     p_old_premium, p_new_premium, SUBSTR(p_message, 1, 4000));
  COMMIT;                                  -- commits ONLY the autonomous transaction
EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK;                              -- logging never breaks the business transaction
END prc_log_event;
/
