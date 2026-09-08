-- Object class: TRIGGER. Census key: POLADM.TRG_POLICY_BIU (on POLADM.POLICY)
-- BEFORE INSERT OR UPDATE, row-level. Assigns the surrogate key from policy_seq, maintains
-- audit columns and row_version, derives active_policy_flag, and logs status/premium changes
-- through the autonomous logger. :NEW / :OLD correlation names throughout.

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
    :NEW.created_dt := SYSDATE;
    :NEW.created_by := SYS_CONTEXT('USERENV','SESSION_USER');
    :NEW.row_version := 1;
    l_event := 'INSERT';
  ELSIF UPDATING THEN
    :NEW.updated_dt  := SYSDATE;
    :NEW.updated_by  := SYS_CONTEXT('USERENV','SESSION_USER');
    :NEW.row_version := NVL(:OLD.row_version, 0) + 1;
    l_event := 'UPDATE';
  END IF;

  -- Normalise identifiers the way pkg_policy_inquiry expects to find them
  :NEW.policy_no := REPLACE(UPPER(TRIM(:NEW.policy_no)), 'AL/', 'ALB-');

  -- '' arriving from the SOAP layer is already NULL by the time it reaches :NEW
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
