-- Object class: PROCEDURE. Census key: POLADM.PRC_LOG_EVENT
-- Autonomous-transaction logger: its INSERT + COMMIT survive a ROLLBACK of the caller.
-- Writes: POLADM.POLICY_AUDIT_LOG (and consumes POLADM.AUDIT_SEQ).

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
    ROLLBACK;                              -- never let logging break the business transaction
END prc_log_event;
/
