-- Object class: TABLE. Census key: POLADM.POLICY_AUDIT_LOG
-- Written only by prc_log_event (autonomous transaction) and by the trigger via that procedure.

CREATE TABLE poladm.policy_audit_log (
  audit_id       NUMBER(14)     NOT NULL,          -- populated from audit_seq inside prc_log_event
  policy_id      NUMBER(12)     NULL,
  event_cd       VARCHAR2(20)   NOT NULL,          -- INSERT / UPDATE / RENEW / ERROR
  old_status     VARCHAR2(10)   NULL,
  new_status     VARCHAR2(10)   NULL,
  old_premium    NUMBER         NULL,
  new_premium    NUMBER         NULL,
  message        VARCHAR2(4000) NULL,
  event_ts       TIMESTAMP(6)   DEFAULT SYSTIMESTAMP NOT NULL,
  session_user   VARCHAR2(30)   DEFAULT SYS_CONTEXT('USERENV','SESSION_USER') NOT NULL,
  client_module  VARCHAR2(64)   DEFAULT SYS_CONTEXT('USERENV','MODULE') NULL,
  CONSTRAINT policy_audit_log_pk PRIMARY KEY (audit_id)
);

CREATE INDEX poladm.policy_audit_policy_ix ON poladm.policy_audit_log (policy_id, event_ts);
