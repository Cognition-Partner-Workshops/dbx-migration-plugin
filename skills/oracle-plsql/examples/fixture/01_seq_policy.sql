-- Fixture: POLADM (policy administration, OLTP profile). Synthetic; no customer data.
-- Object class: SEQUENCE. Census key: POLADM.POLICY_SEQ, POLADM.AUDIT_SEQ

CREATE SEQUENCE poladm.policy_seq
  START WITH 1000000
  INCREMENT BY 1
  CACHE 200          -- cached values are lost on instance restart: gaps are normal
  NOCYCLE
  NOORDER;           -- RAC: values are not monotonic across nodes

CREATE SEQUENCE poladm.audit_seq
  START WITH 1
  INCREMENT BY 1
  CACHE 1000
  NOCYCLE;
