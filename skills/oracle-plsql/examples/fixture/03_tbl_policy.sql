-- Object class: TABLE. Census key: POLADM.POLICY
-- Trap carrier: NUMBER without precision/scale (annual_premium, ipt_rate, sum_insured) and
-- VARCHAR2 columns that applications write as '' (which Oracle stores as NULL).

CREATE TABLE poladm.policy (
  policy_id          NUMBER(12)     NOT NULL,           -- populated by trigger from policy_seq
  policy_no          VARCHAR2(20)   NOT NULL,           -- 'ALB-0001234' or legacy 'AL/0001234'
  party_id           VARCHAR2(20)   NOT NULL,
  broker_id          NUMBER(8)      NULL,
  product_cd         VARCHAR2(6)    NOT NULL,
  policy_status      VARCHAR2(10)   NOT NULL,           -- QUOTED / LIVE / LAPSED / CANCELLED
  active_policy_flag CHAR(1)        DEFAULT 'N' NOT NULL,
  inception_dt       DATE           NOT NULL,           -- written as TRUNC(SYSDATE) by some apps, SYSDATE by others
  expiry_dt          DATE           NOT NULL,
  annual_premium     NUMBER         NOT NULL,           -- NUMBER without scale: up to 38 significant digits, any scale
  ipt_rate           NUMBER         DEFAULT 0.12 NULL,  -- NUMBER without scale, ratio
  sum_insured        NUMBER         NULL,
  premium_ccy        CHAR(3)        DEFAULT 'GBP' NOT NULL,
  cover_note_ref     VARCHAR2(30)   NULL,               -- apps insert '' here; Oracle stores NULL
  underwriter_notes  CLOB           NULL,
  row_version        NUMBER(10)     DEFAULT 1 NOT NULL,
  created_dt         DATE           DEFAULT SYSDATE NOT NULL,
  created_by         VARCHAR2(30)   DEFAULT USER NOT NULL,
  updated_dt         DATE           NULL,
  updated_by         VARCHAR2(30)   NULL,
  CONSTRAINT policy_pk        PRIMARY KEY (policy_id),
  CONSTRAINT policy_no_uk     UNIQUE (policy_no),
  CONSTRAINT policy_party_fk  FOREIGN KEY (party_id)  REFERENCES poladm.party (party_id),
  CONSTRAINT policy_broker_fk FOREIGN KEY (broker_id) REFERENCES poladm.broker (broker_id),
  CONSTRAINT policy_status_ck CHECK (policy_status IN ('QUOTED','LIVE','LAPSED','CANCELLED')),
  CONSTRAINT policy_active_ck CHECK (active_policy_flag IN ('Y','N')),
  CONSTRAINT policy_dates_ck  CHECK (expiry_dt > inception_dt)
);

CREATE INDEX poladm.policy_party_ix  ON poladm.policy (party_id);
CREATE INDEX poladm.policy_expiry_ix ON poladm.policy (policy_status, expiry_dt);

COMMENT ON COLUMN poladm.policy.annual_premium IS 'Gross annual premium; scale not constrained at DDL level';
