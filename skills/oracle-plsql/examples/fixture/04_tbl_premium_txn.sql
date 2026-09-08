-- Object class: TABLE. Census key: POLADM.PREMIUM_TXN
-- Trap carrier: DATE columns that carry a time-of-day component, TIMESTAMP WITH TIME ZONE,
-- TIMESTAMP WITH LOCAL TIME ZONE, INTERVAL DAY TO SECOND, RAW(16) GUID, BINARY_DOUBLE.

CREATE TABLE poladm.premium_txn (
  txn_id             NUMBER(14)                   NOT NULL,
  policy_id          NUMBER(12)                   NOT NULL,
  txn_type_cd        VARCHAR2(4)                  NOT NULL,      -- NB / RN / MTA / CANC / REFD
  txn_dt             DATE           DEFAULT SYSDATE NOT NULL,    -- carries hh24:mi:ss; reports TRUNC() it
  effective_dt       DATE                         NOT NULL,      -- business date, midnight by convention
  posted_ts          TIMESTAMP(6) WITH TIME ZONE  DEFAULT SYSTIMESTAMP NOT NULL,
  settled_ts         TIMESTAMP(6) WITH LOCAL TIME ZONE NULL,     -- normalised to DB time zone on write
  settlement_lag     INTERVAL DAY(3) TO SECOND(0) NULL,
  amount             NUMBER(12,2)                 NOT NULL,
  tax_amount         NUMBER(12,2)   DEFAULT 0     NOT NULL,
  fx_rate            BINARY_DOUBLE                NULL,          -- IEEE double, not NUMBER
  ccy                CHAR(3)        DEFAULT 'GBP' NOT NULL,
  txn_guid           RAW(16)        DEFAULT SYS_GUID() NOT NULL,
  source_system      VARCHAR2(20)                 NULL,
  CONSTRAINT premium_txn_pk PRIMARY KEY (txn_id),
  CONSTRAINT premium_txn_policy_fk FOREIGN KEY (policy_id) REFERENCES poladm.policy (policy_id),
  CONSTRAINT premium_txn_type_ck CHECK (txn_type_cd IN ('NB','RN','MTA','CANC','REFD')),
  CONSTRAINT premium_txn_guid_uk UNIQUE (txn_guid)
)
PARTITION BY RANGE (effective_dt) INTERVAL (NUMTOYMINTERVAL(1,'MONTH'))
( PARTITION p_pre_2020 VALUES LESS THAN (TO_DATE('2020-01-01','YYYY-MM-DD')) );

CREATE INDEX poladm.premium_txn_policy_ix ON poladm.premium_txn (policy_id, effective_dt);
