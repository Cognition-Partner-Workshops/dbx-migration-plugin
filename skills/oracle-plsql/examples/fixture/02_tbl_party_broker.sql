-- Object class: TABLE. Census keys: POLADM.PARTY, POLADM.BROKER
-- PARTY is the OLTP master for people/organisations; BROKER is a self-referencing
-- hierarchy (region -> network -> branch -> agent) consumed by the CONNECT BY query.

CREATE TABLE poladm.party (
  party_id          VARCHAR2(20)   NOT NULL,
  legacy_client_no  NUMBER(10)     NULL,              -- pre-2009 CLIENT_NO, still keyed by SOAP callers
  party_type_cd     CHAR(1)        NOT NULL,          -- P=person, O=organisation
  surname           VARCHAR2(80)   NULL,
  forename          VARCHAR2(80)   NULL,
  org_name          NVARCHAR2(200) NULL,
  postcode          CHAR(8)        NULL,              -- blank-padded: 'SW1A1AA ' vs 'SW1A 1AA'
  date_of_birth     DATE           NULL,              -- midnight in practice, but the type carries time
  created_dt        DATE           DEFAULT SYSDATE NOT NULL,
  updated_dt        DATE           NULL,
  CONSTRAINT party_pk PRIMARY KEY (party_id),
  CONSTRAINT party_legacy_uk UNIQUE (legacy_client_no),
  CONSTRAINT party_type_ck CHECK (party_type_cd IN ('P','O'))
);

CREATE TABLE poladm.broker (
  broker_id         NUMBER(8)      NOT NULL,
  parent_broker_id  NUMBER(8)      NULL,              -- NULL = root of the hierarchy
  broker_ref        VARCHAR2(12)   NOT NULL,
  broker_name       VARCHAR2(120)  NOT NULL,
  tier_cd           VARCHAR2(10)   NOT NULL,          -- REGION / NETWORK / BRANCH / AGENT
  commission_pct    NUMBER         NULL,              -- NUMBER without scale (e.g. 12.5, 0.075)
  active_flag       CHAR(1)        DEFAULT 'Y' NOT NULL,
  CONSTRAINT broker_pk PRIMARY KEY (broker_id),
  CONSTRAINT broker_ref_uk UNIQUE (broker_ref),
  CONSTRAINT broker_parent_fk FOREIGN KEY (parent_broker_id) REFERENCES poladm.broker (broker_id),
  CONSTRAINT broker_active_ck CHECK (active_flag IN ('Y','N'))
);

CREATE INDEX poladm.broker_parent_ix ON poladm.broker (parent_broker_id);
