# oracle-plsql static round-trip report

Fixture files: 15; census rows: 48 enumerated (+1 referenced-only) (DATABASE LINK 1, DML SCRIPT 1, FUNCTION 1, GRANT 8, INDEX 5, MATERIALIZED VIEW 1, MATERIALIZED VIEW LOG 2, PACKAGE 1, PACKAGE FUNCTION 2, PACKAGE PROCEDURE 2, PROCEDURE 1, PUBLIC SYNONYM 1, REDACTION POLICY 1, ROLE 2, ROLE MEMBERSHIP 2, SCHEDULER JOB 1, SCHEDULER PROGRAM 1, SEQUENCE 2, SQLPLUS_SCRIPT 1, SYNONYM 1, TABLE 6, TRIGGER 1, VIEW 3, VPD POLICY 1)
Fixture edges: 57 = FACT 53, INFERRED 4, UNVERIFIABLE 0

Albion pkg_policy_inquiry.sql: census rows 3 enumerated (+3 referenced-only) (ODS.ODS_CLAIMS, ODS.ODS_POLICY_360, ODS.PKG_POLICY_INQUIRY, ODS.PKG_POLICY_INQUIRY.GET_PARTY_CLAIMS, ODS.PKG_POLICY_INQUIRY.GET_POLICY_SUMMARY, TERADATA.STG_POLICY_360)
Albion edges: 3 = FACT 2, INFERRED 1, UNVERIFIABLE 0

## INFERRED edges (genuinely dynamic / external; listed per template Acceptance)

| src | dst | kind | risk | detail |
|---|---|---|---|---|
| POLADM.PKG_POLICY_RENEWAL.ARCHIVE_TO | POLADM.<L_SQL> | writes | dynamic-sql | literal prefix "'INSERT INTO poladm.'" |
| ODS.V_CLAIMS_REMOTE | CLAIMS.CLAIM@CLAIMS_LINK | reads | external-db-link |  |
| POLADM.FN_BROKER_PREDICATE | ODS.V_BROKER_HIERARCHY | reads | dynamic-predicate | VPD predicate string |
| POLADM.14_RPT_POLICY_PAGE | POLADM.<&spool> | writes | substitution-in-identifier | SPOOL &spool_file |
| TERADATA.STG_POLICY_360 | ODS.ODS_POLICY_360 | replication | freshness | GoldenGate nightly copy, up to 26h stale (architecture_overview.md) |

## Nodes referenced but not enumerated (inventory gaps, not lineage uncertainty)

- CLAIMS.CLAIM@CLAIMS_LINK (EXTERNAL_TABLE, external)
- ODS.ODS_POLICY_360 (TABLE, not-in-census)
- ODS.ODS_CLAIMS (TABLE, not-in-census)
- TERADATA.STG_POLICY_360 (EXTERNAL_TABLE, external)

## Census (fixture + Albion)

| key | class | file | signals |
|---|---|---|---|
| CLAIMS.CLAIM@CLAIMS_LINK | EXTERNAL_TABLE | - |  |
| GRANT.ODS_READER.ODS.MV_POLICY_PREMIUM_SUMMARY.SELECT | GRANT | 15_syn_dblink_grants.sql | {"column_level": false, "public": false} |
| GRANT.ODS_READER.POLADM.PARTY.SELECT | GRANT | 15_syn_dblink_grants.sql | {"column_level": false, "public": false} |
| GRANT.ODS_READER.POLADM.POLICY.SELECT | GRANT | 15_syn_dblink_grants.sql | {"column_level": false, "public": false} |
| GRANT.ODS_READER.POLADM.PREMIUM_TXN.SELECT | GRANT | 15_syn_dblink_grants.sql | {"column_level": false, "public": false} |
| GRANT.POLADM_APP.ODS.PKG_POLICY_INQUIRY.EXECUTE | GRANT | 15_syn_dblink_grants.sql | {"column_level": false, "public": false} |
| GRANT.POLADM_APP.POLADM.PKG_POLICY_RENEWAL.EXECUTE | GRANT | 15_syn_dblink_grants.sql | {"column_level": false, "public": false} |
| GRANT.POLADM_APP.POLADM.POLICY.SELECT | GRANT | 15_syn_dblink_grants.sql | {"column_level": true, "public": false} |
| GRANT.PUBLIC.ODS.V_BROKER_HIERARCHY.SELECT | GRANT | 15_syn_dblink_grants.sql | {"column_level": false, "public": true} |
| MLOG$_POLADM.POLICY | MATERIALIZED VIEW LOG | 12_mv_policy_premium_summary.sql |  |
| MLOG$_POLADM.PREMIUM_TXN | MATERIALIZED VIEW LOG | 12_mv_policy_premium_summary.sql |  |
| ODS.MV_POLICY_PREMIUM_SUMMARY | MATERIALIZED VIEW | 12_mv_policy_premium_summary.sql | {"refresh": "FAST DEMAND"} |
| ODS.ODS_CLAIMS | TABLE | - |  |
| ODS.ODS_POLICY_360 | TABLE | - |  |
| ODS.PKG_POLICY_INQUIRY | PACKAGE BODY | pkg_policy_inquiry.sql | {"lines": 42} |
| ODS.PKG_POLICY_INQUIRY.GET_PARTY_CLAIMS | PACKAGE FUNCTION | pkg_policy_inquiry.sql |  |
| ODS.PKG_POLICY_INQUIRY.GET_POLICY_SUMMARY | PACKAGE FUNCTION | pkg_policy_inquiry.sql |  |
| ODS.POLICY | SYNONYM | 15_syn_dblink_grants.sql |  |
| ODS.V_BROKER_HIERARCHY | VIEW | 10_qry_broker_hierarchy.sql |  |
| ODS.V_CLAIMS_REMOTE | VIEW | 15_syn_dblink_grants.sql |  |
| ODS.V_PREMIUM_BY_TXN_TYPE | VIEW | 11_qry_premium_pivot.sql |  |
| POLADM.09_MRG_POLICY_FROM_STG | DML SCRIPT | 09_mrg_policy_from_stg.sql |  |
| POLADM.14_RPT_POLICY_PAGE | SQLPLUS_SCRIPT | 14_rpt_policy_page.sql | {"lines": 56, "substitution_vars": 6} |
| POLADM.AUDIT_SEQ | SEQUENCE | 01_seq_policy.sql |  |
| POLADM.BROKER | TABLE | 02_tbl_party_broker.sql | {"constraints": 4, "temporary": false, "type_traps": ["CHAR(1)", "NUMBER"]} |
| POLADM.BROKER_PARENT_IX | INDEX | 02_tbl_party_broker.sql |  |
| POLADM.CLAIMS_LINK | DATABASE LINK | 15_syn_dblink_grants.sql |  |
| POLADM.FN_BROKER_PREDICATE | FUNCTION | 15_syn_dblink_grants.sql | {"lines": 9} |
| POLADM.JOB_NIGHTLY_RENEWAL | SCHEDULER JOB | 13_job_nightly_renewal.sql | {"repeat_interval": "FREQ=DAILY; BYHOUR=2; BYMINUTE=40; BYSECOND=0; BYDAY=MON,TUE,WED,THU,FRI,SAT", "start_date": "TO_TIMESTAMP_TZ('2019-04-01 02:40:00 Europe/London', 'YYYY-MM-DD HH24:MI:SS TZR')"} |
| POLADM.PARTY | TABLE | 02_tbl_party_broker.sql | {"constraints": 3, "temporary": false, "type_traps": ["CHAR(1)", "CHAR(8)", "DATE"]} |
| POLADM.PARTY_DOB_REDACT | REDACTION POLICY | 15_syn_dblink_grants.sql | {"column": "DATE_OF_BIRTH"} |
| POLADM.PKG_POLICY_RENEWAL | PACKAGE | 08_pkg_policy_renewal.sql | {"lines": 124} |
| POLADM.PKG_POLICY_RENEWAL.ARCHIVE_TO | PACKAGE PROCEDURE | 08_pkg_policy_renewal.sql |  |
| POLADM.PKG_POLICY_RENEWAL.BROKER_UPLIFT | PACKAGE FUNCTION | 08_pkg_policy_renewal.sql |  |
| POLADM.PKG_POLICY_RENEWAL.EXPIRING_CURSOR | PACKAGE FUNCTION | 08_pkg_policy_renewal.sql |  |
| POLADM.PKG_POLICY_RENEWAL.RENEW_EXPIRING | PACKAGE PROCEDURE | 08_pkg_policy_renewal.sql |  |
| POLADM.POLICY | TABLE | 03_tbl_policy.sql | {"constraints": 7, "temporary": false, "type_traps": ["CHAR(1)", "CHAR(3)", "CLOB", "DATE", "NUMBER"]} |
| POLADM.POLICY_AUDIT_LOG | TABLE | 05_tbl_policy_audit_log.sql | {"constraints": 1, "temporary": false, "type_traps": ["NUMBER", "TIMESTAMP(6) DEFAULT SYSTIMESTAMP NOT NULL"]} |
| POLADM.POLICY_AUDIT_POLICY_IX | INDEX | 05_tbl_policy_audit_log.sql |  |
| POLADM.POLICY_BROKER_VPD | VPD POLICY | 15_syn_dblink_grants.sql |  |
| POLADM.POLICY_EXPIRY_IX | INDEX | 03_tbl_policy.sql |  |
| POLADM.POLICY_PARTY_IX | INDEX | 03_tbl_policy.sql |  |
| POLADM.POLICY_SEQ | SEQUENCE | 01_seq_policy.sql |  |
| POLADM.PRC_LOG_EVENT | PROCEDURE | 06_prc_log_event.sql | {"lines": 21} |
| POLADM.PREMIUM_TXN | TABLE | 04_tbl_premium_txn.sql | {"constraints": 4, "temporary": false, "type_traps": ["BINARY_DOUBLE", "CHAR(3)", "DATE", "INTERVAL DAY(3) TO SECOND(0) NULL", "RAW(16)", "TIMESTAMP(6) WITH LOCAL TIME ZONE NULL", "TIMESTAMP(6) WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL"]} |
| POLADM.PREMIUM_TXN_POLICY_IX | INDEX | 04_tbl_premium_txn.sql |  |
| POLADM.PRG_NIGHTLY_RENEWAL | SCHEDULER PROGRAM | 13_job_nightly_renewal.sql | {"repeat_interval": "", "start_date": ""} |
| POLADM.STG_POLICY_FEED | TABLE | 09_mrg_policy_from_stg.sql | {"constraints": 0, "temporary": true, "type_traps": ["CHAR(1)", "DATE", "NUMBER"]} |
| POLADM.TRG_POLICY_BIU | TRIGGER | 07_trg_policy_biu.sql | {"events": "INSERT|UPDATE", "lines": 46} |
| PUBLIC.BROKER | PUBLIC SYNONYM | 15_syn_dblink_grants.sql |  |
| ROLE.ODS_READER | ROLE | 15_syn_dblink_grants.sql |  |
| ROLE.POLADM_APP | ROLE | 15_syn_dblink_grants.sql |  |
| ROLEGRANT.SOAP_SVC.ODS_READER | ROLE MEMBERSHIP | 15_syn_dblink_grants.sql |  |
| ROLEGRANT.SOAP_SVC.POLADM_APP | ROLE MEMBERSHIP | 15_syn_dblink_grants.sql |  |
| TERADATA.STG_POLICY_360 | EXTERNAL_TABLE | docs |  |

## FACT edges

| src | dst | kind | detail |
|---|---|---|---|
| POLADM.POLICY | MLOG$_POLADM.POLICY | defines-on |  |
| POLADM.PREMIUM_TXN | MLOG$_POLADM.PREMIUM_TXN | defines-on |  |
| POLADM.JOB_NIGHTLY_RENEWAL | POLADM.PRG_NIGHTLY_RENEWAL | schedules |  |
| POLADM.POLICY_BROKER_VPD | POLADM.POLICY | defines-on |  |
| POLADM.POLICY_BROKER_VPD | POLADM.FN_BROKER_PREDICATE | calls |  |
| POLADM.PARTY_DOB_REDACT | POLADM.PARTY | defines-on |  |
| POLADM.BROKER_PARENT_IX | POLADM.BROKER | defines-on |  |
| POLADM.POLICY_PARTY_IX | POLADM.POLICY | defines-on |  |
| POLADM.POLICY_EXPIRY_IX | POLADM.POLICY | defines-on |  |
| POLADM.PREMIUM_TXN_POLICY_IX | POLADM.PREMIUM_TXN | defines-on |  |
| POLADM.POLICY_AUDIT_POLICY_IX | POLADM.POLICY_AUDIT_LOG | defines-on |  |
| PUBLIC.BROKER | POLADM.BROKER | alias-of |  |
| ODS.POLICY | POLADM.POLICY | alias-of |  |
| GRANT.ODS_READER.POLADM.POLICY.SELECT | POLADM.POLICY | defines-on |  |
| GRANT.ODS_READER.POLADM.PARTY.SELECT | POLADM.PARTY | defines-on |  |
| GRANT.ODS_READER.POLADM.PREMIUM_TXN.SELECT | POLADM.PREMIUM_TXN | defines-on |  |
| GRANT.ODS_READER.ODS.MV_POLICY_PREMIUM_SUMMARY.SELECT | ODS.MV_POLICY_PREMIUM_SUMMARY | defines-on |  |
| GRANT.PUBLIC.ODS.V_BROKER_HIERARCHY.SELECT | ODS.V_BROKER_HIERARCHY | defines-on |  |
| GRANT.POLADM_APP.POLADM.POLICY.SELECT | POLADM.POLICY | defines-on |  |
| GRANT.POLADM_APP.POLADM.PKG_POLICY_RENEWAL.EXECUTE | POLADM.PKG_POLICY_RENEWAL | defines-on |  |
| GRANT.POLADM_APP.ODS.PKG_POLICY_INQUIRY.EXECUTE | ODS.PKG_POLICY_INQUIRY | defines-on |  |
| POLADM.PRC_LOG_EVENT | POLADM.POLICY_AUDIT_LOG | writes |  [INSERT] |
| POLADM.PRC_LOG_EVENT | POLADM.AUDIT_SEQ | consumes-sequence |  |
| POLADM.TRG_POLICY_BIU | POLADM.POLICY | defines-on |  [INSERT|UPDATE] |
| POLADM.TRG_POLICY_BIU | POLADM.POLICY | writes | :NEW in-flight row [INSERT|UPDATE] |
| POLADM.TRG_POLICY_BIU | POLADM.POLICY_SEQ | consumes-sequence |  |
| POLADM.TRG_POLICY_BIU | POLADM.PRC_LOG_EVENT | calls |  |
| POLADM.PKG_POLICY_RENEWAL.BROKER_UPLIFT | POLADM.BROKER | reads |  |
| POLADM.PKG_POLICY_RENEWAL.EXPIRING_CURSOR | POLADM.POLICY | reads |  |
| POLADM.PKG_POLICY_RENEWAL.RENEW_EXPIRING | POLADM.POLICY | reads |  |
| POLADM.PKG_POLICY_RENEWAL.RENEW_EXPIRING | POLADM.POLICY | writes |  [UPDATE(ANNUAL_PREMIUM,EXPIRY_DT,POLICY_STATUS)] |
| POLADM.PKG_POLICY_RENEWAL.RENEW_EXPIRING | POLADM.PREMIUM_TXN | writes |  [INSERT] |
| POLADM.PKG_POLICY_RENEWAL.RENEW_EXPIRING | POLADM.POLICY_SEQ | consumes-sequence |  |
| POLADM.PKG_POLICY_RENEWAL.RENEW_EXPIRING | POLADM.PRC_LOG_EVENT | calls |  |
| POLADM.PKG_POLICY_RENEWAL.RENEW_EXPIRING | POLADM.PKG_POLICY_RENEWAL.BROKER_UPLIFT | calls | unqualified call resolved against the census |
| POLADM.09_MRG_POLICY_FROM_STG | POLADM.STG_POLICY_FEED | reads |  |
| POLADM.09_MRG_POLICY_FROM_STG | POLADM.BROKER | reads |  |
| POLADM.09_MRG_POLICY_FROM_STG | POLADM.POLICY | writes |  [DELETE|INSERT|UPDATE(ANNUAL_PREMIUM,BROKER_ID,COVER_NOTE_REF,EXPIRY_DT,POLICY_STATUS)] |
| POLADM.09_MRG_POLICY_FROM_STG | POLADM.POLICY_SEQ | consumes-sequence |  |
| ODS.V_BROKER_HIERARCHY | POLADM.BROKER | reads |  |
| ODS.V_PREMIUM_BY_TXN_TYPE | POLADM.PREMIUM_TXN | reads |  |
| ODS.MV_POLICY_PREMIUM_SUMMARY | POLADM.POLICY | reads |  |
| ODS.MV_POLICY_PREMIUM_SUMMARY | POLADM.PREMIUM_TXN | reads |  |
| POLADM.PRG_NIGHTLY_RENEWAL | ODS.MV_POLICY_PREMIUM_SUMMARY | calls | DBMS_MVIEW.REFRESH |
| POLADM.PRG_NIGHTLY_RENEWAL | POLADM.PKG_POLICY_RENEWAL.RENEW_EXPIRING | calls |  |
| ODS.PKG_POLICY_INQUIRY.GET_POLICY_SUMMARY | ODS.ODS_POLICY_360 | reads |  |
| ODS.PKG_POLICY_INQUIRY.GET_PARTY_CLAIMS | ODS.ODS_CLAIMS | reads |  |
| POLADM.14_RPT_POLICY_PAGE | POLADM.BROKER | reads |  |
| POLADM.14_RPT_POLICY_PAGE | POLADM.POLICY | reads |  |
| POLADM.14_RPT_POLICY_PAGE | POLADM.PARTY | reads |  |
| POLADM.PKG_POLICY_RENEWAL.RENEW_EXPIRING | POLADM.POLICY_AUDIT_LOG | writes | trigger fan-out via POLADM.TRG_POLICY_BIU -> POLADM.PRC_LOG_EVENT [INSERT] |
| POLADM.PKG_POLICY_RENEWAL.RENEW_EXPIRING | POLADM.AUDIT_SEQ | consumes-sequence | trigger fan-out via POLADM.TRG_POLICY_BIU -> POLADM.PRC_LOG_EVENT |
| POLADM.09_MRG_POLICY_FROM_STG | POLADM.PRC_LOG_EVENT | calls | trigger fan-out via POLADM.TRG_POLICY_BIU |
| POLADM.09_MRG_POLICY_FROM_STG | POLADM.POLICY_AUDIT_LOG | writes | trigger fan-out via POLADM.TRG_POLICY_BIU -> POLADM.PRC_LOG_EVENT [INSERT] |
| POLADM.09_MRG_POLICY_FROM_STG | POLADM.AUDIT_SEQ | consumes-sequence | trigger fan-out via POLADM.TRG_POLICY_BIU -> POLADM.PRC_LOG_EVENT |
