-- Object class: SYNONYM, DATABASE LINK, GRANT, VPD POLICY, REDACTION POLICY.
-- Census keys: PUBLIC.BROKER (synonym), ODS.POLICY (synonym), POLADM.CLAIMS_LINK (db link),
--              ODS.V_CLAIMS_REMOTE (view over db link), POLADM.POLICY_BROKER_VPD (policy),
--              POLADM.PARTY_DOB_REDACT (redaction policy).
-- Governance inventory objects for section 9. Read-only: the factory records these, it never
-- recreates them on Oracle and never grants on customer production catalogs.

CREATE OR REPLACE PUBLIC SYNONYM broker FOR poladm.broker;              -- consumed by ods.v_broker_hierarchy
CREATE OR REPLACE SYNONYM ods.policy  FOR poladm.policy;                -- ODS reads POLADM through synonym

-- External reference: claims live in a different database (the GoldenGate-fed CLAIMS instance)
CREATE DATABASE LINK poladm.claims_link
  CONNECT TO claims_reader IDENTIFIED BY VALUES ':1'                     -- password never in source control
  USING 'CLAIMSPRD';

CREATE OR REPLACE VIEW ods.v_claims_remote AS
SELECT c.claim_no, c.policy_no, c.claim_status, c.incurred_amt, c.notified_dt
  FROM claims.claim@claims_link c;                                      -- INFERRED edge: target of db link unresolved

-- Grants (DBA_TAB_PRIVS): role-based read for reporting, PUBLIC read on the hierarchy view
CREATE ROLE ods_reader;
CREATE ROLE poladm_app;
GRANT SELECT ON poladm.policy                 TO ods_reader;
GRANT SELECT ON poladm.party                  TO ods_reader;
GRANT SELECT ON poladm.premium_txn            TO ods_reader;
GRANT SELECT ON ods.mv_policy_premium_summary TO ods_reader;
GRANT SELECT ON ods.v_broker_hierarchy        TO PUBLIC;                -- PUBLIC grant: GAP candidate
GRANT SELECT (policy_no, policy_status, expiry_dt) ON poladm.policy TO poladm_app;  -- column-level grant
GRANT EXECUTE ON poladm.pkg_policy_renewal    TO poladm_app;
GRANT EXECUTE ON ods.pkg_policy_inquiry       TO poladm_app;  -- Albion package (api_legacy/plsql)
GRANT ods_reader TO soap_svc;
GRANT poladm_app TO soap_svc;

-- VPD / FGAC: brokers see only their own subtree
CREATE OR REPLACE FUNCTION poladm.fn_broker_predicate(p_schema IN VARCHAR2, p_obj IN VARCHAR2)
RETURN VARCHAR2 AS
BEGIN
  IF SYS_CONTEXT('USERENV','SESSION_USER') IN ('POLADM','SOAP_SVC') THEN
    RETURN NULL;                                                        -- no restriction
  END IF;
  RETURN 'broker_id IN (SELECT broker_id FROM ods.v_broker_hierarchy '
      || 'WHERE region_ref = SYS_CONTEXT(''ALBION_CTX'',''BROKER_REGION''))';
END fn_broker_predicate;
/

BEGIN
  DBMS_RLS.ADD_POLICY(
    object_schema   => 'POLADM',
    object_name     => 'POLICY',
    policy_name     => 'POLICY_BROKER_VPD',
    function_schema => 'POLADM',
    policy_function => 'FN_BROKER_PREDICATE',
    statement_types => 'SELECT',
    policy_type     => DBMS_RLS.CONTEXT_SENSITIVE);
END;
/

-- Data Redaction: partial mask on date of birth for everyone except POLADM
BEGIN
  DBMS_REDACT.ADD_POLICY(
    object_schema => 'POLADM',
    object_name   => 'PARTY',
    policy_name   => 'PARTY_DOB_REDACT',
    column_name   => 'DATE_OF_BIRTH',
    function_type => DBMS_REDACT.PARTIAL,
    function_parameters => 'm1d1y',                                     -- keep year, mask month/day
    expression    => q'[SYS_CONTEXT('USERENV','SESSION_USER') <> 'POLADM']');
END;
/
