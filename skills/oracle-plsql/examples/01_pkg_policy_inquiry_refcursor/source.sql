-- Source: albion-insurance-data-estate/api_legacy/plsql/pkg_policy_inquiry.sql (read-only copy for the worked example)
CREATE OR REPLACE PACKAGE BODY pkg_policy_inquiry AS
/*******************************************************************************
 * PKG_POLICY_INQUIRY — Oracle package behind the SOAP PolicyInquiryService.
 * Consumers: broker extranet (2009), aggregator gateway (2015), the Guidewire
 * integration layer (2021, "temporary"), and an unknown number of Access DBs.
 * Reads a NIGHTLY COPY of Teradata STG_POLICY_360 replicated into Oracle via
 * a 2013-era GoldenGate config — data is up to 26h stale and callers assume
 * it is real-time.
 ******************************************************************************/
  FUNCTION get_policy_summary(p_policy_no IN VARCHAR2) RETURN SYS_REFCURSOR IS
    l_cur SYS_REFCURSOR;
  BEGIN
    OPEN l_cur FOR
      SELECT p.policy_no,
             p.party_id,
             -- Broker extranet contract expects 'CUSTOMER_REF'; we alias the
             -- legacy banking id when present, else the party id. Two id
             -- schemes leak into one API field. Consumers parse the prefix.
             NVL(TO_CHAR(p.legacy_customer_id), p.party_id) AS customer_ref,
             p.product_cd,
             p.policy_status,
             -- 'active' per FINANCE definition (this API is claims-facing...)
             p.active_policy_flag,
             p.annual_premium_gbp,
             p.postcode_dq_status
        FROM ods_policy_360 p
       WHERE p.policy_no = UPPER(TRIM(p_policy_no))
          -- bordereaux-format refs arrive here too; re-key inline (4th place)
          OR p.policy_no = REPLACE(REPLACE(UPPER(TRIM(p_policy_no)),'AL/','ALB-'),'/','-');
  END get_policy_summary;

  FUNCTION get_party_claims(p_party_ref IN VARCHAR2) RETURN SYS_REFCURSOR IS
    l_cur SYS_REFCURSOR;
  BEGIN
    OPEN l_cur FOR
      SELECT c.claim_no, c.policy_no, c.loss_dt,   -- returned as DD/MM/YYYY text
             c.claim_status, c.incurred_amt, c.paid_amt
        FROM ods_claims c
       WHERE c.claimant_party_id = p_party_ref
          OR TO_CHAR(c.legacy_client_no) = p_party_ref;  -- callers pass either key
  END get_party_claims;
END pkg_policy_inquiry;
/
