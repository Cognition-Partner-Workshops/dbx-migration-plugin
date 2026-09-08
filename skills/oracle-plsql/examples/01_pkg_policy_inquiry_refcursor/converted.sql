-- Converted: Albion pkg_policy_inquiry -> Databricks SQL (analytical / API-serving profile).
-- Routing (SKILL.md §6): package -> one schema namespace; each SYS_REFCURSOR-returning function -> a
-- SQL table function (`CREATE FUNCTION ... RETURNS TABLE`, [docs:sql-ref-syntax-ddl-create-sql-function])
-- because the caller only ever OPENs the cursor and fetches; no OUT params, no DML.
-- Column names/types/order are the SOAP contract (Tier 4) and are kept verbatim.

CREATE SCHEMA IF NOT EXISTS ${catalog}.pkg_policy_inquiry
  COMMENT 'Oracle package PKG_POLICY_INQUIRY (SOAP PolicyInquiryService backend)';

-- FUNCTION get_policy_summary(p_policy_no IN VARCHAR2) RETURN SYS_REFCURSOR
CREATE OR REPLACE FUNCTION ${catalog}.pkg_policy_inquiry.get_policy_summary(p_policy_no STRING)
RETURNS TABLE (
  policy_no           STRING,
  party_id            STRING,
  customer_ref        STRING,      -- NVL(TO_CHAR(legacy_customer_id), party_id): two id schemes in one field
  product_cd          STRING,
  policy_status       STRING,
  active_policy_flag  STRING,
  annual_premium_gbp  DECIMAL(38,10),
  postcode_dq_status  STRING
)
COMMENT 'Oracle: OPEN l_cur FOR SELECT ... FROM ods_policy_360; 26h-stale GoldenGate copy of Teradata STG_POLICY_360'
RETURN
  SELECT p.policy_no,
         p.party_id,
         nvl(cast(p.legacy_customer_id AS STRING), p.party_id) AS customer_ref,   -- §5 #1, #27: TO_CHAR(number) default format == cast AS STRING for integers
         p.product_cd,
         p.policy_status,
         p.active_policy_flag,
         p.annual_premium_gbp,
         p.postcode_dq_status
    FROM ${catalog}.ods.ods_policy_360 p
   WHERE p.policy_no = upper(trim(p_policy_no))
      OR p.policy_no = replace(replace(upper(trim(p_policy_no)), 'AL/', 'ALB-'), '/', '-')
   ORDER BY p.policy_no;          -- Oracle returned rows in heap order; a total order is added so Tier 3 can diff

-- FUNCTION get_party_claims(p_party_ref IN VARCHAR2) RETURN SYS_REFCURSOR
CREATE OR REPLACE FUNCTION ${catalog}.pkg_policy_inquiry.get_party_claims(p_party_ref STRING)
RETURNS TABLE (
  claim_no          STRING,
  policy_no         STRING,
  loss_dt           DATE,          -- Oracle DATE; SOAP layer renders DD/MM/YYYY text (kept as DATE; format at the edge)
  claim_status      STRING,
  incurred_amt      DECIMAL(38,10),
  paid_amt          DECIMAL(38,10)
)
RETURN
  SELECT c.claim_no, c.policy_no, c.loss_dt, c.claim_status, c.incurred_amt, c.paid_amt
    FROM ${catalog}.ods.ods_claims c
   WHERE c.claimant_party_id = p_party_ref
      OR cast(c.legacy_client_no AS STRING) = p_party_ref     -- TO_CHAR(number) = text: keep the cast on the column side, do not let Databricks implicitly cast p_party_ref to a number (§7 trap 20)
   ORDER BY c.claim_no;

-- Call shape (replaces OPEN/FETCH/CLOSE on the SYS_REFCURSOR):
--   SELECT * FROM ${catalog}.pkg_policy_inquiry.get_policy_summary('al/0001234');
--
-- Lakebase variant (if the SOAP service is repointed to Lakebase instead of DBSQL): the same two bodies as
-- `CREATE FUNCTION ... RETURNS TABLE (...) LANGUAGE sql` in Postgres [pg17:sql-createfunction]; `nvl` -> `coalesce`,
-- `cast(... AS STRING)` -> `::text`. `ods_policy_360` would be a Lakebase synced table from the ODS Delta copy
-- [lakebase:references/synced-tables.md], which keeps (does not fix) the 26h freshness.
