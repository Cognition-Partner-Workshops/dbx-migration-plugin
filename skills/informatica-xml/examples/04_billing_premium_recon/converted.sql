-- Converted unit: FIN_BILLING / m_BILLING_PREMIUM_RECON  (fixture: source.xml, source.par)
-- Target form: DBSQL materialized view + quarantine table, refreshed by a Lakeflow Job scheduled monthly
-- (databricks-dbsql SKILL.md "Materialized View with Scheduled Refresh" for the MV shape; the schedule itself is
-- owned by the job in converted.job.yml because Control-M WD1 (first working day) is a business-day calendar that a
-- plain SCHEDULE EVERY cannot express - SKILL.md section 6 "Control-M / cron edges").
--
-- The export carries only three transformations; BILLING_DB.PREMIUM_TRANSACTIONS (source) and the target are named in
-- the LKP_APF_ACCOUNTS description and are INFERRED. Lookup condition is APF_ACCOUNT_ID = ACCOUNT_ID (description).

-- Session-level flag not in the export: 'Enable high precision'. The decimal(12,2) ports with `* 0.12` and `/ 24`
-- behave differently under high precision ON (decimal arithmetic) and OFF (double, 15 significant digits).
-- Converted code uses DECIMAL arithmetic (ON). If the live session is OFF, Tier 2 sums drift at the 1e-2 level and
-- the fix is a recorded decision plus decimal_round(places=2) for these two columns only (SKILL.md trap 3).

CREATE OR REPLACE MATERIALIZED VIEW <migration_catalog>.finance.billing_premium_recon
  COMMENT 'm_BILLING_PREMIUM_RECON converted; grain = TRANSACTION_ID; monthly'
AS
WITH src AS (
  SELECT t.TRANSACTION_ID, t.POLICY_NO, t.APF_ACCOUNT_ID, t.ANNUAL_PREMIUM, t.INCEPTION_DT, t.GROSS_AMT
  FROM   <migration_catalog>.billing.premium_transactions t          -- BILLING_DB.PREMIUM_TRANSACTIONS via $DBConnection_SRC
),
-- LKP_APF_ACCOUNTS: connected, cached, reusable (shared object). Multiple-match policy is not exported; ACCOUNT_ID is
-- the banking primary key so a plain LEFT JOIN is exact (SKILL.md row 74).
lkp AS (
  SELECT ACCOUNT_ID, ACCOUNT_STATUS, CUSTOMER_ID
  FROM   <migration_catalog>.core_banking.accounts                   -- CORE_BANKING_DB.ACCOUNTS via $DBConnection_APF
),
exp_earned_24ths AS (
  SELECT TRANSACTION_ID,
         -- v_MONTHS_ON_RISK (integer port) = DATE_DIFF(SYSDATE, in_INCEPTION_DT, 'MM'):
         --   DATE_DIFF returns a DOUBLE (fractional months); assigning it to an INTEGER port ROUNDS (not truncates)
         --   in Informatica (SKILL.md rows 14, 23; trap 23). months_between() is the Spark equivalent of 'MM'.
         --   SYSDATE = Integration Service local time -> current_timestamp() in the job's timezone (row 29, trap 7).
         CAST(round(months_between(current_timestamp(), INCEPTION_DT)) AS INT) AS v_MONTHS_ON_RISK,
         ANNUAL_PREMIUM
  FROM src
),
exp_earned_24ths_out AS (
  SELECT TRANSACTION_ID,
         -- in_ANNUAL_PREMIUM * LEAST(24, GREATEST(0, v * 2 + 1)) / 24
         -- Informatica LEAST/GREATEST return NULL if ANY argument is NULL; Spark ignores NULLs (row 58). v is NULL
         -- when INCEPTION_DT is NULL, so the NULL guard is explicit.
         CAST(CASE WHEN v_MONTHS_ON_RISK IS NULL THEN NULL
                   ELSE ANNUAL_PREMIUM * least(24, greatest(0, v_MONTHS_ON_RISK * 2 + 1)) / 24
              END AS DECIMAL(12,2))                                    AS out_EARNED_PREMIUM_24
  FROM exp_earned_24ths
),
exp_ipt_recalc AS (
  -- ROUND(in_GROSS_AMT * 0.12, 2): both engines round half away from zero (row 16). The rate is reproduced as the
  -- hardcoded literal because that is what the legacy target holds; $$IPT_RATE=0.12 in the .par is unused by the
  -- mapping (estate finding, see NOTE.md). Changing to a parameter or POLICY.IPT_RATE is a decision.
  SELECT TRANSACTION_ID, CAST(round(GROSS_AMT * 0.12, 2) AS DECIMAL(12,2)) AS out_IPT_EXPECTED
  FROM src
)
SELECT s.TRANSACTION_ID, s.POLICY_NO, s.APF_ACCOUNT_ID,
       l.ACCOUNT_STATUS  AS APF_ACCOUNT_STATUS,
       l.CUSTOMER_ID     AS APF_CUSTOMER_ID,
       e.out_EARNED_PREMIUM_24 AS EARNED_PREMIUM_24,
       i.out_IPT_EXPECTED      AS IPT_EXPECTED,
       current_timestamp()     AS LOAD_TS                              -- audit; excluded from Tier 3
FROM src s
JOIN exp_earned_24ths_out e USING (TRANSACTION_ID)
JOIN exp_ipt_recalc i       USING (TRANSACTION_ID)
LEFT JOIN lkp l ON l.ACCOUNT_ID = s.APF_ACCOUNT_ID
WHERE l.ACCOUNT_ID IS NOT NULL;      -- unmatched rows go to the quarantine table below, as the legacy bad file did

-- $OutputFile_BAD_APF=/interface/outbound/finance/BAD_APF_MATCH_$$RUNMONTH.csv -> quarantine table keyed by run month.
-- (~3% orphans monthly, "never re-processed": the table makes them visible; re-processing is a decision.)
CREATE OR REPLACE MATERIALIZED VIEW <migration_catalog>.finance.billing_premium_recon_bad_apf_match
  COMMENT 'Rows whose APF_ACCOUNT_ID has no CORE_BANKING_DB.ACCOUNTS match; replaces BAD_APF_MATCH_<RUNMONTH>.csv'
AS
SELECT s.*, date_format(current_date(), 'yyyyMM') AS RUNMONTH, 'NO_APF_ACCOUNT' AS reject_reason
FROM   <migration_catalog>.billing.premium_transactions s
LEFT JOIN <migration_catalog>.core_banking.accounts a ON a.ACCOUNT_ID = s.APF_ACCOUNT_ID
WHERE  a.ACCOUNT_ID IS NULL;
