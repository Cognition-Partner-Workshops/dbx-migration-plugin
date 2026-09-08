-- Run-time invocation of m_CLAIMS_FNOL_INTRADAY. Executed by task s_m_CLAIMS_FNOL_INTRADAY in converted.job.yml
-- on every scheduled run; the procedure itself is deployed by the preceding task (converted.sql). Kept separate so a
-- run can never succeed by merely (re)defining the procedure without loading anything.
-- Shape: databricks-dbsql references/sql-scripting.md "CALL (Invoke a Procedure)" (OUT argument must be a variable).
-- Table names are the $DBConnection_SRC / $DBConnection_TGT resolutions from the parameter file (INFERRED), recorded
-- in the unit brief; <migration_catalog> is the census-level placeholder used throughout this skill.
DECLARE rows_loaded INT;

CALL <migration_catalog>.claims.m_claims_fnol_intraday(
  p_src_guidewire_table  => '<migration_catalog>.raw.guidewire_cc_events',
  p_src_legacy_clm_table => '<migration_catalog>.raw.legacy_clm_extract',
  p_target_claim_table   => '<migration_catalog>.claims.claim',
  p_quarantine_table     => '<migration_catalog>.claims.claim_quarantine',
  p_rows_loaded          => rows_loaded
);

-- Surfaces $s_m_CLAIMS_FNOL_INTRADAY.TgtSuccessRows in the task output for the Tier 1 recon check.
SELECT rows_loaded AS tgt_success_rows;
