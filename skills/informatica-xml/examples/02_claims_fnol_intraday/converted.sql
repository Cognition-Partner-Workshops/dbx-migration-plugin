-- Converted unit: INS_CLAIMS / m_CLAIMS_FNOL_INTRADAY  (fixture: source.xml)
-- Target form: a DBSQL stored procedure (databricks-dbsql SKILL.md "Stored Procedure with Error Handling":
-- CREATE OR REPLACE PROCEDURE ... LANGUAGE SQL, identifier(p_name) for parameterised table names, EXIT HANDLER)
-- invoked with CALL from a Lakeflow Jobs sql_task every 30 minutes (converted.job.yml).
-- The export has no SOURCE/TARGET/CONNECTOR rows for this mapping (only the four transformations and the
-- description), so the two source pipes and the target CLAIMS_DB.CLAIM are INFERRED from the DESCRIPTION text.
--
-- Legacy shape: Guidewire CC events  --\
--                                       UN_CLAIM_SOURCES (Union) -> EXP_CLAIM_DATES / EXP_CLAIM_FLAGS / EXP_CLAIM_STATUS -> CLAIMS_DB.CLAIM
--               LEGACY_CLM extract   --/

-- Expression translation (SKILL.md section 5):
--   IIF(c, a, b)                       -> CASE WHEN c THEN a ELSE b END                       (row 1)
--   TO_DATE(s, 'MM/DD/YYYY')           -> to_timestamp(s, 'MM/dd/yyyy')                       (row 9; token map)
--   TO_DATE row error on bad input     -> try_to_timestamp(...) IS NULL  => quarantine        (row 9, row 61)
--   DECODE(v, s1, r1, ..., NULL)       -> CASE v WHEN s1 THEN r1 ... ELSE NULL END            (row 3)
--   DECODE(v, ..., v)  (pass-through)  -> CASE v WHEN ... ELSE v END                          (row 3)
--   Union transformation               -> UNION ALL (never UNION)                             (row 73)

CREATE OR REPLACE PROCEDURE <migration_catalog>.claims.m_claims_fnol_intraday(
  IN p_src_guidewire_table STRING,      -- $DBConnection_SRC pipe 1 (INFERRED)
  IN p_src_legacy_clm_table STRING,     -- $DBConnection_SRC pipe 2 (INFERRED)
  IN p_target_claim_table STRING,       -- CLAIMS_DB.CLAIM via $DBConnection_TGT
  IN p_quarantine_table STRING,         -- replaces $BadFileName
  OUT p_rows_loaded INT
)
LANGUAGE SQL
SQL SECURITY INVOKER
BEGIN
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    SET p_rows_loaded = -1;
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'm_CLAIMS_FNOL_INTRADAY failed';   -- session failure -> job task failure
  END;

  CREATE OR REPLACE TEMPORARY VIEW un_claim_sources AS
  SELECT claim_id, source_system AS in_SOURCE_SYSTEM, loss_dt_raw AS in_LOSS_DT_RAW,
         fraud_ind AS in_FRAUD_IND, status_raw AS in_STATUS_RAW, event_ts
  FROM   identifier(p_src_guidewire_table)          -- $DBConnection_SRC pipe 1 (INFERRED)
  UNION ALL                                        -- Union never deduplicates
  SELECT claim_id, source_system, loss_dt_raw, fraud_ind, status_raw, event_ts
  FROM   identifier(p_src_legacy_clm_table);        -- $DBConnection_SRC pipe 2 (INFERRED)

  -- The three Expression transformations run row-by-row on the Union output, so they are converted as ONE pass over
  -- un_claim_sources. claim_id is not unique after a UNION ALL (the same claim can arrive as several events, or from
  -- both pipes); re-joining derived views on claim_id would multiply rows, so no derived view is ever joined back.
  --
  -- EXP_CLAIM_DATES. The legacy expression is reproduced AS WRITTEN, including the documented defect
  -- (Guidewire sends DD/MM/YYYY since 2023, mapping parses MM/DD/YYYY; INC0067812). Fixing the format here would make
  -- the converted store disagree with the legacy store during parallel run; the fix is a recorded decision, not a
  -- conversion (SKILL.md trap 6).
  -- EXP_CLAIM_FLAGS: DECODE with explicit NULL default; 'S' -> 'N' reproduced as written (BTEQ path maps 'S' -> 'Y':
  -- estate finding, not conversion work).
  -- EXP_CLAIM_STATUS: DECODE whose default is the input port itself.
  CREATE OR REPLACE TEMPORARY VIEW exp_claim_derived AS
  SELECT claim_id, in_SOURCE_SYSTEM, in_LOSS_DT_RAW, in_FRAUD_IND, in_STATUS_RAW, event_ts,
         CASE WHEN in_SOURCE_SYSTEM = 'GUIDEWIRE_CC'
              THEN try_to_timestamp(in_LOSS_DT_RAW, 'MM/dd/yyyy')
              ELSE try_to_timestamp(in_LOSS_DT_RAW, 'dd/MM/yyyy')
         END AS out_LOSS_DT,
         -- legacy: an unparsable string is a ROW ERROR (row skipped to the reject file), not a NULL
         (in_LOSS_DT_RAW IS NOT NULL AND
          CASE WHEN in_SOURCE_SYSTEM = 'GUIDEWIRE_CC'
               THEN try_to_timestamp(in_LOSS_DT_RAW, 'MM/dd/yyyy')
               ELSE try_to_timestamp(in_LOSS_DT_RAW, 'dd/MM/yyyy') END IS NULL) AS loss_dt_row_error,
         CASE in_FRAUD_IND WHEN 'Y' THEN 'Y' WHEN 'S' THEN 'N' WHEN 'N' THEN 'N' ELSE NULL END AS out_FRAUD_FLAG,
         CASE in_STATUS_RAW WHEN 'O' THEN 'OPEN' WHEN 'S' THEN 'CLOSED' WHEN 'R' THEN 'REOPENED'
                            WHEN 'D' THEN 'DECLINED' ELSE in_STATUS_RAW END AS out_CLAIM_STATUS
  FROM un_claim_sources;

  -- Reject rows go to a quarantine table instead of $BadFileName (SKILL.md section 6 "Row error handling").
  INSERT INTO identifier(p_quarantine_table)
  SELECT claim_id, in_SOURCE_SYSTEM, in_LOSS_DT_RAW, in_FRAUD_IND, in_STATUS_RAW, event_ts,
         'TO_DATE row error' AS reject_reason, current_timestamp() AS rejected_at
  FROM   exp_claim_derived
  WHERE  loss_dt_row_error;

  -- Target load. The session's 'Treat source rows as' is not in this export: INSERT is INFERRED from the intraday
  -- micro-batch description. If the live session says 'Data driven' this becomes a MERGE (section 5 row 90).
  INSERT INTO identifier(p_target_claim_table)
          (CLAIM_ID, SOURCE_SYSTEM, LOSS_DT, FRAUD_FLAG, CLAIM_STATUS, EVENT_TS, LOAD_TS)
  SELECT claim_id, in_SOURCE_SYSTEM, out_LOSS_DT, out_FRAUD_FLAG, out_CLAIM_STATUS, event_ts,
         current_timestamp()
  FROM   exp_claim_derived
  WHERE  NOT loss_dt_row_error;

  SET p_rows_loaded = (SELECT COUNT(*) FROM exp_claim_derived WHERE NOT loss_dt_row_error);
END;

-- Invoked by the sql_task in converted.job.yml:
-- CALL <migration_catalog>.claims.m_claims_fnol_intraday('<cat>.raw.guidewire_cc_events', '<cat>.raw.legacy_clm_extract',
--                                                         '<cat>.claims.claim', '<cat>.claims.claim_quarantine', ?);
