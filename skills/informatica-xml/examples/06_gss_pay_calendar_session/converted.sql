-- Converted unit: Pay_Calendar / m_Pay_Calendar_Reset_Pay_Calendar + s_Pay_Calendar_Reset_Pay_Calendar
-- (fixture: source.xml, an excerpt of legacy_shared_services/XML/wf_GSS_PAY_CALENDAR.xml)
-- Target form: DBSQL stored procedure (databricks-dbsql SKILL.md "Stored Procedure with Error Handling":
-- CREATE OR REPLACE PROCEDURE, LANGUAGE SQL, EXIT HANDLER, MERGE) deployed by one sql_task and CALLed by the next
-- (converted.call.sql) in converted.job.yml.
-- Routing: SQL-only source->filter->constant->update chain => DBSQL first (SKILL.md section 6).
--
-- Legacy pipeline (CONNECTOR rows):
--   PAY_PERIOD1 (ORA_BIIS, reader connection INFO_TARGET)
--     -> SQ_PAY_PERIOD_RESET   Source Filter: PAY_PERIOD.CURR_PP_FLAG = 'Y'   (Sql Query empty => generated SELECT)
--     -> exp_Initial           o_CURR_PP_FLAG = NULL   (default value ERROR('transformation error'))
--     -> upd_Reset_Current_PP  Update Strategy Expression = DD_UPDATE, Forward Rejected Rows = YES
--     -> RESET_PAY_PERIOD      writer connection INFO_TARGET (same DB as the source), Target load type Normal,
--                              Insert=YES, Update as Update=YES, Delete=YES, Truncate=NO
--   Session: Treat source rows as = Data driven, Commit Type = Target, Commit Interval = 10000,
--            Commit On End Of File = YES, Rollback Transactions on Errors = NO,
--            Recovery Strategy = Fail task and continue workflow.
--
-- Semantics that decide the MERGE shape (SKILL.md rows 90, 71; traps 1, 12, 25):
--   * Data driven + DD_UPDATE on every row + Update as Update=YES  => only UPDATEs reach the target; no insert path.
--   * Target key: PAY_PERIOD's key columns are not in the excerpt (TARGET element elided). PP_NUM + PP_END_YEAR is
--     INFERRED from the three connected ports; recorded in the unit brief.
--   * Commit Interval 10000 / Commit On EOF: the row set is tiny (the one 'current' period), so partial-commit
--     visibility (trap 25) cannot occur; a single MERGE is exact. No equivalent setting exists or is needed.
--   * Recovery Strategy "Fail task and continue workflow" => task max_retries 0 and the downstream link condition
--     `$s.Status = Succeeded` becomes depends_on + run_if ALL_SUCCESS in the job (converted.job.yml).
--   * Rollback Transactions on Errors = NO + Forward Rejected Rows = YES: legacy would keep committed chunks and
--     write rejects to reset_pay_period1.bad. A single-statement MERGE is atomic; the reject file has no rows to
--     receive here because the only expression is a constant (ERROR() default value can never fire).

CREATE OR REPLACE PROCEDURE <migration_catalog>.pay_calendar.s_pay_calendar_reset_pay_calendar(
  IN  p_pay_period_table STRING,        -- INFO_TARGET.PAY_PERIOD (source AND target: same connection)
  OUT p_tgt_success_rows INT            -- $s_Pay_Calendar_Reset_Pay_Calendar.TgtSuccessRows (workflow variable)
)
LANGUAGE SQL
SQL SECURITY INVOKER
BEGIN
  DECLARE v_src_rows INT DEFAULT 0;      -- $s.SrcSuccessRows
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    SET p_tgt_success_rows = -1;
    -- Failure Email session component (on_failure_mail) is a job-level email_notifications.on_failure; the
    -- %s/%b/%c placeholders (session name, start, completion) come from the job run metadata, not from here.
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Error Resetting Pay Period';
  END;

  -- Source Qualifier with Source Filter => the generated SELECT is `SELECT <connected ports> FROM PAY_PERIOD WHERE
  -- PAY_PERIOD.CURR_PP_FLAG = 'Y'` (trap 1: session-level `Sql Query` is empty, so the filter is authoritative).
  SET v_src_rows = (SELECT COUNT(*) FROM identifier(p_pay_period_table) WHERE CURR_PP_FLAG = 'Y');

  MERGE INTO identifier(p_pay_period_table) AS t
  USING (
    SELECT PP_NUM, PP_END_YEAR, CAST(NULL AS STRING) AS o_CURR_PP_FLAG     -- exp_Initial
    FROM   identifier(p_pay_period_table)
    WHERE  CURR_PP_FLAG = 'Y'                                              -- SQ_PAY_PERIOD_RESET Source Filter
  ) AS s
  ON  t.PP_NUM = s.PP_NUM AND t.PP_END_YEAR = s.PP_END_YEAR                -- INFERRED key
  WHEN MATCHED THEN UPDATE SET t.CURR_PP_FLAG = s.o_CURR_PP_FLAG;          -- DD_UPDATE, Update as Update

  -- TgtSuccessRows == SrcSuccessRows here: every filtered row matches (the source IS the target) and DD_UPDATE
  -- has no reject path, so the pre-MERGE count is the exact loaded-row count.
  SET p_tgt_success_rows = v_src_rows;
END;

-- This file only DEFINES the procedure (idempotent CREATE OR REPLACE). The run-time CALL lives in converted.call.sql
-- and is executed by the s_Pay_Calendar_Reset_Pay_Calendar task in converted.job.yml.
