-- Run-time invocation of s_Pay_Calendar_Reset_Pay_Calendar. Executed by the task of the same name in
-- converted.job.yml; the procedure is deployed by the preceding deploy task (converted.sql). Separate files so a run
-- can never report success by only redefining the procedure.
-- Shape: databricks-dbsql references/sql-scripting.md "CALL (Invoke a Procedure)" (OUT argument must be a variable).
-- INFO_TARGET.PAY_PERIOD resolves to the migration-catalog copy of the GSS schema (unit brief); <migration_catalog>
-- is the census-level placeholder used throughout this skill.
DECLARE tgt_success_rows INT;

CALL <migration_catalog>.pay_calendar.s_pay_calendar_reset_pay_calendar(
  p_pay_period_table  => '<migration_catalog>.gss.pay_period',
  p_tgt_success_rows  => tgt_success_rows
);

-- $s_Pay_Calendar_Reset_Pay_Calendar.TgtSuccessRows (post-session variable assignment) in the task output.
SELECT tgt_success_rows;
