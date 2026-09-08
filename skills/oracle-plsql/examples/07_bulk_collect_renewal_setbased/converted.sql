-- Converted: fixture 08_pkg_policy_renewal.sql (package with explicit cursor + BULK COLLECT LIMIT + FORALL, package
-- state, SAVEPOINT, named exceptions, RAISE_APPLICATION_ERROR, dynamic SQL, DBMS_OUTPUT) -> DBSQL stored procedure.
-- Track: analytical/DBSQL first (SKILL.md §6 routing order: SQL Scripting / CREATE PROCEDURE before Jobs control flow,
-- PySpark last). Lakebase variant summarised at the end. Rules: §6 package, cursor, BULK COLLECT/FORALL, exceptions,
-- commit/savepoint, dynamic SQL rows; §7 traps 11, 13, 25.
-- Syntax: [dbsql:sql-scripting.md#CREATE PROCEDURE], [dbsql:sql-scripting.md#Handler Declaration],
-- [dbsql:sql-scripting.md#SIGNAL and RESIGNAL], [dbsql:sql-scripting.md#EXECUTE IMMEDIATE (Dynamic SQL)],
-- [dbsql:sql-scripting.md#SQL Scripting Atomic Blocks].

-- Package -> schema. Package constants -> literals or a one-row config table; package STATE (g_run_id, g_rows_renewed)
-- has no session equivalent in DBSQL (§7 trap 11): it becomes a row in a run-log table written by the procedure.
CREATE SCHEMA IF NOT EXISTS ${catalog}.pkg_policy_renewal;

CREATE TABLE IF NOT EXISTS ${catalog}.pkg_policy_renewal.run_log (
  run_id        BIGINT GENERATED ALWAYS AS IDENTITY,    -- g_run_id
  run_ts        TIMESTAMP,
  rows_renewed  BIGINT,                                 -- g_rows_renewed (per run; the Oracle value was a session total)
  status        STRING
);

-- FUNCTION broker_uplift: NO_DATA_FOUND -> default, TOO_MANY_ROWS -> RAISE_APPLICATION_ERROR(-20002).
-- A SQL scalar function cannot raise on duplicates, so uniqueness is a precondition asserted in the procedure below.
CREATE OR REPLACE FUNCTION ${catalog}.pkg_policy_renewal.broker_uplift(p_broker_id BIGINT)
RETURNS DECIMAL(38,10)
RETURN 1 + coalesce((SELECT max(b.commission_pct) FROM ${catalog}.poladm.broker b WHERE b.broker_id = p_broker_id), 0) / 100;
-- Oracle NUMBER arithmetic is exact to 38 digits; DECIMAL(38,10) division truncates at scale 10 (§7 trap 25): Tier 2 on
-- SUM(annual_premium) with decimal_round(2) is the agreed tolerance because the result is ROUNDed to 2 dp before storage.

-- FUNCTION expiring_cursor RETURN SYS_REFCURSOR -> table function (same pattern as example 01)
CREATE OR REPLACE FUNCTION ${catalog}.pkg_policy_renewal.expiring_cursor(p_as_of_dt TIMESTAMP_NTZ, p_horizon_days INT)
RETURNS TABLE (policy_id BIGINT, policy_no STRING, annual_premium DECIMAL(38,10), broker_id BIGINT, expiry_dt TIMESTAMP_NTZ)
RETURN
  SELECT p.policy_id, p.policy_no, p.annual_premium, p.broker_id, p.expiry_dt
    FROM ${catalog}.poladm.policy p
   WHERE p.policy_status = 'LIVE'
     AND p.expiry_dt >= p_as_of_dt                                          -- DATE compare keeps time-of-day: TIMESTAMP_NTZ both sides (§7 trap 3)
     AND p.expiry_dt <  p_as_of_dt + make_interval(0, 0, 0, p_horizon_days) -- d + n days (§5 #56)
   ORDER BY p.expiry_dt, p.policy_id;

-- PROCEDURE renew_expiring: the cursor/BULK COLLECT/FORALL loop is one set-based UPDATE + one INSERT.
-- OUT parameters cannot carry DEFAULT; the IN defaults are kept.
CREATE OR REPLACE PROCEDURE ${catalog}.pkg_policy_renewal.renew_expiring(
    IN  p_as_of_dt     TIMESTAMP_NTZ  DEFAULT date_trunc('DAY', current_timestamp())::TIMESTAMP_NTZ,   -- TRUNC(SYSDATE)
    IN  p_horizon_days INT            DEFAULT 30,
    IN  p_uplift       DECIMAL(38,10) DEFAULT 1.035,       -- c_default_uplift
    OUT p_rows_out     BIGINT,
    OUT p_status_out   STRING)
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
COMMENT 'Oracle POLADM.PKG_POLICY_RENEWAL.RENEW_EXPIRING (BULK COLLECT/FORALL loop, set-based here)'
AS BEGIN
  DECLARE premium_negative CONDITION FOR SQLSTATE '45001';                 -- e_premium_negative / -20001
  DECLARE duplicate_broker CONDITION FOR SQLSTATE '45002';                 -- RAISE_APPLICATION_ERROR(-20002)
  DECLARE l_dup_brokers BIGINT DEFAULT 0;

  -- WHEN e_premium_negative THEN ROLLBACK; p_status_out := 'NEGATIVE_PREMIUM'; log
  DECLARE EXIT HANDLER FOR premium_negative
    BEGIN
      SET p_status_out = 'NEGATIVE_PREMIUM';
      SET p_rows_out = 0;
      INSERT INTO ${catalog}.poladm.policy_audit_log (policy_id, event_cd, message, event_ts, session_user)
      VALUES (NULL, 'ERROR', 'Negative premium in run', current_timestamp(), current_user());
    END;
  -- WHEN OTHERS THEN ROLLBACK; p_status_out := 'ERR ...'; log; (swallowed, §7 trap 13). Kept swallowed because the
  -- scheduler program (example 05) checks p_status_out; the Jobs task then SIGNALs on <> 'OK'.
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
      SET p_status_out = 'ERR';
      SET p_rows_out = 0;
      INSERT INTO ${catalog}.poladm.policy_audit_log (policy_id, event_cd, message, event_ts, session_user)
      VALUES (NULL, 'ERROR', 'renew_expiring failed', current_timestamp(), current_user());
    END;

  -- TOO_MANY_ROWS guard for broker_uplift (a scalar UDF cannot raise it)
  SET l_dup_brokers = (SELECT count(*) FROM (SELECT broker_id FROM ${catalog}.poladm.broker GROUP BY broker_id HAVING count(*) > 1));
  IF l_dup_brokers > 0 THEN
    SIGNAL duplicate_broker SET MESSAGE_TEXT = 'Duplicate broker rows';
  END IF;

  -- Candidate set = the FOR UPDATE cursor (SKIP LOCKED has no equivalent: Delta has no row locks; the whole
  -- BEGIN ATOMIC block is one snapshot-isolated transaction instead [dbsql:sql-scripting.md#Isolation Levels]).
  CREATE OR REPLACE TEMPORARY VIEW renew_candidates AS
    SELECT p.policy_id,
           p.annual_premium,
           round(p.annual_premium * coalesce(p_uplift, ${catalog}.pkg_policy_renewal.broker_uplift(p.broker_id)), 2) AS new_premium
      FROM ${catalog}.poladm.policy p
     WHERE p.policy_status = 'LIVE'
       AND p.expiry_dt BETWEEN p_as_of_dt AND p_as_of_dt + make_interval(0, 0, 0, p_horizon_days);

  -- IF l_new_premium(i) < 0 THEN RAISE e_premium_negative (checked before any write, as the Oracle loop does per batch)
  IF EXISTS (SELECT 1 FROM renew_candidates WHERE new_premium < 0) THEN
    SIGNAL premium_negative SET MESSAGE_TEXT = 'Negative premium';
  END IF;

  SET p_rows_out = (SELECT count(*) FROM renew_candidates);              -- SQL%ROWCOUNT of the FORALL INSERT

  -- SAVEPOINT sp_batch / ROLLBACK TO on DUP_VAL_ON_INDEX: no savepoints in DBSQL; the two statements below are
  -- one atomic unit (both or neither), which is the whole-run equivalent of the per-batch savepoint (decision in
  -- 06_decisions.md: per-batch partial commits are not reproduced). Requires catalogManaged tables.
  BEGIN ATOMIC
    -- FORALL ... UPDATE poladm.policy
    MERGE INTO ${catalog}.poladm.policy AS t
    USING renew_candidates AS c ON t.policy_id = c.policy_id
    WHEN MATCHED THEN UPDATE SET
      t.annual_premium = c.new_premium,
      t.expiry_dt      = add_months(t.expiry_dt, 12),                     -- ADD_MONTHS month-end clamping matches (§5 #57)
      t.policy_status  = 'LIVE',
      t.row_version    = t.row_version + 1,                                -- trigger fan-out (example 04)
      t.updated_dt     = current_timestamp(),
      t.updated_by     = current_user();

    -- FORALL ... INSERT INTO poladm.premium_txn (txn_id = policy_seq.NEXTVAL -> identity column on the Delta copy)
    INSERT INTO ${catalog}.poladm.premium_txn
      (policy_id, txn_type_cd, txn_dt, effective_dt, amount, tax_amount, ccy, source_system)
    SELECT c.policy_id, 'RN', current_timestamp(), date_trunc('DAY', current_timestamp()),
           c.new_premium, round(c.new_premium * 0.12, 2), 'GBP', 'PKG_POLICY_RENEWAL'
      FROM renew_candidates c;

    -- g_run_id / g_rows_renewed package state -> run_log row (§7 trap 11)
    INSERT INTO ${catalog}.pkg_policy_renewal.run_log (run_ts, rows_renewed, status)
    VALUES (current_timestamp(), p_rows_out, 'OK');
  END;

  SET p_status_out = 'OK';
  -- DBMS_OUTPUT.PUT_LINE -> nothing (the run_log row is the trace); COMMIT -> implicit at END of the atomic block.
END;

-- PROCEDURE archive_to(p_table_name, p_policy_id): EXECUTE IMMEDIATE with a runtime table name (INFERRED lineage edge,
-- risk=dynamic-sql). DBMS_ASSERT.SIMPLE_SQL_NAME -> allow-list check before EXECUTE IMMEDIATE (no identifier-quoting
-- function is cited for DBSQL; the allow-list comes from the census, which is the only set of tables that can exist).
CREATE OR REPLACE PROCEDURE ${catalog}.pkg_policy_renewal.archive_to(IN p_table_name STRING, IN p_policy_id BIGINT)
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
AS BEGIN
  IF p_table_name NOT IN ('policy_archive', 'policy_archive_2019') THEN     -- census-derived allow-list
    SIGNAL SQLSTATE '45003' SET MESSAGE_TEXT = 'archive_to: table not in allow-list';
  END IF;
  EXECUTE IMMEDIATE
    'INSERT INTO ${catalog}.poladm.' || p_table_name ||
    ' SELECT * FROM ${catalog}.poladm.policy WHERE policy_id = ?'
    USING p_policy_id;
END;

-- Scheduler step 1 (example 05, src/sql/call_renew_expiring.sql):
--   BEGIN
--     DECLARE rows_out BIGINT; DECLARE status_out STRING;
--     CALL ${catalog}.pkg_policy_renewal.renew_expiring(p_rows_out => rows_out, p_status_out => status_out);
--     IF status_out <> 'OK' THEN SIGNAL SQLSTATE '45010' SET MESSAGE_TEXT = 'Renewal failed: ' || status_out; END IF;
--   END;

-- Lakebase (OLTP) variant: PL/pgSQL procedure [pg17:sql-createprocedure], [pg17:plpgsql-control-structures].
-- The BULK COLLECT/FORALL loop still collapses to UPDATE ... FROM + INSERT ... SELECT; FOR UPDATE SKIP LOCKED exists in
-- Postgres and is kept; SAVEPOINT/ROLLBACK TO exist [pg17:sql-savepoint] and are kept per batch if batching is retained;
-- RAISE EXCEPTION USING ERRCODE = 'P0001' replaces RAISE_APPLICATION_ERROR [pg17:plpgsql-errors-and-messages];
-- policy_seq.NEXTVAL -> nextval(); the trigger from example 04 maintains row_version/updated_*; package state ->
-- the same run_log table (or a session-scoped temp table).
