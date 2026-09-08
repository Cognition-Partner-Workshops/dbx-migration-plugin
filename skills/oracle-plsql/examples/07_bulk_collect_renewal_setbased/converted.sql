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

-- FUNCTION broker_uplift. Three Oracle outcomes, kept apart:
--   one row            -> 1 + NVL(commission_pct, 0) / 100   (a NULL commission on an existing broker is 1.0)
--   NO_DATA_FOUND      -> c_default_uplift (1.035)            (the scalar subquery is NULL, so the OUTER coalesce applies)
--   TOO_MANY_ROWS      -> RAISE_APPLICATION_ERROR(-20002)     (a SQL scalar function cannot raise it: the procedure below
--                                                              asserts uniqueness for the brokers it will call this for)
CREATE OR REPLACE FUNCTION ${catalog}.pkg_policy_renewal.broker_uplift(p_broker_id BIGINT)
RETURNS DECIMAL(38,10)
RETURN coalesce((SELECT 1 + coalesce(b.commission_pct, 0) / 100
                   FROM ${catalog}.poladm.broker b
                  WHERE b.broker_id = p_broker_id),
                1.035);                                        -- c_default_uplift
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

-- PROCEDURE renew_expiring: the cursor/BULK COLLECT/FORALL loop is one set-based UPDATE + one INSERT, plus the row-level
-- side effects of TRG_POLICY_BIU (BEFORE UPDATE on poladm.policy, fixture 07) that Oracle applies to every updated row and
-- Delta has no trigger for: row_version/updated_*, policy_no re-normalisation, active_policy_flag recomputed from the NEW
-- values, and one policy_audit_log row per policy whose status or premium changed (via prc_log_event / audit_seq).
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

  -- Candidate set = the FOR UPDATE cursor (SKIP LOCKED has no equivalent: Delta has no row locks; the whole
  -- BEGIN ATOMIC block is one snapshot-isolated transaction instead [dbsql:sql-scripting.md#Isolation Levels]).
  -- Materialised as a TEMP TABLE, not a TEMP VIEW: a temp view is re-executed on every access
  -- [dbsql:materialized-views-pipes.md#Temporary Tables vs Temporary Views], so after the MERGE below it would re-read
  -- poladm.policy and no longer see the rows it just renewed (the audit INSERT and premium_txn INSERT would be empty).
  -- The :OLD values the trigger compares against (policy_status, annual_premium) are captured here, before any write.
  DROP TABLE IF EXISTS renew_candidates;
  CREATE TEMPORARY TABLE renew_candidates AS
    SELECT p.policy_id,
           p.policy_status,                                   -- :OLD.policy_status (always 'LIVE' here)
           p.annual_premium,                                  -- :OLD.annual_premium
           p.broker_id,
           cast(NULL AS DECIMAL(38,10))    AS new_premium
      FROM ${catalog}.poladm.policy p
     WHERE p.policy_status = 'LIVE'
       AND p.expiry_dt BETWEEN p_as_of_dt AND p_as_of_dt + make_interval(0, 0, 0, p_horizon_days);

  -- TOO_MANY_ROWS guard for broker_uplift (a scalar UDF cannot raise it). Same scope as the Oracle calls: the PL/SQL
  -- NVL(p_uplift, broker_uplift(..)) evaluates both actual parameters before NVL runs, so the function is called for every
  -- candidate broker even when p_uplift is supplied; the guard therefore covers candidate brokers unconditionally.
  SET l_dup_brokers = (SELECT count(*)
                         FROM (SELECT b.broker_id
                                 FROM ${catalog}.poladm.broker b
                                WHERE b.broker_id IN (SELECT c.broker_id FROM renew_candidates c)
                                GROUP BY b.broker_id
                               HAVING count(*) > 1));
  IF l_dup_brokers > 0 THEN
    SIGNAL duplicate_broker SET MESSAGE_TEXT = 'Duplicate broker rows';   -- caught by the SQLEXCEPTION handler -> 'ERR', as WHEN OTHERS does for -20002
  END IF;

  -- l_new_premium(i) := ROUND(annual_premium * NVL(p_uplift, broker_uplift(broker_id)), 2)
  UPDATE renew_candidates
     SET new_premium = round(annual_premium * coalesce(p_uplift, ${catalog}.pkg_policy_renewal.broker_uplift(broker_id)), 2);

  -- IF l_new_premium(i) < 0 THEN RAISE e_premium_negative (checked before any write, as the Oracle loop does per batch)
  IF EXISTS (SELECT 1 FROM renew_candidates WHERE new_premium < 0) THEN
    SIGNAL premium_negative SET MESSAGE_TEXT = 'Negative premium';
  END IF;

  SET p_rows_out = (SELECT count(*) FROM renew_candidates);              -- SQL%ROWCOUNT of the FORALL INSERT

  -- SAVEPOINT sp_batch / ROLLBACK TO on DUP_VAL_ON_INDEX: no savepoints in DBSQL; the two statements below are
  -- one atomic unit (both or neither), which is the whole-run equivalent of the per-batch savepoint (decision in
  -- 06_decisions.md: per-batch partial commits are not reproduced). Requires catalogManaged tables.
  BEGIN ATOMIC
    -- FORALL ... UPDATE poladm.policy, with the BEFORE UPDATE trigger body folded in (fixture 07_trg_policy_biu.sql)
    MERGE INTO ${catalog}.poladm.policy AS t
    USING renew_candidates AS c ON t.policy_id = c.policy_id
    WHEN MATCHED THEN UPDATE SET
      t.annual_premium     = c.new_premium,
      t.expiry_dt          = add_months(t.expiry_dt, 12),                 -- ADD_MONTHS month-end clamping matches (§5 #57)
      t.policy_status      = 'LIVE',
      -- trigger, UPDATING branch:
      t.row_version        = coalesce(t.row_version, 0) + 1,              -- NVL(:OLD.row_version, 0) + 1
      t.updated_dt         = current_timestamp(),                         -- SYSDATE
      t.updated_by         = current_user(),                              -- SYS_CONTEXT('USERENV','SESSION_USER')
      t.policy_no          = replace(upper(trim(t.policy_no)), 'AL/', 'ALB-'),   -- re-normalised on every row the trigger sees
      -- :NEW.active_policy_flag from the NEW status ('LIVE') and the NEW expiry (+12 months), TRUNC(date) compares
      t.active_policy_flag = CASE WHEN current_date() BETWEEN to_date(t.inception_dt) AND to_date(add_months(t.expiry_dt, 12))
                                  THEN 'Y' ELSE 'N' END;

    -- trigger, prc_log_event branch: fires when NVL(:OLD.policy_status,'~') <> :NEW.policy_status OR
    -- NVL(:OLD.annual_premium,-1) <> :NEW.annual_premium (so an uplift of exactly 1.0 logs nothing, as in Oracle).
    -- audit_id = audit_seq.NEXTVAL -> identity column on the Delta copy (§7 trap 10). PRAGMA AUTONOMOUS_TRANSACTION
    -- difference accepted in example 04: these rows are part of the atomic block, so a failed run has none of them.
    INSERT INTO ${catalog}.poladm.policy_audit_log
      (policy_id, event_cd, old_status, new_status, old_premium, new_premium, event_ts, session_user)
    SELECT c.policy_id, 'UPDATE', c.policy_status, 'LIVE', c.annual_premium, c.new_premium, current_timestamp(), current_user()
      FROM renew_candidates c
     WHERE coalesce(c.policy_status, '~') <> 'LIVE'
        OR coalesce(c.annual_premium, -1) <> c.new_premium;

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
-- policy_seq.NEXTVAL -> nextval(); the trigger from example 04 fires on the UPDATE and maintains row_version/updated_*/
-- active_policy_flag/policy_no and writes the audit rows itself, so NONE of the folded-in trigger columns or the audit
-- INSERT above are repeated on Lakebase; package state -> the same run_log table (or a session-scoped temp table).
