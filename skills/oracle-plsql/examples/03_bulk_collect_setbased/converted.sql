-- Databricks SQL scripting (analytical track). Cites: [dbsql:sql-scripting.md#CREATE PROCEDURE], [#Handler Declaration],
-- [#SIGNAL and RESIGNAL], [#EXECUTE IMMEDIATE (Dynamic SQL)], [#SQL Scripting Atomic Blocks], [dbsql:materialized-views-pipes.md#Temporary Tables].
-- Package -> schema; package state (g_run_id) -> run_log row; the BEFORE UPDATE trigger of example 02 is folded in.
CREATE SCHEMA IF NOT EXISTS ${catalog}.pkg_policy_renewal;
CREATE TABLE IF NOT EXISTS ${catalog}.pkg_policy_renewal.run_log (
  run_id BIGINT GENERATED ALWAYS AS IDENTITY, run_ts TIMESTAMP, rows_renewed BIGINT, status STRING);

-- broker_uplift: one row -> 1 + NVL(pct,0)/100; NO_DATA_FOUND -> 1.035 (outer coalesce); TOO_MANY_ROWS cannot be
-- raised by a scalar SQL function, so the caller asserts uniqueness first.
CREATE OR REPLACE FUNCTION ${catalog}.pkg_policy_renewal.broker_uplift(p_broker_id BIGINT) RETURNS DECIMAL(38,10)
RETURN coalesce((SELECT 1 + coalesce(b.commission_pct, 0) / 100
                   FROM ${catalog}.poladm.broker b WHERE b.broker_id = p_broker_id), 1.035);

CREATE OR REPLACE PROCEDURE ${catalog}.pkg_policy_renewal.renew_expiring(
    IN  p_as_of_dt     TIMESTAMP_NTZ  DEFAULT date_trunc('DAY', current_timestamp())::TIMESTAMP_NTZ,  -- TRUNC(SYSDATE)
    IN  p_horizon_days INT            DEFAULT 30,
    IN  p_uplift       DECIMAL(38,10) DEFAULT 1.035,
    OUT p_rows_out     BIGINT,                          -- OUT parameters take no DEFAULT
    OUT p_status_out   STRING)
LANGUAGE SQL SQL SECURITY INVOKER MODIFIES SQL DATA
AS BEGIN
  DECLARE premium_negative CONDITION FOR SQLSTATE '45001';      -- e_premium_negative / -20001
  DECLARE duplicate_broker CONDITION FOR SQLSTATE '45002';      -- RAISE_APPLICATION_ERROR(-20002)
  DECLARE EXIT HANDLER FOR premium_negative BEGIN               -- WHEN e_premium_negative: ROLLBACK is implicit
    SET p_status_out = 'NEGATIVE_PREMIUM'; SET p_rows_out = 0;
    INSERT INTO ${catalog}.poladm.policy_audit_log (event_cd, message, event_ts) VALUES ('ERROR', 'Negative premium', current_timestamp());
  END;
  DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN                   -- WHEN OTHERS, still swallowed: the Jobs task SIGNALs on <> 'OK'
    SET p_status_out = 'ERR'; SET p_rows_out = 0;
    INSERT INTO ${catalog}.poladm.policy_audit_log (event_cd, message, event_ts) VALUES ('ERROR', 'renew_expiring failed', current_timestamp());
  END;

  -- Cursor FOR UPDATE SKIP LOCKED -> candidate set in a TEMP TABLE (a temp view would be re-executed after the MERGE
  -- and lose the :OLD values). No row locks on Delta: the atomic block below is one snapshot transaction instead.
  DROP TABLE IF EXISTS renew_candidates;
  CREATE TEMPORARY TABLE renew_candidates AS
    SELECT p.policy_id, p.policy_status, p.annual_premium, p.broker_id, cast(NULL AS DECIMAL(38,10)) AS new_premium
      FROM ${catalog}.poladm.policy p
     WHERE p.policy_status = 'LIVE'
       AND p.expiry_dt BETWEEN p_as_of_dt AND p_as_of_dt + make_interval(0, 0, 0, p_horizon_days);   -- DATE + n days

  IF EXISTS (SELECT 1 FROM ${catalog}.poladm.broker b
              WHERE b.broker_id IN (SELECT broker_id FROM renew_candidates) GROUP BY b.broker_id HAVING count(*) > 1) THEN
    SIGNAL duplicate_broker SET MESSAGE_TEXT = 'Duplicate broker rows';   -- TOO_MANY_ROWS parity
  END IF;
  UPDATE renew_candidates                                                 -- the per-row loop, set-based
     SET new_premium = round(annual_premium * coalesce(p_uplift, ${catalog}.pkg_policy_renewal.broker_uplift(broker_id)), 2);
  IF EXISTS (SELECT 1 FROM renew_candidates WHERE new_premium < 0) THEN SIGNAL premium_negative SET MESSAGE_TEXT = 'Negative premium'; END IF;
  SET p_rows_out = (SELECT count(*) FROM renew_candidates);                -- SQL%ROWCOUNT

  -- SAVEPOINT/ROLLBACK TO per batch -> one atomic block for the whole run (decision: no partial batch commits).
  BEGIN ATOMIC
    MERGE INTO ${catalog}.poladm.policy AS t USING renew_candidates AS c ON t.policy_id = c.policy_id   -- FORALL UPDATE
    WHEN MATCHED THEN UPDATE SET
      t.annual_premium = c.new_premium, t.expiry_dt = add_months(t.expiry_dt, 12),
      t.row_version = coalesce(t.row_version, 0) + 1, t.updated_dt = current_timestamp(), t.updated_by = current_user(),
      t.active_policy_flag = CASE WHEN current_date() BETWEEN to_date(t.inception_dt) AND to_date(add_months(t.expiry_dt, 12))
                                  THEN 'Y' ELSE 'N' END;                                              -- trigger UPDATING branch
    INSERT INTO ${catalog}.poladm.policy_audit_log (policy_id, event_cd, old_premium, new_premium, event_ts)   -- trigger's prc_log_event
    SELECT c.policy_id, 'UPDATE', c.annual_premium, c.new_premium, current_timestamp()
      FROM renew_candidates c WHERE coalesce(c.annual_premium, -1) <> c.new_premium;
    INSERT INTO ${catalog}.poladm.premium_txn (policy_id, txn_type_cd, txn_dt, amount, source_system)  -- FORALL INSERT
    SELECT c.policy_id, 'RN', current_timestamp(), c.new_premium, 'PKG_POLICY_RENEWAL' FROM renew_candidates c;
    INSERT INTO ${catalog}.pkg_policy_renewal.run_log (run_ts, rows_renewed, status) VALUES (current_timestamp(), p_rows_out, 'OK');
  END;
  SET p_status_out = 'OK';                                                 -- COMMIT implicit; DBMS_OUTPUT dropped (run_log is the trace)
END;

-- archive_to: EXECUTE IMMEDIATE with a runtime table name. DBMS_ASSERT -> census-derived allow-list before the call.
CREATE OR REPLACE PROCEDURE ${catalog}.pkg_policy_renewal.archive_to(IN p_table_name STRING, IN p_policy_id BIGINT)
LANGUAGE SQL SQL SECURITY INVOKER MODIFIES SQL DATA AS BEGIN
  IF p_table_name NOT IN ('policy_archive', 'policy_archive_2019') THEN
    SIGNAL SQLSTATE '45003' SET MESSAGE_TEXT = 'archive_to: table not in allow-list';
  END IF;
  EXECUTE IMMEDIATE 'INSERT INTO ${catalog}.poladm.' || p_table_name
                 || ' SELECT * FROM ${catalog}.poladm.policy WHERE policy_id = ?' USING p_policy_id;
END;
-- Lakebase variant: PL/pgSQL keeps FOR UPDATE SKIP LOCKED and SAVEPOINT; RAISE EXCEPTION USING ERRCODE replaces
-- RAISE_APPLICATION_ERROR; nextval() stays; the example 02 trigger fires, so no trigger columns/audit rows are folded in.
