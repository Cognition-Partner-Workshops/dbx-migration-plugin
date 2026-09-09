-- Oracle package: package state, named exception, explicit cursor FOR UPDATE SKIP LOCKED, BULK COLLECT LIMIT,
-- FORALL, SAVEPOINT/ROLLBACK TO, NO_DATA_FOUND/TOO_MANY_ROWS, RAISE_APPLICATION_ERROR, dynamic SQL, DBMS_OUTPUT.

CREATE OR REPLACE PACKAGE poladm.pkg_policy_renewal AS
  c_default_uplift   CONSTANT NUMBER := 1.035;                    -- NUMBER without scale
  g_run_id           NUMBER;                                      -- package state, lives for the session
  e_premium_negative EXCEPTION;
  PRAGMA EXCEPTION_INIT(e_premium_negative, -20001);
  TYPE t_id_tab  IS TABLE OF poladm.policy.policy_id%TYPE      INDEX BY PLS_INTEGER;
  TYPE t_num_tab IS TABLE OF poladm.policy.annual_premium%TYPE INDEX BY PLS_INTEGER;

  PROCEDURE renew_expiring(p_as_of_dt IN DATE DEFAULT TRUNC(SYSDATE), p_horizon_days IN NUMBER DEFAULT 30,
                           p_uplift IN NUMBER DEFAULT c_default_uplift,
                           p_rows_out OUT NUMBER, p_status_out OUT VARCHAR2);
  FUNCTION broker_uplift(p_broker_id IN NUMBER) RETURN NUMBER;
END pkg_policy_renewal;
/
CREATE OR REPLACE PACKAGE BODY poladm.pkg_policy_renewal AS
  PROCEDURE archive_to(p_table_name IN VARCHAR2, p_policy_id IN NUMBER) IS BEGIN
    EXECUTE IMMEDIATE 'INSERT INTO poladm.' || DBMS_ASSERT.SIMPLE_SQL_NAME(p_table_name)
                   || ' SELECT * FROM poladm.policy WHERE policy_id = :1' USING p_policy_id;
  END archive_to;

  FUNCTION broker_uplift(p_broker_id IN NUMBER) RETURN NUMBER IS
    l_pct poladm.broker.commission_pct%TYPE;
  BEGIN SELECT commission_pct INTO l_pct FROM poladm.broker WHERE broker_id = p_broker_id;
    RETURN 1 + NVL(l_pct, 0) / 100;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN RETURN c_default_uplift;
    WHEN TOO_MANY_ROWS THEN RAISE_APPLICATION_ERROR(-20002, 'Duplicate broker ' || p_broker_id);
  END broker_uplift;

  PROCEDURE renew_expiring(p_as_of_dt IN DATE DEFAULT TRUNC(SYSDATE), p_horizon_days IN NUMBER DEFAULT 30,
                           p_uplift IN NUMBER DEFAULT c_default_uplift,
                           p_rows_out OUT NUMBER, p_status_out OUT VARCHAR2) IS
    CURSOR c_expiring IS
      SELECT p.policy_id, p.annual_premium, p.broker_id
        FROM poladm.policy p
       WHERE p.policy_status = 'LIVE'
         AND p.expiry_dt BETWEEN p_as_of_dt AND p_as_of_dt + p_horizon_days   -- DATE compare keeps time-of-day
         FOR UPDATE OF p.annual_premium, p.expiry_dt SKIP LOCKED;
    TYPE t_row_tab IS TABLE OF c_expiring%ROWTYPE INDEX BY PLS_INTEGER;
    l_rows t_row_tab;  l_ids t_id_tab;  l_new t_num_tab;
  BEGIN
    g_run_id := NVL(g_run_id, 0) + 1;  p_rows_out := 0;
    OPEN c_expiring;
    LOOP
      FETCH c_expiring BULK COLLECT INTO l_rows LIMIT 500;
      EXIT WHEN l_rows.COUNT = 0;
      FOR i IN 1 .. l_rows.COUNT LOOP
        l_ids(i) := l_rows(i).policy_id;
        l_new(i) := ROUND(l_rows(i).annual_premium * NVL(p_uplift, broker_uplift(l_rows(i).broker_id)), 2);
        IF l_new(i) < 0 THEN RAISE e_premium_negative; END IF;
      END LOOP;
      SAVEPOINT sp_batch;  BEGIN
        FORALL i IN 1 .. l_ids.COUNT
          UPDATE poladm.policy SET annual_premium = l_new(i), expiry_dt = ADD_MONTHS(expiry_dt, 12)
           WHERE policy_id = l_ids(i);
        FORALL i IN 1 .. l_ids.COUNT
          INSERT INTO poladm.premium_txn (txn_id, policy_id, txn_type_cd, txn_dt, amount, source_system)
          VALUES (poladm.txn_seq.NEXTVAL, l_ids(i), 'RN', SYSDATE, l_new(i), 'PKG_POLICY_RENEWAL');
        p_rows_out := p_rows_out + SQL%ROWCOUNT;
      EXCEPTION
        WHEN DUP_VAL_ON_INDEX THEN
          ROLLBACK TO sp_batch;
          poladm.prc_log_event(NULL, 'ERROR', p_message => 'Batch dup: ' || SQLERRM);
      END;
    END LOOP;
    CLOSE c_expiring;  p_status_out := 'OK';  COMMIT;
    DBMS_OUTPUT.PUT_LINE('run ' || g_run_id || ' renewed ' || p_rows_out);
  EXCEPTION
    WHEN e_premium_negative THEN
      ROLLBACK; p_status_out := 'NEGATIVE_PREMIUM';
      poladm.prc_log_event(NULL, 'ERROR', p_message => 'Negative premium in run ' || g_run_id);
    WHEN OTHERS THEN                                               -- swallowed: caller only sees p_status_out
      ROLLBACK; p_status_out := SUBSTR('ERR ' || SQLCODE || ' ' || SQLERRM, 1, 200);
      poladm.prc_log_event(NULL, 'ERROR', p_message => DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);
  END renew_expiring;
END pkg_policy_renewal;
/
