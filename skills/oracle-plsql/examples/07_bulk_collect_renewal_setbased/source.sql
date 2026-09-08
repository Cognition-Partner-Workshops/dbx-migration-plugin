-- Object class: PACKAGE (spec) + PACKAGE BODY. Census key: POLADM.PKG_POLICY_RENEWAL
-- Explicit cursor + BULK COLLECT LIMIT + FORALL, package-level state, SYS_REFCURSOR out,
-- named exception handlers, RAISE_APPLICATION_ERROR, SAVEPOINT/ROLLBACK TO, dynamic SQL,
-- calls to the autonomous logger, and a DBMS_OUTPUT trace.
-- Reads : POLADM.POLICY, POLADM.PARTY, POLADM.BROKER
-- Writes: POLADM.POLICY, POLADM.PREMIUM_TXN (via FORALL), POLADM.POLICY_AUDIT_LOG (via prc_log_event)

CREATE OR REPLACE PACKAGE poladm.pkg_policy_renewal AS

  c_batch_limit      CONSTANT PLS_INTEGER := 500;
  c_default_uplift   CONSTANT NUMBER      := 1.035;      -- NUMBER without scale

  g_run_id           NUMBER;                            -- package state: lives for the session
  g_rows_renewed     PLS_INTEGER := 0;

  e_premium_negative EXCEPTION;
  PRAGMA EXCEPTION_INIT(e_premium_negative, -20001);

  TYPE t_policy_id_tab  IS TABLE OF poladm.policy.policy_id%TYPE     INDEX BY PLS_INTEGER;
  TYPE t_premium_tab    IS TABLE OF poladm.policy.annual_premium%TYPE INDEX BY PLS_INTEGER;

  PROCEDURE renew_expiring(
    p_as_of_dt     IN  DATE     DEFAULT TRUNC(SYSDATE),
    p_horizon_days IN  NUMBER   DEFAULT 30,
    p_uplift       IN  NUMBER   DEFAULT c_default_uplift,
    p_rows_out     OUT NUMBER,
    p_status_out   OUT VARCHAR2);

  FUNCTION expiring_cursor(
    p_as_of_dt     IN DATE,
    p_horizon_days IN NUMBER) RETURN SYS_REFCURSOR;

  FUNCTION broker_uplift(p_broker_id IN NUMBER) RETURN NUMBER;

END pkg_policy_renewal;
/

CREATE OR REPLACE PACKAGE BODY poladm.pkg_policy_renewal AS

  -- Private helper: dynamic SQL against a table whose name arrives at runtime (INFERRED edge)
  PROCEDURE archive_to(p_table_name IN VARCHAR2, p_policy_id IN NUMBER) IS
    l_sql VARCHAR2(4000);
  BEGIN
    l_sql := 'INSERT INTO poladm.' || DBMS_ASSERT.SIMPLE_SQL_NAME(p_table_name)
          || ' SELECT * FROM poladm.policy WHERE policy_id = :1';
    EXECUTE IMMEDIATE l_sql USING p_policy_id;
  END archive_to;

  FUNCTION broker_uplift(p_broker_id IN NUMBER) RETURN NUMBER IS
    l_pct poladm.broker.commission_pct%TYPE;
  BEGIN
    SELECT commission_pct INTO l_pct
      FROM poladm.broker
     WHERE broker_id = p_broker_id;
    RETURN 1 + NVL(l_pct, 0) / 100;          -- NUMBER arithmetic, unbounded scale
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      RETURN c_default_uplift;
    WHEN TOO_MANY_ROWS THEN
      RAISE_APPLICATION_ERROR(-20002, 'Duplicate broker ' || p_broker_id);
  END broker_uplift;

  FUNCTION expiring_cursor(
    p_as_of_dt     IN DATE,
    p_horizon_days IN NUMBER) RETURN SYS_REFCURSOR IS
    l_cur SYS_REFCURSOR;
  BEGIN
    OPEN l_cur FOR
      SELECT p.policy_id, p.policy_no, p.annual_premium, p.broker_id, p.expiry_dt
        FROM poladm.policy p
       WHERE p.policy_status = 'LIVE'
         AND p.expiry_dt >= p_as_of_dt                       -- DATE compare includes time-of-day
         AND p.expiry_dt <  p_as_of_dt + p_horizon_days      -- date arithmetic in days
       ORDER BY p.expiry_dt, p.policy_id;
    RETURN l_cur;
  END expiring_cursor;

  PROCEDURE renew_expiring(
    p_as_of_dt     IN  DATE     DEFAULT TRUNC(SYSDATE),
    p_horizon_days IN  NUMBER   DEFAULT 30,
    p_uplift       IN  NUMBER   DEFAULT c_default_uplift,
    p_rows_out     OUT NUMBER,
    p_status_out   OUT VARCHAR2) IS

    CURSOR c_expiring IS
      SELECT p.policy_id, p.annual_premium, p.broker_id, p.expiry_dt
        FROM poladm.policy p
       WHERE p.policy_status = 'LIVE'
         AND p.expiry_dt BETWEEN p_as_of_dt AND p_as_of_dt + p_horizon_days
         FOR UPDATE OF p.annual_premium, p.expiry_dt SKIP LOCKED;

    TYPE t_row_tab IS TABLE OF c_expiring%ROWTYPE INDEX BY PLS_INTEGER;
    l_rows        t_row_tab;
    l_ids         t_policy_id_tab;
    l_new_premium t_premium_tab;
    l_uplift      NUMBER;
  BEGIN
    g_run_id := NVL(g_run_id, 0) + 1;
    p_rows_out := 0;

    OPEN c_expiring;
    LOOP
      FETCH c_expiring BULK COLLECT INTO l_rows LIMIT c_batch_limit;
      EXIT WHEN l_rows.COUNT = 0;

      FOR i IN 1 .. l_rows.COUNT LOOP
        l_uplift := NVL(p_uplift, broker_uplift(l_rows(i).broker_id));
        l_ids(i)         := l_rows(i).policy_id;
        l_new_premium(i) := ROUND(l_rows(i).annual_premium * l_uplift, 2);
        IF l_new_premium(i) < 0 THEN
          RAISE e_premium_negative;
        END IF;
      END LOOP;

      SAVEPOINT sp_batch;
      BEGIN
        FORALL i IN 1 .. l_ids.COUNT
          UPDATE poladm.policy
             SET annual_premium = l_new_premium(i),
                 expiry_dt      = ADD_MONTHS(expiry_dt, 12),     -- month-end clamping
                 policy_status  = 'LIVE'
           WHERE policy_id = l_ids(i);

        FORALL i IN 1 .. l_ids.COUNT
          INSERT INTO poladm.premium_txn
            (txn_id, policy_id, txn_type_cd, txn_dt, effective_dt, amount, tax_amount, ccy, source_system)
          VALUES
            (poladm.policy_seq.NEXTVAL, l_ids(i), 'RN', SYSDATE, TRUNC(SYSDATE),
             l_new_premium(i), ROUND(l_new_premium(i) * 0.12, 2), 'GBP', 'PKG_POLICY_RENEWAL');

        p_rows_out := p_rows_out + SQL%ROWCOUNT;
      EXCEPTION
        WHEN DUP_VAL_ON_INDEX THEN
          ROLLBACK TO sp_batch;
          poladm.prc_log_event(NULL, 'ERROR', p_message => 'Batch dup: ' || SQLERRM);
      END;

      l_rows.DELETE; l_ids.DELETE; l_new_premium.DELETE;
    END LOOP;
    CLOSE c_expiring;

    g_rows_renewed := g_rows_renewed + p_rows_out;
    p_status_out   := 'OK';
    COMMIT;
    DBMS_OUTPUT.PUT_LINE('run ' || g_run_id || ' renewed ' || p_rows_out);

  EXCEPTION
    WHEN e_premium_negative THEN
      IF c_expiring%ISOPEN THEN CLOSE c_expiring; END IF;
      ROLLBACK;
      p_status_out := 'NEGATIVE_PREMIUM';
      poladm.prc_log_event(NULL, 'ERROR', p_message => 'Negative premium in run ' || g_run_id);
    WHEN OTHERS THEN
      IF c_expiring%ISOPEN THEN CLOSE c_expiring; END IF;
      ROLLBACK;
      p_status_out := SUBSTR('ERR ' || SQLCODE || ' ' || SQLERRM, 1, 200);
      poladm.prc_log_event(NULL, 'ERROR', p_message => DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);
      -- swallowed: caller sees p_status_out, no exception propagates
  END renew_expiring;

END pkg_policy_renewal;
/
