-- OnError event handler of LoadPaymentFact.dtsx, run by the log_onerror task (run_if: AT_LEAST_ONE_FAILED).
INSERT INTO IDENTIFIER(:tgt_catalog || '.' || :tgt_schema || '.etl_log') (package_name, event, message, logged_at)
VALUES ('LoadPaymentFact', 'OnError',
        'load_payment_fact ' || :result_state || ' ' || :error_code || ' run ' || :run_id,
        current_timestamp());
