-- Negative rehearsal A: five target-side defects the set/schema tiers must name. The source is
-- never touched. Apply to the migration branch, run the harness (expected verdict FAIL with the
-- findings listed per statement), then repair with load_target.py + repair_target.sql.
-- Tier 1 fails here, so tiers 2-3 are skipped by design; inject_target_drift.sql covers those.
SET search_path = loan_servicing;

-- tier 1 count gap outside the in-flight allowance; tier 5 pk_missing_on_target [(10,), (11,)]
DELETE FROM payments WHERE payment_id IN (10, 11);

-- tier 1 count gap (target > source); tier 5 pk_extra_on_target [(999999,)]
INSERT INTO escrow_accounts (escrow_id, loan_id, escrow_type, monthly_amount, current_balance,
                             target_balance, shortage_flag, created_date, modified_date)
SELECT 999999, loan_id, 'TAX', 1, 1, 1, 'N', created_date, modified_date
FROM escrow_accounts WHERE escrow_id = 1;

-- tier 6 target_ahead_of_source (replay / out-of-order apply)
UPDATE loan_modifications SET created_date = created_date + INTERVAL '1 day'
WHERE modification_id = (SELECT MAX(modification_id) FROM loan_modifications);

-- tier 7 not_null_missing on loans.loan_status
ALTER TABLE loans ALTER COLUMN loan_status DROP NOT NULL;

-- tier 7 sequence_behind_source on payments.payment_id
ALTER TABLE payments ALTER COLUMN payment_id RESTART WITH 100;
