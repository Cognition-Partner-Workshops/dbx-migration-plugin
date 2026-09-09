-- Negative rehearsal D: drift in an applied row while other rows of the same table are in flight.
-- Run with --depth sampled and a small sample (sample_size 50) so tier 3 does not visit loan 5;
-- tier 2 must still fail on the applied subset. Repair: load_target.py.
SET search_path = loan_servicing;

-- the ten newest loans are not yet applied: stale balances under an older watermark (30s lag,
-- inside cdc_lag_max_s). Their stale values must not produce aggregate findings.
UPDATE loans SET current_balance = current_balance - 1000,
                 modified_date = modified_date - INTERVAL '30 seconds'
 WHERE loan_id > 870;

-- one applied row drifts on a field tier 2 grades fully (int, no rounding rule)
UPDATE loans SET term_months = term_months + 600 WHERE loan_id = 5;
