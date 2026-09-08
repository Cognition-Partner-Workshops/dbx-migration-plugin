-- Negative rehearsal B: counts stay equal so tiers 2-3 run. Applied CDC watermark lags the source
-- on borrowers and one applied loan row drifts. Repair: load_target.py.
SET search_path = loan_servicing;

-- tier 6 cdc_lag_exceeded (300s > cdc_lag_max_s = 60). Every source borrower row is now newer
-- than the target's applied watermark, so tier 0 reports 500 in flight and tier 3 excludes them
-- (in_flight_rows = 500, no field findings) instead of reporting 500 false diffs.
UPDATE borrowers SET modified_date = modified_date - INTERVAL '5 minutes';

-- tier 2 aggregate mismatch on loans.current_balance (sum), tier 3 field mismatch on loan_id 5.
-- modified_date is untouched: the row is applied, so the drift is a real defect.
UPDATE loans SET current_balance = current_balance + 0.01 WHERE loan_id = 5;
