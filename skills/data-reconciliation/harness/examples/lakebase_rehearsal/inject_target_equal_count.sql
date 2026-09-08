-- Negative rehearsal C: defects that leave every table count and every global MAX(watermark)
-- unchanged, so tier 1 and tier 6's lag check see nothing and a sampled tier 3 may never fetch
-- the rows. Only the key-range fingerprints (tier 5) and per-key ordering (tier 6) name them.
-- The source is never touched. Repair: load_target.py.
SET search_path = loan_servicing;

-- tier 5 pk_missing_on_target [(500,)] and pk_extra_on_target [(2989,)]: a key swapped for a
-- stray; payments count stays 2988 and max(created_date) is unchanged
DELETE FROM payments WHERE payment_id = 500;
INSERT INTO payments (payment_id, loan_id, payment_date, effective_date, principal_amt, interest_amt,
                      escrow_amt, late_fee_amt, total_amt, payment_type, payment_method, batch_id,
                      reversal_flag, created_date)
SELECT 2989, loan_id, payment_date, effective_date, principal_amt, interest_amt, escrow_amt,
       late_fee_amt, total_amt, payment_type, payment_method, batch_id, reversal_flag, created_date
FROM payments WHERE payment_id = 501;

-- tier 6 row_ahead_of_source [(7,)]: loan 7's applied row is one second newer than its source
-- row but still below max(modified_date), so max(target) is not ahead of max(source)
UPDATE loans SET modified_date = modified_date + INTERVAL '1 second' WHERE loan_id = 7;
