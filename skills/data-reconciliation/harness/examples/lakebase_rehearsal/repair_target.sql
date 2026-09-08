-- Undo the schema-level defects from inject_target_defects.sql; data is restored by re-running
-- load_target.py (TRUNCATE + COPY + sequence restart).
SET search_path = loan_servicing;
ALTER TABLE loans ALTER COLUMN loan_status SET NOT NULL;
