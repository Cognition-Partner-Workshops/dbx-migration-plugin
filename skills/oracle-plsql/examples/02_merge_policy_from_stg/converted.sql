-- Converted: fixture 09_mrg_policy_from_stg.sql (GTT + MERGE with DELETE WHERE + NEXTVAL) -> Databricks SQL.
-- Track: analytical Delta (the POLADM.POLICY copy in the migration catalog). Lakebase variant at the bottom.
-- Rules: SKILL.md §5 #83 (MERGE), #87 (NEXTVAL), #6/#25 (NULLIF/TRIM), §7 trap 8, trap 1, trap 3.

-- GTT ON COMMIT PRESERVE ROWS -> session-scoped temporary table [dbsql:materialized-views-pipes.md#Temporary Tables]
-- (CREATE OR REPLACE TEMP TABLE is not supported; DROP first when re-running in the same session)
CREATE TEMPORARY TABLE stg_policy_feed (
  policy_no        STRING,
  party_id         STRING,
  broker_ref       STRING,
  product_cd       STRING,
  policy_status    STRING,
  inception_dt     TIMESTAMP_NTZ,      -- Oracle DATE (may carry a time) §4
  expiry_dt        TIMESTAMP_NTZ,
  annual_premium   DECIMAL(38,10),     -- NUMBER without scale §7 trap 2
  cover_note_ref   STRING,
  feed_action      STRING              -- CHAR(1): rstrip_spaces on load
);

-- Oracle MERGE evaluation order: WHEN MATCHED UPDATE (WHERE) then DELETE WHERE on the *updated* row;
-- Databricks evaluates WHEN clauses top-down and takes the first match [docs:delta-merge-into],
-- so the DELETE branch must come first and carry its own predicate. Duplicate source keys raise ORA-30926 in
-- Oracle and DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE on Databricks: dedupe explicitly with
-- QUALIFY [docs:sql-ref-syntax-qry-select-qualify] and count the dropped rows (Tier 1 evidence).
MERGE INTO ${catalog}.poladm.policy AS tgt
USING (
  SELECT replace(upper(trim(s.policy_no)), 'AL/', 'ALB-')  AS policy_no,
         s.party_id,
         b.broker_id,
         s.product_cd,
         s.policy_status,
         date_trunc('DAY', s.inception_dt)                   AS inception_dt,   -- TRUNC(date) §5 #61
         s.expiry_dt,
         s.annual_premium,
         nullif(trim(s.cover_note_ref), '')                  AS cover_note_ref, -- '' must become NULL explicitly on Databricks (§7 trap 1)
         rtrim(s.feed_action)                                AS feed_action
    FROM stg_policy_feed s
    LEFT JOIN ${catalog}.poladm.broker b ON b.broker_ref = s.broker_ref
  QUALIFY row_number() OVER (PARTITION BY replace(upper(trim(s.policy_no)), 'AL/', 'ALB-')
                             ORDER BY s.expiry_dt DESC, s.feed_action) = 1      -- Oracle would have raised ORA-30926 here
) AS src
ON tgt.policy_no = src.policy_no
WHEN MATCHED AND src.feed_action = 'D' AND tgt.row_version >= 1 THEN
  DELETE
WHEN MATCHED AND tgt.row_version >= 1 THEN
  UPDATE SET tgt.policy_status  = src.policy_status,
             tgt.annual_premium = src.annual_premium,
             tgt.expiry_dt      = src.expiry_dt,
             tgt.broker_id      = nvl(src.broker_id, tgt.broker_id),
             tgt.cover_note_ref = src.cover_note_ref,
             tgt.row_version    = tgt.row_version + 1,                          -- trigger fan-out folded in (§2 trigger row; example 04)
             tgt.updated_dt     = current_timestamp(),
             tgt.updated_by     = current_user()
WHEN NOT MATCHED AND src.feed_action <> 'D' THEN
  INSERT (policy_no, party_id, broker_id, product_cd, policy_status,
          inception_dt, expiry_dt, annual_premium, cover_note_ref,
          active_policy_flag, row_version, created_dt, created_by)
  VALUES (src.policy_no, src.party_id, src.broker_id, src.product_cd, src.policy_status,
          src.inception_dt, src.expiry_dt, src.annual_premium, src.cover_note_ref,
          CASE WHEN src.policy_status = 'LIVE'
                AND current_date() BETWEEN to_date(src.inception_dt) AND to_date(src.expiry_dt)
               THEN 'Y' ELSE 'N' END,
          1, current_timestamp(), current_user());
-- policy_id: poladm.policy_seq.NEXTVAL is not listable in a Databricks INSERT; the analytical copy declares
-- policy_id BIGINT GENERATED ALWAYS AS IDENTITY [docs:sql-ref-syntax-ddl-create-table-using] and Tier 3 keys on
-- policy_no (the business key), never on policy_id (§7 trap 10).

-- No COMMIT: each MERGE is its own Delta transaction. If the GTT load + MERGE must be one unit, wrap in
-- BEGIN ATOMIC ... END [dbsql:sql-scripting.md#SQL Scripting Atomic Blocks] (catalogManaged tables required).

-- ---------------------------------------------------------------------------------------------
-- Lakebase (OLTP profile) variant, PostgreSQL 17 syntax [pg17:sql-merge]:
--   CREATE TEMP TABLE stg_policy_feed (...) ON COMMIT PRESERVE ROWS;   -- same clause exists in Postgres
--   MERGE INTO poladm.policy tgt USING (...deduped src...) ON tgt.policy_no = src.policy_no
--   WHEN MATCHED AND src.feed_action = 'D' THEN DELETE
--   WHEN MATCHED THEN UPDATE SET ...
--   WHEN NOT MATCHED AND src.feed_action <> 'D' THEN
--     INSERT (policy_id, ...) VALUES (nextval('poladm.policy_seq'), ...);   -- sequence survives as-is (§4, [pg17:sql-createsequence])
--   Postgres also raises on duplicate source keys ("MERGE command cannot affect row a second time"), so the
--   QUALIFY dedupe (as DISTINCT ON / ROW_NUMBER) stays. The BEFORE trigger from example 04 fires here, so
--   row_version / updated_* are NOT set in the MERGE on Lakebase.
