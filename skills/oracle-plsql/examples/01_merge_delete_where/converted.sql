-- Databricks SQL (analytical track). Oracle fires the example 02 row trigger on this MERGE; Delta has none, so its effects are folded in.
-- Cites: [docs:delta-merge-into], [docs:functions/assert_true], [dbsql:materialized-views-pipes.md#Temporary Tables].

-- GTT ON COMMIT PRESERVE ROWS -> session temp table (no CREATE OR REPLACE TEMP TABLE: DROP first on re-run)
CREATE TEMPORARY TABLE stg_policy_feed (
  policy_no STRING, party_id STRING, broker_ref STRING, product_cd STRING, policy_status STRING,
  inception_dt TIMESTAMP_NTZ, expiry_dt TIMESTAMP_NTZ,   -- Oracle DATE carries a time
  annual_premium DECIMAL(38,10),                         -- NUMBER without scale
  cover_note_ref STRING, feed_action STRING);            -- CHAR(1): rtrim on load

-- USING subquery materialised once: a temp view would be re-executed by every statement below.
DROP TABLE IF EXISTS stg_policy_src;
CREATE TEMPORARY TABLE stg_policy_src AS
  SELECT nullif(replace(upper(trim(s.policy_no)), 'AL/', 'ALB-'), '') AS policy_no,  -- blank key is NULL in Oracle
         s.party_id, b.broker_id, s.product_cd, s.policy_status,
         date_trunc('DAY', s.inception_dt)                   AS inception_dt,   -- TRUNC(date)
         s.expiry_dt, s.annual_premium,
         nullif(trim(s.cover_note_ref), '')                  AS cover_note_ref, -- '' is a value here: NULL it explicitly
         rtrim(s.feed_action)                                AS feed_action
    FROM stg_policy_feed s
    LEFT JOIN ${catalog}.poladm.broker b ON b.broker_ref = s.broker_ref;

-- Key parity (policy_no is NOT NULL UNIQUE in Oracle): ORA-30926 when >1 source row hits one target row, ORA-00001
-- when unmatched duplicates both INSERT, ORA-01400 when a NULL key would INSERT; unmatched duplicate or NULL 'D' rows
-- reach no clause and succeed. Delta has no constraints, so fail on exactly those cases; never QUALIFY-dedupe silently.
SELECT assert_true(count(*) = 0, concat('policy_no keys Oracle would reject: ', count(*)))
  FROM (SELECT s.policy_no FROM stg_policy_src s LEFT JOIN ${catalog}.poladm.policy t ON t.policy_no = s.policy_no
         WHERE t.policy_no IS NOT NULL OR s.feed_action <> 'D'
         GROUP BY s.policy_no HAVING count(*) > 1 OR max(s.policy_no) IS NULL);

-- :OLD image for the trigger's "status or premium changed" audit condition (rows the D branch deletes included).
DROP TABLE IF EXISTS policy_pre_image;
CREATE TEMPORARY TABLE policy_pre_image AS
  SELECT t.policy_id, t.policy_no, t.policy_status, t.annual_premium, t.row_version
    FROM ${catalog}.poladm.policy t
   WHERE t.policy_no IN (SELECT policy_no FROM stg_policy_src);

-- Oracle runs UPDATE then DELETE WHERE on the updated row; Delta takes the first WHEN on the pre-update row: DELETE first.
MERGE INTO ${catalog}.poladm.policy AS tgt
USING stg_policy_src AS src
ON tgt.policy_no = src.policy_no
WHEN MATCHED AND src.feed_action = 'D' AND tgt.row_version >= 1 THEN
  DELETE
WHEN MATCHED AND tgt.row_version >= 1 THEN
  UPDATE SET tgt.policy_status      = src.policy_status,
             tgt.annual_premium     = src.annual_premium,
             tgt.expiry_dt          = src.expiry_dt,
             tgt.broker_id          = nvl(src.broker_id, tgt.broker_id),
             tgt.cover_note_ref     = src.cover_note_ref,
             tgt.row_version        = tgt.row_version + 1,          -- trigger UPDATING branch
             tgt.updated_dt         = current_timestamp(),
             tgt.updated_by         = current_user(),
             tgt.active_policy_flag = CASE WHEN src.policy_status = 'LIVE'
                                            AND current_date() BETWEEN to_date(tgt.inception_dt) AND to_date(src.expiry_dt)
                                           THEN 'Y' ELSE 'N' END
WHEN NOT MATCHED AND src.feed_action <> 'D' THEN
  INSERT (policy_no, party_id, broker_id, product_cd, policy_status, inception_dt, expiry_dt,
          annual_premium, cover_note_ref, active_policy_flag, row_version, created_dt, created_by)
  VALUES (src.policy_no, src.party_id, src.broker_id, src.product_cd, src.policy_status, src.inception_dt,
          src.expiry_dt, src.annual_premium, src.cover_note_ref,
          CASE WHEN src.policy_status = 'LIVE'
                AND current_date() BETWEEN to_date(src.inception_dt) AND to_date(src.expiry_dt) THEN 'Y' ELSE 'N' END,
          1, current_timestamp(), current_user());  -- policy_id: NEXTVAL -> IDENTITY column; recon keys on policy_no

-- Trigger's prc_log_event branch (autonomous in Oracle; commits with the MERGE here: accepted difference).
INSERT INTO ${catalog}.poladm.policy_audit_log
  (policy_id, event_cd, old_status, new_status, old_premium, new_premium, event_ts, session_user)
SELECT coalesce(pre.policy_id, p.policy_id),
       CASE WHEN pre.policy_no IS NULL THEN 'INSERT' ELSE 'UPDATE' END,
       pre.policy_status, src.policy_status, pre.annual_premium, src.annual_premium,
       current_timestamp(), current_user()
  FROM stg_policy_src src
  LEFT JOIN policy_pre_image pre       ON pre.policy_no = src.policy_no
  LEFT JOIN ${catalog}.poladm.policy p ON p.policy_no = src.policy_no AND pre.policy_no IS NULL
 WHERE (pre.policy_no IS NULL AND src.feed_action <> 'D')
    OR (pre.row_version >= 1 AND (coalesce(pre.policy_status, '~') <> src.policy_status
                                  OR coalesce(pre.annual_premium, -1) <> src.annual_premium));

-- Lakebase (OLTP) variant: CREATE TEMP TABLE ... ON COMMIT PRESERVE ROWS; same MERGE with DELETE first, VALUES
-- (nextval('poladm.policy_seq'), ...), pre-check via RAISE EXCEPTION; the example 02 trigger fires there (no fold-in).
