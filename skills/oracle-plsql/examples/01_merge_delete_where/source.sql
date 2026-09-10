-- Oracle: nightly feed upsert. GTT staging, MERGE with UPDATE ... DELETE WHERE, NEXTVAL in the
-- INSERT branch, TRIM/'' handling, TRUNC(DATE); poladm.policy.policy_no is NOT NULL UNIQUE: duplicate matched keys
-- raise ORA-30926, unmatched duplicates ORA-00001, a blank (NULL) key on INSERT ORA-01400.

CREATE GLOBAL TEMPORARY TABLE poladm.stg_policy_feed (
  policy_no        VARCHAR2(20),
  party_id         VARCHAR2(20),
  broker_ref       VARCHAR2(12),
  product_cd       VARCHAR2(6),
  policy_status    VARCHAR2(10),
  inception_dt     DATE,
  expiry_dt        DATE,
  annual_premium   NUMBER,
  cover_note_ref   VARCHAR2(30),
  feed_action      CHAR(1)              -- U = upsert, D = delete
) ON COMMIT PRESERVE ROWS;

MERGE INTO poladm.policy tgt
USING (
  SELECT REPLACE(UPPER(TRIM(s.policy_no)), 'AL/', 'ALB-')      AS policy_no,
         s.party_id,
         b.broker_id,
         s.product_cd,
         s.policy_status,
         TRUNC(s.inception_dt)                                  AS inception_dt,
         s.expiry_dt,
         s.annual_premium,
         NULLIF(TRIM(s.cover_note_ref), '')                     AS cover_note_ref,   -- '' is already NULL
         s.feed_action
    FROM poladm.stg_policy_feed s
    LEFT JOIN poladm.broker b ON b.broker_ref = s.broker_ref
) src
ON (tgt.policy_no = src.policy_no)
WHEN MATCHED THEN
  UPDATE SET tgt.policy_status  = src.policy_status,
             tgt.annual_premium = src.annual_premium,
             tgt.expiry_dt      = src.expiry_dt,
             tgt.broker_id      = NVL(src.broker_id, tgt.broker_id),
             tgt.cover_note_ref = src.cover_note_ref
  WHERE tgt.row_version >= 1
  DELETE WHERE src.feed_action = 'D'
WHEN NOT MATCHED THEN
  INSERT (policy_id, policy_no, party_id, broker_id, product_cd, policy_status,
          inception_dt, expiry_dt, annual_premium, cover_note_ref)
  VALUES (poladm.policy_seq.NEXTVAL, src.policy_no, src.party_id, src.broker_id, src.product_cd,
          src.policy_status, src.inception_dt, src.expiry_dt, src.annual_premium, src.cover_note_ref)
  WHERE src.feed_action <> 'D';

COMMIT;
