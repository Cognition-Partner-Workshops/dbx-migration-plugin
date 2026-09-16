-- Current committed shape (wave 2 added `channel`); the job's source file the proof digests. The
-- canonical failing case: the create-if-absent guard is a no-op on the pre-created wave-1 table (prior_shape.json,
-- the shape the wave-1 proof observed), so the evolved run never lands `channel`.
CREATE TABLE IF NOT EXISTS mig.sales.orders (
  order_id BIGINT NOT NULL,
  amount DECIMAL(18,2),
  channel STRING,
  tags ARRAY<STRUCT<k: STRING, v: STRING>>
) USING DELTA;
INSERT INTO mig.sales.orders SELECT order_id, amount, channel, tags FROM mig.staging.orders_src;
