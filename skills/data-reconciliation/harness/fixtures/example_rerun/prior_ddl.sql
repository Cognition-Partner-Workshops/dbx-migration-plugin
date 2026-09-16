-- Previous committed shape (wave 1): what the evolved run pre-creates before the job runs.
CREATE TABLE IF NOT EXISTS mig.sales.orders (
  order_id BIGINT NOT NULL,
  amount DECIMAL(18,2),
  tags ARRAY<STRUCT<k: STRING, v: STRING>>
) USING DELTA;
