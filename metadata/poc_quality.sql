CREATE TABLE IF NOT EXISTS ops.quality_rule (
  dataset_id STRING, rule_id STRING, rule_type STRING, column_name STRING,
  threshold BIGINT, severity STRING, enabled BOOLEAN
) USING DELTA;

CREATE TABLE IF NOT EXISTS ops.quality_result (
  dataset_id STRING, batch_id STRING, landing_attempt_id STRING, evaluation_id STRING,
  rule_id STRING, measured BIGINT, threshold BIGINT, severity STRING,
  status STRING, recorded_utc TIMESTAMP
) USING DELTA;

MERGE INTO ops.quality_rule target
USING (
  SELECT * FROM VALUES
    ('poc__orders', 'rows_expected', 'min_rows', '', 1L, 'fail', true),
    ('poc__orders', 'id_required', 'null_or_blank', 'order_id', 0L, 'fail', true),
    ('poc__orders', 'id_unique', 'duplicate_keys', 'order_id', 0L, 'fail', true),
    ('poc__orders', 'date_valid', 'invalid_date', 'order_date', 0L, 'fail', true),
    ('poc__orders', 'amount_valid', 'invalid_long', 'amount_minor', 0L, 'fail', true)
  AS rules(dataset_id, rule_id, rule_type, column_name, threshold, severity, enabled)
) seed
ON target.dataset_id = seed.dataset_id AND target.rule_id = seed.rule_id
WHEN NOT MATCHED THEN INSERT *;
