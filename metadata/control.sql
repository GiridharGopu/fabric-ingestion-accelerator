CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.connection (
  connection_id STRING, environment STRING, connector STRING, options_json STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS ops.dataset (
  dataset_id STRING, connection_id STRING, load_mode STRING, enabled BOOLEAN,
  run_group STRING, options_json STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS ops.landing_event (
  client STRING, environment STRING, dataset_id STRING, batch_id STRING,
  attempt_id STRING, status STRING, plan_json STRING, manifest_path STRING,
  object_count BIGINT, error_type STRING, recorded_utc TIMESTAMP
) USING DELTA;
