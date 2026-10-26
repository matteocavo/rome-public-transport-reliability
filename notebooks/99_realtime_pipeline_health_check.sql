-- Databricks notebook source
SELECT
  source_name,
  status,
  COUNT(*) AS runs,
  MIN(run_timestamp) AS first_run,
  MAX(run_timestamp) AS last_run,
  SUM(rows_appended) AS total_rows_appended
FROM rome_transport.bronze.ingestion_runs
WHERE source_name <> '__batch__'
  AND run_timestamp >= current_timestamp() - INTERVAL 4 DAYS
GROUP BY source_name, status
ORDER BY source_name, status;

-- COMMAND ----------

SELECT
  'trip_updates' AS source,
  COUNT(*) AS rows,
  COUNT(DISTINCT feed_timestamp) AS distinct_snapshots,
  MIN(feed_timestamp) AS min_feed_timestamp,
  MAX(feed_timestamp) AS max_feed_timestamp
FROM rome_transport.bronze.trip_updates

UNION ALL

SELECT
  'vehicle_positions',
  COUNT(*),
  COUNT(DISTINCT feed_timestamp),
  MIN(feed_timestamp),
  MAX(feed_timestamp)
FROM rome_transport.bronze.vehicle_positions

UNION ALL

SELECT
  'service_alerts',
  COUNT(*),
  COUNT(DISTINCT feed_timestamp),
  MIN(feed_timestamp),
  MAX(feed_timestamp)
FROM rome_transport.bronze.service_alerts;

-- COMMAND ----------

SELECT
  run_timestamp,
  source_name,
  feed_timestamp,
  entities_received,
  rows_appended,
  status,
  error_message
FROM rome_transport.bronze.ingestion_runs
WHERE source_name <> '__batch__'
ORDER BY run_timestamp DESC
LIMIT 30;

-- COMMAND ----------

-- Watermarks and measured lag are not a claim of a configured freshness SLA.
WITH layers AS (
  SELECT 0 AS position, 'bronze.trip_updates' AS layer, COUNT(*) AS rows,
         MAX(feed_timestamp) AS latest_feed_timestamp FROM rome_transport.bronze.trip_updates
  UNION ALL SELECT 1, 'silver.trip_updates', COUNT(*), MAX(feed_timestamp)
    FROM rome_transport.silver.trip_updates
  UNION ALL SELECT 2, 'silver.realtime_trip_stop_observations', COUNT(*), MAX(feed_timestamp)
    FROM rome_transport.silver.realtime_trip_stop_observations
  UNION ALL SELECT 3, 'silver.realtime_enriched_observations', COUNT(*), MAX(feed_timestamp)
    FROM rome_transport.silver.realtime_enriched_observations
  UNION ALL SELECT 4, 'features.next_stop_delay_scoring', COUNT(*), MAX(feed_timestamp)
    FROM rome_transport.features.next_stop_delay_scoring
)
SELECT layer, rows, latest_feed_timestamp,
       unix_timestamp(current_timestamp()) - latest_feed_timestamp AS feed_age_seconds,
       MAX(latest_feed_timestamp) OVER () - latest_feed_timestamp AS lag_from_bronze_seconds,
       LAG(latest_feed_timestamp) OVER (ORDER BY position) - latest_feed_timestamp AS lag_from_previous_layer_seconds
FROM layers ORDER BY position;

-- COMMAND ----------

SELECT feed_timestamp, entity_id, trip_id, stop_sequence, stop_id, COUNT(*) AS duplicate_rows
FROM rome_transport.features.next_stop_delay_scoring
GROUP BY feed_timestamp, entity_id, trip_id, stop_sequence, stop_id
HAVING COUNT(*) > 1;

-- COMMAND ----------

SELECT COUNT(*) AS rows, COUNT(DISTINCT feed_timestamp) AS feed_epochs,
       COUNT(DISTINCT source_silver_version) AS source_versions,
       COUNT(DISTINCT feature_preparation_batch_id) AS preparation_batches,
       MIN(source_silver_version) AS source_silver_version,
       MAX(feature_prepared_at) AS prepared_at,
       COUNT_IF(feature_contract_hash IS NULL OR feature_contract_hash <>
         '11682b3cc1ab5c1d8440af4f6294e932e815126c3dd86ab609f4d5f8f35c41a5') AS invalid_hash_rows,
       COUNT_IF(feature_prepared_at > current_timestamp()) AS future_preparation_rows
FROM rome_transport.features.next_stop_delay_scoring;

-- COMMAND ----------

SELECT
    run_timestamp,
    source_name,
    feed_timestamp,
    entities_received,
    rows_appended,
    status,
    error_message
FROM rome_transport.bronze.ingestion_runs
WHERE source_name <> '__batch__'
  AND status = 'FAILED'
  AND run_timestamp >= current_timestamp() - INTERVAL 4 DAYS
ORDER BY run_timestamp DESC;

-- COMMAND ----------


-- The following cells require the additive audit migration in docs/INGESTION_AUDIT.md.
-- Legacy rows have NULL record_type; do not infer empty snapshots from rows_appended=0.
SELECT
  source_name,
  MAX(run_timestamp) AS latest_attempt,
  MAX(CASE WHEN status = 'SUCCESS' THEN run_timestamp END) AS latest_success,
  MAX(CASE WHEN status = 'FAILED' THEN run_timestamp END) AS latest_failure,
  COUNT_IF(status = 'SUCCESS' AND is_empty = true) AS successful_empty_feeds
FROM rome_transport.bronze.ingestion_runs
WHERE COALESCE(record_type, 'SOURCE') = 'SOURCE'
GROUP BY source_name;

-- COMMAND ----------

-- Every attempt has a RUNNING event and, if finalization succeeds, a terminal event.
-- A RUNNING event without a terminal event remains unknown/incomplete, never SUCCESS.
SELECT ingestion_batch_id, ingestion_attempt_id, run_timestamp, status, error_type, error_message
FROM rome_transport.bronze.ingestion_runs
WHERE record_type = 'BATCH'
ORDER BY run_timestamp DESC
LIMIT 50;

-- COMMAND ----------

SELECT source_name, run_timestamp, feed_timestamp, feed_incrementality,
       entities_received, parsed_row_count, rows_appended, duplicate_row_count,
       is_empty, status, error_type, error_message
FROM rome_transport.bronze.ingestion_runs
WHERE record_type = 'SOURCE'
ORDER BY run_timestamp DESC
LIMIT 30;
