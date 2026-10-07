-- Databricks notebook source
SELECT
  source_name,
  status,
  COUNT(*) AS runs,
  MIN(run_timestamp) AS first_run,
  MAX(run_timestamp) AS last_run,
  SUM(rows_appended) AS total_rows_appended
FROM rome_transport.bronze.ingestion_runs
WHERE run_timestamp >= current_timestamp() - INTERVAL 4 DAYS
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
ORDER BY run_timestamp DESC
LIMIT 30;

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
WHERE status = 'FAILED'
  AND run_timestamp >= current_timestamp() - INTERVAL 4 DAYS
ORDER BY run_timestamp DESC;

-- COMMAND ----------

