# Databricks notebook source
# MAGIC %md
# MAGIC # Current scoring input staging (PR3)
# MAGIC Prepare observable inputs only. No fit, transform with a model, predictions or future labels.
# MAGIC Run only after a separately authorized deployment and comparison against a pinned Silver version.

# COMMAND ----------

# MAGIC %run ./08_common_feature_preparation

# COMMAND ----------

from datetime import datetime, timezone
import uuid

spark.conf.set("spark.sql.session.timeZone", "Europe/Rome")
SOURCE_TABLE = "rome_transport.silver.realtime_enriched_observations"
OUTPUT_TABLE = "rome_transport.features.next_stop_delay_scoring"
SOURCE_VERSION = None  # Pin a retained Silver version to reproduce a preparation.
REQUESTED_FEED_TIMESTAMP = None  # Optional explicit epoch; never later than preparation time.
ALLOW_STAGING_OVERWRITE = False  # Explicit operator gate; repository-first PR3 does not create runtime tables.
prepared_at = datetime.now(timezone.utc)
batch_id = str(uuid.uuid4())
if SOURCE_VERSION is None:
    SOURCE_VERSION = int(spark.sql(f"DESCRIBE HISTORY {SOURCE_TABLE} LIMIT 1").select("version").first()[0])
source = spark.read.option("versionAsOf", SOURCE_VERSION).table(SOURCE_TABLE)
cutoff = int(prepared_at.timestamp())
eligible = source.filter(F.col("feed_timestamp") <= cutoff)
latest_feed = eligible.agg(F.max("feed_timestamp").alias("feed")).first()["feed"]
if latest_feed is None:
    raise ValueError("No Silver observations available at preparation time")
selected_feed = REQUESTED_FEED_TIMESTAMP if REQUESTED_FEED_TIMESTAMP is not None else latest_feed
if selected_feed > cutoff:
    raise ValueError("Requested feed timestamp is after preparation time")
scoring = prepare_scoring_dataset(source, SOURCE_VERSION, selected_feed, prepared_at, batch_id)
row_count = scoring.count()
if not row_count:
    raise ValueError("No eligible next-stop observations; existing staging output preserved")
print({"source_silver_version": SOURCE_VERSION, "selected_feed_timestamp": selected_feed,
       "feed_age_seconds": cutoff - selected_feed, "rows": row_count,
       "feature_contract_hash": verify_feature_contract(), "batch_id": batch_id})
scoring.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC One feed snapshot replaces staging after validation and explicit opt-in. This is not prediction history.
# MAGIC Silver may be stale; feed_age_seconds is reported without inventing a freshness SLA or refreshing upstream.

# COMMAND ----------

if not ALLOW_STAGING_OVERWRITE:
    raise RuntimeError("Preparation validated; staging write disabled until separately authorized rollout")
(scoring.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(OUTPUT_TABLE))
saved = spark.table(OUTPUT_TABLE)
validate_scoring_rows(saved)
if saved.count() != row_count:
    raise ValueError("Scoring staging read-back row count mismatch")
