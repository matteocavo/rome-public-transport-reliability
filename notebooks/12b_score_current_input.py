# Databricks notebook source
# MAGIC %md
# MAGIC # Scoring-only from a persisted native classifier
# MAGIC No training, labeled-source read, artifact logging, registration or table write.
# MAGIC Current staging is historical while Silver remains stale; this is not live prediction.

# COMMAND ----------

# MAGIC %run ./08_common_feature_preparation

# COMMAND ----------

# MAGIC %run ./12_common_persisted_classifier

# COMMAND ----------

MODEL_URI = None  # Exact persisted MLflow Spark URI, e.g. runs:/<new_run_id>/classifier_pipeline.
MODEL_DFS_TMPDIR = None  # Existing /Volumes/... location if required for native load in this runtime.
STAGING_VERSION = None  # If unset, capture and pin the latest staging Delta version once.
ALLOW_MODEL_INFERENCE = False
ALLOW_SCORING_WRITE = False  # PR4 has no output writer, even if this flag is changed.

if not ALLOW_MODEL_INFERENCE:
    raise RuntimeError("Model inference requires separate runtime authorization")
if ALLOW_SCORING_WRITE:
    raise RuntimeError("PR4 produces only a DataFrame; prediction persistence belongs to PR5")
if not MODEL_URI:
    raise ValueError("MODEL_URI must be configured; no fitting fallback exists")
pipeline,metadata = load_classifier_artifact(MODEL_URI, dfs_tmpdir=MODEL_DFS_TMPDIR)
if STAGING_VERSION is None:
    STAGING_VERSION=int(spark.sql(f"DESCRIBE HISTORY {STAGING_TABLE} LIMIT 1").select("version").first()[0])
staging=spark.read.option("versionAsOf",STAGING_VERSION).table(STAGING_TABLE)
scored_at=datetime.now(timezone.utc)
predictions=score_current_input(staging,pipeline,metadata,scored_at)
print({"model_uri":MODEL_URI,"model_run_id":metadata["model_run_id"],"staging_version":STAGING_VERSION,
    "rows":predictions.count(),"feature_contract_hash":verify_feature_contract(),
    "source_feed_age_seconds":int(scored_at.timestamp())-int(staging.select("feed_timestamp").first()[0]),
    "freshness_note":"Latest scoring source is historical because Silver is currently stale"})
predictions.orderBy(*OBS_KEY).show(20,truncate=False)
