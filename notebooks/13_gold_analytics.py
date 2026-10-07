# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Analytics Layer
# MAGIC Build audited analytical snapshots for Databricks SQL and AI/BI Dashboards after notebook 12. Historical realized outcomes and the latest serving snapshot remain separate. No training, model selection or dashboard creation is performed.
# MAGIC
# MAGIC ## Analytical Contract
# MAGIC Historical KPIs are **observation-weighted**, not unique stop visits or passenger-weighted reliability. Repeated snapshots can contribute multiple observations per trip. Route and stop dimensions refer to the current observation; the outcome is realized delay at its next stop.
# MAGIC
# MAGIC Delay bands: `early < -60`, `on_time [-60, 180)`, `moderate_delay [180, 300)`, and `major_delay >= 300` seconds. Missing current or previous delay receives an `unknown` propagation band. Rates are fractions in [0, 1]. Percentiles use `percentile_approx` with accuracy 10,000.
# MAGIC
# MAGIC Prediction flags remain **strictly probability > 0.45**. Historical labels never enter predictive operations. Frozen notebook 11 TEST metrics remain reference results, not estimates from refit or serving data. Source Delta versions are pinned once. The refresh manifest is published last, after all persisted Gold outputs pass validation.

# COMMAND ----------

from datetime import datetime, timezone
from functools import reduce
import json
import uuid
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

spark.conf.set("spark.sql.session.timeZone", "Europe/Rome")
HISTORICAL_TABLE = "rome_transport.features.next_stop_delay_labeled"
PREDICTION_TABLE = "rome_transport.ml.next_stop_delay_predictions"
GOLD_SCHEMA = "rome_transport.gold"
REG_LABEL = "target_realized_next_stop_delay_seconds"
CLS_LABEL = "target_realized_major_delay_flag"
OBS_KEY = ["feed_timestamp", "entity_id", "trip_id", "stop_sequence", "stop_id"]
MAJOR_DELAY_SECONDS = 300
CLASSIFICATION_THRESHOLD = 0.45
PERSISTENCE_FALLBACK = -71.0
MAX_ABS_REALIZED_DELAY = 43200
MIN_ROUTE_OBSERVATIONS = 1000
MIN_STOP_OBSERVATIONS = 500
PERCENTILE_ACCURACY = 10000
MAX_AGGREGATE_ROWS = 50000
REFRESH_TIMESTAMP = datetime.now(timezone.utc)
REFRESH_ID = str(uuid.uuid4())
RF_PARAMS = dict(featureSubsetStrategy="sqrt", maxBins=32, maxDepth=10,
                 minInstancesPerNode=5, numTrees=100, seed=42)

def require_columns(df, names):
    missing = sorted(set(names) - set(df.columns))
    if missing:
        raise ValueError(f"Missing required analytical columns: {missing}")

def n_where(condition):
    return F.coalesce(F.sum(F.when(condition, 1).otherwise(0)), F.lit(0)).cast("long")

def nonfinite(column):
    x = column.cast("double")
    return F.isnan(x) | (F.abs(x) == float("inf"))

def any_condition(conditions):
    return reduce(lambda a, b: a | b, conditions, F.lit(False))

def duplicate_groups(df, keys):
    return df.groupBy(*keys).count().filter(F.col("count") > 1).count()

def pin_table(name):
    version = int(spark.sql(f"DESCRIBE HISTORY {name} LIMIT 1").first()["version"])
    return spark.read.option("versionAsOf", version).table(name), version

def delay_band(column):
    return (F.when(column.isNull(), "unknown").when(column < -60, "early")
        .when(column < 180, "on_time").when(column < 300, "moderate_delay").otherwise("major_delay"))

def risk_band(column):
    return (F.when(column < 0.20, "low_risk").when(column <= 0.45, "medium_risk")
        .when(column < 0.80, "high_risk").otherwise("critical_risk"))

def assert_empty(df, condition, message):
    if df.filter(condition).limit(1).count():
        raise ValueError(message)

TABLE_NAMES = ["executive_overview", "route_reliability", "stop_reliability",
    "temporal_reliability", "delay_propagation", "predictive_operations",
    "model_performance", "analytics_refresh_metadata"]
DESTINATIONS = {name: f"{GOLD_SCHEMA}.{name}" for name in TABLE_NAMES}


# COMMAND ----------

# MAGIC %md
# MAGIC ## Source Validation
# MAGIC Required analytical columns fail fast if absent. Optional descriptive fields remain typed NULL when unavailable. Labels must respect the existing +/-12-hour gate and 300-second major-delay definition. Invalid sources are never silently filtered or deduplicated.
# MAGIC
# MAGIC Serving data must contain one snapshot with a consistent scoring run, timestamp, threshold and registration status. The previously observed 17,177 rows are context, not a hard-coded count. Deferred registration does not invalidate existing predictions.

# COMMAND ----------

historical_raw, HISTORICAL_VERSION = pin_table(HISTORICAL_TABLE)
predictions_raw, PREDICTION_VERSION = pin_table(PREDICTION_TABLE)
require_columns(historical_raw, OBS_KEY + ["service_date", "feed_datetime", "route_id",
    REG_LABEL, CLS_LABEL, "current_arrival_delay_seconds", "lag_1_arrival_delay_seconds"])
PRED_REQUIRED = OBS_KEY + ["feed_datetime", "service_date", "route_id", "vehicle_id",
    "current_arrival_delay_seconds", "predicted_next_stop_delay_seconds",
    "major_delay_probability", "predicted_major_delay_flag", "classification_threshold",
    "scored_at", "mlflow_run_id", "registration_status"]
require_columns(predictions_raw, PRED_REQUIRED)
historical_checks = historical_raw.agg(
    F.count("*").alias("row_count"),
    n_where(any_condition([F.col(c).isNull() for c in OBS_KEY + ["service_date", "route_id", "feed_datetime"]])).alias("null_keys"),
    n_where(any_condition([nonfinite(F.col(c)) for c in [REG_LABEL, "current_arrival_delay_seconds", "lag_1_arrival_delay_seconds"]])).alias("invalid_numeric"),
    n_where(F.col(REG_LABEL).isNull() | (F.abs(F.col(REG_LABEL)) > MAX_ABS_REALIZED_DELAY)
        | ~F.col(CLS_LABEL).eqNullSafe((F.col(REG_LABEL) >= MAJOR_DELAY_SECONDS).cast("int"))).alias("invalid_labels"),
    n_where(~F.col("feed_datetime").eqNullSafe(F.timestamp_seconds("feed_timestamp"))).alias("invalid_feed_datetime")
).first().asDict()
HISTORICAL_ROWS = historical_checks["row_count"]
if HISTORICAL_ROWS <= 0 or any(v for k, v in historical_checks.items() if k != "row_count"):
    raise ValueError(f"Historical source validation failed: {historical_checks}")
historical_duplicates = duplicate_groups(historical_raw.select(*OBS_KEY), OBS_KEY)
if historical_duplicates:
    raise ValueError(f"Historical duplicate observation groups: {historical_duplicates}")
p = F.col("major_delay_probability")
current = F.col("current_arrival_delay_seconds").cast("double")
expected_persistence = F.when(current.isNull() | F.isnan(current), PERSISTENCE_FALLBACK).otherwise(current)
prediction_checks = predictions_raw.agg(
    F.count("*").alias("row_count"), F.countDistinct("feed_timestamp").alias("snapshot_count"),
    F.countDistinct("scored_at").alias("scoring_time_count"), F.countDistinct("mlflow_run_id").alias("run_count"),
    F.countDistinct("registration_status").alias("status_count"),
    n_where(any_condition([F.col(c).isNull() for c in PRED_REQUIRED if c != "current_arrival_delay_seconds"])).alias("required_nulls"),
    n_where(p.isNull() | nonfinite(p) | ~p.between(0, 1)).alias("invalid_probabilities"),
    n_where(~F.col("classification_threshold").eqNullSafe(F.lit(CLASSIFICATION_THRESHOLD))
        | ~F.col("predicted_major_delay_flag").eqNullSafe((p > CLASSIFICATION_THRESHOLD).cast("int"))).alias("invalid_threshold_or_flag"),
    n_where(nonfinite(F.col("predicted_next_stop_delay_seconds"))
        | ~F.col("predicted_next_stop_delay_seconds").eqNullSafe(expected_persistence)).alias("invalid_regression"),
    n_where(~F.col("feed_datetime").eqNullSafe(F.timestamp_seconds("feed_timestamp"))).alias("invalid_feed_datetime"),
    F.max("feed_timestamp").alias("feed_timestamp"), F.max("feed_datetime").alias("feed_datetime"),
    F.max("scored_at").alias("scored_at"), F.max("mlflow_run_id").alias("model_run_id"),
    F.max("registration_status").alias("registration_status")
).first().asDict()
PREDICTION_ROWS = prediction_checks["row_count"]
if PREDICTION_ROWS <= 0 or any(prediction_checks[k] != 1 for k in
        ["snapshot_count", "scoring_time_count", "run_count", "status_count"]) or any(prediction_checks[k] for k in
        ["required_nulls", "invalid_probabilities", "invalid_threshold_or_flag", "invalid_regression", "invalid_feed_datetime"]):
    raise ValueError(f"Prediction source validation failed: {prediction_checks}")
prediction_duplicates = duplicate_groups(predictions_raw.select(*OBS_KEY), OBS_KEY)
if prediction_duplicates:
    raise ValueError(f"Prediction duplicate observation groups: {prediction_duplicates}")
REGISTRATION_STATUS = prediction_checks["registration_status"]
print("Historical:", HISTORICAL_TABLE, "version:", HISTORICAL_VERSION, "rows:", HISTORICAL_ROWS)
print("Predictions:", PREDICTION_TABLE, "version:", PREDICTION_VERSION, "rows:", PREDICTION_ROWS)
print("Observed registration status:", REGISTRATION_STATUS)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Historical Preparation and Shared Aggregation
# MAGIC One narrow projection supplies five SQL grouping sets: network, route, stop, temporal and propagation. Only compact aggregates are collected, with a strict 50,000-row limit; original observations are never collected. Source integrity checks use additional narrow scans. No cache, RDD, filesystem staging or extra Delta table is required.
# MAGIC
# MAGIC Names use the lexicographically smallest non-NULL observed value per identifier. Temporal slices preserve `service_date`; ISO `day_of_week` (Monday=1), `feed_hour`, weekend and time band refer to feed datetime in Europe/Rome, independently of the service-day rollover.

# COMMAND ----------

def optional_column(df, name, dtype):
    return (F.col(name) if name in df.columns else F.lit(None)).cast(dtype).alias(name)

historical = historical_raw.select(
    "feed_timestamp", "feed_datetime", "service_date", "route_id", "trip_id", "stop_id",
    optional_column(historical_raw, "vehicle_id", "string"),
    optional_column(historical_raw, "route_short_name", "string"), optional_column(historical_raw, "stop_name", "string"),
    F.col(REG_LABEL).cast("double").alias("realized_delay_seconds"),
    F.col(CLS_LABEL).cast("int").alias("realized_major_delay_flag"),
    F.col("current_arrival_delay_seconds").cast("double").alias("current_delay_seconds"),
    F.col("lag_1_arrival_delay_seconds").cast("double").alias("previous_delay_seconds"))
historical = (historical.withColumn("observed_route_id", F.col("route_id"))
    .withColumn("observed_stop_id", F.col("stop_id")).withColumn("observed_service_date", F.col("service_date"))
    .withColumn("day_of_week", ((F.dayofweek("feed_datetime") + 5) % 7 + 1).cast("int"))
    .withColumn("feed_hour", F.hour("feed_datetime"))
    .withColumn("current_delay_band", delay_band(F.col("current_delay_seconds")))
    .withColumn("previous_delay_band", delay_band(F.col("previous_delay_seconds"))))
historical = (historical.withColumn("is_weekend", F.col("day_of_week").isin(6, 7))
    .withColumn("time_band", F.when(F.col("feed_hour") < 6, "night")
        .when(F.col("feed_hour") < 10, "morning_peak").when(F.col("feed_hour") < 16, "midday")
        .when(F.col("feed_hour") < 20, "evening_peak").otherwise("evening")))
HISTORICAL_VIEW = "gold13_historical_" + REFRESH_ID.replace("-", "")
historical.createOrReplaceTempView(HISTORICAL_VIEW)
try:
    grouped = spark.sql(f"""
    SELECT CASE WHEN grouping(route_id) = 0 THEN 'route'
                WHEN grouping(stop_id) = 0 THEN 'stop'
                WHEN grouping(service_date) = 0 THEN 'temporal'
                WHEN grouping(current_delay_band) = 0 THEN 'propagation'
                ELSE 'executive' END AS analytical_group,
        route_id, stop_id, service_date, day_of_week, feed_hour, is_weekend, time_band,
        current_delay_band, previous_delay_band,
        min(route_short_name) AS route_short_name, min(stop_name) AS stop_name,
        count(*) AS observation_count,
        count(DISTINCT feed_timestamp) AS distinct_snapshots,
        count(DISTINCT observed_service_date) AS service_date_count,
        count(DISTINCT observed_route_id) AS route_count,
        count(DISTINCT trip_id) AS trip_count,
        count(DISTINCT vehicle_id) AS vehicle_count,
        count(DISTINCT observed_stop_id) AS stop_count,
        avg(realized_delay_seconds) AS mean_delay_seconds,
        percentile_approx(realized_delay_seconds, array(0.5D, 0.90D, 0.95D, 0.99D), {PERCENTILE_ACCURACY}) AS delay_percentiles,
        sum(CASE WHEN realized_major_delay_flag = 1 THEN 1 ELSE 0 END) AS major_delay_count,
        sum(CASE WHEN realized_delay_seconds < -60 THEN 1 ELSE 0 END) AS early_count,
        sum(CASE WHEN realized_delay_seconds >= -60 AND realized_delay_seconds < 180 THEN 1 ELSE 0 END) AS on_time_count,
        sum(CASE WHEN realized_delay_seconds >= 180 AND realized_delay_seconds < 300 THEN 1 ELSE 0 END) AS moderate_delay_count,
        avg(current_delay_seconds) AS mean_current_delay_seconds,
        avg(previous_delay_seconds) AS mean_previous_stop_delay_seconds,
        count(current_delay_seconds) AS current_delay_observation_count,
        count(previous_delay_seconds) AS previous_delay_observation_count,
        sum(CASE WHEN current_delay_seconds IS NOT NULL AND previous_delay_seconds IS NOT NULL THEN 1 ELSE 0 END) AS paired_delay_observation_count,
        avg(current_delay_seconds - previous_delay_seconds) AS mean_delay_change_from_previous_stop
    FROM {HISTORICAL_VIEW}
    GROUP BY GROUPING SETS ((), (route_id), (stop_id),
        (service_date, day_of_week, feed_hour, is_weekend, time_band),
        (current_delay_band, previous_delay_band))
    """)
    aggregate_schema = grouped.schema
    aggregate_rows = grouped.limit(MAX_AGGREGATE_ROWS + 1).collect()
    if len(aggregate_rows) > MAX_AGGREGATE_ROWS:
        raise ValueError("Compact aggregate limit exceeded; review scale. No Gold tables written.")
    aggregates = spark.createDataFrame(aggregate_rows, aggregate_schema)
    del aggregate_rows
finally:
    spark.catalog.dropTempView(HISTORICAL_VIEW)
for i, name in enumerate(["median_delay_seconds", "p90_delay_seconds", "p95_delay_seconds", "p99_delay_seconds"]):
    aggregates = aggregates.withColumn(name, F.col("delay_percentiles")[i])
aggregates = aggregates.drop("delay_percentiles")
for category in ["major_delay", "early", "on_time", "moderate_delay"]:
    aggregates = aggregates.withColumn(category + "_rate", F.col(category + "_count") / F.col("observation_count"))
BASE_METRICS = ["observation_count", "mean_delay_seconds", "median_delay_seconds", "p90_delay_seconds",
    "p95_delay_seconds", "p99_delay_seconds", "major_delay_count", "major_delay_rate", "early_count", "early_rate",
    "on_time_count", "on_time_rate", "moderate_delay_count", "moderate_delay_rate"]
gold = {}


# COMMAND ----------

# MAGIC %md
# MAGIC ## Executive Overview Metrics
# MAGIC A single row combines network-wide historical aggregates with independently aggregated latest-snapshot predictions. This summary-to-summary cross join never joins labels to individual predictions. No predictive trend is inferred from one snapshot.

# COMMAND ----------

historical_executive = aggregates.filter(F.col("analytical_group") == "executive").select(
    F.col("observation_count").alias("total_observations"), "distinct_snapshots",
    F.col("service_date_count").alias("distinct_service_dates"), F.col("route_count").alias("distinct_routes"),
    F.col("trip_count").alias("distinct_trips"),
    (F.col("vehicle_count") if "vehicle_id" in historical_raw.columns else F.lit(None).cast("long")).alias("distinct_vehicles"),
    *[F.col(c).alias(c.replace("delay_seconds", "realized_delay_seconds")) for c in
        ["mean_delay_seconds", "median_delay_seconds", "p90_delay_seconds", "p95_delay_seconds", "p99_delay_seconds"]],
    *[c for c in BASE_METRICS if c.endswith(("_count", "_rate")) and c != "observation_count"])
prediction_executive = predictions_raw.agg(
    F.max("feed_timestamp").alias("latest_prediction_feed_timestamp"),
    F.max("feed_datetime").alias("latest_prediction_feed_datetime"), F.count("*").alias("latest_scored_rows"),
    F.sum("predicted_major_delay_flag").alias("latest_predicted_major_delay_count"),
    F.avg("predicted_major_delay_flag").alias("latest_predicted_major_delay_rate"),
    F.avg("major_delay_probability").alias("latest_probability_mean"),
    *[F.percentile_approx("major_delay_probability", q, PERCENTILE_ACCURACY).alias(f"latest_probability_p{int(q*100)}")
        for q in [0.90, 0.95, 0.99]])
gold["executive_overview"] = historical_executive.crossJoin(prediction_executive)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Route Reliability
# MAGIC One row per `route_id`. Descending dense ranks include only routes with at least 1,000 observations; smaller samples remain visible with NULL ranks. Ties share rank. Rankings describe the historical source without confidence or causal claims.

# COMMAND ----------

def eligible_ranks(df, key, minimum, metric_rank_pairs):
    result = df.withColumn("ranking_eligible", F.col("observation_count") >= minimum)
    eligible = result.filter("ranking_eligible").select(key, *[m for m, _ in metric_rank_pairs])
    for metric, rank_name in metric_rank_pairs:
        eligible = eligible.withColumn(rank_name, F.dense_rank().over(Window.orderBy(F.col(metric).desc())))
    return result.join(eligible.select(key, *[r for _, r in metric_rank_pairs]), key, "left")
routes = aggregates.filter(F.col("analytical_group") == "route").select(
    "route_id", "route_short_name", "trip_count", "stop_count", "service_date_count", *BASE_METRICS)
gold["route_reliability"] = eligible_ranks(routes, "route_id", MIN_ROUTE_OBSERVATIONS,
    [("major_delay_rate", "major_delay_rate_rank_desc"), ("mean_delay_seconds", "mean_delay_rank_desc"),
     ("p95_delay_seconds", "p95_delay_rank_desc")])


# COMMAND ----------

# MAGIC %md
# MAGIC ## Stop Reliability
# MAGIC One row per current observation `stop_id`, with an optional name. Descending dense ranks require at least 500 observations. The realized next-stop outcome is associated with current stop context; these are not realized arrivals measured at the named stop itself.

# COMMAND ----------

stops = aggregates.filter(F.col("analytical_group") == "stop").select(
    "stop_id", "stop_name", F.col("route_count").alias("distinct_routes"),
    F.col("trip_count").alias("distinct_trips"), "service_date_count", *BASE_METRICS)
gold["stop_reliability"] = eligible_ranks(stops, "stop_id", MIN_STOP_OBSERVATIONS,
    [("major_delay_rate", "major_delay_rate_rank_desc"), ("p95_delay_seconds", "p95_delay_rank_desc")])


# COMMAND ----------

# MAGIC %md
# MAGIC ## Temporal Reliability
# MAGIC Grain: `service_date`, ISO `day_of_week`, `feed_hour`. Weekend and time band derive from the feed clock, not the service date. Night covers 00-05, morning peak 06-09, midday 10-15, evening peak 16-19, and evening 20-23. Cross-midnight service dates remain intact.

# COMMAND ----------

gold["temporal_reliability"] = aggregates.filter(F.col("analytical_group") == "temporal").select(
    "service_date", "day_of_week", "feed_hour", "is_weekend", "time_band", *BASE_METRICS)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Delay Propagation
# MAGIC At most 25 current/previous delay-band combinations summarize the association between observed delay state and realized next-stop outcomes. `mean_delay_change_from_previous_stop` is current arrival delay minus lag-1 arrival delay, averaged over non-NULL pairs. Available-value counts expose lag coverage. Missing history remains `unknown`; it is not imputed. Association does not establish causation.

# COMMAND ----------

gold["delay_propagation"] = aggregates.filter(F.col("analytical_group") == "propagation").select(
    "current_delay_band", "previous_delay_band", "observation_count",
    "current_delay_observation_count", "previous_delay_observation_count", "paired_delay_observation_count",
    "mean_current_delay_seconds", "mean_previous_stop_delay_seconds",
    F.col("mean_delay_seconds").alias("mean_realized_next_stop_delay_seconds"),
    F.col("median_delay_seconds").alias("median_realized_next_stop_delay_seconds"),
    F.col("p90_delay_seconds").alias("p90_realized_next_stop_delay_seconds"),
    "major_delay_rate", "mean_delay_change_from_previous_stop")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Predictive Operations
# MAGIC Preserve the serving observation grain and values. An explicit allowlist excludes targets, label provenance and model/source lineage from this operational table; lineage is kept in refresh metadata. Optional descriptions and context remain NULL when absent.
# MAGIC
# MAGIC Presentation bands: low `p < 0.20`, medium `0.20 <= p <= 0.45`, high `0.45 < p < 0.80`, critical `p >= 0.80`. The flag still uses strict `p > 0.45`. Probability dense ranks are descending within snapshot and snapshot/route; ties share rank.

# COMMAND ----------

PREDICTIVE_COLUMNS = ["feed_timestamp", "feed_datetime", "service_date", "entity_id", "route_id",
    "route_short_name", "trip_id", "vehicle_id", "stop_id", "stop_name", "stop_sequence",
    "current_arrival_delay_seconds", "predicted_next_stop_delay_seconds", "major_delay_probability",
    "predicted_major_delay_flag", "classification_threshold", "active_alert_flag",
    "vehicle_position_available_flag", "current_status"]
optional_types = {"route_short_name": "string", "stop_name": "string", "current_status": "string",
    "active_alert_flag": "int", "vehicle_position_available_flag": "int"}
predictive = predictions_raw.select(*[
    optional_column(predictions_raw, c, optional_types[c]) if c in optional_types else F.col(c)
    for c in PREDICTIVE_COLUMNS])
gold["predictive_operations"] = (predictive.withColumn("risk_band", risk_band(F.col("major_delay_probability")))
    .withColumn("probability_rank_within_snapshot", F.dense_rank().over(
        Window.partitionBy("feed_timestamp").orderBy(F.col("major_delay_probability").desc())))
    .withColumn("route_probability_rank_within_snapshot", F.dense_rank().over(
        Window.partitionBy("feed_timestamp", "route_id").orderBy(F.col("major_delay_probability").desc()))))
PREDICTIVE_OUTPUT_COLUMNS = PREDICTIVE_COLUMNS + ["risk_band", "probability_rank_within_snapshot", "route_probability_rank_within_snapshot"]


# COMMAND ----------

# MAGIC %md
# MAGIC ## Model Performance
# MAGIC Frozen notebook 11 metrics are recorded verbatim in long form. TEST and validation are separate datasets; operational probabilities are not performance metrics. No metric is recomputed on the refit dataset. Regression uses Persistence with a -71.0-second missing-value fallback; classification uses the frozen RandomForestClassifier and strict 0.45 threshold.
# MAGIC
# MAGIC Registration status is read from the serving snapshot (currently reported as `DEFERRED_SERVERLESS_LIMITATION`). It describes the serving registration attempt; reference metrics describe the original frozen evaluation.

# COMMAND ----------

FROZEN_METRICS = [
    ("regression", "Persistence", "test", "MAE", 136.9818),
    ("regression", "Persistence", "test", "RMSE", 401.8525),
    ("regression", "Persistence", "test", "R2", 0.6940),
    ("regression", "Persistence", "test", "WAPE", 0.3293),
    ("classification", "RandomForestClassifier", "test", "accuracy", 0.9401),
    ("classification", "RandomForestClassifier", "test", "precision", 0.8450),
    ("classification", "RandomForestClassifier", "test", "recall", 0.8123),
    ("classification", "RandomForestClassifier", "test", "F1", 0.8283),
    ("classification", "RandomForestClassifier", "test", "ROC_AUC", 0.9690),
    ("classification", "RandomForestClassifier", "test", "PR_AUC", 0.9063),
    ("regression", "Persistence", "validation", "MAE", 141.5978),
    ("classification", "RandomForestClassifier", "validation", "F1", 0.8389),
    ("classification", "RandomForestClassifier", "validation", "ROC_AUC", 0.9697),
    ("classification", "RandomForestClassifier", "validation", "PR_AUC", 0.9170)]
metric_rows = [(task, model, dataset, metric, value,
    CLASSIFICATION_THRESHOLD if task == "classification" else None,
    "Persistence", "RandomForestClassifier", REGISTRATION_STATUS,
    json.dumps(RF_PARAMS, sort_keys=True) if task == "classification" else None,
    PERSISTENCE_FALLBACK if task == "regression" else None,
    "11_model_tuning_and_selection", "Frozen reference evaluation; not recomputed on refit or operational data.")
    for task, model, dataset, metric, value in FROZEN_METRICS]
gold["model_performance"] = spark.createDataFrame(metric_rows,
    "task STRING, model_name STRING, dataset STRING, metric_name STRING, metric_value DOUBLE, threshold DOUBLE, "
    "regression_model STRING, classification_model STRING, registration_status STRING, parameters_json STRING, "
    "regression_fallback_seconds DOUBLE, metric_source STRING, notes STRING")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Validation
# MAGIC Validate every output before writes, then validate each committed Delta version. Checks cover nonempty tables, non-NULL grain, duplicate grain, finite numeric values, rates/probabilities in [0, 1], exact frozen metrics, strict classification boundaries, persistence consistency, band partitions, row reconciliation and ranking eligibility. NULL descriptions, ineligible ranks and unavailable lag means are intentional.
# MAGIC
# MAGIC A failed check raises an exception and blocks readiness. Successful publication is recorded in a refresh manifest with output Delta versions; it is not a multi-table transaction.

# COMMAND ----------

GRAINS = {
    "executive_overview": [], "route_reliability": ["route_id"], "stop_reliability": ["stop_id"],
    "temporal_reliability": ["service_date", "day_of_week", "feed_hour"],
    "delay_propagation": ["current_delay_band", "previous_delay_band"],
    "predictive_operations": OBS_KEY, "model_performance": ["task", "dataset", "metric_name"],
    "analytics_refresh_metadata": []}
PURPOSES = {
    "executive_overview": "Network KPIs and latest predictive snapshot",
    "route_reliability": "Observation-weighted route reliability and eligible ranks",
    "stop_reliability": "Current-stop context and realized next-stop reliability",
    "temporal_reliability": "Service date and civil feed-hour reliability",
    "delay_propagation": "Current/previous delay state and realized outcomes",
    "predictive_operations": "Latest serving predictions for operational inspection",
    "model_performance": "Frozen validation and TEST reference metrics",
    "analytics_refresh_metadata": "Source versions, scoring lineage and successful refresh manifest"}

def quality_report(name, df):
    keys = GRAINS[name]
    require_columns(df, keys)
    numeric_names = [f.name for f in df.schema.fields if isinstance(f.dataType, T.NumericType)]
    rates = [c for c in df.columns if c.endswith("_rate")]
    probabilities = [c for c in df.columns if c == "major_delay_probability" or c.startswith("latest_probability_")]
    expressions = [F.count("*").alias("row_count"),
        n_where(any_condition([F.col(c).isNull() for c in keys])).alias("null_keys"),
        n_where(any_condition([nonfinite(F.col(c)) for c in numeric_names])).alias("nonfinite_metrics"),
        n_where(any_condition([F.col(c).isNull() | ~F.col(c).between(0, 1) for c in rates])).alias("invalid_rates"),
        n_where(any_condition([F.col(c).isNull() | ~F.col(c).between(0, 1) for c in probabilities])).alias("invalid_probabilities")]
    if name in ["route_reliability", "stop_reliability", "temporal_reliability"]:
        mandatory = BASE_METRICS
    elif name == "executive_overview":
        mandatory = [c for c in df.columns if c != "distinct_vehicles"]
    elif name == "delay_propagation":
        mandatory = ["observation_count", "mean_realized_next_stop_delay_seconds", "median_realized_next_stop_delay_seconds", "p90_realized_next_stop_delay_seconds", "major_delay_rate"]
    elif name == "predictive_operations":
        mandatory = [c for c in PREDICTIVE_COLUMNS if c not in list(optional_types) + ["current_arrival_delay_seconds"]]
    elif name == "model_performance":
        mandatory = ["model_name", "metric_value", "registration_status", "metric_source"]
    else:
        mandatory = ["refresh_id", "refresh_timestamp", "historical_source_version", "prediction_source_version", "model_run_id", "registration_status", "gold_versions_json"]
    expressions.append(n_where(any_condition([F.col(c).isNull() for c in mandatory])).alias("required_nulls"))
    report = df.agg(*expressions).first().asDict()
    report["duplicate_groups"] = duplicate_groups(df, keys) if keys else max(report["row_count"] - 1, 0)
    errors = {k: v for k, v in report.items() if k != "row_count" and v != 0}
    if report["row_count"] <= 0 or (not keys and report["row_count"] != 1) or errors:
        raise ValueError(f"{name}: invalid Gold output {report}")
    report.update(table_name=DESTINATIONS[name], quality_status="PASSED", duplicate_check="PASSED")
    return report

def validate_contracts(tables):
    executive = tables["executive_overview"].first().asDict()
    if executive["total_observations"] != HISTORICAL_ROWS or executive["latest_scored_rows"] != PREDICTION_ROWS:
        raise ValueError("Executive/source row count mismatch")
    for name in ["route_reliability", "stop_reliability", "temporal_reliability", "delay_propagation"]:
        if tables[name].agg(F.sum("observation_count")).first()[0] != HISTORICAL_ROWS:
            raise ValueError(f"Historical coverage reconciliation failed: {name}")
    for name in ["executive_overview", "route_reliability", "stop_reliability", "temporal_reliability"]:
        df = tables[name]
        count_name = "total_observations" if name == "executive_overview" else "observation_count"
        categories = ["early", "on_time", "moderate_delay", "major_delay"]
        count_sum = sum((F.col(c + "_count") for c in categories), F.lit(0))
        invalid = [(count_sum != F.col(count_name))]
        invalid += [F.abs(F.col(c + "_rate") - F.col(c + "_count") / F.col(count_name)) > 1e-12 for c in categories]
        assert_empty(df, any_condition(invalid), f"Delay-band partition or denominator mismatch: {name}")
    for name, minimum in [("route_reliability", MIN_ROUTE_OBSERVATIONS), ("stop_reliability", MIN_STOP_OBSERVATIONS)]:
        df = tables[name]
        invalid = [~F.col("ranking_eligible").eqNullSafe(F.col("observation_count") >= minimum)]
        invalid += [(F.col("ranking_eligible") & (F.col(rank).isNull() | (F.col(rank) < 1)))
            | (~F.col("ranking_eligible") & F.col(rank).isNotNull()) for rank in df.columns if rank.endswith("_rank_desc")]
        assert_empty(df, any_condition(invalid), f"Invalid eligible rank: {name}")
    operational = tables["predictive_operations"]
    if set(operational.columns) != set(PREDICTIVE_OUTPUT_COLUMNS):
        raise ValueError("Gold leakage check failed: operational schema differs from explicit allowlist")
    if operational.count() != PREDICTION_ROWS:
        raise ValueError("Predictive observation count mismatch")
    probability = F.col("major_delay_probability")
    assert_empty(operational,
        ~F.col("predicted_major_delay_flag").eqNullSafe((probability > CLASSIFICATION_THRESHOLD).cast("int"))
        | ~F.col("classification_threshold").eqNullSafe(F.lit(CLASSIFICATION_THRESHOLD))
        | ~F.col("risk_band").eqNullSafe(risk_band(probability))
        | ~F.col("predicted_next_stop_delay_seconds").eqNullSafe(expected_persistence), "Operational frozen contract mismatch")
    actual = {(r.task, r.model_name, r.dataset, r.metric_name): r.metric_value
        for r in tables["model_performance"].limit(len(FROZEN_METRICS) + 1).collect()}
    expected = {(task, model, dataset, metric): value for task, model, dataset, metric, value in FROZEN_METRICS}
    if actual != expected:
        raise ValueError("Frozen notebook 11 metric mismatch")
    assert_empty(tables["model_performance"],
        ((F.col("task") == "classification") & ~F.col("threshold").eqNullSafe(F.lit(CLASSIFICATION_THRESHOLD)))
        | ((F.col("task") == "regression") & F.col("threshold").isNotNull()), "Metric threshold mismatch")

# Boundary fixture: rule validation only, no new predictive evaluation.
boundaries = spark.createDataFrame([
    (-61.0, "early", 0.1999, "low_risk", 0), (-60.0, "on_time", 0.20, "medium_risk", 0),
    (179.0, "on_time", 0.45, "medium_risk", 0), (180.0, "moderate_delay", 0.4501, "high_risk", 1),
    (299.0, "moderate_delay", 0.7999, "high_risk", 1), (300.0, "major_delay", 0.80, "critical_risk", 1),
    (None, "unknown", 1.0, "critical_risk", 1)],
    "delay DOUBLE, expected_band STRING, p DOUBLE, expected_risk STRING, expected_flag INT")
assert_empty(boundaries, ~delay_band(F.col("delay")).eqNullSafe(F.col("expected_band"))
    | ~risk_band(F.col("p")).eqNullSafe(F.col("expected_risk"))
    | ~((F.col("p") > CLASSIFICATION_THRESHOLD).cast("int")).eqNullSafe(F.col("expected_flag")), "Boundary fixture failed")
try:
    prewrite_reports = [quality_report(name, df) for name, df in gold.items()]
    validate_contracts(gold)
except Exception as exc:
    print("READY FOR DATABRICKS SQL / AI/BI DASHBOARDS: NO. Pre-write blocker:", str(exc))
    raise
display(spark.createDataFrame(prewrite_reports))
print("Pre-write validation PASSED; historical realized and operational prediction paths remain separate.")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Persist Gold Tables
# MAGIC Overwrite only the eight allowlisted Gold Delta tables, with schema replacement. Validate each committed version and cross-table contracts before publishing refresh metadata. Failures stop execution and report completed writes; a partial refresh must not be treated as a validated release. Consumers should compare the successful manifest with current Gold versions.

# COMMAND ----------

persisted = {}
persisted_versions = {}
persisted_reports = []
written_tables = []
try:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}")
    for name in TABLE_NAMES[:-1]:
        destination = DESTINATIONS[name]
        gold[name].write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(destination)
        written_tables.append(destination)
        saved, version = pin_table(destination)
        persisted[name] = saved
        persisted_versions[name] = version
        persisted_reports.append(quality_report(name, saved))
    validate_contracts(persisted)
    metadata = spark.createDataFrame([(
        REFRESH_ID, REFRESH_TIMESTAMP, HISTORICAL_TABLE, HISTORICAL_VERSION, HISTORICAL_ROWS,
        PREDICTION_TABLE, PREDICTION_VERSION, PREDICTION_ROWS,
        prediction_checks["feed_timestamp"], prediction_checks["scored_at"],
        prediction_checks["model_run_id"], REGISTRATION_STATUS,
        json.dumps({DESTINATIONS[k]: v for k, v in persisted_versions.items()}, sort_keys=True),
        "PASSED", "YES", MIN_ROUTE_OBSERVATIONS, MIN_STOP_OBSERVATIONS,
        CLASSIFICATION_THRESHOLD, "Europe/Rome")],
        "refresh_id STRING, refresh_timestamp TIMESTAMP, historical_source_table STRING, historical_source_version LONG, "
        "historical_row_count LONG, prediction_source_table STRING, prediction_source_version LONG, prediction_row_count LONG, "
        "prediction_feed_timestamp LONG, prediction_scored_at TIMESTAMP, model_run_id STRING, registration_status STRING, "
        "gold_versions_json STRING, quality_status STRING, dashboard_ready STRING, min_route_observations INT, "
        "min_stop_observations INT, classification_threshold DOUBLE, reporting_timezone STRING")
    quality_report("analytics_refresh_metadata", metadata)
    metadata.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(DESTINATIONS["analytics_refresh_metadata"])
    written_tables.append(DESTINATIONS["analytics_refresh_metadata"])
    saved, version = pin_table(DESTINATIONS["analytics_refresh_metadata"])
    persisted["analytics_refresh_metadata"] = saved
    persisted_versions["analytics_refresh_metadata"] = version
    persisted_reports.append(quality_report("analytics_refresh_metadata", saved))
except Exception as exc:
    print("READY FOR DATABRICKS SQL / AI/BI DASHBOARDS: NO")
    print("Remaining blocker:", type(exc).__name__, str(exc))
    print("Tables written before failure:", written_tables)
    print("Partial refresh: rerun after resolving the blocker.")
    raise


# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Gold Inventory
# MAGIC Counts and quality statuses come from persisted Delta versions. Top-route and top-stop displays include only ranking-eligible groups. Predictive samples are ordered by probability, with observation keys as deterministic tie-breakers.

# COMMAND ----------

inventory = [(DESTINATIONS[name], next(r["row_count"] for r in persisted_reports if r["table_name"] == DESTINATIONS[name]),
    ", ".join(GRAINS[name]) if GRAINS[name] else "single row", PURPOSES[name], persisted_versions[name]) for name in TABLE_NAMES]
display(spark.createDataFrame(inventory, "table_name STRING, row_count LONG, grain STRING, purpose STRING, delta_version LONG"))
display(spark.createDataFrame(persisted_reports))
executive = persisted["executive_overview"].first().asDict()
display(persisted["executive_overview"].select("total_observations", "major_delay_rate",
    "median_realized_delay_seconds", "p95_realized_delay_seconds", "latest_predicted_major_delay_rate", "latest_scored_rows"))
display(persisted["route_reliability"].filter("ranking_eligible").orderBy(
    F.col("major_delay_rate").desc(), F.col("observation_count").desc(), "route_id").limit(10))
display(persisted["stop_reliability"].filter("ranking_eligible").orderBy(
    F.col("major_delay_rate").desc(), F.col("observation_count").desc(), "stop_id").limit(10))
display(persisted["temporal_reliability"].orderBy("service_date", "day_of_week", "feed_hour").limit(10))
display(persisted["delay_propagation"].orderBy(F.col("observation_count").desc(), "current_delay_band", "previous_delay_band").limit(10))
display(persisted["predictive_operations"].orderBy(F.col("major_delay_probability").desc(), *OBS_KEY).limit(10))
display(persisted["model_performance"].orderBy("task", "dataset", "metric_name"))
final_report = {
    "source": {"historical_table": HISTORICAL_TABLE, "historical_version": HISTORICAL_VERSION,
        "historical_rows": HISTORICAL_ROWS, "prediction_table": PREDICTION_TABLE,
        "prediction_version": PREDICTION_VERSION, "prediction_rows": PREDICTION_ROWS},
    "gold_tables": persisted_reports,
    "executive_kpis": {k: executive[k] for k in ["major_delay_rate", "median_realized_delay_seconds",
        "p95_realized_delay_seconds", "latest_predicted_major_delay_rate"]},
    "model_performance": {"regression_test_MAE": 136.9818, "classification_test_F1": 0.8283,
        "classification_test_ROC_AUC": 0.9690, "classification_test_PR_AUC": 0.9063, "threshold": CLASSIFICATION_THRESHOLD},
    "quality": {"gold_leakage_check": "PASSED", "invalid_rates": sum(r["invalid_rates"] for r in persisted_reports),
        "invalid_probabilities": sum(r["invalid_probabilities"] for r in persisted_reports),
        "duplicates": sum(r["duplicate_groups"] for r in persisted_reports), "runtime_errors": 0},
    "status": {"ready_for_databricks_sql_ai_bi_dashboards": "YES", "remaining_blockers": [],
        "registration_status": REGISTRATION_STATUS, "refresh_id": REFRESH_ID}}
print(json.dumps(final_report, indent=2, default=str))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Limitations
# MAGIC - Reliability describes labeled observation coverage, not all scheduled service, unique arrivals or passenger experience. Trip/vehicle counts are distinct identifiers, not reconstructed journeys.
# MAGIC - The upstream +/-12-hour realized-label gate mitigates diagnosed rollover artifacts. This notebook validates that contract without changing labels, provisional targets or source rows. Nighttime observations remain included.
# MAGIC - Route/stop slices describe realized next-stop outcomes by current context. Propagation summaries are descriptive associations, not causal estimates.
# MAGIC - One operational snapshot supports prioritization, not predictive time trends. Notebook 12 may score a historical latest-snapshot replay; this output does not imply a live feed or out-of-sample evaluation.
# MAGIC - Reference TEST metrics belong to the original frozen evaluation. Refit predictions have no newly measured performance here. Deferred registration is reported transparently and does not prevent use of validated predictions.
# MAGIC - Exact distinct counts and grouping sets require distributed shuffles. Only bounded compact aggregates cross the driver boundary. Source integrity checks require additional narrow scans; unsupported Serverless caching is avoided.
# MAGIC - Optional descriptions can remain NULL. Missing propagation history has an explicit unknown band; conditional means can be NULL with zero available observations. Percentiles are approximate.
# MAGIC - Gold overwrites are independent Delta commits. Run one refresh at a time and use the successful manifest to identify a complete release. SQL/AI/BI permissions and dashboard creation remain separate deployment steps.