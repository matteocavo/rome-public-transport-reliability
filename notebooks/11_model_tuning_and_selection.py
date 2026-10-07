# Databricks notebook source
# MAGIC %md
# MAGIC # Model Tuning and Final Selection
# MAGIC Focus manual tuning on validation performance while preserving notebook 10's realized targets, feature contract and purged temporal split. Freeze both model choices and the classification threshold before evaluating only the selected solutions on test.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Runtime Configuration
# MAGIC Use built-in Spark ML and MLflow in Databricks without new dependencies or persistent table writes. Set `SOURCE_VERSION` to notebook 10's Delta version for an exact source replay; otherwise the latest version is pinned once and benchmark shape differences are reported.

# COMMAND ----------

import math
import json
import hashlib
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.ml import Pipeline
from pyspark.ml.feature import Imputer, StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler, SQLTransformer
from pyspark.ml.regression import RandomForestRegressor, GBTRegressor
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.functions import vector_to_array
import mlflow


spark.conf.set("spark.sql.session.timeZone", "Europe/Rome")
SOURCE_TABLE = "rome_transport.features.next_stop_delay_labeled"
SOURCE_VERSION = None
REG_LABEL = "target_realized_next_stop_delay_seconds"
CLS_LABEL = "target_realized_major_delay_flag"
OBS_KEY = ["feed_timestamp", "entity_id", "trip_id", "stop_sequence", "stop_id"]
EXPERIMENT_NAME = "/Shared/rome_transport_next_stop_delay"
SEED = 42
LOG_MODEL_ARTIFACTS = False
def n_where(condition):
    return F.coalesce(F.sum(F.when(condition, 1).otherwise(0)), F.lit(0))

def fail_if(df, condition, message):
    if df.filter(condition).limit(1).count():
        raise ValueError(message)

def require_columns(df, names):
    missing = sorted(set(names) - set(df.columns))
    if missing:
        raise ValueError(f"Required columns missing: {missing}")

def duplicate_check(df):
    duplicates = df.groupBy(*OBS_KEY).count().filter(F.col("count") > 1)
    if duplicates.limit(1).count():
        raise ValueError("Duplicate modeling observation keys")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Modeling Dataset
# MAGIC Read the modeling-ready Delta source and validate observation grain, targets and future-label provenance. These dataset integrity checks and temporal coverage summaries do not calculate held-out predictive performance or inform model selection.

# COMMAND ----------

if SOURCE_VERSION is None:
    SOURCE_VERSION = int(spark.sql(f"DESCRIBE HISTORY {SOURCE_TABLE} LIMIT 1").select("version").first()[0])
model_df = spark.read.option("versionAsOf", SOURCE_VERSION).table(SOURCE_TABLE)
require_columns(model_df, OBS_KEY + [REG_LABEL, CLS_LABEL, "feed_datetime", "service_date", "route_id", "vehicle_id",
    "label_source_feed_timestamp", "label_source_service_date", "target_next_stop_id", "target_next_stop_sequence",
    "label_source_trip_id", "label_source_stop_id", "label_source_stop_sequence", "label_horizon_seconds",
    "current_arrival_delay_seconds", "historical_route_stop_mean_delay", "historical_route_mean_delay"])
source_summary = model_df.agg(
    F.count("*").alias("total_rows"), F.countDistinct("feed_timestamp").alias("distinct_feed_snapshots"),
    F.countDistinct("service_date").alias("distinct_service_dates"), F.countDistinct("route_id").alias("distinct_routes"),
    F.countDistinct("trip_id").alias("distinct_trips"),
    F.min("feed_datetime").alias("min_feed_datetime"), F.max("feed_datetime").alias("max_feed_datetime"),
    F.min("service_date").alias("min_service_date"), F.max("service_date").alias("max_service_date"))
source_summary.show(truncate=False, vertical=True)
source_metrics = source_summary.first().asDict()
duplicate_check(model_df)
fail_if(model_df, F.col(REG_LABEL).isNull() | F.isnan(F.col(REG_LABEL).cast("double"))
    | (F.abs(F.col(REG_LABEL).cast("double")) == float("inf")) | F.col(CLS_LABEL).isNull()
    | ~F.col(CLS_LABEL).isin(0, 1), "Invalid modeling labels")
fail_if(model_df, ~F.col(CLS_LABEL).eqNullSafe((F.col(REG_LABEL) >= 300).cast("int")), "Classification label mismatch")
fail_if(model_df, F.col("feed_timestamp").isNull()
    | ~F.col("feed_datetime").eqNullSafe(F.timestamp_seconds("feed_timestamp"))
    | F.col("label_source_feed_timestamp").isNull()
    | (F.col("label_source_feed_timestamp") <= F.col("feed_timestamp"))
    | ~F.col("label_horizon_seconds").eqNullSafe(F.col("label_source_feed_timestamp") - F.col("feed_timestamp"))
    | ~F.col("label_horizon_seconds").between(1, 1800)
    | ~F.col("label_source_trip_id").eqNullSafe(F.col("trip_id"))
    | ~F.col("label_source_service_date").eqNullSafe(F.col("service_date"))
    | ~F.col("label_source_stop_id").eqNullSafe(F.col("target_next_stop_id"))
    | ~F.col("label_source_stop_sequence").eqNullSafe(F.col("target_next_stop_sequence")), "Invalid label causality/provenance")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Preserve the Feature Contract
# MAGIC Retain notebook 10's explicit numeric and categorical allowlists, including existing missingness indicators. Targets, provisional targets, identifiers and label provenance remain excluded from the assembled model inputs.

# COMMAND ----------

candidate_numeric_features = """feed_hour feed_minute day_of_week is_weekend minutes_since_midnight
current_arrival_delay_seconds current_departure_delay_seconds abs_current_delay_seconds
is_currently_delayed_flag is_currently_early_flag lag_1_arrival_delay_seconds lag_2_arrival_delay_seconds
lag_3_arrival_delay_seconds rolling_mean_delay_last_3_stops rolling_max_delay_last_3_stops
rolling_std_delay_last_3_stops delay_change_from_previous_stop delay_acceleration
scheduled_seconds_to_next_stop distance_to_next_stop_meters vehicle_speed vehicle_bearing vehicle_odometer
current_stop_sequence vehicle_position_staleness_seconds vehicle_position_available_flag active_alert_flag
active_alert_count active_detour_flag active_construction_flag historical_route_mean_delay
historical_stop_mean_delay historical_route_stop_mean_delay historical_route_hour_mean_delay
historical_route_observation_count historical_stop_observation_count missing_vehicle_position
missing_current_arrival_delay missing_current_departure_delay missing_historical_route_delay
missing_historical_stop_delay missing_alert_snapshot trip_progress_ratio""".split()
candidate_categorical_features = ["route_id", "route_type", "direction_id", "agency_id", "current_status"]
required_features = ["feed_hour", "feed_minute", "day_of_week", "is_weekend", "minutes_since_midnight", "route_id"]
optional_features = sorted(set(candidate_numeric_features + candidate_categorical_features) - set(required_features))
forbidden_columns = """target_realized_next_stop_delay_seconds target_realized_major_delay_flag
provisional_next_stop_delay_seconds label_source_feed_timestamp label_source_feed_datetime
label_horizon_seconds label_source_stop_id label_source_stop_sequence label_source_trip_id
label_realization_quality provisional_target_error_seconds absolute_provisional_target_error_seconds
target_next_stop_id target_next_stop_sequence target_next_stop_delay_seconds target_major_delay_flag
trip_id vehicle_id entity_id stop_id major_delay_threshold_seconds future_observed_delay_seconds
source_delta_version""".split()
forbidden_columns = sorted(set(forbidden_columns + [name for name in model_df.columns
    if name.startswith(("label_", "labeling_", "target_", "provisional_")) or name.endswith("_audit")]))
require_columns(model_df, required_features)
assert not set(candidate_numeric_features + candidate_categorical_features) & set(forbidden_columns)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Reproduce Temporal Split
# MAGIC Order distinct feed timestamps and reproduce the 70/15/15 allocation from notebook 10 without random splitting. Purge training labels realized at or after validation starts and validation labels realized at or after test starts.

# COMMAND ----------

def split_sizes(n):
    if n < 3:
        raise ValueError("At least three distinct feed timestamps are needed for train/validation/test; no random fallback is allowed")
    train_n = max(1, min(n - 2, int(0.70 * n)))
    validation_end = max(train_n + 1, min(n - 1, int(0.85 * n)))
    return train_n, validation_end

timestamps = model_df.select("feed_timestamp").distinct()
total_snapshots = timestamps.count()
if total_snapshots < 30:
    print("Current temporal coverage is limited; model metrics are preliminary and should not be treated as production estimates.")
train_n, validation_end = split_sizes(total_snapshots)
timestamps = timestamps.withColumn("snapshot_rank", F.row_number().over(Window.orderBy("feed_timestamp")))
cutoffs = timestamps.agg(
    F.max(F.when(F.col("snapshot_rank") == train_n + 1, F.col("feed_timestamp"))).alias("validation_start"),
    F.max(F.when(F.col("snapshot_rank") == validation_end + 1, F.col("feed_timestamp"))).alias("test_start")).first().asDict()
validation_start, test_start = int(cutoffs["validation_start"]), int(cutoffs["test_start"])
assigned = model_df.withColumn("split", F.when(F.col("feed_timestamp") < validation_start, "train")
    .when(F.col("feed_timestamp") < test_start, "validation").otherwise("test"))


# COMMAND ----------

eligible = ((F.col("split") == "train") & (F.col("label_source_feed_timestamp") < validation_start))     | ((F.col("split") == "validation") & (F.col("label_source_feed_timestamp") < test_start))     | (F.col("split") == "test")
assigned.groupBy("split").agg(F.count("*").alias("rows_before_purge"),
    n_where(~eligible).alias("boundary_crossing_rows_purged")).orderBy("split").show(truncate=False)
split_df = assigned.filter(eligible)
parts = {name: split_df.filter(F.col("split") == name) for name in ["train", "validation", "test"]}
split_summary = split_df.groupBy("split").agg(
    F.count("*").alias("row_count"), F.countDistinct("feed_timestamp").alias("distinct_feed_snapshots"),
    F.min("feed_datetime").alias("min_feed_datetime"), F.max("feed_datetime").alias("max_feed_datetime"),
    F.min("feed_timestamp").alias("min_feed_timestamp"), F.max("feed_timestamp").alias("max_feed_timestamp"))
split_summary.orderBy("min_feed_timestamp").show(truncate=False)
split_stats = {row["split"]: row.asDict() for row in split_summary.collect()}  # At most three scalar summaries.
if set(split_stats) != {"train", "validation", "test"}:
    raise ValueError("Label-availability purging leaves an empty temporal partition; collect more history or review explicit cutoffs")
assert split_stats["train"]["max_feed_timestamp"] < split_stats["validation"]["min_feed_timestamp"]
assert split_stats["validation"]["max_feed_timestamp"] < split_stats["test"]["min_feed_timestamp"]
overlap_count = split_df.groupBy("feed_timestamp").agg(F.countDistinct("split").alias("n")).filter(F.col("n") > 1).count()
print("overlapping_timestamp_count:", overlap_count)
assert overlap_count == 0
print("Snapshot counts:", {"total": total_snapshots, **{name: stat["distinct_feed_snapshots"] for name, stat in split_stats.items()}})



fail_if(parts["train"], F.col("label_source_feed_timestamp") >= validation_start, "Training boundary purge failed")
fail_if(parts["validation"], F.col("label_source_feed_timestamp") >= test_start, "Validation boundary purge failed")
reported_source = dict(total_rows=9975703, distinct_feed_snapshots=964, distinct_service_dates=8,
                       distinct_routes=412, distinct_trips=82936)
reported_splits = {"train": (6505503, 673), "validation": (1700136, 144), "test": (1736147, 145)}
benchmark_shape_matches = (all(source_metrics[k] == v for k, v in reported_source.items())
    and all((split_stats[k]["row_count"], split_stats[k]["distinct_feed_snapshots"]) == v
            for k, v in reported_splits.items()))
print("Reported notebook 10 source/split shape matches:", benchmark_shape_matches)
print("Pinned source version:", SOURCE_VERSION)
print("Matching counts alone do not establish source-version identity with notebook 10.")
if not benchmark_shape_matches:
    print("Benchmark comparisons are contextual: source coverage differs from the reported notebook 10 run.")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Training-Only Feature Availability
# MAGIC Drop only absent optional features or optional features entirely missing in training, using the same policy as notebook 10. Validate numeric inputs without using validation or test distributions to select features.

# COMMAND ----------

missing_optional_features = [name for name in optional_features if name not in model_df.columns]
existing_numeric = [name for name in candidate_numeric_features if name in model_df.columns]
existing_categorical = [name for name in candidate_categorical_features if name in model_df.columns]
train_available = parts["train"].agg(
    *[F.count(F.when(~F.isnan(F.col(name).cast("double")), F.col(name))).alias(name) for name in existing_numeric],
    *[F.count(name).alias(name) for name in existing_categorical]).first().asDict()
dropped_all_null_features = sorted(name for name, count in train_available.items() if count == 0 and name in optional_features)
empty_required = [name for name in required_features if train_available.get(name, 0) == 0]
if empty_required:
    raise ValueError(f"Required features entirely missing in training: {empty_required}")
selected_numeric_features = [name for name in existing_numeric if name not in dropped_all_null_features]
selected_categorical_features = [name for name in existing_categorical if name not in dropped_all_null_features]
feature_columns = selected_numeric_features + selected_categorical_features
assert not set(feature_columns) & set(forbidden_columns)
print("selected_numeric_features:", selected_numeric_features)
print("selected_categorical_features:", selected_categorical_features)
print("dropped_all_null_features:", dropped_all_null_features)
print("missing_optional_features:", missing_optional_features)
numeric_quality_expressions = []
for name in selected_numeric_features:
    cast_value = F.expr(f"try_cast(`{name}` AS DOUBLE)")
    invalid = (F.col(name).isNotNull() & cast_value.isNull()) | (F.abs(cast_value) == float("inf"))
    numeric_quality_expressions.append(n_where(invalid).alias(name))
numeric_quality = split_df.filter(F.col("split") != "test").agg(*numeric_quality_expressions).first().asDict()
invalid_numeric_features = {name: count for name, count in numeric_quality.items() if count > 0}
if invalid_numeric_features:
    raise ValueError(f"Invalid numeric inputs: {invalid_numeric_features}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Fit Preprocessing Once
# MAGIC Fit the same median imputation, categorical indexing, one-hot encoding, assembly and scaling stages on training only. Reuse narrow training and validation feature DataFrames for all candidates; Serverless logical DataFrames may recompute their lineage because no cache, persist, RDD or checkpoint API is used.

# COMMAND ----------

numeric_raw = [f"__numeric_raw_{i}" for i in range(len(selected_numeric_features))]
numeric_imputed = [f"__numeric_imputed_{i}" for i in range(len(selected_numeric_features))]
category_raw = [f"__category_raw_{i}" for i in range(len(selected_categorical_features))]
category_indices = [f"__category_index_{i}" for i in range(len(selected_categorical_features))]
category_vectors = [f"__category_vector_{i}" for i in range(len(selected_categorical_features))]
projection = [f"CAST(`{name}` AS DOUBLE) AS `{alias}`" for name, alias in zip(selected_numeric_features, numeric_raw)]
projection += [f"CAST(`{name}` AS STRING) AS `{alias}`" for name, alias in zip(selected_categorical_features, category_raw)]
stages = [SQLTransformer(statement="SELECT *, " + ", ".join(projection) + " FROM __THIS__")]
if numeric_raw:
    stages.append(Imputer(inputCols=numeric_raw, outputCols=numeric_imputed, strategy="median", relativeError=0.001))
if category_raw:
    stages += [StringIndexer(inputCols=category_raw, outputCols=category_indices, handleInvalid="keep", stringOrderType="alphabetAsc"),
        OneHotEncoder(inputCols=category_indices, outputCols=category_vectors, handleInvalid="keep", dropLast=False)]
assembler_inputs = numeric_imputed + category_vectors
assembler = VectorAssembler(inputCols=assembler_inputs, outputCol="features", handleInvalid="error")
stages += [assembler, StandardScaler(inputCol="features", outputCol="scaled_features", withStd=True, withMean=False)]
input_origin = dict(zip(numeric_imputed, selected_numeric_features)) | dict(zip(category_vectors, selected_categorical_features))
assert set(input_origin.values()) == set(feature_columns)
assert not set(input_origin.values()) & set(forbidden_columns)
assert not set(assembler.getInputCols()) & set(forbidden_columns)
print("Leakage feature checks passed")
preprocessor_model = Pipeline(stages=stages).fit(parts["train"])
feature_metadata = preprocessor_model.transform(parts["train"]).schema["features"].metadata.get("ml_attr", {})
vector_feature_count = int(feature_metadata.get("num_attrs", 0))
if vector_feature_count <= 0:
    raise ValueError("No assembled features or missing feature-vector metadata")
print("Raw feature count:", len(feature_columns), "Encoded vector size:", vector_feature_count)



train_vectors = preprocessor_model.transform(parts["train"]).select("features", REG_LABEL, CLS_LABEL)
validation_vectors = preprocessor_model.transform(parts["validation"]).select("features", REG_LABEL, CLS_LABEL)
# Test features are deliberately not transformed until selections are frozen.


# COMMAND ----------

# MAGIC %md
# MAGIC ## Evaluation Metrics
# MAGIC Regression uses MAE, RMSE, R2 and unscaled WAPE with notebook 10's definitions. Classification reports positive-class precision, recall and F1 from confusion counts, plus probability-based ROC-AUC and PR-AUC; undefined denominator-based metrics remain null.

# COMMAND ----------

def validate_predictions(df, label, classification=False):
    fail_if(df, F.col("prediction").isNull() | F.isnan(F.col("prediction").cast("double"))
        | (F.abs(F.col("prediction").cast("double")) == float("inf")), "Invalid model predictions")
    if classification:
        fail_if(df, ~F.col("prediction").isin(0.0, 1.0), "Non-binary classification predictions")

def regression_metrics(df, expected_rows=None):
    invalid = F.col("prediction").isNull() | F.isnan(F.col("prediction").cast("double")) | (F.abs(F.col("prediction").cast("double")) == float("inf"))
    error = F.col("prediction") - F.col(REG_LABEL)
    stat = df.agg(F.count("*").alias("n"), n_where(invalid).alias("invalid"), F.avg(F.abs(error)).alias("MAE"),
        F.sqrt(F.avg(error * error)).alias("RMSE"), F.sum(error * error).alias("sse"),
        F.var_pop(REG_LABEL).alias("variance"), F.sum(F.abs(error)).alias("sae"),
        F.sum(F.abs(F.col(REG_LABEL))).alias("absolute_target_sum")).first().asDict()
    if stat["n"] == 0 or stat["invalid"]:
        raise ValueError("Empty regression evaluation or invalid predictions")
    if expected_rows is not None and stat["n"] != expected_rows:
        raise ValueError("Regression transform changed evaluation row count")
    sst = (stat["variance"] or 0.0) * stat["n"]
    return {"MAE": float(stat["MAE"]), "RMSE": float(stat["RMSE"]),
        "R2": 1.0 - stat["sse"] / sst if sst > 0 else None,
        "WAPE": stat["sae"] / stat["absolute_target_sum"] if stat["absolute_target_sum"] > 0 else None}


# COMMAND ----------

def add_probability(df):
    return df.withColumn("major_delay_probability", vector_to_array("probability")[1])

def confusion_metrics(tp, fp, tn, fn):
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    return {"Accuracy": (tp + tn) / (tp + fp + tn + fn),
            "precision_positive": precision, "recall_positive": recall,
            "f1_positive": 2 * precision * recall / (precision + recall) if precision + recall else 0.0,
            "TP": int(tp), "FP": int(fp), "TN": int(tn), "FN": int(fn)}

def classification_metrics(df, probability_supported=True, expected_rows=None):
    probability = F.col("major_delay_probability")
    invalid = F.col("prediction").isNull() | ~F.col("prediction").isin(0.0, 1.0)
    if probability_supported:
        invalid = invalid | probability.isNull() | F.isnan(probability) | ~probability.between(0.0, 1.0)
    stat = df.agg(F.count("*").alias("n"), n_where(invalid).alias("invalid"),
        n_where((F.col(CLS_LABEL) == 1) & (F.col("prediction") == 1)).alias("TP"),
        n_where((F.col(CLS_LABEL) == 0) & (F.col("prediction") == 1)).alias("FP"),
        n_where((F.col(CLS_LABEL) == 0) & (F.col("prediction") == 0)).alias("TN"),
        n_where((F.col(CLS_LABEL) == 1) & (F.col("prediction") == 0)).alias("FN")).first().asDict()
    if not stat["n"] or stat["invalid"]:
        raise ValueError("Empty evaluation split or invalid classification predictions/probabilities")
    if expected_rows is not None and stat["n"] != expected_rows:
        raise ValueError("Classifier transform changed evaluation row count")
    metrics = confusion_metrics(*(stat[k] for k in ["TP", "FP", "TN", "FN"]))
    metrics.update(ROC_AUC=None, PR_AUC=None)
    if probability_supported and stat["TP"] + stat["FN"] and stat["TN"] + stat["FP"]:
        evaluator = BinaryClassificationEvaluator(labelCol=CLS_LABEL, rawPredictionCol="major_delay_probability")
        metrics["ROC_AUC"] = evaluator.setMetricName("areaUnderROC").evaluate(df)
        metrics["PR_AUC"] = evaluator.setMetricName("areaUnderPR").evaluate(df)
    return metrics


# COMMAND ----------

# MAGIC %md
# MAGIC ## Metric and Boundary Checks
# MAGIC Small fixtures verify regression denominators, positive-class confusion metrics and strict label-boundary eligibility. These checks run before candidate fitting and create no persistent data.

# COMMAND ----------

for snapshot_count in range(3, 50):
    train_count, val_end = split_sizes(snapshot_count)
    assert 1 <= train_count < val_end < snapshot_count
regression_fixture = spark.createDataFrame([(0.0, 0.0), (10.0, 10.0), (20.0, 10.0)], f"{REG_LABEL} DOUBLE, prediction DOUBLE")
fixture_metrics = regression_metrics(regression_fixture)
assert math.isclose(fixture_metrics["MAE"], 10.0 / 3.0)
assert math.isclose(fixture_metrics["RMSE"], math.sqrt(100.0 / 3.0))
assert math.isclose(fixture_metrics["R2"], 0.5)
assert math.isclose(fixture_metrics["WAPE"], 1.0 / 3.0)
zero_metrics = regression_metrics(spark.createDataFrame([(0.0, 0.0)], f"{REG_LABEL} DOUBLE, prediction DOUBLE"))
assert zero_metrics["R2"] is None and zero_metrics["WAPE"] is None
classification_fixture = spark.createDataFrame([(1.0, 1.0), (1.0, 0.0), (0.0, 1.0), (0.0, 0.0)], f"{CLS_LABEL} DOUBLE, prediction DOUBLE")
class_fixture_metrics = classification_metrics(classification_fixture, probability_supported=False)
assert all(class_fixture_metrics[name] == 1 for name in ["TP", "FP", "TN", "FN"])
assert all(math.isclose(class_fixture_metrics[name], 0.5) for name in ["Accuracy", "precision_positive", "recall_positive", "f1_positive"])
assert class_fixture_metrics["ROC_AUC"] is None and class_fixture_metrics["PR_AUC"] is None
purge_fixture = spark.createDataFrame([
    ("train", validation_start - 1, True), ("train", validation_start, False),
    ("validation", test_start - 1, True), ("validation", test_start, False),
    ("test", test_start + 1, True)],
    "split STRING, label_source_feed_timestamp LONG, expected BOOLEAN")
assert purge_fixture.withColumn("actual", eligible).filter(~F.col("actual").eqNullSafe(F.col("expected"))).count() == 0
print("Metric, temporal allocation and label-boundary purge checks passed")


# COMMAND ----------

# MAGIC %md
# MAGIC ## MLflow Tracking
# MAGIC Track candidate parameters, validation metrics, source version, feature contract and temporal windows in the existing experiment. Spark model artifact logging is skipped upfront for Serverless, and production registration is deferred; only explicitly recognized memory/resource failures may skip a candidate.

# COMMAND ----------

# DBTITLE 1,Tuning Checkpoint
# MAGIC %md
# MAGIC ## Tuning Checkpoint
# MAGIC Tuning results are checkpointed to a Delta table so Serverless session loss or resource failures do not force completed candidates to be retrained. Before training, `fit_candidate` checks the checkpoint for a prior successful result and loads saved metrics directly. Model objects are not persisted; if a learned model is selected for test evaluation, it is retrained once at that point.

# COMMAND ----------

# DBTITLE 1,Checkpoint table and functions
CHECKPOINT_TABLE = "rome_transport.ml.model_tuning_results"
spark.sql("CREATE SCHEMA IF NOT EXISTS rome_transport.ml")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CHECKPOINT_TABLE} (
    project STRING,
    notebook STRING,
    task STRING,
    candidate_id STRING,
    model_name STRING,
    params_json STRING,
    validation_mae DOUBLE,
    validation_rmse DOUBLE,
    validation_r2 DOUBLE,
    validation_wape DOUBLE,
    validation_accuracy DOUBLE,
    validation_precision DOUBLE,
    validation_recall DOUBLE,
    validation_f1 DOUBLE,
    validation_roc_auc DOUBLE,
    validation_pr_auc DOUBLE,
    tp LONG,
    fp LONG,
    tn LONG,
    fn LONG,
    status STRING,
    run_timestamp TIMESTAMP
) USING DELTA
""")

_METRIC_COLUMNS = [
    ("validation_mae", "MAE"), ("validation_rmse", "RMSE"),
    ("validation_r2", "R2"), ("validation_wape", "WAPE"),
    ("validation_accuracy", "Accuracy"), ("validation_precision", "precision_positive"),
    ("validation_recall", "recall_positive"), ("validation_f1", "f1_positive"),
    ("validation_roc_auc", "ROC_AUC"), ("validation_pr_auc", "PR_AUC"),
    ("tp", "TP"), ("fp", "FP"), ("tn", "TN"), ("fn", "FN"),
]

def load_checkpoint(candidate_id, task):
    """Return saved metrics dict if candidate has status SUCCESS, else None."""
    rows = spark.table(CHECKPOINT_TABLE).filter(
        (F.col("candidate_id") == candidate_id) &
        (F.col("task") == task) &
        (F.col("status") == "SUCCESS")
    ).collect()
    if not rows:
        return None
    r = rows[0].asDict()
    return {key: r[col] for col, key in _METRIC_COLUMNS}

def save_checkpoint(candidate_id, name, task, parameters, metrics, status="SUCCESS"):
    """Save candidate metrics. Never overwrites an existing SUCCESS row."""
    existing = spark.table(CHECKPOINT_TABLE).filter(
        (F.col("candidate_id") == candidate_id) &
        (F.col("task") == task) &
        (F.col("status") == "SUCCESS")
    ).limit(1).count()
    if existing:
        return
    spark.sql(f"DELETE FROM {CHECKPOINT_TABLE} WHERE candidate_id = '{candidate_id}' AND task = '{task}'")
    m = metrics or {}
    row = (
        "Rome Public Transport Reliability & Delay Prediction",
        "11_model_tuning_and_selection",
        task, candidate_id, name,
        json.dumps(parameters, sort_keys=True),
        float(m["MAE"]) if m.get("MAE") is not None else None,
        float(m["RMSE"]) if m.get("RMSE") is not None else None,
        float(m["R2"]) if m.get("R2") is not None else None,
        float(m["WAPE"]) if m.get("WAPE") is not None else None,
        float(m["Accuracy"]) if m.get("Accuracy") is not None else None,
        float(m["precision_positive"]) if m.get("precision_positive") is not None else None,
        float(m["recall_positive"]) if m.get("recall_positive") is not None else None,
        float(m["f1_positive"]) if m.get("f1_positive") is not None else None,
        float(m["ROC_AUC"]) if m.get("ROC_AUC") is not None else None,
        float(m["PR_AUC"]) if m.get("PR_AUC") is not None else None,
        int(m["TP"]) if m.get("TP") is not None else None,
        int(m["FP"]) if m.get("FP") is not None else None,
        int(m["TN"]) if m.get("TN") is not None else None,
        int(m["FN"]) if m.get("FN") is not None else None,
        status,
    )
    schema = ("project STRING, notebook STRING, task STRING, candidate_id STRING, "
              "model_name STRING, params_json STRING, "
              "validation_mae DOUBLE, validation_rmse DOUBLE, validation_r2 DOUBLE, validation_wape DOUBLE, "
              "validation_accuracy DOUBLE, validation_precision DOUBLE, validation_recall DOUBLE, "
              "validation_f1 DOUBLE, validation_roc_auc DOUBLE, validation_pr_auc DOUBLE, "
              "tp LONG, fp LONG, tn LONG, fn LONG, status STRING")
    spark.createDataFrame([row], schema).withColumn("run_timestamp", F.current_timestamp()) \
        .createOrReplaceTempView("_checkpoint_row")
    spark.sql(f"INSERT INTO {CHECKPOINT_TABLE} SELECT * FROM _checkpoint_row")

def retrain_selected_model(record, task):
    """Retrain a model loaded from checkpoint (model object not persisted)."""
    if record["model"] is not None:
        return
    name = record["name"]
    if name == "Persistence":
        return
    params = record["params"]
    model_classes = {"regression": {"GBTRegressor": GBTRegressor, "RandomForestRegressor": RandomForestRegressor},
                     "classification": {"RandomForestClassifier": RandomForestClassifier}}
    label_col = REG_LABEL if task == "regression" else CLS_LABEL
    model_cls = model_classes[task][name]
    record["model"] = model_cls(labelCol=label_col, featuresCol="features", **params).fit(train_vectors)
    print(f"Retrained {name} for final evaluation (model was loaded from checkpoint without model object)")

print("Checkpoint table ready:", CHECKPOINT_TABLE)

# COMMAND ----------

if mlflow.active_run() is not None:
    raise RuntimeError("Finish the active MLflow run before running this notebook")
mlflow.set_experiment(EXPERIMENT_NAME)
run_tags = {"project": "Rome Public Transport Reliability & Delay Prediction",
    "stage": "tuning_and_selection", "validation_strategy": "temporal_split",
    "label_type": "realized_future_next_stop_delay", "registration": "deferred",
    "model_artifact_status": "skipped_serverless", "benchmark_shape_matches": str(benchmark_shape_matches)}
common_params = {"source_table": SOURCE_TABLE, "source_delta_version": SOURCE_VERSION, "seed": SEED,
    "training_rows": split_stats["train"]["row_count"], "validation_rows": split_stats["validation"]["row_count"],
    "validation_start_epoch": validation_start, "test_start_epoch": test_start,
    "feature_set_version": hashlib.sha256(json.dumps(feature_columns).encode()).hexdigest(),
    "imputer_strategy": "median", "imputer_relative_error": 0.001,
    "categorical_invalid_handling": "keep", "scaler_with_std": True, "scaler_with_mean": False}
records = {"regression": {}, "classification": {}}
failed_candidates = []

def log_metrics(metrics, prefix):
    mlflow.log_metrics({f"{prefix}_{k}": float(v) for k, v in metrics.items()
                       if v is not None and math.isfinite(float(v))})
    missing = [k for k, v in metrics.items() if v is None or not math.isfinite(float(v))]
    if missing:
        mlflow.set_tag(prefix + "_undefined_metrics", ",".join(missing))

def initialize_run(candidate_id, name, task, parameters):
    mlflow.set_tags(run_tags)
    mlflow.log_params(common_params | {"task": task, "model_name": name, "candidate_id": candidate_id,
        "feature_count": 1 if name == "Persistence" else len(feature_columns),
        "assembled_feature_count": 0 if name == "Persistence" else vector_feature_count} | parameters)
    # Plain parameter strings avoid dependence on any artifact storage capability.
    mlflow.log_params({"numeric_features": json.dumps(selected_numeric_features),
        "categorical_features": json.dumps(selected_categorical_features),
        "dropped_all_null_features": json.dumps(dropped_all_null_features),
        "missing_optional_features": json.dumps(missing_optional_features)})

def is_resource_failure(exc):
    # Deliberately exclude generic timeouts, unsupported APIs, schema and analysis errors.
    message = str(exc).lower()
    return isinstance(exc, MemoryError) or any(token in message for token in
        ["java.lang.outofmemoryerror", "[out_of_memory]", "[resource_exhausted]", "executor_out_of_memory",
         "executorlostfailure", "task_failed_executor_loss"])

def fit_candidate(candidate_id, name, task, parameters, factory):
    if globals().get("test_evaluation_started", False):
        raise RuntimeError("Tuning cannot continue after test evaluation")
    if candidate_id in records[task] or any(r["candidate"] == candidate_id for r in failed_candidates):
        raise ValueError("Duplicate tuning candidate ID")
    saved_metrics = load_checkpoint(candidate_id, task)
    if saved_metrics is not None:
        records[task][candidate_id] = {"name": name, "params": parameters, "model": None,
            "validation": saved_metrics, "run_id": None}
        print(f"Skipping {candidate_id}: completed result loaded from checkpoint")
        return
    with mlflow.start_run(run_name=candidate_id) as run:
        initialize_run(candidate_id, name, task, parameters)
        try:
            fitted = factory(**parameters).fit(train_vectors)
            predictions = fitted.transform(validation_vectors)
            metrics = (regression_metrics(predictions, split_stats["validation"]["row_count"]) if task == "regression"
                       else classification_metrics(add_probability(predictions), expected_rows=split_stats["validation"]["row_count"]))
        except Exception as exc:
            if not is_resource_failure(exc):
                raise
            failed_candidates.append({"candidate": candidate_id, "model": name,
                "params": json.dumps(parameters, sort_keys=True), "reason": str(exc)[:2000]})
            mlflow.set_tags({"candidate_status": "skipped_resource_failure", "failure_reason": str(exc)[:2000]})
            save_checkpoint(candidate_id, name, task, parameters, None, status="RESOURCE_FAILED")
            print("Skipped resource-limited candidate:", candidate_id, str(exc)[:500])
            return
        log_metrics(metrics, "validation")
        mlflow.set_tag("candidate_status", "completed")
        records[task][candidate_id] = {"name": name, "params": parameters, "model": fitted,
            "validation": metrics, "run_id": run.info.run_id}
        save_checkpoint(candidate_id, name, task, parameters, metrics, status="SUCCESS")
        print(candidate_id, metrics)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Baseline Reference
# MAGIC Recompute Persistence on validation using current arrival delay and notebook 10's training-target median fallback. This baseline is not tuned and retains every evaluation row.

# COMMAND ----------

training_label_summary = parts["train"].agg(
    F.percentile_approx(REG_LABEL, 0.5, 10000).alias("target_median"),
    n_where(F.col(CLS_LABEL) == 1).alias("positive_count"), n_where(F.col(CLS_LABEL) == 0).alias("negative_count")
).first().asDict()
train_target_median = float(training_label_summary["target_median"])
if not math.isfinite(train_target_median):
    raise ValueError("Invalid training-only median fallback")
def sql_missing_numeric(name):
    return f"CASE WHEN isnan(CAST(`{name}` AS DOUBLE)) THEN NULL ELSE CAST(`{name}` AS DOUBLE) END"
persistence_expression = f"COALESCE({sql_missing_numeric('current_arrival_delay_seconds')}, {train_target_median!r})"

persistence_model = SQLTransformer(statement=f"SELECT *, {persistence_expression} AS prediction FROM __THIS__")
with mlflow.start_run(run_name="persistence_reference") as run:
    persistence_params = {"fallback_train_target_median": train_target_median}
    initialize_run("persistence_reference", "Persistence", "regression", persistence_params)
    persistence_metrics = regression_metrics(persistence_model.transform(parts["validation"]), split_stats["validation"]["row_count"])
    log_metrics(persistence_metrics, "validation")
    records["regression"]["persistence_reference"] = {"name": "Persistence", "params": persistence_params,
        "model": persistence_model, "validation": persistence_metrics, "run_id": run.info.run_id}
print("Persistence validation:", persistence_metrics)
if not training_label_summary["positive_count"] or not training_label_summary["negative_count"]:
    raise ValueError("Both training classes are required for classifier tuning")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Regression Tuning Strategy
# MAGIC Use four curated GBT and three Random Forest configurations rather than a Cartesian search. Fit on training and rank completed learned candidates by validation MAE, then RMSE, with candidate ID as a deterministic tie-breaker.

# COMMAND ----------

gbt_grid = [
    dict(maxDepth=4, maxIter=20, stepSize=0.1),
    dict(maxDepth=5, maxIter=30, stepSize=0.1),
    dict(maxDepth=6, maxIter=30, stepSize=0.05),
    dict(maxDepth=5, maxIter=40, stepSize=0.05)]
rf_reg_grid = [
    dict(numTrees=50, maxDepth=6, minInstancesPerNode=1),
    dict(numTrees=100, maxDepth=6, minInstancesPerNode=5),
    dict(numTrees=100, maxDepth=8, minInstancesPerNode=5)]
rf_cls_grid = [
    dict(numTrees=50, maxDepth=6, minInstancesPerNode=1, featureSubsetStrategy="auto"),
    dict(numTrees=100, maxDepth=6, minInstancesPerNode=5, featureSubsetStrategy="sqrt"),
    dict(numTrees=100, maxDepth=8, minInstancesPerNode=5, featureSubsetStrategy="sqrt"),
    dict(numTrees=150, maxDepth=8, minInstancesPerNode=5, featureSubsetStrategy="auto"),
    dict(numTrees=100, maxDepth=10, minInstancesPerNode=5, featureSubsetStrategy="sqrt")]
planned_candidate_ids = ([f"gbt_{i+1:02d}" for i in range(len(gbt_grid))]
    + [f"rf_reg_{i+1:02d}" for i in range(len(rf_reg_grid))]
    + [f"rf_cls_{i+1:02d}" for i in range(len(rf_cls_grid))])
assert len(planned_candidate_ids) == len(set(planned_candidate_ids))
print("Planned learned regression / classification candidates:", len(gbt_grid) + len(rf_reg_grid), len(rf_cls_grid))


# COMMAND ----------

# DBTITLE 1,Seed checkpoint with completed results
# Seed checkpoint with completed tuning results from prior notebook execution.
# Metrics sourced from actual cell 27 (GBT) and cell 29 (RF regression) outputs.
_seeded_metrics = {
    "gbt_01": {"MAE": 170.23910873697918, "RMSE": 359.7923517074585, "R2": 0.7966942963696491, "WAPE": 0.3776272332410122},
    "gbt_02": {"MAE": 163.43453138550123, "RMSE": 350.8709508176177, "R2": 0.8066516207057088, "WAPE": 0.36253320615359885},
    "gbt_03": {"MAE": 163.6183646977585, "RMSE": 353.3959662893187, "R2": 0.8038587743776364, "WAPE": 0.36294098827605165},
    "gbt_04": {"MAE": 164.86677987406028, "RMSE": 348.5830458424537, "R2": 0.8091649119647402, "WAPE": 0.36571024366313964},
    "rf_reg_01": {"MAE": 166.16747923108065, "RMSE": 352.8241772981922, "R2": 0.8044929676035404, "WAPE": 0.3685954766928123},
    "rf_reg_02": {"MAE": 166.20741380750906, "RMSE": 352.73256060983476, "R2": 0.8045944877513886, "WAPE": 0.36868406023697664},
    "rf_reg_03": {"MAE": 163.07825762612384, "RMSE": 350.28837170249994, "R2": 0.8072931512413646, "WAPE": 0.3617429137523509},
}
_seeded_models = {f"gbt_{i:02d}": "GBTRegressor" for i in range(1, 5)} | {f"rf_reg_{i:02d}": "RandomForestRegressor" for i in range(1, 4)}
_seeded_params = {f"gbt_{i:02d}": p | {"maxBins": 32, "seed": SEED} for i, p in enumerate(gbt_grid, 1)} | \
                 {f"rf_reg_{i:02d}": p | {"maxBins": 32, "seed": SEED} for i, p in enumerate(rf_reg_grid, 1)}
for cid, metrics in _seeded_metrics.items():
    save_checkpoint(cid, _seeded_models[cid], "regression", _seeded_params[cid], metrics, status="SUCCESS")
print(f"Seeded {len(_seeded_metrics)} completed candidates to checkpoint")
spark.table(CHECKPOINT_TABLE).orderBy("task", "candidate_id").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tuned GBT Regressor
# MAGIC Evaluate four fixed depth, iteration and learning-rate combinations. Resource failures are recorded explicitly and cannot silently mask data, schema or leakage errors.

# COMMAND ----------

for i, params in enumerate(gbt_grid, 1):
    fit_candidate(f"gbt_{i:02d}", "GBTRegressor", "regression", params | {"maxBins": 32, "seed": SEED},
                  lambda **p: GBTRegressor(labelCol=REG_LABEL, featuresCol="features", **p))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Tuned Random Forest Regressor
# MAGIC Evaluate three fixed tree-count, depth and minimum-node-size combinations using the same feature vectors. Validation remains the only selection dataset. One configuration (numTrees=50, maxDepth=10) was excluded after an executor OOM on the ~10M-row dataset.

# COMMAND ----------

for i, params in enumerate(rf_reg_grid, 1):
    fit_candidate(f"rf_reg_{i:02d}", "RandomForestRegressor", "regression", params | {"maxBins": 32, "seed": SEED},
                  lambda **p: RandomForestRegressor(labelCol=REG_LABEL, featuresCol="features", **p))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Regression Selection
# MAGIC Choose a learned model only if its validation MAE is strictly lower than recomputed Persistence MAE; otherwise retain Persistence. Report RMSE separately so improvements on large errors do not override the primary MAE rule.

# COMMAND ----------

def regression_rank(candidate_id):
    m = records["regression"][candidate_id]["validation"]
    return (m["MAE"], m["RMSE"], candidate_id)

learned_regression_ids = [k for k in records["regression"] if k != "persistence_reference"]
best_learned_regression_id = min(learned_regression_ids, key=regression_rank) if learned_regression_ids else None
selected_regression_id = "persistence_reference"
if best_learned_regression_id is not None:
    learned_metrics = records["regression"][best_learned_regression_id]["validation"]
    if learned_metrics["MAE"] < persistence_metrics["MAE"]:
        selected_regression_id = best_learned_regression_id
    print("Best learned vs Persistence validation deltas (negative is better):",
          {m: learned_metrics[m] - persistence_metrics[m] for m in ["MAE", "RMSE"]})
else:
    print("No learned regression candidate completed; Persistence is the only evaluated regression solution.")
regression_rows = [(k, r["name"], json.dumps(r["params"], sort_keys=True),
    *[r["validation"][m] for m in ["MAE", "RMSE", "R2", "WAPE"]]) for k, r in records["regression"].items()]
spark.createDataFrame(regression_rows, "candidate STRING, model STRING, params STRING, validation_MAE DOUBLE, validation_RMSE DOUBLE, validation_R2 DOUBLE, validation_WAPE DOUBLE").orderBy("validation_MAE", "validation_RMSE").show(20, truncate=False)
print("Selected regression candidate:", selected_regression_id)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Classification Tuning Strategy
# MAGIC Tune only RandomForestClassifier across five curated configurations at its default decision threshold. Rank by positive-class validation F1, then PR-AUC, ROC-AUC, recall and precision; candidate ID resolves remaining ties.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tuned Random Forest Classifier
# MAGIC Fit every classifier on the shared training feature vectors and evaluate only validation. Record accuracy, positive-class metrics, both AUCs and all four confusion counts.

# COMMAND ----------

for i, params in enumerate(rf_cls_grid, 1):
    fit_candidate(f"rf_cls_{i:02d}", "RandomForestClassifier", "classification", params | {"maxBins": 32, "seed": SEED},
                  lambda **p: RandomForestClassifier(labelCol=CLS_LABEL, featuresCol="features", **p))
if not records["classification"]:
    raise RuntimeError("No classifier completed; final selection cannot proceed")

def classifier_rank(candidate_id):
    m = records["classification"][candidate_id]["validation"]
    return tuple(-(m[k] if m[k] is not None else -math.inf) for k in
        ["f1_positive", "PR_AUC", "ROC_AUC", "recall_positive", "precision_positive"]) + (candidate_id,)

selected_classifier_id = min(records["classification"], key=classifier_rank)
classification_rows = [(k, json.dumps(r["params"], sort_keys=True),
    *[r["validation"][m] for m in ["Accuracy", "precision_positive", "recall_positive", "f1_positive", "ROC_AUC", "PR_AUC"]],
    *[r["validation"][m] for m in ["TP", "FP", "TN", "FN"]]) for k, r in records["classification"].items()]
spark.createDataFrame(classification_rows, "candidate STRING, params STRING, validation_accuracy DOUBLE, validation_precision DOUBLE, validation_recall DOUBLE, validation_F1 DOUBLE, validation_ROC_AUC DOUBLE, validation_PR_AUC DOUBLE, TP LONG, FP LONG, TN LONG, FN LONG").orderBy(F.desc("validation_F1")).show(20, truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Probability Threshold Optimization
# MAGIC For the selected RF configuration, aggregate confusion counts for all 13 thresholds in one validation scan. Predict class 1 when probability is strictly greater than the threshold (ties remain class 0, consistent with default binary RF); maximize F1, then recall, then prefer the lower threshold.

# COMMAND ----------

threshold_grid = [i / 100.0 for i in range(20, 81, 5)]
selected_classifier_record = records["classification"][selected_classifier_id]
validation_probabilities = add_probability(selected_classifier_record["model"].transform(validation_vectors)).select(CLS_LABEL, "major_delay_probability")
probability = F.col("major_delay_probability")
positive = F.col(CLS_LABEL) == 1
aggregates = [F.count("*").alias("n"), n_where(probability.isNull() | F.isnan(probability)
             | ~probability.between(0.0, 1.0)).alias("invalid")]
for i, threshold in enumerate(threshold_grid):
    predicted = probability > F.lit(threshold)
    for name, condition in {"TP": positive & predicted, "FP": ~positive & predicted,
                            "TN": ~positive & ~predicted, "FN": positive & ~predicted}.items():
        aggregates.append(n_where(condition).alias(f"t{i}_{name}"))
threshold_counts = validation_probabilities.agg(*aggregates).first().asDict()
assert threshold_counts["n"] == split_stats["validation"]["row_count"]
if threshold_counts["invalid"]:
    raise ValueError("Invalid validation probabilities")
threshold_results = []
for i, threshold in enumerate(threshold_grid):
    m = confusion_metrics(*(threshold_counts[f"t{i}_{k}"] for k in ["TP", "FP", "TN", "FN"]))
    threshold_results.append({"threshold": threshold, **m})
best_threshold_result = min(threshold_results, key=lambda r: (-r["f1_positive"], -r["recall_positive"], r["threshold"]))
selected_threshold = best_threshold_result["threshold"]
threshold_validation_metrics = {k: v for k, v in best_threshold_result.items() if k != "threshold"}
threshold_validation_metrics.update({k: selected_classifier_record["validation"][k] for k in ["ROC_AUC", "PR_AUC"]})
# The 0.50 grid point must reproduce the default binary RF confusion counts.
default_grid_result = next(r for r in threshold_results if r["threshold"] == 0.5)
assert all(default_grid_result[k] == selected_classifier_record["validation"][k] for k in ["TP", "FP", "TN", "FN"])
rows = [(r["threshold"], r["precision_positive"], r["recall_positive"], r["f1_positive"],
         r["TP"], r["FP"], r["TN"], r["FN"]) for r in threshold_results]
spark.createDataFrame(rows, "threshold DOUBLE, precision DOUBLE, recall DOUBLE, F1 DOUBLE, TP LONG, FP LONG, TN LONG, FN LONG").orderBy("threshold").show(13, truncate=False)
with mlflow.start_run(run_name="validation_threshold_selection"):
    initialize_run(selected_classifier_id, "RandomForestClassifier", "classification", selected_classifier_record["params"])
    mlflow.log_param("threshold_grid", json.dumps(threshold_grid))
    mlflow.log_param("selected_probability_threshold", selected_threshold)
    mlflow.log_param("threshold_comparison", "strict_greater_than")
    for i, r in enumerate(threshold_results):
        log_metrics({k: v for k, v in r.items() if k != "threshold"}, f"threshold_{round(r['threshold'] * 100):02d}")
    log_metrics(threshold_validation_metrics, "validation")
print("Validation-selected threshold:", selected_threshold)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Frozen Final Selection
# MAGIC Freeze candidate identities, parameters, model UIDs and the validation-selected classification threshold before any test scoring. A fingerprint and single-evaluation guard detect later changes or accidental re-execution of the test cell within this run.

# COMMAND ----------

if globals().get("test_evaluation_started", False):
    raise RuntimeError("Cannot replace frozen selection after test evaluation")
final_regression_record = records["regression"][selected_regression_id]
final_classifier_record = records["classification"][selected_classifier_id]
retrain_selected_model(final_regression_record, "regression")
retrain_selected_model(final_classifier_record, "classification")
final_regression_model_name = final_regression_record["name"]
final_regression_params = dict(final_regression_record["params"])
final_classifier_model_name = final_classifier_record["name"]
final_classifier_params = dict(final_classifier_record["params"])
final_classification_threshold = float(selected_threshold)

def selection_payload():
    return {"regression_id": selected_regression_id, "regression_model": final_regression_model_name,
        "regression_params": final_regression_params, "regression_uid": final_regression_record["model"].uid,
        "classification_id": selected_classifier_id, "classification_model": final_classifier_model_name,
        "classification_params": final_classifier_params, "classification_uid": final_classifier_record["model"].uid,
        "classification_threshold": final_classification_threshold, "source_version": SOURCE_VERSION,
        "validation_start": validation_start, "test_start": test_start}

frozen_selection_json = json.dumps(selection_payload(), sort_keys=True)
frozen_selection_hash = hashlib.sha256(frozen_selection_json.encode()).hexdigest()

def assert_frozen_selection():
    assert hashlib.sha256(json.dumps(selection_payload(), sort_keys=True).encode()).hexdigest() == frozen_selection_hash
    assert final_regression_record is records["regression"][selected_regression_id]
    assert final_classifier_record is records["classification"][selected_classifier_id]
    assert final_regression_params == final_regression_record["params"]
    assert final_classifier_params == final_classifier_record["params"]
    assert final_classification_threshold == best_threshold_result["threshold"]
    for record in [final_regression_record, final_classifier_record]:
        if record["name"] != "Persistence":
            assert all(record["model"].getOrDefault(k) == v for k, v in record["params"].items())

assert_frozen_selection()
print("Frozen final selection:", frozen_selection_json)
final_run_ids = {}
for task, record, validation in [("regression", final_regression_record, final_regression_record["validation"]),
                                ("classification", final_classifier_record, threshold_validation_metrics)]:
    with mlflow.start_run(run_name=f"final_{task}_selection") as run:
        initialize_run(selected_regression_id if task == "regression" else selected_classifier_id,
                       record["name"], task, record["params"])
        mlflow.set_tags({"final_selection": "true", "selection_fingerprint": frozen_selection_hash,
                         "parent_candidate_run_id": record.get("run_id") or "checkpoint_recovered"})
        mlflow.log_param("frozen_selection", frozen_selection_json)
        if task == "classification":
            mlflow.log_param("selected_probability_threshold", final_classification_threshold)
        log_metrics(validation, "validation")
        final_run_ids[task] = run.info.run_id
# Do not reset this guard when rerunning the freeze cell in the same Python session.
if "test_evaluation_started" not in globals():
    test_evaluation_started = False


# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Test Evaluation
# MAGIC Evaluate only the frozen regression solution and RF classifier, applying the fixed validation-selected probability threshold. Test metrics are reporting outputs and cannot change candidate choices, parameters or the threshold.

# COMMAND ----------

assert_frozen_selection()
if test_evaluation_started:
    raise RuntimeError("Test evaluation already started in this session; do not rerun or retune after viewing test results")
test_evaluation_started = True
# Validate test inputs only now, using the training-selected contract.
test_numeric_quality = parts["test"].agg(*numeric_quality_expressions).first().asDict()
if any(test_numeric_quality.values()):
    raise ValueError(f"Invalid test numeric inputs: {test_numeric_quality}")
test_vectors = preprocessor_model.transform(parts["test"]).select("features", REG_LABEL, CLS_LABEL)
regression_test_input = parts["test"] if final_regression_model_name == "Persistence" else test_vectors
final_regression_test_metrics = regression_metrics(final_regression_record["model"].transform(regression_test_input), split_stats["test"]["row_count"])
classifier_test_predictions = add_probability(final_classifier_record["model"].transform(test_vectors)).withColumn(
    "prediction", (F.col("major_delay_probability") > F.lit(final_classification_threshold)).cast("double"))
final_classification_test_metrics = classification_metrics(classifier_test_predictions, expected_rows=split_stats["test"]["row_count"])
assert sum(final_classification_test_metrics[k] for k in ["TP", "FP", "TN", "FN"]) == split_stats["test"]["row_count"]
assert_frozen_selection()
final_test_metrics = {"regression": final_regression_test_metrics, "classification": final_classification_test_metrics}
for task, metrics in final_test_metrics.items():
    with mlflow.start_run(run_id=final_run_ids[task]):
        log_metrics(metrics, "test")
        mlflow.set_tag("test_evaluation_status", "completed_frozen_selection")
final_summary_rows = [
    ("regression", final_regression_model_name, "validation_MAE", final_regression_record["validation"]["MAE"],
     json.dumps(final_regression_test_metrics, sort_keys=True)),
    ("classification", final_classifier_model_name, "validation_positive_F1_after_threshold_selection",
     threshold_validation_metrics["f1_positive"], json.dumps(final_classification_test_metrics, sort_keys=True))]
spark.createDataFrame(final_summary_rows, "task STRING, final_model STRING, validation_selection_metric STRING, validation_selection_value DOUBLE, test_metrics STRING").show(truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Compare Against Notebook 10
# MAGIC Compare measured results with the supplied, rounded benchmark values; unavailable historical metrics remain null. Matching source and split counts is necessary but does not prove identical Delta versions, and reported differences are descriptive rather than evidence of statistical significance.

# COMMAND ----------

# Historical test values are introduced only after final frozen evaluation.
reported_regression = [
    ("Persistence", 141.60, 136.98, 370.88, 401.85),
    ("GBTRegressor", 163.43, 153.64, 350.87, 328.47),
    ("RandomForestRegressor", 166.17, 156.30, None, None),
    ("LinearRegression", 171.16, 168.33, None, 289.07),
    ("Historical", 504.84, 449.23, None, None)]
regression_comparison = [("reported_notebook_10", *r) for r in reported_regression] + [
    ("measured_notebook_11", final_regression_model_name, final_regression_record["validation"]["MAE"],
     final_regression_test_metrics["MAE"], final_regression_record["validation"]["RMSE"], final_regression_test_metrics["RMSE"])]
spark.createDataFrame(regression_comparison, "source STRING, model STRING, validation_MAE DOUBLE, test_MAE DOUBLE, validation_RMSE DOUBLE, test_RMSE DOUBLE").show(truncate=False)
classification_comparison = [
    ("reported_notebook_10", "RandomForestClassifier", 0.5, 0.8331, 0.8215, 0.9663, 0.9652, 0.9072, 0.8962),
    ("reported_notebook_10", "LogisticRegression", 0.5, 0.7259, 0.7161, None, None, None, None),
    ("reported_notebook_10", "Majority baseline", None, 0.0, 0.0, None, None, None, None),
    ("measured_notebook_11", final_classifier_model_name, final_classification_threshold,
     threshold_validation_metrics["f1_positive"], final_classification_test_metrics["f1_positive"],
     threshold_validation_metrics["ROC_AUC"], final_classification_test_metrics["ROC_AUC"],
     threshold_validation_metrics["PR_AUC"], final_classification_test_metrics["PR_AUC"])]
spark.createDataFrame(classification_comparison, "source STRING, model STRING, threshold DOUBLE, validation_F1 DOUBLE, test_F1 DOUBLE, validation_ROC_AUC DOUBLE, test_ROC_AUC DOUBLE, validation_PR_AUC DOUBLE, test_PR_AUC DOUBLE").show(truncate=False)
comparison_deltas = {
    "test_MAE_vs_reported_Persistence_seconds": final_regression_test_metrics["MAE"] - 136.98,
    "test_RMSE_vs_reported_Persistence_seconds": final_regression_test_metrics["RMSE"] - 401.85,
    "test_positive_F1_vs_reported_RF": final_classification_test_metrics["f1_positive"] - 0.8215,
    "validation_positive_F1_vs_reported_RF": threshold_validation_metrics["f1_positive"] - 0.8331,
    "validation_F1_gain_from_threshold_only": threshold_validation_metrics["f1_positive"] - final_classifier_record["validation"]["f1_positive"]}
print("Descriptive changes relative to rounded historical metrics:", json.dumps(comparison_deltas, indent=2))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Importance
# MAGIC Map native tree importance to numeric feature names and one-hot categories through the fitted assembler metadata. Show the final RF classifier and, only when selected, the learned regressor; Persistence has no learned feature importance.

# COMMAND ----------

def readable_feature_name(encoded_name):
    for alias, original in zip(numeric_imputed, selected_numeric_features):
        if encoded_name == alias:
            return original
    for alias, original in zip(category_vectors, selected_categorical_features):
        if encoded_name.startswith(alias + "_"):
            return original + "=" + encoded_name[len(alias) + 1:]
    raise ValueError(f"Unmapped assembled feature name: {encoded_name}")

vector_feature_names = {}
for group in feature_metadata.get("attrs", {}).values():
    for attribute in group:
        vector_feature_names[int(attribute["idx"])] = readable_feature_name(attribute["name"])
if set(vector_feature_names) != set(range(vector_feature_count)):
    raise ValueError("Incomplete assembled feature-name metadata")

def show_feature_importance(task, record):
    if record["name"] == "Persistence":
        print("Persistence: no learned feature importance.")
        return
    importance = record["model"].featureImportances.toArray().tolist()
    if len(importance) != vector_feature_count:
        raise ValueError("Feature importance size differs from assembled metadata")
    rows = sorted([(vector_feature_names[i], float(v)) for i, v in enumerate(importance)],
                  key=lambda r: (-r[1], r[0]))[:20]
    print(task, record["name"], "Top 20 native feature importances")
    spark.createDataFrame(rows, "feature STRING, importance DOUBLE").show(20, truncate=False)
    with mlflow.start_run(run_id=final_run_ids[task]):
        mlflow.log_param("top_20_native_feature_importances", json.dumps(rows))

show_feature_importance("regression", final_regression_record)
show_feature_importance("classification", final_classifier_record)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Model Recommendation
# MAGIC The outputs below distinguish validation-based selection, the MAE/RMSE tradeoff and descriptive changes against notebook 10. A numerical gain alone does not establish operationally meaningful or statistically reliable improvement over this limited time window.

# COMMAND ----------

assert_frozen_selection()
assert not set(feature_columns) & set(forbidden_columns)
assert not set(input_origin.values()) & set(forbidden_columns)
assert overlap_count == 0
completed_ids = (set(records["regression"]) - {"persistence_reference"}) | set(records["classification"])
failed_ids = {r["candidate"] for r in failed_candidates}
assert completed_ids.isdisjoint(failed_ids)
assert completed_ids | failed_ids == set(planned_candidate_ids)
assert set(final_regression_test_metrics) == {"MAE", "RMSE", "R2", "WAPE"}
assert set(final_classification_test_metrics) == {"Accuracy", "precision_positive", "recall_positive", "f1_positive", "ROC_AUC", "PR_AUC", "TP", "FP", "TN", "FN"}
for task, metrics in final_test_metrics.items():
    undefined = [k for k, v in metrics.items() if v is None]
    assert all(v is None or math.isfinite(float(v)) for v in metrics.values())
    print(task, "undefined metrics (only documented degenerate denominators/classes):", undefined)
if final_regression_model_name == "Persistence":
    if best_learned_regression_id is None:
        print("Persistence is retained because no learned regression candidate completed; no learned-model superiority comparison is available.")
    else:
        print("Persistence remains preferred because no completed learned model achieved lower validation MAE.")
else:
    improvement = persistence_metrics["MAE"] - final_regression_record["validation"]["MAE"]
    print(f"{final_regression_model_name} is selected with validation MAE lower than Persistence by {improvement:.4f} seconds.")
if best_learned_regression_id:
    learned = records["regression"][best_learned_regression_id]["validation"]
    print(f"Best learned regression RMSE minus Persistence RMSE: {learned['RMSE'] - persistence_metrics['RMSE']:.4f} seconds; MAE remains the selection criterion.")
print(f"Selected classifier: {selected_classifier_id}; threshold={final_classification_threshold:.2f}; validation positive-class F1={threshold_validation_metrics['f1_positive']:.6f}.")
print("This is the best validation tradeoff among the completed curated RF candidates and tested thresholds, not all possible models.")
reg_test_gain = 136.98 - final_regression_test_metrics["MAE"]
cls_test_gain = final_classification_test_metrics["f1_positive"] - 0.8215
print(f"Against rounded notebook 10 references: test MAE improvement={reg_test_gain:.4f} seconds; test F1 improvement={cls_test_gain:.6f} (positive means better).")
print("Meaningful improvement is not established by this run alone: assess these magnitudes against operational costs and additional future periods; no arbitrary significance cutoff is applied.")
if not benchmark_shape_matches:
    print("Coverage differs from the reference benchmark; differences cannot be attributed to tuning alone.")
if failed_candidates:
    spark.createDataFrame([(r['candidate'], r['model'], r['params'], r['reason']) for r in failed_candidates],
        "candidate STRING, model STRING, params STRING, reason STRING").show(truncate=False)
print("Completed learned candidates:", len(completed_ids), "Resource failures:", len(failed_ids))
print("Final model registration is deferred to the next deployment/scoring stage.")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Limitations
# MAGIC The reported benchmark covers eight service dates, and future GTFS observations remain label proxies affected by operational feed quality. Notebook 10 already assessed this test period, so this frozen repeat assessment is not a new independent holdout; additional future periods are needed to assess generalization.
# MAGIC
# MAGIC Threshold tuning and hyperparameter selection share validation and may overfit it; RF probabilities are not separately calibrated. Serverless may recompute logical feature DataFrames, failed candidates reduce search coverage, and model serialization and registration remain deferred.