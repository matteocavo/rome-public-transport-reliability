# Databricks notebook source
# MAGIC %md
# MAGIC # Model Training and Evaluation
# MAGIC Build a preliminary regression and classification pipeline for Rome Public Transport Reliability & Delay Prediction using chronological evaluation and the future labels from notebook 09.
# MAGIC This is a technically valid modeling workflow, but performance remains preliminary until substantially more realtime days are collected; no production model is registered.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Runtime and Experiment Configuration
# MAGIC Use built-in Spark ML and MLflow without installing dependencies, collecting large datasets or relying on RDD/cache APIs.
# MAGIC Databricks documents [Spark ML support on Serverless](https://docs.databricks.com/aws/en/machine-learning/train-model/mllib) and its [compute limitations](https://docs.databricks.com/aws/en/compute/serverless/limitations); model serialization is checked separately.

# COMMAND ----------

import math
import json
import hashlib
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import Imputer, StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler, SQLTransformer
from pyspark.ml.regression import LinearRegression, RandomForestRegressor, GBTRegressor
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator
from pyspark.ml.functions import vector_to_array
import mlflow
import mlflow.spark

spark.conf.set("spark.sql.session.timeZone", "Europe/Rome")
SOURCE_TABLE = "rome_transport.features.next_stop_delay_labeled"
SOURCE_VERSION = None
REG_LABEL = "target_realized_next_stop_delay_seconds"
CLS_LABEL = "target_realized_major_delay_flag"
OBS_KEY = ["feed_timestamp", "entity_id", "trip_id", "stop_sequence", "stop_id"]
EXPERIMENT_NAME = "/Shared/rome_transport_next_stop_delay"
SEED = 42
LIGHTWEIGHT_PROFILE = True
TREE_NUM_TREES = 50 if LIGHTWEIGHT_PROFILE else 100
TREE_MAX_DEPTH = 6 if LIGHTWEIGHT_PROFILE else 8
GBT_ITERATIONS = 30
LOG_MODEL_ARTIFACTS = True

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
# MAGIC ## Load Labeled Feature Dataset
# MAGIC Read the single Unity Catalog Delta source at an explicit version and report its temporal and operational coverage.
# MAGIC Source tables remain unchanged throughout training, comparison and experiment tracking.

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
# MAGIC ## Define Modeling Columns
# MAGIC Use an explicit feature allowlist and exclude identifiers, provisional predictions, final targets and every future-label audit field from model inputs.
# MAGIC Feature availability will be decided using training data only, and no high-cardinality trip, vehicle, entity or stop identifiers enter this first pipeline.

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
# MAGIC ## Temporal Train / Validation / Test Split
# MAGIC Assign approximately 70/15/15 percent of distinct ordered feed timestamps to train, validation and test, keeping every snapshot within one partition.
# MAGIC At least three timestamps are necessary for three nonempty partitions; fewer than 30 timestamps only triggers the requested preliminary-performance warning.

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

# MAGIC %md
# MAGIC ## Label Availability at Split Boundaries
# MAGIC Purge training rows whose future labels arrive at or after validation starts, and validation rows whose labels arrive at or after test starts.
# MAGIC This preserves the future-label availability contract from notebook 09; if purging empties a partition, the split is genuinely unusable and the notebook raises a clear error instead of relaxing causality.

# COMMAND ----------

eligible = ((F.col("split") == "train") & (F.col("label_source_feed_timestamp") < validation_start))     | ((F.col("split") == "validation") & (F.col("label_source_feed_timestamp") < test_start))     | (F.col("split") == "test")
assigned.groupBy("split").agg(F.count("*").alias("rows_before_purge"),
    n_where(~eligible).alias("boundary_crossing_rows_purged")).orderBy("split").show(truncate=False)
split_df = assigned.filter(eligible)
parts = {name: split_df.filter(F.col("split") == name) for name in ["train", "validation", "test"]}
split_summary = split_df.groupBy("split").agg(
    F.count("*").alias("row_count"), F.countDistinct("feed_timestamp").alias("distinct_feed_snapshots"),
    F.min("feed_datetime").alias("min_feed_datetime"), F.max("feed_datetime").alias("max_feed_datetime"),
    F.min("feed_timestamp").alias("min_feed_timestamp"), F.max("feed_timestamp").alias("max_feed_timestamp"),
    (100.0 * F.avg(CLS_LABEL)).alias("major_delay_rate_pct"))
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


# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Availability Check
# MAGIC Inspect existing columns and training-only non-null counts, dropping only optional absent or entirely missing features.
# MAGIC This excludes trip_progress_ratio if it is all null in training without inspecting held-out values, while retaining sparse vehicle fields whenever training contains usable observations.

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
numeric_quality = split_df.agg(*numeric_quality_expressions).first().asDict()
invalid_numeric_features = {name: count for name, count in numeric_quality.items() if count > 0}
if invalid_numeric_features:
    raise ValueError(f"Invalid numeric inputs: {invalid_numeric_features}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Preprocessing Pipeline
# MAGIC Fit median imputation, categorical indexing, one-hot encoding, assembly and scaling on training data only through a Spark ML Pipeline.
# MAGIC The scaler keeps sparse vectors uncentered for linear/logistic models; trees use the unscaled vector, and unknown categories are retained rather than dropping validation/test rows.

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


# COMMAND ----------

# MAGIC %md
# MAGIC ## Regression Evaluation Functions
# MAGIC Compute MAE and RMSE in seconds, R² from residual and total sums of squares, and WAPE as an unscaled absolute-error ratio.
# MAGIC R² is undefined for a constant evaluation target and WAPE for a zero absolute-target denominator, so these results remain null instead of invented values.

# COMMAND ----------

def validate_predictions(df, label, classification=False):
    fail_if(df, F.col("prediction").isNull() | F.isnan(F.col("prediction").cast("double"))
        | (F.abs(F.col("prediction").cast("double")) == float("inf")), "Invalid model predictions")
    if classification:
        fail_if(df, ~F.col("prediction").isin(0.0, 1.0), "Non-binary classification predictions")

def regression_metrics(df):
    validate_predictions(df, REG_LABEL)
    error = F.col("prediction") - F.col(REG_LABEL)
    stat = df.agg(F.count("*").alias("n"), F.avg(F.abs(error)).alias("MAE"),
        F.sqrt(F.avg(error * error)).alias("RMSE"), F.sum(error * error).alias("sse"),
        F.var_pop(REG_LABEL).alias("variance"), F.sum(F.abs(error)).alias("sae"),
        F.sum(F.abs(F.col(REG_LABEL))).alias("absolute_target_sum")).first().asDict()
    if stat["n"] == 0:
        raise ValueError("Cannot evaluate an empty split")
    sst = (stat["variance"] or 0.0) * stat["n"]
    return {"MAE": float(stat["MAE"]), "RMSE": float(stat["RMSE"]),
        "R2": 1.0 - stat["sse"] / sst if sst > 0 else None,
        "WAPE": stat["sae"] / stat["absolute_target_sum"] if stat["absolute_target_sum"] > 0 else None}


# COMMAND ----------

# MAGIC %md
# MAGIC ## Classification Evaluation Functions
# MAGIC Use Spark evaluators for accuracy and probability-based AUC, and derive precision, recall and F1 explicitly for positive class 1 from TP/FP/TN/FN.
# MAGIC Zero-denominator positive-class scores use zero by convention; AUC remains null for a single-class evaluation split or a constant hard-label baseline.

# COMMAND ----------

def add_probability(df):
    if "probability" in df.columns:
        return df.withColumn("major_delay_probability", vector_to_array("probability")[1])
    return df.withColumn("major_delay_probability", F.lit(None).cast("double"))

def classification_metrics(df, probability_supported=True):
    validate_predictions(df, CLS_LABEL, classification=True)
    stat = df.agg(F.count("*").alias("n"),
        n_where((F.col(CLS_LABEL) == 1) & (F.col("prediction") == 1)).alias("TP"),
        n_where((F.col(CLS_LABEL) == 0) & (F.col("prediction") == 1)).alias("FP"),
        n_where((F.col(CLS_LABEL) == 0) & (F.col("prediction") == 0)).alias("TN"),
        n_where((F.col(CLS_LABEL) == 1) & (F.col("prediction") == 0)).alias("FN")).first().asDict()
    if stat["n"] == 0:
        raise ValueError("Cannot evaluate an empty split")
    tp, fp, tn, fn = (stat[name] for name in ["TP", "FP", "TN", "FN"])
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    accuracy = MulticlassClassificationEvaluator(labelCol=CLS_LABEL, predictionCol="prediction", metricName="accuracy").evaluate(df)
    roc_auc, pr_auc = None, None
    if probability_supported:
        fail_if(df, F.col("major_delay_probability").isNull() | F.isnan("major_delay_probability")
            | ~F.col("major_delay_probability").between(0.0, 1.0), "Invalid probability of major delay")
        if tp + fn > 0 and tn + fp > 0:
            evaluator = BinaryClassificationEvaluator(labelCol=CLS_LABEL, rawPredictionCol="major_delay_probability")
            roc_auc = evaluator.setMetricName("areaUnderROC").evaluate(df)
            pr_auc = evaluator.setMetricName("areaUnderPR").evaluate(df)
    return {"Accuracy": float(accuracy), "Precision": precision, "Recall": recall, "F1": f1,
        "precision_positive": precision, "recall_positive": recall, "f1_positive": f1,
        "ROC_AUC": roc_auc, "PR_AUC": pr_auc, "TP": tp, "FP": fp, "TN": tn, "FN": fn}


# COMMAND ----------

# MAGIC %md
# MAGIC ## Metric and Split Edge Checks
# MAGIC Validate temporal split allocation and metric edge cases with tiny in-memory fixtures before model training.
# MAGIC These checks cover zero WAPE/R² denominators and positive-class confusion metrics without creating persistent data.

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
assert all(math.isclose(class_fixture_metrics[name], 0.5) for name in ["Accuracy", "Precision", "Recall", "F1"])
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
# MAGIC ## MLflow Experiment Tracking
# MAGIC Track every baseline and fitted model with feature lists, hyperparameters, source version, split sizes/windows and preliminary-modeling tags using integrated MLflow.
# MAGIC Validation determines the preliminary candidates before test evaluation; model serialization is attempted as an optional artifact capability, with no registry calls.

# COMMAND ----------

if mlflow.active_run() is not None:
    raise RuntimeError("An MLflow run is already active; finish it before starting this notebook")
experiment = mlflow.set_experiment(EXPERIMENT_NAME)
feature_version = hashlib.sha256(json.dumps(feature_columns).encode("utf-8")).hexdigest()[:16]
common_params = {"source_table": SOURCE_TABLE, "source_delta_version": SOURCE_VERSION,
    "feature_set_version": feature_version, "imputer_strategy": "median", "imputer_relative_error": 0.001,
    "categorical_invalid_handling": "keep", "scaler_with_mean": False, "scaler_with_std": True,
    "validation_start_epoch": validation_start,
    "test_start_epoch": test_start, "seed": SEED, "lightweight_profile": LIGHTWEIGHT_PROFILE}
for split, stat in split_stats.items():
    common_params.update({f"{split}_rows": stat["row_count"], f"{split}_feed_snapshots": stat["distinct_feed_snapshots"],
        f"{split}_window_start": str(stat["min_feed_datetime"]), f"{split}_window_end": str(stat["max_feed_datetime"])})
run_tags = {"project": "Rome Public Transport Reliability & Delay Prediction", "stage": "preliminary_modeling",
    "validation_strategy": "temporal_split", "label_type": "realized_future_next_stop_delay",
    "label_semantics": "future_GTFS_observation_proxy", "registration": "deferred"}
records = {"regression": {}, "classification": {}}
skipped_components = []

def optional_capability_error(exc):
    message = str(exc).lower()
    return isinstance(exc, (ImportError, NotImplementedError)) or any(token in message for token in [
        "jvm_attribute_not_supported", "unsupported_operation", "operation_not_supported", "not supported on serverless", "not supported with spark connect",
        "not supported for spark connect", "does not support spark connect", "dbfs is not supported",
        "uc volume path", "mlflow_dfs_tmp", "dfs_tmpdir",
    ]) or (("rdd" in message or "sparkcontext" in message) and ("not implemented" in message or "not supported" in message))

def log_metrics(metrics, split):
    finite = {f"{split}_{name}": float(value) for name, value in metrics.items()
        if value is not None and math.isfinite(float(value))}
    mlflow.log_metrics(finite)
    undefined = [name for name, value in metrics.items() if value is None or not math.isfinite(float(value))]
    if undefined:
        mlflow.set_tag(f"{split}_undefined_metrics", ",".join(undefined))

def initialize_run(name, task, parameters, used_features):
    mlflow.set_tags(run_tags)
    mlflow.log_params(common_params | {"model_name": name, "task": task, "feature_count": len(used_features)} | parameters)
    mlflow.log_dict({"numeric_features": selected_numeric_features, "categorical_features": selected_categorical_features,
        "actual_model_inputs": used_features, "dropped_all_null_features": dropped_all_null_features,
        "missing_optional_features": missing_optional_features, "forbidden_columns": forbidden_columns}, "feature_contract.json")

def log_model_optional(model):
    if not LOG_MODEL_ARTIFACTS:
        mlflow.set_tag("model_artifact_status", "disabled_by_configuration")
        return
    try:
        mlflow.spark.log_model(model, artifact_path="model")
        mlflow.set_tag("model_artifact_status", "logged")
    except Exception as exc:
        if not optional_capability_error(exc):
            raise
        message = f"Optional Spark model logging unavailable: {type(exc).__name__}: {str(exc)[:1000]}"
        print(message)
        skipped_components.append(message)
        mlflow.set_tag("model_artifact_status", "unsupported_environment")
        mlflow.log_text(message, "model_logging_unavailable.txt")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Importance Mapping
# MAGIC Map tree feature-vector indices through the assembler metadata to the original numeric fields and one-hot category names.
# MAGIC Only models exposing native feature importance receive importance artifacts; permutation importance will be added after final model selection.

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
TREE_IMPORTANCE_MODELS = {"RandomForestRegressor", "GBTRegressor", "RandomForestClassifier"}

def log_feature_importance(name, fitted):
    if name not in TREE_IMPORTANCE_MODELS:
        return []
    importance = fitted.stages[-1].featureImportances.toArray().tolist()
    if len(importance) != vector_feature_count:
        raise ValueError("Feature importance size differs from assembled metadata")
    rows = sorted([(vector_feature_names[i], float(value)) for i, value in enumerate(importance)],
        key=lambda item: (-item[1], item[0]))
    spark.createDataFrame(rows[:20], "feature STRING, importance DOUBLE").show(20, truncate=False)
    mlflow.log_dict({"importance_type": "native_tree_importance", "features": [
        {"feature": feature, "importance": value} for feature, value in rows]}, "feature_importance.json")
    return rows


# COMMAND ----------

# MAGIC %md
# MAGIC ## Reusable Model Run Workflow
# MAGIC Fit each estimator on the training partition using already fitted preprocessing stages and evaluate validation before any test comparison.
# MAGIC Runs preserve hyperparameters, metrics, confusion counts, feature contracts and supported model artifacts; optional capability failures are reported explicitly and other exceptions propagate.

# COMMAND ----------

def evaluate_model(fitted, task, split, probability_supported=True):
    predictions = fitted.transform(parts[split])
    expected_rows = split_stats[split]["row_count"]
    if predictions.count() != expected_rows:
        raise ValueError("Model transform dropped or duplicated evaluation rows")
    if task == "classification":
        predictions = add_probability(predictions)
        metrics = classification_metrics(predictions, probability_supported)
    else:
        metrics = regression_metrics(predictions)
    return predictions, metrics

def store_run(name, task, fitted, parameters, used_features, run_id, probability_supported=True):
    predictions, metrics = evaluate_model(fitted, task, "validation", probability_supported)
    log_metrics(metrics, "validation")
    if task == "classification":
        mlflow.log_dict({key: metrics[key] for key in ["TP", "FP", "TN", "FN"]}, "validation_confusion_matrix.json")
    importance_rows = log_feature_importance(name, fitted)
    log_model_optional(fitted)
    records[task][name] = {"model": fitted, "run_id": run_id, "parameters": parameters,
        "validation": metrics, "probability_supported": probability_supported,
        "importance": importance_rows}
    print(name, "validation:", metrics)

def train_estimator(name, task, factory, parameters, optional=False):
    with mlflow.start_run(run_name=name) as run:
        initialize_run(name, task, parameters, feature_columns)
        mlflow.log_param("assembled_feature_count", vector_feature_count)
        try:
            estimator = factory()
            fitted = Pipeline(stages=preprocessor_model.stages + [estimator]).fit(parts["train"])
            store_run(name, task, fitted, parameters, feature_columns, run.info.run_id)
        except Exception as exc:
            if not optional or not optional_capability_error(exc):
                raise
            message = f"Optional model unavailable in current environment: {type(exc).__name__}: {str(exc)[:1000]}"
            print(message)
            skipped_components.append(name + ": " + message)
            mlflow.set_tag("component_status", "skipped_environment_capability")
            mlflow.log_text(message, "optional_capability_failure.txt")

def baseline_run(name, task, statement, parameters, used_features):
    with mlflow.start_run(run_name=name) as run:
        initialize_run(name, task, parameters, used_features)
        fitted = PipelineModel(stages=[SQLTransformer(statement=statement)])
        store_run(name, task, fitted, parameters, used_features, run.info.run_id, probability_supported=False)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Regression Baselines
# MAGIC Persistence uses the current arrival delay, while the historical baseline falls back from route-stop history to route history; both ultimately use the median regression target from training only.
# MAGIC The same fallback strategy scores every validation/test row, avoiding artificial coverage advantages from dropping missing baseline inputs.

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
historical_expression = f"COALESCE({sql_missing_numeric('historical_route_stop_mean_delay')}, {sql_missing_numeric('historical_route_mean_delay')}, {train_target_median!r})"
baseline_run("Persistence", "regression", f"SELECT *, {persistence_expression} AS prediction FROM __THIS__",
    {"fallback_train_target_median": train_target_median}, ["current_arrival_delay_seconds"])
baseline_run("Historical", "regression", f"SELECT *, {historical_expression} AS prediction FROM __THIS__",
    {"fallback_train_target_median": train_target_median}, ["historical_route_stop_mean_delay", "historical_route_mean_delay"])


# COMMAND ----------

# MAGIC %md
# MAGIC ## Linear Regression
# MAGIC Train a regularized linear model using the scaled vector and fixed initial parameters without extended tuning.
# MAGIC The scaler and imputer were fitted on training only, and regularization is recorded for transparent comparison.

# COMMAND ----------

linear_params = {"regParam": 0.1, "elasticNetParam": 0.0, "maxIter": 100, "standardization": False}
train_estimator("LinearRegression", "regression",
    lambda: LinearRegression(labelCol=REG_LABEL, featuresCol="scaled_features", **linear_params), linear_params)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Random Forest and Gradient-Boosted Regression
# MAGIC Train seeded tree ensembles on the unscaled vector, using the explicit lightweight profile of 50 trees/depth 6 and 30 boosting iterations/depth 5 by default.
# MAGIC The larger forest profile is configurable upfront; no resource-driven retry changes model complexity based on held-out results.

# COMMAND ----------

rf_params = {"numTrees": TREE_NUM_TREES, "maxDepth": TREE_MAX_DEPTH, "maxBins": 32, "seed": SEED}
gbt_params = {"maxIter": GBT_ITERATIONS, "maxDepth": 5, "stepSize": 0.1, "maxBins": 32, "seed": SEED}
train_estimator("RandomForestRegressor", "regression",
    lambda: RandomForestRegressor(labelCol=REG_LABEL, featuresCol="features", **rf_params), rf_params)
train_estimator("GBTRegressor", "regression",
    lambda: GBTRegressor(labelCol=REG_LABEL, featuresCol="features", **gbt_params), gbt_params)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Classification Baseline and Training Class Balance
# MAGIC Determine the majority class from training alone, choosing class 0 on a tie and using constant hard predictions for the baseline.
# MAGIC AUC is not reported for this baseline; if training contains only one class, learned binary classifiers are explicitly skipped while regression and baseline evaluation continue.

# COMMAND ----------

positive_count, negative_count = training_label_summary["positive_count"], training_label_summary["negative_count"]
majority_class = 1 if positive_count > negative_count else 0
print("Training classes:", {"major_delay": positive_count, "non_major_delay": negative_count})
baseline_run("MajorityClass", "classification",
    f"SELECT *, CAST({majority_class} AS DOUBLE) AS prediction FROM __THIS__",
    {"majority_class": majority_class, "tie_policy": "class_0"}, [])
can_train_classifier = positive_count > 0 and negative_count > 0
if not can_train_classifier:
    message = "Training contains only one class; learned binary classifiers are not identifiable and are skipped."
    print(message)
    skipped_components.append(message)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Logistic Regression and Random Forest Classification
# MAGIC Train logistic regression with scaling and a random forest on unscaled features when both training classes are present.
# MAGIC Use fixed parameters and the default 0.5 probability decision threshold, with no class balancing or threshold optimization fitted on validation/test data.

# COMMAND ----------

logistic_params = {"regParam": 0.1, "elasticNetParam": 0.0, "maxIter": 100,
    "standardization": False, "threshold": 0.5}
if can_train_classifier:
    train_estimator("LogisticRegression", "classification",
        lambda: LogisticRegression(labelCol=CLS_LABEL, featuresCol="scaled_features", **logistic_params), logistic_params)
    train_estimator("RandomForestClassifier", "classification",
        lambda: RandomForestClassifier(labelCol=CLS_LABEL, featuresCol="features", **rf_params), rf_params)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Preliminary Candidate Selection
# MAGIC Identify the preliminary regression candidate by validation MAE and the classification candidate by positive-class validation F1, including baselines in both comparisons.
# MAGIC Resolve exact metric ties by model name for reproducibility, and freeze these names before evaluating test performance.

# COMMAND ----------

best_preliminary_regression_model_by_validation_MAE = min(records["regression"],
    key=lambda name: (records["regression"][name]["validation"]["MAE"], name))
best_preliminary_classification_model_by_validation_F1 = min(records["classification"],
    key=lambda name: (-records["classification"][name]["validation"]["F1"], name))
frozen_preliminary_selection = {
    "regression": best_preliminary_regression_model_by_validation_MAE,
    "classification": best_preliminary_classification_model_by_validation_F1,
}
print("Frozen preliminary candidates selected on validation only:", frozen_preliminary_selection)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Evaluation After Selection
# MAGIC Evaluate every fixed candidate once on the untouched test partition after freezing the preliminary selection and all parameters.
# MAGIC Log test metrics to the corresponding run without refitting preprocessing, retuning models or changing candidates in response to test results.

# COMMAND ----------

for task, models in records.items():
    for name, record in models.items():
        predictions, metrics = evaluate_model(record["model"], task, "test", record["probability_supported"])
        record["test"] = metrics
        with mlflow.start_run(run_id=record["run_id"]):
            log_metrics(metrics, "test")
            if task == "classification":
                mlflow.log_dict({key: metrics[key] for key in ["TP", "FP", "TN", "FN"]}, "test_confusion_matrix.json")
            mlflow.set_tag("preliminary_candidate_selected_on_validation", str(name == frozen_preliminary_selection[task]))
        print(name, "test:", metrics)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Regression Error Analysis
# MAGIC Analyze validation errors for the already selected preliminary regression candidate through signed bias, absolute-error percentiles and MAE by route and hour.
# MAGIC These operational diagnostics stay in the notebook and MLflow and do not create Gold or production prediction tables.

# COMMAND ----------

best_regression_record = records["regression"][frozen_preliminary_selection["regression"]]
regression_errors = (best_regression_record["model"].transform(parts["validation"])
    .withColumn("error", F.col("prediction") - F.col(REG_LABEL))
    .withColumn("absolute_error", F.abs("error")))
regression_error_summary = regression_errors.agg(
    F.avg("error").alias("mean_error"),
    F.percentile_approx("absolute_error", 0.5, 10000).alias("median_absolute_error"),
    F.percentile_approx("absolute_error", 0.90, 10000).alias("P90_absolute_error"),
    F.percentile_approx("absolute_error", 0.95, 10000).alias("P95_absolute_error"))
regression_error_summary.show(truncate=False)
regression_errors.select("route_id", "feed_hour", REG_LABEL, "prediction", "error", "absolute_error").orderBy(
    "route_id", "feed_hour", REG_LABEL, "prediction").show(20, truncate=False)
regression_errors.groupBy("route_id").agg(F.count("*").alias("observations"), F.avg("absolute_error").alias("MAE")).orderBy(
    F.col("MAE").desc(), "route_id").show(50, truncate=False)
regression_errors.groupBy("feed_hour").agg(F.count("*").alias("observations"), F.avg("absolute_error").alias("MAE")).orderBy(
    "feed_hour").show(24, truncate=False)
with mlflow.start_run(run_id=best_regression_record["run_id"]):
    mlflow.log_dict(regression_error_summary.first().asDict(), "validation_error_summary.json")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Confusion Matrix and Probability Analysis
# MAGIC Inspect TP/FP/TN/FN and the actual-versus-predicted count table for the validation-selected classifier.
# MAGIC For learned classifiers, summarize class-1 probabilities separately by actual class and fixed probability bins; a selected majority baseline has no meaningful probability analysis.

# COMMAND ----------

best_classification_record = records["classification"][frozen_preliminary_selection["classification"]]
classification_predictions = add_probability(best_classification_record["model"].transform(parts["validation"]))
print("Positive-class confusion counts:", {key: best_classification_record["validation"][key] for key in ["TP", "FP", "TN", "FN"]})
classification_predictions.select(F.col(CLS_LABEL).alias("actual"), "prediction").groupBy(
    "actual", "prediction").count().orderBy("actual", "prediction").show(truncate=False)
if best_classification_record["probability_supported"]:
    probability_summary = classification_predictions.groupBy(CLS_LABEL).agg(
        F.count("*").alias("count"), F.avg("major_delay_probability").alias("mean_probability"),
        F.min("major_delay_probability").alias("min_probability"),
        F.percentile_approx("major_delay_probability", [0.1, 0.5, 0.9], 10000).alias("probability_P10_P50_P90"),
        F.max("major_delay_probability").alias("max_probability"))
    probability_summary.orderBy(CLS_LABEL).show(truncate=False)
    classification_predictions.withColumn("probability_bin", F.least(F.lit(9), F.floor(F.col("major_delay_probability") * 10)))         .groupBy(CLS_LABEL, "probability_bin").count().orderBy(CLS_LABEL, "probability_bin").show(20, truncate=False)
    with mlflow.start_run(run_id=best_classification_record["run_id"]):
        mlflow.log_dict({"by_actual_class": [row.asDict() for row in probability_summary.collect()]}, "validation_probability_summary.json")
else:
    print("The validation-selected majority baseline has no probability-vector distribution; ROC/PR AUC remain undefined.")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Model Comparison and Validation–Test Gaps
# MAGIC Compare all models transparently across validation and test without declaring a production winner.
# MAGIC The regression gap is test MAE minus validation MAE and the classification gap is validation F1 minus test F1; positive values indicate deterioration, without an arbitrary success threshold.

# COMMAND ----------

regression_metric_names = ["MAE", "RMSE", "R2", "WAPE"]
classification_metric_names = ["Accuracy", "precision_positive", "recall_positive", "f1_positive", "ROC_AUC", "PR_AUC"]
regression_rows, classification_rows, regression_long_rows = [], [], []
for name, record in records["regression"].items():
    values = [record[split][metric] for split in ["validation", "test"] for metric in regression_metric_names]
    gap = record["test"]["MAE"] - record["validation"]["MAE"]
    regression_rows.append((name, *values, gap))
    for split in ["validation", "test"]:
        regression_long_rows.append((name, split, *[record[split][metric] for metric in regression_metric_names]))
    with mlflow.start_run(run_id=record["run_id"]):
        mlflow.log_metric("regression_validation_test_MAE_gap", gap)
for name, record in records["classification"].items():
    values = [record[split][metric] for split in ["validation", "test"] for metric in classification_metric_names]
    gap = record["validation"]["F1"] - record["test"]["F1"]
    classification_rows.append((name, *values, gap))
    with mlflow.start_run(run_id=record["run_id"]):
        mlflow.log_metric("classification_validation_test_F1_gap", gap)
regression_schema = "model STRING, " + ", ".join(f"{split}_{metric} DOUBLE" for split in ["validation", "test"] for metric in regression_metric_names)
classification_schema = "model STRING, " + ", ".join(f"{split}_{'accuracy' if metric == 'Accuracy' else metric} DOUBLE" for split in ["validation", "test"] for metric in classification_metric_names)
regression_comparison = spark.createDataFrame(regression_rows, regression_schema + ", regression_validation_test_MAE_gap DOUBLE")
classification_comparison = spark.createDataFrame(classification_rows, classification_schema + ", classification_validation_test_F1_gap DOUBLE")
regression_long_comparison = spark.createDataFrame(regression_long_rows, "model STRING, split STRING, MAE DOUBLE, RMSE DOUBLE, R2 DOUBLE, WAPE DOUBLE")
regression_long_comparison.orderBy("model", "split").show(truncate=False)
regression_comparison.orderBy("validation_MAE", "model").show(truncate=False)
classification_comparison.orderBy(F.col("validation_f1_positive").desc(), "model").show(truncate=False)
with mlflow.start_run(run_name="PreliminaryModelComparison"):
    mlflow.set_tags(run_tags)
    mlflow.log_params(common_params)
    mlflow.log_dict({"regression": [row.asDict() for row in regression_comparison.collect()],
        "classification": [row.asDict() for row in classification_comparison.collect()],
        "validation_selected_candidates": frozen_preliminary_selection,
        "model_runs": {task: {name: record["run_id"] for name, record in models.items()} for task, models in records.items()},
        "skipped_components": skipped_components}, "model_comparison.json")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Validation and Outputs
# MAGIC Confirm the feature contract, train-only preprocessing, chronological split and completed validation/test results, then display the preliminary candidates and supported tree importances.
# MAGIC Model registration is intentionally deferred until sufficient realtime history has been collected.

# COMMAND ----------

assert not set(feature_columns) & set(forbidden_columns)
assert not set(input_origin.values()) & set(forbidden_columns)
assert overlap_count == 0
assert all("validation" in record and "test" in record for models in records.values() for record in models.values())
assert frozen_preliminary_selection["regression"] == best_preliminary_regression_model_by_validation_MAE
assert frozen_preliminary_selection["classification"] == best_preliminary_classification_model_by_validation_F1
print("best_preliminary_regression_model_by_validation_MAE:", best_preliminary_regression_model_by_validation_MAE)
print("best_preliminary_classification_model_by_validation_F1:", best_preliminary_classification_model_by_validation_F1)
for task in ["regression", "classification"]:
    available_trees = [name for name, record in records[task].items() if record["importance"]]
    if available_trees:
        tree_name = min(available_trees, key=lambda name: (
            records[task][name]["validation"]["MAE"] if task == "regression" else -records[task][name]["validation"]["F1"], name))
        print(f"Top {task} feature importance from supported tree model {tree_name}:")
        spark.createDataFrame(records[task][tree_name]["importance"][:20], "feature STRING, importance DOUBLE").show(20, truncate=False)
    else:
        print(f"No supported native {task} feature importance is available")
print("MLflow experiment:", {"name": experiment.name, "experiment_id": experiment.experiment_id, "tracking_uri": mlflow.get_tracking_uri()})
print("Optional/skipped components:", skipped_components)
print("Model registration is intentionally deferred until sufficient realtime history has been collected.")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Modeling Limitations
# MAGIC Metrics remain preliminary under limited temporal coverage and future-GTFS labels remain observation proxies whose realization quality varies, so results must not be described as final production performance.
# MAGIC Historical features assume sequential scoring may use earlier observed feed values, while retrospective static metadata and ingestion-time availability remain upstream limitations; route generalization, class imbalance and label-boundary purging require continued review as history grows.
# MAGIC No hyperparameter search, random evaluation split, Gold output, production batch scoring or automatic model registration is performed, and fixed seeds improve reproducibility without guaranteeing identical floating-point reductions across runtime versions.