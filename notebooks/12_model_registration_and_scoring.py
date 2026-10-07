# Databricks notebook source
# MAGIC %md
# MAGIC # Model Registration and Batch Scoring
# MAGIC Refit the frozen Random Forest classifier on historically eligible data and generate auditable batch predictions with an explicit Persistence regression rule. Historical notebook 11 test metrics remain reference results, not estimates for this full-data refit.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Model Contract
# MAGIC Hyperparameters, threshold and regression fallback are fixed from the supplied notebook 11 results; no selection or evaluation search is performed. Classification uses strict probability greater than 0.45, and Persistence retains the original training median of -71.0 seconds.

# COMMAND ----------

import json
import math
import hashlib
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, TimestampType
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import Imputer, StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler, SQLTransformer
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.functions import vector_to_array
import mlflow
import mlflow.spark
from mlflow import MlflowClient
from mlflow.models import ModelSignature
from mlflow.types.schema import Schema, ColSpec

spark.conf.set("spark.sql.session.timeZone", "Europe/Rome")
SOURCE_TABLE = "rome_transport.features.next_stop_delay_labeled"
REQUESTED_SOURCE_VERSION = 8
SOURCE_VERSION = REQUESTED_SOURCE_VERSION
CHECKPOINT_TABLE = "rome_transport.ml.model_tuning_results"
METADATA_TABLE = "rome_transport.ml.model_registry_metadata"
PREDICTIONS_TABLE = "rome_transport.ml.next_stop_delay_predictions"
REGISTERED_MODEL_NAME = "rome_transport.ml.major_delay_classifier"
EXPERIMENT_NAME = "/Shared/rome_transport_next_stop_delay"
MODEL_NAME = "major_delay_classifier"
REG_LABEL = "target_realized_next_stop_delay_seconds"
CLS_LABEL = "target_realized_major_delay_flag"
OBS_KEY = ["feed_timestamp", "entity_id", "trip_id", "stop_sequence", "stop_id"]
FINAL_PARAMS = dict(featureSubsetStrategy="sqrt", maxBins=32, maxDepth=10,
                    minInstancesPerNode=5, numTrees=100, seed=42)
CLASSIFICATION_THRESHOLD = 0.45
PERSISTENCE_FALLBACK = -71.0
REGRESSION_METHOD = "persistence_with_train_median_fallback"
# Optional existing, writable UC volume directory; never creates a volume or changes permissions.
UC_MODEL_TMP_DIR = None  # e.g. /Volumes/<catalog>/<schema>/<existing_volume>/spark_ml_tmp
# Only needed if both source version 8 and the original MLflow contract are unavailable.
FROZEN_NUMERIC_FEATURES = None
FROZEN_CATEGORICAL_FEATURES = None
SELECTION_RUN_ID = None  # Optional exact notebook 11 final-classification run ID.
DEPLOYMENT_CUTOFF = datetime.now(timezone.utc)
DEPLOYMENT_EPOCH = int(DEPLOYMENT_CUTOFF.timestamp())
REFERENCE_TEST = dict(accuracy=0.9401, precision_positive=0.8450, recall_positive=0.8123,
    f1_positive=0.8283, roc_auc=0.9690, pr_auc=0.9063, TP=251060, FP=46054, TN=1381007, FN=58026)
REFERENCE_REGRESSION_TEST = dict(MAE=136.9818, RMSE=401.8525, R2=0.6940, WAPE=0.3293)
FROZEN_CONTRACT_JSON = json.dumps(dict(params=FINAL_PARAMS, threshold=CLASSIFICATION_THRESHOLD,
    fallback=PERSISTENCE_FALLBACK), sort_keys=True)
def assert_frozen():
    assert json.dumps(dict(params=FINAL_PARAMS, threshold=CLASSIFICATION_THRESHOLD,
        fallback=PERSISTENCE_FALLBACK), sort_keys=True) == FROZEN_CONTRACT_JSON

def n_where(condition):
    return F.coalesce(F.sum(F.when(condition, 1).otherwise(0)), F.lit(0))
def require_columns(df, columns):
    missing = sorted(set(columns) - set(df.columns))
    if missing: raise ValueError(f"Missing required columns: {missing}")
def fail_if(df, condition, message):
    if df.filter(condition).limit(1).count(): raise ValueError(message)
def duplicate_check(df):
    count = df.groupBy(*OBS_KEY).count().filter(F.col("count") > 1).limit(1).count()
    if count: raise ValueError("Duplicate observation grain")
    return count


# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Modeling Dataset
# MAGIC Attempt a materialized read of Delta version 8 and fall back to the latest fixed version only for recognized time-travel or missing historical-file errors. Labels used for refitting must have been realized before the captured deployment cutoff; the cutoff and actual source version are recorded.

# COMMAND ----------

def historical_version_unavailable(exc):
    text = str(exc).lower()
    return any(token in text for token in ["delta_version_not_found", "delta_cannot_time_travel",
        "delta_truncated_transaction_log", "delta_file_not_found", "delta_missing_part_files",
        "cannot time travel delta table to version", "cannot time travel delta table to timestamp"]) or (
        "filenotfoundexception" in text and ("vacuum" in text or "delta" in text))

def read_version(version):
    df = spark.read.option("versionAsOf", version).table(SOURCE_TABLE)
    # Force a full data read: a metadata-only count would not detect vacuumed data files.
    probe = df.agg(F.count("*").alias("rows"), F.min(F.col(CLS_LABEL).cast("double")).alias("min_label"),
                   F.max(F.col("feed_timestamp")).alias("max_feed")).first().asDict()
    if not probe["rows"]: raise ValueError("Empty source dataset")
    return df, probe

source_fallback_reason = None
try:
    model_df, source_probe = read_version(SOURCE_VERSION)
except Exception as exc:
    if not historical_version_unavailable(exc): raise
    source_fallback_reason = str(exc)[:2000]
    SOURCE_VERSION = int(spark.sql(f"DESCRIBE HISTORY {SOURCE_TABLE} LIMIT 1").select("version").first()[0])
    if SOURCE_VERSION == REQUESTED_SOURCE_VERSION: raise
    print(f"VERSION 8 UNAVAILABLE; explicitly substituting fixed Delta version {SOURCE_VERSION}: {source_fallback_reason}")
    model_df, source_probe = read_version(SOURCE_VERSION)
print("Actual pinned source:", SOURCE_TABLE, SOURCE_VERSION)
require_columns(model_df, OBS_KEY + [REG_LABEL, CLS_LABEL, "feed_datetime", "service_date", "vehicle_id", "route_id",
    "current_arrival_delay_seconds", "label_source_feed_timestamp", "label_horizon_seconds"])
fail_if(model_df, F.col("feed_timestamp").isNull() | F.col("label_source_feed_timestamp").isNull(), "Missing eligibility timestamps")
training_df = model_df.filter((F.col("feed_timestamp") < DEPLOYMENT_EPOCH)
    & (F.col("label_source_feed_timestamp") < DEPLOYMENT_EPOCH))
fail_if(training_df, F.col(CLS_LABEL).isNull() | ~F.col(CLS_LABEL).isin(0, 1)
    | F.col(REG_LABEL).isNull() | F.isnan(F.col(REG_LABEL).cast("double"))
    | (F.abs(F.col(REG_LABEL)) > 43200)
    | ~F.col(CLS_LABEL).eqNullSafe((F.col(REG_LABEL) >= 300).cast("int"))
    | (F.col("label_source_feed_timestamp") <= F.col("feed_timestamp"))
    | ~F.col("label_horizon_seconds").eqNullSafe(F.col("label_source_feed_timestamp") - F.col("feed_timestamp"))
    | ~F.col("label_horizon_seconds").between(1, 1800), "Invalid historical labels or causality")
training_stats = training_df.agg(F.count("*").alias("training_rows"),
    F.countDistinct("feed_timestamp").alias("training_snapshots"), F.countDistinct("service_date").alias("service_date_count"),
    F.countDistinct(CLS_LABEL).alias("classes"), F.min("feed_timestamp").alias("training_min_feed"),
    F.max("feed_timestamp").alias("training_max_feed"), F.max("label_source_feed_timestamp").alias("max_realization")).first().asDict()
assert training_stats["training_rows"] > 0 and training_stats["classes"] == 2
assert training_stats["max_realization"] < DEPLOYMENT_EPOCH
duplicate_check(training_df)
print("Eligible training coverage:", training_stats)
print("Rows excluded after deployment cutoff:", source_probe["rows"] - training_stats["training_rows"])


# COMMAND ----------

# MAGIC %md
# MAGIC ## Selection Provenance and Feature Contract
# MAGIC Read checkpoint metadata without changing it or treating candidate rows as a new selection exercise. Recover the frozen feature lists from a matching final MLflow run; if unavailable, reproduce notebook 11's original purged training-feature availability on version 8, never select additional features from the full refit dataset.

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

if mlflow.active_run() is not None:
    raise RuntimeError("Finish the active MLflow run before executing this notebook")
mlflow.autolog(disable=True)
experiment = mlflow.set_experiment(EXPERIMENT_NAME)
client = MlflowClient()
checkpoint_available = spark.catalog.tableExists(CHECKPOINT_TABLE)
checkpoint_version = None
if checkpoint_available:
    checkpoint_version = int(spark.sql(f"DESCRIBE HISTORY {CHECKPOINT_TABLE} LIMIT 1").select("version").first()[0])
    checkpoint_df = spark.read.option("versionAsOf", checkpoint_version).table(CHECKPOINT_TABLE)
    require_columns(checkpoint_df, ["project", "notebook", "task", "candidate_id", "model_name", "params_json", "status", "run_timestamp"])
    checkpoint_rows = checkpoint_df.filter((F.col("project") == "Rome Public Transport Reliability & Delay Prediction")
        & (F.col("notebook") == "11_model_tuning_and_selection") & (F.col("task") == "classification")
        & (F.col("candidate_id") == "rf_cls_05") & (F.col("status") == "SUCCESS"))
    checkpoint_row = checkpoint_rows.orderBy(F.desc("run_timestamp")).first()
    if checkpoint_row is not None:
        checkpoint_record = checkpoint_row.asDict()
        if checkpoint_record["model_name"] != "RandomForestClassifier" or json.loads(checkpoint_record["params_json"]) != FINAL_PARAMS:
            raise ValueError("rf_cls_05 checkpoint conflicts with the frozen final parameters")
        print("Matching selected-candidate checkpoint:", checkpoint_record)
    else:
        print("No successful rf_cls_05 checkpoint found; supplied frozen results remain the selection authority.")
    print("Checkpoint has no source version, threshold or feature lists; it cannot prove full selection provenance.")
else:
    print("Checkpoint table unavailable: using the explicitly supplied frozen results and verified feature contract.")

def matching_selection_run(run):
    p, tags = run.data.params, run.data.tags
    try:
        return (tags.get("stage") == "tuning_and_selection" and tags.get("final_selection") == "true"
            and p.get("task") == "classification" and p.get("model_name") == "RandomForestClassifier"
            and int(p.get("source_delta_version", -1)) == 8
            and float(p.get("selected_probability_threshold", -1)) == CLASSIFICATION_THRESHOLD
            and all(str(p.get(k)) == str(v) for k, v in FINAL_PARAMS.items()))
    except (TypeError, ValueError): return False

selection_run = None
if SELECTION_RUN_ID is not None:
    selection_run = client.get_run(SELECTION_RUN_ID)
    if not matching_selection_run(selection_run): raise ValueError("Explicit selection run conflicts with frozen contract")
else:
    runs = client.search_runs([experiment.experiment_id],
        filter_string="tags.stage = 'tuning_and_selection' AND tags.final_selection = 'true'",
        order_by=["attributes.start_time DESC"], max_results=100)
    selection_run = next((r for r in runs if matching_selection_run(r)), None)
selection_contract_run_id = selection_run.info.run_id if selection_run else None
if (FROZEN_NUMERIC_FEATURES is None) != (FROZEN_CATEGORICAL_FEATURES is None):
    raise ValueError("Provide both frozen feature lists, or neither")
if selection_run:
    selected_numeric_features = json.loads(selection_run.data.params["numeric_features"])
    selected_categorical_features = json.loads(selection_run.data.params["categorical_features"])
    feature_contract_origin = "notebook_11_final_mlflow_run"
    if FROZEN_NUMERIC_FEATURES is not None:
        assert selected_numeric_features == FROZEN_NUMERIC_FEATURES
        assert selected_categorical_features == FROZEN_CATEGORICAL_FEATURES
elif FROZEN_NUMERIC_FEATURES is not None:
    selected_numeric_features = list(FROZEN_NUMERIC_FEATURES)
    selected_categorical_features = list(FROZEN_CATEGORICAL_FEATURES)
    feature_contract_origin = "explicit_verified_contract_override"
else:
    if SOURCE_VERSION != 8:
        raise ValueError("Version 8 unavailable and frozen feature lists not recovered: provide notebook 11 feature lists or SELECTION_RUN_ID; do not infer a new contract from latest data")
    timestamps = model_df.select("feed_timestamp").distinct()
    n = timestamps.count()
    if n < 3: raise ValueError("Insufficient snapshots to reproduce original training contract")
    train_n = max(1, min(n - 2, int(0.70 * n)))
    original_validation_start = timestamps.withColumn("rank", F.row_number().over(Window.orderBy("feed_timestamp"))).filter(F.col("rank") == train_n + 1).select("feed_timestamp").first()[0]
    original_train = model_df.filter((F.col("feed_timestamp") < original_validation_start)
        & (F.col("label_source_feed_timestamp") < original_validation_start))
    existing_numeric = [c for c in candidate_numeric_features if c in model_df.columns]
    existing_categorical = [c for c in candidate_categorical_features if c in model_df.columns]
    available = original_train.agg(
        *[F.count(F.when(~F.isnan(F.col(c).cast("double")), F.col(c))).alias(c) for c in existing_numeric],
        *[F.count(c).alias(c) for c in existing_categorical]).first().asDict()
    if any(available.get(c, 0) == 0 for c in required_features): raise ValueError("Empty required feature in original training")
    selected_numeric_features = [c for c in existing_numeric if available[c] > 0]
    selected_categorical_features = [c for c in existing_categorical if available[c] > 0]
    feature_contract_origin = "version_8_original_purged_training_reconstruction"
assert selected_numeric_features == [c for c in candidate_numeric_features if c in selected_numeric_features]
assert selected_categorical_features == [c for c in candidate_categorical_features if c in selected_categorical_features]
feature_columns = selected_numeric_features + selected_categorical_features
assert len(feature_columns) == len(set(feature_columns))
assert set(required_features) <= set(feature_columns)
assert not set(feature_columns) & set(forbidden_columns)
require_columns(model_df, feature_columns)
feature_contract_json = json.dumps(dict(numeric=selected_numeric_features, categorical=selected_categorical_features,
    forbidden=forbidden_columns, imputer="median", relativeError=0.001, stringOrderType="alphabetAsc",
    indexerHandleInvalid="keep", encoderHandleInvalid="keep", dropLast=False,
    scalerWithStd=True, scalerWithMean=False), sort_keys=True)
feature_contract_hash = hashlib.sha256(feature_contract_json.encode()).hexdigest()
print("Frozen features:", feature_columns, "origin:", feature_contract_origin)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Reproduce Final Preprocessing
# MAGIC Keep notebook 11's stage definitions and feature order, including the scaler even though RF uses the unscaled vector. Refit stage statistics and the category vocabulary on all eligible historical data now that selection is frozen; this creates a new deployment model, not the old holdout-evaluated model.

# COMMAND ----------

numeric_checks = []
for c in selected_numeric_features:
    value = F.expr(f"try_cast(`{c}` AS DOUBLE)")
    numeric_checks.append(n_where((F.col(c).isNotNull() & value.isNull()) | (F.abs(value) == float("inf"))).alias(c))
invalid_numeric = training_df.agg(*numeric_checks).first().asDict()
if any(invalid_numeric.values()): raise ValueError(f"Invalid numeric training inputs: {invalid_numeric}")
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
preprocessor_model = Pipeline(stages=stages).fit(training_df.select(*feature_columns))
feature_metadata = preprocessor_model.transform(training_df.select(*feature_columns)).schema["features"].metadata.get("ml_attr", {})
vector_feature_count = int(feature_metadata.get("num_attrs", 0))
if vector_feature_count <= 0:
    raise ValueError("No assembled features or missing feature-vector metadata")
print("Raw feature count:", len(feature_columns), "Encoded vector size:", vector_feature_count)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Fit Final Classifier
# MAGIC Fit the single frozen RF configuration using all eligible labels and the fitted preprocessing stages. Log the supplied test results as historical references only; this notebook computes no new accuracy, F1 or AUC against the refit data.

# COMMAND ----------

tags = {"project": "Rome Public Transport Reliability & Delay Prediction", "stage": "final_model_fit",
    "task": "classification", "model_name": "RandomForestClassifier", "selection_source": "notebook_11",
    "selection_metric": "validation_f1", "selected_threshold": "0.45", "deployment_status": "candidate_for_registration",
    "reference_test_metrics_origin": "notebook_11_frozen_holdout_evaluation_not_refit",
    "scoring_mode": "historical_latest_snapshot_replay_not_out_of_sample"}
run = client.create_run(experiment.experiment_id, tags=tags)
MLFLOW_RUN_ID = run.info.run_id
params = FINAL_PARAMS | {"source_table": SOURCE_TABLE, "source_version": SOURCE_VERSION,
    "requested_source_version": 8, "training_rows": training_stats["training_rows"],
    "training_snapshots": training_stats["training_snapshots"], "service_date_count": training_stats["service_date_count"],
    "feature_count": len(feature_columns), "encoded_feature_count": vector_feature_count,
    "selected_threshold": CLASSIFICATION_THRESHOLD, "threshold_semantics": "strict_greater_than",
    "deployment_cutoff_utc": DEPLOYMENT_CUTOFF.isoformat(), "feature_contract_origin": feature_contract_origin,
    "feature_contract_hash": feature_contract_hash, "feature_contract_json": feature_contract_json,
    "selection_run_id": selection_contract_run_id or "unavailable", "checkpoint_table": CHECKPOINT_TABLE,
    "checkpoint_version": checkpoint_version if checkpoint_version is not None else "unavailable",
    "regression_fallback": PERSISTENCE_FALLBACK, "regression_scoring_method": REGRESSION_METHOD,
    "spark_version": spark.version, "mlflow_version": mlflow.__version__}
for k, v in params.items(): client.log_param(MLFLOW_RUN_ID, k, v)
if source_fallback_reason: client.set_tag(MLFLOW_RUN_ID, "source_version_fallback_reason", source_fallback_reason)
for k, v in REFERENCE_TEST.items(): client.log_metric(MLFLOW_RUN_ID, "test_" + k, float(v))
for k, v in REFERENCE_REGRESSION_TEST.items(): client.log_metric(MLFLOW_RUN_ID, "reference_persistence_test_" + k, v)
try:
    assert_frozen()
    train_vectors = preprocessor_model.transform(training_df.select(*feature_columns, CLS_LABEL)).select("features", CLS_LABEL)
    fitted_rf = RandomForestClassifier(labelCol=CLS_LABEL, featuresCol="features", **FINAL_PARAMS).fit(train_vectors)
    final_pipeline = PipelineModel(stages=preprocessor_model.stages + [fitted_rf])
    assert all(fitted_rf.getOrDefault(k) == v for k, v in FINAL_PARAMS.items())
except Exception:
    client.set_terminated(MLFLOW_RUN_ID, status="FAILED")
    raise


# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Final Classifier
# MAGIC Validate the fitted pipeline on label-free historical inputs, checking output validity, feature dimensions and row preservation. This is an integrity check, not a new performance evaluation or threshold-selection step.

# COMMAND ----------

def model_inputs(df):
    return df.select(*[F.col(c).cast("double").alias(c) for c in selected_numeric_features],
                     *[F.col(c).cast("string").alias(c) for c in selected_categorical_features])

integrity_inputs = model_inputs(training_df)
assert set(integrity_inputs.columns) == set(feature_columns)
assert not set(integrity_inputs.columns) & set(forbidden_columns)
integrity_predictions = final_pipeline.transform(integrity_inputs)
require_columns(integrity_predictions, ["prediction", "probability", "features"])
prob = vector_to_array("probability")
vector = vector_to_array("features")
invalid_probability = F.exists(prob, lambda x: x.isNull() | F.isnan(x) | ~x.between(0.0, 1.0))
integrity = integrity_predictions.agg(F.count("*").alias("rows"),
    n_where(F.col("probability").isNull() | (F.size(prob) != 2) | invalid_probability
        | (F.abs(F.aggregate(prob, F.lit(0.0), lambda a, x: a + x) - 1.0) > 1e-8)).alias("invalid_probability"),
    n_where(F.col("prediction").isNull() | ~F.col("prediction").isin(0.0, 1.0)).alias("invalid_prediction"),
    F.min(F.size(vector)).alias("min_vector_size"), F.max(F.size(vector)).alias("max_vector_size")
).first().asDict()
assert integrity["rows"] == training_stats["training_rows"]
assert integrity["invalid_probability"] == integrity["invalid_prediction"] == 0
assert integrity["min_vector_size"] == integrity["max_vector_size"] == vector_feature_count
assert fitted_rf.numFeatures == vector_feature_count
assert_frozen()
print("Classifier integrity:", integrity)
client.log_metric(MLFLOW_RUN_ID, "integrity_validated_rows", integrity["rows"])


# COMMAND ----------

# MAGIC %md
# MAGIC ## Register Final Classifier
# MAGIC Attempt to log the complete preprocessing/RF pipeline with a label-free signature and register it in Unity Catalog; Persistence is never registered. Recognized Serverless/model-registry capability limits defer registration, while permission, schema, model and unrelated errors propagate ([MLflow Spark API](https://mlflow.org/docs/latest/api_reference/python_api/mlflow.spark.html)).

# COMMAND ----------

def known_registration_limitation(exc):
    message = str(exc).lower()
    if "uc volume path must be provided" in message:
        return True
    if "jvm_attribute_not_supported" in message:
        return True
    capability = any(x in message for x in ["not supported", "not implemented", "unavailable", "not available"])
    environment = any(x in message for x in ["serverless", "spark connect", "free edition"])
    model_context = any(x in message for x in ["sparkml", "spark ml", "model registry", "model registration", "model logging", "sparkcontext", "rdd"])
    return capability and environment and model_context

registration_status = "NOT_ATTEMPTED"
registration_reason = None
registered_model_name = None
registered_model_version = None
artifact_uri = None
signature = ModelSignature(inputs=Schema(
    [ColSpec("double", c, required=False) for c in selected_numeric_features]
    + [ColSpec("string", c, required=False) for c in selected_categorical_features]),
    outputs=Schema([ColSpec("double", "prediction")]))
if UC_MODEL_TMP_DIR is not None and not UC_MODEL_TMP_DIR.startswith("/Volumes/"):
    raise ValueError("UC_MODEL_TMP_DIR must refer to an existing /Volumes/ directory")
spark.sql("CREATE SCHEMA IF NOT EXISTS rome_transport.ml")
try:
    mlflow.set_registry_uri("databricks-uc")
    with mlflow.start_run(run_id=MLFLOW_RUN_ID):
        kwargs = {"dfs_tmpdir": UC_MODEL_TMP_DIR} if UC_MODEL_TMP_DIR else {}
        info = mlflow.spark.log_model(final_pipeline, artifact_path="classifier_pipeline", signature=signature, **kwargs)
        artifact_uri = info.model_uri
        registered = mlflow.register_model(model_uri=artifact_uri, name=REGISTERED_MODEL_NAME, await_registration_for=60)
        registered_model_name = registered.name
        registered_model_version = str(registered.version)
        if str(registered.status) not in {"READY", "PENDING_REGISTRATION"}:
            raise RuntimeError(f"Unexpected model registration status: {registered.status}")
        registration_status = "REGISTERED" if str(registered.status) == "READY" else "REGISTRATION_PENDING"
        registration_reason = None if registration_status == "REGISTERED" else str(registered.status)
except Exception as exc:
    if not known_registration_limitation(exc):
        client.set_tag(MLFLOW_RUN_ID, "deployment_status", "registration_failed_unexpected")
        client.set_terminated(MLFLOW_RUN_ID, status="FAILED")
        raise
    registration_status = "DEFERRED_SERVERLESS_LIMITATION"
    registration_reason = f"{type(exc).__name__}: {str(exc)[:2500]}"
    print("Registration deferred; continue with the in-memory fitted pipeline:", registration_reason)
client.set_tag(MLFLOW_RUN_ID, "deployment_status", registration_status)
client.set_tag(MLFLOW_RUN_ID, "registration_status", registration_status)
if registration_reason: client.set_tag(MLFLOW_RUN_ID, "registration_reason", registration_reason)
if registered_model_version: client.set_tag(MLFLOW_RUN_ID, "registered_model_version", registered_model_version)
print("Registration:", registration_status, registered_model_name, registered_model_version, artifact_uri)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Deployment Metadata
# MAGIC MERGE one classifier metadata row per model/task and retain the last successful registration if a later attempt is deferred or pending. The latest attempt is recorded separately within that row, while every scoring batch carries its own current run and registration provenance.

# COMMAND ----------

metadata_schema = """model_name STRING, task STRING, algorithm STRING, mlflow_run_id STRING,
registered_model_name STRING, registered_model_version STRING, registration_status STRING,
registration_reason STRING, artifact_uri STRING, source_table STRING, source_version LONG,
training_rows LONG, training_snapshots LONG, service_date_count LONG, feature_count LONG,
encoded_feature_count LONG, probability_threshold DOUBLE, validation_selection_metric STRING,
reference_test_metric_name STRING, reference_test_metric_value DOUBLE,
params_json STRING, feature_contract_json STRING, feature_contract_hash STRING,
reference_test_metrics_json STRING, regression_rule_json STRING, selection_run_id STRING,
checkpoint_table STRING, checkpoint_version LONG, deployment_cutoff TIMESTAMP,
source_fallback_reason STRING, created_timestamp TIMESTAMP, last_attempt_json STRING,
last_attempt_timestamp TIMESTAMP"""
created_timestamp = datetime.now(timezone.utc)
attempt_payload = dict(model_name=MODEL_NAME, task="classification", mlflow_run_id=MLFLOW_RUN_ID,
    registered_model_name=registered_model_name, registered_model_version=registered_model_version,
    registration_status=registration_status, registration_reason=registration_reason, artifact_uri=artifact_uri,
    source_table=SOURCE_TABLE, source_version=SOURCE_VERSION, feature_contract_hash=feature_contract_hash,
    params=FINAL_PARAMS, feature_contract=json.loads(feature_contract_json), threshold=CLASSIFICATION_THRESHOLD,
    training=training_stats, checkpoint_version=checkpoint_version, selection_run_id=selection_contract_run_id,
    deployment_cutoff=DEPLOYMENT_CUTOFF.isoformat(), created_timestamp=created_timestamp.isoformat())
metadata_values = (MODEL_NAME, "classification", "RandomForestClassifier", MLFLOW_RUN_ID,
    registered_model_name, registered_model_version, registration_status, registration_reason, artifact_uri,
    SOURCE_TABLE, int(SOURCE_VERSION), int(training_stats["training_rows"]), int(training_stats["training_snapshots"]),
    int(training_stats["service_date_count"]), len(feature_columns), vector_feature_count, CLASSIFICATION_THRESHOLD,
    "validation_f1", "historical_notebook_11_test_f1_positive", REFERENCE_TEST["f1_positive"],
    json.dumps(FINAL_PARAMS, sort_keys=True), feature_contract_json, feature_contract_hash, json.dumps(REFERENCE_TEST),
    json.dumps(dict(method=REGRESSION_METHOD, fallback=PERSISTENCE_FALLBACK, reference_test=REFERENCE_REGRESSION_TEST)),
    selection_contract_run_id, CHECKPOINT_TABLE, checkpoint_version, DEPLOYMENT_CUTOFF, source_fallback_reason,
    created_timestamp, json.dumps(attempt_payload, sort_keys=True), created_timestamp)
metadata_df = spark.createDataFrame([metadata_values], metadata_schema)
spark.sql(f"CREATE TABLE IF NOT EXISTS {METADATA_TABLE} ({metadata_schema}) USING DELTA")
existing_metadata = spark.table(METADATA_TABLE)
require_columns(existing_metadata, metadata_df.columns)
if existing_metadata.groupBy("model_name", "task").count().filter(F.col("count") > 1).limit(1).count():
    raise ValueError("Duplicate model/task metadata keys; resolve before MERGE")
metadata_df.createOrReplaceTempView("_model_registration_metadata_attempt")
spark.sql(f"""MERGE INTO {METADATA_TABLE} AS t
USING _model_registration_metadata_attempt AS s
ON t.model_name = s.model_name AND t.task = s.task
WHEN MATCHED AND t.registration_status = 'REGISTERED' AND s.registration_status <> 'REGISTERED'
THEN UPDATE SET t.last_attempt_json = s.last_attempt_json, t.last_attempt_timestamp = s.last_attempt_timestamp
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *""")
print("Registry metadata saved; current attempt status:", registration_status)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Persistence Regression Scoring Rule
# MAGIC Use current arrival delay unchanged, falling back to -71.0 seconds for missing values (including NaN, consistent with notebook 11). The fallback is never recomputed from the full refit dataset.

# COMMAND ----------

def persistence_prediction(column):
    value = column.cast("double")
    return F.when(value.isNull() | F.isnan(value), F.lit(PERSISTENCE_FALLBACK)).otherwise(value)

# Small boundary checks cover missing values and exact-threshold semantics.
fixture = spark.createDataFrame([(None, 0.45, -71.0, 0), (float("nan"), 0.450001, -71.0, 1),
    (-10.0, 0.449999, -10.0, 0), (350.0, 1.0, 350.0, 1)],
    "delay DOUBLE, probability DOUBLE, expected_delay DOUBLE, expected_flag INT")
assert fixture.withColumn("actual_delay", persistence_prediction(F.col("delay"))).withColumn(
    "actual_flag", (F.col("probability") > CLASSIFICATION_THRESHOLD).cast("int")).filter(
    ~F.col("actual_delay").eqNullSafe(F.col("expected_delay"))
    | ~F.col("actual_flag").eqNullSafe(F.col("expected_flag"))).count() == 0


# COMMAND ----------

# MAGIC %md
# MAGIC ## Batch Classification Scoring
# MAGIC Score the latest feed snapshot available before the deployment cutoff in the pinned source version, not a silently refreshed table. This is historical replay with overlap with refit data, not an out-of-sample benchmark; the registered pipeline's default prediction is ignored in favor of its probability and the frozen strict threshold.

# COMMAND ----------

audit_columns = ["feed_timestamp", "feed_datetime", "service_date", "entity_id", "trip_id", "vehicle_id",
    "route_id", "stop_id", "stop_sequence", "current_arrival_delay_seconds"]
optional_audit = [c for c in ["route_short_name", "stop_name", "current_status", "active_alert_flag",
    "vehicle_position_available_flag"] if c in model_df.columns]
require_columns(model_df, audit_columns)
# Project away ALL label/provenance columns before selecting scoring observations or predicting.
observable_columns = list(dict.fromkeys(audit_columns + optional_audit + feature_columns))
observations = model_df.select(*observable_columns).filter(F.col("feed_timestamp") < DEPLOYMENT_EPOCH)
latest_scoring_feed = observations.agg(F.max("feed_timestamp").alias("feed")).first()["feed"]
if latest_scoring_feed is None: raise ValueError("No eligible scoring observations")
scoring_input = observations.filter(F.col("feed_timestamp") == latest_scoring_feed)
expected_scoring_rows = scoring_input.count()
if expected_scoring_rows == 0: raise ValueError("Empty scoring batch")
duplicate_check(scoring_input)
invalid_scoring_numeric = scoring_input.agg(*numeric_checks).first().asDict()
if any(invalid_scoring_numeric.values()): raise ValueError(f"Invalid numeric scoring inputs: {invalid_scoring_numeric}")

def is_future_column(name):
    return name in {REG_LABEL, CLS_LABEL, "absolute_provisional_target_error_seconds", "future_observed_delay_seconds", "source_delta_version"} or name.startswith(("label_", "labeling_", "target_", "provisional_")) or name.endswith("_audit")
assert not any(is_future_column(c) for c in scoring_input.columns)
# Pipeline selects only allowlisted fields for assembly; identifiers pass through solely for output audit.
def score_observations(df, pipeline):
    require_columns(df, feature_columns + audit_columns)
    if any(is_future_column(c) for c in df.columns): raise ValueError("Future/label columns supplied to scoring")
    transformed = pipeline.transform(df)
    return (transformed.withColumn("major_delay_probability", vector_to_array("probability")[1])
        .withColumn("predicted_major_delay_flag", (F.col("major_delay_probability") > F.lit(CLASSIFICATION_THRESHOLD)).cast("int"))
        .withColumn("predicted_next_stop_delay_seconds", persistence_prediction(F.col("current_arrival_delay_seconds"))))

batch_predictions = score_observations(scoring_input, final_pipeline)
print("Scoring snapshot:", latest_scoring_feed, "expected rows:", expected_scoring_rows)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Scoring Output
# MAGIC Retain observation identifiers, selected observable context, predictions and batch/model provenance only. Registered name/version are populated only for this fitted model's registration attempt; a preserved older metadata row is never used to label new predictions.

# COMMAND ----------

scored_at = datetime.now(timezone.utc)
output = batch_predictions.select(*(audit_columns + optional_audit),
    "predicted_next_stop_delay_seconds", "major_delay_probability", "predicted_major_delay_flag")
constants = {"regression_scoring_method": REGRESSION_METHOD, "regression_fallback_seconds": PERSISTENCE_FALLBACK,
    "classification_threshold": CLASSIFICATION_THRESHOLD, "model_name": MODEL_NAME,
    "model_algorithm": "RandomForestClassifier", "mlflow_run_id": MLFLOW_RUN_ID,
    "registered_model_name": registered_model_name, "registered_model_version": registered_model_version,
    "registration_status": registration_status, "source_table": SOURCE_TABLE, "source_version": int(SOURCE_VERSION),
    "feature_contract_hash": feature_contract_hash, "scoring_mode": "historical_latest_snapshot_replay",
    "deployment_cutoff": DEPLOYMENT_CUTOFF, "scored_at": scored_at}
for name, value in constants.items():
    expression = F.lit(value)
    if name in ["registered_model_name", "registered_model_version"]: expression = expression.cast("string")
    if name == "source_version": expression = expression.cast("long")
    output = output.withColumn(name, expression)
assert not any(is_future_column(c) for c in output.columns)
assert not set(["features", "scaled_features", "probability", "rawPrediction", "prediction"]) & set(output.columns)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Scoring Quality Validation
# MAGIC Reject missing/nonfinite outputs, probabilities outside [0,1], invalid flags, threshold/rule mismatches or duplicate observation keys before writing. Report distribution summaries and predicted major-delay prevalence without comparing predictions to historical target labels.

# COMMAND ----------

def scoring_quality(df):
    assert not any(is_future_column(c) for c in df.columns)
    p = F.col("major_delay_probability")
    delay = F.col("predicted_next_stop_delay_seconds")
    flag = F.col("predicted_major_delay_flag")
    invalid_key = F.lit(False)
    for c in OBS_KEY: invalid_key = invalid_key | F.col(c).isNull()
    return df.agg(F.count("*").alias("total_scored_rows"),
        n_where(delay.isNull()).alias("null_regression_predictions"),
        n_where(p.isNull()).alias("null_probabilities"), n_where(flag.isNull()).alias("null_flags"),
        n_where(F.isnan(delay) | (F.abs(delay) == float("inf"))).alias("nonfinite_regression_predictions"),
        n_where(F.isnan(p) | ~p.between(0.0, 1.0)).alias("invalid_probability_range"),
        n_where(~flag.isin(0, 1)).alias("invalid_flags"), n_where(invalid_key).alias("null_grain_keys"),
        n_where(~F.col("classification_threshold").eqNullSafe(F.lit(CLASSIFICATION_THRESHOLD))).alias("invalid_threshold"),
        n_where(~flag.eqNullSafe((p > CLASSIFICATION_THRESHOLD).cast("int"))).alias("threshold_rule_mismatches"),
        n_where(~delay.eqNullSafe(persistence_prediction(F.col("current_arrival_delay_seconds")))).alias("persistence_rule_mismatches"),
        n_where(~F.col("mlflow_run_id").eqNullSafe(F.lit(MLFLOW_RUN_ID))
            | ~F.col("source_version").eqNullSafe(F.lit(SOURCE_VERSION))
            | ~F.col("registration_status").eqNullSafe(F.lit(registration_status))).alias("provenance_mismatches"),
        (100 * F.avg(flag)).alias("predicted_major_delay_rate_pct"),
        F.avg(p).alias("probability_mean"), F.percentile_approx(p, [0.5, 0.9, 0.95, 0.99], 10000).alias("probability_median_p90_p95_p99"),
        F.avg(delay).alias("regression_prediction_mean"),
        F.percentile_approx(delay, [0.5, 0.9, 0.95, 0.99], 10000).alias("regression_prediction_median_p90_p95_p99")
    ).first().asDict()

quality = scoring_quality(output)
error_checks = ["null_regression_predictions", "null_probabilities", "null_flags", "nonfinite_regression_predictions",
    "invalid_probability_range", "invalid_flags", "null_grain_keys", "invalid_threshold", "threshold_rule_mismatches",
    "persistence_rule_mismatches", "provenance_mismatches"]
assert quality["total_scored_rows"] == expected_scoring_rows
assert all(quality[k] == 0 for k in error_checks), quality
duplicate_grain_count = duplicate_check(output)
assert_frozen()
print("Scoring quality:", json.dumps(quality, indent=2))
print("duplicate_grain_count:", duplicate_grain_count, "serving_label_leakage_check: PASSED")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Persist Scoring Results
# MAGIC Overwrite only the ML serving Delta table with schema replacement after successful quality checks. Re-read the committed output and verify its row count, rules, provenance and grain; no source feature table or Gold table is written.

# COMMAND ----------

assert PREDICTIONS_TABLE == "rome_transport.ml.next_stop_delay_predictions"
assert PREDICTIONS_TABLE != SOURCE_TABLE
output.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(PREDICTIONS_TABLE)
predictions_delta_version = int(spark.sql(f"DESCRIBE HISTORY {PREDICTIONS_TABLE} LIMIT 1").select("version").first()[0])
saved_predictions = spark.read.option("versionAsOf", predictions_delta_version).table(PREDICTIONS_TABLE)
saved_quality = scoring_quality(saved_predictions)
assert saved_quality["total_scored_rows"] == expected_scoring_rows
assert all(saved_quality[k] == 0 for k in error_checks), saved_quality
assert duplicate_check(saved_predictions) == 0
assert set(saved_predictions.columns) == set(output.columns)
assert not any(is_future_column(c) for c in saved_predictions.columns)
client.log_param(MLFLOW_RUN_ID, "predictions_table", PREDICTIONS_TABLE)
client.log_param(MLFLOW_RUN_ID, "predictions_delta_version", predictions_delta_version)
client.log_param(MLFLOW_RUN_ID, "scoring_feed_timestamp", latest_scoring_feed)
client.log_metric(MLFLOW_RUN_ID, "total_scored_rows", saved_quality["total_scored_rows"])
client.log_metric(MLFLOW_RUN_ID, "predicted_major_delay_rate_pct", saved_quality["predicted_major_delay_rate_pct"])
client.set_tag(MLFLOW_RUN_ID, "batch_scoring_status", "persisted_and_validated")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Sample
# MAGIC Display 20 deterministic sample rows from the committed serving table. The sample contains observed context, model predictions and registration provenance, with no future labels.

# COMMAND ----------

sample_columns = ["route_id", "trip_id", "stop_id", "feed_datetime", "current_arrival_delay_seconds",
    "predicted_next_stop_delay_seconds", "major_delay_probability", "predicted_major_delay_flag"]
if "active_alert_flag" in saved_predictions.columns: sample_columns.append("active_alert_flag")
sample_columns.append("registration_status")
saved_predictions.orderBy(*OBS_KEY).select(*sample_columns).show(20, truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Validation
# MAGIC Report the current model attempt, fixed regression rule, committed scoring quality and registry metadata state separately. A deferred attempt never inherits the model version of a previously registered model.

# COMMAND ----------

assert_frozen()
assert not set(feature_columns) & set(forbidden_columns)
assert all(fitted_rf.getOrDefault(k) == v for k, v in FINAL_PARAMS.items())
stored_metadata = spark.table(METADATA_TABLE).filter((F.col("model_name") == MODEL_NAME)
    & (F.col("task") == "classification")).first().asDict()
assert json.loads(stored_metadata["last_attempt_json"])["mlflow_run_id"] == MLFLOW_RUN_ID
if registration_status == "REGISTERED":
    assert registered_model_name == REGISTERED_MODEL_NAME and registered_model_version is not None
    assert stored_metadata["mlflow_run_id"] == MLFLOW_RUN_ID
if registration_status == "DEFERRED_SERVERLESS_LIMITATION":
    assert registered_model_version is None and registration_reason
summary = dict(
    final_classifier=dict(algorithm="RandomForestClassifier", params=FINAL_PARAMS, threshold=CLASSIFICATION_THRESHOLD,
        threshold_semantics="strict_greater_than", mlflow_run_id=MLFLOW_RUN_ID,
        registration_status=registration_status, registered_model_name=registered_model_name,
        registered_model_version=registered_model_version, artifact_uri=artifact_uri),
    regression=dict(method=REGRESSION_METHOD, fallback=PERSISTENCE_FALLBACK, registered=False),
    scoring=dict(table=PREDICTIONS_TABLE, delta_version=predictions_delta_version, source_table=SOURCE_TABLE,
        source_version=SOURCE_VERSION, feed_timestamp=latest_scoring_feed, quality=saved_quality,
        duplicate_grain_count=0, serving_leakage_check="PASSED", mode="historical_latest_snapshot_replay"),
    registration=dict(current_attempt=registration_status, reason=registration_reason,
        metadata_table_status=stored_metadata["registration_status"], metadata_model_run=stored_metadata["mlflow_run_id"]))
print(json.dumps(summary, indent=2, default=str))
client.set_terminated(MLFLOW_RUN_ID, status="FINISHED")
print("No new model selection, source-table updates, Gold tables or dashboards were performed.")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Limitations
# MAGIC The batch replays the latest eligible snapshot within the pinned labeled source; it does not claim real-time coverage, an unbiased production sample or new holdout performance. Full-history preprocessing/refitting changes fitted statistics and may change probability calibration, but the supplied threshold remains frozen.
# MAGIC
# MAGIC If registration is deferred, metadata alone cannot reproduce fitted tree weights: reuse the live pipeline for this run or refit in a supported environment using the recorded contract/version. Registry consumers must load the Spark pipeline and apply `score_observations` with the recorded threshold; its default RF `prediction`/pyfunc output is not the final 0.45 decision rule.
# MAGIC
# MAGIC Serverless logical DataFrames can recompute lineage because caching and RDD APIs are avoided; persisted predictions provide a stable downstream boundary. Unexpected failures stop execution and may leave earlier metadata/model artifacts, so completed predictions are distinguished by the persisted-and-validated run tag.