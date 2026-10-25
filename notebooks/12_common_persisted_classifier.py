# Databricks notebook source
# MAGIC %md
# MAGIC # Native persisted classifier contract
# MAGIC Definitions only; load after 08_common_feature_preparation. No runtime actions on import.

# COMMAND ----------

import copy
import math
from datetime import datetime, timezone

FINAL_CLASSIFIER_PARAMS = dict(numTrees=100, maxDepth=10, minInstancesPerNode=5,
    featureSubsetStrategy="sqrt", maxBins=32, seed=42)
TARGET_EVENT = "next_stop_delay_ge_300s"
TARGET_SECONDS = 300
DECISION_THRESHOLD = 0.45
CLASSIFIER_LABEL = "target_realized_major_delay_flag"
REGRESSION_LABEL = "target_realized_next_stop_delay_seconds"
TRAINING_TABLE = "rome_transport.features.next_stop_delay_labeled"
STAGING_TABLE = "rome_transport.features.next_stop_delay_scoring"
ARTIFACT_METADATA_KEY = "rome_transport_classifier"
ARTIFACT_SCHEMA_VERSION = 1
IDENTITY_COLUMNS = ["feed_timestamp", "entity_id", "trip_id", "stop_sequence", "stop_id",
    "target_next_stop_id", "target_next_stop_sequence", "vehicle_id", "service_date", "start_time"]
OUTPUT_METADATA_COLUMNS = ["model_run_id", "feature_contract_hash", "decision_threshold", "model_algorithm", "scored_at"]
OUTPUT_COLUMNS = list(dict.fromkeys(IDENTITY_COLUMNS + ["major_delay_probability", "predicted_major_delay_flag"]
    + SCORING_LINEAGE_COLUMNS + OUTPUT_METADATA_COLUMNS))


def preprocessing_layout():
    numeric_raw = [f"__numeric_raw_{i}" for i in range(len(NUMERICAL_FEATURES))]
    numeric_imputed = [f"__numeric_imputed_{i}" for i in range(len(NUMERICAL_FEATURES))]
    category_raw = [f"__category_raw_{i}" for i in range(len(CATEGORICAL_FEATURES))]
    category_indices = [f"__category_index_{i}" for i in range(len(CATEGORICAL_FEATURES))]
    category_vectors = [f"__category_vector_{i}" for i in range(len(CATEGORICAL_FEATURES))]
    projection = [f"CAST(`{name}` AS DOUBLE) AS `{alias}`" for name, alias in zip(NUMERICAL_FEATURES, numeric_raw)]
    projection += [f"CAST(`{name}` AS STRING) AS `{alias}`" for name, alias in zip(CATEGORICAL_FEATURES, category_raw)]
    return dict(numeric_raw=numeric_raw, numeric_imputed=numeric_imputed, category_raw=category_raw,
        category_indices=category_indices, category_vectors=category_vectors,
        statement="SELECT *, " + ", ".join(projection) + " FROM __THIS__")


def build_preprocessing_pipeline():
    # Matches historical 11/12. RF consumes features, not the persisted scaled_features.
    from pyspark.ml import Pipeline
    from pyspark.ml.feature import SQLTransformer, Imputer, StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler
    x = preprocessing_layout()
    return Pipeline(stages=[
        SQLTransformer(statement=x["statement"]),
        Imputer(inputCols=x["numeric_raw"], outputCols=x["numeric_imputed"], strategy="median", relativeError=0.001),
        StringIndexer(inputCols=x["category_raw"], outputCols=x["category_indices"], handleInvalid="keep", stringOrderType="alphabetAsc"),
        OneHotEncoder(inputCols=x["category_indices"], outputCols=x["category_vectors"], handleInvalid="keep", dropLast=False),
        VectorAssembler(inputCols=x["numeric_imputed"] + x["category_vectors"], outputCol="features", handleInvalid="error"),
        StandardScaler(inputCol="features", outputCol="scaled_features", withStd=True, withMean=False),
    ])


def class_one_index(labels):
    # Native Spark RF uses integer class indices. No label StringIndexer is fitted.
    if labels != [0, 1]:
        raise ValueError("Expected direct binary Spark labels [0, 1]; positive-class ordering unverified")
    return labels.index(1)


def probability_and_flag(probabilities, labels):
    index = class_one_index(labels)
    if len(probabilities) != 2 or any(v is None or not math.isfinite(v) or not 0 <= v <= 1 for v in probabilities):
        raise ValueError("Invalid binary probabilities")
    if abs(sum(probabilities) - 1.0) > 1e-8:
        raise ValueError("Binary probabilities do not sum to one")
    p = probabilities[index]
    return p, int(p > DECISION_THRESHOLD)


def make_artifact_metadata(run_id, source_version, cutoff, training_timestamp, git_commit, vector_size, category_sizes):
    if not run_id or not isinstance(source_version, int) or source_version < 0:
        raise ValueError("Pinned training version and model run ID are required")
    if len(git_commit) != 40 or any(c not in "0123456789abcdef" for c in git_commit.lower()):
        raise ValueError("Explicit 40-character code commit required")
    return dict(artifact_schema_version=ARTIFACT_SCHEMA_VERSION, model_run_id=run_id,
        feature_contract_hash=verify_feature_contract(), feature_contract=copy.deepcopy(MODEL_CONTRACT),
        raw_features=list(MODEL_FEATURES), algorithm="RandomForestClassifier", hyperparameters=dict(FINAL_CLASSIFIER_PARAMS),
        target_event_id=TARGET_EVENT, target_threshold_seconds=TARGET_SECONDS,
        decision_threshold=DECISION_THRESHOLD, decision_operator=">", class_labels=[0, 1],
        training_source_table=TRAINING_TABLE, training_source_version=source_version,
        training_population="all_historically_eligible_labels_before_cutoff_no_random_resplit",
        deployment_cutoff_utc=cutoff, trained_at_utc=training_timestamp, code_git_commit=git_commit,
        encoded_vector_size=vector_size, categorical_sizes=list(category_sizes),
        historical_encoded_vector_size_reference=475, metric_scope="new_fit_in_sample_not_historical_test")


def validate_artifact_metadata(metadata):
    expected = dict(artifact_schema_version=ARTIFACT_SCHEMA_VERSION, feature_contract_hash=verify_feature_contract(),
        feature_contract=MODEL_CONTRACT, raw_features=MODEL_FEATURES, algorithm="RandomForestClassifier",
        hyperparameters=FINAL_CLASSIFIER_PARAMS, target_event_id=TARGET_EVENT, target_threshold_seconds=300,
        decision_threshold=0.45, decision_operator=">", training_source_table=TRAINING_TABLE,
        training_population="all_historically_eligible_labels_before_cutoff_no_random_resplit",
        metric_scope="new_fit_in_sample_not_historical_test")
    for key, value in expected.items():
        if metadata.get(key) != value:
            raise ValueError("Artifact metadata mismatch: " + key)
    class_one_index(metadata.get("class_labels"))
    for key in ["model_run_id", "deployment_cutoff_utc", "trained_at_utc", "code_git_commit"]:
        if not metadata.get(key): raise ValueError("Missing artifact metadata: " + key)
    commit = metadata["code_git_commit"]
    if not isinstance(commit,str) or len(commit)!=40 or any(c not in "0123456789abcdef" for c in commit.lower()):
        raise ValueError("Invalid artifact code commit")
    for key in ["deployment_cutoff_utc", "trained_at_utc"]:
        instant = datetime.fromisoformat(metadata[key])
        if instant.tzinfo is None or instant.utcoffset().total_seconds()!=0:
            raise ValueError("Artifact timestamp must be UTC: " + key)
    size = metadata.get("encoded_vector_size")
    categories = metadata.get("categorical_sizes", [])
    if not isinstance(size, int) or size <= 0 or len(categories) != 5 or any(not isinstance(n, int) or n < 1 for n in categories):
        raise ValueError("Invalid fitted vector/category dimensions")
    if size != 42 + sum(n + 1 for n in categories):
        raise ValueError("Encoded dimension disagrees with fitted keep/dropLast=False categories")
    version = metadata.get("training_source_version")
    if not isinstance(version, int) or version < 0:
        raise ValueError("Unpinned training source")
    return metadata


def validate_fitted_pipeline(pipeline, metadata):
    validate_artifact_metadata(metadata)
    stages = pipeline.stages
    names = [s.__class__.__name__ for s in stages]
    if names != ["SQLTransformer", "ImputerModel", "StringIndexerModel", "OneHotEncoderModel",
                 "VectorAssembler", "StandardScalerModel", "RandomForestClassificationModel"]:
        raise ValueError("Expected complete fitted native preprocessing + RF pipeline")
    x = preprocessing_layout()
    checks = [(stages[0], "statement", x["statement"]),
        (stages[1], "inputCols", x["numeric_raw"]), (stages[1], "outputCols", x["numeric_imputed"]),
        (stages[1], "strategy", "median"), (stages[1], "relativeError", 0.001),
        (stages[2], "inputCols", x["category_raw"]), (stages[2], "outputCols", x["category_indices"]),
        (stages[2], "handleInvalid", "keep"), (stages[2], "stringOrderType", "alphabetAsc"),
        (stages[3], "inputCols", x["category_indices"]), (stages[3], "outputCols", x["category_vectors"]),
        (stages[3], "handleInvalid", "keep"), (stages[3], "dropLast", False),
        (stages[4], "inputCols", x["numeric_imputed"]+x["category_vectors"]),
        (stages[4], "handleInvalid", "error"), (stages[4], "outputCol", "features"),
        (stages[5], "inputCol", "features"), (stages[5], "outputCol", "scaled_features"),
        (stages[5], "withStd", True), (stages[5], "withMean", False),
        (stages[6], "featuresCol", "features"), (stages[6], "labelCol", CLASSIFIER_LABEL),
        (stages[6], "probabilityCol", "probability"), (stages[6], "predictionCol", "prediction"),
        (stages[6], "rawPredictionCol", "rawPrediction")]
    checks += [(stages[6], key, value) for key, value in FINAL_CLASSIFIER_PARAMS.items()]
    for stage, key, expected in checks:
        if stage.getOrDefault(key) != expected:
            raise ValueError("Fitted pipeline parameter mismatch: " + key)
    if not math.isnan(stages[1].getOrDefault("missingValue")):
        raise ValueError("Imputer missing-value semantics changed")
    if stages[-1].numClasses != 2 or stages[-1].numFeatures != metadata["encoded_vector_size"]:
        raise ValueError("RF class/vector dimension mismatch")
    if list(stages[3].categorySizes) != metadata["categorical_sizes"]:
        raise ValueError("One-hot fitted category dimensions mismatch")
    if [len(v)+1 for v in stages[2].labelsArray] != metadata["categorical_sizes"]:
        raise ValueError("Indexer vocabulary/unknown bucket mismatch")
    return pipeline


def load_classifier_artifact(model_uri, model_info_loader=None, pipeline_loader=None, dfs_tmpdir=None):
    if not isinstance(model_uri, str) or not model_uri.strip():
        raise ValueError("MODEL_URI must identify an already persisted MLflow Spark artifact")
    # Avoid aliases/stages that can move between metadata inspection and native load.
    if model_uri.startswith("models:/"):
        identifier = model_uri[len("models:/"):]
        if not (identifier.startswith("m-") and "/" not in identifier) and not ("/" in identifier and identifier.rsplit("/",1)[1].isdigit()):
            raise ValueError("MODEL_URI must pin a model version or immutable logged-model ID")
    if model_info_loader is None or pipeline_loader is None:
        import mlflow.models
        import mlflow.spark
        model_info_loader = model_info_loader or mlflow.models.get_model_info
        pipeline_loader = pipeline_loader or mlflow.spark.load_model
    info = model_info_loader(model_uri)
    metadata = (info.metadata or {}).get(ARTIFACT_METADATA_KEY)
    if not isinstance(metadata, dict):
        raise ValueError("Artifact lacks the persisted classifier metadata contract")
    validate_artifact_metadata(metadata)
    if getattr(info, "run_id", None) != metadata["model_run_id"]:
        raise ValueError("Artifact model_run_id does not match its MLflow origin")
    pipeline = pipeline_loader(model_uri, dfs_tmpdir=dfs_tmpdir) if dfs_tmpdir else pipeline_loader(model_uri)
    return validate_fitted_pipeline(pipeline, metadata), metadata


def persist_classifier_artifact(pipeline, metadata, mlflow_module, allow_persist=False, dfs_tmpdir=None):
    if not allow_persist: raise RuntimeError("Model artifact persistence is disabled")
    validate_fitted_pipeline(pipeline, metadata)
    active = mlflow_module.active_run()
    if active is None or active.info.run_id != metadata["model_run_id"]:
        raise ValueError("Active MLflow run differs from artifact lineage")
    from mlflow.models import ModelSignature
    from mlflow.types.schema import Schema, ColSpec
    signature = ModelSignature(inputs=Schema(
        [ColSpec("double", c, required=False) for c in NUMERICAL_FEATURES]
        + [ColSpec("string", c, required=False) for c in CATEGORICAL_FEATURES]),
        outputs=Schema([ColSpec("double", "prediction")]))
    kwargs = {"dfs_tmpdir": dfs_tmpdir} if dfs_tmpdir else {}
    info = mlflow_module.spark.log_model(pipeline, artifact_path="classifier_pipeline", signature=signature,
        metadata={ARTIFACT_METADATA_KEY: metadata}, **kwargs)
    # Persistence is not success until native load and contract validation succeed. No registry dependency.
    loaded, stored = load_classifier_artifact(info.model_uri, dfs_tmpdir=dfs_tmpdir)
    if stored != metadata: raise ValueError("Persisted metadata round-trip mismatch")
    return info.model_uri, loaded


def validate_scoring_input(df):
    required = set(SCORING_COLUMNS)
    if not required <= set(df.columns):
        raise ValueError("Missing staging identity/features/lineage: " + str(sorted(required-set(df.columns))))
    if [c for c in df.columns if c in MODEL_FEATURES] != MODEL_FEATURES:
        raise ValueError("Raw model feature order mismatch")
    allowed_target = {"target_next_stop_id", "target_next_stop_sequence", "target_feed_timestamp", "target_trip_id", "target_service_date"}
    for c in df.columns:
        if c.startswith(("label_", "labeling_", "provisional_", "future_", "predicted_")) or (c.startswith("target_") and c not in allowed_target) or c in {"prediction", "probability", "rawPrediction"}:
            raise ValueError("Outcome/prediction column supplied to inference: " + c)
    validate_scoring_rows(df)
    assert_no_rows(df, reduce(lambda a,b:a|b,[F.col(c).isNull() for c in SCORING_LINEAGE_COLUMNS]), "Missing staging lineage")
    if df.select(*SCORING_LINEAGE_COLUMNS).dropDuplicates().count() != 1:
        raise ValueError("Mixed staging preparation lineage")
    if df.select("feed_timestamp").dropDuplicates().count() != 1:
        raise ValueError("Scoring staging must contain one feed snapshot")
    if not df.limit(1).count(): raise ValueError("Empty scoring staging")


def score_current_input(df, pipeline, metadata, scored_at):
    from pyspark.ml.functions import vector_to_array
    validate_fitted_pipeline(pipeline, metadata)
    validate_scoring_input(df)
    # Identifiers/lineage pass through; only the fitted SQLTransformer selects raw columns for assembly.
    selected = df.select(*SCORING_COLUMNS)
    transformed = pipeline.transform(selected)
    probabilities = vector_to_array("probability")
    invalid = F.col("probability").isNull() | (F.size(probabilities) != 2) | F.exists(
        probabilities, lambda p: p.isNull() | F.isnan(p) | ~p.between(0.0, 1.0))
    invalid = invalid | (F.abs(F.aggregate(probabilities, F.lit(0.0), lambda a,b:a+b)-1.0)>1e-8)
    assert_no_rows(transformed, invalid, "Invalid persisted-model probabilities")
    assert_no_rows(transformed, F.size(vector_to_array("features")) != metadata["encoded_vector_size"], "Inference vector size changed")
    result = (transformed.withColumn("major_delay_probability", probabilities[class_one_index(metadata["class_labels"])])
        .withColumn("predicted_major_delay_flag", (F.col("major_delay_probability") > DECISION_THRESHOLD).cast("int"))
        .withColumn("model_run_id", F.lit(metadata["model_run_id"]))
        .withColumn("decision_threshold", F.lit(DECISION_THRESHOLD))
        .withColumn("model_algorithm", F.lit(metadata["algorithm"]))
        .withColumn("scored_at", F.lit(scored_at).cast("timestamp"))
        .select(*OUTPUT_COLUMNS))
    if result.count() != df.count(): raise ValueError("Inference changed observation row count")
    keys = OBS_KEY + ["target_next_stop_id", "target_next_stop_sequence", "vehicle_id", "service_date", "start_time"]
    if df.select(*keys).exceptAll(result.select(*keys)).limit(1).count() or result.select(*keys).exceptAll(df.select(*keys)).limit(1).count():
        raise ValueError("Inference changed observation/next-stop identity")
    return result
