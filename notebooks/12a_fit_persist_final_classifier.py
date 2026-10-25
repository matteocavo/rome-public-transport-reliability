# Databricks notebook source
# MAGIC %md
# MAGIC # Explicit final classifier fit and mandatory native artifact persistence
# MAGIC Repository-first PR4. Do not run without separate runtime authorization and compatible Spark ML persistence.
# MAGIC This is the historical deployment refit population, not a new holdout evaluation or tuning exercise.

# COMMAND ----------

# MAGIC %run ./08_common_feature_preparation

# COMMAND ----------

# MAGIC %run ./12_common_persisted_classifier

# COMMAND ----------

ALLOW_MODEL_FIT = False
ALLOW_MODEL_PERSIST = False
ALLOW_UC_REGISTRATION = False
TRAINING_SOURCE_VERSION = 8  # Explicit retained labeled version; no automatic substitution.
CODE_GIT_COMMIT = None  # Exact deployed code SHA, supplied at separately authorized rollout.
DEPLOYMENT_CUTOFF_UTC = None  # Optional timezone-aware datetime; captured once if unset.
MODEL_DFS_TMPDIR = None  # Optional existing writable storage path supported by MLflow/Spark.
EXPERIMENT_NAME = "/Shared/rome_transport_next_stop_delay"
UC_MODEL_NAME = "rome_transport.ml.major_delay_classifier"

if not (ALLOW_MODEL_FIT and ALLOW_MODEL_PERSIST):
    raise RuntimeError("Both explicit fit and artifact persistence authorization are required")
if not isinstance(TRAINING_SOURCE_VERSION, int) or TRAINING_SOURCE_VERSION < 0:
    raise ValueError("Pin a retained training Delta version; no latest-data fallback")
if not isinstance(CODE_GIT_COMMIT, str) or len(CODE_GIT_COMMIT) != 40 or any(c not in "0123456789abcdef" for c in CODE_GIT_COMMIT.lower()):
    raise ValueError("Provide the exact deployed 40-character code commit before fitting")
cutoff = DEPLOYMENT_CUTOFF_UTC or datetime.now(timezone.utc)
if cutoff.tzinfo is None:
    raise ValueError("Deployment cutoff must be timezone-aware")
cutoff = cutoff.astimezone(timezone.utc)
cutoff_epoch = int(cutoff.timestamp())
verify_feature_contract()

# COMMAND ----------

from pyspark.ml import PipelineModel
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.functions import vector_to_array
import mlflow
import mlflow.spark

source = spark.read.option("versionAsOf", TRAINING_SOURCE_VERSION).table(TRAINING_TABLE)
required = MODEL_FEATURES + OBS_KEY + [CLASSIFIER_LABEL, REGRESSION_LABEL, "label_source_feed_timestamp", "label_horizon_seconds"]
if not set(required) <= set(source.columns): raise ValueError("Training source lacks frozen features/label provenance")
validate_model_schema({f.name:f.dataType.simpleString() for f in source.schema.fields})
assert_no_rows(source, F.col("feed_timestamp").isNull() | F.col("label_source_feed_timestamp").isNull(), "Missing eligibility timestamps")
training = source.filter((F.col("feed_timestamp") < cutoff_epoch) & (F.col("label_source_feed_timestamp") < cutoff_epoch))
assert_no_rows(training, F.col(CLASSIFIER_LABEL).isNull() | ~F.col(CLASSIFIER_LABEL).isin(0,1)
    | F.col(REGRESSION_LABEL).isNull() | F.isnan(F.col(REGRESSION_LABEL).cast("double"))
    | (F.abs(F.col(REGRESSION_LABEL)) > 43200)
    | ~F.col(CLASSIFIER_LABEL).eqNullSafe((F.col(REGRESSION_LABEL) >= TARGET_SECONDS).cast("int"))
    | (F.col("label_source_feed_timestamp") <= F.col("feed_timestamp"))
    | ~F.col("label_horizon_seconds").eqNullSafe(F.col("label_source_feed_timestamp")-F.col("feed_timestamp"))
    | ~F.col("label_horizon_seconds").between(1,1800), "Invalid historical labels or causality")
if training.groupBy(*OBS_KEY).count().filter(F.col("count")>1).limit(1).count(): raise ValueError("Duplicate training keys")
labels = sorted(r[0] for r in training.select(CLASSIFIER_LABEL).distinct().collect())
class_one_index(labels)
training_rows = training.count()
if not training_rows: raise ValueError("Empty eligible training population")
available = training.agg(*[F.count(F.when(~F.isnan(F.col(c).cast("double")),F.col(c))).alias(c) for c in NUMERICAL_FEATURES]).first().asDict()
if any(n==0 for n in available.values()): raise ValueError("Frozen numeric input entirely missing; do not silently drop it")
assert_no_rows(training, reduce(lambda a,b:a|b,[F.abs(F.col(c).cast("double"))==float("inf") for c in NUMERICAL_FEATURES]),"Infinite training feature")
if mlflow.active_run() is not None: raise RuntimeError("Finish the active MLflow run; never append to a historical run")
mlflow.autolog(disable=True)
mlflow.set_experiment(EXPERIMENT_NAME)

# COMMAND ----------

with mlflow.start_run(run_name="explicit_final_classifier_fit_and_persist", tags={
    "stage":"explicit_fit_and_persist", "metric_scope":"new_fit_in_sample_not_historical_test",
    "target_event_id":TARGET_EVENT, "training_population":"historical_eligible_deployment_refit"}) as run:
    # Same two fits as historical 12, now isolated from inference.
    preprocessor = build_preprocessing_pipeline().fit(training.select(*MODEL_FEATURES))
    vectors = preprocessor.transform(training.select(*MODEL_FEATURES,CLASSIFIER_LABEL))
    vector_size = int(vectors.schema["features"].metadata.get("ml_attr",{}).get("num_attrs",0))
    if vector_size <= 0: raise ValueError("Missing learned vector dimension")
    rf = RandomForestClassifier(labelCol=CLASSIFIER_LABEL,featuresCol="features",**FINAL_CLASSIFIER_PARAMS).fit(vectors.select("features",CLASSIFIER_LABEL))
    final = PipelineModel(stages=preprocessor.stages+[rf])
    metadata = make_artifact_metadata(run.info.run_id,TRAINING_SOURCE_VERSION,cutoff.isoformat(),
        datetime.now(timezone.utc).isoformat(),CODE_GIT_COMMIT,vector_size,list(preprocessor.stages[3].categorySizes))
    validate_fitted_pipeline(final,metadata)
    mlflow.log_params(FINAL_CLASSIFIER_PARAMS | {"feature_contract_hash":verify_feature_contract(),
        "training_source_table":TRAINING_TABLE,"training_source_version":TRAINING_SOURCE_VERSION,
        "training_rows":training_rows,"decision_threshold":DECISION_THRESHOLD,"decision_operator":">",
        "encoded_vector_size":vector_size,"historical_vector_size_reference":475})
    MODEL_URI,loaded = persist_classifier_artifact(final,metadata,mlflow,allow_persist=ALLOW_MODEL_PERSIST,dfs_tmpdir=MODEL_DFS_TMPDIR)
    # Real round-trip parity on deterministic raw rows; no target-dependent input assembly.
    probe = training.orderBy(*OBS_KEY).limit(64).select(*list(dict.fromkeys(OBS_KEY+MODEL_FEATURES)))
    def probabilities(model):
        return model.transform(probe).select(*OBS_KEY,vector_to_array("probability").alias("probabilities"))
    before,after = probabilities(final),probabilities(loaded)
    if before.exceptAll(after).limit(1).count() or after.exceptAll(before).limit(1).count():
        raise ValueError("Persisted model probability round-trip mismatch")
    # Own fit diagnostics: IN-SAMPLE ONLY. Historical TEST metrics remain in docs/11, not new-fit metrics.
    evaluated = loaded.transform(training.select(*MODEL_FEATURES,CLASSIFIER_LABEL))
    positive = vector_to_array("probability")[class_one_index(labels)]
    evaluated = evaluated.withColumn("major_delay_probability",positive)
    flag = F.col("major_delay_probability") > DECISION_THRESHOLD
    counts = evaluated.agg(
        F.count("*").alias("n"),
        F.sum(F.when(flag & (F.col(CLASSIFIER_LABEL)==1),1).otherwise(0)).alias("tp"),
        F.sum(F.when(flag & (F.col(CLASSIFIER_LABEL)==0),1).otherwise(0)).alias("fp"),
        F.sum(F.when(~flag & (F.col(CLASSIFIER_LABEL)==1),1).otherwise(0)).alias("fn"),
        F.sum(F.when(~flag & (F.col(CLASSIFIER_LABEL)==0),1).otherwise(0)).alias("tn")).first().asDict()
    precision = counts["tp"] / max(1,counts["tp"]+counts["fp"])
    recall = counts["tp"] / max(1,counts["tp"]+counts["fn"])
    metrics = dict(accuracy=(counts["tp"]+counts["tn"])/counts["n"],precision=precision,recall=recall,
        f1=2*precision*recall/(precision+recall) if precision+recall else 0.0)
    for name,metric in [("roc_auc","areaUnderROC"),("pr_auc","areaUnderPR")]:
        metrics[name]=BinaryClassificationEvaluator(labelCol=CLASSIFIER_LABEL,rawPredictionCol="major_delay_probability",metricName=metric).evaluate(evaluated)
    mlflow.log_metrics({"new_fit_in_sample_"+k:float(v) for k,v in metrics.items()})
    mlflow.set_tag("artifact_load_verified","true")
    mlflow.set_tag("vector_dimension_changed_from_reference",str(vector_size!=475).lower())
    if ALLOW_UC_REGISTRATION:
        try:
            mlflow.set_registry_uri("databricks-uc")
            registered=mlflow.register_model(MODEL_URI,UC_MODEL_NAME,await_registration_for=60)
            mlflow.set_tag("optional_registration_status",str(registered.status))
        except Exception as exc:
            # Only registration is optional. Artifact log/load/round-trip errors always propagate.
            mlflow.set_tag("optional_registration_status","FAILED_OR_UNSUPPORTED")
            mlflow.set_tag("optional_registration_reason",str(exc)[:2000])
    print({"model_uri":MODEL_URI,"model_run_id":run.info.run_id,"vector_size":vector_size,
        "vector_reference":475,"metrics_scope":"in_sample_only","metrics":metrics})
