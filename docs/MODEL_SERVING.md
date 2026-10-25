# Model runtime: external operational target and historical Spark reference

**Operational ML: EXTERNAL PYTHON BACKEND TARGET.** FastAPI/Python/scikit-learn will own training, evaluation, complete fitted artifacts, loading and inference in the future `rome-mobility-intelligence` repository. No backend or model is implemented by this handoff PR. [ADR-001](ARCHITECTURE_DECISIONS.md), [data/access contract](OPERATIONAL_ML_HANDOFF.md), [roadmap](ROADMAP.md).

**Spark ML: HISTORICAL / EXPERIMENTAL ML - RESEARCH / REFERENCE IMPLEMENTATION.** Notebook 11's frozen selection, historical notebook 12 and PR4 implementation/tests remain preserved. The technical sections below describe that historical/reference design only; they are not the production rollout plan. PR4 production fit, artifact persistence, native reload and scoring never ran. No prediction history, Gold, ingestion or Jobs changed.

## Verified runtime decision, 25 October 2026

Explicit `mlflow.set_tracking_uri("databricks")` and `mlflow.set_registry_uri("databricks-uc")` enabled experiment/run/parameter logging. Isolated Volume storage access passed. The fresh serialization smoke then failed at `VectorAssembler` construction with `Py4JSecurityException` on the Spark Connect constructor whitelist, before minimal fit or artifact creation. No production fit followed.

One authorized classic 16.4 LTS ML CPU Dedicated compute request was rejected because the organization has no associated worker environments. No cluster was created or left active. The target therefore no longer depends on Spark persistence, paid classic compute or UC registration. Future sklearn algorithm/preprocessing/threshold choices require new evaluation; historical Spark settings are not a conversion specification.

`historical_model_threshold = 0.45` (strict >); **future operational model threshold: to be validated from new model evaluation**. Historical TEST metrics below belong only to Spark/Persistence. Training v12 is validated; v8 remains the historical reference and is no longer readable under retention/VACUUM. No automatic version fallback or relabeling is authorized.

## Audit and architecture

Historical run `a9b568f478344d9dba90436358f14092` has status `DEFERRED_SERVERLESS_LIMITATION`, no artifact URI and an encoded size of 475. A read-only metadata check confirmed the recorded MLflow error: an existing UC Volume path was required for SparkML save/log/load on shared/serverless compute. The old notebook attempted artifact logging and registration together, deferred the known failure and continued using the in-memory fitted pipeline. It therefore provided no independently reloadable artifact. Changing the path did not establish native persistence: the subsequent smoke stopped at Spark ML construction. This path is now reference-only.

The preserved inference diagram is:

```text
ordered raw inputs: 42 numerical + 5 categorical
  -> fitted SQL projection / ImputerModel
  -> fitted StringIndexerModel / OneHotEncoderModel
  -> VectorAssembler / fitted StandardScalerModel
  -> fitted RandomForestClassificationModel (uses unscaled features)
  -> native probability vector, verified direct class order [0, 1]
  -> P(class 1) -> strict probability > 0.45
```

The scaler is retained because it was present in the historical preprocessing, even though RF uses `features`, not `scaled_features`. No regression estimator is fitted or persisted by PR4.

## Files and responsibilities

- `12_common_persisted_classifier`: definitions for preprocessing, artifact metadata validation, native loading and scoring; no runtime actions on import. Load it after `08_common_feature_preparation` using separate relative `%run` cells.
- `12a_fit_persist_final_classifier`: explicit historically eligible deployment refit, mandatory complete artifact persistence, native reload/parity and new in-sample diagnostics. Optional UC registration follows successful artifact validation.
- `12b_score_current_input`: pins `features.next_stop_delay_scoring`, loads an existing artifact and calls only transform. No labeled-source read, fit, MLflow logging, registration or output table write.

The original notebooks 09–12 and PR3 staging preparation logic are unchanged. There is no renumbering of `13_gold_analytics`.

## Immutable model/feature contract

The 47 raw features remain in their frozen order, with 42 numerical and 5 categorical inputs: route_id, route_type, direction_id, agency_id and current_status. Hash:

`11682b3cc1ab5c1d8440af4f6294e932e815126c3dd86ab609f4d5f8f35c41a5`

The event is `next_stop_delay_ge_300s`; positive label is realized next-stop GTFS-RT proxy delay **>=300 seconds**. The business flag uses **strict p>0.45**. Exactly 0.45 remains negative; the RF default prediction is not used for this flag.

Frozen RandomForestClassifier parameters: numTrees=100, maxDepth=10, minInstancesPerNode=5, featureSubsetStrategy=sqrt, maxBins=32, seed=42. Native class ordering is established by requiring exactly direct numeric labels [0,1], checking a binary RF and excluding any label-indexing stage. Metadata records this order and the scorer derives its positive index from it. Unknown/reordered class metadata is rejected.

Preprocessing is unchanged: raw numerical DOUBLE casts, median imputation with relativeError=0.001, categorical STRING casts, alphabetical StringIndexer with handleInvalid=keep, OneHotEncoder with handleInvalid=keep/dropLast=False, assembler handleInvalid=error, scaler withStd=True/withMean=False. Raw feature NULLs continue to the fitted preprocessing; no new imputation is introduced at inference.

Persisted indexer vocabularies and encoder category sizes handle new route/agency/status values and NULL categories through existing keep buckets. The loader verifies these settings and the complete fitted-stage sequence. The assembled dimension must agree with the fitted category sizes and RF numFeatures. Historical 475 is recorded as a reference, not a hard requirement: a new fit can observe different category levels. Any change is tagged and must be reviewed during rollout. No labels/statistics are refitted to accommodate a new scoring batch.

## Artifact format, metadata and loading

Use [MLflow's native Spark flavor](https://mlflow.org/docs/latest/api_reference/python_api/mlflow.spark.html) to log the complete Spark PipelineModel. The classifier alone is insufficient. The log call embeds the `rome_transport_classifier` metadata in the artifact's MLmodel file; no pickle, manual preprocessing serialization or custom pyfunc is introduced. MLflow may include its standard auxiliary flavors, but this implementation loads with `mlflow.spark.load_model` exclusively.

The metadata includes an artifact-schema version, model_run_id, the exact existing feature/preprocessing contract and ordered raw features, algorithm/hyperparameters, event/target threshold, strict decision rule, class order, pinned training source/version, deployment cutoff, training timestamp, exact code Git SHA, actual encoded dimension and fitted category sizes. Learned imputation values and categorical mappings reside in the native fitted stages rather than duplicated JSON vocabularies.

`MODEL_URI` is required and has no workspace-specific hardcoded value. Use the URI returned by successful logging, for example `runs:/<new_run_id>/classifier_pipeline`, an immutable logged-model ID or an exact numbered model version. Mutable registry aliases/stages are rejected to avoid resolving different artifacts between metadata inspection and native load. Scoring never depends on UC registration. MLflow origin run ID, metadata, fitted stages, parameters and dimensions are validated before inference.

`MODEL_DFS_TMPDIR` can point to an existing authorized `/Volumes/...` location if required by shared/serverless MLflow Spark. The value is passed to both log and native load, including the immediate persistence round-trip. Nothing creates a volume, changes permissions or silently switches compute/storage. A compatible runtime and writable location must be validated separately. Logging/loading failures propagate; there is no in-memory scoring fallback and no success claim without a reloaded artifact. Registry registration is off by default and best effort after mandatory artifact validation; its failure cannot hide artifact failures.

## Fit population and metrics

The fit path preserves historical notebook 12's deployment-refit population: a pinned labeled Delta version, observations and future-label source epochs strictly before a captured deployment cutoff, valid direct 0/1 labels derived from >=300 seconds, horizon 1–1800 seconds, and the existing ±43,200-second label-quality rule. Both classes and nonduplicate keys are required. All frozen numerical columns must have usable training values; none are silently dropped. There is no random split, new hyperparameter search, threshold tuning or feature selection.

Default training source version is the original v8. If retained files are unavailable, stop: unlike old 12, no automatic substitution to latest is allowed. A later rollout may explicitly authorize another pinned version only after checking that the population/label contract is suitable. Features are never inferred from optional-column availability in a newer dataset.

This is a deployment refit on all historically eligible labels, matching old 12, rather than a new 70/15/15 selection exercise. The chronological split/purging and historical TEST evidence in 10/11 remain untouched. The new run computes its own `new_fit_in_sample_*` accuracy, precision, recall, F1, ROC-AUC and PR-AUC after reload. These are training-population diagnostics and cannot establish independent generalization. No new holdout performance is claimed; genuinely future evaluation remains separate.

Historical reference TEST results remain:

| Task | Historical metric | Value |
| --- | --- | ---: |
| Persistence regression | MAE seconds | 136.9818 |
| Persistence regression | RMSE seconds | 401.8525 |
| Persistence regression | R² | 0.6940 |
| Persistence regression | WAPE | 0.3293 |
| Classifier | Accuracy | 0.9401 |
| Classifier | Precision | 0.8450 |
| Classifier | Recall | 0.8123 |
| Classifier | F1 | 0.8283 |
| Classifier | ROC-AUC | 0.9690 |
| Classifier | PR-AUC | 0.9063 |

These values are not logged as achieved metrics of the new fit. Equal settings and seed do not prove equal learned parameters, category vocabulary, vector size or predictions; those require runtime comparison.

## Scoring input/output and gates

The scoring path reads a single pinned staging version, verifies the exact raw feature order/types/hash, one preparation lineage and one feed epoch, and rejects label/outcome/prediction columns. Only documented next-stop identity fields may use the target_ prefix. Identity/service/vehicle fields pass through for output and are not assembled into the model vector. The output DataFrame contains the observation key, next-stop IDs, vehicle/service/start-time identity, P(class 1), strict flag, model_run_id, feature_contract_hash, decision_threshold, model_algorithm and scored_at, retaining the existing staging preparation lineage as well. Row count and identity multisets must be preserved, probabilities finite/in-range/summing to one, and vector size unchanged from the loaded artifact.

All gates default OFF: ALLOW_MODEL_FIT, ALLOW_MODEL_PERSIST, ALLOW_UC_REGISTRATION, ALLOW_MODEL_INFERENCE and ALLOW_SCORING_WRITE. Fit requires both fit and persistence gates plus a pinned source and exact code commit before it begins. Existing active MLflow runs are rejected, preventing accidental writes into the historical run. Scoring has no writer even if its write flag is changed. The former Spark PR5 plan is superseded: permanent prediction history is roadmap step F after external model validation. The existing next_stop_delay_predictions table is not overwritten.

**Latest scoring source is historical because Silver is currently stale.** PR3 staging was validated at Silver v8 / feed epoch 1790928849 (2 October 2026), with several optional inputs/categories about 75–79% NULL. The scorer reports feed age, does not manufacture freshness and does not call this live prediction. No new imputation or model-quality claim is justified by the input contract alone.

## Local validation of the preserved reference implementation

Offline tests use fake fitted stages/pipelines and execute actual loading/scoring definitions. They cover contract/hash/parameters, direct class order, strict threshold, identity preservation, new/NULL categories, storage propagation into load, mandatory persistence/reload, default gates, and no inference fit/labeled read/table writes. These are contract tests, not proof that Spark persistence or categorical vectors work on the target runtime.

The former Spark rollout plan required a serialization smoke before any full fit,
then complete artifact persistence, native reload/parity, unknown/NULL category tests
and independent inference, with UC registration optional. The actual attempt stopped
at the first requirement. Its original implementation and prior documentation remain
in Git history; this superseded plan is not an instruction to resume Spark deployment.

No production PR4 artifact has been produced or loaded. Spark category encodings and round-trip parity remain unverified on this workspace; no success is inferred from offline tests. Future production acceptance belongs to the external backend and the [new roadmap](ROADMAP.md), including newly evaluated thresholds and independently reloaded sklearn artifacts.
