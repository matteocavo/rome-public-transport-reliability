# Operational ML data and access handoff

Status: repository contract implemented; runtime data reads verified on 25 October 2026.
The backend, new model and orchestrators are not implemented here. Architecture:
[ADR-001](ARCHITECTURE_DECISIONS.md). Implementation order: [roadmap](ROADMAP.md).

## Machine-readable contracts

[`contracts/operational_ml.json`](../contracts/operational_ml.json) is the explicit
training/inference/serving handoff. The standard-library module
[`operational_ml.py`](../contracts/operational_ml.py) generates SELECT-only projections;
it has no network client, model, credentials, materialization or execution side effects.

Both exports preserve the exact ordered **47 raw features: 42 numerical + 5 categorical**.
Categoricals: `route_id`, `route_type`, `direction_id`, `agency_id`, `current_status`.
Names, order and raw types are checked against `08_common_feature_preparation` by tests.
The retained input-provenance SHA-256 is:

`11682b3cc1ab5c1d8440af4f6294e932e815126c3dd86ab609f4d5f8f35c41a5`

That hash includes historical Spark preprocessing. It does not oblige sklearn to reproduce
Spark's imputer/indexer/encoder or imply a matching learned vector. Optional feature NULLs
remain NULL; no export-time fitting, imputation or dropping of optional inputs occurs.
Future preprocessing is learned on training only, persisted with the estimator, and tested
on unknown/NULL categories after reload. Infinity, missing columns and schema/hash drift fail validation.

### Training export

Logical source: `rome_transport.features.next_stop_delay_labeled`.
Current validated version: **12**. Historical experiment reference: **8**.
v8 is no longer readable under retention; subsequent VACUUM is recorded in history.
v9-v12 contain maintenance/metadata operations, and v12's population/labels passed
compatibility validation. This is evidence of logical compatibility, not a direct v8/v12
full-row equivalence test. No automatic fallback to a newer Delta version is allowed.

The explicit projection includes observation identity, service date/time, target next-stop
identity, all raw features, realized delay/class label and label timestamp/horizon audit.
Identity/labels/provenance are never estimator inputs. Metadata records source table/version,
hash, split boundaries, label-availability cutoff, code SHA and export/sampling manifest.

Business event: `next_stop_delay_ge_300s`, display **Ritardo ≥ 5 min**.
Positive label: `target_realized_next_stop_delay_seconds >= 300`.
Retain `ABS(delay) <= 43200`, valid observation keys, horizon 1-1800 seconds,
`label_source_feed_timestamp > feed_timestamp` and exact horizon consistency.
The stored class must equal the derived class; count rejected rows and fail release on
unexpected exclusions rather than silently redefining the validated population.

`training_select(version=12, start_epoch=..., end_epoch=..., label_available_before=...)`
requires explicit nonnegative integer bounds, half-open feed interval and a strict label
availability cutoff. Every partition's input and accepted/excluded counts must be reconciled.
For train, the label cutoff is the validation start; for validation, test start; for test,
the observation-complete evaluation cutoff. Missing or duplicate keys block export release.

### Inference export

Source: `rome_transport.features.next_stop_delay_scoring`; grain:
`(feed_timestamp, entity_id, trip_id, stop_sequence, stop_id)`.
Use `inference_select(version=..., feed_timestamp=...)`, pinning both values from one
validated preparation. Never use `SELECT *`, the labeled table or a fallback older epoch.

The allowlist is exactly PR3's observation, target next-stop, service/vehicle identity,
47 features and four lineage fields: `source_silver_version`, `feature_prepared_at`,
`feature_preparation_batch_id`, `feature_contract_hash`. Same-snapshot `target_*` identity
is metadata, not future outcome. Export also adds pinned `export_source_version`,
`exported_at` and `feed_age_seconds`; compare epochs in UTC and display Europe/Rome.
There are **no future labels, realized outcomes, historical predictions or prediction outputs**.

Validated snapshot: **v0, 24,797 rows, epoch 1790928849**, 2 October 2026 10:14:09
Europe/Rome. This remains **HISTORICAL SCORING VALIDATION, NOT LIVE INFERENCE**.
Missingness of some optional inputs/categories is approximately 75-79%; a valid schema
does not establish acceptable model quality on this population.

Backend release checks: exact schema/order/hash; one non-null preparation lineage/epoch;
expected row count, non-null/unique observation keys, next sequence > current sequence;
no labels; no infinite numerics; no future-dated source/preparation; feed age and
preparation age inside an explicitly approved SLA. An SLA is not invented in this PR.
Stale/empty/incomplete data returns an explicit unavailable/stale response and never
silently masquerades as current. Preparation time alone cannot prove fresh source data.

## Recommended access: SQL Statement Execution API

Use the existing serverless SQL warehouse and authenticated HTTPS
`POST /api/2.0/sql/statements` with SELECT/DESCRIBE/SHOW only. No new connector/SDK,
cluster or file export is necessary. Small metadata responses use INLINE JSON_ARRAY.
Large feature exports use **EXTERNAL_LINKS + ARROW_STREAM**, bounded by a byte budget.
Official [API contract](https://docs.databricks.com/api/statement-execution/v1/statement-execution)
and [chunk download guide](https://docs.databricks.com/aws/en/dev-tools/sql-execution-tutorial).

Observed read-only probes: pinned training v12 aggregates succeeded; pinned scoring v0
read succeeded; Gold executive overview read succeeded. A two-row/five-key scoring query
returned one Arrow chunk (1,840 bytes), not truncated; its signed URL downloaded with HTTP
200 using no Authorization header. This proves this channel works now, not full-volume
export throughput, future backend authentication or unattended renewal. Only the existing
local authenticated profile was used; no token or signed URL is committed or logged publicly.
Both generated contract SELECTs also succeeded against pinned versions (two-row probes):
67 training columns and 70 inference columns including export/freshness metadata.

Future backend requirements:

1. Supply host/warehouse/auth via private deployment configuration. Use a supported
   workspace authentication method that is actually available to that account; validate
   unattended renewal before enabling scheduling. Interactive developer authentication
   alone is not proof of service availability. Never expose credentials to React/n8n logs.
2. Submit fixed projections and bound parameters/validated integer versions. Pin every
   source once. Poll PENDING/RUNNING to a terminal status with timeout and cancellation;
   retry transient transport/rate/quota failures with bounded backoff, not schema failures.
3. Fetch all manifest chunks and verify schema, total rows/bytes, contiguous offsets,
   uniqueness and `truncated == false`. INLINE has a 25 MiB limit; chunked external
   results support up to 100 GiB. An explicit row/byte cap can truncate: reject partial
   exports. Polling or fetching must not silently apply the audit helper's 1,000-row limit.
4. Download short-lived signed external links without forwarding Databricks Authorization.
   Links are secrets and expire within 15 minutes; renew through the API as necessary.
   Decode Arrow in bounded batches, preserving nullable integer categories, UTC timestamps
   and dates. Validate each chunk and the final manifest before promoting a cached dataset.
5. Cache a successful immutable export once per table/version/projection/split/sampling
   manifest. Store interoperable Arrow/Parquet outside Git in the future backend's storage.
   Subsequent training reuses that validated export; do not download ten million rows per fit.

Warehouse auto-start from a SELECT is normal read execution; this audit changed no warehouse
settings. Free Edition has one 2X-Small warehouse, fair-use quotas and no SLA. Do not claim
commercial production uptime. Backend hosting, credentials and capacity acceptance are
separate deployment work, with explicit tests rather than an assumed available service.

## Measured training footprint and selection strategy

Pinned v12 measurements (full-table server-side aggregates; no bulk local download):

| Measure | Result |
| --- | ---: |
| Eligible rows | 9,975,703 |
| Positive / negative | 1,878,569 / 8,097,134 |
| Positive fraction | 18.8314% |
| Service dates / routes / trips / vehicles | 8 / 412 / 82,936 / 2,157 |
| Feed epochs | 964 |
| Active Delta files / compressed full-table bytes | 23 / 1,207,872,358 |
| 47-feature JSON string-array footprint, mean / maximum bytes per row | 337.134 / 405 |
| Date × route × feed_hour × class nonempty strata | 60,856 |
| Stratum rows: minimum / p10 / median / p90 / maximum | 1 / 9 / 97 / 403 / 1,816 |

The JSON estimate casts feature values to strings and includes NULLs; it is a proxy,
not Arrow bytes, pandas RAM or artifact size. It implies about **3.36 GB** feature-only
transfer before metadata. A dense float64 matrix of 42 numerics alone is **3.35 GB**;
the historical 475-dimensional dense vector would be **37.91 GB**, before copies or
estimator overhead. Neither is a recommendation to construct it.

| Service date | Negative | Positive | Negative / positive routes | Negative / positive hours |
| --- | ---: | ---: | --- | --- |
| 2026-09-25 | 756,150 | 200,639 | 370 / 308 | 16 / 16 |
| 2026-09-26 | 1,040,654 | 245,536 | 371 / 313 | 24 / 24 |
| 2026-09-27 | 716,529 | 184,724 | 334 / 274 | 23 / 23 |
| 2026-09-28 | 1,389,286 | 237,113 | 377 / 330 | 23 / 23 |
| 2026-09-29 | 1,283,348 | 320,915 | 378 / 331 | 24 / 24 |
| 2026-09-30 | 1,105,064 | 307,933 | 368 / 319 | 24 / 24 |
| 2026-10-01 | 1,384,897 | 332,251 | 373 / 328 | 24 / 24 |
| 2026-10-02 | 421,206 | 49,458 | 335 / 247 | 7 / 7 |

Recommendation: create a separately authorized curated Delta training release with
immutable version/manifest, then cache one bounded export. Do not materialize anything
in this PR. No sampling percentage, model or final row budget has been selected.

The next training-design change must:

1. Assign **all distinct feed epochs chronologically** to train/validation/test before
   sampling, keeping tied epochs together. Freeze boundaries without looking at TEST
   performance; purge labels not yet available at the next partition boundary. Preserve
   causal full-history feature preparation before any sampling; never recompute histories
   on the sampled subset or current snapshot alone.
2. Measure a bounded Arrow pilot's bytes/row, nullable/sparse preprocessing expansion,
   peak resident memory and fit memory for actual candidate implementations on the chosen
   backend host. Derive row capacity from the host's usable memory minus measured overhead
   and an explicit reserve, plus transfer/time budgets. Record the measured capacity;
   no naive full-table `toPandas()` and no unsupported guessed sampling percentage.
3. Within TRAIN only, allocate deterministic proportional quotas over observed
   date/route/hour/class strata, preserving each nonempty stratum when capacity allows.
   Rank by a recorded seed and stable observation-key hash with full-key tie-break.
   Record exact SQL/hash algorithm, quotas, inclusion probabilities, source version,
   rejected rows and selected key checksum. If a minimum-one quota exceeds capacity,
   fail the proposed budget and revise the design explicitly; do not silently lose routes,
   dates, rare positives or hours. Minimum quotas can alter class proportions: document
   sampling weights and evaluate weighting choices on validation.
4. Preserve natural class prevalence in complete temporal validation/TEST when capacity
   permits, or use an independently specified reproducible evaluation sample with
   inclusion weights and reported uncertainty. Stratification must never move observations
   across temporal partitions. Report coverage and both classes for each partition.
5. Select model/preprocessing/calibration and threshold using TRAIN/validation only; use
   untouched later dates for the new final TEST. Eight historical dates and previously
   assessed TEST data cannot establish new independent generalization. Persist the complete
   sklearn pipeline with its own provenance and prove reload/parity before inference.

## Semantics and historical evidence

`historical_model_threshold = 0.45`, comparison **strict >** (ties negative).
`future_operational_model_threshold = null`: **to be validated from new model evaluation**.
The target's 300 seconds does not change. Historical risk bands and flag logic remain
reference display rules, not calibrated guarantees or automatic thresholds for sklearn.

**HISTORICAL REFERENCE TEST METRICS**, Spark Random Forest only:
Accuracy 0.9401; precision 0.8450; recall 0.8123; F1 0.8283; ROC-AUC 0.9690; PR-AUC 0.9063.
Persistence regression remains historical reference (MAE 136.9818s, RMSE 401.8525s,
R² 0.6940, WAPE 0.3293). None is attributed to an untrained external model.

## Freshness gap and internal orchestration design

The read-only Job inventory contains one scheduled ingestion Job, with **one task pointing
to 05**, periodic 10 minutes, unpaused. There are no 06/07/08 task dependencies or other
listed downstream Jobs. Notebook 05 only appends Bronze/audit; it never invokes downstream
notebooks. Notebook 06 overwrites Silver outputs when explicitly executed; 07 pins inputs
and overwrites enrichment; `08_current_scoring_dataset` requires an explicit disabled-by-default
staging-write gate. No Delta commit automatically runs these notebooks.

Observed latest Bronze Trip Update epoch **1792960055** (25 October); Silver observations,
enrichment and staging all remain **1790928849** (2 October). Enrichment is v8 and staging
v0. This establishes missing downstream orchestration, not an ingestion outage. It does
not prove every source is continuously healthy. Existing Gold/model results are historical.

Proposed separately authorized Databricks task chain:

```text
05 ingestion + terminal audit
  -> verify required source success / explicit partial-source policy
  -> 06 Silver
  -> 07 enrichment (causal positions <=180s; empty-alert evidence)
  -> 08_current_scoring_dataset (shared 08 helper; write gate explicitly authorized)
  -> validate and publish readiness manifest
```

Do not run the old training 08 adapter, 09 labeling or 10-12 ML on the ingestion cadence.
Training releases/label maturity use a separate offline workflow. Gold descriptive refresh
and future prediction-serving refresh are separate, versioned releases; do not run the
current 13 unchanged against external model outputs. It currently assumes historical
Spark metadata/threshold and writes eight tables.

Before enabling this DAG: back up sources/Jobs, measure full-history 06/07/feature cost
against the 10-minute interval and Free quotas; resolve 06's unpinned multi-source reads,
session timezone and known historical duplicate handling; reconcile versions and reject
ambiguous keys. Implement pinned batch/source versions and a single writer, coordinate
with ongoing ingestion, bounded task retries, idempotent same-batch handling, no-overlap
and notification checks. Do not promise a ten-minute downstream SLA before measuring it.
If full rebuild cost is excessive, implement and parity-test incremental history/state in
a separate change before enabling a schedule. Silver full-table overwrites are not incremental.

Readiness must carry preparation batch/hash, Bronze/Silver/staging versions, source/feed
watermarks, counts, quality status and completion instant. Publish only after all checks;
partially committed tables are not a successful release. Keep a last-good manifest for
diagnosis but mark it stale when the SLA fails. No new readiness table is created here.

n8n checks this readiness and the approved freshness policy, deduplicates
`(staging_version, preparation_batch_id, model_version)`, calls FastAPI, retries transient
failures with the same idempotency key and alerts on stale/failed runs. It does not duplicate
Databricks ingestion scheduling or perform Spark transformations. FastAPI rechecks the
manifest before inference; n8n checks are not an integrity boundary. Prediction history
and downstream serving commits require their own idempotent release contract.

## Serving mapping and release boundary

These are **future FastAPI resource names**, not existing endpoints. The same mapping is
machine-readable in the JSON. All current Gold resources represent historical snapshots.

| Databricks table | Future resource | Interpretation |
| --- | --- | --- |
| `gold.executive_overview` | `/network/overview` | Network KPI snapshot; historical and prediction dates separate |
| `gold.route_reliability` | `/reliability/routes` | Observation-weighted routes, minimum 1,000 observations for rankings |
| `gold.stop_reliability` | `/reliability/stops` | Observation stop context, minimum 500 for rankings |
| `gold.temporal_reliability` | `/reliability/temporal` | Service date/day/feed hour |
| `features.next_stop_delay_scoring` | `/scoring/inputs` | Pinned observable input; currently historical |
| `gold.predictive_operations` | `/predictions/reference` | Historical Spark replay, never current external predictions |
| `gold.model_performance` | `/models/reference/evaluation` | Frozen historical metrics |
| `ml.model_registry_metadata` | `/models/reference/metadata` | Historical model/run/attempt metadata |
| `gold.analytics_refresh_metadata` | `/monitoring/analytics` | Refresh ID, source versions, Gold manifest |
| `bronze.ingestion_runs` | `/monitoring/ingestion` | Append-only SOURCE/BATCH audit events |

All names above use catalog `rome_transport`. Verify `gold_versions_json` against actual
Gold Delta versions before exposing a coherent release; pin each mapped table to that
manifest. A successful quality flag is not proof of current freshness. Paginate/filter
large resources with stable keys, allowlist queries and bound parameters; do not expose
arbitrary SQL. Audit events are not uniquely keyed by attempt/status alone: retries,
legacy NULL IDs and multiple terminal/failure events require event-aware aggregation.

Future operational predictions and model/evaluation endpoints must come from the new
artifact/assessment and separately designed prediction history. No new serving table or
endpoint is claimed here. Existing 17,177 historical predictions remain untouched and are
not replaced by the 24,797 input rows. Schema, model version, validated threshold,
freshness, identity, run lineage and release IDs must accompany future prediction output.

## Separate runtime acceptance

No Job, notebook execution, downstream table, model, dashboard live definition or publish
is changed by this PR. Needed next: authorize and validate the downstream DAG above;
approve backend hosting/auth/capacity; release a curated export; implement/evaluate/persist
and reload the external model; independent inference; then prediction history, serving
adaptation and n8n. Dashboard source corrections and required PDF/screenshot replacement
are tracked in the [dashboard audit](../dashboard/databricks/README.md).
