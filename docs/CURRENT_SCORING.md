# Common features and inference export input (PR3)

**Current architecture:** Databricks owns observable input preparation; the future FastAPI/sklearn backend owns operational model inference. [ADR-001](ARCHITECTURE_DECISIONS.md) supersedes the proposed Spark production path. [Training/inference export contracts](OPERATIONAL_ML_HANDOFF.md) preserve the ordered 47 features and identity without future labels. The historical 0.45 Spark probability threshold is not automatically the future backend threshold.

PR3 was deployed and validated in a separately authorized controlled rollout on 25 October 2026 from main `c343f1e8420a801bab98d22f298c939f4b8c0ae0`. Shared preparation passed Spark old/new parity; input staging was created once and read back exactly. Existing MLflow runs, models, predictions, Gold and dashboard outputs are unchanged. Notebook 12 remains historical refit/replay; PR4's Spark persistence implementation is preserved as reference, not the operational target. **The latest available Silver input is from 2 October, not a fresh live feed.**

## Architecture and selection

`silver.realtime_enriched_observations` at a pinned Delta version feeds the shared definitions notebook `08_common_feature_preparation` via a separate `%run` cell. The helper performs no reads, writes, fitting or labeling by itself.

- Training: notebook 08 calls the common builder, attaches its original same-snapshot provisional audit fields and preserves the original feature-table projection and eligibility filter. Notebook 09 alone matches future observations and creates realized proxy labels. Notebooks 09–12 are unchanged.
- Current input: notebook `08_current_scoring_dataset` reads Silver directly, restricts history to the selected feed epoch, computes the common features on all valid past/current observations, then selects one feed snapshot. It never reads the feature/labeled tables or requires a next-stop delay value.

The default epoch is the maximum Silver feed timestamp no later than preparation time. An explicit epoch can be requested for controlled validation. No fallback to an older nonempty scoring population occurs. A snapshot without eligible rows blocks overwrite. Latest available Silver is not necessarily fresh: feed age is printed, and this PR does not invent a freshness SLA or refresh Silver.

## Observation and next-stop contract

Staging table: `rome_transport.features.next_stop_delay_scoring`. Grain stays `(feed_timestamp, entity_id, trip_id, stop_sequence, stop_id)`.

Each row represents a reported trip-stop estimate observed at the feed epoch. `stop_id/stop_sequence` identify that observation, not necessarily the physical position of a bus. The target IDs identify the next represented stop in the same trip/entity/service-date/start-time/vehicle snapshot. Sparse feeds can skip scheduled stops. A row needs next-stop identifiers with increasing sequence, but never a next-stop delay, future snapshot or label. Terminal rows are excluded; a represented next stop with NULL delay remains eligible.

The original vehicle `current_stop_sequence/current_status` and optional `vehicle_current_stop_id` remain distinct from the observation stop. No arbitrary single-stop-per-vehicle selection is introduced. Repeated stop IDs on a looping trip are permitted when stop sequences differ.

The staging schema contains observation/vehicle/service identifiers, the five same-snapshot target identity fields, the ordered 47 raw model features and four lineage fields. Target identity fields are audit metadata, not assembled model features; no target delay, classification label, provisional outcome or label provenance is persisted. `SCORING_COLUMNS` is the explicit output allowlist. The legacy notebook 12 future-column guard is not reused for staging; external inference must preserve this explicit separation of identity metadata from raw model inputs.

## Frozen feature contract

The runtime registry metadata was read without modification. Its exact JSON and feature order are embedded in the helper. Source MLflow run: `a9b568f478344d9dba90436358f14092`.

Before and after SHA-256:
`11682b3cc1ab5c1d8440af4f6294e932e815126c3dd86ab609f4d5f8f35c41a5`

42 numerical features precede 5 categorical features, in the existing order. Hashing preserves the original forbidden-column list and preprocessing settings; it does not redefine the model. Raw expected types were checked against the existing runtime feature schema. Numerical Spark types compatible with the frozen DOUBLE projection are allowed; route_type/direction_id remain integer categories, and route_id/agency_id/current_status remain strings. Every model column must exist. NULL values are allowed, including entirely NULL optional inputs; the already-fit imputer/indexer handles them later. No new imputation, fit or encoding occurs. Incompatible types, missing model columns, hash mismatch, duplicate or incoherent keys and noncausal provenance block persistence. Infinity remains invalid; NaN/NULL numerical values are left for the frozen median imputer.

`trip_progress_ratio` remains NULL in common preparation because full static trip extent is unavailable. It is retained as a technical column in the legacy training output, but is absent from both the frozen model feature list and current scoring projection.

## Feature/source/leakage audit matrix

The following lists all raw model inputs in contract order. All are used by the model. The time basis is feed-time availability, not proof of historical ingestion-time availability. Same-snapshot earlier-stop values are contemporaneous feed estimates, not certified completed-stop outcomes.

| Feature | Raw type | Source / time basis | Future or target-derived? |
| --- | --- | --- | --- |
| `feed_hour` | int | Current feed_datetime, Europe/Rome | No |
| `feed_minute` | int | Current feed_datetime, Europe/Rome | No |
| `day_of_week` | int | Current service_date | No |
| `is_weekend` | int | Current service_date | No |
| `minutes_since_midnight` | int | Current feed_datetime, Europe/Rome | No |
| `current_arrival_delay_seconds` | int | Current-stop arrival/departure feed estimate at t | No |
| `current_departure_delay_seconds` | int | Current-stop arrival/departure feed estimate at t | No |
| `abs_current_delay_seconds` | int | Current-stop arrival/departure feed estimate at t | No |
| `is_currently_delayed_flag` | int | Current-stop arrival/departure feed estimate at t | No |
| `is_currently_early_flag` | int | Current-stop arrival/departure feed estimate at t | No |
| `lag_1_arrival_delay_seconds` | int | Current/preceding represented stops in same snapshot; no following delays | No |
| `lag_2_arrival_delay_seconds` | int | Current/preceding represented stops in same snapshot; no following delays | No |
| `lag_3_arrival_delay_seconds` | int | Current/preceding represented stops in same snapshot; no following delays | No |
| `rolling_mean_delay_last_3_stops` | double | Current/preceding represented stops in same snapshot; no following delays | No |
| `rolling_max_delay_last_3_stops` | int | Current/preceding represented stops in same snapshot; no following delays | No |
| `rolling_std_delay_last_3_stops` | double | Current/preceding represented stops in same snapshot; no following delays | No |
| `delay_change_from_previous_stop` | int | Current/preceding represented stops in same snapshot; no following delays | No |
| `delay_acceleration` | int | Current/preceding represented stops in same snapshot; no following delays | No |
| `scheduled_seconds_to_next_stop` | int | Static schedule/coordinates for current and next represented stop at t | No |
| `distance_to_next_stop_meters` | double | Static schedule/coordinates for current and next represented stop at t | No |
| `vehicle_speed` | double | Causal vehicle match at or before feed; age ≤180s | No |
| `vehicle_bearing` | double | Causal vehicle match at or before feed; age ≤180s | No |
| `vehicle_odometer` | double | Causal vehicle match at or before feed; age ≤180s | No |
| `current_stop_sequence` | int | Causal vehicle match at or before feed; age ≤180s | No |
| `vehicle_position_staleness_seconds` | bigint | Causal vehicle match at or before feed; age ≤180s | No |
| `vehicle_position_available_flag` | int | Causal vehicle match at or before feed; age ≤180s | No |
| `active_alert_flag` | int | Latest alert snapshot at or before feed; unknown availability stays NULL | No |
| `active_alert_count` | int | Latest alert snapshot at or before feed; unknown availability stays NULL | No |
| `active_detour_flag` | int | Latest alert snapshot at or before feed; unknown availability stays NULL | No |
| `active_construction_flag` | int | Latest alert snapshot at or before feed; unknown availability stays NULL | No |
| `historical_route_mean_delay` | double | Current-arrival estimates from feed epochs strictly before t; ties excluded | No |
| `historical_stop_mean_delay` | double | Current-arrival estimates from feed epochs strictly before t; ties excluded | No |
| `historical_route_stop_mean_delay` | double | Current-arrival estimates from feed epochs strictly before t; ties excluded | No |
| `historical_route_hour_mean_delay` | double | Current-arrival estimates from feed epochs strictly before t; ties excluded | No |
| `historical_route_observation_count` | bigint | Current-arrival estimates from feed epochs strictly before t; ties excluded | No |
| `historical_stop_observation_count` | bigint | Current-arrival estimates from feed epochs strictly before t; ties excluded | No |
| `missing_vehicle_position` | int | Availability of corresponding observable input/history | No |
| `missing_current_arrival_delay` | int | Availability of corresponding observable input/history | No |
| `missing_current_departure_delay` | int | Availability of corresponding observable input/history | No |
| `missing_historical_route_delay` | int | Availability of corresponding observable input/history | No |
| `missing_historical_stop_delay` | int | Availability of corresponding observable input/history | No |
| `missing_alert_snapshot` | int | Availability of corresponding observable input/history | No |
| `route_id` | string | Silver route/static metadata | No |
| `route_type` | int | Silver route/static metadata | No |
| `direction_id` | int | Silver route/static metadata | No |
| `agency_id` | string | Silver route/static metadata | No |
| `current_status` | string | Causal vehicle state at or before feed; age ≤180s | No |

Historical means/counts include all valid source observations before the original training label filter. Windows exclude all same-epoch peers. Computing history on the latest snapshot alone would change inputs and is intentionally avoided. Static lead operations read schedule/coordinates or next-stop identity within the current feed; they do not read future delay observations. Remaining limitations: upstream retrospective schedules, feed timestamps versus actual ingestion availability and GTFS-RT proxy outcomes are unchanged.

## Persistence and lineage

This is current scoring input/staging, not prediction history. Validated, nonempty output overwrites only `next_stop_delay_scoring`, with schema overwrite matching repository conventions. `ALLOW_STAGING_OVERWRITE=False` is the default operator gate; preparation can be checked before opting into a separately authorized write. No other table is written. The four added lineage fields are `source_silver_version` (BIGINT), `feature_prepared_at` (TIMESTAMP), `feature_preparation_batch_id` (STRING) and `feature_contract_hash` (STRING); the feed epoch and datetime already identify observation time. Session timezone is Europe/Rome; the captured preparation instant is timezone-aware UTC.

The writer validates before overwrite and reads back count/schema/key consistency. This is not a transaction across source, staging and future predictions. Concurrent preparation writers are not supported; coordinate a single authorized run. No prediction history is introduced.

## Validation scope and rollout procedure

Local tests execute the production definitions and a pinned copy of pre-refactor 08 expressions on small deterministic frames. They compare all training-preparation values, types and row counts, check the original eligibility filter, and independently check known historical/lag results. Additional tests cover exact contract/order/hash, current-versus-next identifiers, NULL next-delay eligibility, absence of labels/outcomes from staging, future-row exclusion, schema failures and causal guards. The test evaluator is intentionally limited to these expressions; it is not Spark or a replacement for Spark/Delta integration. Fixture epochs use Rome's January UTC+1 offset and do not validate daylight-saving behavior. No Spark/Java dependency was installed.

The following procedure was followed during the authorized rollout. Keep these steps for future redeployments; no new runtime authorization is implied:

1. Record source/output versions and back up runtime notebook 08. Deploy the shared definitions notebook alongside both callers; its workspace name must be `08_common_feature_preparation` for relative `%run` to resolve. Do not execute 09–12.
2. Read a pinned Silver version and compare old/new 08 preparation in memory on the same deterministic set of complete trip snapshots and past history. Compare all original output columns, types, counts and keys before any training feature overwrite. Do not trim history before deriving historical inputs. No unexplained difference is acceptable.
3. Execute current preparation with staging writes disabled; verify the exact 47-feature order/hash, source epoch/version, row eligibility, missingness and current/next-stop identities. Require only known identifiers, not future delays or outcomes.
4. If approved, enable the staging gate and create/overwrite only `rome_transport.features.next_stop_delay_scoring`. Verify Delta schema, counts, keys, no future/outcome columns and lineage. Keep the old staging version if replacing an existing table.
5. Do not execute training, scoring, registration, Gold or dashboards. Do not wire a Job/schedule under this historical procedure. Notebook 12 represents labeled replay; independent operational inference belongs to the future external backend.


## Controlled runtime validation results

### Deployed notebooks and source baseline

The runtime directory is the existing project notebook folder. All three deployed sources were re-exported and matched main exactly, including the default disabled staging gate. The original runtime 08 was exported before replacement; its executable AST matched the pre-PR3 main baseline. Runtime 09–12 had only comment differences from main and were preserved byte-for-byte. They were not executed.

| Notebook | Action |
| --- | --- |
| `08_common_feature_preparation` | Created from main; relative `%run` verified in real Spark |
| `08_feature_engineering` | Updated from main; full writing notebook not executed |
| `08_current_scoring_dataset` | Created from main; actual body validated with writes disabled and pinned Silver |

One temporary validation notebook ran two unscheduled feature-only executions and was deleted after success. Its source, original 08 export, object/update metadata and structured reports were retained in the local operator evidence bundle. No production Job or schedule was modified.

Pinned source: `rome_transport.silver.realtime_enriched_observations` **v8**, 15,402,700 rows; feed range **1790334480–1790928849**, service dates **2026-09-25–2026-10-02**. Silver remained v8, historical features remained v8 and labeled remained v12 before/after. No feature or label table was overwritten.

### Old/new training preparation parity

Read-only validation run 84499547661340 (runtime evidence retained privately) completed with validation PASS. The legacy expressions came from the actual exported runtime 08; the new helper and actual main training adapter were executed only in memory.

The deterministic scope used routes **719, 314 and 46**, retaining **504,787 history observations across 919 epochs**. Comparisons used epochs **1790334480, 1790630629, 1790927629 and 1790928849**. After the original training eligibility filter: **2,648 old rows = 2,648 new rows**, **105 projected columns**, identical Spark schemas including metadata/nullability and **zero EXCEPT ALL differences in either direction**. This included keys, feature values and provisional audit fields.

Compared epochs covered 76 trips and 63 vehicles, with 1,086 observations missing a vehicle position, 1,675 with one, 31 NULL current delays, 2,185 known alert snapshots and 576 unknown alert snapshots. No sparse next-stop sequence appeared in this particular parity scope; the separate current-snapshot check below found 55 real sparse transitions. The parity result is representative scoped validation, not an exhaustive comparison of the entire historical feature table.

Historical provenance guards passed in Spark. Independent strict-past joins on 20 deterministic real rows checked route, stop, route-stop and route-hour histories: counts matched exactly and means matched within 1e-9. Every contributing maximum epoch was strictly earlier than the current epoch; same-epoch peers were excluded. The full scoring preparation also passed common causality validation on its pinned source context.

### Current input staging and persistence

The actual scoring notebook body ran against pinned Silver v8 with `ALLOW_STAGING_OVERWRITE=False`; it raised only the expected disabled-write gate after validation. Following acceptance of all read-only results, write/read-back run 1103286384923896 (runtime evidence retained privately) explicitly enabled the gate in the isolated execution and made one controlled write. The deployed notebook default remains False.

The staging did not exist before rollout. It now contains **24,797 rows**, **300 routes**, **1,313 trips**, **1,312 vehicles** and **6,740 observation stops**, all at epoch **1790928849**. An independent SQL window check reproduced the eligibility count, including **236 rows whose next represented stop had no arrival-delay estimate** and **55 sparse sequence transitions**. Thus no next-stop delay or future outcome was required.

Grain duplicates: **0**. Invalid target ordering: **0**. Current observation IDs, next-stop IDs and physical vehicle state were inspected separately. For example, an observation at sequence 9 targeted sequence 10 while the physical vehicle remained at sequence 8; these identities were not conflated. Repeated stop IDs are still governed by increasing sequences rather than an invalid blanket stop-ID inequality.

The raw model order, names and compatible Spark types passed: **47 = 42 numerical + 5 categorical**, with the original hash unchanged. `trip_progress_ratio`, delay targets, label/provisional outcomes, future columns and prediction outputs are absent from the persisted staging. Only the five documented target identity fields are present as metadata.

Staging Delta version: **0**, operation **CREATE OR REPLACE TABLE AS SELECT**, **24,797 output rows**. The prepared DataFrame and read-back had identical column order/names/types and counts, with **zero full-row EXCEPT ALL differences in either direction**. Delta relaxed nullable metadata on five missingness indicators and four lineage fields; this storage-level relaxation was recorded explicitly rather than treated as a value/schema-type change. Required identities and lineage values were separately checked.

Persisted lineage:

- `source_silver_version`: **8**
- `feature_preparation_batch_id`: **dd58c7ab-88f8-46f9-80fd-d07b9db8feae**
- `feature_prepared_at`: **2026-10-25T17:49:47.563874Z**
- `feature_contract_hash`: **11682b3cc1ab5c1d8440af4f6294e932e815126c3dd86ab609f4d5f8f35c41a5**

The post-write SQL checks confirmed one source version, one hash and one preparation batch, the single feed epoch, zero duplicate keys and valid target ordering. Staging is overwrite-only current input, not an append history, predictions or evaluation table.

### Missingness and timezone

All 47 inputs were profiled before and after persistence. Profiles matched exactly. There were **zero NaN and infinity values**, and **no completely NULL/unusable model feature**. High NULL rates are explicit limitations of the available input, not newly imputed values. Nullable categories remain for the frozen indexer's invalid-value handling.

| Raw feature | NULL rows | NULL % |
| --- | ---: | ---: |
| `feed_hour` | 0 | 0.00000 |
| `feed_minute` | 0 | 0.00000 |
| `day_of_week` | 0 | 0.00000 |
| `is_weekend` | 0 | 0.00000 |
| `minutes_since_midnight` | 0 | 0.00000 |
| `current_arrival_delay_seconds` | 467 | 1.88329 |
| `current_departure_delay_seconds` | 18,688 | 75.36396 |
| `abs_current_delay_seconds` | 467 | 1.88329 |
| `is_currently_delayed_flag` | 467 | 1.88329 |
| `is_currently_early_flag` | 467 | 1.88329 |
| `lag_1_arrival_delay_seconds` | 1,779 | 7.17425 |
| `lag_2_arrival_delay_seconds` | 3,055 | 12.32004 |
| `lag_3_arrival_delay_seconds` | 4,299 | 17.33677 |
| `rolling_mean_delay_last_3_stops` | 414 | 1.66956 |
| `rolling_max_delay_last_3_stops` | 414 | 1.66956 |
| `rolling_std_delay_last_3_stops` | 1,732 | 6.98472 |
| `delay_change_from_previous_stop` | 1,832 | 7.38799 |
| `delay_acceleration` | 3,155 | 12.72331 |
| `scheduled_seconds_to_next_stop` | 18,756 | 75.63818 |
| `distance_to_next_stop_meters` | 18,756 | 75.63818 |
| `vehicle_speed` | 19,311 | 77.87636 |
| `vehicle_bearing` | 19,580 | 78.96116 |
| `vehicle_odometer` | 2,587 | 10.43271 |
| `current_stop_sequence` | 24 | 0.09679 |
| `vehicle_position_staleness_seconds` | 24 | 0.09679 |
| `vehicle_position_available_flag` | 0 | 0.00000 |
| `active_alert_flag` | 0 | 0.00000 |
| `active_alert_count` | 0 | 0.00000 |
| `active_detour_flag` | 0 | 0.00000 |
| `active_construction_flag` | 0 | 0.00000 |
| `historical_route_mean_delay` | 0 | 0.00000 |
| `historical_stop_mean_delay` | 1 | 0.00403 |
| `historical_route_stop_mean_delay` | 2 | 0.00807 |
| `historical_route_hour_mean_delay` | 0 | 0.00000 |
| `historical_route_observation_count` | 0 | 0.00000 |
| `historical_stop_observation_count` | 0 | 0.00000 |
| `missing_vehicle_position` | 0 | 0.00000 |
| `missing_current_arrival_delay` | 0 | 0.00000 |
| `missing_current_departure_delay` | 0 | 0.00000 |
| `missing_historical_route_delay` | 0 | 0.00000 |
| `missing_historical_stop_delay` | 0 | 0.00000 |
| `missing_alert_snapshot` | 0 | 0.00000 |
| `route_id` | 0 | 0.00000 |
| `route_type` | 18,751 | 75.61802 |
| `direction_id` | 0 | 0.00000 |
| `agency_id` | 18,751 | 75.61802 |
| `current_status` | 24 | 0.09679 |

Notable limitations: departure delay is about 75.36% NULL; schedule interval/distance about 75.64%; speed/bearing about 77.88%/78.96%; route_type/agency_id about 75.62%. These are compatible nullable inputs for the frozen preprocessing, but this rollout does not establish model quality under that missingness pattern.

Spark session timezone was explicitly **Europe/Rome**. Real rows at epoch 1790928849 matched Python timezone conversion: **2026-10-02T10:14:09+02:00**, feed_hour=10, feed_minute=14, minutes_since_midnight=614, day_of_week=6 (Friday), is_weekend=0. This is a nonambiguous date before the DST transition. The previous session timezone **Etc/UTC** was restored in both isolated runs; no permanent timezone setting changed.

### Boundaries, rollback and readiness

No rollback was needed. The original runtime 08 source and update metadata remain available in the evidence bundle; helper/scoring were new objects. The staging was new, with no prior version to restore. A future rollback must preserve evidence and avoid touching Silver or historical feature/label tables; no automatic DROP or RESTORE was performed.

No model was loaded, fitted or transformed during PR3. No MLflow run or registration was created/modified, and notebooks 09–13 were not run. Gold, dashboards, Power BI, ingestion settings and the app repository were untouched. The validated input contract is reused for the external operational handoff; source freshness and high missingness remain limitations. This result does not claim current operational service or prediction quality.

## Downstream freshness gap

Read-only inventory on 25 October 2026 confirms a single scheduled Job with one task: notebook 05, every 10 minutes. It does not invoke 06, 07 or current-scoring preparation. Bronze Trip Updates advanced to epoch 1792960055; Silver observations/enrichment and staging remain at 1790928849. The staging overwrite gate remains OFF by default. No automatic downstream refresh is configured.

The separately authorized next rollout must implement a version-pinned 05 -> 06 -> 07 -> `08_current_scoring_dataset` dependency chain, measure full-history costs, reconcile duplicate/source/timezone behavior and publish a validated readiness manifest before external inference. Do not schedule historical labeling/training on this cadence. See [freshness analysis and orchestration acceptance](OPERATIONAL_ML_HANDOFF.md#freshness-gap-and-internal-orchestration-design).
