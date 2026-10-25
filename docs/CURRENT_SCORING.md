# Common features and current scoring input (PR3)

Repository implementation only: no PR3 notebook or staging table has been deployed or executed in Databricks. Existing MLflow runs, models, predictions, Gold and dashboard outputs are unchanged. Notebook 12 remains the historical refit/replay workflow; scoring-only artifacts belong to PR4.

## Architecture and selection

`silver.realtime_enriched_observations` at a pinned Delta version feeds the shared definitions notebook `08_common_feature_preparation` via a separate `%run` cell. The helper performs no reads, writes, fitting or labeling by itself.

- Training: notebook 08 calls the common builder, attaches its original same-snapshot provisional audit fields and preserves the original feature-table projection and eligibility filter. Notebook 09 alone matches future observations and creates realized proxy labels. Notebooks 09–12 are unchanged.
- Current input: notebook `08_current_scoring_dataset` reads Silver directly, restricts history to the selected feed epoch, computes the common features on all valid past/current observations, then selects one feed snapshot. It never reads the feature/labeled tables or requires a next-stop delay value.

The default epoch is the maximum Silver feed timestamp no later than preparation time. An explicit epoch can be requested for controlled validation. No fallback to an older nonempty scoring population occurs. A snapshot without eligible rows blocks overwrite. Latest available Silver is not necessarily fresh: feed age is printed, and this PR does not invent a freshness SLA or refresh Silver.

## Observation and next-stop contract

Staging table: `rome_transport.features.next_stop_delay_scoring`. Grain stays `(feed_timestamp, entity_id, trip_id, stop_sequence, stop_id)`.

Each row represents a reported trip-stop estimate observed at the feed epoch. `stop_id/stop_sequence` identify that observation, not necessarily the physical position of a bus. The target IDs identify the next represented stop in the same trip/entity/service-date/start-time/vehicle snapshot. Sparse feeds can skip scheduled stops. A row needs next-stop identifiers with increasing sequence, but never a next-stop delay, future snapshot or label. Terminal rows are excluded; a represented next stop with NULL delay remains eligible.

The original vehicle `current_stop_sequence/current_status` and optional `vehicle_current_stop_id` remain distinct from the observation stop. No arbitrary single-stop-per-vehicle selection is introduced. Repeated stop IDs on a looping trip are permitted when stop sequences differ.

The staging schema contains observation/vehicle/service identifiers, the five same-snapshot target identity fields, the ordered 47 raw model features and four lineage fields. Target identity fields are audit metadata, not assembled model features; no target delay, classification label, provisional outcome or label provenance is persisted. `SCORING_COLUMNS` is the explicit output allowlist. The legacy notebook 12 future-column guard is not reused for this new staging; PR4 must separate identifiers from raw model inputs explicitly.

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

## Validation and controlled rollout still required

Local tests execute the production definitions and a pinned copy of pre-refactor 08 expressions on small deterministic frames. They compare all training-preparation values, types and row counts, check the original eligibility filter, and independently check known historical/lag results. Additional tests cover exact contract/order/hash, current-versus-next identifiers, NULL next-delay eligibility, absence of labels/outcomes from staging, future-row exclusion, schema failures and causal guards. The test evaluator is intentionally limited to these expressions; it is not Spark or a replacement for Spark/Delta integration. Fixture epochs use Rome's January UTC+1 offset and do not validate daylight-saving behavior. No Spark/Java dependency was installed.

After merge, with separate runtime authorization:

1. Record source/output versions and back up runtime notebook 08. Deploy the shared definitions notebook alongside both callers; its workspace name must be `08_common_feature_preparation` for relative `%run` to resolve. Do not execute 09–12.
2. Read a pinned Silver version and compare old/new 08 preparation in memory on the same deterministic set of complete trip snapshots and past history. Compare all original output columns, types, counts and keys before any training feature overwrite. Do not trim history before deriving historical inputs. No unexplained difference is acceptable.
3. Execute current preparation with staging writes disabled; verify the exact 47-feature order/hash, source epoch/version, row eligibility, missingness and current/next-stop identities. Require only known identifiers, not future delays or outcomes.
4. If approved, enable the staging gate and create/overwrite only `rome_transport.features.next_stop_delay_scoring`. Verify Delta schema, counts, keys, no future/outcome columns and lineage. Keep the old staging version if replacing an existing table.
5. Do not execute training, scoring, registration, Gold or dashboards. Do not wire a new Job/schedule in this PR. The existing notebook 12 still represents historical labeled replay; PR4 will provide the scoring-only artifact.
