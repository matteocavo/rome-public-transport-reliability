# GTFS-Realtime ingestion audit contract

## Operational architecture boundary

Ingestion remains owned by Databricks. The 25 October 2026 read-only inventory confirms
the existing periodic Job runs only notebook 05; it does not refresh 06 Silver, 07
enrichment or 08 current-scoring preparation. Bronze freshness therefore does not imply
fresh features. A separately authorized downstream DAG and readiness contract are
specified in [operational handoff](OPERATIONAL_ML_HANDOFF.md). n8n will coordinate external
FastAPI calls/retry/alerts after readiness, without replacing this ingestion schedule.
No runtime settings are modified by the architectural handoff PR.

## Scope and deployment status

PR2 was merged as repository code without runtime changes. A separately authorized controlled rollout on 25 October 2026 applied the additive audit migration and deployed notebooks 05, 07 and 99 from main merge `ae7f9ef11724daaf7bb1cd3e9530673d1c7ef9a8`. The manual production ingestion run succeeded; detailed evidence and remaining validation limits follow below. Notebook 05 is the production writer; notebook 04 remains a historical exploration/bootstrap notebook and must not be scheduled alongside it. Notebook 06 is unchanged. ML, features, labels, Gold and dashboard artifacts are unchanged.

The three GTFS entity table schemas and parser payloads remain unchanged. The audit extension and notebook 07 empty-alert alignment require the explicit rollout below. Do not deploy notebook 05 before the audit migration: preflight verifies column names/types and fails before network retrieval or entity writes.

## Header, parsing and empty responses

The protobuf header is read before building any DataFrame. A request/decode with an initialized header, nonempty GTFS version and positive explicit feed timestamp can represent zero entities: SOURCE SUCCESS, entities_received=0, parsed_row_count=0, rows_appended=0, is_empty=true. No entity row is fabricated. The explicit timestamp requirement is this pipeline's snapshot-key policy; it is not a claim that every optional protobuf field is required by GTFS.

Incrementality follows the [official GTFS-Realtime reference](https://gtfs.org/documentation/realtime/reference/#enum-incrementality): omitted incrementality uses the protobuf FULL_DATASET default. Explicit DIFFERENTIAL is rejected as INVALID_FEED_METADATA, before any entity append. Deleted or unexpected entity types in a FULL feed are rejected as PARSE_ERROR rather than silently skipped. Full/differential mode is validated per fetched payload; historical Roman feed modes are not retroactively certified.

Zero parsed rows with nonzero entities is not labeled an empty feed. A TripUpdate entity may legitimately have no stop updates. entities_received now counts decoded protobuf entities, not distinct parsed entity IDs or stop rows. The separate parsed_row_count preserves this distinction.

NETWORK_ERROR, HTTP_ERROR, INCOMPLETE_READ, DECODE_ERROR, PARSE_ERROR, INVALID_FEED_METADATA, WRITE_ERROR and UNKNOWN_ERROR are stable categories. Original error_message is retained. Metadata known before failure remains available; unknown parsed/duplicate counts are NULL. The legacy non-null rows_appended=0 on a failure is not proof that a timed-out write could not commit.

## Additive audit migration - applied during the authorized rollout

Existing table: rome_transport.bronze.ingestion_runs. Preserve its seven existing columns:
run_timestamp TIMESTAMP, source_name STRING, feed_timestamp BIGINT, entities_received BIGINT, rows_appended BIGINT, status STRING, error_message STRING.

The authorized rollout applied the following nine missing columns after checking all seven legacy column types. This SQL remains documentation; CI and the writer do not execute it automatically:

```sql
ALTER TABLE rome_transport.bronze.ingestion_runs ADD COLUMNS (
  record_type STRING,
  ingestion_batch_id STRING,
  ingestion_attempt_id STRING,
  parsed_row_count BIGINT,
  duplicate_row_count BIGINT,
  is_empty BOOLEAN,
  feed_incrementality STRING,
  decode_status STRING,
  error_type STRING
);
```

Inspect DESCRIBE first and add only missing columns if the migration was partially applied. No automatic ALTER, mergeSchema, rewrite, backfill or new table creation is performed. Legacy rows retain NULL new fields. Consumers use COALESCE(record_type, 'SOURCE') for historical source rows, and must not invent historical incrementality or empty-feed evidence.

record_type=SOURCE records preserve source status SUCCESS/FAILED. record_type=BATCH uses reserved source_name=__batch__ and status RUNNING/SUCCESS/PARTIAL_SUCCESS/FAILED. Existing consumers must exclude __batch__ when calculating per-source counts. Old health queries are adjusted with this backward-compatible exclusion; new health cells require the migrated columns. No redundant entity_count/inserted_count/batch_status columns are added.

duplicate_row_count = parsed_row_count - rows_appended includes exact duplicates within the payload and keys already in Bronze. The values are attempt-level, not reconstructed historical totals. BATCH rows do not represent a source snapshot: their source counts are placeholders and must never enter feed metrics.

## Batch outcomes and failure behavior

Each execution appends a BATCH RUNNING record before acquisition, then one SOURCE outcome per feed, then a terminal BATCH event. There is no replacement of previous attempts.

- SUCCESS: every source succeeded, including valid empty full feeds.
- PARTIAL_SUCCESS: at least one source succeeded and one failed.
- FAILED: every source failed, or an audit/finalization failure prevents a usable batch record.

All sources are attempted even after a source failure. After terminal audit persistence, PARTIAL_SUCCESS and FAILED raise a task error so existing Job failure notifications can apply. No Job setting is changed. The authorized happy-path runtime test passed. Same-snapshot retry and failure notification delivery remain untested in production; failure propagation is covered by code review and offline tests.

An initial audit-write failure stops before source writes. A later audit-write failure is not swallowed: remaining sources are attempted and final status is FAILED. A final audit-write failure propagates. Cross-table writes are not atomic: a process crash or storage outage can still leave entity commits without their audit outcome; a RUNNING-only attempt means incomplete/unknown. This PR does not claim exactly-once audit transactions.

## Retry and idempotency

The existing snapshot keys are preserved: Trip Updates (feed_timestamp, entity_id, stop_sequence, stop_id), positions (feed_timestamp, entity_id), alerts (feed_timestamp, alert_id). Exact repeated rows within one payload are collapsed; conflicting payloads at one incoming key fail rather than selecting an arbitrary row. The anti-join is null-safe, preventing NULL stop keys from bypassing retry deduplication. No source values are corrected or overwritten.

Run only one writer at a time. The current Job already has max_concurrent_runs=1; no additional writer, legacy notebook 04 run, or simultaneous manual run is safe without coordination. Delta does not enforce the logical keys, and an anti-join is not a multi-writer uniqueness constraint.

run_realtime_ingestion(ingestion_batch_id=...) accepts a stable logical ID for explicit retries. Every call creates a fresh ingestion_attempt_id. With no supplied ID, each invocation starts a new logical batch; platform retries are not automatically grouped into one batch because this repository has no verified stable parent-run context contract. Bronze deduplication remains safe independently of audit grouping. If automatic cross-retry grouping is required, wire a stable root Job run identifier in a separately approved Job/notebook-parameter change; do not guess an execution-attempt ID is stable. No such change is made here.

## Service Alerts and downstream impact

A successful FULL_DATASET snapshot with zero alerts is affirmative evidence of an empty feed, unlike missing acquisition. Notebook 07 adds only successful empty FULL source audit timestamps to the existing alert-row timestamp timeline, so a newer empty snapshot supersedes older alert rows. Its equality join finds no alerts at that timestamp; coverage remains available with zero active alerts. DIFFERENTIAL and failed audit records are never evidence of empty full state. Nonempty audit timestamps are not unioned independently: if they are newer than the pinned alert table, doing so could falsely imply zero alerts under concurrent ingestion.

With no migrated audit columns, notebook 07 retains its historical row-based behavior. Partially present new fields fail validation. Source Delta versions, including ingestion_runs when used, are pinned for reproducible actions within the run. Legacy empty snapshots cannot be reconstructed from zero inserted rows: repeated nonempty snapshots can also insert zero rows.

No enrichment is run in PR2. Applying new empty-snapshot evidence requires a separately authorized notebook 07 refresh, which can legitimately change alert context after such a snapshot; dependent features must then be refreshed separately. Existing outputs and historical labels/models/Gold are untouched. This fixes future availability semantics rather than silently changing current analytical results.

## Offline validation and runtime follow-up

Tests extract production function definitions with AST, avoiding notebook top-level execution. Mocked protobuf-like objects and in-memory DataFrame doubles cover header metadata, zero entities, decode/error paths, source/batch status, audit failures, explicit retry IDs, null-safe anti-join and duplicate conflicts. They do not replace an actual protobuf/Spark/Delta integration test.

Runtime validation below covers schema compatibility, nonempty FULL feeds, audit persistence and append integrity. Remaining follow-up: same-snapshot retries in an isolated Spark/Delta environment, real protobuf empty/Differential integration, notebook 07 fixture execution without a downstream refresh, and failure notification delivery. Do not execute the existing notebook 04 exploration cells as a deployment procedure. No new package is required by the runtime implementation.


## Controlled runtime rollout evidence

Main deployed: `ae7f9ef11724daaf7bb1cd3e9530673d1c7ef9a8` (PR2 merge). All three deployed sources were re-exported and compared with main. The existing runtime 05/07 exports matched the PR1 baseline exactly before replacement; their prior object IDs and source backups were retained in the operator evidence bundle. Notebook 99 was absent from this runtime folder and was created from main. Notebook 04 remains unscheduled/exploratory; 06 and 08+ were not changed.

Workspace folder: the existing project notebook folder.

| Notebook | Runtime action |
| --- | --- |
| `05_gtfs_realtime_pipeline` | Updated and executed by its existing Job |
| `07_gtfs_realtime_enrichment` | Updated only; no enrichment execution |
| `99_realtime_pipeline_health_check` | Created; all seven SQL cells executed through the SQL warehouse |

### Migration and rollback baseline

The stable pre-migration `ingestion_runs` version was **10501**, with **10,173 rows** and the seven legacy fields listed above. DESCRIBE TABLE EXTENDED, table properties, recent Delta history and the latest 20 audit records were captured before migration. The additive ALTER created version **10502** without changing the row count; all nine new columns were NULL on all 10,173 legacy records. No backfill, property change, OPTIMIZE or VACUUM was requested. Existing automatic Delta optimization appeared in the pre-rollout history and was not initiated by this rollout.

After the manual run the audit was version **10507**, with **10,178 rows**: precisely five new records. The seven original columns retain their names/types and the nine additions retain the documented types.

Rollback was not needed. The prepared recovery point is audit version 10501 plus the exported prior notebook sources. If rollback becomes necessary, pause the trigger and wait for active runs, preserve subsequent audit evidence, restore the old writer together with the compatible audit schema/data using a separately authorized `RESTORE TABLE rome_transport.bronze.ingestion_runs TO VERSION AS OF 10501`, then re-import the saved 05/07 sources. Notebook 99 was newly created, so it has no prior revision. Time Travel availability must be rechecked before recovery; restoration would remove post-baseline audit records from the current view, so it is not an automatic or lossless operational rollback. Bronze entity rows need no rollback for this append-only successful run. Do not resume the legacy writer against the extended schema without validating append compatibility.

### Manual run and audit outcomes

Controlled Job run 588842343799032 (runtime evidence retained privately) completed SUCCESS. Task run: 401724133439920. Logical batch: `b5ea5cf1-e87a-4c3b-bf9b-5b452ea33076`; attempt: `b72364ae-cfe2-4138-8042-7c443a3d5994`.

Exactly one BATCH RUNNING, three SOURCE SUCCESS and one terminal BATCH SUCCESS were persisted. Every source had the same non-null batch/attempt IDs, a positive feed timestamp, `FULL_DATASET`, `DECODED`, `is_empty=false` and NULL error fields.

| Source | Feed timestamp | Entities | Parsed rows | Appended | Duplicates |
| --- | ---: | ---: | ---: | ---: | ---: |
| trip_updates | 1792938901 | 700 | 13,514 | 13,480 | 34 |
| vehicle_positions | 1792939051 | 667 | 667 | 667 | 0 |
| service_alerts | 1792938901 | 128 | 128 | 128 | 0 |

The 34 discarded Trip Update rows demonstrate duplicate elimination within this input; they are not a same-snapshot retry test.

### Bronze integrity

The baseline was captured after the last old-writer scheduled run had terminated and while the trigger was paused.

| Table | Delta version before / after manual run | Rows before / after | Duplicate groups / excess rows, unchanged |
| --- | --- | --- | --- |
| trip_updates | 3874 / 3875 | 55,937,673 / 55,951,153 | 232,698 / 236,180 |
| vehicle_positions | 3533 / 3534 | 2,898,974 / 2,899,641 | 138 / 138 |
| service_alerts | 3501 / 3502 | 517,767 / 517,895 | 0 / 0 |

All three entity schemas matched before/after. `EXCEPT ALL` from each pinned pre-run version to its pinned post-run version returned zero missing or changed rows, including payload and ingestion fields. Row-count deltas matched source rows_appended exactly. Grouping on the documented keys, including NULL keys, showed no duplicate growth. Historical duplicates were preserved, not repaired or hidden. Feed timestamps advanced from the previous snapshot.

### Job, health checks and remaining limits

Job **195066091322974** still has one task pointing to notebook 05, a 10-minute periodic trigger, `max_concurrent_runs=1`, queue enabled and the original failure email notification. The trigger was temporarily PAUSED, an already-started old-writer run was allowed to finish, and the original UNPAUSED trigger was restored. A complete comparison of Job settings before and after restoration matched exactly. No compute, task, retry or notification configuration was changed permanently.

All seven notebook 99 SQL cells succeeded. Source queries excluded `__batch__`; latest success/failure remained available across legacy and new rows; SOURCE details and both BATCH events were visible. Successful-empty query executed successfully with zero observations, which does not prove a live empty feed occurred.

The 20 offline tests and notebook compilation passed again. FULL nonempty feeds and Spark/Delta production writes were observed. Valid empty FULL, rejected DIFFERENTIAL, null-safe same-snapshot retry, new attempt IDs under a supplied stable batch ID, and PARTIAL_SUCCESS/failure-after-audit remain supported by offline tests; no intentional production failure or replay was introduced. Failure email delivery was not exercised. No separate isolated Spark notebook was created. Notebook 07 was deployed but not executed, so its empty-alert timeline is code/offline-verified and existing enriched output remains unchanged.

ML training/scoring/registration, Gold, dashboard, Power BI and the app repository were untouched. No downstream refresh was run.


### First scheduled observation after restoration

Periodic run 1030587734568633 (runtime evidence retained privately) started automatically after trigger restoration and completed SUCCESS. It persisted the expected five records under new logical batch `6e821354-466c-4e97-a8f7-f2ed895fe9ef` and attempt `d0daff62-c731-412f-a022-a72cbe0bb8d9`. All three sources were SUCCESS, DECODED, FULL_DATASET, nonempty and without errors; terminal BATCH was SUCCESS.

| Source | Entities | Parsed | Appended | Duplicates |
| --- | ---: | ---: | ---: | ---: |
| trip_updates | 684 | 13,627 | 13,579 | 48 |
| vehicle_positions | 684 | 684 | 684 | 0 |
| service_alerts | 128 | 128 | 128 | 0 |

Each new feed timestamp was newer than the manual run. No duplicate keys were present in these new snapshots, and total row-count increments matched the three append counts. This confirms operational scheduled ingestion within the observed window, not a guarantee against future feed or infrastructure failures. The offline-only limitations above remain open; no additional production replay or failure injection was used.
