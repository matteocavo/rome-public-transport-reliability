# GTFS-Realtime ingestion audit contract

## Scope and deployment status

Repository implementation only. PR2 does not migrate or modify Databricks tables, deploy notebooks, change Jobs/schedules, or execute ingestion. Notebook 05 is the production writer; notebook 04 remains a historical exploration/bootstrap notebook and must not be scheduled alongside it. Notebook 06 is unchanged. ML, features, labels, Gold and dashboard artifacts are unchanged.

The three GTFS entity table schemas and parser payloads remain unchanged. The audit extension and notebook 07 empty-alert alignment require the explicit rollout below. Do not deploy notebook 05 before the audit migration: preflight verifies column names/types and fails before network retrieval or entity writes.

## Header, parsing and empty responses

The protobuf header is read before building any DataFrame. A request/decode with an initialized header, nonempty GTFS version and positive explicit feed timestamp can represent zero entities: SOURCE SUCCESS, entities_received=0, parsed_row_count=0, rows_appended=0, is_empty=true. No entity row is fabricated. The explicit timestamp requirement is this pipeline's snapshot-key policy; it is not a claim that every optional protobuf field is required by GTFS.

Incrementality follows the [official GTFS-Realtime reference](https://gtfs.org/documentation/realtime/reference/#enum-incrementality): omitted incrementality uses the protobuf FULL_DATASET default. Explicit DIFFERENTIAL is rejected as INVALID_FEED_METADATA, before any entity append. Deleted or unexpected entity types in a FULL feed are rejected as PARSE_ERROR rather than silently skipped. Full/differential mode is validated per fetched payload; historical Roman feed modes are not retroactively certified.

Zero parsed rows with nonzero entities is not labeled an empty feed. A TripUpdate entity may legitimately have no stop updates. entities_received now counts decoded protobuf entities, not distinct parsed entity IDs or stop rows. The separate parsed_row_count preserves this distinction.

NETWORK_ERROR, HTTP_ERROR, INCOMPLETE_READ, DECODE_ERROR, PARSE_ERROR, INVALID_FEED_METADATA, WRITE_ERROR and UNKNOWN_ERROR are stable categories. Original error_message is retained. Metadata known before failure remains available; unknown parsed/duplicate counts are NULL. The legacy non-null rows_appended=0 on a failure is not proof that a timed-out write could not commit.

## Additive audit migration — review and execute separately

Existing table: rome_transport.bronze.ingestion_runs. Preserve its seven existing columns:
run_timestamp TIMESTAMP, source_name STRING, feed_timestamp BIGINT, entities_received BIGINT, rows_appended BIGINT, status STRING, error_message STRING.

An authorized operator must apply the following additive migration before deploying the new writer. This SQL is documentation, not executed by CI or the notebook:

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

All sources are attempted even after a source failure. After terminal audit persistence, PARTIAL_SUCCESS and FAILED raise a task error so existing Job failure notifications can apply. No Job setting is changed. Scheduled retry/notification behavior must be validated in an authorized runtime test before rollout.

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

Before deployment: review migration, verify Spark schema compatibility, test empty/nonempty FULL and rejected DIFFERENTIAL payloads offline or in an approved isolated runtime, confirm append idempotency and audit behavior, validate notebook 07's empty-alert fixture, and verify task failure notifications. Do not execute the existing notebook 04 exploration cells as a deployment procedure. No new package is required by the runtime implementation.
