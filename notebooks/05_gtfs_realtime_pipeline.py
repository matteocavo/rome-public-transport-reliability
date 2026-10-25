# Databricks notebook source
# DBTITLE 1,Environment Setup
# MAGIC %md
# MAGIC ## Environment Setup
# MAGIC
# MAGIC Imports the GTFS-Realtime protobuf decoder, PySpark schema types, and defines the three Rome GTFS-Realtime feed endpoints (trip updates, vehicle positions, service alerts). This notebook is designed to run automatically on a schedule to periodically pull fresh realtime data.

# COMMAND ----------

import urllib.request
import urllib.error
from http.client import IncompleteRead
from urllib.error import HTTPError, URLError
from google.protobuf.message import DecodeError
from functools import reduce
import uuid
from pyspark.sql import functions as F

from datetime import datetime, timezone
from google.transit import gtfs_realtime_pb2

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    DoubleType,
    TimestampType,
    ArrayType
)

TRIP_UPDATES_URL = "https://romamobilita.it/sites/default/files/rome_rtgtfs_trip_updates_feed.pb"
VEHICLE_POSITIONS_URL = "https://romamobilita.it/sites/default/files/rome_rtgtfs_vehicle_positions_feed.pb"
SERVICE_ALERTS_URL = "https://romamobilita.it/sites/default/files/rome_rtgtfs_service_alerts_feed.pb"

# COMMAND ----------

# DBTITLE 1,Spark Schemas
# MAGIC %md
# MAGIC ## Spark Schemas
# MAGIC
# MAGIC Defines explicit PySpark `StructType` schemas for trip updates, vehicle positions, and service alerts. Strict schemas ensure protobuf fields are consistently typed when written to Delta tables, preventing schema drift across feed snapshots.

# COMMAND ----------

trip_updates_schema = StructType([
    StructField("feed_timestamp", LongType(), False),
    StructField("entity_id", StringType(), False),

    StructField("trip_id", StringType(), True),
    StructField("route_id", StringType(), True),
    StructField("start_date", StringType(), True),
    StructField("start_time", StringType(), True),
    StructField("direction_id", IntegerType(), True),
    StructField("vehicle_id", StringType(), True),

    StructField("stop_sequence", IntegerType(), True),
    StructField("stop_id", StringType(), True),

    StructField("arrival_delay", IntegerType(), True),
    StructField("arrival_time", LongType(), True),
    StructField("arrival_uncertainty", IntegerType(), True),

    StructField("departure_delay", IntegerType(), True),
    StructField("departure_time", LongType(), True),
    StructField("departure_uncertainty", IntegerType(), True),

    StructField("schedule_relationship", StringType(), True),

    StructField("_source_url", StringType(), False),
    StructField("_ingestion_timestamp", TimestampType(), False),
])

# COMMAND ----------

vehicle_positions_schema = StructType([
    StructField("feed_timestamp", LongType(), False),
    StructField("entity_id", StringType(), False),

    StructField("trip_id", StringType(), True),
    StructField("route_id", StringType(), True),
    StructField("start_date", StringType(), True),
    StructField("start_time", StringType(), True),
    StructField("direction_id", IntegerType(), True),

    StructField("vehicle_id", StringType(), True),

    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("bearing", DoubleType(), True),
    StructField("speed", DoubleType(), True),
    StructField("odometer", DoubleType(), True),

    StructField("current_stop_sequence", IntegerType(), True),
    StructField("current_status", StringType(), True),
    StructField("stop_id", StringType(), True),
    StructField("vehicle_timestamp", LongType(), True),

    StructField("_source_url", StringType(), False),
    StructField("_ingestion_timestamp", TimestampType(), False),
])

# COMMAND ----------

active_period_schema = StructType([
    StructField("start", LongType(), True),
    StructField("end", LongType(), True),
])

informed_entity_schema = StructType([
    StructField("agency_id", StringType(), True),
    StructField("route_id", StringType(), True),
    StructField("route_type", IntegerType(), True),
    StructField("trip_id", StringType(), True),
    StructField("stop_id", StringType(), True),
    StructField("direction_id", IntegerType(), True),
])

translation_schema = StructType([
    StructField("text", StringType(), True),
    StructField("language", StringType(), True),
])

service_alerts_schema = StructType([
    StructField("feed_timestamp", LongType(), False),
    StructField("alert_id", StringType(), False),

    StructField("cause", StringType(), True),
    StructField("effect", StringType(), True),

    StructField(
        "active_periods",
        ArrayType(active_period_schema),
        True
    ),

    StructField(
        "informed_entities",
        ArrayType(informed_entity_schema),
        True
    ),

    StructField(
        "header_translations",
        ArrayType(translation_schema),
        True
    ),

    StructField(
        "description_translations",
        ArrayType(translation_schema),
        True
    ),

    StructField("_source_url", StringType(), False),
    StructField("_ingestion_timestamp", TimestampType(), False),
])

# COMMAND ----------

# DBTITLE 1,Feed Retrieval & Idempotent Append Logic
# MAGIC %md
# MAGIC ## Feed Retrieval & Idempotent Append Logic
# MAGIC
# MAGIC Header metadata is validated independently of rows, including empty full snapshots. DIFFERENTIAL is rejected rather than misread as full state. Single-writer retries use null-safe anti-joins.
# MAGIC
# MAGIC `fetch_feed` downloads and decodes a GTFS-Realtime protobuf feed into a `FeedMessage` object. `append_new_rows` performs an anti-join against existing Delta table keys so only unseen snapshot rows are appended, making each pipeline run idempotent.

# COMMAND ----------

class IngestionError(RuntimeError):
    def __init__(self, error_type, message, metadata=None):
        super().__init__(message)
        self.error_type = error_type
        self.metadata = metadata or {}


def error_type(exc):
    if isinstance(exc, IngestionError):
        return exc.error_type
    if isinstance(exc, HTTPError):
        return "HTTP_ERROR"
    if isinstance(exc, IncompleteRead):
        return "INCOMPLETE_READ"
    if isinstance(exc, (URLError, TimeoutError, ConnectionError)):
        return "NETWORK_ERROR"
    if isinstance(exc, DecodeError):
        return "DECODE_ERROR"
    return "UNKNOWN_ERROR"


def fetch_feed(url):
    feed = gtfs_realtime_pb2.FeedMessage()
    with urllib.request.urlopen(url, timeout=60) as response:
        payload = response.read()
    feed.ParseFromString(payload)
    return feed


def feed_metadata(feed):
    # Incrementality defaults to FULL_DATASET when omitted, per the protobuf contract.
    metadata = {"decode_status": "DECODED", "entities_received": len(feed.entity),
                "is_empty": len(feed.entity) == 0}
    if not feed.HasField("header"):
        raise IngestionError("INVALID_FEED_METADATA", "Missing feed header", metadata)
    header = feed.header
    if header.HasField("timestamp"):
        metadata["feed_timestamp"] = int(header.timestamp)
    incrementality = int(header.incrementality)
    metadata["feed_incrementality"] = {0: "FULL_DATASET", 1: "DIFFERENTIAL"}.get(
        incrementality, "UNKNOWN")
    if (not feed.IsInitialized() or not header.gtfs_realtime_version
            or not header.HasField("timestamp") or header.timestamp <= 0):
        raise IngestionError("INVALID_FEED_METADATA", "Incomplete header or invalid timestamp", metadata)
    if incrementality != 0:
        raise IngestionError("INVALID_FEED_METADATA",
                             "Only FULL_DATASET is supported; differential feeds are not snapshots", metadata)
    return metadata


def validate_entities(feed, source_name):
    expected = {"trip_updates": "trip_update", "vehicle_positions": "vehicle",
                "service_alerts": "alert"}[source_name]
    for entity in feed.entity:
        if entity.is_deleted or not entity.HasField(expected):
            raise IngestionError("PARSE_ERROR", "Deleted or unexpected entity in full snapshot")


def append_new_rows(df, table_name, key_columns):
    # Collapse exact repeated records, but never arbitrarily select conflicting payloads.
    incoming = df.dropDuplicates()
    if incoming.groupBy(*key_columns).count().filter(F.col("count") > 1).limit(1).count():
        raise IngestionError("PARSE_ERROR", "Conflicting rows for the same snapshot key")
    if spark.catalog.tableExists(table_name):
        existing = spark.table(table_name).select(*key_columns).dropDuplicates().alias("existing")
        incoming = incoming.alias("incoming")
        condition = reduce(lambda a, b: a & b, [
            F.col("incoming." + key).eqNullSafe(F.col("existing." + key)) for key in key_columns])
        new_df = incoming.join(existing, condition, "left_anti")
    else:
        new_df = incoming
    new_rows = new_df.count()
    if new_rows > 0:
        new_df.write.format("delta").mode("append").saveAsTable(table_name)
    return new_rows


# COMMAND ----------

# DBTITLE 1,Trip Updates Builder
# MAGIC %md
# MAGIC ## Trip Updates Builder
# MAGIC
# MAGIC Decodes the trip-updates feed into one row per stop-time update, extracting arrival/departure delays, timestamps, and schedule relationship. Produces a Spark DataFrame conforming to `trip_updates_schema`, ready for idempotent append to the bronze table.

# COMMAND ----------

def build_trip_updates_df(feed=None):
    if feed is None:
        feed = fetch_feed(TRIP_UPDATES_URL)
    ingestion_timestamp = datetime.now(timezone.utc)

    rows = []

    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue

        tu = entity.trip_update
        trip = tu.trip

        vehicle_id = tu.vehicle.id if tu.HasField("vehicle") else None

        for stu in tu.stop_time_update:
            arrival_delay = None
            arrival_time = None
            arrival_uncertainty = None

            if stu.HasField("arrival"):
                if stu.arrival.HasField("delay"):
                    arrival_delay = stu.arrival.delay

                if stu.arrival.HasField("time"):
                    arrival_time = stu.arrival.time

                if stu.arrival.HasField("uncertainty"):
                    arrival_uncertainty = stu.arrival.uncertainty

            departure_delay = None
            departure_time = None
            departure_uncertainty = None

            if stu.HasField("departure"):
                if stu.departure.HasField("delay"):
                    departure_delay = stu.departure.delay

                if stu.departure.HasField("time"):
                    departure_time = stu.departure.time

                if stu.departure.HasField("uncertainty"):
                    departure_uncertainty = stu.departure.uncertainty

            try:
                schedule_relationship = (
                    gtfs_realtime_pb2.TripUpdate.StopTimeUpdate
                    .ScheduleRelationship.Name(stu.schedule_relationship)
                )
            except ValueError:
                schedule_relationship = None

            rows.append({
                "feed_timestamp": feed.header.timestamp,
                "entity_id": entity.id,

                "trip_id": trip.trip_id,
                "route_id": trip.route_id,
                "start_date": trip.start_date,
                "start_time": trip.start_time,
                "direction_id": trip.direction_id,
                "vehicle_id": vehicle_id,

                "stop_sequence": stu.stop_sequence,
                "stop_id": stu.stop_id,

                "arrival_delay": arrival_delay,
                "arrival_time": arrival_time,
                "arrival_uncertainty": arrival_uncertainty,

                "departure_delay": departure_delay,
                "departure_time": departure_time,
                "departure_uncertainty": departure_uncertainty,

                "schedule_relationship": schedule_relationship,

                "_source_url": TRIP_UPDATES_URL,
                "_ingestion_timestamp": ingestion_timestamp
            })

    return spark.createDataFrame(
        rows,
        schema=trip_updates_schema
    )

# COMMAND ----------

# DBTITLE 1,Vehicle Positions Builder
# MAGIC %md
# MAGIC ## Vehicle Positions Builder
# MAGIC
# MAGIC Decodes the vehicle-positions feed into one row per active vehicle, capturing GPS coordinates, bearing, speed, and current stop status. Produces a Spark DataFrame conforming to `vehicle_positions_schema` for the bronze layer.

# COMMAND ----------

def build_vehicle_positions_df(feed=None):
    if feed is None:
        feed = fetch_feed(VEHICLE_POSITIONS_URL)
    ingestion_timestamp = datetime.now(timezone.utc)

    rows = []

    for entity in feed.entity:
        if not entity.HasField("vehicle"):
            continue

        v = entity.vehicle

        trip_id = v.trip.trip_id if v.HasField("trip") else None
        route_id = v.trip.route_id if v.HasField("trip") else None
        start_date = v.trip.start_date if v.HasField("trip") else None
        start_time = v.trip.start_time if v.HasField("trip") else None
        direction_id = v.trip.direction_id if v.HasField("trip") else None

        vehicle_id = (
            v.vehicle.id
            if v.HasField("vehicle")
            else None
        )

        latitude = None
        longitude = None
        bearing = None
        speed = None
        odometer = None

        if v.HasField("position"):
            latitude = v.position.latitude
            longitude = v.position.longitude

            if v.position.HasField("bearing"):
                bearing = v.position.bearing

            if v.position.HasField("speed"):
                speed = v.position.speed

            if v.position.HasField("odometer"):
                odometer = v.position.odometer

        try:
            current_status = (
                gtfs_realtime_pb2.VehiclePosition
                .VehicleStopStatus.Name(v.current_status)
            )
        except ValueError:
            current_status = None

        rows.append({
            "feed_timestamp": feed.header.timestamp,
            "entity_id": entity.id,

            "trip_id": trip_id,
            "route_id": route_id,
            "start_date": start_date,
            "start_time": start_time,
            "direction_id": direction_id,

            "vehicle_id": vehicle_id,

            "latitude": latitude,
            "longitude": longitude,
            "bearing": bearing,
            "speed": speed,
            "odometer": odometer,

            "current_stop_sequence": v.current_stop_sequence,
            "current_status": current_status,
            "stop_id": v.stop_id,
            "vehicle_timestamp": v.timestamp,

            "_source_url": VEHICLE_POSITIONS_URL,
            "_ingestion_timestamp": ingestion_timestamp
        })

    return spark.createDataFrame(
        rows,
        schema=vehicle_positions_schema
    )

# COMMAND ----------

# DBTITLE 1,Service Alerts Builder
# MAGIC %md
# MAGIC ## Service Alerts Builder
# MAGIC
# MAGIC Decodes the service-alerts feed into one row per alert, including cause, effect, active periods, informed entities, and translated header/description text. Produces a Spark DataFrame conforming to `service_alerts_schema` for the bronze layer.

# COMMAND ----------

def build_service_alerts_df(feed=None):
    if feed is None:
        feed = fetch_feed(SERVICE_ALERTS_URL)
    ingestion_timestamp = datetime.now(timezone.utc)

    rows = []

    for entity in feed.entity:
        if not entity.HasField("alert"):
            continue

        alert = entity.alert

        active_periods = []

        for period in alert.active_period:
            active_periods.append({
                "start": (
                    period.start
                    if period.HasField("start")
                    else None
                ),
                "end": (
                    period.end
                    if period.HasField("end")
                    else None
                )
            })

        informed_entities = []

        for ie in alert.informed_entity:
            trip_id = (
                ie.trip.trip_id
                if ie.HasField("trip")
                else None
            )

            informed_entities.append({
                "agency_id": (
                    ie.agency_id
                    if ie.agency_id
                    else None
                ),
                "route_id": (
                    ie.route_id
                    if ie.route_id
                    else None
                ),
                "route_type": (
                    ie.route_type
                    if ie.HasField("route_type")
                    else None
                ),
                "trip_id": trip_id,
                "stop_id": (
                    ie.stop_id
                    if ie.stop_id
                    else None
                ),
                "direction_id": (
                    ie.direction_id
                    if ie.HasField("direction_id")
                    else None
                )
            })

        header_translations = [
            {
                "text": t.text,
                "language": t.language if t.language else None
            }
            for t in alert.header_text.translation
        ]

        description_translations = [
            {
                "text": t.text,
                "language": t.language if t.language else None
            }
            for t in alert.description_text.translation
        ]

        cause = (
            gtfs_realtime_pb2.Alert.Cause.Name(alert.cause)
            if alert.HasField("cause")
            else None
        )

        effect = (
            gtfs_realtime_pb2.Alert.Effect.Name(alert.effect)
            if alert.HasField("effect")
            else None
        )

        rows.append({
            "feed_timestamp": feed.header.timestamp,
            "alert_id": entity.id,

            "cause": cause,
            "effect": effect,

            "active_periods": active_periods,
            "informed_entities": informed_entities,
            "header_translations": header_translations,
            "description_translations": description_translations,

            "_source_url": SERVICE_ALERTS_URL,
            "_ingestion_timestamp": ingestion_timestamp
        })

    return spark.createDataFrame(
        rows,
        schema=service_alerts_schema
    )

# COMMAND ----------

# DBTITLE 1,Ingestion Audit Logging
# MAGIC %md
# MAGIC ## Ingestion Audit Logging
# MAGIC
# MAGIC Append SOURCE outcomes and BATCH start/final records to ingestion_runs. Legacy columns remain; additive fields require an explicitly approved migration before deployment. Source failures do not stop attempts on other feeds, but make the task fail after audit finalization. No schema migration or job change is executed here.

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    TimestampType
)

# Additive migration is deliberately not performed by this notebook. See docs/INGESTION_AUDIT.md.
AUDIT_TABLE = "rome_transport.bronze.ingestion_runs"
AUDIT_FIELDS = [
    ("run_timestamp", "timestamp"), ("source_name", "string"), ("feed_timestamp", "bigint"),
    ("entities_received", "bigint"), ("rows_appended", "bigint"), ("status", "string"),
    ("error_message", "string"), ("record_type", "string"),
    ("ingestion_batch_id", "string"), ("ingestion_attempt_id", "string"),
    ("parsed_row_count", "bigint"), ("duplicate_row_count", "bigint"),
    ("is_empty", "boolean"), ("feed_incrementality", "string"),
    ("decode_status", "string"), ("error_type", "string"),
]
AUDIT_SCHEMA = ", ".join(name + " " + dtype for name, dtype in AUDIT_FIELDS)


def require_audit_schema():
    if not spark.catalog.tableExists(AUDIT_TABLE):
        raise RuntimeError("Audit table missing: apply the reviewed migration before ingestion")
    actual = {field.name: field.dataType.simpleString() for field in spark.table(AUDIT_TABLE).schema.fields}
    if any(actual.get(name) != dtype for name, dtype in AUDIT_FIELDS):
        raise RuntimeError("Audit migration required; no feeds fetched and no Bronze writes attempted")


def log_ingestion_run(record):
    values = dict(record)
    values.setdefault("run_timestamp", datetime.now(timezone.utc))
    values.setdefault("entities_received", 0)
    values.setdefault("rows_appended", 0)
    spark.createDataFrame([values], AUDIT_SCHEMA).write.format("delta").mode("append").saveAsTable(AUDIT_TABLE)


def batch_status(statuses, infrastructure_failure=False):
    if infrastructure_failure or not statuses or all(s == "FAILED" for s in statuses):
        return "FAILED"
    return "SUCCESS" if all(s == "SUCCESS" for s in statuses) else "PARTIAL_SUCCESS"


def ingest_source(source_name, url, builder, keys):
    metadata = {"decode_status": "NOT_DECODED"}
    try:
        feed = fetch_feed(url)
        metadata.update(feed_metadata(feed))
        validate_entities(feed, source_name)
        try:
            df = builder(feed)
            metadata["parsed_row_count"] = df.count()
        except Exception as exc:
            raise IngestionError("PARSE_ERROR", str(exc), metadata) from exc
        try:
            inserted = append_new_rows(df, "rome_transport.bronze." + source_name, keys)
        except IngestionError:
            raise
        except Exception as exc:
            # A write can commit before the client sees an error: inserted count is unknown on failure.
            raise IngestionError("WRITE_ERROR", str(exc), metadata) from exc
        metadata.update(rows_appended=inserted,
                        duplicate_row_count=metadata["parsed_row_count"] - inserted)
        return metadata
    except Exception as exc:
        metadata.update(getattr(exc, "metadata", {}))
        raise IngestionError(error_type(exc), str(exc), metadata) from exc


def run_ingestion_attempt(sources, process, audit, ingestion_batch_id=None):
    # Supply the same logical batch ID for explicit retries. Attempt IDs are always new.
    batch_id = ingestion_batch_id or str(uuid.uuid4())
    attempt_id = str(uuid.uuid4())
    common = {"ingestion_batch_id": batch_id, "ingestion_attempt_id": attempt_id}
    audit(dict(common, record_type="BATCH", source_name="__batch__", status="RUNNING"))
    statuses, audit_errors, outcomes = [], [], {}
    for source in sources:
        record = dict(common, record_type="SOURCE", source_name=source, status="SUCCESS")
        try:
            record.update(process(source))
        except Exception as exc:
            record.update(getattr(exc, "metadata", {}))
            record.update(status="FAILED", error_type=error_type(exc), error_message=str(exc))
        statuses.append(record["status"])
        outcomes[source] = record
        try:
            audit(record)
        except Exception as exc:
            # Still attempt the remaining feeds; never hide an audit-storage failure.
            audit_errors.append(source + ": " + str(exc))
    status = batch_status(statuses, bool(audit_errors))
    final = dict(common, record_type="BATCH", source_name="__batch__", status=status)
    if audit_errors:
        final.update(error_type="WRITE_ERROR", error_message="; ".join(audit_errors))
    audit(final)  # If finalization fails, propagate; never report task success.
    if status != "SUCCESS":
        raise RuntimeError("Ingestion attempt " + attempt_id + " ended " + status
                           + "; inspect persisted SOURCE/BATCH audit records")
    return {"ingestion_batch_id": batch_id, "ingestion_attempt_id": attempt_id,
            "status": status, "sources": outcomes}


def run_realtime_ingestion(ingestion_batch_id=None):
    require_audit_schema()  # Fail before network or data writes when migration is missing.
    sources = {
        "trip_updates": (TRIP_UPDATES_URL, build_trip_updates_df,
                         ["feed_timestamp", "entity_id", "stop_sequence", "stop_id"]),
        "vehicle_positions": (VEHICLE_POSITIONS_URL, build_vehicle_positions_df,
                              ["feed_timestamp", "entity_id"]),
        "service_alerts": (SERVICE_ALERTS_URL, build_service_alerts_df,
                           ["feed_timestamp", "alert_id"]),
    }
    return run_ingestion_attempt(sources, lambda name: ingest_source(name, *sources[name]),
                                 log_ingestion_run, ingestion_batch_id)

# COMMAND ----------

# DBTITLE 1,Bronze Table Verification
# MAGIC %md
# MAGIC ## Bronze Table Verification
# MAGIC
# MAGIC Quick count queries against the three bronze Delta tables to verify accumulated row counts, distinct feed snapshots, and entity cardinalities. Useful for sanity-checking data before and after each ingestion run.

# COMMAND ----------

print("TRIP UPDATES")
spark.sql("""
SELECT
    COUNT(*) AS rows,
    COUNT(DISTINCT feed_timestamp) AS snapshots,
    COUNT(DISTINCT trip_id) AS trips,
    COUNT(DISTINCT vehicle_id) AS vehicles
FROM rome_transport.bronze.trip_updates
""").show()

print("VEHICLE POSITIONS")
spark.sql("""
SELECT
    COUNT(*) AS rows,
    COUNT(DISTINCT feed_timestamp) AS snapshots,
    COUNT(DISTINCT trip_id) AS trips,
    COUNT(DISTINCT vehicle_id) AS vehicles
FROM rome_transport.bronze.vehicle_positions
""").show()

print("SERVICE ALERTS")
spark.sql("""
SELECT
    COUNT(*) AS rows,
    COUNT(DISTINCT feed_timestamp) AS snapshots,
    COUNT(DISTINCT alert_id) AS alerts
FROM rome_transport.bronze.service_alerts
""").show()

# COMMAND ----------

# DBTITLE 1,Scheduled Pipeline Execution
# MAGIC %md
# MAGIC ## Scheduled Pipeline Execution
# MAGIC
# MAGIC Triggers a single ingestion cycle and then reviews the latest `ingestion_runs` audit entries. In production, this notebook runs on a recurring schedule to continuously collect GTFS-Realtime snapshots throughout the day.

# COMMAND ----------

result = run_realtime_ingestion()

print("Realtime ingestion completed")
print(result)

# COMMAND ----------

spark.sql("""
SELECT *
FROM rome_transport.bronze.ingestion_runs
ORDER BY run_timestamp DESC
""").show(truncate=False)
