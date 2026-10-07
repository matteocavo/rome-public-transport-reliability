# Databricks notebook source
# DBTITLE 1,Environment Setup
# MAGIC %md
# MAGIC ## Environment Setup
# MAGIC
# MAGIC Imports the GTFS-Realtime protobuf decoder, PySpark schema types, and defines the three Rome GTFS-Realtime feed endpoints (trip updates, vehicle positions, service alerts). This notebook is designed to run automatically on a schedule to periodically pull fresh realtime data.

# COMMAND ----------

import urllib.request

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
# MAGIC `fetch_feed` downloads and decodes a GTFS-Realtime protobuf feed into a `FeedMessage` object. `append_new_rows` performs an anti-join against existing Delta table keys so only unseen snapshot rows are appended, making each pipeline run idempotent.

# COMMAND ----------

def fetch_feed(url):
    feed = gtfs_realtime_pb2.FeedMessage()

    with urllib.request.urlopen(url) as response:
        feed.ParseFromString(response.read())

    return feed


def append_new_rows(df, table_name, key_columns):
    if spark.catalog.tableExists(table_name):
        existing_keys = spark.table(table_name).select(*key_columns)

        new_df = df.join(
            existing_keys,
            key_columns,
            "left_anti"
        )
    else:
        new_df = df

    new_rows = new_df.count()

    if new_rows > 0:
        (
            new_df.write
            .format("delta")
            .mode("append")
            .saveAsTable(table_name)
        )

    return new_rows

# COMMAND ----------

# DBTITLE 1,Trip Updates Builder
# MAGIC %md
# MAGIC ## Trip Updates Builder
# MAGIC
# MAGIC Decodes the trip-updates feed into one row per stop-time update, extracting arrival/departure delays, timestamps, and schedule relationship. Produces a Spark DataFrame conforming to `trip_updates_schema`, ready for idempotent append to the bronze table.

# COMMAND ----------

def build_trip_updates_df():
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

def build_vehicle_positions_df():
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

def build_service_alerts_df():
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
# MAGIC Defines the `ingestion_runs` audit schema and `log_ingestion_run` function, which records each feed ingestion attempt (source, row counts, status, errors) to `rome_transport.bronze.ingestion_runs`. This provides full observability and auditability of every pipeline run.

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    TimestampType
)

ingestion_run_schema = StructType([
    StructField("run_timestamp", TimestampType(), False),
    StructField("source_name", StringType(), False),
    StructField("feed_timestamp", LongType(), True),
    StructField("entities_received", LongType(), False),
    StructField("rows_appended", LongType(), False),
    StructField("status", StringType(), False),
    StructField("error_message", StringType(), True),
])


def log_ingestion_run(
    source_name,
    feed_timestamp,
    entities_received,
    rows_appended,
    status="SUCCESS",
    error_message=None
):
    audit_data = [{
        "run_timestamp": datetime.now(timezone.utc),
        "source_name": source_name,
        "feed_timestamp": feed_timestamp,
        "entities_received": int(entities_received),
        "rows_appended": int(rows_appended),
        "status": status,
        "error_message": error_message
    }]

    audit_df = spark.createDataFrame(
        audit_data,
        schema=ingestion_run_schema
    )

    (
        audit_df.write
        .format("delta")
        .mode("append")
        .saveAsTable("rome_transport.bronze.ingestion_runs")
    )

# COMMAND ----------

# DBTITLE 1,Pipeline Orchestration
# MAGIC %md
# MAGIC ## Pipeline Orchestration
# MAGIC
# MAGIC `run_realtime_ingestion` orchestrates all three feeds sequentially — building DataFrames, appending new rows to their respective bronze Delta tables, and logging each step. Errors are caught per-feed so one failure does not block the others.

# COMMAND ----------

def run_realtime_ingestion():
    summary = {}

    # -------------------------
    # Trip Updates
    # -------------------------
    try:
        trip_updates_df = build_trip_updates_df()

        trip_updates_feed_timestamp = (
            trip_updates_df
            .select("feed_timestamp")
            .first()[0]
        )

        trip_updates_entities = (
            trip_updates_df
            .select("entity_id")
            .distinct()
            .count()
        )

        trip_updates_new = append_new_rows(
            trip_updates_df,
            "rome_transport.bronze.trip_updates",
            [
                "feed_timestamp",
                "entity_id",
                "stop_sequence",
                "stop_id"
            ]
        )

        log_ingestion_run(
            source_name="trip_updates",
            feed_timestamp=trip_updates_feed_timestamp,
            entities_received=trip_updates_entities,
            rows_appended=trip_updates_new,
            status="SUCCESS"
        )

        summary["trip_updates_new_rows"] = trip_updates_new

    except Exception as e:
        log_ingestion_run(
            source_name="trip_updates",
            feed_timestamp=None,
            entities_received=0,
            rows_appended=0,
            status="FAILED",
            error_message=str(e)
        )
        summary["trip_updates_error"] = str(e)

    # -------------------------
    # Vehicle Positions
    # -------------------------
    try:
        vehicle_positions_df = build_vehicle_positions_df()

        vehicle_feed_timestamp = (
            vehicle_positions_df
            .select("feed_timestamp")
            .first()[0]
        )

        vehicle_entities = vehicle_positions_df.count()

        vehicle_positions_new = append_new_rows(
            vehicle_positions_df,
            "rome_transport.bronze.vehicle_positions",
            [
                "feed_timestamp",
                "entity_id"
            ]
        )

        log_ingestion_run(
            source_name="vehicle_positions",
            feed_timestamp=vehicle_feed_timestamp,
            entities_received=vehicle_entities,
            rows_appended=vehicle_positions_new,
            status="SUCCESS"
        )

        summary["vehicle_positions_new_rows"] = vehicle_positions_new

    except Exception as e:
        log_ingestion_run(
            source_name="vehicle_positions",
            feed_timestamp=None,
            entities_received=0,
            rows_appended=0,
            status="FAILED",
            error_message=str(e)
        )
        summary["vehicle_positions_error"] = str(e)

    # -------------------------
    # Service Alerts
    # -------------------------
    try:
        service_alerts_df = build_service_alerts_df()

        alerts_feed_timestamp = (
            service_alerts_df
            .select("feed_timestamp")
            .first()[0]
        )

        alerts_entities = service_alerts_df.count()

        service_alerts_new = append_new_rows(
            service_alerts_df,
            "rome_transport.bronze.service_alerts",
            [
                "feed_timestamp",
                "alert_id"
            ]
        )

        log_ingestion_run(
            source_name="service_alerts",
            feed_timestamp=alerts_feed_timestamp,
            entities_received=alerts_entities,
            rows_appended=service_alerts_new,
            status="SUCCESS"
        )

        summary["service_alerts_new_rows"] = service_alerts_new

    except Exception as e:
        log_ingestion_run(
            source_name="service_alerts",
            feed_timestamp=None,
            entities_received=0,
            rows_appended=0,
            status="FAILED",
            error_message=str(e)
        )
        summary["service_alerts_error"] = str(e)

    return summary

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
