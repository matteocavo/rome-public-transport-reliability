# Databricks notebook source
# DBTITLE 1,GTFS-Realtime Feed Endpoints
# MAGIC %md
# MAGIC ## GTFS-Realtime Feed Endpoints
# MAGIC
# MAGIC Defines the three Rome GTFS-Realtime protobuf feed URLs (trip updates, vehicle positions, service alerts) used as data sources throughout the notebook.

# COMMAND ----------

TRIP_UPDATES_URL = "https://romamobilita.it/sites/default/files/rome_rtgtfs_trip_updates_feed.pb"
VEHICLE_POSITIONS_URL = "https://romamobilita.it/sites/default/files/rome_rtgtfs_vehicle_positions_feed.pb"
SERVICE_ALERTS_URL = "https://romamobilita.it/sites/default/files/rome_rtgtfs_service_alerts_feed.pb"

# COMMAND ----------

# DBTITLE 1,Environment Setup
# MAGIC %md
# MAGIC ## Environment Setup
# MAGIC
# MAGIC Restarts the Python kernel to ensure the `protobuf` library and GTFS-RT bindings are cleanly loaded before parsing feeds.

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,GTFS-Realtime Dependencies
# MAGIC %md
# MAGIC ## GTFS-Realtime Dependencies
# MAGIC
# MAGIC Verifies the installed protobuf version and confirms that the official GTFS-Realtime protocol buffer bindings import correctly.

# COMMAND ----------

import google.protobuf
print("protobuf version:", google.protobuf.__version__)

# COMMAND ----------

from google.transit import gtfs_realtime_pb2

print("GTFS-Realtime bindings loaded correctly")

# COMMAND ----------

# DBTITLE 1,Trip Updates — Feed Inspection
# MAGIC %md
# MAGIC ## Trip Updates — Feed Inspection
# MAGIC
# MAGIC Fetches the live trip updates feed and inspects entity structure, sample records, and field coverage to validate the data shape before schema design. This is an exploratory profiling step, not part of the production pipeline.

# COMMAND ----------

import urllib.request
from google.transit import gtfs_realtime_pb2

feed = gtfs_realtime_pb2.FeedMessage()

with urllib.request.urlopen(TRIP_UPDATES_URL) as response:
    feed.ParseFromString(response.read())

print("Entities:", len(feed.entity))
print("Feed timestamp:", feed.header.timestamp)

# COMMAND ----------

for entity in feed.entity[:3]:
    print(entity)
    print("-" * 100)

# COMMAND ----------

sample = []

for entity in feed.entity[:50]:
    if entity.HasField("trip_update"):
        tu = entity.trip_update

        sample.append({
            "entity_id": entity.id,
            "trip_id": tu.trip.trip_id,
            "route_id": tu.trip.route_id,
            "start_date": tu.trip.start_date,
            "start_time": tu.trip.start_time,
            "direction_id": tu.trip.direction_id,
            "vehicle_id": tu.vehicle.id if tu.HasField("vehicle") else None,
            "num_stop_updates": len(tu.stop_time_update)
        })

display(spark.createDataFrame(sample))

# COMMAND ----------

total_entities = 0
with_vehicle = 0
with_arrival_delay = 0
with_departure_delay = 0
total_stop_updates = 0

for entity in feed.entity:
    if not entity.HasField("trip_update"):
        continue

    total_entities += 1
    tu = entity.trip_update

    if tu.HasField("vehicle"):
        with_vehicle += 1

    for stu in tu.stop_time_update:
        total_stop_updates += 1

        if stu.HasField("arrival") and stu.arrival.HasField("delay"):
            with_arrival_delay += 1

        if stu.HasField("departure") and stu.departure.HasField("delay"):
            with_departure_delay += 1

print("Trip update entities:", total_entities)
print("Entities with vehicle:", with_vehicle)
print("Total stop_time_updates:", total_stop_updates)
print("With arrival delay:", with_arrival_delay)
print("With departure delay:", with_departure_delay)

# COMMAND ----------

# DBTITLE 1,Trip Updates — Bronze Schema Design & Parsing
# MAGIC %md
# MAGIC ## Trip Updates — Bronze Schema Design & Parsing
# MAGIC
# MAGIC Defines an explicit Spark schema for trip updates, then parses the protobuf feed into typed rows including arrival/departure delays and schedule relationships. Produces `trip_updates_df`.

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    TimestampType
)

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

from datetime import datetime, timezone

rows = []

ingestion_timestamp = datetime.now(timezone.utc)

for entity in feed.entity:

    if not entity.HasField("trip_update"):
        continue

    tu = entity.trip_update
    trip = tu.trip

    vehicle_id = (
        tu.vehicle.id
        if tu.HasField("vehicle")
        else None
    )

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

        schedule_relationship = (
            gtfs_realtime_pb2.TripUpdate.StopTimeUpdate
            .ScheduleRelationship.Name(stu.schedule_relationship)
        )

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

# COMMAND ----------

trip_updates_df = spark.createDataFrame(
    rows,
    schema=trip_updates_schema
)

print("Rows:", trip_updates_df.count())

display(trip_updates_df)

# COMMAND ----------

# DBTITLE 1,Trip Updates — Initial Bronze Write
# MAGIC %md
# MAGIC ## Trip Updates — Initial Bronze Write
# MAGIC
# MAGIC Appends the parsed trip updates to the `rome_transport.bronze.trip_updates` Delta table and validates row, snapshot, trip, and vehicle counts.

# COMMAND ----------

(
    trip_updates_df.write
    .format("delta")
    .mode("append")
    .saveAsTable("rome_transport.bronze.trip_updates")
)

# COMMAND ----------

spark.table("rome_transport.bronze.trip_updates").count()

# COMMAND ----------

spark.sql("""
SELECT
    COUNT(*) AS rows,
    COUNT(DISTINCT feed_timestamp) AS snapshots,
    COUNT(DISTINCT trip_id) AS trips,
    COUNT(DISTINCT vehicle_id) AS vehicles
FROM rome_transport.bronze.trip_updates
""").show()

# COMMAND ----------

# DBTITLE 1,Vehicle Positions — Feed Inspection
# MAGIC %md
# MAGIC ## Vehicle Positions — Feed Inspection
# MAGIC
# MAGIC Fetches the live vehicle positions feed and profiles field availability (GPS, bearing, speed, vehicle/trip IDs) to guide schema design. This is an exploratory profiling step.

# COMMAND ----------

vehicle_feed = gtfs_realtime_pb2.FeedMessage()

with urllib.request.urlopen(VEHICLE_POSITIONS_URL) as response:
    vehicle_feed.ParseFromString(response.read())

print("Entities:", len(vehicle_feed.entity))
print("Feed timestamp:", vehicle_feed.header.timestamp)

# COMMAND ----------

for entity in vehicle_feed.entity[:3]:
    print(entity)
    print("-" * 100)

# COMMAND ----------

vehicle_profile = {
    "entities": 0,
    "with_vehicle_id": 0,
    "with_trip_id": 0,
    "with_stop_id": 0,
    "with_position": 0,
    "with_bearing": 0,
    "with_speed": 0,
    "with_odometer": 0,
    "with_timestamp": 0
}

for entity in vehicle_feed.entity:
    if not entity.HasField("vehicle"):
        continue

    v = entity.vehicle
    vehicle_profile["entities"] += 1

    if v.HasField("vehicle") and v.vehicle.id:
        vehicle_profile["with_vehicle_id"] += 1

    if v.HasField("trip") and v.trip.trip_id:
        vehicle_profile["with_trip_id"] += 1

    if v.stop_id:
        vehicle_profile["with_stop_id"] += 1

    if v.HasField("position"):
        vehicle_profile["with_position"] += 1

        if v.position.HasField("bearing"):
            vehicle_profile["with_bearing"] += 1

        if v.position.HasField("speed"):
            vehicle_profile["with_speed"] += 1

        if v.position.HasField("odometer"):
            vehicle_profile["with_odometer"] += 1

    if v.timestamp:
        vehicle_profile["with_timestamp"] += 1

vehicle_profile

# COMMAND ----------

# DBTITLE 1,Vehicle Positions — Bronze Schema Design & Parsing
# MAGIC %md
# MAGIC ## Vehicle Positions — Bronze Schema Design & Parsing
# MAGIC
# MAGIC Defines the schema for vehicle positions and parses the feed into typed rows with GPS coordinates, status, and trip association. Produces `vehicle_positions_df`.

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType,
    LongType, DoubleType, TimestampType
)

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

from datetime import datetime, timezone

vehicle_rows = []
ingestion_timestamp = datetime.now(timezone.utc)

for entity in vehicle_feed.entity:

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

    vehicle_rows.append({
        "feed_timestamp": vehicle_feed.header.timestamp,
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

# COMMAND ----------

vehicle_positions_df = spark.createDataFrame(
    vehicle_rows,
    schema=vehicle_positions_schema
)

print("Rows:", vehicle_positions_df.count())

display(vehicle_positions_df)

# COMMAND ----------

# DBTITLE 1,Vehicle Positions — Initial Bronze Write
# MAGIC %md
# MAGIC ## Vehicle Positions — Initial Bronze Write
# MAGIC
# MAGIC Appends the parsed vehicle positions to the `rome_transport.bronze.vehicle_positions` Delta table and validates row and snapshot counts.

# COMMAND ----------

(
    vehicle_positions_df.write
    .format("delta")
    .mode("append")
    .saveAsTable("rome_transport.bronze.vehicle_positions")
)

# COMMAND ----------

spark.sql("""
SELECT
    COUNT(*) AS rows,
    COUNT(DISTINCT feed_timestamp) AS snapshots,
    COUNT(DISTINCT trip_id) AS trips,
    COUNT(DISTINCT vehicle_id) AS vehicles
FROM rome_transport.bronze.vehicle_positions
""").show()

# COMMAND ----------

# DBTITLE 1,Service Alerts — Feed Inspection
# MAGIC %md
# MAGIC ## Service Alerts — Feed Inspection
# MAGIC
# MAGIC Fetches the live service alerts feed and profiles alert structure including active periods, informed entities, cause/effect, and translated text. This is an exploratory profiling step.

# COMMAND ----------

alerts_feed = gtfs_realtime_pb2.FeedMessage()

with urllib.request.urlopen(SERVICE_ALERTS_URL) as response:
    alerts_feed.ParseFromString(response.read())

print("Entities:", len(alerts_feed.entity))
print("Feed timestamp:", alerts_feed.header.timestamp)

# COMMAND ----------

for entity in alerts_feed.entity[:3]:
    print(entity)
    print("-" * 100)

# COMMAND ----------

alerts_profile = {
    "alerts": 0,
    "multiple_active_periods": 0,
    "multiple_informed_entities": 0,
    "with_route_id": 0,
    "with_stop_id": 0,
    "with_trip": 0,
    "with_cause": 0,
    "with_effect": 0,
    "with_header_text": 0,
    "with_description_text": 0
}

max_active_periods = 0
max_informed_entities = 0

for entity in alerts_feed.entity:
    if not entity.HasField("alert"):
        continue

    alert = entity.alert
    alerts_profile["alerts"] += 1

    n_periods = len(alert.active_period)
    n_entities = len(alert.informed_entity)

    max_active_periods = max(max_active_periods, n_periods)
    max_informed_entities = max(max_informed_entities, n_entities)

    if n_periods > 1:
        alerts_profile["multiple_active_periods"] += 1

    if n_entities > 1:
        alerts_profile["multiple_informed_entities"] += 1

    if any(x.route_id for x in alert.informed_entity):
        alerts_profile["with_route_id"] += 1

    if any(x.stop_id for x in alert.informed_entity):
        alerts_profile["with_stop_id"] += 1

    if any(x.HasField("trip") for x in alert.informed_entity):
        alerts_profile["with_trip"] += 1

    if alert.HasField("cause"):
        alerts_profile["with_cause"] += 1

    if alert.HasField("effect"):
        alerts_profile["with_effect"] += 1

    if len(alert.header_text.translation) > 0:
        alerts_profile["with_header_text"] += 1

    if len(alert.description_text.translation) > 0:
        alerts_profile["with_description_text"] += 1

print(alerts_profile)
print("Max active periods per alert:", max_active_periods)
print("Max informed entities per alert:", max_informed_entities)

# COMMAND ----------

# DBTITLE 1,Service Alerts — Bronze Schema Design
# MAGIC %md
# MAGIC ## Service Alerts — Bronze Schema Design
# MAGIC
# MAGIC Defines a nested Spark schema for service alerts with arrays for active periods, informed entities, and header/description translations.

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType,
    LongType, TimestampType,
    ArrayType
)

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

# DBTITLE 1,Service Alerts — Protobuf Parsing
# MAGIC %md
# MAGIC ## Service Alerts — Protobuf Parsing
# MAGIC
# MAGIC Parses the alerts feed into structured rows matching the nested schema, extracting active periods, informed entities, and translated text. Produces `service_alerts_df`.

# COMMAND ----------

from datetime import datetime, timezone

alert_rows = []
ingestion_timestamp = datetime.now(timezone.utc)

for entity in alerts_feed.entity:

    if not entity.HasField("alert"):
        continue

    alert = entity.alert

    active_periods = []

    for period in alert.active_period:
        active_periods.append({
            "start": period.start if period.HasField("start") else None,
            "end": period.end if period.HasField("end") else None
        })

    informed_entities = []

    for ie in alert.informed_entity:

        trip_id = None

        if ie.HasField("trip"):
            trip_id = ie.trip.trip_id

        informed_entities.append({
            "agency_id": ie.agency_id if ie.agency_id else None,
            "route_id": ie.route_id if ie.route_id else None,
            "route_type": (
                ie.route_type
                if ie.HasField("route_type")
                else None
            ),
            "trip_id": trip_id,
            "stop_id": ie.stop_id if ie.stop_id else None,
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

    alert_rows.append({
        "feed_timestamp": alerts_feed.header.timestamp,
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

# COMMAND ----------

service_alerts_df = spark.createDataFrame(
    alert_rows,
    schema=service_alerts_schema
)

print("Rows:", service_alerts_df.count())

display(service_alerts_df)

# COMMAND ----------

# DBTITLE 1,Service Alerts — Initial Bronze Write
# MAGIC %md
# MAGIC ## Service Alerts — Initial Bronze Write
# MAGIC
# MAGIC Deduplicates new alerts against existing bronze rows by `(feed_timestamp, alert_id)`, appends only new records to `rome_transport.bronze.service_alerts`, and validates the table.

# COMMAND ----------

table_name = "rome_transport.bronze.service_alerts"

if spark.catalog.tableExists(table_name):

    existing_keys = (
        spark.table(table_name)
        .select("feed_timestamp", "alert_id")
    )

    new_alerts_df = (
        service_alerts_df
        .join(
            existing_keys,
            ["feed_timestamp", "alert_id"],
            "left_anti"
        )
    )

else:
    new_alerts_df = service_alerts_df

new_rows = new_alerts_df.count()

print("New rows to append:", new_rows)

# COMMAND ----------

if new_rows > 0:
    (
        new_alerts_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(table_name)
    )

# COMMAND ----------

spark.sql("""
SELECT
    COUNT(*) AS rows,
    COUNT(DISTINCT feed_timestamp) AS snapshots,
    COUNT(DISTINCT alert_id) AS alerts
FROM rome_transport.bronze.service_alerts
""").show()

# COMMAND ----------

# DBTITLE 1,Reusable Function Prototyping
# MAGIC %md
# MAGIC ## Reusable Function Prototyping
# MAGIC
# MAGIC Refactors feed fetching, idempotent appending, and per-feed parsing into reusable functions (`fetch_feed`, `append_new_rows`, `build_*_df`) with live validation tests for each feed.

# COMMAND ----------

import urllib.request
from google.transit import gtfs_realtime_pb2

def fetch_feed(url):
    feed = gtfs_realtime_pb2.FeedMessage()

    with urllib.request.urlopen(url) as response:
        feed.ParseFromString(response.read())

    return feed

# COMMAND ----------

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

from datetime import datetime, timezone

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

            schedule_relationship = (
                gtfs_realtime_pb2.TripUpdate.StopTimeUpdate
                .ScheduleRelationship.Name(stu.schedule_relationship)
            )

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

    return spark.createDataFrame(rows, schema=trip_updates_schema)

# COMMAND ----------

test_trip_updates_df = build_trip_updates_df()

print("Rows:", test_trip_updates_df.count())
display(test_trip_updates_df.limit(10))

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

        vehicle_id = v.vehicle.id if v.HasField("vehicle") else None

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

test_vehicle_positions_df = build_vehicle_positions_df()

print("Rows:", test_vehicle_positions_df.count())
display(test_vehicle_positions_df.limit(10))

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
                "start": period.start if period.HasField("start") else None,
                "end": period.end if period.HasField("end") else None
            })

        informed_entities = []
        for ie in alert.informed_entity:
            trip_id = ie.trip.trip_id if ie.HasField("trip") else None

            informed_entities.append({
                "agency_id": ie.agency_id if ie.agency_id else None,
                "route_id": ie.route_id if ie.route_id else None,
                "route_type": ie.route_type if ie.HasField("route_type") else None,
                "trip_id": trip_id,
                "stop_id": ie.stop_id if ie.stop_id else None,
                "direction_id": ie.direction_id if ie.HasField("direction_id") else None
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

test_service_alerts_df = build_service_alerts_df()

print("Rows:", test_service_alerts_df.count())
display(test_service_alerts_df.limit(10))

# COMMAND ----------

# DBTITLE 1,End-to-End Ingestion Orchestrator
# MAGIC %md
# MAGIC ## End-to-End Ingestion Orchestrator
# MAGIC
# MAGIC Runs a full ingestion pass across all three feeds using the prototyped functions, appending new rows to the bronze tables, and verifying table growth.

# COMMAND ----------

def run_realtime_ingestion():
    summary = {}

    # -------------------------
    # Trip Updates
    # -------------------------
    trip_updates_df = build_trip_updates_df()

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

    summary["trip_updates_new_rows"] = trip_updates_new

    # -------------------------
    # Vehicle Positions
    # -------------------------
    vehicle_positions_df = build_vehicle_positions_df()

    vehicle_positions_new = append_new_rows(
        vehicle_positions_df,
        "rome_transport.bronze.vehicle_positions",
        [
            "feed_timestamp",
            "entity_id"
        ]
    )

    summary["vehicle_positions_new_rows"] = vehicle_positions_new

    # -------------------------
    # Service Alerts
    # -------------------------
    service_alerts_df = build_service_alerts_df()

    service_alerts_new = append_new_rows(
        service_alerts_df,
        "rome_transport.bronze.service_alerts",
        [
            "feed_timestamp",
            "alert_id"
        ]
    )

    summary["service_alerts_new_rows"] = service_alerts_new

    return summary

# COMMAND ----------

result = run_realtime_ingestion()

result

# COMMAND ----------

spark.sql("""
SELECT
    COUNT(*) AS rows,
    COUNT(DISTINCT feed_timestamp) AS snapshots
FROM rome_transport.bronze.trip_updates
""").show()

spark.sql("""
SELECT
    COUNT(*) AS rows,
    COUNT(DISTINCT feed_timestamp) AS snapshots
FROM rome_transport.bronze.vehicle_positions
""").show()

spark.sql("""
SELECT
    COUNT(*) AS rows,
    COUNT(DISTINCT feed_timestamp) AS snapshots
FROM rome_transport.bronze.service_alerts
""").show()

# COMMAND ----------

