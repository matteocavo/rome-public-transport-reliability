# Databricks notebook source
# DBTITLE 1,GTFS Source Paths
# MAGIC %md
# MAGIC ## GTFS Source Paths
# MAGIC
# MAGIC Read the `agency.txt` GTFS file from the Bronze volume using Spark's inferred schema for initial exploration of structure and content. This establishes the source path pattern used throughout the notebook.

# COMMAND ----------

agency_path = "/Volumes/rome_transport/bronze/gtfs_raw/agency.txt"

agency_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(agency_path)
)

display(agency_df)

# COMMAND ----------

# DBTITLE 1,Initial Schema Inspection
# MAGIC %md
# MAGIC ## Initial Schema Inspection
# MAGIC
# MAGIC Inspect the inferred schema and row count to understand column types and data volume before enforcing explicit schemas.

# COMMAND ----------

agency_df.printSchema()

# COMMAND ----------

agency_df.count()

# COMMAND ----------

# DBTITLE 1,Explicit Schema Enforcement
# MAGIC %md
# MAGIC ## Explicit Schema Enforcement
# MAGIC
# MAGIC Define an explicit `StructType` schema for the `agency` table to guarantee type consistency. Re-read the CSV with the enforced schema, add Bronze traceability metadata (`_source_file` and `_ingestion_timestamp`), and persist the result as a Delta table in `rome_transport.bronze`. Bronze preserves source fidelity while adding traceability metadata such as source file and ingestion timestamp.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType

agency_schema = StructType([
    StructField("agency_id", StringType(), True),
    StructField("agency_name", StringType(), True),
    StructField("agency_url", StringType(), True),
    StructField("agency_timezone", StringType(), True),
    StructField("agency_lang", StringType(), True),
    StructField("agency_phone", StringType(), True),
    StructField("agency_fare_url", StringType(), True),
])

# COMMAND ----------

agency_bronze_df = (
    spark.read
    .option("header", True)
    .schema(agency_schema)
    .csv(agency_path)
)

# COMMAND ----------

agency_bronze_df.printSchema()
display(agency_bronze_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col

agency_bronze_df = (
    spark.read
    .option("header", True)
    .schema(agency_schema)
    .csv(agency_path)
    .select(
        "*",
        col("_metadata.file_path").alias("_source_file")
    )
    .withColumn("_ingestion_timestamp", current_timestamp())
)

display(agency_bronze_df)

# COMMAND ----------

(
    agency_bronze_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("rome_transport.bronze.agency")
)

# COMMAND ----------

spark.table("rome_transport.bronze.agency").show(truncate=False)

# COMMAND ----------

spark.sql("""
SELECT *
FROM rome_transport.bronze.agency
""").show(truncate=False)

# COMMAND ----------

spark.sql("""
DESCRIBE TABLE rome_transport.bronze.agency
""").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Static GTFS Table Ingestion
# MAGIC %md
# MAGIC ## Static GTFS Table Ingestion
# MAGIC
# MAGIC Apply the same Bronze ingestion pattern to the `routes` table: define an explicit schema, read the CSV with metadata columns, write to Delta, and verify the persisted table.

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType
)

routes_schema = StructType([
    StructField("route_id", StringType(), True),
    StructField("agency_id", StringType(), True),
    StructField("route_short_name", StringType(), True),
    StructField("route_long_name", StringType(), True),
    StructField("route_type", IntegerType(), True),
    StructField("route_url", StringType(), True),
    StructField("route_color", StringType(), True),
    StructField("route_text_color", StringType(), True),
])

# COMMAND ----------

routes_path = "/Volumes/rome_transport/bronze/gtfs_raw/routes.txt"

routes_bronze_df = (
    spark.read
    .option("header", True)
    .schema(routes_schema)
    .csv(routes_path)
    .select(
        "*",
        col("_metadata.file_path").alias("_source_file")
    )
    .withColumn("_ingestion_timestamp", current_timestamp())
)

# COMMAND ----------

routes_bronze_df.printSchema()
display(routes_bronze_df)

# COMMAND ----------

routes_bronze_df.count()

# COMMAND ----------

(
    routes_bronze_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("rome_transport.bronze.routes")
)

# COMMAND ----------

spark.table("rome_transport.bronze.routes").count()

# COMMAND ----------

spark.sql("""
DESCRIBE TABLE rome_transport.bronze.routes
""").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Reusable Bronze Ingestion Function
# MAGIC %md
# MAGIC ## Reusable Bronze Ingestion Function
# MAGIC
# MAGIC Encapsulate the Bronze ingestion pattern — schema-enforced CSV read, metadata enrichment, and Delta write — into a reusable function to avoid repetition across GTFS tables.

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col

def load_gtfs_to_bronze(file_name, schema, table_name):
    file_path = f"/Volumes/rome_transport/bronze/gtfs_raw/{file_name}"

    df = (
        spark.read
        .option("header", True)
        .schema(schema)
        .csv(file_path)
        .select(
            "*",
            col("_metadata.file_path").alias("_source_file")
        )
        .withColumn("_ingestion_timestamp", current_timestamp())
    )

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(f"rome_transport.bronze.{table_name}")
    )

    return df

# COMMAND ----------

# DBTITLE 1,Batch Static GTFS Loading
# MAGIC %md
# MAGIC Define explicit schemas for the remaining static GTFS tables (`calendar_dates`, `stops`, `trips`, `shapes`) and load them with the reusable Bronze function, then print row counts to verify ingestion.

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType
)

calendar_dates_schema = StructType([
    StructField("service_id", StringType(), True),
    StructField("date", StringType(), True),
    StructField("exception_type", IntegerType(), True),
])

stops_schema = StructType([
    StructField("stop_id", StringType(), True),
    StructField("stop_code", StringType(), True),
    StructField("stop_name", StringType(), True),
    StructField("stop_desc", StringType(), True),
    StructField("stop_lat", DoubleType(), True),
    StructField("stop_lon", DoubleType(), True),
    StructField("stop_url", StringType(), True),
    StructField("wheelchair_boarding", IntegerType(), True),
    StructField("stop_timezone", StringType(), True),
    StructField("location_type", IntegerType(), True),
    StructField("parent_station", StringType(), True),
])

trips_schema = StructType([
    StructField("route_id", StringType(), True),
    StructField("service_id", StringType(), True),
    StructField("trip_id", StringType(), True),
    StructField("trip_headsign", StringType(), True),
    StructField("trip_short_name", StringType(), True),
    StructField("direction_id", IntegerType(), True),
    StructField("block_id", StringType(), True),
    StructField("shape_id", StringType(), True),
    StructField("wheelchair_accessible", IntegerType(), True),
    StructField("exceptional", IntegerType(), True),
])

shapes_schema = StructType([
    StructField("shape_id", StringType(), True),
    StructField("shape_pt_lat", DoubleType(), True),
    StructField("shape_pt_lon", DoubleType(), True),
    StructField("shape_pt_sequence", IntegerType(), True),
    StructField("shape_dist_traveled", DoubleType(), True),
])

# COMMAND ----------

calendar_dates_df = load_gtfs_to_bronze(
    "calendar_dates.txt",
    calendar_dates_schema,
    "calendar_dates"
)

stops_df = load_gtfs_to_bronze(
    "stops.txt",
    stops_schema,
    "stops"
)

trips_df = load_gtfs_to_bronze(
    "trips.txt",
    trips_schema,
    "trips"
)

shapes_df = load_gtfs_to_bronze(
    "shapes.txt",
    shapes_schema,
    "shapes"
)

# COMMAND ----------

print("calendar_dates:", calendar_dates_df.count())
print("stops:", stops_df.count())
print("trips:", trips_df.count())
print("shapes:", shapes_df.count())

# COMMAND ----------

# DBTITLE 1,stop_times Large-Scale Ingestion
# MAGIC %md
# MAGIC ## stop_times Large-Scale Ingestion
# MAGIC
# MAGIC Define the `stop_times` schema and load the largest GTFS file separately, as it contains every scheduled stop event across all trips — typically the most voluminous table in the feed.

# COMMAND ----------

stop_times_schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("arrival_time", StringType(), True),
    StructField("departure_time", StringType(), True),
    StructField("stop_id", StringType(), True),
    StructField("stop_sequence", IntegerType(), True),
    StructField("stop_headsign", StringType(), True),
    StructField("pickup_type", IntegerType(), True),
    StructField("drop_off_type", IntegerType(), True),
    StructField("shape_dist_traveled", DoubleType(), True),
    StructField("timepoint", IntegerType(), True),
])

# COMMAND ----------

stop_times_df = load_gtfs_to_bronze(
    "stop_times.txt",
    stop_times_schema,
    "stop_times"
)

# COMMAND ----------

stop_times_df.printSchema()
print("stop_times:", stop_times_df.count())

# COMMAND ----------

# DBTITLE 1,Referential Integrity Checks
# MAGIC %md
# MAGIC ## Referential Integrity Checks
# MAGIC
# MAGIC Validate cross-table referential integrity by detecting orphan foreign keys: `trip_id` in stop_times missing from trips, `stop_id` in stop_times missing from stops, `route_id` in trips missing from routes, and `service_id` in trips missing from calendar_dates.

# COMMAND ----------

# 1. trip_id presenti in stop_times ma assenti in trips
orphan_trip_ids = spark.sql("""
SELECT COUNT(*) AS orphan_trip_ids
FROM (
    SELECT DISTINCT st.trip_id
    FROM rome_transport.bronze.stop_times st
    LEFT ANTI JOIN rome_transport.bronze.trips t
        ON st.trip_id = t.trip_id
)
""")

display(orphan_trip_ids)

# COMMAND ----------

# 2. stop_id presenti in stop_times ma assenti in stops
orphan_stop_ids = spark.sql("""
SELECT COUNT(*) AS orphan_stop_ids
FROM (
    SELECT DISTINCT st.stop_id
    FROM rome_transport.bronze.stop_times st
    LEFT ANTI JOIN rome_transport.bronze.stops s
        ON st.stop_id = s.stop_id
)
""")

display(orphan_stop_ids)

# COMMAND ----------

# 3. route_id presenti in trips ma assenti in routes
orphan_route_ids = spark.sql("""
SELECT COUNT(*) AS orphan_route_ids
FROM (
    SELECT DISTINCT t.route_id
    FROM rome_transport.bronze.trips t
    LEFT ANTI JOIN rome_transport.bronze.routes r
        ON t.route_id = r.route_id
)
""")

display(orphan_route_ids)

# COMMAND ----------

# 4. service_id presenti in trips ma assenti in calendar_dates
orphan_service_ids = spark.sql("""
SELECT COUNT(*) AS orphan_service_ids
FROM (
    SELECT DISTINCT t.service_id
    FROM rome_transport.bronze.trips t
    LEFT ANTI JOIN rome_transport.bronze.calendar_dates c
        ON t.service_id = c.service_id
)
""")

display(orphan_service_ids)

# COMMAND ----------

# DBTITLE 1,Bronze Data Quality Validation
# MAGIC %md
# MAGIC ## Bronze Data Quality Validation
# MAGIC
# MAGIC Run a comprehensive data quality suite: detect duplicate primary keys across core tables, null or invalid critical fields, out-of-range coordinates, and malformed shape point sequences.

# COMMAND ----------

dq_results = {}

dq_results["duplicate_agency_id"] = spark.sql("""
SELECT COUNT(*) FROM (
    SELECT agency_id
    FROM rome_transport.bronze.agency
    GROUP BY agency_id
    HAVING COUNT(*) > 1
)
""").first()[0]

dq_results["duplicate_route_id"] = spark.sql("""
SELECT COUNT(*) FROM (
    SELECT route_id
    FROM rome_transport.bronze.routes
    GROUP BY route_id
    HAVING COUNT(*) > 1
)
""").first()[0]

dq_results["duplicate_trip_id"] = spark.sql("""
SELECT COUNT(*) FROM (
    SELECT trip_id
    FROM rome_transport.bronze.trips
    GROUP BY trip_id
    HAVING COUNT(*) > 1
)
""").first()[0]

dq_results["duplicate_stop_id"] = spark.sql("""
SELECT COUNT(*) FROM (
    SELECT stop_id
    FROM rome_transport.bronze.stops
    GROUP BY stop_id
    HAVING COUNT(*) > 1
)
""").first()[0]

dq_results["null_critical_stop_times"] = spark.sql("""
SELECT COUNT(*)
FROM rome_transport.bronze.stop_times
WHERE trip_id IS NULL
   OR stop_id IS NULL
   OR stop_sequence IS NULL
""").first()[0]

dq_results["invalid_stop_sequence"] = spark.sql("""
SELECT COUNT(*)
FROM rome_transport.bronze.stop_times
WHERE stop_sequence <= 0
""").first()[0]

dq_results["invalid_stop_coordinates"] = spark.sql("""
SELECT COUNT(*)
FROM rome_transport.bronze.stops
WHERE stop_lat IS NULL
   OR stop_lon IS NULL
   OR stop_lat NOT BETWEEN -90 AND 90
   OR stop_lon NOT BETWEEN -180 AND 180
""").first()[0]

dq_results["invalid_shape_sequence"] = spark.sql("""
SELECT COUNT(*)
FROM rome_transport.bronze.shapes
WHERE shape_pt_sequence IS NULL
   OR shape_pt_sequence <= 0
""").first()[0]

dq_results["orphan_shape_ids"] = spark.sql("""
SELECT COUNT(*) FROM (
    SELECT DISTINCT t.shape_id
    FROM rome_transport.bronze.trips t
    LEFT ANTI JOIN (
        SELECT DISTINCT shape_id
        FROM rome_transport.bronze.shapes
    ) s
    ON t.shape_id = s.shape_id
    WHERE t.shape_id IS NOT NULL
)
""").first()[0]

dq_results["invalid_shape_sequence"] = spark.sql("""
SELECT COUNT(*)
FROM rome_transport.bronze.shapes
WHERE shape_pt_sequence IS NULL
   OR shape_pt_sequence < 0
""").first()[0]

dq_results["invalid_shape_sequence"]

# COMMAND ----------

invalid_shape_order = spark.sql("""
WITH shape_order AS (
    SELECT
        shape_id,
        shape_pt_sequence,
        LAG(shape_pt_sequence) OVER (
            PARTITION BY shape_id
            ORDER BY shape_pt_sequence
        ) AS previous_sequence
    FROM rome_transport.bronze.shapes
)
SELECT COUNT(*) AS invalid_shape_order
FROM shape_order
WHERE previous_sequence IS NOT NULL
  AND shape_pt_sequence <= previous_sequence
""")

display(invalid_shape_order)

# COMMAND ----------

