# Databricks notebook source
# DBTITLE 1,Load Bronze Sources
# MAGIC %md
# MAGIC ## Load Bronze Sources
# MAGIC
# MAGIC Loads the seven Bronze-layer GTFS static tables (`agency`, `routes`, `trips`, `stops`, `stop_times`, `calendar_dates`, `shapes`) as Spark DataFrames and prints row counts to verify ingestion completeness. These tables are the raw inputs for all Silver-layer transformations.

# COMMAND ----------

agency = spark.table("rome_transport.bronze.agency")
routes = spark.table("rome_transport.bronze.routes")
trips = spark.table("rome_transport.bronze.trips")
stops = spark.table("rome_transport.bronze.stops")
stop_times = spark.table("rome_transport.bronze.stop_times")
calendar_dates = spark.table("rome_transport.bronze.calendar_dates")
shapes = spark.table("rome_transport.bronze.shapes")

# COMMAND ----------

print("agency:", agency.count())
print("routes:", routes.count())
print("trips:", trips.count())
print("stops:", stops.count())
print("stop_times:", stop_times.count())
print("calendar_dates:", calendar_dates.count())
print("shapes:", shapes.count())

# COMMAND ----------

# DBTITLE 1,Service Calendar Transformation
# MAGIC %md
# MAGIC ## Service Calendar Transformation
# MAGIC
# MAGIC Parses the `calendar_dates` Bronze table into a typed Silver dimension by converting the integer `date` column (YYYYMMDD format) into a proper `DATE` type. The result is persisted as `rome_transport.silver.service_calendar`, which maps each service ID to its active and exception dates.

# COMMAND ----------

from pyspark.sql.functions import to_date, col

service_calendar = (
    calendar_dates
    .select(
        col("service_id"),
        to_date(col("date"), "yyyyMMdd").alias("service_date"),
        col("exception_type")
    )
)

display(service_calendar)

# COMMAND ----------

service_calendar.printSchema()

# COMMAND ----------

(
    service_calendar.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("rome_transport.silver.service_calendar")
)

# COMMAND ----------

spark.table("rome_transport.silver.service_calendar").count()

# COMMAND ----------

# DBTITLE 1,Trip Schedule Enrichment
# MAGIC %md
# MAGIC ## Trip Schedule Enrichment
# MAGIC
# MAGIC Enriches the `trips` table with route and agency attributes via left joins on `route_id` and `agency_id`. This produces `rome_transport.silver.trip_schedule`, a trip-level dimension capturing route type, agency name, direction, accessibility, and shape references for downstream scheduling analysis.

# COMMAND ----------

trip_schedule = (
    trips.alias("t")
    .join(
        routes.alias("r"),
        col("t.route_id") == col("r.route_id"),
        "left"
    )
    .join(
        agency.alias("a"),
        col("r.agency_id") == col("a.agency_id"),
        "left"
    )
    .select(
        col("t.trip_id"),
        col("t.route_id"),
        col("r.route_short_name"),
        col("r.route_long_name"),
        col("r.route_type"),
        col("r.agency_id"),
        col("a.agency_name"),
        col("t.service_id"),
        col("t.trip_headsign"),
        col("t.trip_short_name"),
        col("t.direction_id"),
        col("t.shape_id"),
        col("t.wheelchair_accessible"),
        col("t.exceptional")
    )
)

display(trip_schedule)

# COMMAND ----------

trip_schedule.printSchema()
print("trip_schedule:", trip_schedule.count())

# COMMAND ----------

(
    trip_schedule.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("rome_transport.silver.trip_schedule")
)

# COMMAND ----------

# DBTITLE 1,Trip-Stop Schedule Construction
# MAGIC %md
# MAGIC ## Trip-Stop Schedule Construction
# MAGIC
# MAGIC Joins `stop_times` with the enriched `trip_schedule` and `stops` tables to build `rome_transport.silver.trip_stop_schedule`. This fact-level table links every stop visit to its trip, route, agency, and geographic coordinates, forming the backbone of stop-level delay and reliability analysis.

# COMMAND ----------

trip_stop_schedule = (
    stop_times.alias("st")
    .join(
        trip_schedule.alias("ts"),
        col("st.trip_id") == col("ts.trip_id"),
        "left"
    )
    .join(
        stops.alias("s"),
        col("st.stop_id") == col("s.stop_id"),
        "left"
    )
    .select(
        col("ts.trip_id"),
        col("ts.route_id"),
        col("ts.route_short_name"),
        col("ts.route_type"),
        col("ts.agency_id"),
        col("ts.agency_name"),
        col("ts.service_id"),
        col("ts.trip_headsign"),
        col("ts.direction_id"),
        col("ts.shape_id"),

        col("st.stop_id"),
        col("s.stop_name"),
        col("s.stop_lat"),
        col("s.stop_lon"),
        col("st.stop_sequence"),

        col("st.arrival_time"),
        col("st.departure_time"),
        col("st.pickup_type"),
        col("st.drop_off_type"),
        col("st.shape_dist_traveled"),
        col("st.timepoint")
    )
)

display(trip_stop_schedule)

# COMMAND ----------

trip_stop_schedule.printSchema()
print("trip_stop_schedule:", trip_stop_schedule.count())

# COMMAND ----------

(
    trip_stop_schedule.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("rome_transport.silver.trip_stop_schedule")
)

# COMMAND ----------

spark.table("rome_transport.silver.trip_stop_schedule").count()

# COMMAND ----------

# DBTITLE 1,GTFS Time Normalization
# MAGIC %md
# MAGIC ## GTFS Time Normalization
# MAGIC
# MAGIC Converts GTFS `arrival_time` and `departure_time` strings (HH:MM:SS) into integer seconds from the start of the service day. GTFS times can legitimately exceed 24:00:00 because a trip that departs before midnight may still be running past midnight — the standard encodes these as cumulative hours (e.g., 25:30:00 means 1:30 AM the following day). The cell validates how many records exceed 86,400 seconds (one full day) before persisting the enriched Silver table.

# COMMAND ----------

from pyspark.sql.functions import split, col

trip_stop_schedule_enriched = (
    trip_stop_schedule
    .withColumn(
        "arrival_seconds",
        split(col("arrival_time"), ":").getItem(0).cast("int") * 3600
        + split(col("arrival_time"), ":").getItem(1).cast("int") * 60
        + split(col("arrival_time"), ":").getItem(2).cast("int")
    )
    .withColumn(
        "departure_seconds",
        split(col("departure_time"), ":").getItem(0).cast("int") * 3600
        + split(col("departure_time"), ":").getItem(1).cast("int") * 60
        + split(col("departure_time"), ":").getItem(2).cast("int")
    )
)

display(
    trip_stop_schedule_enriched.select(
        "trip_id",
        "stop_sequence",
        "arrival_time",
        "arrival_seconds",
        "departure_time",
        "departure_seconds"
    )
)

# COMMAND ----------

trip_stop_schedule_enriched.selectExpr(
    "MAX(arrival_seconds) AS max_arrival_seconds",
    "MAX(departure_seconds) AS max_departure_seconds"
).show()

# COMMAND ----------

trip_stop_schedule_enriched.filter(
    (col("arrival_seconds") >= 86400) |
    (col("departure_seconds") >= 86400)
).count()

# COMMAND ----------

(
    trip_stop_schedule_enriched.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("rome_transport.silver.trip_stop_schedule")
)

# COMMAND ----------

# DBTITLE 1,Service-Day Offset Handling
# MAGIC %md
# MAGIC ## Service-Day Offset Handling
# MAGIC
# MAGIC Decomposes the cumulative seconds into a `day_offset` (number of full days past the service start) and `seconds_of_day` (time within that day) using `floor` and `pmod`. This split makes it straightforward to join scheduled times against actual service dates and real-time observations. The final enriched table overwrites `rome_transport.silver.trip_stop_schedule`.

# COMMAND ----------

from pyspark.sql.functions import floor, pmod, lit

trip_stop_schedule_enriched = (
    spark.table("rome_transport.silver.trip_stop_schedule")
    .withColumn(
        "arrival_day_offset",
        floor(col("arrival_seconds") / lit(86400))
    )
    .withColumn(
        "departure_day_offset",
        floor(col("departure_seconds") / lit(86400))
    )
    .withColumn(
        "arrival_seconds_of_day",
        pmod(col("arrival_seconds"), lit(86400))
    )
    .withColumn(
        "departure_seconds_of_day",
        pmod(col("departure_seconds"), lit(86400))
    )
)

# COMMAND ----------

(
    trip_stop_schedule_enriched.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("rome_transport.silver.trip_stop_schedule")
)

# COMMAND ----------

