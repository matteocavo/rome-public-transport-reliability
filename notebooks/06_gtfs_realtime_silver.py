# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC ## A. Load sources
# MAGIC Carico le tabelle Bronze e le tabelle Silver di input, così il notebook riparte sempre da un punto noto senza dipendenze da esecuzioni precedenti.
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F

trip_updates = spark.table("rome_transport.bronze.trip_updates")
vehicle_positions = spark.table("rome_transport.bronze.vehicle_positions")
service_alerts = spark.table("rome_transport.bronze.service_alerts")
trip_stop_schedule = spark.table("rome_transport.silver.trip_stop_schedule")
service_calendar = spark.table("rome_transport.silver.service_calendar")

source_counts = {
    "trip_updates": trip_updates.count(),
    "vehicle_positions": vehicle_positions.count(),
    "service_alerts": service_alerts.count(),
    "trip_stop_schedule": trip_stop_schedule.count(),
    "service_calendar": service_calendar.count(),
}
print(source_counts)


# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Build Silver Trip Updates
# MAGIC Trasformo i trip updates in una vista Silver pulita, convertendo timestamp e delay e mantenendo solo i campi utili al matching e al modello.
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F


def to_tolerant_date(column_name):
    return (
        F.when(
            F.trim(F.col(column_name).cast("string")).rlike(r"^\d{8}$"),
            F.to_date(F.col(column_name).cast("string"), "yyyyMMdd")
        )
        .otherwise(None)
    )


def parse_epoch_to_timestamp(column_name):
    return (
        F.when(
            F.trim(F.col(column_name).cast("string")).rlike(r"^-?\d+$"),
            F.to_timestamp(F.from_unixtime(F.col(column_name).cast("long")))
        )
        .otherwise(None)
    )

trip_updates_silver = (
    trip_updates
    .withColumn("feed_datetime", F.to_timestamp(F.from_unixtime(F.col("feed_timestamp"))))
    .withColumn("service_date", to_tolerant_date("start_date"))
    .withColumn("arrival_datetime", parse_epoch_to_timestamp("arrival_time"))
    .withColumn("departure_datetime", parse_epoch_to_timestamp("departure_time"))
    .withColumn("arrival_delay_minutes", F.col("arrival_delay") / F.lit(60.0))
    .withColumn("departure_delay_minutes", F.col("departure_delay") / F.lit(60.0))
    .select(
        "feed_timestamp",
        "feed_datetime",
        "entity_id",
        "trip_id",
        "route_id",
        "service_date",
        "start_time",
        "direction_id",
        "vehicle_id",
        "stop_sequence",
        "stop_id",
        "arrival_delay",
        "arrival_delay_minutes",
        "arrival_time",
        "arrival_datetime",
        "arrival_uncertainty",
        "departure_delay",
        "departure_delay_minutes",
        "departure_time",
        "departure_datetime",
        "departure_uncertainty",
        "schedule_relationship",
        "_ingestion_timestamp",
    )
)

trip_updates_silver.printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Deduplicate Trip Updates
# MAGIC Rimuovo i duplicati usando la chiave di business e mantengo l'ultima lettura per timestamp di ingestione, così il dataset Silver è coerente e deterministico.
# MAGIC

# COMMAND ----------

from pyspark.sql.window import Window

trip_updates_dedup_key = [
    "feed_timestamp",
    "entity_id",
    "trip_id",
    "stop_sequence",
    "stop_id",
]

trip_updates_before_count = trip_updates.count()
trip_updates_dedup_window = (
    Window
    .partitionBy(*trip_updates_dedup_key)
    .orderBy(F.col("_ingestion_timestamp").desc())
)

trip_updates_silver = (
    trip_updates_silver
    .withColumn("_dedup_rank", F.row_number().over(trip_updates_dedup_window))
    .filter(F.col("_dedup_rank") == 1)
    .drop("_dedup_rank")
)

trip_updates_after_count = trip_updates_silver.count()
print(f"trip_updates before dedup: {trip_updates_before_count}")
print(f"trip_updates after dedup: {trip_updates_after_count}")
print(f"duplicate rows removed: {trip_updates_before_count - trip_updates_after_count}")

trip_updates_duplicate_groups = (
    trip_updates
    .groupBy(*trip_updates_dedup_key)
    .count()
    .filter(F.col("count") > 1)
)
print(f"duplicate groups remaining: {trip_updates_duplicate_groups.count()}")

(
    trip_updates_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("rome_transport.silver.trip_updates")
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Build Silver Vehicle Positions
# MAGIC Costruisco la vista Silver di vehicle positions con parsing robusto delle date vuote o non valide e preservando i campi essenziali per il join temporale.
# MAGIC

# COMMAND ----------

vehicle_positions_silver = (
    vehicle_positions
    .withColumn("feed_datetime", F.to_timestamp(F.from_unixtime(F.col("feed_timestamp"))))
    .withColumn("service_date", to_tolerant_date("start_date"))
    .withColumn(
        "vehicle_datetime",
        F.when(
            F.trim(F.col("vehicle_timestamp").cast("string")).rlike(r"^-?\d+$"),
            F.to_timestamp(F.from_unixtime(F.col("vehicle_timestamp").cast("long")))
        ).otherwise(None)
    )
    .select(
        "feed_timestamp",
        "feed_datetime",
        "entity_id",
        "trip_id",
        "route_id",
        "service_date",
        "start_time",
        "direction_id",
        "vehicle_id",
        "latitude",
        "longitude",
        "bearing",
        "speed",
        "odometer",
        "current_stop_sequence",
        "current_status",
        "stop_id",
        "vehicle_timestamp",
        "vehicle_datetime",
        "_ingestion_timestamp",
    )
)

vehicle_positions_silver.printSchema()

(
    vehicle_positions_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("rome_transport.silver.vehicle_positions")
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## E. Validate Silver inputs
# MAGIC Controllo i dataset Silver prima del matching per evitare di basare il risultato finale su input già corrotti o non coerenti.
# MAGIC

# COMMAND ----------

spark.sql("""
SELECT
    COUNT(*) AS rows,
    SUM(CASE WHEN service_date IS NULL THEN 1 ELSE 0 END) AS null_service_date,
    SUM(CASE WHEN trip_id IS NULL THEN 1 ELSE 0 END) AS null_trip_id,
    SUM(CASE WHEN vehicle_id IS NULL THEN 1 ELSE 0 END) AS null_vehicle_id
FROM rome_transport.silver.trip_updates
""").show(truncate=False)

spark.sql("""
SELECT
    COUNT(*) AS rows,
    SUM(CASE WHEN service_date IS NULL THEN 1 ELSE 0 END) AS null_service_date,
    SUM(CASE WHEN trip_id IS NULL THEN 1 ELSE 0 END) AS null_trip_id,
    SUM(CASE WHEN vehicle_id IS NULL THEN 1 ELSE 0 END) AS null_vehicle_id,
    SUM(CASE WHEN latitude IS NULL OR longitude IS NULL THEN 1 ELSE 0 END) AS null_position
FROM rome_transport.silver.vehicle_positions
""").show(truncate=False)

trip_updates_duplicate_groups = (
    spark.table("rome_transport.silver.trip_updates")
    .groupBy("feed_timestamp", "entity_id", "trip_id", "stop_sequence", "stop_id")
    .count()
    .filter(F.col("count") > 1)
)
print(f"trip_updates duplicate groups in Silver: {trip_updates_duplicate_groups.count()}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## F. Causal backward as-of matching (180s maximum staleness)
# MAGIC A causal backward as-of join selects the latest vehicle position at or before each Trip Update timestamp, with a maximum staleness of 180 seconds. Only positions already available by vehicle timestamp are eligible, preventing future-position temporal leakage in downstream machine-learning features.
# MAGIC

# COMMAND ----------

from pyspark.sql.window import Window

tu = spark.table("rome_transport.silver.trip_updates").alias("tu")
vp = spark.table("rome_transport.silver.vehicle_positions").alias("vp")

candidates = (
    tu.join(
        vp,
        (
            (F.col("tu.vehicle_id") == F.col("vp.vehicle_id")) &
            (F.col("tu.trip_id") == F.col("vp.trip_id")) &
            (F.col("vp.vehicle_timestamp") <= F.col("tu.feed_timestamp")) &
            ((F.col("tu.feed_timestamp") - F.col("vp.vehicle_timestamp")) <= 180)
        ),
        "left"
    )
    .withColumn(
        "position_time_diff_seconds",
        F.col("tu.feed_timestamp") - F.col("vp.vehicle_timestamp")
    )
)

matching_window = (
    Window
    .partitionBy(
        F.col("tu.feed_timestamp"),
        F.col("tu.entity_id"),
        F.col("tu.trip_id"),
        F.col("tu.stop_sequence"),
        F.col("tu.stop_id")
    )
    .orderBy(F.col("position_time_diff_seconds").asc_nulls_last())
)

matched = (
    candidates
    .withColumn("_match_rank", F.row_number().over(matching_window))
    .filter(F.col("_match_rank") == 1)
)

matched.limit(5).select(
    "tu.feed_timestamp",
    "tu.entity_id",
    "tu.trip_id",
    "tu.stop_sequence",
    "tu.stop_id",
    "vp.vehicle_id",
    "position_time_diff_seconds",
    "vp.vehicle_datetime",
).show(truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC ## G. Build realtime_trip_stop_observations
# MAGIC Genero la tabella Silver finale dei stop realtime, unendo informazioni di trip update e posizione del veicolo con il tempo di differenza più vicino.
# MAGIC

# COMMAND ----------

realtime_trip_stop_observations = (
    matched.select(
        F.col("tu.feed_timestamp").alias("feed_timestamp"),
        F.col("tu.feed_datetime").alias("feed_datetime"),
        F.col("tu.entity_id").alias("entity_id"),
        F.col("tu.trip_id").alias("trip_id"),
        F.col("tu.route_id").alias("route_id"),
        F.col("tu.service_date").alias("service_date"),
        F.col("tu.start_time").alias("start_time"),
        F.col("tu.direction_id").alias("direction_id"),
        F.col("tu.vehicle_id").alias("vehicle_id"),
        F.col("tu.stop_sequence").alias("stop_sequence"),
        F.col("tu.stop_id").alias("stop_id"),
        F.col("tu.arrival_delay").alias("arrival_delay_seconds"),
        F.col("tu.arrival_delay_minutes").alias("arrival_delay_minutes"),
        F.col("tu.arrival_datetime").alias("predicted_arrival_datetime"),
        F.col("tu.departure_delay").alias("departure_delay_seconds"),
        F.col("tu.departure_delay_minutes").alias("departure_delay_minutes"),
        F.col("tu.schedule_relationship").alias("schedule_relationship"),
        F.col("vp.latitude").alias("vehicle_latitude"),
        F.col("vp.longitude").alias("vehicle_longitude"),
        F.col("vp.bearing").alias("vehicle_bearing"),
        F.col("vp.speed").alias("vehicle_speed"),
        F.col("vp.odometer").alias("vehicle_odometer"),
        F.col("vp.current_stop_sequence").alias("current_stop_sequence"),
        F.col("vp.current_status").alias("current_status"),
        F.col("vp.stop_id").alias("vehicle_current_stop_id"),
        F.col("vp.vehicle_datetime").alias("vehicle_datetime"),
        F.col("position_time_diff_seconds").alias("position_time_diff_seconds"),
    )
)

realtime_trip_stop_observations.printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC ## H. Save Silver table
# MAGIC Salvo la table Silver finale in Delta con overwrite schema attivo, così la tabella rimane coerente con il flusso di elaborazione.
# MAGIC

# COMMAND ----------

(
    realtime_trip_stop_observations.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("rome_transport.silver.realtime_trip_stop_observations")
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## I. Final validation
# MAGIC Concludo con la validazione finale del match rate: total rows, matched rows, unmatched rows e percentuale di matching calcolata sui dati reali.
# MAGIC

# COMMAND ----------

final_observations = spark.table("rome_transport.silver.realtime_trip_stop_observations")
final_validation = (
    final_observations
    .selectExpr(
        "COUNT(*) AS total_trip_updates",
        "COUNT(position_time_diff_seconds) AS matched_vehicle_positions",
        "COUNT(*) - COUNT(position_time_diff_seconds) AS unmatched_vehicle_positions",
        "ROUND(100.0 * COUNT(position_time_diff_seconds) / NULLIF(COUNT(*), 0), 2) AS vehicle_match_rate_pct",
        "COALESCE(SUM(CASE WHEN vehicle_datetime > feed_datetime OR position_time_diff_seconds < 0 THEN 1 ELSE 0 END), 0) AS rows_with_future_vehicle_position",
        "MIN(position_time_diff_seconds) AS min_position_time_diff_seconds",
        "MAX(position_time_diff_seconds) AS max_position_time_diff_seconds",
        "AVG(position_time_diff_seconds) AS avg_position_time_diff_seconds"
    )
)

final_validation.show(truncate=False)
validation_metrics = final_validation.first().asDict()
print(validation_metrics)

final_duplicate_groups = (
    final_observations
    .groupBy("feed_timestamp", "entity_id", "trip_id", "stop_sequence", "stop_id")
    .count()
    .filter(F.col("count") > 1)
)
final_duplicate_summary = final_duplicate_groups.selectExpr(
    "COUNT(*) AS duplicate_groups",
    "COALESCE(SUM(count - 1), 0) AS duplicate_extra_rows"
)
final_duplicate_summary.show(truncate=False)
duplicate_metrics = final_duplicate_summary.first().asDict()

assert validation_metrics["rows_with_future_vehicle_position"] == 0, "Future vehicle positions detected"
if validation_metrics["matched_vehicle_positions"] > 0:
    assert validation_metrics["min_position_time_diff_seconds"] >= 0, "Negative position staleness detected"
    assert validation_metrics["max_position_time_diff_seconds"] <= 180, "Position exceeds 180-second staleness limit"
assert duplicate_metrics["duplicate_extra_rows"] == 0, "Duplicate final observation keys detected"
assert validation_metrics["total_trip_updates"] == tu.count(), "Matching changed the Trip Update row count"