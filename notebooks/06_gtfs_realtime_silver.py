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

spark.conf.set("spark.sql.session.timeZone", "Europe/Rome")
ALLOW_SILVER_OVERWRITE = False
try:
    ALLOW_SILVER_OVERWRITE = dbutils.widgets.get("allow_silver_overwrite").lower() == "true"
except Exception:
    pass
SOURCE_VERSION_OVERRIDES = {}
SOURCE_TABLES = {
    "trip_updates": "rome_transport.bronze.trip_updates",
    "vehicle_positions": "rome_transport.bronze.vehicle_positions",
    "service_alerts": "rome_transport.bronze.service_alerts",
    "trip_stop_schedule": "rome_transport.silver.trip_stop_schedule",
    "service_calendar": "rome_transport.silver.service_calendar",
}
source_versions = {}
sources = {}
for name, table in SOURCE_TABLES.items():
    version = SOURCE_VERSION_OVERRIDES.get(name)
    if version is None:
        version = int(spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()["version"])
    source_versions[name] = int(version)
    sources[name] = spark.read.option("versionAsOf", int(version)).table(table)
trip_updates = sources["trip_updates"]
vehicle_positions = sources["vehicle_positions"]
service_alerts = sources["service_alerts"]
trip_stop_schedule = sources["trip_stop_schedule"]
service_calendar = sources["service_calendar"]
print({"source_versions": source_versions, "write_enabled": ALLOW_SILVER_OVERWRITE})


def deduplicate_latest(df, keys):
    """Preserve latest-ingestion semantics; reject conflicting latest-time ties."""
    from pyspark.sql.window import Window
    missing = None
    for key in keys:
        bad = F.col(key).isNull()
        missing = bad if missing is None else missing | bad
    if df.filter(missing | F.col("_ingestion_timestamp").isNull()).limit(1).count():
        raise ValueError("Missing source observation key/ingestion timestamp; write blocked")
    window = Window.partitionBy(*keys).orderBy(F.col("_ingestion_timestamp").desc())
    latest = df.withColumn("_latest_rank", F.dense_rank().over(window)).filter(
        F.col("_latest_rank") == 1).drop("_latest_rank").dropDuplicates()
    if latest.groupBy(*keys).count().filter(F.col("count") > 1).limit(1).count():
        raise ValueError("Conflicting payloads at latest ingestion time; write blocked")
    return latest

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


# At least 99.9% of observations must retain a date, both globally and in
# EVERY feed snapshot. A per-snapshot budget prevents history diluting an outage.
# Unresolvable observations stay in pinned Bronze; no calendar date is invented.
MAX_INVALID_SERVICE_DATE_RATE = 0.001


def normalize_service_date(value):
    """Classify diagnostic samples; accept only real Gregorian YYYYMMDD dates."""
    import re
    from datetime import datetime
    if value is None:
        return None
    text = str(value).strip(" ")
    if not re.fullmatch(r"[0-9]{8}", text):
        return None
    try:
        return datetime.strptime(text, "%Y%m%d").date()
    except ValueError:
        return None


def validate_service_date_quality(total, invalid, repaired, worst_snapshot_rate):
    """Exclusions are bounded; invalid dates can never reach Silver."""
    import math
    if (total <= 0 or invalid < 0 or invalid >= total or repaired < 0
            or repaired > total - invalid or not math.isfinite(worst_snapshot_rate)
            or not 0 <= worst_snapshot_rate <= 1):
        raise ValueError("trip_updates: invalid service-date quality metrics; write blocked")
    if (invalid / total > MAX_INVALID_SERVICE_DATE_RATE
            or worst_snapshot_rate > MAX_INVALID_SERVICE_DATE_RATE):
        raise ValueError("trip_updates: service-date exclusion budget exceeded; write blocked")
    return {"total_rows_checked": total, "valid_rows": total - invalid - repaired,
            "invalid_service_date_rows": invalid, "repaired_rows": repaired,
            "excluded_rows": invalid, "invalid_percentage": 100.0 * invalid / total,
            "worst_snapshot_invalid_percentage": 100.0 * worst_snapshot_rate,
            "max_invalid_percentage": 100.0 * MAX_INVALID_SERVICE_DATE_RATE,
            "quality_status": "PASSED"}


def to_tolerant_date(column_name):
    text = F.trim(F.col(column_name).cast("string"))
    return F.when(text.rlike(r"^[0-9]{8}$"),
                  F.try_to_timestamp(text, F.lit("yyyyMMdd")).cast("date")).otherwise(None)


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
    .withColumn("_service_date_raw", F.col("start_date"))
    .withColumn("_service_date_repaired", F.col("service_date").isNotNull() &
                (F.col("start_date") != F.trim(F.col("start_date"))))
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
        "_service_date_raw",
        "_service_date_repaired",
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
trip_updates_silver = deduplicate_latest(trip_updates_silver, trip_updates_dedup_key)
trip_updates_after_count = trip_updates_silver.count()
print({"trip_updates_before": trip_updates_before_count, "trip_updates_after": trip_updates_after_count,
       "duplicates_removed": trip_updates_before_count - trip_updates_after_count})


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

vehicle_positions_silver = deduplicate_latest(
    vehicle_positions_silver, ["feed_timestamp", "entity_id"])


# COMMAND ----------

# MAGIC %md
# MAGIC ## E. Validate Silver inputs
# MAGIC Controllo i dataset Silver prima del matching per evitare di basare il risultato finale su input già corrotti o non coerenti.
# MAGIC

# COMMAND ----------

date_metrics = trip_updates_silver.agg(
    F.count("*").alias("total"),
    F.count(F.when(F.col("service_date").isNull(), 1)).alias("invalid"),
    F.count(F.when(F.col("_service_date_repaired"), 1)).alias("repaired"),
).first().asDict()
worst_snapshot_rate = 0.0
if date_metrics["invalid"]:
    snapshot_quality = trip_updates_silver.groupBy("feed_timestamp").agg(
        F.count("*").alias("total"),
        F.count(F.when(F.col("service_date").isNull(), 1)).alias("invalid"),
    )
    worst_snapshot_rate = snapshot_quality.agg(
        F.max(F.col("invalid") / F.col("total"))).first()[0]
    # Date-only, bounded diagnostics; observation identifiers/payloads are not logged.
    samples = trip_updates_silver.filter(F.col("service_date").isNull()).select(
        "_service_date_raw").distinct().limit(10).collect()
    print({"service_date_invalid_samples": [
        {"value": str(row[0])[:32], "parseable": normalize_service_date(row[0]) is not None}
        for row in samples]})
print({"service_date_quality_before_gate": date_metrics,
       "worst_snapshot_invalid_rate": worst_snapshot_rate})
try:
    service_date_quality = validate_service_date_quality(
        date_metrics["total"], date_metrics["invalid"], date_metrics["repaired"],
        worst_snapshot_rate)
except ValueError:
    print({"service_date_quality_status": "REJECTED", "write_blocked": True})
    raise
print({"service_date_quality": service_date_quality})
trip_updates_silver = trip_updates_silver.filter(F.col("service_date").isNotNull()).drop(
    "_service_date_raw", "_service_date_repaired")
if trip_updates_silver.filter(F.col("service_date").isNull()).limit(1).count():
    raise ValueError("trip_updates: invalid service date escaped isolation; write blocked")
for name, frame in [("trip_updates", trip_updates_silver), ("vehicle_positions", vehicle_positions_silver)]:
    # VehiclePosition trip/service dates remain optional, as in the original parser.
    print({"silver_input": name, "rows": frame.count(),
           "unknown_service_dates": frame.filter(F.col("service_date").isNull()).count()})


# COMMAND ----------

# MAGIC %md
# MAGIC ## F. Causal backward as-of matching (180s maximum staleness)
# MAGIC A causal backward as-of join selects the latest vehicle position at or before each Trip Update timestamp, with a maximum staleness of 180 seconds. Only positions already available by vehicle timestamp are eligible, preventing future-position temporal leakage in downstream machine-learning features.
# MAGIC

# COMMAND ----------

from pyspark.sql.window import Window

tu = trip_updates_silver.alias("tu")
vp = vehicle_positions_silver.alias("vp")

candidates = (
    tu.join(
        vp,
        (
            (F.col("tu.vehicle_id") == F.col("vp.vehicle_id")) &
            (F.col("tu.trip_id") == F.col("vp.trip_id")) &
            (F.col("vp.vehicle_timestamp") <= F.col("tu.feed_timestamp")) &
            (F.col("vp.feed_timestamp") <= F.col("tu.feed_timestamp")) &
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
    .orderBy(F.col("position_time_diff_seconds").asc_nulls_last(),
             F.col("vp.feed_timestamp").desc_nulls_last(), F.col("vp._ingestion_timestamp").desc_nulls_last(),
             F.col("vp.entity_id").asc_nulls_last())
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

# All three outputs are validated in memory before any overwrite below.


# COMMAND ----------

# MAGIC %md
# MAGIC ## I. Final validation
# MAGIC Concludo con la validazione finale del match rate: total rows, matched rows, unmatched rows e percentuale di matching calcolata sui dati reali.
# MAGIC

# COMMAND ----------

final_observations = realtime_trip_stop_observations
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Controlled persistence after complete validation
# MAGIC Full rebuild from pinned Bronze; no append watermark exists. Default write gate stays OFF.
# MAGIC The feed-time guard excludes positions published after the observation, even with an older vehicle timestamp.
# MAGIC This is not a cross-table transaction. Downstream consumers require successful run completion.

# COMMAND ----------

outputs = {"trip_updates": trip_updates_silver, "vehicle_positions": vehicle_positions_silver,
           "realtime_trip_stop_observations": realtime_trip_stop_observations}
expected_counts = {}
for name, frame in outputs.items():
    destination = "rome_transport.silver." + name
    expected = {f.name: f.dataType.simpleString() for f in spark.table(destination).schema.fields}
    actual = {f.name: f.dataType.simpleString() for f in frame.schema.fields}
    if actual != expected:
        raise ValueError(f"{name}: schema differs from existing Silver; write blocked")
    expected_counts[name] = frame.count()
    if not expected_counts[name]:
        raise ValueError(f"{name}: empty rebuild; write blocked")
    source_max = sources["vehicle_positions" if name == "vehicle_positions" else "trip_updates"].agg(
        F.max("feed_timestamp")).first()[0]
    if frame.agg(F.max("feed_timestamp")).first()[0] != source_max:
        raise ValueError(f"{name}: latest source epoch lost; write blocked")
if not ALLOW_SILVER_OVERWRITE:
    raise RuntimeError("Silver validation passed; overwrite disabled until explicitly authorized")
written_versions = {}
for name, frame in outputs.items():
    destination = "rome_transport.silver." + name
    frame.write.format("delta").mode("overwrite").saveAsTable(destination)
    version = int(spark.sql(f"DESCRIBE HISTORY {destination} LIMIT 1").first()["version"])
    saved = spark.read.option("versionAsOf", version).table(destination)
    if saved.count() != expected_counts[name]:
        raise ValueError(f"{name}: read-back count mismatch; stop downstream")
    written_versions[name] = version
print({"source_versions": source_versions, "written_versions": written_versions,
       "counts": expected_counts, "quality_status": "PASSED"})
