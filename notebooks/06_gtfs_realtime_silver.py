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

import hashlib
import json
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

# ---- Run modes -------------------------------------------------------------------------
# full (default): unchanged full rebuild from the pinned Bronze versions.
# incremental_shadow: controlled verification only; computes the incremental window, compares it with the
#   existing Silver, never writes. Must be requested explicitly; never an ordinary automatic run.
# incremental: recomputes only [watermark - safety window, latest] and replaces exactly that window.
#   Needs allow_silver_overwrite AND allow_incremental_write; any inconsistency fails without writing
#   and never falls back to a full rebuild by itself.
RUN_MODES = ("full", "incremental_shadow", "incremental")
VP_MAX_STALENESS_SECONDS = 180
SILVER_OUTPUTS = ("trip_updates", "vehicle_positions", "realtime_trip_stop_observations")


def read_widget(name, default):
    try:
        try:
            dbutils.widgets.text(name, str(default))
        except Exception:
            pass
        return dbutils.widgets.get(name)
    except Exception:
        return str(default)


def resolve_run_mode(value):
    mode = str(value if value is not None else "full").strip().lower() or "full"
    if mode not in RUN_MODES:
        raise ValueError(f"unknown run_mode {mode!r}; expected one of {RUN_MODES}")
    return mode


def parse_positive_int(value, name):
    try:
        number = int(str(value).strip())
    except (TypeError, ValueError):
        raise ValueError(f"{name} must be a positive integer") from None
    if number <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return number


def plan_incremental_window(watermarks, source_max_feed, safety_seconds, max_window_seconds):
    """Pure planner. The window starts below the OLDEST output watermark, so a run that partially wrote
    (outputs at different watermarks) is repaired by simply re-running the same window."""
    if not watermarks or any(value is None for value in watermarks.values()):
        raise ValueError("incremental needs populated Silver outputs; run the explicit full rebuild first")
    if source_max_feed is None:
        raise ValueError("incremental: Bronze trip_updates is empty; write blocked")
    safety_seconds = parse_positive_int(safety_seconds, "safety_window_seconds")
    max_window_seconds = parse_positive_int(max_window_seconds, "max_window_seconds")
    watermark = min(int(value) for value in watermarks.values())
    source_max_feed = int(source_max_feed)
    if source_max_feed < watermark:
        raise ValueError("incremental: Bronze is behind Silver; inconsistent state, write blocked")
    start = watermark - safety_seconds
    if source_max_feed - start > max_window_seconds:
        raise ValueError("incremental: window exceeds max_window_seconds; run the explicit full rebuild")
    return {"start": start, "watermark": watermark, "source_max_feed": source_max_feed,
            "window_seconds": source_max_feed - start}


def assert_epoch_column(frame, column="feed_timestamp"):
    if frame.schema[column].dataType.simpleString() not in ("bigint", "int"):
        raise ValueError(f"{column} is not an integral epoch; incremental disabled")


def scope_from(frame, start, column="feed_timestamp"):
    """Row filter on the (immutable per snapshot) epoch; no-op for the full rebuild."""
    if start is None:
        return frame
    assert_epoch_column(frame, column)
    return frame.filter(F.col(column) >= F.lit(int(start)))


def vehicle_epoch_expr(column="vehicle_timestamp"):
    return F.when(F.trim(F.col(column).cast("string")).rlike(r"^-?\d+$"), F.col(column).cast("long"))


def count_late_arrivals(bronze_df, boundary, ingestion_cutoff):
    """Rows OLDER than the window but ingested after the last processed ingestion cannot be repaired
    by the window: fail instead of silently skipping them."""
    if ingestion_cutoff is None:
        raise ValueError("incremental needs a Silver ingestion cutoff; run the explicit full rebuild first")
    return bronze_df.filter((F.col("feed_timestamp") < F.lit(int(boundary)))
                            & (F.col("_ingestion_timestamp") > F.lit(ingestion_cutoff))).limit(1).count()


def count_vp_lookback_escapes(bronze_vp, vp_start):
    """Positions the causal join could use (vehicle_timestamp inside the lookback) that the feed-time
    scope would miss, e.g. a vehicle clock ahead of the feed clock."""
    return bronze_vp.filter((F.col("feed_timestamp") < F.lit(int(vp_start)))
                            & (vehicle_epoch_expr() >= F.lit(int(vp_start)))).limit(1).count()


def lost_window_keys(existing_window, new_window, keys):
    """Bronze never deletes rows, so every previously published key must survive the recompute."""
    return existing_window.select(*keys).exceptAll(new_window.select(*keys)).limit(1).count()


def overlap_difference(existing, new, upto):
    """Row-level differences restricted to the already-published region (feed <= upto)."""
    old_part = existing.filter(F.col("feed_timestamp") <= F.lit(int(upto)))
    new_part = new.filter(F.col("feed_timestamp") <= F.lit(int(upto)))
    return {"only_existing": old_part.exceptAll(new_part).count(),
            "only_incremental": new_part.exceptAll(old_part).count()}


def replace_window(frame, destination, start, expected_window_rows):
    """Idempotent replaceWhere of exactly [start, +inf) with before/after conservation checks."""
    start = int(start)
    predicate = f"feed_timestamp >= {start}"
    before = spark.table(destination)
    before_total = before.count()
    before_window = before.filter(predicate).count()
    frame.write.format("delta").mode("overwrite").option("replaceWhere", predicate).saveAsTable(destination)
    version = int(spark.sql(f"DESCRIBE HISTORY {destination} LIMIT 1").first()["version"])
    saved = spark.read.option("versionAsOf", version).table(destination)
    if (saved.count() != before_total - before_window + expected_window_rows
            or saved.filter(predicate).count() != expected_window_rows):
        raise ValueError(f"{destination}: window read-back mismatch; stop downstream")
    return version


RUN_MODE = resolve_run_mode(read_widget("run_mode", "full"))
ALLOW_INCREMENTAL_WRITE = str(read_widget("allow_incremental_write", "false")).lower() == "true"
INCREMENTAL_PLAN = None
SCOPE_START = None      # None: full rebuild
VP_SCOPE_START = None
if RUN_MODE != "full":
    _silver = {name: spark.table("rome_transport.silver." + name) for name in SILVER_OUTPUTS}
    for _frame in _silver.values():
        assert_epoch_column(_frame)
    assert_epoch_column(sources["trip_updates"])
    assert_epoch_column(sources["vehicle_positions"])
    INCREMENTAL_PLAN = plan_incremental_window(
        {"trip_updates": _silver["trip_updates"].agg(F.max("feed_timestamp")).first()[0],
         "realtime_trip_stop_observations":
             _silver["realtime_trip_stop_observations"].agg(F.max("feed_timestamp")).first()[0]},
        sources["trip_updates"].agg(F.max("feed_timestamp")).first()[0],
        read_widget("safety_window_seconds", 3600), read_widget("max_window_seconds", 21600))
    SCOPE_START = INCREMENTAL_PLAN["start"]
    VP_SCOPE_START = SCOPE_START - VP_MAX_STALENESS_SECONDS
    _tu_cutoff = _silver["trip_updates"].agg(F.max("_ingestion_timestamp")).first()[0]
    _quarantine_cutoff = spark.table("rome_transport.silver.trip_updates_quarantine").agg(
        F.max("_ingestion_timestamp")).first()[0]
    _tu_cutoff = max([c for c in (_tu_cutoff, _quarantine_cutoff) if c is not None], default=None)
    _vp_cutoff = _silver["vehicle_positions"].agg(F.max("_ingestion_timestamp")).first()[0]
    if count_late_arrivals(sources["trip_updates"], SCOPE_START, _tu_cutoff):
        raise ValueError("trip_updates: late arrival older than the safety window; write blocked")
    if count_late_arrivals(sources["vehicle_positions"], VP_SCOPE_START, _vp_cutoff):
        raise ValueError("vehicle_positions: late arrival older than the lookback window; write blocked")
    if count_vp_lookback_escapes(sources["vehicle_positions"], VP_SCOPE_START):
        raise ValueError("vehicle_positions: usable positions outside the feed-time scope; write blocked")
    trip_updates = scope_from(trip_updates, SCOPE_START)
    vehicle_positions = scope_from(vehicle_positions, VP_SCOPE_START)
    service_alerts = service_alerts.limit(0)   # not an input of 06 outputs; never scanned incrementally
print({"run_mode": RUN_MODE, "incremental_plan": INCREMENTAL_PLAN,
       "incremental_write_enabled": ALLOW_INCREMENTAL_WRITE})


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
    # service_alerts is not an input of the three Silver outputs: no scan outside the full rebuild.
    "service_alerts": service_alerts.count() if RUN_MODE == "full" else None,
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

# Reviewed, exact-identity exception (2026-11-20). Bronze is NOT modified.
# Scope: ONLY the 32 rows of entity 1255 / trip 101528146R50972 in snapshot 1794498556 (transient feed glitch:
# trip descriptor with empty start_date). The single entity-1308 row of that snapshot is NOT registered and stays
# under the ordinary 0.1% gate. Other identities or changes to the checked profile remain unreviewed.
REVIEWED_SERVICE_DATE_EXCEPTIONS = {
    1794498556: {
        "entity_id": "1255", "trip_id": "101528146R50972", "n": 32,
        "vehicle_id": "7740", "direction_id": "0",   # content profile: route_id/start_time must also be empty
        "sha256": "f1e98d9ab7629000f335e0d341c3e4b018bfbe46ba92af06aece03f97ea73867",
        "stop_keys": [
        ("10", "80519"),
        ("11", "71629"),
        ("12", "78905"),
        ("13", "78975"),
        ("14", "78906"),
        ("15", "71630"),
        ("16", "71631"),
        ("17", "71632"),
        ("18", "71633"),
        ("19", "76179"),
        ("2", "71620"),
        ("20", "79313"),
        ("21", "76180"),
        ("22", "76181"),
        ("23", "76182"),
        ("24", "76436"),
        ("25", "76437"),
        ("26", "76438"),
        ("27", "76439"),
        ("28", "76440"),
        ("29", "81579"),
        ("3", "71621"),
        ("30", "74268"),
        ("31", "80492"),
        ("32", "74271"),
        ("33", "81578"),
        ("4", "71622"),
        ("5", "71623"),
        ("6", "75683"),
        ("7", "71625"),
        ("8", "71626"),
        ("9", "80518"),
        ],
    },
}


def reviewed_exception_keys(snapshot, entry):
    return [(str(snapshot), entry["entity_id"], entry["trip_id"], seq, stop) for seq, stop in entry["stop_keys"]]


def validate_reviewed_service_date_registry(registry):
    """Verify the reviewed identities before constructing any exception."""
    for _snap, _entry in registry.items():
        _keys = reviewed_exception_keys(_snap, _entry)
        _digest = hashlib.sha256("\n".join("|".join(k) for k in sorted(_keys)).encode()).hexdigest()
        if len(set(_keys)) != _entry["n"] or len(_keys) != _entry["n"] or _digest != _entry["sha256"]:
            raise ValueError("reviewed service_date exception registry corrupted; write blocked")


def reviewed_service_date_exception_expr(registry):
    """Only the reviewed key and missing-date content profile may bypass the gate."""
    validate_reviewed_service_date_registry(registry)
    _reviewed_expr = F.lit(False)
    for _snap, _entry in registry.items():
        _ids = ["|".join(k[3:]) for k in reviewed_exception_keys(_snap, _entry)]
        _reviewed_expr = _reviewed_expr | (
            (F.col("feed_timestamp").cast("string") == str(_snap))
            & (F.col("entity_id").cast("string") == _entry["entity_id"])
            & (F.col("trip_id").cast("string") == _entry["trip_id"])
            & (F.col("vehicle_id").cast("string") == _entry["vehicle_id"])
            & (F.col("direction_id").cast("string") == _entry["direction_id"])
            & (F.col("route_id").cast("string") == "")
            & (F.col("start_time").cast("string") == "")
            & (F.col("_service_date_raw").cast("string") == "")
            & F.col("service_date").isNull()
            & F.concat_ws("|", F.col("stop_sequence").cast("string"), F.col("stop_id").cast("string")).isin(_ids))
    return F.coalesce(_reviewed_expr, F.lit(False))


def validate_service_date_conservation(total, silver_rows, reviewed_rows, invalid_rows):
    """Every deduplicated input belongs to exactly one output/exclusion count."""
    if any(type(value) is not int or value < 0
           for value in (total, silver_rows, reviewed_rows, invalid_rows)):
        raise ValueError("invalid conservation counts; write blocked")
    if total != silver_rows + reviewed_rows + invalid_rows:
        raise ValueError("trip_updates conservation violated; write blocked")


def write_reviewed_quarantine(frame, expected_keys):
    """Overwrite the reviewed isolation first; verify actual persisted identities."""
    if len(expected_keys) != len(set(expected_keys)):
        raise ValueError("duplicate quarantine identity; write blocked")
    registered = {key for snapshot, entry in REVIEWED_SERVICE_DATE_EXCEPTIONS.items()
                  for key in reviewed_exception_keys(snapshot, entry)}
    if not set(expected_keys).issubset(registered):
        raise ValueError("unreviewed quarantine identity; write blocked")
    destination = "rome_transport.silver.trip_updates_quarantine"
    frame.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(destination)
    saved = spark.table(destination)
    if saved.schema != frame.schema:
        raise ValueError("quarantine read-back schema mismatch; stop downstream")
    actual_keys = [tuple(str(value) for value in row)
                   for row in saved.select(*trip_updates_dedup_key).limit(len(registered) + 1).collect()]
    if sorted(actual_keys) != sorted(expected_keys):
        raise ValueError("quarantine read-back identity mismatch; stop downstream")


trip_updates_silver = trip_updates_silver.withColumn(
    "_reviewed_exception", reviewed_service_date_exception_expr(REVIEWED_SERVICE_DATE_EXCEPTIONS))
_unreviewed_invalid = F.col("service_date").isNull() & ~F.col("_reviewed_exception")

date_metrics = trip_updates_silver.agg(
    F.count("*").alias("total"),
    F.count(F.when(_unreviewed_invalid, 1)).alias("invalid"),   # reviewed exception rows are excluded from the gate
    F.count(F.when(F.col("_service_date_repaired"), 1)).alias("repaired"),
).first().asDict()
worst_snapshot_rate = 0.0
if date_metrics["invalid"]:
    snapshot_quality = trip_updates_silver.groupBy("feed_timestamp").agg(
        F.count("*").alias("total"),
        F.count(F.when(_unreviewed_invalid, 1)).alias("invalid"),
    )
    worst_snapshot_rate = snapshot_quality.agg(
        F.max(F.col("invalid") / F.col("total"))).first()[0]
    # Date-only, bounded diagnostics; observation identifiers/payloads are not logged.
    samples = trip_updates_silver.filter(_unreviewed_invalid).select(
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
quarantine_df = trip_updates_silver.filter(F.col("service_date").isNull() & F.col("_reviewed_exception")).drop(
    "_service_date_repaired", "_reviewed_exception")
quarantine_count = quarantine_df.count()
if quarantine_count > sum(e["n"] for e in REVIEWED_SERVICE_DATE_EXCEPTIONS.values()):
    raise ValueError("quarantine larger than reviewed registry; write blocked")
if RUN_MODE != "full" and quarantine_count not in (0, sum(e["n"] for e in REVIEWED_SERVICE_DATE_EXCEPTIONS.values())):
    # The quarantine table is a full-registry table: a partial slice must never overwrite it.
    raise ValueError("incremental: partial quarantine slice; run the explicit full rebuild")
trip_updates_silver = trip_updates_silver.filter(F.col("service_date").isNotNull()).drop(
    "_service_date_raw", "_service_date_repaired", "_reviewed_exception")
silver_filtered_count = trip_updates_silver.count()
validate_service_date_conservation(
    date_metrics["total"], silver_filtered_count, quarantine_count, date_metrics["invalid"])
# The unchanged gate counts only unreviewed invalids. Keep its diagnostics honest:
# reviewed rows are also excluded, and must not be reported as valid Silver rows.
service_date_quality.update(
    reviewed_quarantine_rows=quarantine_count,
    excluded_rows=quarantine_count + date_metrics["invalid"],
    valid_rows=silver_filtered_count - date_metrics["repaired"],
)
quarantine_keys = [tuple(str(value) for value in row)
                   for row in quarantine_df.select(*trip_updates_dedup_key).limit(33).collect()]
if len(quarantine_keys) != quarantine_count:
    raise ValueError("quarantine key count mismatch; write blocked")
print({"service_date_quality": service_date_quality})
print({"quarantine_rows": quarantine_count, "unreviewed_invalid_dropped": date_metrics["invalid"],
       "silver_rows": silver_filtered_count})
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

OUTPUT_KEYS = {
    "trip_updates": trip_updates_dedup_key,
    "vehicle_positions": ["feed_timestamp", "entity_id"],
    "realtime_trip_stop_observations": ["feed_timestamp", "entity_id", "trip_id", "stop_sequence", "stop_id"],
}
# The join reads positions down to SCOPE_START - 180; the published window starts at SCOPE_START.
vehicle_positions_out = (vehicle_positions_silver if SCOPE_START is None
                         else vehicle_positions_silver.filter(F.col("feed_timestamp") >= F.lit(SCOPE_START)))
outputs = {"trip_updates": trip_updates_silver, "vehicle_positions": vehicle_positions_out,
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

if RUN_MODE != "full":
    for name, frame in outputs.items():
        existing = spark.table("rome_transport.silver." + name)
        existing_window = existing.filter(F.col("feed_timestamp") >= F.lit(SCOPE_START))
        if lost_window_keys(existing_window, frame, OUTPUT_KEYS[name]):
            raise ValueError(f"{name}: published keys lost by the recompute; write blocked")
        if RUN_MODE == "incremental_shadow":
            upto = INCREMENTAL_PLAN["watermark"]
            print({"shadow": name, "window_rows": expected_counts[name],
                   "overlap_difference": overlap_difference(existing_window, frame, upto)})
    if RUN_MODE == "incremental_shadow":
        print({"run_mode": RUN_MODE, "written": False, "quality_status": "SHADOW_PASSED"})
    elif not (ALLOW_SILVER_OVERWRITE and ALLOW_INCREMENTAL_WRITE):
        raise RuntimeError("Incremental validation passed; write disabled until both write widgets are true")
    else:
        # Quarantine only when this window actually contains the reviewed rows; otherwise it stays untouched.
        if quarantine_count:
            write_reviewed_quarantine(quarantine_df, quarantine_keys)
        # Order matters: observations LAST so 07 never reads a half-run. A partial write is repaired by
        # re-running the same window (min-watermark planner + idempotent replaceWhere).
        written_versions = {name: replace_window(frame, "rome_transport.silver." + name, SCOPE_START,
                                                 expected_counts[name])
                            for name, frame in outputs.items()}
        print({"source_versions": source_versions, "written_versions": written_versions,
               "window_start": SCOPE_START, "counts": expected_counts, "quality_status": "PASSED"})
    dbutils.notebook.exit(json.dumps({"run_mode": RUN_MODE, "status": "DONE"}))

# Full rebuild (default).
if not ALLOW_SILVER_OVERWRITE:
    raise RuntimeError("Silver validation passed; overwrite disabled until explicitly authorized")
# Quarantine FIRST (full rebuild, idempotent); Silver observations stay LAST so 07 never sees a half-run.
write_reviewed_quarantine(quarantine_df, quarantine_keys)
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
