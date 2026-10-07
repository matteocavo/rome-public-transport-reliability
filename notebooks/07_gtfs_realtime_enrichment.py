# Databricks notebook source
# MAGIC %md
# MAGIC # GTFS Realtime Enrichment
# MAGIC Enrich Rome Public Transport Reliability & Delay Prediction observations with scheduled service, metadata and active service-alert context after notebooks 03, 05 and 06.
# MAGIC The output preserves one row per realtime trip-stop observation and feed snapshot; ML features, lag/rolling calculations and target definitions belong to the next notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Runtime Configuration
# MAGIC Use the existing Databricks Spark session and Unity Catalog Delta tables, with Europe/Rome as the schedule timezone.
# MAGIC Transformations use PySpark and SQL without external packages, local files, RDDs or caching, consistent with [Serverless limitations](https://docs.databricks.com/aws/en/compute/serverless/limitations).

# COMMAND ----------

from functools import reduce
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark.conf.set("spark.sql.session.timeZone", "Europe/Rome")
OUTPUT_TABLE = "rome_transport.silver.realtime_enriched_observations"
OBS_KEY = ["feed_timestamp", "entity_id", "trip_id", "stop_sequence", "stop_id"]
SCHEDULE_KEY = ["trip_id", "stop_sequence", "stop_id"]
SOURCE_TABLES = {
    "realtime_observations": "rome_transport.silver.realtime_trip_stop_observations",
    "trip_stop_schedule": "rome_transport.silver.trip_stop_schedule",
    "service_alerts": "rome_transport.bronze.service_alerts",
    "service_calendar": "rome_transport.silver.service_calendar",
}

def require_columns(df, required, label):
    missing = sorted(set(required) - set(df.columns))
    if missing:
        raise ValueError(f"{label}: missing required columns {missing}")

def n_where(condition):
    return F.coalesce(F.sum(F.when(condition, 1).otherwise(0)), F.lit(0))

def duplicate_report(df, keys, label):
    groups = df.groupBy(*keys).count().filter(F.col("count") > 1)
    summary = groups.agg(
        F.count("*").alias("duplicate_groups"),
        F.coalesce(F.sum(F.col("count") - 1), F.lit(0)).alias("duplicate_extra_rows"),
    ).first().asDict()
    print(label, summary)
    if summary["duplicate_groups"]:
        groups.orderBy(F.col("count").desc(), *keys).show(20, truncate=False)
    return summary

def unique_dimension(df, keys, label):
    duplicate_report(df, keys, f"{label}: before exact deduplication")
    distinct_rows = df.dropDuplicates()  # Only identical selected records are interchangeable.
    report = duplicate_report(distinct_rows, keys, f"{label}: after exact deduplication")
    if report["duplicate_extra_rows"]:
        raise ValueError(f"{label}: conflicting dimension keys; resolve upstream before saving")
    return distinct_rows

def require_same_count(df, expected, label):
    actual = df.count()
    print(f"{label}: {actual} rows; expected {expected}")
    if actual != expected:
        raise ValueError(f"{label}: grain changed; output write blocked")
    return actual

def scheduled_datetime(seconds_column):
    return F.expr(
        f"timestampadd(SECOND, CAST(`{seconds_column}` AS BIGINT), "
        "CAST(service_date AS TIMESTAMP))"
    )

def active_at(periods, timestamp):
    return F.exists(
        periods,
        lambda p: (p["active_start"].isNull() | (timestamp >= p["active_start"]))
        & (p["active_end"].isNull() | (timestamp < p["active_end"])),
    )

def preferred_translation(column_name):
    translations = F.col(column_name)
    italian = F.filter(translations, lambda t: F.lower(t["language"]) == F.lit("it"))
    return F.coalesce(
        F.try_element_at(italian, F.lit(1))["text"],
        F.try_element_at(translations, F.lit(1))["text"],
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Silver Sources
# MAGIC Load the four required sources at explicit Delta versions so repeated actions within this run read stable inputs.
# MAGIC The schedule already includes route, stop and agency metadata, so additional Bronze dimension joins are unnecessary; printed versions support reproducible reruns while those Delta versions remain retained.

# COMMAND ----------

# Optionally supply previously printed versions to reproduce an earlier run.
SOURCE_VERSION_OVERRIDES = {}
source_versions = {}
sources = {}
for name, table in SOURCE_TABLES.items():  # Four metadata lookups, never observation-level loops.
    version = SOURCE_VERSION_OVERRIDES.get(name)
    if version is None:
        version = spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").select("version").first()[0]
    source_versions[name] = int(version)
    sources[name] = spark.read.option("versionAsOf", int(version)).table(table)

realtime_observations = sources["realtime_observations"]
trip_stop_schedule = sources["trip_stop_schedule"]
service_alerts = sources["service_alerts"]
service_calendar = sources["service_calendar"]
print("Source Delta versions:", source_versions)

REALTIME_COLUMNS = """feed_timestamp feed_datetime entity_id trip_id route_id service_date
start_time direction_id vehicle_id stop_sequence stop_id arrival_delay_seconds
arrival_delay_minutes predicted_arrival_datetime departure_delay_seconds departure_delay_minutes
schedule_relationship vehicle_latitude vehicle_longitude vehicle_bearing vehicle_speed
vehicle_odometer current_stop_sequence current_status vehicle_current_stop_id vehicle_datetime
position_time_diff_seconds""".split()
SCHEDULE_COLUMNS = SCHEDULE_KEY + """route_id route_short_name route_type agency_id agency_name
service_id trip_headsign direction_id shape_id stop_name stop_lat stop_lon arrival_time
departure_time arrival_seconds departure_seconds arrival_day_offset departure_day_offset""".split()
ALERT_COLUMNS = """alert_id feed_timestamp cause effect active_periods informed_entities
header_translations description_translations""".split()
require_columns(realtime_observations, REALTIME_COLUMNS, "Realtime observations")
require_columns(trip_stop_schedule, SCHEDULE_COLUMNS, "Trip-stop schedule")
require_columns(service_calendar, ["service_id", "service_date", "exception_type"], "Service calendar")
require_columns(service_alerts, ALERT_COLUMNS, "Service alerts")
for name, df in sources.items():
    print(f"{name}: {df.count()} rows")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Source Grain
# MAGIC Report source completeness and duplicates on the full logical observation key before any join.
# MAGIC No realtime rows are removed: duplicate observations block processing because preserving their count and producing a unique final grain would otherwise be incompatible.

# COMMAND ----------

source_summary = realtime_observations.agg(
    F.count("*").alias("total_rows"),
    F.countDistinct("feed_timestamp").alias("distinct_feed_timestamps"),
    F.countDistinct("trip_id").alias("distinct_trip_id"),
    F.countDistinct("vehicle_id").alias("distinct_vehicle_id"),
    n_where(F.col("trip_id").isNull()).alias("null_trip_id"),
    n_where(F.col("stop_id").isNull()).alias("null_stop_id"),
    n_where(F.col("route_id").isNull()).alias("null_route_id"),
    n_where(F.col("feed_timestamp").isNull()).alias("null_feed_timestamp"),
    n_where(~F.col("feed_datetime").eqNullSafe(
        F.timestamp_seconds(F.col("feed_timestamp"))
    )).alias("inconsistent_feed_datetime"),
).first().asDict()
print(source_summary)
source_count = source_summary["total_rows"]
source_duplicates = duplicate_report(realtime_observations, OBS_KEY, "Realtime source grain")
if source_duplicates["duplicate_extra_rows"]:
    raise ValueError("Duplicate realtime source keys: repair upstream; no rows were removed")
if source_summary["null_feed_timestamp"] or source_summary["inconsistent_feed_datetime"]:
    raise ValueError("Feed timestamps must be populated and agree with feed_datetime")
if source_count == 0:
    raise ValueError("Realtime input is empty; refusing to overwrite an existing output with no observations")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Scheduled GTFS Enrichment
# MAGIC Left join on trip, stop sequence and stop ID to retain realtime observations and add scheduled route, stop and agency attributes.
# MAGIC Exact schedule duplicates are collapsed in memory; conflicting keys stop the run instead of selecting arbitrary metadata, and the joined row count must equal the source count.

# COMMAND ----------

schedule = unique_dimension(
    trip_stop_schedule.select(*SCHEDULE_COLUMNS), SCHEDULE_KEY, "Trip-stop schedule"
).withColumn("_schedule_match", F.lit(True))
r, s = realtime_observations.alias("r"), schedule.alias("s")
schedule_condition = reduce(
    lambda a, b: a & b,
    [F.col(f"r.{key}") == F.col(f"s.{key}") for key in SCHEDULE_KEY],
)
schedule_metadata = [c for c in SCHEDULE_COLUMNS if c not in SCHEDULE_KEY + ["route_id", "direction_id"]]
scheduled_observations = r.join(s, schedule_condition, "left").select(
    "r.*",
    *[F.col(f"s.{c}").alias(c) for c in schedule_metadata],
    F.col("s.route_id").alias("scheduled_route_id"),
    F.col("s.direction_id").alias("scheduled_direction_id"),
    F.coalesce(F.col("s._schedule_match"), F.lit(False)).alias("schedule_match_flag"),
)
require_same_count(scheduled_observations, source_count, "Schedule LEFT JOIN")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Service Calendar Context
# MAGIC Left join the unique service/date calendar to expose active or removed service without filtering observed vehicles.
# MAGIC The existing calendar is derived from calendar_dates: absent records remain unknown rather than being interpreted as inactive service.

# COMMAND ----------

calendar = unique_dimension(
    service_calendar.select(
        "service_id", "service_date",
        F.expr("try_cast(exception_type AS INT)").alias("service_exception_type"),
    ),
    ["service_id", "service_date"], "Service calendar",
)
scheduled_observations = (
    scheduled_observations.join(calendar, ["service_id", "service_date"], "left")
    .withColumn(
        "service_active_flag",
        F.when(F.col("service_exception_type") == 1, True)
        .when(F.col("service_exception_type") == 2, False)
        .otherwise(F.lit(None).cast("boolean")),
    )
)
require_same_count(scheduled_observations, source_count, "Calendar LEFT JOIN")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Scheduled Datetime Construction
# MAGIC Add cumulative GTFS seconds to service-date midnight in Europe/Rome, preserving hours beyond 24:00 and the original day offsets.
# MAGIC This follows the requested midnight arithmetic using [timestampadd](https://docs.databricks.com/aws/en/sql/language-manual/functions/timestampadd); daylight-saving transition dates are flagged because GTFS service-time conventions need a separate transition-day review.

# COMMAND ----------

scheduled_observations = (
    scheduled_observations
    .withColumn("scheduled_arrival_datetime", scheduled_datetime("arrival_seconds"))
    .withColumn("scheduled_departure_datetime", scheduled_datetime("departure_seconds"))
    .withColumn(
        "service_date_dst_transition_flag",
        (F.date_add("service_date", 1).cast("timestamp").cast("long")
         - F.col("service_date").cast("timestamp").cast("long")) != 86400,
    )
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Delay Validation
# MAGIC Preserve feed delays and predictions, and reconstruct arrival delay only where predicted and scheduled datetimes exist.
# MAGIC The consistency difference is reconstructed delay minus feed delay, strictly a quality diagnostic rather than a target or a corrected feed value.

# COMMAND ----------

scheduled_observations = (
    scheduled_observations
    .withColumn(
        "calculated_arrival_delay_seconds",
        F.col("predicted_arrival_datetime").cast("long")
        - F.col("scheduled_arrival_datetime").cast("long"),
    )
    .withColumn(
        "arrival_delay_consistency_diff_seconds",
        F.col("calculated_arrival_delay_seconds") - F.col("arrival_delay_seconds"),
    )
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Service Alert Normalization
# MAGIC Create an in-memory Silver DataFrame at alert ID × feed timestamp × distinct informed entity, preferring Italian text with a first-translation fallback.
# MAGIC Keep all active intervals in an array: scalar active_start/active_end describe the outer bounds for inspection only, while matching checks each interval separately under the [GTFS start-inclusive, end-exclusive rule](https://gtfs.org/documentation/realtime/reference/#message-timerange).

# COMMAND ----------

alerts = unique_dimension(
    service_alerts.select(*ALERT_COLUMNS), ["alert_id", "feed_timestamp"], "Alert snapshots"
)
if alerts.filter(F.col("alert_id").isNull() | F.col("feed_timestamp").isNull()).limit(1).count():
    raise ValueError("Alert ID and feed timestamp cannot be null")
if alerts.filter(F.exists(
    "active_periods", lambda p: p.isNull() | (
        p["start"].isNotNull() & p["end"].isNotNull() & (p["end"] <= p["start"])
    )
)).limit(1).count():
    raise ValueError("Malformed alert active periods; inspect Bronze without modifying it")

open_period = F.array(F.struct(
    F.lit(None).cast("timestamp").alias("active_start"),
    F.lit(None).cast("timestamp").alias("active_end"),
))
alerts = (
    alerts
    .withColumn("header_text", preferred_translation("header_translations"))
    .withColumn("description_text", preferred_translation("description_translations"))
    .withColumn(
        "normalized_active_periods",
        F.when(F.coalesce(F.size("active_periods"), F.lit(0)) == 0, open_period)
        .otherwise(F.transform("active_periods", lambda p: F.struct(
            F.timestamp_seconds(p["start"]).alias("active_start"),
            F.timestamp_seconds(p["end"]).alias("active_end"),
        ))),
    )
    .withColumn("informed_entity", F.explode_outer(F.array_distinct("informed_entities")))
)
alerts_normalized = alerts.select(
    "alert_id",
    F.col("feed_timestamp").alias("alert_feed_timestamp"),
    F.timestamp_seconds("feed_timestamp").alias("alert_feed_datetime"),
    "cause", "effect",
    *[F.col(f"informed_entity.{c}").alias(c) for c in
      ["agency_id", "route_id", "route_type", "stop_id", "direction_id", "trip_id"]],
    "informed_entity", "normalized_active_periods", "header_text", "description_text",
).withColumn(
    "active_start",
    F.when(~F.exists("normalized_active_periods", lambda p: p["active_start"].isNull()),
           F.array_min(F.transform("normalized_active_periods", lambda p: p["active_start"]))),
).withColumn(
    "active_end",
    F.when(~F.exists("normalized_active_periods", lambda p: p["active_end"].isNull()),
           F.array_max(F.transform("normalized_active_periods", lambda p: p["active_end"]))),
)
normalization_report = duplicate_report(
    alerts_normalized, ["alert_id", "alert_feed_timestamp", "informed_entity"], "Normalized alerts"
)
if normalization_report["duplicate_extra_rows"]:
    raise ValueError("Normalized alert grain is not unique")
alerts_normalized.groupBy("cause", "effect").count().orderBy("cause", "effect").show(50, truncate=False)
print("Alert entities without route_id (not eligible for route matching):",
      alerts_normalized.filter(F.col("route_id").isNull()).count())
alerts_normalized.orderBy("alert_feed_timestamp", "alert_id", "route_id", "stop_id").show(20, truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Alert Snapshot Alignment
# MAGIC Map each observation feed timestamp to the latest recorded alert snapshot at or before it, then use equality joins for alert matching.
# MAGIC The ordered window runs only over distinct snapshot timestamps and creates no ML lag or rolling features; selecting a whole snapshot prevents superseded alert versions from remaining active after removal.

# COMMAND ----------

def align_alert_snapshots(observation_times, alert_times):
    timeline = (
        observation_times.select(F.col("feed_timestamp").alias("timeline_timestamp"))
        .withColumn("available_alert_timestamp", F.lit(None).cast("long"))
        .unionByName(alert_times.select(
            F.col("feed_timestamp").alias("timeline_timestamp"),
            F.col("feed_timestamp").alias("available_alert_timestamp"),
        ))
        .groupBy("timeline_timestamp")
        .agg(F.max("available_alert_timestamp").alias("available_alert_timestamp"))
    )
    snapshot_window = Window.orderBy("timeline_timestamp").rowsBetween(
        Window.unboundedPreceding, Window.currentRow
    )
    timeline = timeline.withColumn(
        "matched_alert_feed_timestamp",
        F.last("available_alert_timestamp", ignorenulls=True).over(snapshot_window),
    ).select(F.col("timeline_timestamp").alias("feed_timestamp"), "matched_alert_feed_timestamp")
    return observation_times.join(timeline, "feed_timestamp", "left")

snapshot_alignment = align_alert_snapshots(
    realtime_observations.select("feed_timestamp").distinct(),
    service_alerts.select("feed_timestamp").distinct(),
)
observations_with_snapshot = (
    scheduled_observations.join(snapshot_alignment, "feed_timestamp", "left")
    .withColumn("alert_snapshot_available_flag", F.col("matched_alert_feed_timestamp").isNotNull())
    .withColumn("alert_snapshot_age_seconds",
                F.col("feed_timestamp") - F.col("matched_alert_feed_timestamp"))
)
require_same_count(observations_with_snapshot, source_count, "Alert snapshot alignment")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Active Alert Matching
# MAGIC Match snapshot and route first, then require the observation time to fall inside at least one active interval, treating null boundaries as open.
# MAGIC Apply stop and other entity selectors when supplied so a stop-specific alert does not affect every stop on the route; all realtime rows survive the left join.

# COMMAND ----------

o, a = observations_with_snapshot.alias("o"), alerts_normalized.alias("a")
alert_condition = (
    (F.col("o.matched_alert_feed_timestamp") == F.col("a.alert_feed_timestamp"))
    & (F.col("o.route_id") == F.col("a.route_id"))
    & active_at(F.col("a.normalized_active_periods"), F.col("o.feed_datetime"))
)
for selector in ["stop_id", "agency_id", "route_type", "direction_id", "trip_id"]:
    alert_condition = alert_condition & (
        F.col(f"a.{selector}").isNull()
        | (F.col(f"a.{selector}").cast("string") == F.col(f"o.{selector}").cast("string"))
    )
alert_matches = o.join(a, alert_condition, "left").select(
    *[F.col(f"o.{key}").alias(key) for key in OBS_KEY],
    F.col("a.alert_id").alias("alert_id"),
    F.col("a.cause").alias("cause"),
    F.col("a.effect").alias("effect"),
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregate Alerts per Observation
# MAGIC Collapse overlapping intervals, repeated entity matches and simultaneous alerts into distinct, sorted arrays at the original observation grain.
# MAGIC Counts use distinct alert IDs; detour and construction flags are derived from observed effect/cause values while the complete vocabulary remains available in the arrays.

# COMMAND ----------

alert_context = alert_matches.groupBy(*OBS_KEY).agg(
    F.sort_array(F.collect_set("alert_id")).alias("active_alert_ids"),
    F.sort_array(F.collect_set("cause")).alias("alert_causes"),
    F.sort_array(F.collect_set("effect")).alias("alert_effects"),
).withColumn("active_alert_count", F.size("active_alert_ids"))
alert_context = (
    alert_context
    .withColumn("active_alert_flag", F.col("active_alert_count") > 0)
    .withColumn("active_detour_flag", F.exists("alert_effects", lambda x: F.upper(x) == "DETOUR"))
    .withColumn("active_construction_flag", F.exists("alert_causes", lambda x: F.upper(x) == "CONSTRUCTION"))
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Enriched Observations
# MAGIC Attach aggregated alert context with null-safe equality on the full observation key, retaining observations with missing trip or stop identifiers.
# MAGIC The final schema preserves every original realtime column plus schedule, calendar, alert provenance and quality fields.

# COMMAND ----------

o, a = observations_with_snapshot.alias("o"), alert_context.alias("a")
context_condition = reduce(
    lambda x, y: x & y,
    [F.col(f"o.{key}").eqNullSafe(F.col(f"a.{key}")) for key in OBS_KEY],
)
ALERT_OUTPUT_COLUMNS = """active_alert_flag active_alert_count active_alert_ids alert_causes
alert_effects active_detour_flag active_construction_flag""".split()
enriched_observations = o.join(a, context_condition, "left").select(
    "o.*", *[F.col(f"a.{c}").alias(c) for c in ALERT_OUTPUT_COLUMNS]
)
FINAL_COLUMNS = """feed_timestamp feed_datetime entity_id trip_id route_id route_short_name
route_type agency_id agency_name service_date start_time direction_id vehicle_id stop_sequence
stop_id stop_name stop_lat stop_lon trip_headsign shape_id scheduled_arrival_datetime
scheduled_departure_datetime arrival_seconds departure_seconds arrival_day_offset departure_day_offset
arrival_delay_seconds arrival_delay_minutes predicted_arrival_datetime departure_delay_seconds
departure_delay_minutes schedule_relationship vehicle_latitude vehicle_longitude vehicle_bearing
vehicle_speed vehicle_odometer current_stop_sequence current_status vehicle_current_stop_id
vehicle_datetime position_time_diff_seconds active_alert_flag active_alert_count active_alert_ids
alert_causes alert_effects active_detour_flag active_construction_flag
arrival_delay_consistency_diff_seconds""".split()
require_columns(enriched_observations, FINAL_COLUMNS, "Enriched output")
enriched_observations = enriched_observations.select(
    *FINAL_COLUMNS, *[c for c in enriched_observations.columns if c not in FINAL_COLUMNS]
)
enriched_observations.printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformation Edge Checks
# MAGIC Exercise cumulative times beyond midnight, alert interval gaps and endpoints, open intervals, translation fallback and past-only snapshot alignment using small in-memory Spark fixtures.
# MAGIC These checks use the same transformation helpers as the enrichment and create no tables or persistent test artifacts.

# COMMAND ----------

time_fixture = spark.createDataFrame(
    [("2026-09-25", 91800, "2026-09-26 01:30:00"),
     ("2026-09-25", 0, "2026-09-25 00:00:00"),
     ("2026-09-25", None, None)],
    "service_date STRING, arrival_seconds LONG, expected STRING",
).withColumn("service_date", F.to_date("service_date"))
assert time_fixture.withColumn("actual", scheduled_datetime("arrival_seconds")).filter(
    ~F.col("actual").eqNullSafe(F.col("expected").cast("timestamp"))
).count() == 0

interval_fixture = spark.createDataFrame(
    [(10, True), (19, True), (20, False), (25, False), (30, True), (40, False)],
    "epoch LONG, expected BOOLEAN",
).withColumn("periods", F.array(
    F.struct(F.timestamp_seconds(F.lit(10)).alias("active_start"),
             F.timestamp_seconds(F.lit(20)).alias("active_end")),
    F.struct(F.timestamp_seconds(F.lit(30)).alias("active_start"),
             F.timestamp_seconds(F.lit(40)).alias("active_end")),
))
assert interval_fixture.filter(
    active_at(F.col("periods"), F.timestamp_seconds("epoch")) != F.col("expected")
).count() == 0
assert interval_fixture.filter(~active_at(open_period, F.timestamp_seconds("epoch"))).count() == 0

translation_fixture = spark.createDataFrame(
    [([("English", "en"), ("Italiano", "it")], "Italiano"),
     ([("Fallback", "en")], "Fallback"), ([], None)],
    "translations ARRAY<STRUCT<text:STRING,language:STRING>>, expected STRING",
)
assert translation_fixture.filter(
    ~preferred_translation("translations").eqNullSafe(F.col("expected"))
).count() == 0

alignment_fixture = align_alert_snapshots(
    spark.createDataFrame([(5,), (10,), (15,), (20,)], "feed_timestamp LONG"),
    spark.createDataFrame([(10,), (20,)], "feed_timestamp LONG"),
)
expected_alignment = spark.createDataFrame(
    [(5, None), (10, 10), (15, 10), (20, 20)],
    "feed_timestamp LONG, expected LONG",
)
assert alignment_fixture.join(expected_alignment, "feed_timestamp").filter(
    ~F.col("matched_alert_feed_timestamp").eqNullSafe(F.col("expected"))
).count() == 0
print("Transformation edge checks passed")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Validation
# MAGIC Summarize completeness, schedule coverage, alert and vehicle-position coverage, and absolute delay consistency differences before writing.
# MAGIC Additional diagnostics expose future vehicle positions inherited from notebook 06, calendar gaps, schedule identifier mismatches and daylight-saving service dates without altering feed values.

# COMMAND ----------

quality_summary = enriched_observations.agg(
    F.count("*").alias("total_rows"),
    F.countDistinct("feed_timestamp").alias("distinct_feed_snapshots"),
    F.countDistinct("trip_id").alias("distinct_trips"),
    F.countDistinct("vehicle_id").alias("distinct_vehicles"),
    n_where(~F.col("schedule_match_flag")).alias("null_schedule_matches"),
    n_where(F.col("stop_name").isNull() | F.col("stop_lat").isNull()
            | F.col("stop_lon").isNull()).alias("null_stop_metadata"),
    n_where(F.col("route_short_name").isNull() | F.col("route_type").isNull()).alias("null_route_metadata"),
    n_where(F.col("active_alert_flag")).alias("rows_with_active_alert"),
    n_where(F.col("vehicle_latitude").isNotNull()
            & F.col("vehicle_longitude").isNotNull()).alias("rows_with_vehicle_position"),
    F.avg(F.abs("arrival_delay_consistency_diff_seconds")).alias("average_abs_delay_consistency_difference"),
    F.count("arrival_delay_consistency_diff_seconds").alias("rows_with_delay_consistency_check"),
    n_where(F.col("vehicle_datetime") > F.col("feed_datetime")).alias("rows_with_future_vehicle_position"),
    n_where(~F.col("alert_snapshot_available_flag")).alias("rows_without_alert_snapshot"),
    F.max("alert_snapshot_age_seconds").alias("max_alert_snapshot_age_seconds"),
    n_where(F.col("service_active_flag").isNull()).alias("rows_with_unknown_service_status"),
    n_where(F.col("service_active_flag") == False).alias("rows_on_removed_service"),
    n_where(F.col("service_date_dst_transition_flag")).alias("rows_on_dst_transition_service_date"),
    n_where(F.col("route_id") != F.col("scheduled_route_id")).alias("route_id_mismatches"),
    n_where(F.col("direction_id") != F.col("scheduled_direction_id")).alias("direction_id_mismatches"),
).withColumn(
    "pct_rows_with_active_alert", F.round(100.0 * F.col("rows_with_active_alert") / F.col("total_rows"), 2)
).withColumn(
    "pct_rows_with_vehicle_position", F.round(100.0 * F.col("rows_with_vehicle_position") / F.col("total_rows"), 2)
)
quality_summary.show(truncate=False, vertical=True)
quality = quality_summary.first().asDict()
if quality["total_rows"] != source_count:
    raise ValueError("Final row count differs from realtime source count; write blocked")
if enriched_observations.filter(
    F.col("active_alert_count").isNull()
    | (F.col("active_alert_count") != F.size("active_alert_ids"))
    | (F.col("active_alert_flag") != (F.col("active_alert_count") > 0))
    | (F.col("matched_alert_feed_timestamp") > F.col("feed_timestamp"))
).limit(1).count():
    raise ValueError("Invalid alert aggregation or future alert snapshot; write blocked")

# Check the exact key set in both directions, not just the total count.
source_keys = realtime_observations.select(*OBS_KEY)
final_keys = enriched_observations.select(*OBS_KEY)
if source_keys.exceptAll(final_keys).limit(1).count() or final_keys.exceptAll(source_keys).limit(1).count():
    raise ValueError("Final observation keys differ from the source; write blocked")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Duplicate Check
# MAGIC Verify the final key feed_timestamp × entity_id × trip_id × stop_sequence × stop_id, reporting duplicate groups and extra rows explicitly.
# MAGIC Saving is permitted only when duplicate_extra_rows is zero and the source row count has been preserved.

# COMMAND ----------

final_duplicates = duplicate_report(enriched_observations, OBS_KEY, "Final enriched grain")
if final_duplicates["duplicate_extra_rows"] != 0:
    raise ValueError("Final grain is duplicated; output write blocked")
require_same_count(enriched_observations, source_count, "Final pre-write row count")
validation_passed = True


# COMMAND ----------

# MAGIC %md
# MAGIC ## Persist Silver Output
# MAGIC Overwrite only the requested Silver output as a Unity Catalog Delta table after all blocking checks pass.
# MAGIC The same pinned inputs produce the same logical records and sorted alert arrays; Bronze and existing Silver sources remain unchanged.

# COMMAND ----------

if not validation_passed:
    raise RuntimeError("Complete validation before persisting")
(
    enriched_observations.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(OUTPUT_TABLE)
)
print(f"Saved {OUTPUT_TABLE}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Validation
# MAGIC Read the persisted table back to verify row count, distinct snapshots, trips and vehicles, then display a deterministic sample of 20 observations.
# MAGIC Repeat the duplicate check on the saved output to confirm that alert enrichment preserved the expected grain.

# COMMAND ----------

saved_output = spark.table(OUTPUT_TABLE)
saved_summary = saved_output.agg(
    F.count("*").alias("total_rows"),
    F.countDistinct("feed_timestamp").alias("distinct_feed_snapshots"),
    F.countDistinct("trip_id").alias("distinct_trips"),
    F.countDistinct("vehicle_id").alias("distinct_vehicles"),
)
saved_summary.show(truncate=False)
if saved_summary.first()["total_rows"] != source_count:
    raise ValueError("Persisted row count differs from the validated source count")
if duplicate_report(saved_output, OBS_KEY, "Persisted grain")["duplicate_extra_rows"]:
    raise ValueError("Persisted output contains duplicate keys")
saved_output.orderBy(*OBS_KEY).select(
    "route_short_name", "trip_id", "vehicle_id", "stop_name", "feed_datetime",
    "scheduled_arrival_datetime", "predicted_arrival_datetime", "arrival_delay_minutes",
    "current_status", "active_alert_flag", "alert_causes", "alert_effects",
).show(20, truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Assumptions and Operational Limits
# MAGIC Alert history is assumed to contain full snapshots: the supplied Bronze table cannot represent empty snapshots, ingestion outages or deleted-entity markers, so the latest recorded snapshot may be stale and feed-time alignment does not establish ingestion-time availability.
# MAGIC Static schedules are not versioned by service validity in the existing Silver schema, and upstream nearest-time vehicle matching can include future positions; the coverage and timing diagnostics must therefore be reviewed before later ML use.
# MAGIC Europe/Rome midnight arithmetic follows the requested rule and preserves GTFS seconds, with transition-day rows explicitly exposed for review; an unavailable alert snapshot means unknown coverage even when the matched-alert count is zero.