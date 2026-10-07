# Databricks notebook source
# MAGIC %md
# MAGIC # Feature Engineering for Next-Stop Delay Prediction
# MAGIC Prepare regression and classification inputs for Rome Public Transport Reliability & Delay Prediction after notebooks 01–07.
# MAGIC This notebook writes only the feature table; encoding, temporal splits, training, MLflow and dashboards belong to later notebooks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Enriched Realtime Observations
# MAGIC Read only the enriched Unity Catalog Delta source, pinning its version for consistent actions and reproducible reruns while that version is retained.
# MAGIC The 300-second major-delay threshold is the explicit working definition requested for this notebook, and temporal features use Europe/Rome.

# COMMAND ----------

from functools import reduce
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark.conf.set("spark.sql.session.timeZone", "Europe/Rome")
SOURCE_TABLE = "rome_transport.silver.realtime_enriched_observations"
OUTPUT_TABLE = "rome_transport.features.next_stop_delay_features"
SOURCE_VERSION = None  # Set to a previously printed version to reproduce a run.
major_delay_threshold_seconds = 300
current_delay_tolerance_seconds = 60
OBS_KEY = ["feed_timestamp", "entity_id", "trip_id", "stop_sequence", "stop_id"]
TRIP_SNAPSHOT_KEY = ["feed_timestamp", "trip_id", "service_date", "entity_id", "start_time", "vehicle_id"]
if SOURCE_VERSION is None:
    SOURCE_VERSION = int(spark.sql(f"DESCRIBE HISTORY {SOURCE_TABLE} LIMIT 1").select("version").first()[0])
source = spark.read.option("versionAsOf", SOURCE_VERSION).table(SOURCE_TABLE)
print("Source Delta version:", SOURCE_VERSION)
required_source_columns = """feed_timestamp feed_datetime service_date entity_id trip_id vehicle_id
start_time route_id route_short_name route_type agency_id direction_id stop_id stop_sequence
arrival_delay_seconds departure_delay_seconds arrival_seconds stop_lat stop_lon
vehicle_speed vehicle_bearing vehicle_odometer current_stop_sequence current_status
vehicle_datetime position_time_diff_seconds active_alert_flag active_alert_count
active_detour_flag active_construction_flag alert_causes alert_effects active_alert_ids
alert_snapshot_available_flag matched_alert_feed_timestamp alert_snapshot_age_seconds""".split()
missing_columns = sorted(set(required_source_columns) - set(source.columns))
if missing_columns:
    raise ValueError(f"Missing source columns: {missing_columns}; rerun notebook 07")

def n_where(condition):
    return F.coalesce(F.sum(F.when(condition, 1).otherwise(0)), F.lit(0))

def assert_no_rows(df, condition, message):
    invalid = df.filter(condition)
    if invalid.limit(1).count():
        invalid.select(*[c for c in OBS_KEY if c in df.columns]).show(20, truncate=False)
        raise ValueError(message)

def duplicate_report(df, keys, label):
    groups = df.groupBy(*keys).count().filter(F.col("count") > 1)
    summary = groups.agg(F.count("*").alias("duplicate_groups"),
        F.coalesce(F.sum(F.col("count") - 1), F.lit(0)).alias("duplicate_extra_rows")).first().asDict()
    print(label, summary)
    if summary["duplicate_extra_rows"]:
        groups.orderBy(F.col("count").desc()).show(20, truncate=False)
    return summary

def historical_window(keys):
    # Exclude ALL observations at the current epoch second, including tied peers.
    return Window.partitionBy(*keys).orderBy("feed_timestamp").rangeBetween(Window.unboundedPreceding, -1)

def haversine_meters(lat1, lon1, lat2, lon2):
    p1, p2 = F.radians(lat1), F.radians(lat2)
    a = F.pow(F.sin((p2 - p1) / 2), 2) + F.cos(p1) * F.cos(p2) * F.pow(F.sin(F.radians(lon2 - lon1) / 2), 2)
    valid = lat1.between(-90, 90) & lat2.between(-90, 90) & lon1.between(-180, 180) & lon2.between(-180, 180)
    return F.when(valid, 2 * 6371000.0 * F.asin(F.sqrt(F.least(F.lit(1.0), F.greatest(F.lit(0.0), a)))))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Source Grain
# MAGIC Report coverage, required identifiers and duplicates without changing Silver.
# MAGIC Duplicate keys or noncausal upstream vehicle positions block the run; observations missing identifiers needed for ordering are counted and excluded explicitly.

# COMMAND ----------

source_summary = source.agg(
    F.count("*").alias("total_rows"),
    *[F.countDistinct(c).alias(name) for c, name in [
        ("feed_timestamp", "distinct_feed_snapshots"), ("trip_id", "distinct_trips"),
        ("vehicle_id", "distinct_vehicles"), ("route_id", "distinct_routes"), ("stop_id", "distinct_stops")]],
    *[n_where(F.col(c).isNull()).alias(f"null_{c}") for c in
      ["trip_id", "stop_id", "stop_sequence", "feed_timestamp", "service_date"]],
)
source_summary.show(truncate=False, vertical=True)
source_count = source_summary.first()["total_rows"]
if source_count == 0:
    raise ValueError("Empty source; output write blocked")
if duplicate_report(source, OBS_KEY, "Source grain")["duplicate_extra_rows"]:
    raise ValueError("Duplicate source keys; resolve upstream")
future_position = (F.col("vehicle_datetime") > F.col("feed_datetime")) | (F.col("position_time_diff_seconds") < 0)
source.agg(n_where(future_position).alias("rows_with_future_vehicle_position")).show()
assert_no_rows(source, future_position | (F.col("position_time_diff_seconds") > 180),
    "Noncausal/stale positions: rerun corrected notebook 06 and notebook 07")
assert_no_rows(source,
    (F.col("vehicle_datetime").isNotNull() & F.col("position_time_diff_seconds").isNull())
    | (F.col("vehicle_datetime").isNull() & F.col("position_time_diff_seconds").isNotNull()),
    "Inconsistent vehicle match provenance")
assert_no_rows(source, F.col("feed_timestamp").isNotNull()
    & ~F.col("feed_datetime").eqNullSafe(F.timestamp_seconds("feed_timestamp")), "Feed datetime/epoch mismatch")
valid_ordering = reduce(lambda a, b: a & b, [F.col(c).isNotNull() for c in
    ["feed_timestamp", "feed_datetime", "service_date", "entity_id", "trip_id", "stop_sequence", "stop_id"]])
valid_ordering = valid_ordering & (F.col("stop_sequence") >= 0)
observations = source.filter(valid_ordering)
ordered_source_count = observations.count()
print("Rows excluded for missing/invalid ordering identifiers:", source_count - ordered_source_count)
if ordered_source_count == 0:
    raise ValueError("No valid ordering identifiers")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Observation Ordering
# MAGIC Partition by feed timestamp, trip and service date, retaining entity, start time and vehicle boundaries to avoid mixing trip instances.
# MAGIC Stop sequence must be unique within each partition; the next stop means the next represented stop in that snapshot because sparse feeds do not prove that intervening scheduled stops are present.

# COMMAND ----------

if duplicate_report(observations, TRIP_SNAPSHOT_KEY + ["stop_sequence"], "Snapshot stop ordering")["duplicate_extra_rows"]:
    raise ValueError("Ambiguous stop ordering; resolve upstream")
stop_order = Window.partitionBy(*TRIP_SNAPSHOT_KEY).orderBy("stop_sequence")
last_three_stops = stop_order.rowsBetween(-2, 0)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Next-Stop Targets
# MAGIC Use LEAD of arrival_delay_seconds within the same snapshot, with no departure-delay fallback or skipping a null next-stop delay.
# MAGIC This is a contemporaneous feed-estimate target rather than verified future arrival ground truth; classify delays of at least 300 seconds as major delay and reserve target-derived values for labels or audit.

# COMMAND ----------

features = observations
for source_col, target_col in [
    ("arrival_delay_seconds", "target_next_stop_delay_seconds"),
    ("stop_sequence", "target_next_stop_sequence"), ("stop_id", "target_next_stop_id"),
    ("feed_timestamp", "target_feed_timestamp"), ("trip_id", "target_trip_id"),
    ("service_date", "target_service_date"),
]:
    features = features.withColumn(target_col, F.lead(source_col, 1).over(stop_order))
features = (features
    .withColumn("target_major_delay_flag", F.when(F.col("target_next_stop_delay_seconds").isNotNull(),
        (F.col("target_next_stop_delay_seconds") >= major_delay_threshold_seconds).cast("int")))
    .withColumn("major_delay_threshold_seconds", F.lit(major_delay_threshold_seconds))
    .withColumn("target_stop_sequence_gap", F.col("target_next_stop_sequence") - F.col("stop_sequence"))
    .withColumn("source_delta_version", F.lit(SOURCE_VERSION))
    .withColumn("target_definition", F.lit("same_snapshot_next_reported_stop_arrival_delay")))
# Retain terminal/null-target observations until historical features are complete.


# COMMAND ----------

# MAGIC %md
# MAGIC ## Temporal Features
# MAGIC Derive clock features from feed_datetime and calendar features from service_date, never from target timestamps.
# MAGIC Day of week follows Spark's Sunday=1 through Saturday=7 convention; weekend means Saturday or Sunday.

# COMMAND ----------

features = (features.withColumn("feed_hour", F.hour("feed_datetime"))
    .withColumn("feed_minute", F.minute("feed_datetime"))
    .withColumn("day_of_week", F.dayofweek("service_date"))
    .withColumn("is_weekend", F.col("day_of_week").isin(1, 7).cast("int"))
    .withColumn("service_month", F.month("service_date"))
    .withColumn("service_day_of_month", F.dayofmonth("service_date"))
    .withColumn("minutes_since_midnight", F.col("feed_hour") * 60 + F.col("feed_minute")))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Current Delay Features
# MAGIC Preserve current arrival and departure delays and derive minutes and magnitude without imputing missing values.
# MAGIC Currently delayed means above +60 seconds and early means below −60 seconds, based on the current stop's arrival-delay estimate.

# COMMAND ----------

features = (features
    .withColumn("current_arrival_delay_seconds", F.col("arrival_delay_seconds"))
    .withColumn("current_arrival_delay_minutes", F.col("arrival_delay_seconds") / 60.0)
    .withColumn("current_departure_delay_seconds", F.col("departure_delay_seconds"))
    .withColumn("current_departure_delay_minutes", F.col("departure_delay_seconds") / 60.0)
    .withColumn("abs_current_delay_seconds", F.abs("current_arrival_delay_seconds"))
    .withColumn("is_currently_delayed_flag", (F.col("current_arrival_delay_seconds") > current_delay_tolerance_seconds).cast("int"))
    .withColumn("is_currently_early_flag", (F.col("current_arrival_delay_seconds") < -current_delay_tolerance_seconds).cast("int")))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Delay History Features
# MAGIC Build lag 1–3 from preceding stop records in the same trip snapshot and rolling summaries over the current and two preceding stops.
# MAGIC These are feed-available estimates ordered by stop sequence, not necessarily completed-stop outcomes; missing delays never pull values from the next stop.

# COMMAND ----------

for offset in (1, 2, 3):
    features = features.withColumn(f"lag_{offset}_arrival_delay_seconds", F.lag("arrival_delay_seconds", offset).over(stop_order))
features = (features
    .withColumn("rolling_mean_delay_last_3_stops", F.avg("arrival_delay_seconds").over(last_three_stops))
    .withColumn("rolling_max_delay_last_3_stops", F.max("arrival_delay_seconds").over(last_three_stops))
    .withColumn("rolling_std_delay_last_3_stops", F.stddev_samp("arrival_delay_seconds").over(last_three_stops))
    .withColumn("lag_1_stop_sequence_audit", F.lag("stop_sequence", 1).over(stop_order))
    .withColumn("rolling_max_stop_sequence_audit", F.max("stop_sequence").over(last_three_stops)))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Delay Propagation Features
# MAGIC Measure delay change and acceleration using only current and preceding stop estimates.
# MAGIC Acceleration equals the current first difference minus the preceding first difference and stays null when required history is missing.

# COMMAND ----------

features = (features
    .withColumn("delay_change_from_previous_stop", F.col("current_arrival_delay_seconds") - F.col("lag_1_arrival_delay_seconds"))
    .withColumn("delay_acceleration", F.col("delay_change_from_previous_stop")
        - (F.col("lag_1_arrival_delay_seconds") - F.col("lag_2_arrival_delay_seconds"))))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Schedule and Trip Progress Features
# MAGIC Use static schedule metadata already carried by the enriched source for the next represented stop's scheduled interval and spatial context.
# MAGIC The source does not carry a verified full-trip max_stop_sequence_for_trip, so trip_progress_ratio remains null with an availability flag instead of treating partial realtime coverage as a complete schedule.

# COMMAND ----------

for source_col, next_col in [
    ("arrival_seconds", "next_scheduled_arrival_seconds_audit"),
    ("stop_lat", "next_static_stop_lat_audit"), ("stop_lon", "next_static_stop_lon_audit"),
]:
    features = features.withColumn(next_col, F.lead(source_col, 1).over(stop_order))
features = (features
    .withColumn("scheduled_seconds_to_next_stop", F.col("next_scheduled_arrival_seconds_audit") - F.col("arrival_seconds"))
    .withColumn("trip_progress_ratio", F.lit(None).cast("double"))
    .withColumn("missing_trip_progress_ratio", F.lit(1))
    .withColumn("trip_progress_basis", F.lit("unavailable_full_static_trip_extent")))
assert_no_rows(features, F.col("scheduled_seconds_to_next_stop") < 0,
    "Static arrival_seconds decrease in stop order; inspect schedule enrichment")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Vehicle State Features
# MAGIC Retain upstream vehicle state only when its position timestamp and staleness establish a match at or before the feed timestamp, within 180 seconds.
# MAGIC Missing positions remain eligible for modeling with indicators and null vehicle state rather than automatic row removal.

# COMMAND ----------

position_available = F.col("vehicle_datetime").isNotNull() & F.col("position_time_diff_seconds").between(0, 180)
features = (features
    .withColumn("vehicle_position_available_flag", position_available.cast("int"))
    .withColumn("vehicle_position_staleness_seconds", F.when(position_available, F.col("position_time_diff_seconds"))))
for name in ["vehicle_speed", "vehicle_bearing", "vehicle_odometer", "current_stop_sequence", "current_status"]:
    features = features.withColumn(name, F.when(position_available, F.col(name)))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Route and Stop Context
# MAGIC Preserve route, agency, direction and stop coordinates, and compute straight-line distance to the next represented stop with native Spark Haversine expressions.
# MAGIC Coordinates come from static stop metadata rather than future vehicle positions; invalid or missing coordinates yield null distance.

# COMMAND ----------

for name in ["stop_lat", "stop_lon", "next_static_stop_lat_audit", "next_static_stop_lon_audit"]:
    features = features.withColumn(name, F.expr(f"try_cast(`{name}` AS DOUBLE)"))
features = features.withColumn("distance_to_next_stop_meters", haversine_meters(
    F.col("stop_lat"), F.col("stop_lon"), F.col("next_static_stop_lat_audit"), F.col("next_static_stop_lon_audit")))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Alert Features
# MAGIC Use upstream snapshot availability to distinguish no active alert from unknown coverage, masking numeric alert context when a snapshot is absent.
# MAGIC Original alert arrays remain audit columns; inputs use flags and counts instead of high-cardinality text.

# COMMAND ----------

assert_no_rows(features,
    (F.col("matched_alert_feed_timestamp") > F.col("feed_timestamp"))
    | ~F.col("alert_snapshot_available_flag").eqNullSafe(F.col("matched_alert_feed_timestamp").isNotNull()),
    "Alert snapshot availability or timing is inconsistent")
alert_available = F.coalesce(F.col("alert_snapshot_available_flag"), F.lit(False))
for name in ["active_alert_flag", "active_alert_count", "active_detour_flag", "active_construction_flag"]:
    features = features.withColumn(name, F.when(alert_available, F.col(name).cast("int")))
features = (features
    .withColumn("has_alert_cause_flag", F.when(alert_available, (F.size("alert_causes") > 0).cast("int")))
    .withColumn("has_alert_effect_flag", F.when(alert_available, (F.size("alert_effects") > 0).cast("int"))))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Historical Reliability and Count Features
# MAGIC Compute cumulative means and observation counts from strictly earlier feed timestamps, excluding every same-timestamp peer with a numeric range frame.
# MAGIC History uses current arrival-delay estimates from all valid source observations before target filtering, with no global-mean fallback; repeated snapshots represent observation-weighted history rather than independent completed trips.

# COMMAND ----------

HISTORY_SPECS = [
    ("route", ["route_id"], "historical_route_mean_delay"),
    ("stop", ["stop_id"], "historical_stop_mean_delay"),
    ("route_stop", ["route_id", "stop_id"], "historical_route_stop_mean_delay"),
    ("route_hour", ["route_id", "feed_hour"], "historical_route_hour_mean_delay"),
]
historical_audit_columns = []
for label, keys, mean_name in HISTORY_SPECS:
    window = historical_window(keys)
    valid_keys = reduce(lambda a, b: a & b, [F.col(c).isNotNull() for c in keys])
    max_time_name = f"historical_{label}_max_feed_timestamp_audit"
    count_name = f"historical_{label}_observation_count"
    delay_count_name = f"historical_{label}_nonnull_delay_count_audit"
    features = (features
        .withColumn(mean_name, F.when(valid_keys, F.avg("current_arrival_delay_seconds").over(window)))
        .withColumn(count_name, F.when(valid_keys, F.count(F.lit(1)).over(window)))
        .withColumn(delay_count_name, F.when(valid_keys, F.count("current_arrival_delay_seconds").over(window)))
        .withColumn(max_time_name, F.when(valid_keys, F.max("feed_timestamp").over(window))))
    historical_audit_columns += [max_time_name, delay_count_name]


# COMMAND ----------

# MAGIC %md
# MAGIC ## Missingness Indicators
# MAGIC Expose missing current delays, vehicle state, historical context and alert coverage as numeric flags.
# MAGIC Historical means with no prior non-null delay remain null, and unknown alert coverage stays distinct from a known snapshot with no applicable alerts.

# COMMAND ----------

missingness_sources = {
    "missing_current_arrival_delay": "current_arrival_delay_seconds",
    "missing_current_departure_delay": "current_departure_delay_seconds",
    "missing_historical_route_delay": "historical_route_mean_delay",
    "missing_historical_stop_delay": "historical_stop_mean_delay",
    "missing_historical_route_stop_delay": "historical_route_stop_mean_delay",
    "missing_historical_route_hour_delay": "historical_route_hour_mean_delay",
}
for name, input_column in missingness_sources.items():
    features = features.withColumn(name, F.col(input_column).isNull().cast("int"))
features = (features
    .withColumn("missing_vehicle_position", 1 - F.col("vehicle_position_available_flag"))
    .withColumn("missing_alert_snapshot", (~F.coalesce(F.col("alert_snapshot_available_flag"), F.lit(False))).cast("int")))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Feature Selection
# MAGIC Define numeric, categorical, audit and target lists, preserving timestamps for later chronological splits and leaving encoding and imputation to the modeling pipeline.
# MAGIC Audit identifiers may also be contextual features, but target fields and target-derived audit values must never enter model inputs.

# COMMAND ----------

numeric_features = """feed_hour feed_minute day_of_week is_weekend service_month service_day_of_month
minutes_since_midnight current_arrival_delay_seconds current_arrival_delay_minutes
current_departure_delay_seconds current_departure_delay_minutes abs_current_delay_seconds
is_currently_delayed_flag is_currently_early_flag lag_1_arrival_delay_seconds
lag_2_arrival_delay_seconds lag_3_arrival_delay_seconds rolling_mean_delay_last_3_stops
rolling_max_delay_last_3_stops rolling_std_delay_last_3_stops delay_change_from_previous_stop
delay_acceleration stop_sequence trip_progress_ratio scheduled_seconds_to_next_stop
stop_lat stop_lon distance_to_next_stop_meters vehicle_speed vehicle_bearing vehicle_odometer
current_stop_sequence position_time_diff_seconds vehicle_position_staleness_seconds
vehicle_position_available_flag active_alert_flag active_alert_count active_detour_flag
active_construction_flag has_alert_cause_flag has_alert_effect_flag historical_route_mean_delay
historical_stop_mean_delay historical_route_stop_mean_delay historical_route_hour_mean_delay
historical_route_observation_count historical_stop_observation_count
historical_route_stop_observation_count historical_route_hour_observation_count
missing_vehicle_position missing_current_arrival_delay missing_current_departure_delay
missing_historical_route_delay missing_historical_stop_delay missing_historical_route_stop_delay
missing_historical_route_hour_delay missing_alert_snapshot missing_trip_progress_ratio""".split()
categorical_features = ["route_id", "route_type", "direction_id", "agency_id", "current_status"]
audit_columns = """feed_timestamp feed_datetime service_date entity_id trip_id vehicle_id
start_time route_id route_short_name stop_id stop_sequence target_next_stop_id
target_next_stop_sequence target_feed_timestamp target_trip_id target_service_date
major_delay_threshold_seconds target_stop_sequence_gap target_definition source_delta_version
arrival_seconds next_scheduled_arrival_seconds_audit next_static_stop_lat_audit next_static_stop_lon_audit
lag_1_stop_sequence_audit rolling_max_stop_sequence_audit trip_progress_basis vehicle_datetime
active_alert_ids alert_causes alert_effects alert_snapshot_available_flag matched_alert_feed_timestamp
alert_snapshot_age_seconds""".split() + historical_audit_columns
target_columns = ["target_next_stop_delay_seconds", "target_major_delay_flag"]
forbidden_feature_columns = target_columns + """target_next_stop_id target_next_stop_sequence
target_feed_timestamp target_trip_id target_service_date target_stop_sequence_gap
target_definition major_delay_threshold_seconds predicted_arrival_datetime
next_predicted_arrival_datetime next_arrival_delay_seconds next_departure_delay_seconds
arrival_delay_seconds departure_delay_seconds source_delta_version""".split() + historical_audit_columns
feature_columns = numeric_features + categorical_features
assert len(feature_columns) == len(set(feature_columns)), "Duplicate feature names"
assert not (set(feature_columns) & set(forbidden_feature_columns)), "Forbidden feature selected"
assert not any(name.startswith("target_") for name in feature_columns)
assert not (set(numeric_features) & set(categorical_features))
print("Numeric features:", numeric_features)
print("Categorical features:", categorical_features)
print("Targets:", target_columns)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Leakage Validation
# MAGIC Validate same-snapshot target provenance, preceding stop lags, rolling boundaries, causal vehicle positions and historical maximum contributing timestamps.
# MAGIC These checks establish feed-time and stop-order causality for the declared inputs; ingestion-time availability and true arrival outcomes are not established by this source schema.

# COMMAND ----------

assert_no_rows(features,
    F.col("target_next_stop_sequence").isNotNull() & (
        (F.col("target_next_stop_sequence") <= F.col("stop_sequence"))
        | ~F.col("target_feed_timestamp").eqNullSafe(F.col("feed_timestamp"))
        | ~F.col("target_trip_id").eqNullSafe(F.col("trip_id"))
        | ~F.col("target_service_date").eqNullSafe(F.col("service_date"))),
    "Target outside the ordered trip snapshot")
assert_no_rows(features,
    (F.col("lag_1_stop_sequence_audit") >= F.col("stop_sequence"))
    | (F.col("rolling_max_stop_sequence_audit") > F.col("stop_sequence")),
    "Delay history contains a nonpreceding/future stop")
for label, keys, mean_name in HISTORY_SPECS:
    assert_no_rows(features, F.col(f"historical_{label}_max_feed_timestamp_audit") >= F.col("feed_timestamp"),
        f"{label} history contains current/future timestamps")
assert_no_rows(features,
    (F.col("vehicle_datetime") > F.col("feed_datetime"))
    | (F.col("vehicle_position_staleness_seconds") < 0)
    | (F.col("vehicle_position_staleness_seconds") > 180), "Invalid vehicle timing")
features.orderBy(*OBS_KEY).select(
    "feed_timestamp", "route_id", "historical_route_observation_count",
    "historical_route_mean_delay", "historical_route_max_feed_timestamp_audit",
).show(20, truncate=False)
print("Leakage provenance checks passed")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Causal Window Edge Checks
# MAGIC Small in-memory Spark fixtures verify that simultaneous observations cannot enter each other's history and stop lags cannot cross snapshot or trip boundaries.
# MAGIC Haversine checks verify zero distance and missing-coordinate handling; these checks create no persistent artifacts or external dependencies.

# COMMAND ----------

history_fixture = spark.createDataFrame(
    [("r", 100, 10.0, 0, None), ("r", 100, 30.0, 0, None),
     ("r", 101, 50.0, 2, 20.0), ("r", 102, 70.0, 3, 30.0)],
    "route_id STRING, feed_timestamp LONG, delay DOUBLE, expected_count LONG, expected_mean DOUBLE")
fixture_window = historical_window(["route_id"])
history_fixture = history_fixture.withColumn("actual_count", F.count(F.lit(1)).over(fixture_window)).withColumn(
    "actual_mean", F.avg("delay").over(fixture_window))
assert history_fixture.filter(
    ~F.col("actual_mean").eqNullSafe(F.col("expected_mean")) | (F.col("actual_count") != F.col("expected_count"))
).count() == 0
stop_fixture = spark.createDataFrame(
    [(100, "t", "2026-09-25", "e", "08:00:00", "v", 1, 10.0, None, 20.0),
     (100, "t", "2026-09-25", "e", "08:00:00", "v", 2, 20.0, 10.0, None),
     (101, "t", "2026-09-25", "e", "08:00:00", "v", 1, 99.0, None, None),
     (100, "other", "2026-09-25", "e2", "08:00:00", "v2", 1, 50.0, None, None)],
    "feed_timestamp LONG, trip_id STRING, service_date STRING, entity_id STRING, start_time STRING, vehicle_id STRING, stop_sequence INT, arrival_delay_seconds DOUBLE, expected_lag DOUBLE, expected_target DOUBLE")
stop_fixture = stop_fixture.withColumn("actual_lag", F.lag("arrival_delay_seconds").over(stop_order)).withColumn(
    "actual_target", F.lead("arrival_delay_seconds").over(stop_order))
assert stop_fixture.filter(
    ~F.col("actual_lag").eqNullSafe(F.col("expected_lag"))
    | ~F.col("actual_target").eqNullSafe(F.col("expected_target"))
).count() == 0
geo_fixture = spark.createDataFrame([(41.9, 12.5), (None, 12.5)], "lat DOUBLE, lon DOUBLE").withColumn(
    "distance", haversine_meters(F.col("lat"), F.col("lon"), F.col("lat"), F.col("lon")))
assert geo_fixture.filter(
    (F.col("lat").isNull() & F.col("distance").isNotNull())
    | (F.col("lat").isNotNull() & (F.abs("distance") > 0.000001))
).count() == 0
print("Causal window and spatial edge checks passed")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Training-Ready Dataset
# MAGIC Remove observations with no valid next-stop arrival-delay target only after historical features have been calculated, retaining missing inputs for later modeling decisions.
# MAGIC No arbitrary delay outlier filtering is applied; excluded terminal and missing-label observations are reported separately.

# COMMAND ----------

features.agg(
    n_where(F.col("target_next_stop_sequence").isNull()).alias("rows_without_next_reported_stop"),
    n_where(F.col("target_next_stop_sequence").isNotNull() & F.col("target_next_stop_delay_seconds").isNull()).alias("rows_with_missing_next_arrival_delay"),
    n_where(F.col("target_stop_sequence_gap") > 1).alias("rows_with_stop_sequence_gap"),
).show(truncate=False)
training_ready = features.filter(
    F.col("target_next_stop_id").isNotNull()
    & (F.col("target_next_stop_sequence") > F.col("stop_sequence"))
    & F.col("target_next_stop_delay_seconds").isNotNull())
output_columns = list(dict.fromkeys(audit_columns + numeric_features + categorical_features + target_columns))
training_ready = training_ready.select(*output_columns)
feature_count = training_ready.count()
print("Training-ready rows:", feature_count, "of", ordered_source_count, "valid source observations")
if feature_count == 0:
    raise ValueError("No valid next-stop targets; output write blocked")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Target Validation
# MAGIC Report regression distribution, requested percentiles and classification prevalence using the documented 300-second threshold.
# MAGIC These full-dataset summaries are quality diagnostics and must not be used to tune models, select thresholds or alter the future test partition.

# COMMAND ----------

target_summary = training_ready.agg(
    F.count("*").alias("total_feature_rows"),
    n_where(F.col("target_next_stop_delay_seconds").isNull()).alias("target_delay_nulls"),
    n_where(F.col("target_major_delay_flag").isNull()).alias("target_major_delay_nulls"),
    F.avg("target_next_stop_delay_seconds").alias("target_delay_mean"),
    F.percentile_approx("target_next_stop_delay_seconds", 0.5, 10000).alias("target_delay_median"),
    F.stddev_samp("target_next_stop_delay_seconds").alias("target_delay_stddev"),
    F.min("target_next_stop_delay_seconds").alias("target_delay_min"),
    F.max("target_next_stop_delay_seconds").alias("target_delay_max"),
    F.sum("target_major_delay_flag").alias("major_delay_count"),
    (100.0 * F.avg("target_major_delay_flag")).alias("major_delay_rate_pct"),
    *[F.percentile_approx("target_next_stop_delay_seconds", q, 10000).alias(name)
      for q, name in [(0.50, "P50"), (0.75, "P75"), (0.90, "P90"), (0.95, "P95"), (0.99, "P99")]],
)
target_summary.show(truncate=False, vertical=True)
assert_no_rows(training_ready,
    ~F.col("target_major_delay_flag").eqNullSafe((F.col("target_next_stop_delay_seconds") >= major_delay_threshold_seconds).cast("int")),
    "Classification target disagrees with the regression label")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Quality Validation
# MAGIC Report null rates for every numeric and categorical feature and coverage for vehicle positions, historical context and alerts.
# MAGIC Non-finite numeric values block persistence, while null inputs remain allowed and unavailable full-trip progress is visible in the null report.

# COMMAND ----------

null_summary = training_ready.agg(*[
    (100.0 * F.avg(F.col(name).isNull().cast("double"))).alias(name) for name in feature_columns])
null_summary.show(truncate=False, vertical=True)
training_ready.agg(
    (100.0 * F.avg("vehicle_position_available_flag")).alias("vehicle_position_coverage_pct"),
    (100.0 * F.avg(F.col("historical_route_mean_delay").isNotNull().cast("int"))).alias("historical_route_feature_coverage_pct"),
    (100.0 * F.avg(F.col("historical_stop_mean_delay").isNotNull().cast("int"))).alias("historical_stop_feature_coverage_pct"),
    (100.0 * F.avg(1 - F.col("missing_alert_snapshot"))).alias("alert_coverage_pct"),
).show(truncate=False, vertical=True)
nonfinite_columns = numeric_features + ["target_next_stop_delay_seconds"]
nonfinite_summary = training_ready.agg(*[
    n_where(F.isnan(F.col(name).cast("double")) | (F.abs(F.col(name).cast("double")) == float("inf"))).alias(name)
    for name in nonfinite_columns])
nonfinite_summary.show(truncate=False, vertical=True)
if any(value > 0 for value in nonfinite_summary.first().asDict().values()):
    raise ValueError("NaN/Infinity in numeric inputs or target; inspect source values")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Duplicate Validation
# MAGIC Require one modeling observation per original current-stop key after label filtering.
# MAGIC The final count may decrease only through documented ordering and target exclusions, and duplicate_extra_rows must remain zero.

# COMMAND ----------

if duplicate_report(training_ready, OBS_KEY, "Final feature grain")["duplicate_extra_rows"]:
    raise ValueError("Duplicate feature observations; output write blocked")
assert feature_count <= ordered_source_count
assert not (set(feature_columns) & set(forbidden_feature_columns))
training_ready.printSchema()
validation_passed = True


# COMMAND ----------

# MAGIC %md
# MAGIC ## Persist Feature Table
# MAGIC Overwrite only rome_transport.features.next_stop_delay_features after blocking checks pass, using the features schema created by notebook 01.
# MAGIC The table includes identifiers and audit columns, so downstream modeling must explicitly select numeric_features and categorical_features rather than every non-target column.

# COMMAND ----------

if not validation_passed:
    raise RuntimeError("Complete validation before saving")
(training_ready.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(OUTPUT_TABLE))
print("Saved:", OUTPUT_TABLE)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Validation
# MAGIC Read back the feature output to confirm counts and display a deterministic 20-row sample.
# MAGIC No random split is created; feed_timestamp, feed_datetime and service_date remain available for chronological evaluation.

# COMMAND ----------

saved = spark.table(OUTPUT_TABLE)
saved_summary = saved.agg(
    F.count("*").alias("total_rows"), F.countDistinct("feed_timestamp").alias("distinct_feed_snapshots"),
    F.countDistinct("trip_id").alias("distinct_trips"), F.countDistinct("vehicle_id").alias("distinct_vehicles"),
    F.countDistinct("route_id").alias("distinct_routes"))
saved_summary.show(truncate=False)
assert saved_summary.first()["total_rows"] == feature_count
assert duplicate_report(saved, OBS_KEY, "Persisted feature grain")["duplicate_extra_rows"] == 0
saved.orderBy(*OBS_KEY).select(
    "route_id", "trip_id", "stop_id", "stop_sequence", "current_arrival_delay_seconds",
    "lag_1_arrival_delay_seconds", "historical_route_mean_delay", "vehicle_speed", "active_alert_flag",
    "target_next_stop_delay_seconds", "target_major_delay_flag",
).show(20, truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Modeling Handoff and Limits
# MAGIC The label predicts the next represented stop's estimate already present in the same snapshot, so this dataset alone cannot demonstrate forecasting skill against later realized arrivals; stop gaps and target provenance remain auditable.
# MAGIC Strict-past histories assume sequential scoring can consume earlier feed observations during validation/test periods, while downstream preprocessing must be fitted on training data only with a time-based split.
# MAGIC The source cannot prove ingestion-time availability or represent empty alert snapshots and full static trip extent, so stale alert coverage, retrospective schedule changes and unavailable trip progress remain upstream limitations.