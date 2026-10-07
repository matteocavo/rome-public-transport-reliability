# Databricks notebook source
# MAGIC %md
# MAGIC # Future Outcome Labeling
# MAGIC Attach future next-stop labels to the existing leakage-safe features for Rome Public Transport Reliability & Delay Prediction.
# MAGIC All feature values remain unchanged; future observations, provisional targets and label-quality evidence are strictly labels or audit data, never model inputs.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Feature and Realtime Sources
# MAGIC Read the feature table as the driving dataset and enriched Silver as the future-label source, pinning both Delta versions for stable repeated actions.
# MAGIC The recorded versions support reproducible reruns while retained; this notebook writes only the new labeled feature table.

# COMMAND ----------

from functools import reduce
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark.conf.set("spark.sql.session.timeZone", "Europe/Rome")
FEATURE_TABLE = "rome_transport.features.next_stop_delay_features"
REALTIME_TABLE = "rome_transport.silver.realtime_enriched_observations"
OUTPUT_TABLE = "rome_transport.features.next_stop_delay_labeled"
MAX_LABEL_HORIZON_SECONDS = 1800
major_delay_threshold_seconds = 300
MAX_QUALITY_POSITION_STALENESS_SECONDS = 180
SOURCE_VERSION_OVERRIDES = {}  # Optional keys: features, realtime.
OBS_KEY = ["feed_timestamp", "entity_id", "trip_id", "stop_sequence", "stop_id"]
source_versions = {}
sources = {}
for name, table in [("features", FEATURE_TABLE), ("realtime", REALTIME_TABLE)]:
    version = SOURCE_VERSION_OVERRIDES.get(name)
    if version is None:
        version = int(spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").select("version").first()[0])
    source_versions[name] = int(version)
    sources[name] = spark.read.option("versionAsOf", int(version)).table(table)
features_df, realtime_df = sources["features"], sources["realtime"]
print("Input Delta versions:", source_versions)

def require_columns(df, names, label):
    missing = sorted(set(names) - set(df.columns))
    if missing:
        raise ValueError(f"{label}: missing columns {missing}")

def n_where(condition):
    return F.coalesce(F.sum(F.when(condition, 1).otherwise(0)), F.lit(0))

def assert_no_rows(df, condition, message):
    if df.filter(condition).limit(1).count():
        raise ValueError(message)

def duplicate_report(df, keys, label):
    groups = df.groupBy(*keys).count().filter(F.col("count") > 1)
    summary = groups.agg(F.count("*").alias("duplicate_groups"),
        F.coalesce(F.sum(F.col("count") - 1), F.lit(0)).alias("duplicate_extra_rows")).first().asDict()
    print(label, summary)
    if summary["duplicate_extra_rows"]:
        groups.orderBy(F.col("count").desc()).show(20, truncate=False)
    return summary

require_columns(features_df, OBS_KEY + ["feed_datetime", "service_date", "vehicle_id", "route_id",
    "start_time", "target_next_stop_id", "target_next_stop_sequence", "target_next_stop_delay_seconds",
    "target_major_delay_flag", "target_definition", "major_delay_threshold_seconds", "current_arrival_delay_seconds"], "Features")
require_columns(realtime_df, OBS_KEY + ["feed_datetime", "service_date", "arrival_delay_seconds",
    "departure_delay_seconds", "start_time", "vehicle_id"], "Realtime")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Input Grain
# MAGIC Require unique feature and Silver observation keys, and verify timestamp consistency before matching.
# MAGIC Missing target identifiers remain unmatched rather than being replaced, while duplicate input grains or a conflicting classification threshold stop the run.

# COMMAND ----------

total_feature_rows = features_df.count()
print("total_feature_rows:", total_feature_rows)
print("total_realtime_rows:", realtime_df.count())
for df, name in [(features_df, "Feature input"), (realtime_df, "Realtime input")]:
    if duplicate_report(df, OBS_KEY, name)["duplicate_extra_rows"]:
        raise ValueError(f"{name}: duplicate keys; resolve upstream")
    assert_no_rows(df, F.col("feed_timestamp").isNull()
        | ~F.col("feed_datetime").eqNullSafe(F.timestamp_seconds("feed_timestamp")),
        f"{name}: invalid feed timestamp/datetime")
assert_no_rows(features_df,
    ~F.col("major_delay_threshold_seconds").eqNullSafe(F.lit(major_delay_threshold_seconds)),
    "Upstream threshold differs from the required 300 seconds")
features_df.agg(
    n_where(F.col("target_next_stop_id").isNull()).alias("missing_target_stop_id"),
    n_where(F.col("target_next_stop_sequence").isNull()).alias("missing_target_stop_sequence"),
    n_where(F.col("service_date").isNull()).alias("missing_service_date"),
).show(truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Labeling Strategy
# MAGIC Match the same trip, service date and target stop ID/sequence, selecting the earliest eligible snapshot strictly after t0 and no more than 1800 seconds later to avoid remote outcomes.
# MAGIC When both trip start times are populated they must agree, and no absolute-nearest or same-snapshot matching is allowed.
# MAGIC A later GTFS-RT delay can still be a revised forecast, so the requested realized target name denotes a future-observation proxy whose realization evidence is reported separately.

# COMMAND ----------

PROVISIONAL_RENAMES = {
    "target_next_stop_delay_seconds": "provisional_next_stop_delay_seconds",
    "target_major_delay_flag": "provisional_major_delay_flag",
    "target_definition": "provisional_target_definition",
}
for old, new in PROVISIONAL_RENAMES.items():
    if new in features_df.columns:
        raise ValueError(f"Ambiguous provisional audit column: {new}")
current = features_df.select(*[
    F.col(name).alias(PROVISIONAL_RENAMES.get(name, name)) for name in features_df.columns
])
preserved_columns = list(current.columns)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Identify Future Next-Stop Observations
# MAGIC Use future arrival delay when present and departure delay only as a documented fallback, retaining the selected source for audit.
# MAGIC Optional vehicle-state fields support quality grading only; missing evidence never disqualifies an otherwise valid future trip-stop label.

# COMMAND ----------

future_source = realtime_df
OPTIONAL_QUALITY_FIELDS = {
    "current_status": "string", "current_stop_sequence": "long",
    "vehicle_current_stop_id": "string", "vehicle_datetime": "timestamp",
    "position_time_diff_seconds": "long",
}
for name, dtype in OPTIONAL_QUALITY_FIELDS.items():
    if name not in future_source.columns:
        future_source = future_source.withColumn(name, F.lit(None).cast(dtype))

FUTURE_COLUMN_MAP = {
    "feed_timestamp": "label_source_feed_timestamp",
    "feed_datetime": "label_source_feed_datetime",
    "service_date": "label_source_service_date",
    "trip_id": "label_source_trip_id",
    "entity_id": "label_source_entity_id",
    "vehicle_id": "label_source_vehicle_id",
    "start_time": "label_source_start_time",
    "stop_id": "label_source_stop_id",
    "stop_sequence": "label_source_stop_sequence",
    "arrival_delay_seconds": "label_source_arrival_delay_seconds",
    "departure_delay_seconds": "label_source_departure_delay_seconds",
    "current_status": "label_source_current_status",
    "current_stop_sequence": "label_source_current_stop_sequence",
    "vehicle_current_stop_id": "label_source_vehicle_current_stop_id",
    "vehicle_datetime": "label_source_vehicle_datetime",
    "position_time_diff_seconds": "label_source_position_staleness_seconds",
}
future = future_source.select(*[F.col(src).alias(dest) for src, dest in FUTURE_COLUMN_MAP.items()])
future = (future
    .withColumn("future_observed_delay_seconds", F.coalesce(
        "label_source_arrival_delay_seconds", "label_source_departure_delay_seconds"))
    .withColumn("label_delay_source", F.when(F.col("label_source_arrival_delay_seconds").isNotNull(), F.lit("arrival"))
        .when(F.col("label_source_departure_delay_seconds").isNotNull(), F.lit("departure_fallback")))
    .withColumn("label_delay_source_priority", F.when(F.col("label_delay_source") == "arrival", 0).otherwise(1))
)
future.agg(
    F.count("*").alias("future_source_rows"),
    n_where(F.col("future_observed_delay_seconds").isNull()).alias("rows_without_delay"),
    n_where(F.col("label_delay_source") == "departure_fallback").alias("departure_fallback_rows"),
).show(truncate=False)
assert_no_rows(future,
    F.isnan(F.col("future_observed_delay_seconds").cast("double"))
    | (F.abs(F.col("future_observed_delay_seconds").cast("double")) == float("inf")),
    "Non-finite future delay; inspect input rather than silently selecting a later label")
future = future.filter(F.col("future_observed_delay_seconds").isNotNull()).dropDuplicates()
if set(preserved_columns) & set(future.columns):
    raise ValueError("Future-label audit names collide with existing feature columns")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Future Snapshot Matching
# MAGIC Use a bounded left join to retain every feature row, then rank candidates by earliest future timestamp, arrival-source preference on ties and a complete deterministic ordering of audit fields.
# MAGIC Identical projected future records are collapsed before ranking, so selecting one candidate preserves the feature grain without forcing coverage.

# COMMAND ----------

def match_future_labels(current_df, future_df):
    c, f = current_df.alias("c"), future_df.alias("f")
    horizon = F.col("f.label_source_feed_timestamp") - F.col("c.feed_timestamp")
    current_start = F.trim(F.col("c.start_time"))
    future_start = F.trim(F.col("f.label_source_start_time"))
    start_compatible = (
        current_start.isNull() | (current_start == "")
        | future_start.isNull() | (future_start == "") | (current_start == future_start)
    )
    condition = (
        (F.col("c.trip_id") == F.col("f.label_source_trip_id"))
        & (F.col("c.service_date") == F.col("f.label_source_service_date"))
        & (F.col("c.target_next_stop_id") == F.col("f.label_source_stop_id"))
        & (F.col("c.target_next_stop_sequence") == F.col("f.label_source_stop_sequence"))
        & (horizon > 0) & (horizon <= MAX_LABEL_HORIZON_SECONDS) & start_compatible
    )
    candidates = c.join(f, condition, "left").select(
        "c.*", *[F.col(f"f.{name}").alias(name) for name in future_df.columns])
    rank_columns = ["label_source_feed_timestamp", "label_delay_source_priority"]
    rank_columns += sorted(set(future_df.columns) - set(rank_columns))
    ranking = Window.partitionBy(*OBS_KEY).orderBy(*[F.col(name).asc_nulls_last() for name in rank_columns])
    selected = candidates.withColumn("_label_rank", F.row_number().over(ranking)).filter(F.col("_label_rank") == 1).drop("_label_rank")
    return selected

matched = match_future_labels(current, future)
matched_count = matched.count()
print("Rows after selecting one future candidate:", matched_count)
if matched_count != total_feature_rows:
    raise ValueError("Future matching changed the driving feature row count")
if duplicate_report(matched, OBS_KEY, "Matched feature grain")["duplicate_extra_rows"]:
    raise ValueError("Future matching duplicated feature observations")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Realized Delay Label
# MAGIC Assign the selected future observation's arrival delay, or documented departure fallback, to the final regression target without changing the original feature table.
# MAGIC Keep the same-snapshot regression and classification labels under provisional audit names, and retain missing future labels as null until coverage has been reported.

# COMMAND ----------

labeled_all = (matched
    .withColumn("target_realized_next_stop_delay_seconds", F.col("future_observed_delay_seconds"))
    .withColumn("label_horizon_seconds", F.col("label_source_feed_timestamp") - F.col("feed_timestamp"))
    .withColumn("label_max_horizon_seconds", F.lit(MAX_LABEL_HORIZON_SECONDS))
    .withColumn("label_definition", F.lit("earliest_future_gtfs_stop_delay_proxy"))
    .withColumn("labeling_feature_delta_version", F.lit(source_versions["features"]))
    .withColumn("labeling_realtime_delta_version", F.lit(source_versions["realtime"]))
    .withColumn("provisional_target_error_seconds",
        F.col("provisional_next_stop_delay_seconds") - F.col("target_realized_next_stop_delay_seconds"))
    .withColumn("absolute_provisional_target_error_seconds", F.abs("provisional_target_error_seconds"))
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Label Provenance and Realization Quality
# MAGIC High quality requires STOPPED_AT plus matching vehicle stop ID and sequence, while medium requires the same stop consistency with INCOMING_AT or IN_TRANSIT_TO; both require vehicle evidence later than t0, no later than the label snapshot and at most 180 seconds stale.
# MAGIC Other valid labels are basic, and unmatched rows have null quality; these grades express supporting evidence rather than certified arrival truth and do not change earliest-snapshot selection.

# COMMAND ----------

target_vehicle_consistency = (
    (F.col("label_source_current_stop_sequence") == F.col("label_source_stop_sequence"))
    & (F.col("label_source_vehicle_current_stop_id") == F.col("label_source_stop_id"))
)
fresh_future_vehicle_evidence = (
    (F.col("label_source_vehicle_datetime") > F.col("feed_datetime"))
    & (F.col("label_source_vehicle_datetime") <= F.col("label_source_feed_datetime"))
    & F.col("label_source_position_staleness_seconds").between(0, MAX_QUALITY_POSITION_STALENESS_SECONDS)
)
strong_evidence = target_vehicle_consistency & fresh_future_vehicle_evidence
labeled_all = labeled_all.withColumn("label_realization_quality",
    F.when(F.col("target_realized_next_stop_delay_seconds").isNull(), F.lit(None).cast("string"))
    .when(strong_evidence & (F.col("label_source_current_status") == "STOPPED_AT"), F.lit("high"))
    .when(strong_evidence & F.col("label_source_current_status").isin("INCOMING_AT", "IN_TRANSIT_TO"), F.lit("medium"))
    .otherwise(F.lit("basic")))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Classification Target
# MAGIC Derive the final binary target exclusively from the realized regression label, with major delay defined as at least 300 seconds.
# MAGIC Unlabeled observations retain a null class rather than being turned into a non-major-delay example, and the upstream threshold column remains unchanged.

# COMMAND ----------

labeled_all = labeled_all.withColumn("target_realized_major_delay_flag",
    F.when(F.col("target_realized_next_stop_delay_seconds").isNotNull(),
        (F.col("target_realized_next_stop_delay_seconds") >= major_delay_threshold_seconds).cast("int")))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Label Coverage
# MAGIC Measure label availability across every input feature row before excluding any unmatched observation.
# MAGIC Limited snapshot history can legitimately produce low or zero coverage; missing future observations are never imputed as zero delay.

# COMMAND ----------

coverage = labeled_all.agg(
    F.count("*").alias("total_feature_rows"),
    F.count("target_realized_next_stop_delay_seconds").alias("rows_with_realized_label"),
    n_where(F.col("target_realized_next_stop_delay_seconds").isNull()).alias("rows_without_realized_label"),
).withColumn("realized_label_coverage_pct", F.when(F.col("total_feature_rows") > 0,
    100.0 * F.col("rows_with_realized_label") / F.col("total_feature_rows")))
coverage.show(truncate=False, vertical=True)
coverage_metrics = coverage.first().asDict()
if coverage_metrics["total_feature_rows"] != total_feature_rows:
    raise ValueError("Label coverage denominator differs from feature input count")
labeled_all.groupBy("label_realization_quality", "label_delay_source").count().orderBy(
    "label_realization_quality", "label_delay_source").show(truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Preservation
# MAGIC Compare all preserved source columns before label filtering, allowing only the explicit renaming of provisional label audit fields.
# MAGIC No lag, historical, vehicle, alert or distance feature is recomputed; trip_progress_ratio and sparse vehicle fields remain unchanged for later modeling decisions.

# COMMAND ----------

preserved_projection = labeled_all.select(*preserved_columns)
if (preserved_projection.exceptAll(current.select(*preserved_columns)).limit(1).count()
    or current.select(*preserved_columns).exceptAll(preserved_projection).limit(1).count()):
    raise ValueError("Source feature or audit values changed during labeling")
print("All upstream feature values preserved")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Leakage and Causality Validation
# MAGIC Require every non-null label to come from the same trip, service date and target stop, strictly after t0 within the configured horizon.
# MAGIC Explicit same-snapshot and past-snapshot counts must be zero, and every classification label must agree with the realized regression label; violations raise ValueError.

# COMMAND ----------

def validate_causality(df, require_complete=False):
    has_label = F.col("target_realized_next_stop_delay_seconds").isNotNull()
    conditions = {
        "future_timestamp": F.col("label_source_feed_timestamp") > F.col("feed_timestamp"),
        "positive_horizon": F.col("label_horizon_seconds") > 0,
        "bounded_horizon": F.col("label_horizon_seconds") <= MAX_LABEL_HORIZON_SECONDS,
        "horizon_arithmetic": F.col("label_horizon_seconds").eqNullSafe(
            F.col("label_source_feed_timestamp") - F.col("feed_timestamp")),
        "trip_consistency": F.col("label_source_trip_id").eqNullSafe(F.col("trip_id")),
        "service_date_consistency": F.col("label_source_service_date").eqNullSafe(F.col("service_date")),
        "stop_id_consistency": F.col("label_source_stop_id").eqNullSafe(F.col("target_next_stop_id")),
        "stop_sequence_consistency": F.col("label_source_stop_sequence").eqNullSafe(F.col("target_next_stop_sequence")),
        "source_datetime_consistency": F.col("label_source_feed_datetime").eqNullSafe(F.timestamp_seconds("label_source_feed_timestamp")),
        "delay_source_consistency": F.col("target_realized_next_stop_delay_seconds").eqNullSafe(
            F.coalesce("label_source_arrival_delay_seconds", "label_source_departure_delay_seconds")),
        "classification_consistency": F.col("target_realized_major_delay_flag").eqNullSafe(
            (F.col("target_realized_next_stop_delay_seconds") >= major_delay_threshold_seconds).cast("int")),
    }
    summary = df.agg(
        n_where(F.col("label_source_feed_timestamp") == F.col("feed_timestamp")).alias("same_snapshot_label_count"),
        n_where(F.col("label_source_feed_timestamp") < F.col("feed_timestamp")).alias("past_snapshot_label_count"),
        n_where(F.col("label_source_feed_timestamp") > F.col("feed_timestamp")).alias("future_label_count"),
        n_where(~has_label).alias("target_null_count"),
        n_where(F.col("target_realized_major_delay_flag").isNull()).alias("classification_target_null_count"),
        *[n_where(has_label & ~F.coalesce(condition, F.lit(False))).alias(f"invalid_{name}")
          for name, condition in conditions.items()],
        n_where(~has_label & (F.col("label_source_feed_timestamp").isNotNull()
            | F.col("target_realized_major_delay_flag").isNotNull())).alias("invalid_unlabeled_provenance"),
    )
    summary.show(truncate=False, vertical=True)
    metrics = summary.first().asDict()
    failures = {name: count for name, count in metrics.items()
        if (name.startswith("invalid_") or name in ["same_snapshot_label_count", "past_snapshot_label_count"]) and count != 0}
    if require_complete and (metrics["target_null_count"] or metrics["classification_target_null_count"]):
        failures["missing_final_targets"] = metrics["target_null_count"] + metrics["classification_target_null_count"]
    if failures:
        raise ValueError(f"Label causality validation failed: {failures}")
    return metrics

causality_metrics = validate_causality(labeled_all)
if causality_metrics["future_label_count"] != coverage_metrics["rows_with_realized_label"]:
    raise ValueError("Future-label count disagrees with coverage")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Forbidden Model Features
# MAGIC List every newly created label/provenance field and all provisional and final targets as audit-only data excluded from model inputs.
# MAGIC Downstream modeling must reuse notebook 08's explicit input feature lists rather than infer inputs by dropping only the final target; no definitive feature selection is performed here.

# COMMAND ----------

final_target_columns = ["target_realized_next_stop_delay_seconds", "target_realized_major_delay_flag"]
label_audit_columns = sorted(set(
    ["provisional_next_stop_delay_seconds", "provisional_major_delay_flag", "provisional_target_definition",
     "provisional_target_error_seconds", "absolute_provisional_target_error_seconds",
     "label_source_feed_timestamp", "label_source_feed_datetime", "label_horizon_seconds",
     "label_source_stop_id", "label_source_stop_sequence", "label_source_trip_id",
     "label_source_service_date", "label_realization_quality", "major_delay_threshold_seconds"]
    + final_target_columns
    + [name for name in labeled_all.columns if name not in preserved_columns]
    + [name for name in preserved_columns if name.startswith("target_") or name.endswith("_audit")]
))
if not set(final_target_columns).issubset(label_audit_columns):
    raise ValueError("Targets absent from forbidden-model-feature list")
if not (set(labeled_all.columns) - set(preserved_columns)).issubset(label_audit_columns):
    raise ValueError("New future-derived fields are not marked audit-only")
print("Label and audit columns excluded from modeling:", label_audit_columns)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Matching Edge Checks
# MAGIC Exercise the actual matching function on small in-memory Spark fixtures covering strict future ordering, horizon boundaries, missing labels and trip/service/stop consistency.
# MAGIC The fixture also verifies earliest-snapshot precedence over delay-source preference and deterministic arrival preference only for equal timestamps, without persisting test artifacts.

# COMMAND ----------

fixture_cases = [
    ("A", 1001, 15.0), ("B", 2800, 20.0), ("C", None, None), ("D", None, None),
    ("E", None, None), ("F", None, None), ("G", None, None), ("H", 1100, 80.0),
    ("I", None, None), ("J", 1100, 90.0), ("K", None, None),
]
fixture_current = spark.createDataFrame(
    [(1000, "entity_" + stop, "trip", "current", 0, "2026-09-25", "08:00:00", stop, 1)
     for stop, _, _ in fixture_cases],
    "feed_timestamp LONG, entity_id STRING, trip_id STRING, stop_id STRING, stop_sequence INT, service_date STRING, start_time STRING, target_next_stop_id STRING, target_next_stop_sequence INT")
fixture_future = spark.createDataFrame([
    (999, "trip", "2026-09-25", "08:00:00", "A", 1, 100.0, 0),
    (1000, "trip", "2026-09-25", "08:00:00", "A", 1, 100.0, 0),
    (1001, "trip", "2026-09-25", "08:00:00", "A", 1, 15.0, 1),
    (1002, "trip", "2026-09-25", "08:00:00", "A", 1, 16.0, 0),
    (2800, "trip", "2026-09-25", "08:00:00", "B", 1, 20.0, 0),
    (2801, "trip", "2026-09-25", "08:00:00", "C", 1, 30.0, 0),
    (1000, "trip", "2026-09-25", "08:00:00", "D", 1, 40.0, 0),
    (1100, "other_trip", "2026-09-25", "08:00:00", "E", 1, 50.0, 0),
    (1100, "trip", "2026-09-26", "08:00:00", "F", 1, 60.0, 0),
    (1100, "trip", "2026-09-25", "08:00:00", "G", 2, 70.0, 0),
    (1100, "trip", "2026-09-25", "08:00:00", "H", 1, 81.0, 1),
    (1100, "trip", "2026-09-25", "08:00:00", "H", 1, 80.0, 0),
    (1100, "trip", "2026-09-25", "09:00:00", "I", 1, 90.0, 0),
    (1100, "trip", "2026-09-25", None, "J", 1, 90.0, 0),
    (1100, "trip", "2026-09-25", "08:00:00", "K", 1, None, 1),
], "label_source_feed_timestamp LONG, label_source_trip_id STRING, label_source_service_date STRING, label_source_start_time STRING, label_source_stop_id STRING, label_source_stop_sequence INT, future_observed_delay_seconds DOUBLE, label_delay_source_priority INT")
fixture_future = fixture_future.filter(F.col("future_observed_delay_seconds").isNotNull()).dropDuplicates()
fixture_expected = spark.createDataFrame(
    [("entity_" + stop, ts, delay) for stop, ts, delay in fixture_cases],
    "entity_id STRING, expected_timestamp LONG, expected_delay DOUBLE")
fixture_result = match_future_labels(fixture_current, fixture_future)
if fixture_result.count() != len(fixture_cases):
    raise ValueError("Matching edge test lost or duplicated input rows")
assert_no_rows(fixture_result.join(fixture_expected, "entity_id"),
    ~F.col("label_source_feed_timestamp").eqNullSafe(F.col("expected_timestamp"))
    | ~F.col("future_observed_delay_seconds").eqNullSafe(F.col("expected_delay")),
    "Future matching edge test failed")
print("Future matching edge checks passed")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Training-Ready Labeled Dataset
# MAGIC After reporting coverage, retain only rows with a non-null realized regression label while preserving every upstream feature value.
# MAGIC Zero-label history produces an empty training-ready table and an explicit message rather than fabricated outcomes or artificially increased coverage.

# COMMAND ----------

training_ready = labeled_all.filter(F.col("target_realized_next_stop_delay_seconds").isNotNull())
training_count = training_ready.count()
if training_count != coverage_metrics["rows_with_realized_label"]:
    raise ValueError("Training-ready count disagrees with label coverage")
if training_count == 0:
    print("No future labels within the configured horizon; output is empty and more future history is required")
if duplicate_report(training_ready, OBS_KEY, "Final labeled grain")["duplicate_extra_rows"]:
    raise ValueError("Final labeled feature grain is duplicated")
validate_causality(training_ready, require_complete=True)


# COMMAND ----------

# DBTITLE 1,Realized Label Quality Gate
# MAGIC %md
# MAGIC ## Realized Label Quality Gate
# MAGIC Apply a ±12-hour plausibility threshold (`MAX_PLAUSIBLE_ABS_REALIZED_DELAY_SECONDS = 43200`) to the realized regression target before persisting the training-ready dataset.
# MAGIC This is a data-quality rule, not arbitrary outlier trimming: analysis of the full labeled set shows that 31,317 rows (~0.31%) have an absolute realized delay exceeding 12 hours, and ~99.8% of those occur between feed hours 21:00 and 03:59, with only 36.3% already extreme in the provisional GTFS-RT target.
# MAGIC The pattern strongly indicates midnight/service-day rollover or GTFS-RT feed artifacts rather than plausible next-stop operational delay.
# MAGIC Rejected rows are excluded from the persisted table; values are not capped, winsorized, or replaced. Delays between 1 hour and 12 hours are retained, and the 30-minute label horizon, causal matching, and provisional targets remain unchanged.

# COMMAND ----------

# DBTITLE 1,Apply quality gate
MAX_PLAUSIBLE_ABS_REALIZED_DELAY_SECONDS = 43200

pre_gate_count = training_ready.count()
rejected_extreme_count = training_ready.filter(
    F.abs(F.col("target_realized_next_stop_delay_seconds")) > MAX_PLAUSIBLE_ABS_REALIZED_DELAY_SECONDS
).count()
rejected_extreme_pct = (100.0 * rejected_extreme_count / pre_gate_count) if pre_gate_count else 0.0

quality_gate_report = training_ready.agg(
    F.lit(pre_gate_count).alias("total_realized_labels_before_quality_gate"),
    F.lit(rejected_extreme_count).alias("rejected_extreme_label_count"),
    F.lit(rejected_extreme_pct).alias("rejected_extreme_label_pct"),
)
quality_gate_report.show(truncate=False, vertical=True)

training_ready = training_ready.filter(
    F.abs(F.col("target_realized_next_stop_delay_seconds")) <= MAX_PLAUSIBLE_ABS_REALIZED_DELAY_SECONDS
)
training_count = training_ready.count()

if duplicate_report(training_ready, OBS_KEY, "Post-quality-gate labeled grain")["duplicate_extra_rows"]:
    raise ValueError("Post-quality-gate labeled grain is duplicated")
validate_causality(training_ready, require_complete=True)

training_ready.agg(
    F.count("*").alias("final_training_label_count"),
    (100.0 * F.avg("target_realized_major_delay_flag")).alias("final_major_delay_rate_pct"),
    F.avg("target_realized_next_stop_delay_seconds").alias("mean"),
    F.percentile_approx("target_realized_next_stop_delay_seconds", 0.5, 10000).alias("median"),
    F.stddev_samp("target_realized_next_stop_delay_seconds").alias("stddev"),
    F.min("target_realized_next_stop_delay_seconds").alias("min"),
    F.max("target_realized_next_stop_delay_seconds").alias("max"),
    F.percentile_approx("target_realized_next_stop_delay_seconds", 0.90, 10000).alias("P90"),
    F.percentile_approx("target_realized_next_stop_delay_seconds", 0.95, 10000).alias("P95"),
    F.percentile_approx("target_realized_next_stop_delay_seconds", 0.99, 10000).alias("P99"),
).show(truncate=False, vertical=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Label Horizon Distribution
# MAGIC Summarize how far into the future each selected label is observed, including the requested median and upper percentiles.
# MAGIC The distribution describes temporal coverage within the configured 30-minute cap and does not alter label selection.

# COMMAND ----------

training_ready.agg(
    F.avg("label_horizon_seconds").alias("mean_label_horizon_seconds"),
    F.percentile_approx("label_horizon_seconds", 0.5, 10000).alias("median_label_horizon_seconds"),
    *[F.percentile_approx("label_horizon_seconds", q, 10000).alias(name)
      for q, name in [(0.75, "P75"), (0.90, "P90"), (0.95, "P95"), (0.99, "P99")]],
    F.max("label_horizon_seconds").alias("max_label_horizon_seconds"),
).show(truncate=False, vertical=True)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Regression Target Distribution
# MAGIC Report the future-delay distribution without arbitrary outlier removal, including sample count, central tendency, dispersion, extremes and percentiles.
# MAGIC These diagnostics characterize the labeled population and should not be used to tune against a future test partition.

# COMMAND ----------

training_ready.agg(
    F.count("target_realized_next_stop_delay_seconds").alias("count"),
    F.avg("target_realized_next_stop_delay_seconds").alias("mean"),
    F.percentile_approx("target_realized_next_stop_delay_seconds", 0.5, 10000).alias("median"),
    F.stddev_samp("target_realized_next_stop_delay_seconds").alias("stddev"),
    F.min("target_realized_next_stop_delay_seconds").alias("min"),
    F.max("target_realized_next_stop_delay_seconds").alias("max"),
    *[F.percentile_approx("target_realized_next_stop_delay_seconds", q, 10000).alias(name)
      for q, name in [(0.50, "P50"), (0.75, "P75"), (0.90, "P90"), (0.95, "P95"), (0.99, "P99")]],
).show(truncate=False, vertical=True)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Classification Target Distribution
# MAGIC Measure class prevalence using the realized target alone, with the 300-second definition unchanged.
# MAGIC Counts remain zero for an empty labeled table, while the undefined class rate remains null.

# COMMAND ----------

training_ready.agg(
    n_where(F.col("target_realized_major_delay_flag") == 1).alias("major_delay_count"),
    n_where(F.col("target_realized_major_delay_flag") == 0).alias("non_major_delay_count"),
    (100.0 * F.avg("target_realized_major_delay_flag")).alias("major_delay_rate_pct"),
).show(truncate=False, vertical=True)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Provisional Target Quality Analysis
# MAGIC Compare the original t0 estimate with the selected future delay through absolute-error summaries and correlation.
# MAGIC These are audit diagnostics only, and differences can reflect forecast revisions or the documented arrival/departure fallback rather than measured arrival error.

# COMMAND ----------

training_ready.agg(
    F.count("absolute_provisional_target_error_seconds").alias("compared_label_pairs"),
    F.avg("absolute_provisional_target_error_seconds").alias("mean_absolute_provisional_error"),
    F.percentile_approx("absolute_provisional_target_error_seconds", 0.5, 10000).alias("median_absolute_provisional_error"),
    F.percentile_approx("absolute_provisional_target_error_seconds", 0.90, 10000).alias("P90_absolute_error"),
    F.percentile_approx("absolute_provisional_target_error_seconds", 0.95, 10000).alias("P95_absolute_error"),
    F.corr("provisional_next_stop_delay_seconds", "target_realized_next_stop_delay_seconds").alias("provisional_realized_correlation"),
).show(truncate=False, vertical=True)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Persist Labeled Feature Table
# MAGIC Overwrite only rome_transport.features.next_stop_delay_labeled as Delta after coverage, feature preservation, duplicate and causality checks.
# MAGIC Silver, Bronze and the notebook 08 feature table remain unchanged, including all-null trip progress and high-missingness vehicle features.

# COMMAND ----------

training_ready.printSchema()
(training_ready.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(OUTPUT_TABLE))
print("Saved:", OUTPUT_TABLE)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Read-Back Validation
# MAGIC Read the persisted output and verify counts, duplicate grain, non-null targets and future-label causality.
# MAGIC These checks confirm that saved records satisfy the same conditions as the validated in-memory labeled dataset.

# COMMAND ----------

saved = spark.table(OUTPUT_TABLE)
saved_summary = saved.agg(
    F.count("*").alias("total_rows"),
    F.countDistinct("feed_timestamp").alias("distinct_feed_snapshots"),
    F.countDistinct("trip_id").alias("distinct_trips"),
    F.countDistinct("vehicle_id").alias("distinct_vehicles"),
    F.countDistinct("route_id").alias("distinct_routes"),
)
saved_summary.show(truncate=False)
if saved_summary.first()["total_rows"] != training_count:
    raise ValueError("Persisted row count differs from validated labeled count")
if duplicate_report(saved, OBS_KEY, "Persisted labeled grain")["duplicate_extra_rows"]:
    raise ValueError("Persisted labeled grain contains duplicate rows")
validate_causality(saved, require_complete=True)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample Output
# MAGIC Display 20 rows ordered by the original observation key to inspect provisional versus future targets and label provenance.
# MAGIC The sample includes prediction time, horizon and realization quality without promoting future fields to model inputs.

# COMMAND ----------

saved.orderBy(*OBS_KEY).select(
    "route_id", "trip_id", "stop_id", "stop_sequence", "feed_datetime",
    "current_arrival_delay_seconds", "target_next_stop_id", "target_next_stop_sequence",
    "provisional_next_stop_delay_seconds", "target_realized_next_stop_delay_seconds",
    "label_source_feed_datetime", "label_horizon_seconds", "target_realized_major_delay_flag",
    "label_realization_quality",
).show(20, truncate=False)
print("Realized-label coverage:", coverage_metrics)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Modeling Handoff and Limitations
# MAGIC The earliest future snapshot is a stronger temporal label than the same-snapshot forecast, but basic/medium grades and departure fallback remain outcome proxies; sparse feeds, missing start times and the 30-minute cap can limit coverage or trip-instance certainty.
# MAGIC Use only notebook 08's declared feature inputs, exclude all labels/audit fields, and exclude trip_progress_ratio during modeling if it remains entirely null; keep sparse vehicle features until feature selection is evaluated there.
# MAGIC For chronological splits, training labels must be available before the validation cutoff using label_source_feed_timestamp, so purge or embargo boundary-crossing examples; source-feed timestamps do not independently prove ingestion-time availability.