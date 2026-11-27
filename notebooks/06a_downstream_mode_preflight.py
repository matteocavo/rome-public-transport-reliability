# Databricks notebook source
# MAGIC %md
# MAGIC # 06a Downstream mode preflight
# MAGIC Read-only task that runs before 06 and 07 in the downstream refresh job and decides ONE `run_mode`
# MAGIC (`full` or `incremental`) for both. It writes nothing: it only reads `max(feed_timestamp)` of four tables and
# MAGIC publishes task values (`run_mode`, `reason`, `window_06_seconds`, `window_07_seconds`).
# MAGIC
# MAGIC The window planners below are copied VERBATIM from notebooks 06 and 07; `tests/test_hybrid_preflight.py` fails if they drift.
# MAGIC Any anomaly (empty or null table, Bronze behind Silver, Silver behind Enriched, invalid parameter, Spark error) raises:
# MAGIC the task fails and, with `run_if=ALL_SUCCESS`, 06/07/08/13 do not run. The preflight never falls back to `full` on an error.
# MAGIC `full` is chosen only for a valid window above `max_window_seconds - preflight_margin_seconds`, or when `incremental_enabled=false`.

# COMMAND ----------

import time

from pyspark.sql import functions as F

# COMMAND ----------

# Verbatim copies of the notebook 06 / 07 definitions (checked by tests/test_hybrid_preflight.py).

OBSERVED_MAX_INGESTION_LAG_SECONDS = 901


def parse_positive_int(value, name):
    try:
        number = int(str(value).strip())
    except (TypeError, ValueError):
        raise ValueError(f"{name} must be a positive integer") from None
    if number <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return number


def assert_safety_margin(safety_seconds):
    """Fail closed if the overlap is smaller than the lag actually observed in Bronze."""
    value = parse_positive_int(safety_seconds, "safety_window_seconds")
    if value < OBSERVED_MAX_INGESTION_LAG_SECONDS:
        raise ValueError(f"safety_window_seconds={value} is below the observed max ingestion lag "
                         f"({OBSERVED_MAX_INGESTION_LAG_SECONDS}s); write blocked")
    return value


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


def plan_enrichment_window(output_max_feed, source_max_feed, safety_seconds, max_window_seconds):
    """Pure planner. The window starts below the output watermark; a partial write is repaired by
    re-running the same window."""
    if output_max_feed is None:
        raise ValueError("incremental needs a populated enriched table; run the explicit full rebuild first")
    if source_max_feed is None:
        raise ValueError("incremental: realtime observations are empty; write blocked")
    safety_seconds = parse_positive_int(safety_seconds, "safety_window_seconds")
    max_window_seconds = parse_positive_int(max_window_seconds, "max_window_seconds")
    output_max_feed, source_max_feed = int(output_max_feed), int(source_max_feed)
    if source_max_feed < output_max_feed:
        raise ValueError("incremental: Silver observations are behind the enriched table; write blocked")
    start = output_max_feed - safety_seconds
    if source_max_feed - start > max_window_seconds:
        raise ValueError("incremental: window exceeds max_window_seconds; run the explicit full rebuild")
    return {"start": start, "watermark": output_max_feed, "source_max_feed": source_max_feed,
            "window_seconds": source_max_feed - start}

# COMMAND ----------

RUN_MODE_FULL = "full"
RUN_MODE_INCREMENTAL = "incremental"
UNBOUNDED_WINDOW_SECONDS = 10 ** 12   # only used to obtain the window length from the verbatim planners
MIN_VALID_EPOCH = 1_577_836_800        # 2020-01-01T00:00:00Z: earlier epochs (e.g. -1, 0) are invalid
FUTURE_TOLERANCE_SECONDS = 300         # explicit clock-skew tolerance against the single UTC instant `now_epoch`


def validate_watermarks(now_epoch, named_values):
    """Every watermark must be a non-null epoch in [MIN_VALID_EPOCH, now_epoch + FUTURE_TOLERANCE_SECONDS]."""
    for name, value in named_values.items():
        if value is None:
            raise ValueError(f"{name} watermark is empty")
        if isinstance(value, bool) or int(value) != value:
            raise ValueError(f"{name} watermark is not an integer epoch")
        if int(value) < MIN_VALID_EPOCH:
            raise ValueError(f"{name} watermark {value} is not a valid epoch")
        if int(value) > now_epoch + FUTURE_TOLERANCE_SECONDS:
            raise ValueError(f"{name} watermark {value} is in the future")


def parse_bool(value, name):
    text = str(value).strip().lower()
    if text not in ("true", "false"):
        raise ValueError(f"{name} must be 'true' or 'false'")
    return text == "true"


def parse_non_negative_int(value, name):
    try:
        number = int(str(value).strip())
    except (TypeError, ValueError):
        raise ValueError(f"{name} must be a non-negative integer") from None
    if number < 0:
        raise ValueError(f"{name} must be a non-negative integer")
    return number


def decide_run_mode(incremental_enabled, silver_trip_updates_max, silver_observations_max, bronze_trip_updates_max,
                    enriched_max, safety_seconds, max_window_seconds, margin_seconds, now_epoch):
    """Pure decision. Returns a dict; raises ValueError on any invalid or inconsistent input (fail-closed)."""
    enabled = parse_bool(incremental_enabled, "incremental_enabled")
    safety = assert_safety_margin(safety_seconds)
    max_window = parse_positive_int(max_window_seconds, "max_window_seconds")
    margin = parse_non_negative_int(margin_seconds, "preflight_margin_seconds")
    if margin >= max_window:
        raise ValueError("preflight_margin_seconds must be smaller than max_window_seconds")
    threshold = max_window - margin
    validate_watermarks(int(now_epoch), {"silver.trip_updates": silver_trip_updates_max,
                                         "silver.realtime_trip_stop_observations": silver_observations_max,
                                         "bronze.trip_updates": bronze_trip_updates_max,
                                         "silver.realtime_enriched_observations": enriched_max})
    # Explicit ordering, BEFORE the planners (which only look at the oldest Silver watermark):
    # Bronze >= every Silver output, and Enriched <= current Silver observations.
    if bronze_trip_updates_max < max(silver_trip_updates_max, silver_observations_max):
        raise ValueError("Bronze is behind a Silver output; inconsistent state")
    if enriched_max > silver_observations_max:
        raise ValueError("Enriched is ahead of Silver observations; inconsistent state")
    # Inputs are always validated, also when incremental is disabled, so that the logged state is trustworthy.
    plan_06 = plan_incremental_window(
        {"trip_updates": silver_trip_updates_max, "realtime_trip_stop_observations": silver_observations_max},
        bronze_trip_updates_max, safety, UNBOUNDED_WINDOW_SECONDS)
    # 07 reads Silver observations AFTER 06: conservative source = the later of Bronze and current Silver observations.
    plan_07 = plan_enrichment_window(
        enriched_max, max(int(bronze_trip_updates_max), int(silver_observations_max)), safety, UNBOUNDED_WINDOW_SECONDS)
    result = {"window_06_seconds": plan_06["window_seconds"], "window_07_seconds": plan_07["window_seconds"],
              "threshold_seconds": threshold, "max_window_seconds": max_window, "margin_seconds": margin,
              "safety_window_seconds": safety}
    if not enabled:
        return dict(result, run_mode=RUN_MODE_FULL, reason="incremental_disabled")
    if max(plan_06["window_seconds"], plan_07["window_seconds"]) > threshold:
        return dict(result, run_mode=RUN_MODE_FULL, reason="window_exceeds_threshold")
    return dict(result, run_mode=RUN_MODE_INCREMENTAL, reason="window_within_threshold")


# COMMAND ----------

def read_epoch_max(table):
    frame = spark.table(table)
    if frame.schema["feed_timestamp"].dataType.simpleString() not in ("bigint", "int"):
        raise ValueError(f"{table}.feed_timestamp is not an integral epoch")
    return frame.agg(F.max("feed_timestamp")).first()[0]


# No try/except and no default: a missing widget or a Spark error fails the task (fail-closed).
DECISION = decide_run_mode(
    dbutils.widgets.get("incremental_enabled"),
    read_epoch_max("rome_transport.silver.trip_updates"),
    read_epoch_max("rome_transport.silver.realtime_trip_stop_observations"),
    read_epoch_max("rome_transport.bronze.trip_updates"),
    read_epoch_max("rome_transport.silver.realtime_enriched_observations"),
    dbutils.widgets.get("safety_window_seconds"),
    dbutils.widgets.get("max_window_seconds"),
    dbutils.widgets.get("preflight_margin_seconds"),
    int(time.time()))
print({"preflight": DECISION})
for _key in ("run_mode", "reason", "window_06_seconds", "window_07_seconds"):
    dbutils.jobs.taskValues.set(key=_key, value=DECISION[_key])
