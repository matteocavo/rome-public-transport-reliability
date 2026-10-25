# Databricks notebook source
# MAGIC %md
# MAGIC # Shared observable feature preparation
# MAGIC Load with `%run ./08_common_feature_preparation`. Definitions only: no table reads/writes or fitting.

# COMMAND ----------

import hashlib
import json
from functools import reduce
from pyspark.sql import functions as F
from pyspark.sql.window import Window

MODEL_CONTRACT = {'categorical': ['route_id', 'route_type', 'direction_id', 'agency_id', 'current_status'],
 'dropLast': False,
 'encoderHandleInvalid': 'keep',
 'forbidden': ['absolute_provisional_target_error_seconds',
               'entity_id',
               'future_observed_delay_seconds',
               'historical_route_hour_max_feed_timestamp_audit',
               'historical_route_hour_nonnull_delay_count_audit',
               'historical_route_max_feed_timestamp_audit',
               'historical_route_nonnull_delay_count_audit',
               'historical_route_stop_max_feed_timestamp_audit',
               'historical_route_stop_nonnull_delay_count_audit',
               'historical_stop_max_feed_timestamp_audit',
               'historical_stop_nonnull_delay_count_audit',
               'label_definition',
               'label_delay_source',
               'label_delay_source_priority',
               'label_horizon_seconds',
               'label_max_horizon_seconds',
               'label_realization_quality',
               'label_source_arrival_delay_seconds',
               'label_source_current_status',
               'label_source_current_stop_sequence',
               'label_source_departure_delay_seconds',
               'label_source_entity_id',
               'label_source_feed_datetime',
               'label_source_feed_timestamp',
               'label_source_position_staleness_seconds',
               'label_source_service_date',
               'label_source_start_time',
               'label_source_stop_id',
               'label_source_stop_sequence',
               'label_source_trip_id',
               'label_source_vehicle_current_stop_id',
               'label_source_vehicle_datetime',
               'label_source_vehicle_id',
               'labeling_feature_delta_version',
               'labeling_realtime_delta_version',
               'lag_1_stop_sequence_audit',
               'major_delay_threshold_seconds',
               'next_scheduled_arrival_seconds_audit',
               'next_static_stop_lat_audit',
               'next_static_stop_lon_audit',
               'provisional_major_delay_flag',
               'provisional_next_stop_delay_seconds',
               'provisional_target_definition',
               'provisional_target_error_seconds',
               'rolling_max_stop_sequence_audit',
               'source_delta_version',
               'stop_id',
               'target_feed_timestamp',
               'target_major_delay_flag',
               'target_next_stop_delay_seconds',
               'target_next_stop_id',
               'target_next_stop_sequence',
               'target_realized_major_delay_flag',
               'target_realized_next_stop_delay_seconds',
               'target_service_date',
               'target_stop_sequence_gap',
               'target_trip_id',
               'trip_id',
               'vehicle_id'],
 'imputer': 'median',
 'indexerHandleInvalid': 'keep',
 'numeric': ['feed_hour',
             'feed_minute',
             'day_of_week',
             'is_weekend',
             'minutes_since_midnight',
             'current_arrival_delay_seconds',
             'current_departure_delay_seconds',
             'abs_current_delay_seconds',
             'is_currently_delayed_flag',
             'is_currently_early_flag',
             'lag_1_arrival_delay_seconds',
             'lag_2_arrival_delay_seconds',
             'lag_3_arrival_delay_seconds',
             'rolling_mean_delay_last_3_stops',
             'rolling_max_delay_last_3_stops',
             'rolling_std_delay_last_3_stops',
             'delay_change_from_previous_stop',
             'delay_acceleration',
             'scheduled_seconds_to_next_stop',
             'distance_to_next_stop_meters',
             'vehicle_speed',
             'vehicle_bearing',
             'vehicle_odometer',
             'current_stop_sequence',
             'vehicle_position_staleness_seconds',
             'vehicle_position_available_flag',
             'active_alert_flag',
             'active_alert_count',
             'active_detour_flag',
             'active_construction_flag',
             'historical_route_mean_delay',
             'historical_stop_mean_delay',
             'historical_route_stop_mean_delay',
             'historical_route_hour_mean_delay',
             'historical_route_observation_count',
             'historical_stop_observation_count',
             'missing_vehicle_position',
             'missing_current_arrival_delay',
             'missing_current_departure_delay',
             'missing_historical_route_delay',
             'missing_historical_stop_delay',
             'missing_alert_snapshot'],
 'relativeError': 0.001,
 'scalerWithMean': False,
 'scalerWithStd': True,
 'stringOrderType': 'alphabetAsc'}
FEATURE_CONTRACT_HASH = '11682b3cc1ab5c1d8440af4f6294e932e815126c3dd86ab609f4d5f8f35c41a5'
NUMERICAL_FEATURES = MODEL_CONTRACT["numeric"]
CATEGORICAL_FEATURES = MODEL_CONTRACT["categorical"]
MODEL_FEATURES = NUMERICAL_FEATURES + CATEGORICAL_FEATURES
MODEL_RAW_TYPES = {'feed_hour': 'int',
 'feed_minute': 'int',
 'day_of_week': 'int',
 'is_weekend': 'int',
 'minutes_since_midnight': 'int',
 'current_arrival_delay_seconds': 'int',
 'current_departure_delay_seconds': 'int',
 'abs_current_delay_seconds': 'int',
 'is_currently_delayed_flag': 'int',
 'is_currently_early_flag': 'int',
 'lag_1_arrival_delay_seconds': 'int',
 'lag_2_arrival_delay_seconds': 'int',
 'lag_3_arrival_delay_seconds': 'int',
 'rolling_mean_delay_last_3_stops': 'double',
 'rolling_max_delay_last_3_stops': 'int',
 'rolling_std_delay_last_3_stops': 'double',
 'delay_change_from_previous_stop': 'int',
 'delay_acceleration': 'int',
 'scheduled_seconds_to_next_stop': 'int',
 'distance_to_next_stop_meters': 'double',
 'vehicle_speed': 'double',
 'vehicle_bearing': 'double',
 'vehicle_odometer': 'double',
 'current_stop_sequence': 'int',
 'vehicle_position_staleness_seconds': 'bigint',
 'vehicle_position_available_flag': 'int',
 'active_alert_flag': 'int',
 'active_alert_count': 'int',
 'active_detour_flag': 'int',
 'active_construction_flag': 'int',
 'historical_route_mean_delay': 'double',
 'historical_stop_mean_delay': 'double',
 'historical_route_stop_mean_delay': 'double',
 'historical_route_hour_mean_delay': 'double',
 'historical_route_observation_count': 'bigint',
 'historical_stop_observation_count': 'bigint',
 'missing_vehicle_position': 'int',
 'missing_current_arrival_delay': 'int',
 'missing_current_departure_delay': 'int',
 'missing_historical_route_delay': 'int',
 'missing_historical_stop_delay': 'int',
 'missing_alert_snapshot': 'int',
 'route_id': 'string',
 'route_type': 'int',
 'direction_id': 'int',
 'agency_id': 'string',
 'current_status': 'string'}
OBS_KEY = ['feed_timestamp', 'entity_id', 'trip_id', 'stop_sequence', 'stop_id']
TRIP_SNAPSHOT_KEY = ['feed_timestamp', 'trip_id', 'service_date', 'entity_id', 'start_time', 'vehicle_id']
HISTORY_SPECS = [('route', ['route_id'], 'historical_route_mean_delay'),
 ('stop', ['stop_id'], 'historical_stop_mean_delay'),
 ('route_stop', ['route_id', 'stop_id'], 'historical_route_stop_mean_delay'),
 ('route_hour', ['route_id', 'feed_hour'], 'historical_route_hour_mean_delay')]
REQUIRED_SOURCE_COLUMNS = ['feed_timestamp',
 'feed_datetime',
 'service_date',
 'entity_id',
 'trip_id',
 'vehicle_id',
 'start_time',
 'route_id',
 'route_short_name',
 'route_type',
 'agency_id',
 'direction_id',
 'stop_id',
 'stop_sequence',
 'arrival_delay_seconds',
 'departure_delay_seconds',
 'arrival_seconds',
 'stop_lat',
 'stop_lon',
 'vehicle_speed',
 'vehicle_bearing',
 'vehicle_odometer',
 'current_stop_sequence',
 'current_status',
 'vehicle_datetime',
 'position_time_diff_seconds',
 'active_alert_flag',
 'active_alert_count',
 'active_detour_flag',
 'active_construction_flag',
 'alert_causes',
 'alert_effects',
 'active_alert_ids',
 'alert_snapshot_available_flag',
 'matched_alert_feed_timestamp',
 'alert_snapshot_age_seconds']
historical_audit_columns = [name for label, _, _ in HISTORY_SPECS for name in [f"historical_{label}_max_feed_timestamp_audit", f"historical_{label}_nonnull_delay_count_audit"]]

def historical_window(keys):
    return Window.partitionBy(*keys).orderBy('feed_timestamp').rangeBetween(Window.unboundedPreceding, -1)

def haversine_meters(lat1, lon1, lat2, lon2):
    p1, p2 = (F.radians(lat1), F.radians(lat2))
    a = F.pow(F.sin((p2 - p1) / 2), 2) + F.cos(p1) * F.cos(p2) * F.pow(F.sin(F.radians(lon2 - lon1) / 2), 2)
    valid = lat1.between(-90, 90) & lat2.between(-90, 90) & lon1.between(-180, 180) & lon2.between(-180, 180)
    return F.when(valid, 2 * 6371000.0 * F.asin(F.sqrt(F.least(F.lit(1.0), F.greatest(F.lit(0.0), a)))))

def assert_no_rows(df, condition, message):
    if df.filter(condition).limit(1).count():
        raise ValueError(message)


def verify_feature_contract():
    digest = hashlib.sha256(json.dumps(MODEL_CONTRACT, sort_keys=True).encode()).hexdigest()
    if digest != FEATURE_CONTRACT_HASH or len(MODEL_FEATURES) != 47:
        raise ValueError("Frozen feature contract mismatch")
    if len(NUMERICAL_FEATURES) != 42 or len(CATEGORICAL_FEATURES) != 5 or len(set(MODEL_FEATURES)) != 47:
        raise ValueError("Frozen feature order/count mismatch")
    return digest


def prepare_common_features(observations):
    """Use observable Silver history; no labels, outcome filtering, fitting or writes."""
    stop_order = Window.partitionBy(*TRIP_SNAPSHOT_KEY).orderBy("stop_sequence")
    last_three_stops = stop_order.rowsBetween(-2, 0)
    current_delay_tolerance_seconds = 60
    features = observations
    for source_col, target_col in [
        ("stop_sequence", "target_next_stop_sequence"), ("stop_id", "target_next_stop_id"),
        ("feed_timestamp", "target_feed_timestamp"), ("trip_id", "target_trip_id"),
        ("service_date", "target_service_date"),
    ]:
        features = features.withColumn(target_col, F.lead(source_col, 1).over(stop_order))





    features = (features.withColumn("feed_hour", F.hour("feed_datetime"))
        .withColumn("feed_minute", F.minute("feed_datetime"))
        .withColumn("day_of_week", F.dayofweek("service_date"))
        .withColumn("is_weekend", F.col("day_of_week").isin(1, 7).cast("int"))
        .withColumn("service_month", F.month("service_date"))
        .withColumn("service_day_of_month", F.dayofmonth("service_date"))
        .withColumn("minutes_since_midnight", F.col("feed_hour") * 60 + F.col("feed_minute")))





    features = (features
        .withColumn("current_arrival_delay_seconds", F.col("arrival_delay_seconds"))
        .withColumn("current_arrival_delay_minutes", F.col("arrival_delay_seconds") / 60.0)
        .withColumn("current_departure_delay_seconds", F.col("departure_delay_seconds"))
        .withColumn("current_departure_delay_minutes", F.col("departure_delay_seconds") / 60.0)
        .withColumn("abs_current_delay_seconds", F.abs("current_arrival_delay_seconds"))
        .withColumn("is_currently_delayed_flag", (F.col("current_arrival_delay_seconds") > current_delay_tolerance_seconds).cast("int"))
        .withColumn("is_currently_early_flag", (F.col("current_arrival_delay_seconds") < -current_delay_tolerance_seconds).cast("int")))





    for offset in (1, 2, 3):
        features = features.withColumn(f"lag_{offset}_arrival_delay_seconds", F.lag("arrival_delay_seconds", offset).over(stop_order))
    features = (features
        .withColumn("rolling_mean_delay_last_3_stops", F.avg("arrival_delay_seconds").over(last_three_stops))
        .withColumn("rolling_max_delay_last_3_stops", F.max("arrival_delay_seconds").over(last_three_stops))
        .withColumn("rolling_std_delay_last_3_stops", F.stddev_samp("arrival_delay_seconds").over(last_three_stops))
        .withColumn("lag_1_stop_sequence_audit", F.lag("stop_sequence", 1).over(stop_order))
        .withColumn("rolling_max_stop_sequence_audit", F.max("stop_sequence").over(last_three_stops)))





    features = (features
        .withColumn("delay_change_from_previous_stop", F.col("current_arrival_delay_seconds") - F.col("lag_1_arrival_delay_seconds"))
        .withColumn("delay_acceleration", F.col("delay_change_from_previous_stop")
            - (F.col("lag_1_arrival_delay_seconds") - F.col("lag_2_arrival_delay_seconds"))))





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





    position_available = F.col("vehicle_datetime").isNotNull() & F.col("position_time_diff_seconds").between(0, 180)
    features = (features
        .withColumn("vehicle_position_available_flag", position_available.cast("int"))
        .withColumn("vehicle_position_staleness_seconds", F.when(position_available, F.col("position_time_diff_seconds"))))
    for name in ["vehicle_speed", "vehicle_bearing", "vehicle_odometer", "current_stop_sequence", "current_status"]:
        features = features.withColumn(name, F.when(position_available, F.col(name)))





    for name in ["stop_lat", "stop_lon", "next_static_stop_lat_audit", "next_static_stop_lon_audit"]:
        features = features.withColumn(name, F.expr(f"try_cast(`{name}` AS DOUBLE)"))
    features = features.withColumn("distance_to_next_stop_meters", haversine_meters(
        F.col("stop_lat"), F.col("stop_lon"), F.col("next_static_stop_lat_audit"), F.col("next_static_stop_lon_audit")))





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
    return features


SCORING_ID_COLUMNS = [
    "feed_timestamp", "feed_datetime", "service_date", "entity_id", "trip_id", "start_time",
    "vehicle_id", "stop_id", "stop_sequence", "target_next_stop_id", "target_next_stop_sequence",
    "target_feed_timestamp", "target_trip_id", "target_service_date", "vehicle_current_stop_id",
    "vehicle_datetime",
]
SCORING_LINEAGE_COLUMNS = [
    "source_silver_version", "feature_prepared_at", "feature_preparation_batch_id", "feature_contract_hash",
]
SCORING_COLUMNS = list(dict.fromkeys(SCORING_ID_COLUMNS + MODEL_FEATURES + SCORING_LINEAGE_COLUMNS))


def validate_model_schema(types, contract_hash=FEATURE_CONTRACT_HASH):
    """Raw nullable values are allowed; no replacement or new imputation is introduced."""
    if contract_hash != verify_feature_contract():
        raise ValueError("Feature contract hash mismatch")
    missing = sorted(set(MODEL_FEATURES) - set(types))
    if missing:
        raise ValueError(f"Missing model features: {missing}")
    numeric_types = {"byte", "tinyint", "short", "smallint", "int", "integer", "long", "bigint", "float", "double"}
    integer_types = numeric_types - {"float", "double"}
    for name in MODEL_FEATURES:
        dtype = types[name].lower()
        if name in NUMERICAL_FEATURES:
            compatible = dtype in numeric_types or dtype.startswith("decimal(")
        elif MODEL_RAW_TYPES[name] == "string":
            compatible = dtype == "string"
        else:
            compatible = dtype in integer_types
        if not compatible:
            raise ValueError(f"Incompatible feature type: {name}={dtype}")


def validate_common_causality(features):
    assert_no_rows(features,
        (F.col("lag_1_stop_sequence_audit") >= F.col("stop_sequence"))
        | (F.col("rolling_max_stop_sequence_audit") > F.col("stop_sequence")),
        "Stop history crosses the current observation")
    for label, _, _ in HISTORY_SPECS:
        assert_no_rows(features,
            F.col(f"historical_{label}_max_feed_timestamp_audit") >= F.col("feed_timestamp"),
            "Historical features include the current or a future feed timestamp")
    assert_no_rows(features,
        (F.col("vehicle_datetime") > F.col("feed_datetime"))
        | (F.col("position_time_diff_seconds") < 0) | (F.col("position_time_diff_seconds") > 180)
        | (F.col("vehicle_datetime").isNull() != F.col("position_time_diff_seconds").isNull()),
        "Invalid vehicle provenance")


def select_scoring_rows(features, feed_timestamp):
    # All reported current-stop rows at this epoch, not one arbitrary row per vehicle.
    # A next represented stop is required; its delay/label is never required or inspected.
    return features.filter(
        (F.col("feed_timestamp") == feed_timestamp)
        & F.col("target_next_stop_id").isNotNull()
        & (F.col("target_next_stop_sequence") > F.col("stop_sequence")))


def validate_scoring_rows(df):
    validate_model_schema({f.name: f.dataType.simpleString() for f in df.schema.fields})
    if "feature_contract_hash" in df.columns:
        assert_no_rows(df, ~F.col("feature_contract_hash").eqNullSafe(F.lit(FEATURE_CONTRACT_HASH)),
            "Persisted feature contract hash mismatch")
    required_ids = OBS_KEY + ["feed_datetime", "service_date", "target_next_stop_id", "target_next_stop_sequence"]
    assert_no_rows(df, reduce(lambda a, b: a | b, [F.col(c).isNull() for c in required_ids]),
        "Missing observation/next-stop identity")
    assert_no_rows(df,
        (F.col("target_next_stop_sequence") <= F.col("stop_sequence"))
        | ~F.col("target_feed_timestamp").eqNullSafe(F.col("feed_timestamp"))
        | ~F.col("target_trip_id").eqNullSafe(F.col("trip_id"))
        | ~F.col("target_service_date").eqNullSafe(F.col("service_date"))
        | ~F.col("feed_datetime").eqNullSafe(F.timestamp_seconds("feed_timestamp")),
        "Inconsistent current/next-stop keys")
    if df.groupBy(*OBS_KEY).count().filter(F.col("count") > 1).limit(1).count():
        raise ValueError("Duplicate scoring observation keys")
    # NaN/NULL are left to the frozen median imputer; infinity was never supported.
    assert_no_rows(df, reduce(lambda a, b: a | b,
        [F.abs(F.col(c).cast("double")) == float("inf") for c in NUMERICAL_FEATURES]),
        "Infinite model input")


def prepare_scoring_dataset(source, source_version, feed_timestamp, prepared_at, batch_id):
    """Prepare full past context BEFORE selecting the current snapshot; never read labeled data."""
    verify_feature_contract()
    missing = sorted(set(REQUIRED_SOURCE_COLUMNS) - set(source.columns))
    if missing:
        raise ValueError(f"Missing Silver columns: {missing}")
    if "vehicle_current_stop_id" not in source.columns:
        source = source.withColumn("vehicle_current_stop_id", F.lit(None).cast("string"))
    observable = source.select(*REQUIRED_SOURCE_COLUMNS, "vehicle_current_stop_id")
    assert_no_rows(observable, F.col("feed_timestamp").isNull(), "Missing observation feed timestamp")
    observable = observable.filter(F.col("feed_timestamp") <= feed_timestamp)
    assert_no_rows(observable,
        ~F.col("feed_datetime").eqNullSafe(F.timestamp_seconds("feed_timestamp")),
        "Feed datetime/epoch mismatch")
    ordering_ids = ["feed_timestamp", "feed_datetime", "service_date", "entity_id", "trip_id", "stop_sequence", "stop_id"]
    valid = reduce(lambda a, b: a & b, [F.col(c).isNotNull() for c in ordering_ids])
    assert_no_rows(observable, ~valid | (F.col("stop_sequence") < 0),
        "Missing or invalid scoring observation identifiers")
    for keys in [OBS_KEY, TRIP_SNAPSHOT_KEY + ["stop_sequence"]]:
        if observable.groupBy(*keys).count().filter(F.col("count") > 1).limit(1).count():
            raise ValueError("Ambiguous Silver observation/stop ordering")
    features = prepare_common_features(observable)
    validate_common_causality(features)
    scoring = select_scoring_rows(features, feed_timestamp)
    validate_scoring_rows(scoring)
    return (scoring
        .withColumn("source_silver_version", F.lit(source_version).cast("long"))
        .withColumn("feature_prepared_at", F.lit(prepared_at).cast("timestamp"))
        .withColumn("feature_preparation_batch_id", F.lit(batch_id))
        .withColumn("feature_contract_hash", F.lit(FEATURE_CONTRACT_HASH))
        .select(*SCORING_COLUMNS))
