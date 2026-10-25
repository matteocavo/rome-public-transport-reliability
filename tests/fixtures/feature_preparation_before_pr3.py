"""Original 08 transformations at main 0caf466, frozen as a parity oracle only."""

def baseline_training_features(observations, source_version):
    SOURCE_VERSION = source_version
    major_delay_threshold_seconds = 300
    current_delay_tolerance_seconds = 60
    stop_order = Window.partitionBy(*TRIP_SNAPSHOT_KEY).orderBy("stop_sequence")
    last_three_stops = stop_order.rowsBetween(-2, 0)
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
