# Databricks notebook source
# MAGIC %md
# MAGIC # Current operations alongside historical model evidence
# MAGIC Definitions only. Preserve historical columns and model tables; add explicit ops_ metrics.
# MAGIC Operational rates describe reported CURRENT-stop arrival delay, never a future realized label.

# COMMAND ----------

import json
import uuid
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def operational_delay_band(column):
    return (F.when(column.isNull(), "unknown").when(column < -60, "early")
        .when(column < 180, "on_time").when(column < 300, "moderate_delay").otherwise("major_delay"))


def validate_operational_lineage(lineage, enrichment_version, contract_hash):
    if (not lineage["rows"] or lineage["epochs"] != 1 or lineage["batches"] != 1
        or lineage["version_min"] != enrichment_version or lineage["version_max"] != enrichment_version
        or lineage["hash_min"] != contract_hash or lineage["hash_max"] != contract_hash
        or lineage["prepared_at"] is None or lineage["feed"] is None):
        raise ValueError("Validated scoring lineage required before Gold refresh")


def reconcile_operational_network(network, feed_epoch):
    count = network["ops_observation_count"]
    known = network["ops_known_delay_count"]
    missing = network["ops_missing_delay_count"]
    categories = [network["ops_" + c + "_count"] for c in
                  ["major_delay", "early", "on_time", "moderate_delay"]]
    if (count <= 0 or min([known, missing] + categories) < 0 or known + missing != count
        or sum(categories) != known or network["ops_latest_feed_timestamp"] != feed_epoch):
        raise ValueError("Operational epochs/counts/known-delay denominators do not reconcile")


def refresh_operational_gold(spark, *, enrichment_version, scoring_version, allow_write=False):
    """Pinned full-history aggregation, additive columns, manifest published last."""
    if type(enrichment_version) is not int or enrichment_version < 0:
        raise ValueError("Explicit validated enrichment version required")
    if type(scoring_version) is not int or scoring_version < 0:
        raise ValueError("Explicit validated scoring version required")
    spark.conf.set("spark.sql.session.timeZone", "Europe/Rome")
    release_id = str(uuid.uuid4())
    source_table = "rome_transport.silver.realtime_enriched_observations"
    staging_table = "rome_transport.features.next_stop_delay_scoring"
    contract_hash = "11682b3cc1ab5c1d8440af4f6294e932e815126c3dd86ab609f4d5f8f35c41a5"
    source = spark.read.option("versionAsOf", enrichment_version).table(source_table)
    staging = spark.read.option("versionAsOf", scoring_version).table(staging_table)
    lineage = staging.agg(F.count("*").alias("rows"), F.countDistinct("feed_timestamp").alias("epochs"),
        F.countDistinct("feature_preparation_batch_id").alias("batches"),
        F.min("source_silver_version").alias("version_min"), F.max("source_silver_version").alias("version_max"),
        F.min("feature_contract_hash").alias("hash_min"), F.max("feature_contract_hash").alias("hash_max"),
        F.max("feed_timestamp").alias("feed"), F.max("feature_prepared_at").alias("prepared_at")).first().asDict()
    validate_operational_lineage(lineage, enrichment_version, contract_hash)
    if staging.filter(F.col("feature_preparation_batch_id").isNull()
        | F.col("source_silver_version").isNull() | F.col("feature_contract_hash").isNull()).limit(1).count():
        raise ValueError("Missing preparation lineage")

    table_names = ["executive_overview", "route_reliability", "stop_reliability", "temporal_reliability",
                   "delay_propagation", "predictive_operations", "model_performance", "analytics_refresh_metadata"]
    before_versions, previous = {}, {}
    for name in table_names:
        table = "rome_transport.gold." + name
        version = int(spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()["version"])
        before_versions[name] = version
        pinned = spark.read.option("versionAsOf", version).table(table)
        # Old current Gold versions can become unreadable under retention after overwrite.
        # Materialize only the bounded, curated Gold snapshots before any write, not source observations.
        schema = pinned.schema
        old_rows = pinned.limit(50001).collect()
        if len(old_rows) > 50000:
            raise ValueError("Historical Gold snapshot exceeds bounded preservation limit")
        previous[name] = spark.createDataFrame(old_rows, schema)
    metadata_before = previous["analytics_refresh_metadata"]
    if metadata_before.count() != 1:
        raise ValueError("One existing historical release metadata row required")
    for name in ["predictive_operations", "model_performance"]:
        if not previous[name].count():
            raise ValueError("Historical model evidence must remain available")

    partition = ["feed_timestamp", "trip_id", "service_date", "entity_id", "start_time", "vehicle_id"]
    observations = source.select(*list(dict.fromkeys(partition + ["stop_id", "stop_sequence", "feed_datetime",
        "route_id", "route_short_name", "stop_name", "arrival_delay_seconds"])))
    invalid = F.col("feed_timestamp").isNull() | F.col("service_date").isNull()
    for key in ["entity_id", "trip_id", "stop_sequence", "stop_id", "route_id"]:
        invalid = invalid | F.col(key).isNull()
    if observations.filter(invalid | ~F.col("feed_datetime").eqNullSafe(
        F.timestamp_seconds("feed_timestamp"))).limit(1).count():
        raise ValueError("Invalid operational observation identity/time")
    observations = observations.withColumn("delay", F.col("arrival_delay_seconds").cast("double"))
    if observations.filter(F.isnan("delay") | (F.abs("delay") == float("inf"))).limit(1).count():
        raise ValueError("Nonfinite operational delay")
    observations = observations.withColumn("previous_delay", F.lag("delay").over(
        Window.partitionBy(*partition).orderBy("stop_sequence")))

    observations = (observations.withColumn("day_of_week", ((F.dayofweek("feed_datetime") + 5) % 7 + 1).cast("int"))
        .withColumn("feed_hour", F.hour("feed_datetime"))
        .withColumn("current_delay_band", operational_delay_band(F.col("delay")))
        .withColumn("previous_delay_band", operational_delay_band(F.col("previous_delay"))))
    view = "ops13_" + uuid.uuid4().hex
    observations.createOrReplaceTempView(view)
    try:
        aggregate = spark.sql(f"""
        SELECT CASE WHEN grouping(route_id)=0 THEN 'route'
                    WHEN grouping(stop_id)=0 THEN 'stop'
                    WHEN grouping(service_date)=0 THEN 'temporal'
                    WHEN grouping(current_delay_band)=0 THEN 'propagation' ELSE 'executive' END AS kind,
            route_id, stop_id, service_date, day_of_week, feed_hour, current_delay_band, previous_delay_band,
            min(route_short_name) AS ops_route_short_name, min(stop_name) AS ops_stop_name,
            count(*) AS ops_observation_count, count(delay) AS ops_known_delay_count,
            count(*)-count(delay) AS ops_missing_delay_count,
            count(DISTINCT feed_timestamp) AS ops_distinct_snapshots,
            count(DISTINCT service_date) AS ops_distinct_service_dates,
            count(DISTINCT route_id) AS ops_distinct_routes,
            count(DISTINCT trip_id) AS ops_distinct_trips,
            count(DISTINCT vehicle_id) AS ops_distinct_vehicles,
            count(DISTINCT stop_id) AS ops_distinct_stops,
            avg(delay) AS ops_mean_delay_seconds,
            min(delay) AS ops_min_delay_seconds, max(delay) AS ops_max_delay_seconds,
            sum(CASE WHEN abs(delay)>43200 THEN 1 ELSE 0 END) AS ops_extreme_delay_count,
            percentile_approx(delay,array(0.5D,0.90D,0.95D,0.99D),10000) AS percentiles,
            sum(CASE WHEN delay>=300 THEN 1 ELSE 0 END) AS ops_major_delay_count,
            sum(CASE WHEN delay< -60 THEN 1 ELSE 0 END) AS ops_early_count,
            sum(CASE WHEN delay>= -60 AND delay<180 THEN 1 ELSE 0 END) AS ops_on_time_count,
            sum(CASE WHEN delay>=180 AND delay<300 THEN 1 ELSE 0 END) AS ops_moderate_delay_count,
            count(previous_delay) AS ops_previous_delay_count,
            sum(CASE WHEN delay IS NOT NULL AND previous_delay IS NOT NULL THEN 1 ELSE 0 END) AS ops_paired_delay_count,
            avg(previous_delay) AS ops_mean_previous_delay_seconds,
            avg(delay-previous_delay) AS ops_mean_delay_change_from_previous_stop,
            max(feed_timestamp) AS ops_latest_feed_timestamp, min(feed_timestamp) AS ops_first_feed_timestamp
        FROM {view}
        GROUP BY GROUPING SETS ((),(route_id),(stop_id),(service_date,day_of_week,feed_hour),
                                (current_delay_band,previous_delay_band))
        """)
        schema = aggregate.schema
        rows = aggregate.limit(50001).collect()
        if len(rows) > 50000:
            raise ValueError("Compact aggregate cap exceeded; no Gold writes")
        aggregate = spark.createDataFrame(rows, schema)
    finally:
        spark.catalog.dropTempView(view)
    for index, label in enumerate(["median", "p90", "p95", "p99"]):
        aggregate = aggregate.withColumn("ops_" + label + "_delay_seconds", F.col("percentiles")[index])
    aggregate = aggregate.drop("percentiles")
    for category in ["major_delay", "early", "on_time", "moderate_delay"]:
        aggregate = aggregate.withColumn("ops_" + category + "_rate", F.when(F.col("ops_known_delay_count") > 0,
            F.col("ops_" + category + "_count") / F.col("ops_known_delay_count")))
    network = aggregate.filter(F.col("kind") == "executive").first().asDict()
    reconcile_operational_network(network, lineage["feed"])
    aggregate = aggregate.withColumn("ops_transition_share", F.when(
        F.lit(network["ops_paired_delay_count"]) > 0,
        F.col("ops_paired_delay_count") / F.lit(network["ops_paired_delay_count"])))

    group_specs = {"executive_overview": ("executive", []), "route_reliability": ("route", ["route_id"]),
        "stop_reliability": ("stop", ["stop_id"]),
        "temporal_reliability": ("temporal", ["service_date", "day_of_week", "feed_hour"]),
        "delay_propagation": ("propagation", ["current_delay_band", "previous_delay_band"])}
    prepared, expected_counts = {}, {}
    for name, (kind, keys) in group_specs.items():
        current = aggregate.filter(F.col("kind") == kind).select(*keys,
            *[c for c in aggregate.columns if c.startswith("ops_")])
        if name in ["route_reliability", "stop_reliability"]:
            minimum = 1000 if name == "route_reliability" else 500
            current = current.withColumn("ops_ranking_eligible", F.col("ops_known_delay_count") >= minimum)
            eligible = current.filter("ops_ranking_eligible").select(*keys,
                "ops_major_delay_rate", "ops_mean_delay_seconds", "ops_p95_delay_seconds")
            ranks = [("ops_major_delay_rate", "ops_major_delay_rate_rank_desc"),
                     ("ops_mean_delay_seconds", "ops_mean_delay_rank_desc"),
                     ("ops_p95_delay_seconds", "ops_p95_delay_rank_desc")]
            for metric, rank in ranks:
                eligible = eligible.withColumn(rank, F.dense_rank().over(Window.orderBy(F.col(metric).desc())))
            current = current.join(eligible.select(*keys, *[r for _, r in ranks]), keys, "left")
        historical = previous[name].select(*[c for c in previous[name].columns if not c.startswith("ops_")])
        if keys:
            if current.groupBy(*keys).count().filter(F.col("count") > 1).limit(1).count():
                raise ValueError("Duplicate operational aggregate keys")
            combined = historical.join(current, keys, "full")
        else:
            if historical.count() != 1:
                raise ValueError("One historical overview row required")
            combined = historical.crossJoin(current)
        combined = (combined.withColumn("ops_source_version", F.lit(enrichment_version).cast("long"))
            .withColumn("ops_release_id", F.lit(release_id))
            .withColumn("ops_metric_definition", F.lit("reported_current_stop_arrival_delay_seconds")))
        # Existing values, including future-outcome metrics, remain byte-for-value reference evidence.
        existing_columns = historical.columns
        if historical.exceptAll(combined.select(*existing_columns).filter(
                F.col(existing_columns[0]).isNotNull())).limit(1).count():
            raise ValueError("Historical Gold values would be lost")
        prepared[name] = combined
        expected_counts[name] = combined.count()
    if not allow_write:
        raise RuntimeError("Operational Gold validated; write disabled")

    persisted_versions = dict(before_versions)
    for name, frame in prepared.items():
        table = "rome_transport.gold." + name
        frame.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
        version = int(spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()["version"])
        saved = spark.read.option("versionAsOf", version).table(table)
        if saved.count() != expected_counts[name] or frame.exceptAll(saved).limit(1).count() or saved.exceptAll(frame).limit(1).count():
            raise ValueError("Operational Gold read-back mismatch; manifest withheld")
        persisted_versions[name] = version
    for name in ["predictive_operations", "model_performance"]:
        table = "rome_transport.gold." + name
        version = int(spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()["version"])
        current = spark.read.option("versionAsOf", version).table(table)
        if previous[name].exceptAll(current).limit(1).count() or current.exceptAll(previous[name]).limit(1).count():
            raise ValueError("Historical model evidence values changed during refresh")
        persisted_versions[name] = version
        expected_counts[name] = previous[name].count()
    completed_at = datetime.now(timezone.utc)
    manifest = {"rome_transport.gold." + k: v for k, v in persisted_versions.items() if k != "analytics_refresh_metadata"}
    metadata = (metadata_before.withColumn("historical_refresh_timestamp",
            F.coalesce(F.col("historical_refresh_timestamp"), F.col("refresh_timestamp"))
            if "historical_refresh_timestamp" in metadata_before.columns else F.col("refresh_timestamp"))
        .withColumn("refresh_id", F.lit(release_id))
        .withColumn("refresh_timestamp", F.lit(completed_at))
        .withColumn("gold_versions_json", F.lit(json.dumps(manifest, sort_keys=True)))
        .withColumn("quality_status", F.lit("PASSED")).withColumn("dashboard_ready", F.lit("YES"))
        .withColumn("ops_source_table", F.lit(source_table))
        .withColumn("ops_source_version", F.lit(enrichment_version).cast("long"))
        .withColumn("ops_scoring_version", F.lit(scoring_version).cast("long"))
        .withColumn("ops_latest_feed_timestamp", F.lit(lineage["feed"]).cast("long"))
        .withColumn("ops_scoring_prepared_at", F.lit(lineage["prepared_at"]))
        .withColumn("ops_observation_count", F.lit(network["ops_observation_count"]).cast("long"))
        .withColumn("ops_status", F.lit("PINNED_INPUT_ALIGNED"))
        .withColumn("model_evidence_status", F.lit("HISTORICAL_MODEL_EVIDENCE")))
    table = "rome_transport.gold.analytics_refresh_metadata"
    metadata.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
    saved = spark.table(table)
    if saved.count() != 1 or metadata.exceptAll(saved).limit(1).count() or saved.exceptAll(metadata).limit(1).count():
        raise ValueError("Operational release metadata read-back mismatch")
    persisted_versions["analytics_refresh_metadata"] = int(spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()["version"])
    expected_counts["analytics_refresh_metadata"] = 1
    return {"status": "PASS", "source_version": enrichment_version, "scoring_version": scoring_version,
        "latest_feed_timestamp": lineage["feed"], "refresh_timestamp": completed_at.isoformat(),
        "gold_versions": persisted_versions, "row_counts": expected_counts, "network": network,
        "historical_model_evidence_unchanged": True, "model_scoring_executed": False}
