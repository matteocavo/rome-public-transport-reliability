"""Read-only SQL projections for the external operational ML handoff.

No client, credentials, execution, sampling, model or table writer lives here.
The JSON retains the historical Spark hash as input provenance, not a mandate
to reuse Spark preprocessing or its probability threshold in scikit-learn.
"""

import json
from pathlib import Path


def load_contract():
    return json.loads(Path(__file__).with_suffix(".json").read_text(encoding="utf-8"))


def _integer(value, name):
    if type(value) is not int or value < 0:
        raise ValueError(f"{name} must be an explicit nonnegative integer")
    return value


def _columns(names):
    return ", ".join(f"`{name}`" for name in names)


def training_select(*, version, start_epoch, end_epoch, label_available_before):
    """Export one chronological partition; labels must mature before its boundary.

    Split ALL feed epochs before sampling. Bounds are UTC Unix seconds and
    half-open. The caller records them in the immutable export manifest.
    Stored class labels are checked against the business event, never corrected.
    """
    for name, value in locals().copy().items():
        _integer(value, name)
    if not start_epoch < end_epoch <= label_available_before:
        raise ValueError("Require start < end <= label availability cutoff")
    c = load_contract()
    features = c["numerical_features"] + c["categorical_features"]
    t = c["target"]
    columns = list(dict.fromkeys(c["training"]["metadata_columns"] + features
                               + [t["delay_column"], t["label_column"]]))
    identities = " AND ".join(f"`{key}` IS NOT NULL" for key in c["observation_key"])
    return (
        f"SELECT {_columns(columns)}, {version} AS export_source_version, "
        f"'{c['feature_contract_hash']}' AS feature_contract_hash "
        f"FROM {c['training']['table']} VERSION AS OF {version} "
        f"WHERE feed_timestamp >= {start_epoch} AND feed_timestamp < {end_epoch} "
        f"AND label_source_feed_timestamp < {label_available_before} "
        "AND label_source_feed_timestamp > feed_timestamp "
        f"AND label_horizon_seconds BETWEEN 1 AND {t['max_label_horizon_seconds']} "
        "AND label_horizon_seconds = label_source_feed_timestamp - feed_timestamp "
        f"AND ABS(`{t['delay_column']}`) <= {t['max_abs_delay_seconds']} "
        f"AND `{t['label_column']}` = CAST(`{t['delay_column']}` >= {t['seconds']} AS INT) "
        f"AND {identities}"
    )


def inference_select(*, version, feed_timestamp):
    """Exact staging allowlist, one pinned version/epoch, no outcome columns.

    Consumers must also check count, keys, hash, preparation lineage, freshness
    and manifest truncation. An empty result never triggers an older fallback.
    """
    _integer(version, "version")
    _integer(feed_timestamp, "feed_timestamp")
    c = load_contract()
    return (
        f"SELECT {_columns(c['inference']['columns'])}, "
        f"{version} AS export_source_version, current_timestamp() AS exported_at, "
        "unix_timestamp(current_timestamp()) - feed_timestamp AS feed_age_seconds "
        f"FROM {c['inference']['table']} VERSION AS OF {version} "
        f"WHERE feed_timestamp = {feed_timestamp} "
        f"AND feature_contract_hash = '{c['feature_contract_hash']}'"
    )
