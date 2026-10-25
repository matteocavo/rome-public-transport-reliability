"""Offline tests of production notebook functions; no Spark, feeds or Databricks calls."""
import ast
from datetime import datetime, timezone
from functools import reduce
from http.client import IncompleteRead
from pathlib import Path
from types import SimpleNamespace
from urllib.error import HTTPError, URLError
from unittest import TestCase
import json
import unittest
import uuid

ROOT = Path(__file__).resolve().parents[1]


class DecodeError(Exception):
    pass


class Proto(SimpleNamespace):
    def HasField(self, name):
        return name in vars(self)


class Feed(Proto):
    def IsInitialized(self):
        return getattr(self, "initialized", True)


def feed(entities=(), timestamp=123, incrementality=0):
    return Feed(header=Proto(timestamp=timestamp, incrementality=incrementality,
                             gtfs_realtime_version="2.0"), entity=list(entities))


class Expr:
    def __init__(self, fn): self.fn = fn
    def __eq__(self, other): return Expr(lambda row: self.fn(row) == other)
    def __ne__(self, other): return Expr(lambda row: self.fn(row) != other)
    def __or__(self, other): return Expr(lambda row: self.fn(row) or other.fn(row))
    def isNotNull(self): return Expr(lambda row: self.fn(row) is not None)
    def isNull(self): return Expr(lambda row: self.fn(row) is None)
    def eqNullSafe(self, other): return Expr(lambda row: self.fn(row) == other.fn(row))
    def __and__(self, other): return Expr(lambda row: self.fn(row) and other.fn(row))
    def __gt__(self, value): return Expr(lambda row: self.fn(row) > value)


class Frame:
    def __init__(self, rows, spark): self.rows, self.spark, self.prefix = rows, spark, None
    def count(self): return len(self.rows)
    def dropDuplicates(self):
        seen, rows = set(), []
        for row in self.rows:
            key = json.dumps(row, sort_keys=True, default=str)
            if key not in seen: rows.append(row); seen.add(key)
        return Frame(rows, self.spark)
    def distinct(self): return self.dropDuplicates()
    def select(self, *keys): return Frame([{k: r[k] for k in keys} for r in self.rows], self.spark)
    def alias(self, name): self.prefix = name; return self
    def join(self, other, predicate, how):
        assert how == "left_anti"
        def matched(row, prior):
            merged = {self.prefix + "." + k: v for k, v in row.items()}
            merged.update({other.prefix + "." + k: v for k, v in prior.items()})
            return predicate.fn(merged)
        return Frame([r for r in self.rows if not any(matched(r, p) for p in other.rows)], self.spark)
    def groupBy(self, *keys):
        def count():
            counts = {}
            for row in self.rows:
                key = tuple(row[k] for k in keys); counts[key] = counts.get(key, 0) + 1
            return Frame([dict(zip(keys, k), count=v) for k, v in counts.items()], self.spark)
        return SimpleNamespace(count=count)
    def filter(self, expr): return Frame([r for r in self.rows if expr.fn(r)], self.spark)
    def limit(self, n): return Frame(self.rows[:n], self.spark)
    @property
    def write(self): return self
    def format(self, name): assert name == "delta"; return self
    def mode(self, name): assert name == "append"; return self
    def saveAsTable(self, name): self.spark.tables.setdefault(name, []).extend(self.rows)


class Spark:
    def __init__(self):
        self.tables = {}
        self.catalog = SimpleNamespace(tableExists=lambda name: name in self.tables)
    def table(self, name): return Frame(self.tables[name], self)
    def createDataFrame(self, rows, schema): return Frame(rows, self)


def load_contract():
    tree = ast.parse((ROOT / "notebooks/05_gtfs_realtime_pipeline.py").read_text(encoding="utf-8"))
    enum = SimpleNamespace(Name=lambda value: "ENUM_" + str(value))
    pb = SimpleNamespace(TripUpdate=SimpleNamespace(StopTimeUpdate=SimpleNamespace(ScheduleRelationship=enum)),
                         VehiclePosition=SimpleNamespace(VehicleStopStatus=enum),
                         Alert=SimpleNamespace(Cause=enum, Effect=enum))
    scope = dict(datetime=datetime, timezone=timezone, reduce=reduce, uuid=uuid,
                 HTTPError=HTTPError, URLError=URLError, IncompleteRead=IncompleteRead,
                 DecodeError=DecodeError, gtfs_realtime_pb2=pb, spark=Spark(),
                 F=SimpleNamespace(col=lambda name: Expr(lambda row: row[name])))
    scope.update({k: k for k in ["TRIP_UPDATES_URL", "VEHICLE_POSITIONS_URL", "SERVICE_ALERTS_URL",
                               "trip_updates_schema", "vehicle_positions_schema", "service_alerts_schema"]})
    nodes = [n for n in tree.body if isinstance(n, (ast.FunctionDef, ast.ClassDef))]
    exec(compile(ast.Module(body=nodes, type_ignores=[]), "ingestion_functions", "exec"), scope)
    return scope


class IngestionContractTest(TestCase):
    def setUp(self): self.s = load_contract()

    def test_missing_audit_migration_blocks_before_network(self):
        self.s["AUDIT_TABLE"] = "rome_transport.bronze.ingestion_runs"
        self.s["fetch_feed"] = lambda url: self.fail("must not fetch before migration")
        with self.assertRaisesRegex(RuntimeError, "migration"):
            self.s["run_realtime_ingestion"]()

    def test_valid_trip_and_vehicle_values(self):
        trip = Proto(trip_id="t", route_id="r", start_date="20261025", start_time="08:00:00", direction_id=0)
        stop = Proto(stop_sequence=2, stop_id="s", schedule_relationship=0,
                     arrival=Proto(delay=12, time=456, uncertainty=1))
        tu = Proto(trip=trip, vehicle=Proto(id="v"), stop_time_update=[stop])
        rows = self.s["build_trip_updates_df"](feed([Proto(id="e", trip_update=tu)])).rows
        self.assertEqual(len(rows), 1)
        self.assertEqual({k: rows[0][k] for k in ["feed_timestamp", "entity_id", "trip_id", "route_id",
                                                "vehicle_id", "stop_sequence", "stop_id", "arrival_delay",
                                                "arrival_time", "departure_delay"]},
                         dict(feed_timestamp=123, entity_id="e", trip_id="t", route_id="r", vehicle_id="v",
                              stop_sequence=2, stop_id="s", arrival_delay=12, arrival_time=456, departure_delay=None))
        vehicle = Proto(trip=trip, vehicle=Proto(id="v"), current_status=0, current_stop_sequence=2,
                        stop_id="s", timestamp=120, position=Proto(latitude=41.9, longitude=12.5, speed=3.0))
        rows = self.s["build_vehicle_positions_df"](feed([Proto(id="e", vehicle=vehicle)])).rows
        self.assertEqual({k: rows[0][k] for k in ["feed_timestamp", "trip_id", "vehicle_id", "latitude",
                                                "longitude", "speed", "bearing", "vehicle_timestamp"]},
                         dict(feed_timestamp=123, trip_id="t", vehicle_id="v", latitude=41.9,
                              longitude=12.5, speed=3.0, bearing=None, vehicle_timestamp=120))

    def test_empty_header_timestamp_survives_without_rows(self):
        result = self.s["feed_metadata"](feed())
        self.assertEqual(result["feed_timestamp"], 123)
        self.assertTrue(result["is_empty"])
        self.assertEqual(result["entities_received"], 0)
        for source in ["trip_updates", "vehicle_positions", "service_alerts"]:
            self.s["validate_entities"](feed(), source)
            self.assertEqual(self.s["build_" + source + "_df"](feed()).count(), 0)

    def test_invalid_metadata_and_differential_are_not_full_snapshots(self):
        for value in [feed(timestamp=0), feed(incrementality=1), Feed(entity=[])]:
            with self.subTest(value=value), self.assertRaises(self.s["IngestionError"]):
                self.s["feed_metadata"](value)

    def test_omitted_incrementality_uses_proto_default(self):
        class DefaultHeader(Proto):
            incrementality = 0
        value = Feed(header=DefaultHeader(timestamp=123, gtfs_realtime_version="2.0"), entity=[])
        self.assertEqual(self.s["feed_metadata"](value)["feed_incrementality"], "FULL_DATASET")

    def test_valid_alert_payload_unchanged(self):
        alert = Proto(active_period=[], informed_entity=[], header_text=Proto(translation=[]),
                      description_text=Proto(translation=[]))
        value = feed([Proto(id="a", alert=alert, is_deleted=False)])
        self.s["validate_entities"](value, "service_alerts")
        rows = self.s["build_service_alerts_df"](value).rows
        self.assertEqual({k: v for k, v in rows[0].items() if k != "_ingestion_timestamp"},
                         dict(feed_timestamp=123, alert_id="a", cause=None, effect=None,
                              active_periods=[], informed_entities=[], header_translations=[],
                              description_translations=[], _source_url="SERVICE_ALERTS_URL"))
        self.assertFalse(self.s["feed_metadata"](value)["is_empty"])

    def test_wrong_or_deleted_entity_rejected(self):
        for entity in [Proto(id="x", is_deleted=True), Proto(id="x", is_deleted=False)]:
            with self.assertRaises(self.s["IngestionError"]):
                self.s["validate_entities"](feed([entity]), "service_alerts")

    def test_decode_failure_audited_and_remaining_sources_attempted(self):
        records, attempted = [], []
        def process(source):
            attempted.append(source)
            if source == "bad": raise DecodeError("malformed protobuf")
            return self.s["feed_metadata"](feed())
        with self.assertRaisesRegex(RuntimeError, "PARTIAL_SUCCESS"):
            self.s["run_ingestion_attempt"](["bad", "empty", "ok"], process, records.append, "batch")
        self.assertEqual(attempted, ["bad", "empty", "ok"])
        self.assertEqual(records[1]["error_type"], "DECODE_ERROR")
        self.assertEqual(records[-1]["status"], "PARTIAL_SUCCESS")
        self.assertTrue(records[2]["is_empty"])

    def test_all_success_and_all_failed(self):
        for failure, expected in [(False, "SUCCESS"), (True, "FAILED")]:
            records = []
            def process(source):
                if failure: raise URLError("offline")
                return self.s["feed_metadata"](feed())
            if failure:
                with self.assertRaisesRegex(RuntimeError, expected):
                    self.s["run_ingestion_attempt"](["a", "b"], process, records.append)
            else:
                self.s["run_ingestion_attempt"](["a", "b"], process, records.append)
            self.assertEqual(records[-1]["status"], expected)
            self.assertEqual(len([r for r in records if r["record_type"] == "SOURCE"]), 2)

    def test_audit_failure_is_not_suppressed(self):
        records, attempted = [], []
        def process(source): attempted.append(source); return {}
        def audit(record):
            if record["record_type"] == "SOURCE" and record["source_name"] == "a":
                raise OSError("audit storage unavailable")
            records.append(record)
        with self.assertRaisesRegex(RuntimeError, "FAILED"):
            self.s["run_ingestion_attempt"](["a", "b"], process, audit)
        self.assertEqual(attempted, ["a", "b"])
        self.assertEqual(records[-1]["error_type"], "WRITE_ERROR")

    def test_start_audit_failure_prevents_source_writes(self):
        def audit(record): raise OSError("offline")
        with self.assertRaises(OSError):
            self.s["run_ingestion_attempt"](["a"], lambda source: self.fail("must not run"), audit)

    def test_retry_preserves_logical_batch_and_changes_attempt(self):
        records = []
        for _ in range(2):
            self.s["run_ingestion_attempt"](["a"], lambda source: {}, records.append, "stable-batch")
        self.assertEqual({r["ingestion_batch_id"] for r in records}, {"stable-batch"})
        self.assertEqual(len({r["ingestion_attempt_id"] for r in records}), 2)

    def test_actual_null_safe_anti_join_and_duplicate_conflict(self):
        spark = self.s["spark"]; append = self.s["append_new_rows"]
        keys = ["feed_timestamp", "entity_id", "stop_id"]
        row = dict(feed_timestamp=123, entity_id="e", stop_id=None, delay=12)
        self.assertEqual(append(Frame([row, dict(row)], spark), "bronze", keys), 1)
        self.assertEqual(append(Frame([row], spark), "bronze", keys), 0)
        self.assertEqual(len(spark.tables["bronze"]), 1)
        with self.assertRaises(self.s["IngestionError"]):
            append(Frame([row, dict(row, delay=13)], spark), "bronze", keys)

    def test_error_categories(self):
        for exc, expected in [(HTTPError("url", 500, "bad", {}, None), "HTTP_ERROR"),
                              (IncompleteRead(b"x"), "INCOMPLETE_READ"),
                              (TimeoutError(), "NETWORK_ERROR"), (ValueError(), "UNKNOWN_ERROR")]:
            self.assertEqual(self.s["error_type"](exc), expected)

    def test_empty_full_alert_audit_is_alignment_input(self):
        tree = ast.parse((ROOT / "notebooks/07_gtfs_realtime_enrichment.py").read_text(encoding="utf-8"))
        nodes = [n for n in tree.body if isinstance(n, ast.FunctionDef)
                 and n.name in {"assert_no_rows", "successful_empty_full_alert_snapshot_times"}]
        exec(compile(ast.Module(body=nodes, type_ignores=[]), "alert_evidence", "exec"), self.s)
        def row(timestamp, **changes):
            return dict(dict(record_type="SOURCE", source_name="service_alerts", status="SUCCESS",
                             feed_incrementality="FULL_DATASET", feed_timestamp=timestamp,
                             is_empty=False, parsed_row_count=1), **changes)
        records = [row(10), row(20, is_empty=True, parsed_row_count=0),
                   row(30, status="FAILED"), row(40, feed_incrementality="DIFFERENTIAL")]
        result = self.s["successful_empty_full_alert_snapshot_times"](Frame(records, self.s["spark"]))
        self.assertEqual(result.rows, [{"feed_timestamp": 20}])
        with self.assertRaises(ValueError):
            self.s["successful_empty_full_alert_snapshot_times"](
                Frame([row(50, is_empty=True)], self.s["spark"]))

    def test_fetch_decode_failure_with_mocked_payload(self):
        class Message:
            def ParseFromString(self, payload):
                assert payload == b"malformed"
                raise DecodeError("bad wire format")
        class Response:
            def __enter__(self): return self
            def __exit__(self, *args): pass
            def read(self): return b"malformed"
        self.s["gtfs_realtime_pb2"].FeedMessage = Message
        self.s["urllib"] = SimpleNamespace(request=SimpleNamespace(urlopen=lambda url, timeout: Response()))
        with self.assertRaises(DecodeError): self.s["fetch_feed"]("mock://feed")

    def test_source_empty_success_and_write_failure_keep_metadata(self):
        self.s["fetch_feed"] = lambda url: feed()
        result = self.s["ingest_source"]("service_alerts", "mock://feed",
                    self.s["build_service_alerts_df"], ["feed_timestamp", "alert_id"])
        self.assertEqual(result["feed_timestamp"], 123)
        self.assertEqual(result["parsed_row_count"], 0)
        self.assertEqual(result["rows_appended"], 0)
        self.assertTrue(result["is_empty"])
        def broken_write(*args): raise OSError("write failed")
        self.s["append_new_rows"] = broken_write
        with self.assertRaises(self.s["IngestionError"]) as caught:
            self.s["ingest_source"]("service_alerts", "mock://feed",
                    self.s["build_service_alerts_df"], ["feed_timestamp", "alert_id"])
        self.assertEqual(caught.exception.error_type, "WRITE_ERROR")
        self.assertEqual(caught.exception.metadata["feed_timestamp"], 123)


if __name__ == "__main__": unittest.main()
