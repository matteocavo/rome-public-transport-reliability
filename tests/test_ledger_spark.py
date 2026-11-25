"""Real-Spark checks of the quarantine ledger semantics (local, in-memory; no Delta, no Databricks)."""
import ast
import hashlib
import os
import sys
import unittest
import uuid
from pathlib import Path

try:
    from pyspark.sql import SparkSession, functions as F
    HAVE_SPARK = True
except Exception:  # pragma: no cover
    HAVE_SPARK = False

PATH = Path(__file__).resolve().parents[1] / "notebooks/06_gtfs_realtime_silver.py"
FUNCS = {"reviewed_exception_keys", "validate_reviewed_service_date_registry",
         "reviewed_service_date_exception_expr", "cumulative_gate_counts", "validate_service_date_quality",
         "require_ledger_ready"}
CONSTS = {"REVIEWED_SERVICE_DATE_EXCEPTIONS", "MAX_INVALID_SERVICE_DATE_RATE", "LEDGER_PROPERTY", "LEDGER_VERSION"}
FINGERPRINT = "f1e98d9ab7629000f335e0d341c3e4b018bfbe46ba92af06aece03f97ea73867"
SNAP = 1794498556


@unittest.skipUnless(HAVE_SPARK, "pyspark unavailable")
class LedgerSparkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ["PYSPARK_PYTHON"] = sys.executable
        os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
        try:
            cls.spark = (SparkSession.builder.master("local[1]").appName("ledger")
                         .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2").getOrCreate())
        except Exception as exc:
            raise unittest.SkipTest(f"Spark unavailable: {exc}")
        tree = ast.parse(PATH.read_text(encoding="utf-8"))
        body = [n for n in tree.body if (isinstance(n, ast.FunctionDef) and n.name in FUNCS)
                or (isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id in CONSTS for t in n.targets))]
        cls.n06 = {"F": F, "hashlib": hashlib, "uuid": uuid}
        exec(compile(ast.Module(body=body, type_ignores=[]), "n06", "exec"), cls.n06)
        cls.entry = cls.n06["REVIEWED_SERVICE_DATE_EXCEPTIONS"][SNAP]

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def rows(self, spec):
        """spec: (feed_timestamp, entity, trip, vehicle, direction, route, start, raw, service_date, seq, stop)"""
        def lit(v):
            return "NULL" if v is None else (repr(v) if isinstance(v, str) else str(v))
        values = ", ".join("(" + ", ".join(lit(v) for v in r) + ")" for r in spec)
        cols = ("feed_timestamp,entity_id,trip_id,vehicle_id,direction_id,route_id,start_time,_service_date_raw,"
                "service_date,stop_sequence,stop_id")
        return self.spark.sql(f"SELECT * FROM VALUES {values} AS t({cols})")

    def reviewed_rows(self, **override):
        e = self.entry
        base = dict(ts=SNAP, entity=e["entity_id"], trip=e["trip_id"], veh=e["vehicle_id"], d=e["direction_id"],
                    route="", start="", raw="")
        base.update(override)
        return [(base["ts"], base["entity"], base["trip"], base["veh"], base["d"], base["route"], base["start"],
                 base["raw"], None, seq, stop) for seq, stop in e["stop_keys"]]

    def test_registry_fingerprint_and_32_rows(self):
        self.n06["validate_reviewed_service_date_registry"](self.n06["REVIEWED_SERVICE_DATE_EXCEPTIONS"])
        keys = self.n06["reviewed_exception_keys"](SNAP, self.entry)
        digest = hashlib.sha256("\n".join("|".join(k) for k in sorted(keys)).encode()).hexdigest()
        self.assertEqual((len(keys), digest), (32, FINGERPRINT))

    def test_reviewed_expression_identifies_exactly_the_32(self):
        expr = self.n06["reviewed_service_date_exception_expr"](self.n06["REVIEWED_SERVICE_DATE_EXCEPTIONS"])
        other = [(SNAP, "1308", "T9", "V9", "0", "", "", "", None, "1", "X1"),            # unreviewed invalid, same snapshot
                 (SNAP + 5, "1308", "T9", "V9", "0", "", "", "", None, "1", "X1"),        # unreviewed invalid
                 (SNAP, "1255", "101528146R50972", "7740", "0", "", "", "", "20260101", "2", "71620")]  # valid date
        df = self.rows(self.reviewed_rows() + other)
        got = df.select(expr.alias("r"), F.col("service_date").isNull().alias("n")).groupBy("r", "n").count().collect()
        self.assertEqual({(r["r"], r["n"]): r["count"] for r in got}, {(True, True): 32, (False, True): 2, (False, False): 1})

    def test_profile_changes_are_not_reviewed(self):
        expr = self.n06["reviewed_service_date_exception_expr"](self.n06["REVIEWED_SERVICE_DATE_EXCEPTIONS"])
        for change in ({"route": "R1"}, {"start": "10:00:00"}, {"raw": "x"}, {"veh": "9999"}, {"d": "1"},
                       {"trip": "other"}, {"ts": SNAP + 1}):
            n = self.rows(self.reviewed_rows(**change)).filter(expr).count()
            self.assertEqual(n, 0, change)

    def test_cumulative_counts_equal_full_mode_over_any_split(self):
        """ledger(ts<S) - reviewed(ts<S) + window_invalid over silver+ledger(ts<S)+window_total == full counts."""
        expr = self.n06["reviewed_service_date_exception_expr"](self.n06["REVIEWED_SERVICE_DATE_EXCEPTIONS"])
        valid = [(t, "v", "T", "V", "0", "r", "s", "20260101", "20260101", "1", f"S{t}") for t in range(1, 2001)]
        invalid = [(SNAP + 10 * i, "9", "T", "V", "0", "r", "s", "", None, "1", f"I{i}") for i in range(2)]
        df = self.rows(valid + self.reviewed_rows() + invalid)
        n = df.count()
        full_invalid = df.filter(F.col("service_date").isNull() & ~expr).count()
        self.assertEqual((n, full_invalid), (2034, 2))
        for split in (1, 1000, SNAP, SNAP + 5, SNAP + 10, SNAP + 11, SNAP + 100):
            hist, win = df.filter(F.col("feed_timestamp") < split), df.filter(F.col("feed_timestamp") >= split)
            ledger = hist.filter(F.col("service_date").isNull())
            silver_hist = hist.filter(F.col("service_date").isNotNull()).count()
            got = self.n06["cumulative_gate_counts"](
                silver_hist, ledger.count(), ledger.filter(expr).count(), win.count(),
                win.filter(F.col("service_date").isNull() & ~expr).count())
            self.assertEqual(got, {"total": n, "invalid": full_invalid}, split)

    def test_gate_unchanged_with_reviewed_excluded(self):
        gate = self.n06["validate_service_date_quality"]
        gate(10000, 10, 0, 0.001)                      # <= 0.1%
        with self.assertRaises(ValueError):
            gate(10000, 11, 0, 0.0)

    def test_marker_fail_closed(self):
        req = self.n06["require_ledger_ready"]
        req({self.n06["LEDGER_PROPERTY"]: self.n06["LEDGER_VERSION"]})
        for props in ({}, {self.n06["LEDGER_PROPERTY"]: "v0"}):
            with self.assertRaises(ValueError):
                req(props)


if __name__ == "__main__":
    unittest.main()
