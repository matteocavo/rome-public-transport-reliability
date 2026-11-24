"""Local tests for incremental notebooks 06/07 (real PySpark local[1]; no Databricks, no Delta).

Production definitions are extracted from the notebooks with ast. The causal-join cell of 06 is executed
verbatim on synthetic frames. replaceWhere/Delta retention/RESTORE are NOT exercised here (no local Delta).
"""
import ast
import os
import shutil
import sys
import subprocess
import unittest
from pathlib import Path

try:
    from pyspark.sql import SparkSession, functions as F
    HAVE_SPARK = True
except Exception:  # pragma: no cover
    HAVE_SPARK = False

ROOT = Path(__file__).resolve().parents[1]
N06 = (ROOT / "notebooks/06_gtfs_realtime_silver.py").read_text(encoding="utf-8")
N07 = (ROOT / "notebooks/07_gtfs_realtime_enrichment.py").read_text(encoding="utf-8")
SHARED = {"RUN_MODES", "VP_MAX_STALENESS_SECONDS", "MAX_INVALID_SERVICE_DATE_RATE",
          "INCREMENTAL_WRITE_BLOCKERS", "EXPECTED_SHADOW_DIFFERENCES"}
FUNCS = {"resolve_run_mode", "parse_positive_int", "plan_incremental_window", "plan_enrichment_window",
         "assert_epoch_column", "scope_from", "vehicle_epoch_expr", "count_late_arrivals",
         "count_vp_lookback_escapes", "lost_window_keys", "overlap_difference", "replace_window",
         "deduplicate_latest", "validate_service_date_quality", "classify_shadow",
         "assert_incremental_write_allowed", "check_replace_window"}
SHARED_EXTRA = {"INCREMENTAL_WRITE_BLOCKERS", "EXPECTED_SHADOW_DIFFERENCES"}
KEY = ["feed_timestamp", "entity_id", "trip_id", "stop_sequence", "stop_id"]


def extract(source, scope):
    body = [n for n in ast.parse(source).body
            if (isinstance(n, ast.FunctionDef) and n.name in FUNCS)
            or (isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id in SHARED for t in n.targets))]
    exec(compile(ast.Module(body=body, type_ignores=[]), "nb", "exec"), scope)
    return scope


def cell(source, marker):
    return next(c for c in source.split("# COMMAND ----------") if marker in c and "# MAGIC %md" not in c)


class FakeTable:
    """In-memory stand-in for a Delta table used only to exercise replace_window's conservation logic."""
    def __init__(self, frame): self.frame = frame


@unittest.skipUnless(HAVE_SPARK, "pyspark/java unavailable")
class IncrementalTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ["PYSPARK_PYTHON"] = sys.executable
        os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
        try:
            cls.spark = (SparkSession.builder.master("local[1]").appName("inc")
                         .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2").getOrCreate())
        except Exception as exc:  # no JVM available
            raise unittest.SkipTest(f"Spark unavailable: {exc}")
        cls.n06 = extract(N06, {"F": F})
        cls.n07 = extract(N07, {"F": F})

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    # ---- synthetic data -------------------------------------------------------------------------
    def frame(self, rows, schema):
        """Build a DataFrame through SQL VALUES (no Python workers: createDataFrame is unreliable on Windows)."""
        cols = [c.split() for c in schema.split(",")]
        sql_type = {"long": "bigint", "int": "int", "string": "string"}
        def lit(v, t):
            return "NULL" if v is None else (repr(v) if t == "string" else str(v))
        select = ", ".join(f"CAST({lit(None, 'x')} AS {sql_type[t]}) AS {n}" for n, t in cols)
        if not rows:
            return self.spark.sql(f"SELECT {select} WHERE 1 = 0")
        values = ", ".join("(" + ", ".join(lit(v, t) for v, (_, t) in zip(r, cols)) + ")" for r in rows)
        names = ", ".join(n for n, _ in cols)
        casts = ", ".join(f"CAST({n} AS {sql_type[t]}) AS {n}" for n, t in cols)
        return self.spark.sql(f"SELECT {casts} FROM VALUES {values} AS t({names})")

    def tu(self, rows):
        return self.frame(rows, "feed_timestamp long, entity_id string, trip_id string, "
            "vehicle_id string, stop_sequence int, stop_id string, _ingestion_timestamp long, delay int")

    def vp(self, rows):
        df = self.frame(rows, "feed_timestamp long, entity_id string, trip_id string, "
            "vehicle_id string, vehicle_timestamp long, _ingestion_timestamp long")
        return df.withColumn("vehicle_datetime", F.col("vehicle_timestamp").cast("timestamp"))

    def match(self, tu, vp):
        scope = {"F": F, "trip_updates_silver": tu, "vehicle_positions_silver": vp}
        code = cell(N06, "matching_window =")
        exec(code.replace(".show(truncate=False)", ".count()"), scope)
        return scope["matched"]

    def canon(self, df, cols):
        return sorted(tuple(r) for r in df.select(*cols).collect())

    def dataset(self):
        tu = [(t, f"e{t}", "T1", "V1", 1, "S1", t + 1, 0) for t in range(1000, 1400, 20)]
        tu += [(1200, "e1200", "T1", "V1", 1, "S1", 9999, 5)]            # duplicate, later ingestion wins
        vp = [(t, f"p{t}", "T1", "V1", t - 10, t + 1) for t in range(900, 1400, 15)]
        return tu, vp

    # ---- 1 / 6 / 7: equivalence, causality, 180/181 boundary ---------------------------------------
    def test_full_vs_incremental_join_equivalence_and_causality(self):
        tu_rows, vp_rows = self.dataset()
        dedup = self.n06["deduplicate_latest"]
        tu_full = dedup(self.tu(tu_rows), ["feed_timestamp", "entity_id", "trip_id", "stop_sequence", "stop_id"])
        full = self.match(tu_full, self.vp(vp_rows))
        start = 1200
        tu_inc = self.n06["scope_from"](self.tu(tu_rows), start)
        vp_inc = self.n06["scope_from"](self.vp(vp_rows), start - 180)
        inc = self.match(dedup(tu_inc, KEY), vp_inc)
        cols = ["tu.feed_timestamp", "tu.entity_id", "tu.delay", "vp.entity_id", "position_time_diff_seconds"]
        expected = [r for r in self.canon(full, cols) if r[0] >= start]
        self.assertEqual(self.canon(inc, cols), expected)
        self.assertEqual(inc.count(), len(expected))
        # row conservation: one output per deduplicated trip update; latest ingestion won the duplicate
        self.assertEqual(full.count(), tu_full.count())
        self.assertEqual([r[2] for r in self.canon(full, cols) if r[0] == 1200], [5])
        # causality: no matched position is newer than the trip update
        self.assertTrue(all(r[4] is None or r[4] >= 0 for r in self.canon(full, cols)))

    def test_boundary_180_and_181_seconds(self):
        tu = self.tu([(1000, "e", "T", "V", 1, "S", 1, 0)])
        for gap, matched in ((180, True), (181, False), (0, True)):
            vp = self.vp([(1000, "p", "T", "V", 1000 - gap, 1)])
            row = self.match(tu, vp).select("vp.entity_id").first()[0]
            self.assertEqual(row is not None, matched, gap)
        future = self.vp([(1000, "p", "T", "V", 1001, 1)])      # position newer than the update: never used
        self.assertIsNone(self.match(tu, future).select("vp.entity_id").first()[0])

    def test_scope_does_not_lose_boundary_position(self):
        # A position 180 s before the window start is still inside the VP scope (start - 180).
        tu = self.tu([(1200, "e", "T", "V", 1, "S", 1, 0)])
        vps = [(1020, "p", "T", "V", 1020, 1)]
        full = self.match(tu, self.vp(vps)).select("vp.entity_id").first()[0]
        scoped = self.match(self.n06["scope_from"](tu, 1200),
                            self.n06["scope_from"](self.vp(vps), 1200 - 180)).select("vp.entity_id").first()[0]
        self.assertEqual((full, scoped), ("p", "p"))

    # ---- 2 / 5 / 9: idempotence, conservation, partial-write recovery -------------------------------
    def window_replace(self, existing, new, start):
        keep = existing.filter(F.col("feed_timestamp") < start)
        return keep.unionByName(self.n06["scope_from"](new, start))

    def test_idempotent_window_replace_and_conservation(self):
        base = self.frame([(t, f"e{t}", "T", 1, "S") for t in range(0, 100, 10)],
                                          "feed_timestamp long, entity_id string, trip_id string, stop_sequence int, stop_id string")
        new = base.unionByName(self.frame([(100, "e100", "T", 1, "S")], "feed_timestamp long, entity_id string, trip_id string, stop_sequence int, stop_id string"))
        once = self.window_replace(base, new, 50)
        twice = self.window_replace(once, new, 50)
        self.assertEqual(self.canon(once, KEY), self.canon(twice, KEY))
        self.assertEqual(self.canon(once, KEY), self.canon(new, KEY))
        lost = self.n06["lost_window_keys"]
        self.assertEqual(lost(self.n06["scope_from"](base, 50), new, KEY), 0)
        shrunk = new.filter("feed_timestamp != 70")
        self.assertEqual(lost(self.n06["scope_from"](base, 50), shrunk, KEY), 1)
        diff = self.n06["overlap_difference"](base, new, 90)
        self.assertEqual(diff, {"only_existing": 0, "only_incremental": 0})
        changed = new.withColumn("trip_id", F.when(F.col("feed_timestamp") == 60, "X").otherwise(F.col("trip_id")))
        self.assertEqual(self.n06["overlap_difference"](base, changed, 90), {"only_existing": 1, "only_incremental": 1})

    def test_partial_write_is_repaired_by_rerunning_same_window(self):
        plan = self.n06["plan_incremental_window"]
        # trip_updates already advanced to 200, observations (written last) still at 150: plan uses the minimum
        first = plan({"trip_updates": 200, "realtime_trip_stop_observations": 150}, 200, 50, 1000)
        self.assertEqual(first["start"], 100)
        # after a complete re-run both are at 200; re-planning is monotonic and covers the same data
        again = plan({"trip_updates": 200, "realtime_trip_stop_observations": 200}, 200, 50, 1000)
        self.assertGreaterEqual(again["start"], first["start"])
        # 07 reads observations (written last by 06): its window starts at or below 06's start
        p07 = self.n07["plan_enrichment_window"](150, 200, 50, 1000)
        self.assertLessEqual(p07["start"], first["start"] + 50)

    # ---- 3: late arrivals ---------------------------------------------------------------------------
    def test_late_arrival_guard(self):
        bronze = self.tu([(500, "e", "T", "V", 1, "S", 5000, 0), (1500, "e2", "T", "V", 1, "S", 5000, 0)])
        late = self.n06["count_late_arrivals"]
        self.assertEqual(late(bronze, 1000, 6000), 0)          # already ingested before the cutoff
        self.assertEqual(late(bronze, 1000, 4000), 1)          # old feed time but ingested after the cutoff
        with self.assertRaises(ValueError):
            late(bronze, 1000, None)

    def test_vp_lookback_escape_guard(self):
        vp = self.vp([(500, "p", "T", "V", 1100, 1)])   # feed time before scope, vehicle clock inside it
        self.assertEqual(self.n06["count_vp_lookback_escapes"](vp, 1000), 1)
        self.assertEqual(self.n06["count_vp_lookback_escapes"](self.vp([(500, "p", "T", "V", 600, 1)]), 1000), 0)

    # ---- 8: empty / invalid ------------------------------------------------------------------------
    def test_planner_fail_closed(self):
        plan, plan7 = self.n06["plan_incremental_window"], self.n07["plan_enrichment_window"]
        for args in (({}, 10, 5, 100), ({"a": None}, 10, 5, 100), ({"a": 10}, None, 5, 100),
                     ({"a": 10}, 5, 5, 100), ({"a": 10}, 500, 5, 100), ({"a": 10}, 20, 0, 100), ({"a": 10}, 20, "x", 100)):
            with self.assertRaises(ValueError): plan(*args)
        for args in ((None, 10, 5, 100), (10, None, 5, 100), (10, 5, 5, 100), (10, 500, 5, 100)):
            with self.assertRaises(ValueError): plan7(*args)
        self.assertEqual(plan({"a": 100}, 120, 30, 1000)["start"], 70)
        self.assertEqual(plan7(100, 120, 30, 1000)["start"], 70)
        self.assertEqual(self.n06["resolve_run_mode"](None), "full")
        with self.assertRaises(ValueError): self.n06["resolve_run_mode"]("auto")

    def test_non_epoch_column_rejected_and_empty_scope(self):
        bad = self.frame([("x",)], "feed_timestamp string")
        with self.assertRaises(ValueError): self.n06["scope_from"](bad, 1)
        self.assertIs(self.n06["scope_from"](bad, None), bad)     # full mode untouched
        self.assertEqual(self.n06["scope_from"](self.tu([]), 10).count(), 0)

    # ---- 11: quality gate unchanged ------------------------------------------------------------------
    def test_invalid_gate_unchanged(self):
        gate = self.n06["validate_service_date_quality"]
        self.assertEqual(self.n06["MAX_INVALID_SERVICE_DATE_RATE"], 0.001)
        gate(10000, 10, 0, 0.001)
        with self.assertRaises(ValueError): gate(10000, 11, 0, 0.0)
        with self.assertRaises(ValueError): gate(10000, 1, 0, 0.002)

    # ---- 10: schema / lineage / 47-feature compatibility ---------------------------------------------
    def test_07_output_contract_unchanged_vs_head(self):
        def literal(source, name):
            for n in ast.parse(source).body:
                if isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == name for t in n.targets):
                    return ast.dump(n.value)
        try:
            head = subprocess.run(["git", "show", "HEAD:notebooks/07_gtfs_realtime_enrichment.py"], cwd=ROOT,
                                  capture_output=True, text=True, encoding="utf-8", check=True).stdout
        except Exception:
            self.skipTest("git HEAD unavailable")
        for name in ("FINAL_COLUMNS", "OUTPUT_TABLE", "OBS_KEY", "SOURCE_TABLES"):
            self.assertEqual(literal(N07, name), literal(head, name), name)
        for text in (N06, N07):
            self.assertIn('mode("overwrite")', text)
        self.assertIn("versionAsOf", N06)
        self.assertIn("incremental_shadow", N06 + N07)

    # ---- hardening (PR review) -------------------------------------------------------------------
    def test_partial_write_every_table_subset_recovers_from_observations_marker(self):
        plan = self.n06["plan_incremental_window"]
        old, new = 150, 200
        # write order tu -> vp -> obs: obs is the commit marker, so only these states are reachable
        for tu_w, obs_w in ((old, old), (new, old), (new, new)):
            got = plan({"trip_updates": tu_w, "realtime_trip_stop_observations": obs_w}, new, 50, 1000)
            self.assertEqual(got["start"], min(tu_w, obs_w) - 50)      # never skips an unpublished range
        # an order-violating state (obs ahead of tu) is still covered by the minimum, not trusted
        self.assertEqual(plan({"trip_updates": old, "realtime_trip_stop_observations": new}, new, 50, 1000)["start"], 100)
        # observations-last is asserted on the real notebook text
        self.assertLess(N06.index('"vehicle_positions": '), N06.index('"realtime_trip_stop_observations": '))
        self.assertIn("observations LAST", N06)

    def test_shadow_difference_fails_and_documented_is_not_pass(self):
        classify = self.n06["classify_shadow"]
        clean = {"t": {"only_existing": 0, "only_incremental": 0}}
        self.assertEqual(classify(clean, {})["status"], "SHADOW_MATCH")
        diff = {"t": {"only_existing": 1, "only_incremental": 0}}
        self.assertEqual(classify(diff, {})["status"], "SHADOW_FAILED")
        # a documented entry without a reason or with different counts never explains the difference
        self.assertEqual(classify(diff, {"t": {"only_existing": 1, "only_incremental": 0}})["status"], "SHADOW_FAILED")
        self.assertEqual(classify(diff, {"t": {"only_existing": 2, "only_incremental": 0, "reason": "x"}})["status"],
                         "SHADOW_FAILED")
        ok = classify(diff, {"t": {"only_existing": 1, "only_incremental": 0, "reason": "reviewed"}})
        self.assertEqual(ok["status"], "SHADOW_DOCUMENTED_DIFFERENCES")
        self.assertNotEqual(ok["status"], "SHADOW_MATCH")
        self.assertEqual(self.n06["EXPECTED_SHADOW_DIFFERENCES"], {})
        self.assertIn('raise ValueError("incremental_shadow FAIL', N06)
        self.assertIn('raise ValueError("incremental_shadow FAIL', N07)

    def test_incremental_write_blocked_while_prerequisites_unproven(self):
        allowed = self.n06["assert_incremental_write_allowed"]
        blockers = self.n06["INCREMENTAL_WRITE_BLOCKERS"]
        self.assertTrue(any("global_invalid_gate" in b for b in blockers))
        self.assertTrue(any("late_arrival_checkpoint" in b for b in blockers))
        self.assertTrue(any("delta_replacewhere" in b for b in blockers))
        with self.assertRaises(RuntimeError):
            allowed(blockers)
        allowed(())   # only an explicit reviewed removal of every blocker opens the gate
        for text in (N06, N07):   # the blocker check precedes the first write call
            call = text.index("assert_incremental_write_allowed(INCREMENTAL_WRITE_BLOCKERS)")
            self.assertGreater(text.index("replace_window(", call), call)       # a write call follows the guard
        self.assertIn("window_only_NOT_equivalent", N06)

    def test_replace_window_preconditions(self):
        check = self.n06["check_replace_window"]
        target = self.tu([(100, "e", "T", "V", 1, "S", 1, 0)]).schema
        good = self.tu([(150, "e", "T", "V", 1, "S", 1, 0)])
        check(good, target, 120)
        with self.assertRaises(ValueError):     # row below the predicate violates Delta's replaceWhere constraint
            check(self.tu([(100, "e", "T", "V", 1, "S", 1, 0)]), target, 120)
        with self.assertRaises(ValueError):     # NULL feed_timestamp is outside any predicate
            check(self.tu([(None, "e", "T", "V", 1, "S", 1, 0)]), target, 120)
        with self.assertRaises(ValueError):     # reordered columns
            check(good.select(*reversed(good.columns)), target, 120)
        with self.assertRaises(ValueError):     # changed type
            check(good.withColumn("delay", F.col("delay").cast("string")), target, 120)

    def test_default_is_full_and_writes_are_opt_in(self):
        for text in (N06, N07):
            self.assertIn('read_widget("run_mode", "full")', text)
            self.assertIn('read_widget("allow_incremental_write", "false")', text)


if __name__ == "__main__":
    unittest.main()
