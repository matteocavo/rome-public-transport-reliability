"""Tests for notebook 06a (hybrid mode preflight). Pure python: the notebook functions are extracted with ast."""
import ast
import json
import random
import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
N06 = (ROOT / "notebooks/06_gtfs_realtime_silver.py").read_text(encoding="utf-8")
N07 = (ROOT / "notebooks/07_gtfs_realtime_enrichment.py").read_text(encoding="utf-8")
NPF = (ROOT / "notebooks/06a_downstream_mode_preflight.py").read_text(encoding="utf-8")
JOB_PATH = ROOT / "jobs/downstream_hybrid_refresh.json"
COPIED = ("OBSERVED_MAX_INGESTION_LAG_SECONDS", "parse_positive_int", "assert_safety_margin",
          "plan_incremental_window", "plan_enrichment_window")
LOCAL = ("RUN_MODE_FULL", "RUN_MODE_INCREMENTAL", "UNBOUNDED_WINDOW_SECONDS", "parse_bool",
         "parse_non_negative_int", "decide_run_mode", "MIN_VALID_EPOCH", "FUTURE_TOLERANCE_SECONDS",
         "validate_watermarks")


def _nodes(source):
    out = {}
    for node in ast.parse(source).body:
        if isinstance(node, ast.FunctionDef):
            out[node.name] = node
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    out[target.id] = node
    return out


def _load(names):
    nodes = _nodes(NPF)
    ns = {}
    code = ast.Module(body=[nodes[n] for n in names], type_ignores=[])
    exec(compile(code, "06a", "exec"), ns)
    return ns


NS = _load(COPIED + LOCAL)
decide = NS["decide_run_mode"]
N06_NODES, N07_NODES, NPF_NODES = _nodes(N06), _nodes(N07), _nodes(NPF)
NOW = 1_800_000_000
H = 3600


def run(sil_tu, sil_obs, bronze, enriched, safety=3600, maxw=21600, margin=1800, enabled="true", now=NOW):
    return decide(enabled, sil_tu, sil_obs, bronze, enriched, safety, maxw, margin, now)


class CopyIdentity(unittest.TestCase):
    def test_copies_match_notebooks_06_07_verbatim(self):
        for name in COPIED:
            origin = N07_NODES if name == "plan_enrichment_window" else N06_NODES
            self.assertEqual(ast.dump(NPF_NODES[name]), ast.dump(origin[name]), name)


class Equivalence(unittest.TestCase):
    def test_margin_zero_matches_real_planners_randomised(self):
        rng = random.Random(7)
        plan06, plan07 = NS["plan_incremental_window"], NS["plan_enrichment_window"]
        for _ in range(2000):
            safety = rng.choice([901, 1800, 3600])
            maxw = rng.choice([7200, 21600])
            base = NOW
            tu, ob = base - rng.randint(0, 40000), base - rng.randint(0, 40000)
            br = base + rng.randint(-100, 100)
            en = base - rng.randint(0, 40000)
            try:
                w06 = plan06({"trip_updates": tu, "realtime_trip_stop_observations": ob}, br, safety, maxw)
                w07 = plan07(en, max(br, ob), safety, maxw)
                expected = "incremental"
            except ValueError:
                expected = "full"
            try:
                got = run(tu, ob, br, en, safety, maxw, 0)["run_mode"]
            except ValueError:
                got = "anomaly"
            if expected == "incremental":
                # the preflight is stricter (explicit ordering): it may raise, but never returns full here
                stricter = br < max(tu, ob) or en > ob
                self.assertEqual(got, "anomaly" if stricter else "incremental")
            else:
                # real planners reject: preflight must return full (over-limit) or raise (anomaly); never incremental
                self.assertNotEqual(got, "incremental")


class Scenarios(unittest.TestCase):
    def test_normal_daytime_is_incremental(self):
        r = run(NOW - 600, NOW - 600, NOW, NOW - 600)
        self.assertEqual((r["run_mode"], r["reason"]), ("incremental", "window_within_threshold"))
        self.assertEqual(r["window_06_seconds"], 600 + H)

    def test_night_gap_is_full(self):
        r = run(NOW - 12 * H, NOW - 12 * H, NOW, NOW - 12 * H)
        self.assertEqual((r["run_mode"], r["reason"]), ("full", "window_exceeds_threshold"))

    def test_boundary_at_threshold(self):
        # window = (source - wm) + safety ; threshold = 21600 - 1800 = 19800
        at = NOW - (19800 - 3600)
        self.assertEqual(run(at, at, NOW, at)["run_mode"], "incremental")
        self.assertEqual(run(at - 1, at - 1, NOW, at - 1)["run_mode"], "full")

    def test_margin_changes_threshold(self):
        wm = NOW - 17 * 1000  # window = 17000 + 3600 = 20600
        self.assertEqual(run(wm, wm, NOW, wm, margin=0)["run_mode"], "incremental")
        self.assertEqual(run(wm, wm, NOW, wm, margin=1800)["run_mode"], "full")

    def test_window_uses_min_watermark_and_enriched(self):
        r = run(NOW - 100, NOW - 9 * H, NOW, NOW - 9 * H)
        self.assertEqual(r["run_mode"], "full")
        r = run(NOW - 100, NOW - 100, NOW, NOW - 9 * H)
        self.assertEqual(r["run_mode"], "full")

    def test_incremental_disabled_forces_full(self):
        r = run(NOW - 60, NOW - 60, NOW, NOW - 60, enabled="false")
        self.assertEqual((r["run_mode"], r["reason"]), ("full", "incremental_disabled"))

    def test_disabled_still_validates_data(self):
        with self.assertRaises(ValueError):
            run(None, NOW, NOW, NOW, enabled="false")


class FailClosed(unittest.TestCase):
    def test_ordering_counterexamples(self):
        with self.assertRaises(ValueError):   # Enriched ahead of current Silver observations
            run(NOW - 600, NOW - 600, NOW, NOW - 300)
        with self.assertRaises(ValueError):   # Bronze behind the most recent Silver
            run(NOW - 600, NOW - 100, NOW - 300, NOW - 600)
        with self.assertRaises(ValueError):
            run(NOW - 100, NOW - 600, NOW - 300, NOW - 600)

    def test_epoch_validity_and_future_bounds(self):
        for bad in (-1, 0, 1_577_836_799):
            with self.assertRaises(ValueError, msg=str(bad)):
                run(bad, bad, bad, bad)
        future = NOW + 301
        with self.assertRaises(ValueError):
            run(future, future, future, future, now=NOW)
        with self.assertRaises(ValueError):
            run(future + 10 * H, future + 10 * H, future + 10 * H, future + 10 * H)
        ok = NOW + 300   # exactly at tolerance
        self.assertEqual(run(ok, ok, ok, ok)["run_mode"], "incremental")
        lo = 1_577_836_800
        self.assertEqual(run(lo, lo, lo, lo, now=lo)["run_mode"], "incremental")
        for bad in (1.5, True, "x"):
            with self.assertRaises((ValueError, TypeError)):
                run(NOW, NOW, NOW, bad)

    def test_inconsistent_or_missing_data_raises(self):
        for args in [(None, NOW, NOW, NOW), (NOW, None, NOW, NOW), (NOW, NOW, None, NOW), (NOW, NOW, NOW, None),
                     (NOW, NOW, NOW - 1000, NOW),   # Bronze behind Silver
                     (NOW, NOW, NOW, NOW + 5000)]:  # Enriched ahead of Silver observations/Bronze
            with self.assertRaises(ValueError, msg=str(args)):
                run(*args)

    def test_invalid_parameters_raise(self):
        good = (NOW - 60, NOW - 60, NOW, NOW - 60)
        for kw in [dict(safety=100), dict(safety="abc"), dict(maxw=0), dict(maxw="x"), dict(margin=-1),
                   dict(margin="x"), dict(margin=21600), dict(margin=30000), dict(enabled="yes"), dict(enabled="")]:
            with self.assertRaises(ValueError, msg=str(kw)):
                run(*good, **kw)

    def test_notebook_has_no_fallback_or_writes(self):
        # int() parsing helpers may catch; decision logic, Spark reads and the top-level script may not.
        guarded = [n for n in ast.parse(NPF).body
                   if not (isinstance(n, ast.FunctionDef) and n.name.startswith(("parse_", "assert_", "plan_")))]
        for node in guarded:
            self.assertFalse([x for x in ast.walk(node) if isinstance(x, (ast.Try, ast.ExceptHandler))])
        for forbidden in (r"\.write\b", r"saveAsTable", r"\bMERGE\b", r"\bINSERT\b", r"\bDELETE\b", r"\bOPTIMIZE\b",
                          r"\bVACUUM\b", r"\.insertInto", r"(?<!py)spark\.sql", r"createOrReplace"):
            self.assertIsNone(re.search(forbidden, NPF), forbidden)
        self.assertEqual(set(re.findall(r'spark\.table\(([^)]*)\)', NPF)), {"table"})
        for table in ("silver.trip_updates", "silver.realtime_trip_stop_observations", "bronze.trip_updates",
                      "silver.realtime_enriched_observations"):
            self.assertIn(f"rome_transport.{table}", NPF)

    def test_publishes_expected_task_values(self):
        self.assertIn('("run_mode", "reason", "window_06_seconds", "window_07_seconds")', NPF)
        self.assertIn("taskValues.set", NPF)


class JobConfig(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.raw = JOB_PATH.read_text(encoding="utf-8")
        cls.job = json.loads(cls.raw)
        cls.tasks = {t["task_key"]: t for t in cls.job["tasks"]}

    def test_dependency_order(self):
        deps = {k: [d["task_key"] for d in t.get("depends_on", [])] for k, t in self.tasks.items()}
        self.assertEqual(deps, {"preflight_mode": [], "silver_06": ["preflight_mode"],
                                "enrichment_07": ["silver_06"], "scoring_input_08": ["enrichment_07"],
                                "operational_gold_13": ["scoring_input_08"]})

    def test_same_dynamic_mode_for_06_and_07(self):
        ref = "{{tasks.preflight_mode.values.run_mode}}"
        for key in ("silver_06", "enrichment_07"):
            params = self.tasks[key]["notebook_task"]["base_parameters"]
            self.assertEqual(params["run_mode"], ref)
            self.assertEqual(params["allow_incremental_write"], "true")
        for key in ("scoring_input_08", "operational_gold_13", "preflight_mode"):
            self.assertNotIn("run_mode", self.tasks[key]["notebook_task"].get("base_parameters", {}))

    def test_no_job_level_override_of_mode(self):
        names = {p["name"] for p in self.job["parameters"]}
        self.assertEqual(names, {"incremental_enabled", "safety_window_seconds", "max_window_seconds"})
        self.assertNotIn("run_mode", names)
        self.assertNotIn("allow_incremental_write", names)

    def test_preflight_task(self):
        t = self.tasks["preflight_mode"]
        self.assertEqual(t["notebook_task"]["base_parameters"], {"preflight_margin_seconds": "1800"})
        self.assertTrue(t["notebook_task"]["notebook_path"].endswith("/06a_downstream_mode_preflight"))
        self.assertLessEqual(t["timeout_seconds"], 300)
        for key in ("silver_06", "enrichment_07", "scoring_input_08", "operational_gold_13"):
            self.assertIn(self.tasks[key].get("run_if", "ALL_SUCCESS"), ("ALL_SUCCESS",))

    def test_timeouts_and_concurrency(self):
        self.assertEqual(self.tasks["silver_06"]["timeout_seconds"], 3000)
        self.assertEqual(self.job["max_concurrent_runs"], 1)
        self.assertTrue(self.job["queue"]["enabled"])

    def test_template_is_paused_and_consistent(self):
        self.assertEqual(self.job["trigger"]["pause_status"], "PAUSED")
        self.assertEqual(self.job["trigger"]["periodic"], {"interval": 3, "unit": "HOURS"})
        self.assertIn("PAUSED", self.job["description"])
        self.assertNotIn("2 ore", self.job["description"])

    def test_paths_are_placeholders_not_personal(self):
        self.assertNotIn("@", self.raw)
        self.assertNotIn("/Workspace/Users", self.raw)
        for t in self.tasks.values():
            self.assertTrue(t["notebook_task"]["notebook_path"].startswith("{{NOTEBOOK_ROOT}}/"))


if __name__ == "__main__":
    unittest.main()
