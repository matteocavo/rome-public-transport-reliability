"""Operational readiness rejects stale lineage and misleading metric denominators."""
import ast
from datetime import datetime
from pathlib import Path
import unittest
from test_semantic_contract import Column, Functions
from feature_frame import Frame, Functions as FrameFunctions


class DownstreamRefreshTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        path = Path(__file__).resolve().parents[1] / "notebooks/13_current_operational_analytics.py"
        tree = ast.parse(path.read_text(encoding="utf-8"))
        functions = [n for n in tree.body if isinstance(n, ast.FunctionDef)
                     and n.name in ["validate_operational_lineage", "reconcile_operational_network", "operational_delay_band"]]
        cls.scope = {"F": Functions(0)}
        exec(compile(ast.Module(body=functions, type_ignores=[]), str(path), "exec"), cls.scope)

    def test_operational_reported_delay_boundaries_and_missingness(self):
        for value, expected in [(-61, "early"), (-60, "on_time"), (179, "on_time"),
                                (180, "moderate_delay"), (299, "moderate_delay"),
                                (300, "major_delay"), (600, "major_delay"), (None, "unknown")]:
            with self.subTest(value=value):
                self.assertEqual(self.scope["operational_delay_band"](Column(value)), expected)

    def test_static_schedule_conflicts_preserve_raw_identity_and_mask_attributes(self):
        path = Path(__file__).resolve().parents[1] / "notebooks/07_gtfs_realtime_enrichment.py"
        text = path.read_text(encoding="utf-8")
        start = text.index('scheduled_observations = scheduled_observations.withColumn("schedule_metadata_conflict_flag"')
        end = text.index('require_same_count(scheduled_observations, source_count, "Schedule LEFT JOIN")', start)
        metadata = ["route_short_name", "agency_id", "stop_lat", "arrival_seconds"]
        good = dict(route_id="r1", scheduled_route_id="r1", direction_id=0, scheduled_direction_id=0,
                    schedule_match_flag=True, route_short_name="R1", agency_id="a1", stop_lat=41.9, arrival_seconds=100)
        rows = [good, dict(good, scheduled_route_id="r2"), dict(good, scheduled_direction_id=1)]
        scope = dict(F=FrameFunctions, scheduled_observations=Frame(rows), schedule_metadata=metadata)
        exec(compile(text[start:end], str(path), "exec"), scope)
        result = scope["scheduled_observations"].rows
        self.assertEqual(len(result), 3)
        self.assertFalse(result[0]["schedule_metadata_conflict_flag"])
        for name in metadata:
            self.assertEqual(result[0][name], good[name])
        for row in result[1:]:
            self.assertEqual(row["route_id"], "r1")
            self.assertEqual(row["direction_id"], 0)
            self.assertTrue(row["schedule_metadata_conflict_flag"])
            self.assertFalse(row["schedule_match_flag"])
            self.assertTrue(all(row[name] is None for name in metadata))

    def test_lineage_requires_same_pinned_source_single_epoch_and_preparation(self):
        good = dict(rows=10, epochs=1, batches=1, version_min=9, version_max=9,
                    hash_min="frozen", hash_max="frozen", prepared_at=datetime(2026, 10, 25), feed=100)
        self.scope["validate_operational_lineage"](good, 9, "frozen")
        for changes in [dict(rows=0), dict(epochs=2), dict(batches=2), dict(version_min=8),
                        dict(version_max=10), dict(hash_min="changed"), dict(hash_max="changed"),
                        dict(prepared_at=None), dict(feed=None)]:
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                self.scope["validate_operational_lineage"](dict(good, **changes), 9, "frozen")

    def test_known_delay_denominator_keeps_missing_observations_separate(self):
        good = dict(ops_observation_count=10, ops_known_delay_count=8, ops_missing_delay_count=2,
                    ops_major_delay_count=2, ops_early_count=1, ops_on_time_count=4,
                    ops_moderate_delay_count=1, ops_latest_feed_timestamp=100)
        self.scope["reconcile_operational_network"](good, 100)
        for changes in [dict(ops_observation_count=0), dict(ops_known_delay_count=10),
                        dict(ops_missing_delay_count=0), dict(ops_major_delay_count=3),
                        dict(ops_on_time_count=-1), dict(ops_latest_feed_timestamp=99)]:
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                self.scope["reconcile_operational_network"](dict(good, **changes), 100)


if __name__ == "__main__":
    unittest.main()
