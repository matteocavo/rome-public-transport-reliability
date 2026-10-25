"""Check the real handoff projections against the frozen preparation contract."""

import ast
import hashlib
import json
from pathlib import Path
import re
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from contracts.operational_ml import inference_select, load_contract, training_select


def preparation_constants():
    tree = ast.parse((ROOT / "notebooks/08_common_feature_preparation.py").read_text(encoding="utf-8"))
    names = {"MODEL_CONTRACT", "MODEL_RAW_TYPES", "OBS_KEY", "SCORING_ID_COLUMNS", "SCORING_LINEAGE_COLUMNS"}
    return {t.id: ast.literal_eval(n.value) for n in tree.body if isinstance(n, ast.Assign)
            for t in n.targets if isinstance(t, ast.Name) and t.id in names}


class OperationalMLContractTest(unittest.TestCase):
    def setUp(self):
        self.c = load_contract()
        self.frozen = preparation_constants()

    def test_exact_features_order_types_and_historical_hash(self):
        numeric, categorical = self.c["numerical_features"], self.c["categorical_features"]
        self.assertEqual(len(numeric), 42)
        self.assertEqual(categorical, ["route_id", "route_type", "direction_id", "agency_id", "current_status"])
        self.assertEqual(len(set(numeric + categorical)), 47)
        self.assertEqual(numeric, self.frozen["MODEL_CONTRACT"]["numeric"])
        self.assertEqual(categorical, self.frozen["MODEL_CONTRACT"]["categorical"])
        self.assertEqual(self.c["raw_feature_types"], self.frozen["MODEL_RAW_TYPES"])
        self.assertEqual(self.c["historical_spark_contract"], self.frozen["MODEL_CONTRACT"])
        digest = hashlib.sha256(json.dumps(self.frozen["MODEL_CONTRACT"], sort_keys=True).encode()).hexdigest()
        self.assertEqual(self.c["feature_contract_hash"], digest)
        self.assertEqual(digest, "11682b3cc1ab5c1d8440af4f6294e932e815126c3dd86ab609f4d5f8f35c41a5")

    def test_inference_is_exact_input_allowlist_without_outcomes(self):
        expected = list(dict.fromkeys(self.frozen["SCORING_ID_COLUMNS"]
                                     + self.c["numerical_features"] + self.c["categorical_features"]
                                     + self.frozen["SCORING_LINEAGE_COLUMNS"]))
        self.assertEqual(self.c["inference"]["columns"], expected)
        query = inference_select(version=0, feed_timestamp=1790928849)
        selected = re.findall(r"`([^`]+)`", query.split(" FROM ")[0])
        self.assertEqual(selected, expected)
        self.assertEqual([x for x in selected if x in self.c["numerical_features"]
                          + self.c["categorical_features"]],
                         self.c["numerical_features"] + self.c["categorical_features"])
        self.assertNotIn("*", query)
        self.assertNotIn("labeled", query)
        for field in selected:
            self.assertFalse(any(x in field for x in ("realized", "provisional", "prediction", "label_")), field)
        self.assertIn("VERSION AS OF 0", query)
        self.assertIn("feed_timestamp = 1790928849", query)
        self.assertIn("feed_age_seconds", query)
        self.assertIn(self.c["feature_contract_hash"], query)

    def test_identity_and_lineage_are_separate_from_features(self):
        self.assertEqual(self.c["observation_key"], self.frozen["OBS_KEY"])
        self.assertTrue(set(self.c["observation_key"]).issubset(self.c["inference"]["columns"]))
        features = self.c["numerical_features"] + self.c["categorical_features"]
        self.assertTrue(set(features).isdisjoint(self.c["observation_key"]))
        self.assertTrue(set(features).isdisjoint(self.c["inference"]["lineage_columns"]))
        self.assertTrue(all(not f.startswith("target_") for f in features))

    def test_training_semantics_temporal_boundaries_and_quality(self):
        self.assertEqual(self.c["event_id"], "next_stop_delay_ge_300s")
        self.assertEqual(self.c["target"]["seconds"], 300)
        self.assertEqual(self.c["target"]["operator"], ">=")
        self.assertEqual(self.c["training"]["validated_version"], 12)
        self.assertEqual(self.c["training"]["historical_reference_version"], 8)
        query = training_select(version=12, start_epoch=100, end_epoch=200, label_available_before=200)
        for predicate in ["VERSION AS OF 12", "feed_timestamp >= 100", "feed_timestamp < 200",
                          "label_source_feed_timestamp < 200", "label_source_feed_timestamp > feed_timestamp",
                          "label_horizon_seconds BETWEEN 1 AND 1800",
                          "label_horizon_seconds = label_source_feed_timestamp - feed_timestamp",
                          "ABS(`target_realized_next_stop_delay_seconds`) <= 43200",
                          "`target_realized_major_delay_flag` = CAST(`target_realized_next_stop_delay_seconds` >= 300 AS INT)"]:
            self.assertIn(predicate, query)
        for field in self.c["observation_key"]:
            self.assertIn(f"`{field}` IS NOT NULL", query)

    def test_versions_and_epoch_bounds_cannot_inject_sql_or_silently_default(self):
        for bad in [None, True, -1, 1.2, "12; DROP TABLE x"]:
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    inference_select(version=bad, feed_timestamp=100)
                with self.assertRaises(ValueError):
                    training_select(version=12, start_epoch=100, end_epoch=200, label_available_before=bad)
        for start, end, cutoff in [(200, 100, 300), (100, 100, 200), (100, 200, 199)]:
            with self.assertRaises(ValueError):
                training_select(version=12, start_epoch=start, end_epoch=end, label_available_before=cutoff)
        with self.assertRaises(TypeError):
            inference_select(feed_timestamp=100)

    def test_historical_evidence_is_not_a_future_model_configuration(self):
        self.assertEqual(self.c["historical_model_threshold"], 0.45)
        self.assertEqual(self.c["historical_model_comparison"], ">")
        self.assertIsNone(self.c["future_operational_model_threshold"])
        self.assertEqual(self.c["historical_reference_test_metrics"],
                         dict(accuracy=.9401, precision=.8450, recall=.8123, f1=.8283, roc_auc=.9690, pr_auc=.9063))

    def test_serving_mapping_names_grains_and_historical_scope(self):
        resources = self.c["serving_resources"]
        self.assertEqual(len({r["resource"] for r in resources}), len(resources))
        for item in resources:
            self.assertRegex(item["table"], r"^rome_transport\.(gold|features|ml|bronze)\.[a-z_]+$")
            self.assertTrue(item["grain"])
            self.assertTrue(item["resource"].startswith("/"))
        predictions = next(r for r in resources if r["resource"] == "/predictions/reference")
        self.assertEqual(predictions["status"], "historical_replay")
        self.assertEqual(predictions["grain"], self.c["observation_key"])
        audit = next(r for r in resources if r["resource"] == "/monitoring/ingestion")
        self.assertFalse(audit["unique_key_available"])

    def test_dashboard_target_scope_without_visual_redesign(self):
        dashboard = json.loads((ROOT / "dashboard/databricks/dashboard/Affidabilita_Trasporto_Pubblico_Roma.lvdash.json").read_text(encoding="utf-8"))
        text = json.dumps(dashboard, ensure_ascii=False)
        self.assertIn("FastAPI/sklearn", text)
        self.assertIn("soglia da rivalidare", text)
        self.assertNotIn("RMSE di 489", text)
        self.assertNotIn("Punteggi F1 stabili", text)
        deployment = next(d for d in dashboard["datasets"] if d["name"] == "model_deployment")
        query = "".join(deployment["queryLines"])
        self.assertNotRegex(query.upper(), r"\b(INSERT|UPDATE|DELETE|CREATE|ALTER|MERGE)\b")
        self.assertIn("non inference live", query)


if __name__ == "__main__":
    unittest.main()
