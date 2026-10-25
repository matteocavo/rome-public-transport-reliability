"""Exercise the actual notebook expressions without Spark or Databricks access."""

import ast
import json
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]


class Column:
    def __init__(self, value):
        self.value = value

    def isNull(self):
        return Column(self.value is None)

    def __lt__(self, other):
        return Column(False if self.value is None else self.value < unwrap(other))

    def __le__(self, other):
        return Column(False if self.value is None else self.value <= unwrap(other))

    def __gt__(self, other):
        return Column(False if self.value is None else self.value > unwrap(other))

    def cast(self, dtype):
        assert dtype == "int"
        return Column(int(self.value))


def unwrap(value):
    return value.value if isinstance(value, Column) else value


class When:
    def __init__(self, condition, value):
        self.matches = [(unwrap(condition), value)]

    def when(self, condition, value):
        self.matches.append((unwrap(condition), value))
        return self

    def otherwise(self, fallback):
        return next((value for condition, value in self.matches if condition), fallback)


class Functions:
    when = When
    lit = staticmethod(Column)

    def __init__(self, probability):
        self.probability = probability

    def col(self, name):
        assert name == "major_delay_probability"
        return Column(self.probability)


class SemanticContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.gold = ast.parse((ROOT / "notebooks/13_gold_analytics.py").read_text(encoding="utf-8"))
        functions = [node for node in cls.gold.body
                     if isinstance(node, ast.FunctionDef) and node.name in {"delay_band", "risk_band"}]
        cls.scope = {"F": Functions(0)}
        exec(compile(ast.Module(body=functions, type_ignores=[]), "gold_band_functions", "exec"), cls.scope)

    def test_delay_boundaries(self):
        cases = [(-61, "early"), (-60, "on_time"), (179, "on_time"),
                 (180, "moderate_delay"), (299, "moderate_delay"),
                 (300, "major_delay"), (600, "major_delay"), (601, "major_delay"),
                 (None, "unknown")]
        for value, expected in cases:
            with self.subTest(value=value):
                self.assertEqual(self.scope["delay_band"](Column(value)), expected)

    def test_probability_bands_and_actual_scoring_flag(self):
        scoring = ast.parse((ROOT / "notebooks/12_model_registration_and_scoring.py").read_text(encoding="utf-8"))
        score_function = next(node for node in scoring.body
                              if isinstance(node, ast.FunctionDef) and node.name == "score_observations")
        flag = next(node.args[1] for node in ast.walk(score_function)
                    if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                    and node.func.attr == "withColumn" and node.args
                    and isinstance(node.args[0], ast.Constant)
                    and node.args[0].value == "predicted_major_delay_flag")
        threshold = next(ast.literal_eval(node.value) for node in scoring.body
                         if isinstance(node, ast.Assign)
                         and any(isinstance(t, ast.Name) and t.id == "CLASSIFICATION_THRESHOLD"
                                 for t in node.targets))
        self.assertEqual(threshold, 0.45)
        expression = compile(ast.Expression(body=flag), "actual_scoring_flag", "eval")
        cases = [(0.1999, "low_risk", 0), (0.20, "medium_risk", 0),
                 (0.45, "medium_risk", 0), (0.4501, "high_risk", 1),
                 (0.7999, "high_risk", 1), (0.80, "critical_risk", 1)]
        for value, band, predicted in cases:
            with self.subTest(value=value):
                self.assertEqual(self.scope["risk_band"](Column(value)), band)
                result = eval(expression, {"F": Functions(value), "CLASSIFICATION_THRESHOLD": threshold})
                self.assertEqual(result.value, predicted)

    def test_dashboard_semantic_text(self):
        path = ROOT / "dashboard/databricks/dashboard/Affidabilita_Trasporto_Pubblico_Roma.lvdash.json"
        dashboard = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(len(dashboard["pages"]), 5)
        legend = dashboard["pages"][0]["layout"][18]["widget"]["multilineTextboxSpec"]["lines"]
        self.assertIn("[180, 300)", legend[5])
        self.assertIn("next_stop_delay_ge_300s", legend[6])
        self.assertIn("p = 0.45 negativo", legend[6])
        risk = dashboard["pages"][3]["layout"][13]["widget"]["multilineTextboxSpec"]["lines"]
        for text, expected in zip(risk[4:8], ["p < 20%", "20% ≤ p ≤ 45%", "45% < p < 80%", "p ≥ 80%"]):
            self.assertIn(expected, text)
        self.assertNotIn("> 600", json.dumps(dashboard, ensure_ascii=False))


if __name__ == "__main__":
    unittest.main()
