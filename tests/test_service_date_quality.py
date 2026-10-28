"""Execute the notebook's production date policy without importing Spark."""
import ast
from datetime import date, datetime
from pathlib import Path
import re
import unittest

ROOT = Path(__file__).resolve().parents[1]


class Expr:
    def __init__(self, fn): self.fn = fn
    def cast(self, dtype):
        if dtype == "string":
            return Expr(lambda row: None if self.fn(row) is None else str(self.fn(row)))
        return Expr(lambda row: None if self.fn(row) is None else self.fn(row).date())
    def rlike(self, pattern):
        return Expr(lambda row: self.fn(row) is not None and re.fullmatch(pattern, self.fn(row)) is not None)
    def otherwise(self, value): return self


class Functions:
    @staticmethod
    def col(name): return Expr(lambda row: row[name])
    @staticmethod
    def lit(value): return value
    @staticmethod
    def trim(value): return Expr(lambda row: None if value.fn(row) is None else value.fn(row).strip(" "))
    @staticmethod
    def when(condition, value): return Expr(lambda row: value.fn(row) if condition.fn(row) else None)
    @staticmethod
    def try_to_timestamp(value, fmt):
        assert fmt == "yyyyMMdd"
        def parse(row):
            try: return datetime.strptime(value.fn(row), "%Y%m%d")
            except (ValueError, TypeError): return None
        return Expr(parse)


class ServiceDateQualityTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        path = ROOT / "notebooks/06_gtfs_realtime_silver.py"
        tree = ast.parse(path.read_text(encoding="utf-8"))
        names = {"normalize_service_date", "validate_service_date_quality", "to_tolerant_date"}
        body = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names]
        cls.scope = {"F": Functions, "MAX_INVALID_SERVICE_DATE_RATE": 0.001}
        exec(compile(ast.Module(body=body, type_ignores=[]), str(path), "exec"), cls.scope)

    def test_dates_and_native_expression_agree(self):
        cases = [("20261028", date(2026, 10, 28)), (None, None), ("", None),
                 ("2026-10-28", None), ("20260230", None), ("202610281", None),
                 (" 20261028 ", date(2026, 10, 28)), ("20240229", date(2024, 2, 29)),
                 ("20230229", None), ("２０２６１０２８", None)]
        native = self.scope["to_tolerant_date"]("start_date")
        for raw, expected in cases:
            with self.subTest(raw=raw):
                self.assertEqual(self.scope["normalize_service_date"](raw), expected)
                self.assertEqual(native.fn({"start_date": raw}), expected)

    def test_real_anomaly_is_excluded_without_fabricating_date(self):
        self.assertIsNone(self.scope["normalize_service_date"](""))
        result = self.scope["validate_service_date_quality"](60000, 1, 0, 1 / 60000)
        self.assertEqual(result["excluded_rows"], 1)
        self.assertEqual(result["repaired_rows"], 0)
        self.assertEqual(result["valid_rows"], 59999)

    def test_valid_rows_unchanged_and_repairs_accounted(self):
        result = self.scope["validate_service_date_quality"](10000, 0, 2, 0)
        self.assertEqual(result["valid_rows"], 9998)
        self.assertEqual(result["repaired_rows"], 2)
        self.assertEqual(result["excluded_rows"], 0)

    def test_quality_budget_boundary(self):
        self.scope["validate_service_date_quality"](1000, 1, 0, 0.001)
        for args in [(1000, 2, 0, 0.002), (1000000, 1, 0, 0.5),
                     (0, 0, 0, 0), (10, 10, 0, 1), (10, -1, 0, 0),
                     (10, 0, 11, 0), (10, 0, 0, float("nan"))]:
            with self.subTest(args=args), self.assertRaises(ValueError):
                self.scope["validate_service_date_quality"](*args)

    def test_isolation_precedes_outputs_and_write_gate(self):
        text = (ROOT / "notebooks/06_gtfs_realtime_silver.py").read_text(encoding="utf-8")
        isolation = text.index('trip_updates_silver = trip_updates_silver.filter(F.col("service_date").isNotNull())')
        self.assertLess(isolation, text.index('tu = trip_updates_silver.alias'))
        self.assertLess(isolation, text.index('if not ALLOW_SILVER_OVERWRITE:'))
        self.assertIn('latest source epoch lost; write blocked', text)
        self.assertIn('invalid service date escaped isolation; write blocked', text)


if __name__ == "__main__":
    unittest.main()
