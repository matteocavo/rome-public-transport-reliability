"""Spark-free regression: notebook 07 must not call functions that are never defined.

Run 989900664539471 (2026-11-26) failed with NameError: assert_epoch_column, called by check_replace_window
but defined only in notebook 06. This test statically resolves every plain-name call inside the function
bodies of notebook 07 against module-level names, builtins, Databricks-injected globals and local bindings.
"""
import ast
import builtins
import unittest
from pathlib import Path

NOTEBOOK = Path(__file__).resolve().parents[1] / "notebooks/07_gtfs_realtime_enrichment.py"
DATABRICKS_GLOBALS = {"spark", "dbutils", "display", "displayHTML", "sc", "sqlContext"}


def _bindings(node):
    names = set()
    for n in ast.walk(node):
        if isinstance(n, ast.Name) and isinstance(n.ctx, (ast.Store, ast.Del)):
            names.add(n.id)
        elif isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            names.add(n.name)
        elif isinstance(n, ast.arg):
            names.add(n.arg)
        elif isinstance(n, (ast.Import, ast.ImportFrom)):
            names.update((a.asname or a.name).split(".")[0] for a in n.names)
        elif isinstance(n, ast.ExceptHandler) and n.name:
            names.add(n.name)
    return names


class Notebook07NamesTests(unittest.TestCase):
    def test_assert_epoch_column_is_defined_and_used_by_incremental_path(self):
        tree = ast.parse(NOTEBOOK.read_text(encoding="utf-8"))
        defs = {n.name: n for n in tree.body if isinstance(n, ast.FunctionDef)}
        self.assertIn("assert_epoch_column", defs)
        for caller in ("scope_from", "check_replace_window"):
            calls = {c.func.id for c in ast.walk(defs[caller]) if isinstance(c, ast.Call) and isinstance(c.func, ast.Name)}
            self.assertIn("assert_epoch_column", calls, caller)

    def test_no_undefined_function_calls_in_function_bodies(self):
        tree = ast.parse(NOTEBOOK.read_text(encoding="utf-8"))
        module_names = _bindings(tree) | set(dir(builtins)) | DATABRICKS_GLOBALS
        missing = []
        for fn in (n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)):
            known = module_names | _bindings(fn)
            for c in ast.walk(fn):
                if isinstance(c, ast.Call) and isinstance(c.func, ast.Name) and c.func.id not in known:
                    missing.append(f"{fn.name}:{c.lineno} {c.func.id}")
        self.assertEqual(missing, [])


if __name__ == "__main__":
    unittest.main()
