import unittest

from scripts.check_repository import forbidden_path


class RepositoryPolicyTest(unittest.TestCase):
    def test_sensitive_paths_rejected(self):
        for name in [
            ".env",
            ".env.local",
            ".databricks/auth.json",
            "tokens.json",
            "credentials.json",
            "cache/value",
            "tmp/session",
            "trace.log",
            "model.pkl",
            "model.joblib",
            "data_exports/sample.json",
            "data/raw/feed.pb",
            "data/private.csv",
            "artifacts/model/metadata.json",
            "data/reference/gtfs_static/routes.txt",
        ]:
            with self.subTest(name=name):
                self.assertTrue(forbidden_path(name))

    def test_source_and_approved_deliverables_allowed(self):
        for name in [
            ".env.example",
            ".env.template",
            "app/core/config.py",
            "artifacts/.gitkeep",
            "docs/DATA_ACCESS.md",
            "tests/test_contract.py",
            "dashboard/export.pdf",
        ]:
            with self.subTest(name=name):
                self.assertFalse(forbidden_path(name))
