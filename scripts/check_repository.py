"""Repository policy: reject credentials, runtime data and unpinned Actions."""

import re
import subprocess
from pathlib import Path, PurePosixPath

SAFE_ENV = {".env.example", ".env.template"}
FORBIDDEN_DIRS = {
    ".databricks",
    ".secrets",
    ".venv",
    "venv",
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    ".cache",
    "cache",
    "data_exports",
    "tmp",
    "temp",
}
FORBIDDEN_SUFFIXES = {
    ".pyc",
    ".pyo",
    ".pem",
    ".key",
    ".p12",
    ".pfx",
    ".jks",
    ".log",
    ".tmp",
    ".temp",
    ".bak",
    ".joblib",
    ".pickle",
    ".pkl",
    ".parquet",
    ".arrow",
    ".orc",
    ".avro",
    ".csv",
    ".pbix",
    ".pbit",
}
FORBIDDEN_NAMES = {
    "credentials.json",
    "tokens.json",
    "token.json",
    "token.txt",
    ".databrickscfg",
}


def forbidden_path(name: str) -> bool:
    p = PurePosixPath(name)
    if p.name in SAFE_ENV:
        return False
    if p.name == ".env" or p.name.startswith(".env."):
        return True
    if any(part in FORBIDDEN_DIRS for part in p.parts):
        return True
    if p.name in FORBIDDEN_NAMES or p.suffix.lower() in FORBIDDEN_SUFFIXES:
        return True
    if p.parts and p.parts[0] == "artifacts" and name != "artifacts/.gitkeep":
        return True
    if name.startswith("data/reference/gtfs_static/") or name.startswith(
        ("data/raw/", "data/generated/")
    ):
        return True
    return p.name.endswith("~")


def main():
    paths = subprocess.check_output(["git", "ls-files", "-z"]).decode().split("\0")
    patterns = [
        rb"dapi[a-f0-9]{32,}",
        rb"gh[pousr]_[A-Za-z0-9]{30,}",
        rb"github_pat_[A-Za-z0-9_]{40,}",
        rb"AKIA[A-Z0-9]{16}",
        rb"-----BEGIN [A-Z ]*PRIVATE KEY-----",
    ]
    count = 0
    for name in filter(None, paths):
        if forbidden_path(name):
            raise ValueError(f"Forbidden tracked file: {name}")
        path = Path(name)
        if path.is_symlink():
            raise ValueError(f"Tracked symlink requires explicit review: {name}")
        raw = path.read_bytes()
        if any(re.search(pattern, raw) for pattern in patterns):
            raise ValueError(f"Potential secret in {name}; value suppressed")
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError:
            if path.suffix.lower() not in {".png", ".jpg", ".jpeg", ".pdf"}:
                raise ValueError(f"Unreviewed binary file: {name}") from None
            continue
        if path.suffix == ".md":
            for link in re.findall(r"\]\(([^)]+)\)", text):
                if "://" not in link and not link.startswith("#"):
                    target = link.split("#")[0]
                    if target and not (path.parent / target).exists():
                        raise ValueError(f"Broken local link: {name}")
        if name.startswith(".github/workflows/"):
            for action in re.findall(r"uses:\s*([^\s#]+)", text):
                if not action.startswith("./") and not re.fullmatch(
                    r"[^@]+@[a-f0-9]{40}", action
                ):
                    raise ValueError(f"Unpinned action in {name}")
            if "pull_request_target" in text:
                raise ValueError(
                    f"Privileged PR trigger requires security review: {name}"
                )
        count += 1
    print(f"Repository policy PASS: {count} text files; secrets never printed")


if __name__ == "__main__":
    main()
