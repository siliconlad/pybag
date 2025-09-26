#!/usr/bin/env python3
"""Update project version metadata for a release."""
from __future__ import annotations

import argparse
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PYPROJECT = ROOT / "pyproject.toml"
INIT_FILE = ROOT / "src" / "pybag" / "__init__.py"


def _update_file(path: Path, pattern: str, replacement: str) -> None:
    original = path.read_text()
    updated, count = re.subn(pattern, replacement, original, count=0, flags=re.MULTILINE)
    if count != 1:
        raise RuntimeError(f"Expected to update exactly one occurrence in {path}, but changed {count}.")
    path.write_text(updated)


def main(version: str) -> None:
    clean_version = version.removeprefix("v")
    if not clean_version:
        raise SystemExit("Version cannot be empty.")

    _update_file(PYPROJECT, r'^(version\s*=\s*)"[^"]+"', rf'\1"{clean_version}"')
    _update_file(INIT_FILE, r'^(__version__\s*=\s*)"[^"]+"', rf'\1"{clean_version}"')
    print(f"Updated project files to version {clean_version}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Update project version metadata.")
    parser.add_argument("version", help="Version string, optionally prefixed with 'v'.")
    args = parser.parse_args()
    main(args.version)
