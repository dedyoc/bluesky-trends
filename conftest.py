"""Ensure the repo root is importable so tests can import the ingest/ and schemas/
packages without installing the project (pyproject sets package = false)."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
