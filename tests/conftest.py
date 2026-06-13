"""Shared test fixtures: load the small JSON event fixtures from disk."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

_FIXTURES = Path(__file__).parent / "fixtures"


def _load(name: str) -> dict[str, Any]:
    return json.loads((_FIXTURES / name).read_text())  # type: ignore[no-any-return]


@pytest.fixture
def post_event() -> dict[str, Any]:
    return _load("post_event.json")


@pytest.fixture
def like_event() -> dict[str, Any]:
    return _load("like_event.json")


@pytest.fixture
def follow_event() -> dict[str, Any]:
    return _load("follow_event.json")


@pytest.fixture
def malformed_post_event() -> dict[str, Any]:
    return _load("malformed_post_event.json")
