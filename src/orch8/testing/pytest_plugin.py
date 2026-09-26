"""pytest fixtures for Orch8 tests.

Enable in ``conftest.py``::

    pytest_plugins = ["orch8.testing.pytest_plugin"]
"""
from __future__ import annotations

from collections.abc import Iterator

import pytest

from ..client import Orch8Client
from .fake_engine import FakeEngine
from .native import NativeEnvironment, native_available


@pytest.fixture
def orch8_engine() -> FakeEngine:
    """A fresh in-memory fake engine per test."""
    return FakeEngine()


@pytest.fixture
def orch8_client(orch8_engine: FakeEngine) -> Iterator[Orch8Client]:
    """An ``Orch8Client`` bound in-process to ``orch8_engine``."""
    yield orch8_engine.client()


@pytest.fixture
def orch8_native() -> NativeEnvironment:
    """The embedded native engine; skips when the bindings are not installed."""
    if not native_available():
        pytest.skip("orch8-engine-native is not installed")
    return NativeEnvironment()
