"""Test utilities for Orch8 applications.

* :class:`FakeEngine` — pure-Python in-memory engine (worker protocol,
  instances, jobs) with a virtual clock; no server or native code needed.
* :class:`NativeEnvironment` — the real engine embedded through the optional
  ``orch8-engine-native`` bindings, with time-skipping execution.
* ``orch8.testing.pytest_plugin`` — fixtures (``orch8_engine``,
  ``orch8_client``, ``orch8_native``); enable with
  ``pytest_plugins = ["orch8.testing.pytest_plugin"]``.
"""
from .fake_engine import FakeClock, FakeEngine, FakeInstance, FakeTask
from .native import NativeEnvironment, NativeRunResult, load_native, native_available

__all__ = [
    "FakeClock",
    "FakeEngine",
    "FakeInstance",
    "FakeTask",
    "NativeEnvironment",
    "NativeRunResult",
    "load_native",
    "native_available",
]
