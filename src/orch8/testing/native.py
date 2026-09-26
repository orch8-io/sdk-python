"""Time-skipping, in-memory execution on the real engine via PyO3 bindings.

Requires the optional ``orch8-engine-native`` package, built from
``engine/packages/python-native`` with maturin (it is not on PyPI yet). The
native runner executes built-in handlers in dry-run mode on an isolated
in-memory engine and advances a virtual clock across delays and retry
backoffs, so a workflow that sleeps for days finishes in milliseconds. A
workflow that waits for human input, a signal or an external worker returns
in ``waiting`` state instead of blocking.
"""
from __future__ import annotations

import json
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import ModuleType
from typing import Any

NATIVE_MODULE = "orch8_engine"
INSTALL_HINT = (
    "orch8.testing.NativeEnvironment needs the orch8-engine-native package; "
    "build it from engine/packages/python-native with `maturin build` and "
    "pip install the wheel"
)


def load_native() -> ModuleType:
    """Import the native engine bindings or raise ``ImportError`` with a hint."""
    try:
        import importlib

        return importlib.import_module(NATIVE_MODULE)
    except ImportError as exc:  # pragma: no cover - exercised when absent
        raise ImportError(INSTALL_HINT) from exc


def native_available() -> bool:
    try:
        load_native()
    except ImportError:
        return False
    return True


@dataclass
class NativeRunResult:
    """Outcome of :meth:`NativeEnvironment.run`."""

    state: str
    context: dict[str, Any]
    outputs: list[dict[str, Any]]
    ticks: int
    raw: dict[str, Any] = field(repr=False, default_factory=dict)

    @property
    def data(self) -> dict[str, Any]:
        return self.context.get("data", {}) if isinstance(self.context, dict) else {}

    def output(self, block_id: str) -> Any:
        """Most recent output recorded for ``block_id``."""
        for item in reversed(self.outputs):
            if item.get("block_id") == block_id:
                return item.get("output")
        raise KeyError(block_id)

    @property
    def completed(self) -> bool:
        return self.state == "completed"


class NativeEnvironment:
    """Run sequences on the embedded engine with virtual time.

    ``module`` may be injected (e.g. a stub in unit tests); by default the
    ``orch8_engine`` bindings are imported lazily.
    """

    def __init__(self, *, tenant_id: str = "test", module: ModuleType | Any = None) -> None:
        self.tenant_id = tenant_id
        self._module = module

    @property
    def module(self) -> Any:
        if self._module is None:
            self._module = load_native()
        return self._module

    @property
    def schema_version(self) -> int:
        return int(self.module.sequence_schema_version())

    def _full_definition(self, sequence: Mapping[str, Any]) -> dict[str, Any]:
        payload = dict(sequence)
        payload.setdefault("id", str(uuid.uuid4()))
        payload.setdefault("tenant_id", self.tenant_id)
        payload.setdefault("namespace", "default")
        payload.setdefault("version", 1)
        payload.setdefault("created_at", "2026-01-01T00:00:00Z")
        return payload

    def validate(self, sequence: Mapping[str, Any]) -> dict[str, Any]:
        """Strictly validate with the engine; returns the normalized sequence."""
        normalized = self.module.validate_sequence_json(
            json.dumps(self._full_definition(sequence))
        )
        return json.loads(normalized)

    def run(
        self,
        sequence: Mapping[str, Any],
        input: Mapping[str, Any] | None = None,
        *,
        max_ticks: int = 1000,
    ) -> NativeRunResult:
        raw = json.loads(
            self.module.run_sequence_json(
                json.dumps(self._full_definition(sequence)),
                json.dumps(dict(input or {})),
                max_ticks,
            )
        )
        state = raw.get("state")
        return NativeRunResult(
            state=str(state).lower() if state is not None else "unknown",
            context=raw.get("context") or {},
            outputs=list(raw.get("outputs") or []),
            ticks=int(raw.get("ticks") or 0),
            raw=raw,
        )
