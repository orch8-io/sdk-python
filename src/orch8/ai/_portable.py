from __future__ import annotations

import dataclasses
import json
from collections.abc import Callable, Mapping
from typing import Any

from ..builder import WorkflowBuilder


class PortableStateError(TypeError):
    """Raised when agent state cannot be stored as portable JSON."""


def to_portable(value: Any, path: str = "state") -> Any:
    """Convert ``value`` to plain JSON types or raise :class:`PortableStateError`.

    Pydantic models (``model_dump``), dataclasses and objects exposing
    ``to_dict``/``dict`` are converted; anything else must already be JSON.
    """
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, Mapping):
        out = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise PortableStateError(f"{path} has a non-string key {key!r}")
            out[key] = to_portable(item, f"{path}.{key}")
        return out
    if isinstance(value, (list, tuple)):
        return [to_portable(item, f"{path}[{i}]") for i, item in enumerate(value)]
    dump = getattr(value, "model_dump", None)
    if callable(dump):
        try:
            return to_portable(dump(mode="json"), path)
        except TypeError:
            return to_portable(dump(), path)
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return to_portable(dataclasses.asdict(value), path)
    for name in ("to_dict", "dict"):
        method = getattr(value, name, None)
        if callable(method):
            return to_portable(method(), path)
    raise PortableStateError(
        f"{path} holds a non-portable {type(value).__name__}; exclude it via "
        "portable_keys or pass a serialize= hook"
    )


def json_size(value: Any) -> int:
    return len(json.dumps(value, separators=(",", ":")))


def select_keys(state: Mapping[str, Any], keys: frozenset[str] | None) -> dict[str, Any]:
    if keys is None:
        return dict(state)
    return {k: v for k, v in state.items() if k in keys}


def turn_loop(
    builder: WorkflowBuilder,
    id: str,
    handler: str,
    *,
    done_key: str = "agent_done",
    max_turns: int = 20,
    retain_iterations: int | None = 5,
    params: Mapping[str, Any] | None = None,
    retry: Mapping[str, Any] | None = None,
    timeout: int | None = None,
) -> WorkflowBuilder:
    """Append a loop that runs one agent turn per iteration until the turn
    handler returns ``{done_key: true}`` or ``max_turns`` is reached.
    Each iteration is a separate durable step, i.e. a turn-boundary checkpoint.
    """
    step_options: dict[str, Any] = {}
    if retry is not None:
        step_options["retry"] = dict(retry)
    if timeout is not None:
        step_options["timeout"] = timeout

    def body(b: WorkflowBuilder) -> None:
        b.step(f"{id}_turn", handler, dict(params or {}), **step_options)

    return builder.loop(
        id,
        f"data.{done_key} != true",
        body,
        max_iterations=max_turns,
        retain_iterations=retain_iterations,
    )


StateHook = Callable[[Any], Any]
