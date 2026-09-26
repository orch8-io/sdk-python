"""Durable LangGraph adapter: one graph turn per Orch8 worker task.

::

    graph = builder.compile()           # no checkpointer needed
    handlers = {"support_turn": langgraph_turn_handler(
        graph, state_key="support", portable_keys={"messages", "ticket"},
        is_done=lambda s: s.get("resolved", False),
    )}

    seq = turn_loop(workflow("support"), "chat", "support_turn").build()

Orch8, not the LangGraph checkpointer, is the durable store: the handler
restores the previous turn's portable state from ``context.data[state_key]``,
invokes the graph once, and returns the allowlisted state (checked to be
plain JSON) as the step output, which the engine merges into
``context.data``. That output is the turn-boundary checkpoint; a crashed or
retried turn restarts from the last completed turn. ``thread_id`` is pinned
to the Orch8 instance so graph-internal memory stays per workflow.
"""
from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from typing import Any

from ..types import WorkerTask
from ._portable import PortableStateError, json_size, select_keys, to_portable

__all__ = ["langgraph_turn_handler", "PortableStateError"]


def _data(task: WorkerTask) -> dict[str, Any]:
    context = task.context if isinstance(task.context, dict) else {}
    data = context.get("data", {})
    return data if isinstance(data, dict) else {}


def langgraph_turn_handler(
    graph: Any,
    *,
    state_key: str = "agent_state",
    portable_keys: Iterable[str] | None = None,
    input_key: str | None = "input",
    done_key: str = "agent_done",
    is_done: Callable[[Mapping[str, Any]], bool] | None = None,
    serialize: Callable[[Mapping[str, Any]], Mapping[str, Any]] | None = None,
    deserialize: Callable[[Mapping[str, Any]], Mapping[str, Any]] | None = None,
    build_input: Callable[[Mapping[str, Any], Any], Mapping[str, Any]] | None = None,
    config: Mapping[str, Any] | None = None,
    max_state_bytes: int = 256 * 1024,
) -> Callable[[WorkerTask], Any]:
    """Return an Orch8 handler that runs one LangGraph turn.

    Parameters
    ----------
    graph:
        A compiled graph (anything with ``ainvoke`` or ``invoke``).
    state_key:
        ``context.data`` key holding the portable state between turns.
    portable_keys:
        Allowlist of state keys to persist; others (clients, caches) are
        dropped. ``None`` persists every key, all of which must be portable.
    input_key:
        Step ``params`` key merged into the state as this turn's input; with
        ``None`` the whole ``params`` mapping is merged.
    is_done:
        Predicate on the new state; its result is returned as ``done_key``
        for :func:`orch8.ai.turn_loop`.
    serialize / deserialize:
        Hooks converting between graph state and portable JSON (e.g. for
        LangChain message objects).
    build_input:
        Custom ``(restored_state, turn_input) -> graph_input``.
    """
    keys = frozenset(portable_keys) if portable_keys is not None else None

    async def handle(task: WorkerTask) -> dict[str, Any]:
        data = _data(task)
        stored = data.get(state_key) or {}
        if not isinstance(stored, dict):
            raise PortableStateError(f"context.data.{state_key} must be an object")
        restored = dict(deserialize(stored)) if deserialize else dict(stored)
        params = task.params if isinstance(task.params, dict) else {}
        turn_input = params.get(input_key) if input_key is not None else params
        if build_input is not None:
            graph_input = dict(build_input(restored, turn_input))
        elif isinstance(turn_input, Mapping):
            graph_input = {**restored, **turn_input}
        elif turn_input is None:
            graph_input = restored
        else:
            raise TypeError(
                f"params.{input_key} must be an object merged into the graph state; "
                "pass build_input= to map other shapes"
            )

        run_config: dict[str, Any] = dict(config or {})
        configurable = dict(run_config.get("configurable") or {})
        configurable.setdefault("thread_id", task.instance_id)
        run_config["configurable"] = configurable

        if callable(getattr(graph, "ainvoke", None)):
            result = await graph.ainvoke(graph_input, run_config)
        elif callable(getattr(graph, "invoke", None)):
            result = graph.invoke(graph_input, run_config)
        else:
            raise TypeError("graph must implement ainvoke or invoke")
        if not isinstance(result, Mapping):
            raise PortableStateError("graph turn must return a state mapping")

        state = dict(serialize(result)) if serialize else dict(result)
        portable = to_portable(select_keys(state, keys), f"context.data.{state_key}")
        size = json_size(portable)
        if size > max_state_bytes:
            raise PortableStateError(
                f"portable state is {size} bytes (limit {max_state_bytes}); "
                "store large values as artifacts and keep references in state"
            )
        turn = data.get(f"{state_key}_turn", 0)
        return {
            state_key: portable,
            f"{state_key}_turn": (turn if isinstance(turn, int) else 0) + 1,
            done_key: bool(is_done(result)) if is_done else False,
        }

    return handle
