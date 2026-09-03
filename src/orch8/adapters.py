"""Framework-neutral durable worker adapters."""

from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import Any


def durable_agent_handler(runner: Any) -> Callable[[Any], Any]:
    """Adapt LangGraph/CrewAI/AutoGen-style runners to an Orch8 handler."""

    async def handle(task: Any) -> Any:
        params = task.params if hasattr(task, "params") else task["params"]
        instance_id = getattr(task, "instance_id", None) or (
            task.get("instance_id") if isinstance(task, dict) else None
        )
        config = {"configurable": {"thread_id": instance_id}} if instance_id else None
        if callable(getattr(runner, "ainvoke", None)):
            return await runner.ainvoke(params, config)
        for name in ("invoke", "kickoff", "run"):
            method = getattr(runner, name, None)
            if callable(method):
                result = method(params, config) if name == "invoke" else method(params)
                return await result if inspect.isawaitable(result) else result
        raise TypeError("agent runner must implement ainvoke, invoke, kickoff, or run")

    return handle
