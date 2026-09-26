"""Durable OpenAI Agents SDK adapter: tool calls as idempotent, journaled steps.

::

    handlers = {"assistant_turn": openai_agent_turn_handler(agent)}
    seq = turn_loop(workflow("assistant"), "chat", "assistant_turn").build()

Each Orch8 task runs one ``Runner.run`` turn over the conversation history
stored (as portable JSON input items) in ``context.data[history_key]``.
Every function tool is wrapped so that a call is executed at most once per
step: results are journaled by the model's ``tool_call_id`` in the task's
durable checkpoint (heartbeat ``checkpoint``), which the engine carries onto
retries and lease reclaims. When a retried turn re-issues the same call —
same id, or with ``match_arguments`` the same tool name and arguments — the
journaled result is returned instead of running the tool again. Tools with
external effects should pass :func:`tool_idempotency_key` to the provider so
that even an interrupted first execution is deduplicated downstream.
"""
from __future__ import annotations

import contextvars
import dataclasses
import hashlib
import json
from collections.abc import Callable, Iterable, Mapping
from typing import Any

from ..types import WorkerTask
from ..worker import TaskContext, current_task
from ._portable import to_portable

__all__ = [
    "ToolJournal",
    "durable_tool",
    "durable_tools",
    "openai_agent_turn_handler",
    "tool_idempotency_key",
]

_JOURNAL_KEY = "orch8_tool_journal"
_idempotency: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "orch8_tool_idempotency_key", default=None
)


def tool_idempotency_key() -> str | None:
    """Stable key for the tool call currently executing:
    ``{instance_id}:{block_id}:{tool_call_id}``."""
    return _idempotency.get()


def _args_key(tool_name: str, arguments: str) -> str:
    try:
        canonical = json.dumps(json.loads(arguments or "{}"), sort_keys=True, separators=(",", ":"))
    except ValueError:
        canonical = arguments
    digest = hashlib.sha256(f"{tool_name}\0{canonical}".encode()).hexdigest()
    return f"args:{digest}"


class ToolJournal:
    """Per-task record of completed tool calls, persisted as a checkpoint."""

    def __init__(
        self,
        entries: Mapping[str, Any] | None = None,
        *,
        task: TaskContext | None = None,
        match_arguments: bool = True,
        extra_state: Mapping[str, Any] | None = None,
    ) -> None:
        self.entries: dict[str, Any] = dict(entries or {})
        self.task = task
        self.match_arguments = match_arguments
        self.extra_state = dict(extra_state or {})

    @classmethod
    def for_current_task(cls, *, match_arguments: bool = True) -> ToolJournal:
        ctx = current_task()
        checkpoint = ctx.resume_checkpoint if ctx else None
        entries = {}
        extra: dict[str, Any] = {}
        if isinstance(checkpoint, dict):
            entries = checkpoint.get(_JOURNAL_KEY) or {}
            extra = {k: v for k, v in checkpoint.items() if k != _JOURNAL_KEY}
        return cls(entries, task=ctx, match_arguments=match_arguments, extra_state=extra)

    def lookup(self, call_id: str | None, tool_name: str, arguments: str) -> tuple[bool, Any]:
        if call_id and call_id in self.entries:
            return True, self.entries[call_id]
        if self.match_arguments:
            key = _args_key(tool_name, arguments)
            if key in self.entries:
                return True, self.entries[key]
        return False, None

    async def record(self, call_id: str | None, tool_name: str, arguments: str, result: Any) -> None:
        portable = to_portable(result, f"tool {tool_name} result")
        if call_id:
            self.entries[call_id] = portable
        if self.match_arguments:
            self.entries[_args_key(tool_name, arguments)] = portable
        if self.task is not None:
            await self.task.checkpoint({**self.extra_state, _JOURNAL_KEY: self.entries})


def durable_tool(tool: Any, journal: ToolJournal) -> Any:
    """Wrap an Agents SDK ``FunctionTool`` so each call runs at most once."""
    invoke = getattr(tool, "on_invoke_tool", None)
    if not callable(invoke):
        return tool  # hosted tools (web search, file search, ...) run server-side
    tool_name = getattr(tool, "name", "tool")

    async def on_invoke_tool(ctx: Any, arguments: str) -> Any:
        call_id = getattr(ctx, "tool_call_id", None)
        hit, cached = journal.lookup(call_id, tool_name, arguments)
        if hit:
            return cached
        prefix = journal.task.idempotency_prefix if journal.task else "local"
        token = _idempotency.set(f"{prefix}:{call_id or _args_key(tool_name, arguments)}")
        try:
            result = await invoke(ctx, arguments)
        finally:
            _idempotency.reset(token)
        await journal.record(call_id, tool_name, arguments, result)
        return result

    if dataclasses.is_dataclass(tool) and not isinstance(tool, type):
        return dataclasses.replace(tool, on_invoke_tool=on_invoke_tool)
    import copy

    wrapped = copy.copy(tool)
    wrapped.on_invoke_tool = on_invoke_tool
    return wrapped


def durable_tools(tools: Iterable[Any], journal: ToolJournal) -> list[Any]:
    return [durable_tool(tool, journal) for tool in tools]


def _with_tools(agent: Any, tools: list[Any]) -> Any:
    clone = getattr(agent, "clone", None)
    if callable(clone):
        return clone(tools=tools)
    if dataclasses.is_dataclass(agent) and not isinstance(agent, type):
        return dataclasses.replace(agent, tools=tools)
    import copy

    copied = copy.copy(agent)
    copied.tools = tools
    return copied


def _default_runner() -> Any:
    from agents import Runner  # openai-agents

    return Runner


def openai_agent_turn_handler(
    agent: Any,
    *,
    runner: Any = None,
    history_key: str = "agent_history",
    input_key: str = "input",
    done_key: str = "agent_done",
    is_done: Callable[[Any], bool] | None = None,
    max_turns: int | None = None,
    context_factory: Callable[[WorkerTask], Any] | None = None,
    match_arguments: bool = True,
) -> Callable[[WorkerTask], Any]:
    """Return an Orch8 handler running one agent turn with durable tools.

    ``params[input_key]`` (a string or list of input items) is appended to the
    stored history. The step output is ``{history_key: [...], "final_output":
    ..., done_key: bool}``; ``is_done`` receives the run result.
    """

    async def handle(task: WorkerTask) -> dict[str, Any]:
        context = task.context if isinstance(task.context, dict) else {}
        data = context.get("data") if isinstance(context.get("data"), dict) else {}
        history = list(data.get(history_key) or [])
        params = task.params if isinstance(task.params, dict) else {}
        new_input = params.get(input_key)
        if isinstance(new_input, str):
            history.append({"role": "user", "content": new_input})
        elif isinstance(new_input, list):
            history.extend(new_input)
        elif new_input is not None:
            raise TypeError(f"params.{input_key} must be a string or a list of input items")

        journal = ToolJournal.for_current_task(match_arguments=match_arguments)
        durable_agent = _with_tools(agent, durable_tools(getattr(agent, "tools", []) or [], journal))
        run = (runner or _default_runner()).run
        kwargs: dict[str, Any] = {}
        if context_factory is not None:
            kwargs["context"] = context_factory(task)
        if max_turns is not None:
            kwargs["max_turns"] = max_turns
        result = await run(durable_agent, history, **kwargs)
        return {
            history_key: to_portable(result.to_input_list(), f"context.data.{history_key}"),
            "final_output": to_portable(result.final_output, "final_output"),
            done_key: bool(is_done(result)) if is_done else False,
        }

    return handle
