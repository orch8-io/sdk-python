"""Durable LangGraph / OpenAI Agents adapters, tested with mocks and the fake engine."""
from __future__ import annotations

import dataclasses
import json
from types import SimpleNamespace
from typing import Any

import pytest

from orch8 import workflow
from orch8.ai import PortableStateError, to_portable, turn_loop
from orch8.ai.langgraph import langgraph_turn_handler
from orch8.ai.openai_agents import (
    ToolJournal,
    durable_tool,
    openai_agent_turn_handler,
    tool_idempotency_key,
)
from orch8.testing import FakeEngine
from orch8.types import WorkerTask


def _task(data: dict[str, Any] | None = None, params: dict[str, Any] | None = None, **extra: Any) -> WorkerTask:
    return WorkerTask(
        id="wt-1",
        instance_id="inst-1",
        block_id="chat_turn",
        handler_name="turn",
        params=params or {},
        context={"data": data or {}},
        created_at="2026-01-01T00:00:00Z",
        **extra,
    )


# --------------------------------------------------------------------------- #
# LangGraph
# --------------------------------------------------------------------------- #


class CounterGraph:
    """Mimics a compiled LangGraph: returns the next state."""

    def __init__(self) -> None:
        self.calls: list[tuple[dict[str, Any], dict[str, Any]]] = []

    async def ainvoke(self, state: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
        self.calls.append((state, config))
        messages = [*state.get("messages", []), {"role": "user", "content": state["question"]}]
        return {
            "messages": [*messages, {"role": "assistant", "content": f"answer {len(messages)}"}],
            "question": state["question"],
            "client": object(),  # non-portable runtime handle
            "resolved": len(messages) >= 3,
        }


async def test_langgraph_turn_restores_filters_and_checkpoints() -> None:
    graph = CounterGraph()
    handle = langgraph_turn_handler(
        graph,
        state_key="support",
        portable_keys={"messages", "question"},
        is_done=lambda s: s["resolved"],
    )
    out = await handle(_task(params={"input": {"question": "hi"}}))
    assert out["support"]["messages"][-1]["content"] == "answer 1"
    assert "client" not in out["support"]
    assert out["support_turn"] == 1 and out["agent_done"] is False
    assert graph.calls[0][1] == {"configurable": {"thread_id": "inst-1"}}

    second = await handle(_task(data=out, params={"input": {"question": "again"}}))
    restored_state = graph.calls[1][0]
    assert len(restored_state["messages"]) == 2 and restored_state["question"] == "again"
    assert second["support_turn"] == 2
    assert json.loads(json.dumps(second)) == second  # plain JSON only


async def test_langgraph_rejects_non_portable_state() -> None:
    handle = langgraph_turn_handler(CounterGraph())  # no allowlist: "client" leaks
    with pytest.raises(PortableStateError, match="client"):
        await handle(_task(params={"input": {"question": "hi"}}))


async def test_langgraph_turns_run_durably_on_fake_engine() -> None:
    engine = FakeEngine()
    client = engine.client()
    handle = langgraph_turn_handler(CounterGraph(), portable_keys={"messages", "question"})
    seq = workflow("chat").step("t1", "turn", {"input": {"question": "q1"}}).step(
        "t2", "turn", {"input": {"question": "q2"}}
    ).build()
    inst = await engine.run(client, seq, {"turn": handle})
    assert inst.state == "completed"
    assert [m["content"] for m in inst.data["agent_state"]["messages"]] == [
        "q1", "answer 1", "q2", "answer 3",
    ]
    assert inst.data["agent_state_turn"] == 2


def test_turn_loop_emits_bounded_loop() -> None:
    seq = turn_loop(workflow("agent"), "chat", "turn", max_turns=8).build()
    loop = seq["blocks"][0]
    assert loop["type"] == "loop" and loop["max_iterations"] == 8
    assert loop["condition"] == "data.agent_done != true"
    assert loop["retain_iterations"] == 5 and loop["body"][0]["handler"] == "turn"


def test_to_portable_converts_models_and_dataclasses() -> None:
    from pydantic import BaseModel

    class M(BaseModel):
        x: int

    @dataclasses.dataclass
    class D:
        y: str

    assert to_portable({"m": M(x=1), "d": D(y="a"), "l": (1, 2)}) == {
        "m": {"x": 1}, "d": {"y": "a"}, "l": [1, 2],
    }


def test_optional_real_langgraph() -> None:
    graph_mod = pytest.importorskip("langgraph.graph")
    from typing_extensions import TypedDict

    class State(TypedDict, total=False):
        count: int

    builder = graph_mod.StateGraph(State)
    builder.add_node("inc", lambda s: {"count": s.get("count", 0) + 1})
    builder.add_edge(graph_mod.START, "inc")
    builder.add_edge("inc", graph_mod.END)
    import asyncio

    handle = langgraph_turn_handler(builder.compile(), input_key=None)
    out = asyncio.run(handle(_task(data={"agent_state": {"count": 4}})))
    assert out["agent_state"] == {"count": 5}


# --------------------------------------------------------------------------- #
# OpenAI Agents
# --------------------------------------------------------------------------- #


@dataclasses.dataclass
class FakeFunctionTool:
    name: str
    on_invoke_tool: Any


@dataclasses.dataclass
class FakeAgent:
    name: str
    tools: list[Any]

    def clone(self, **kwargs: Any) -> FakeAgent:
        return dataclasses.replace(self, **kwargs)


class FakeResult:
    def __init__(self, items: list[Any], output: Any) -> None:
        self._items = items
        self.final_output = output

    def to_input_list(self) -> list[Any]:
        return self._items


class ScriptedRunner:
    """Replays a model that calls ``charge`` with a fixed call id."""

    def __init__(self, call_id: str = "call_1", fail_after_tool: bool = False) -> None:
        self.call_id = call_id
        self.fail_after_tool = fail_after_tool
        self.agents: list[Any] = []

    async def run(self, agent: Any, history: list[Any], **kwargs: Any) -> FakeResult:
        self.agents.append(agent)
        ctx = SimpleNamespace(tool_call_id=self.call_id, tool_name="charge")
        receipt = await agent.tools[0].on_invoke_tool(ctx, '{"cents": 500}')
        if self.fail_after_tool:
            raise RuntimeError("model API timed out after the tool ran")
        items = [*history, {"type": "function_call_output", "call_id": self.call_id, "output": receipt}]
        return FakeResult(items, {"receipt": receipt})


def _charging_agent(executions: list[str]) -> FakeAgent:
    async def charge(ctx: Any, arguments: str) -> str:
        executions.append(tool_idempotency_key() or "")
        return f"rcpt-{json.loads(arguments)['cents']}"

    return FakeAgent("billing", [FakeFunctionTool("charge", charge), "hosted-web-search"])


async def test_tool_calls_are_journaled_by_call_id_across_retries() -> None:
    engine = FakeEngine()
    client = engine.client()
    executions: list[str] = []
    agent = _charging_agent(executions)
    flaky = ScriptedRunner(fail_after_tool=True)
    attempts = {"n": 0}

    async def turn(task: WorkerTask) -> Any:
        attempts["n"] += 1
        runner = flaky if attempts["n"] == 1 else ScriptedRunner()
        return await openai_agent_turn_handler(agent, runner=runner)(task)

    seq = workflow("bill").step(
        "pay", "turn", {"input": "charge me"},
        retry={"max_attempts": 2, "initial_backoff": 100, "max_backoff": 100},
    ).build()

    class Retryable(Exception):
        retryable = True

    async def retrying_turn(task: WorkerTask) -> Any:
        try:
            return await turn(task)
        except RuntimeError as exc:
            raise Retryable(str(exc)) from exc

    inst = await engine.run(client, seq, {"turn": retrying_turn})
    assert inst.state == "completed"
    # The tool ran once; the retried turn reused the journaled result.
    assert len(executions) == 1
    assert executions[0].endswith(":pay:call_1")
    assert inst.data["final_output"] == {"receipt": "rcpt-500"}
    assert inst.data["agent_history"][0] == {"role": "user", "content": "charge me"}
    # Hosted tools pass through untouched.
    assert flaky.agents[0].tools[1] == "hosted-web-search"


async def test_argument_match_covers_regenerated_call_ids() -> None:
    journal = ToolJournal()
    calls: list[str] = []

    async def invoke(ctx: Any, arguments: str) -> dict[str, int]:
        calls.append(ctx.tool_call_id)
        return {"ok": 1}

    tool = durable_tool(FakeFunctionTool("t", invoke), journal)
    await tool.on_invoke_tool(SimpleNamespace(tool_call_id="a"), '{"x": 1, "y": 2}')
    await tool.on_invoke_tool(SimpleNamespace(tool_call_id="b"), '{"y": 2, "x": 1}')
    assert calls == ["a"]
    strict = durable_tool(FakeFunctionTool("t", invoke), ToolJournal(match_arguments=False))
    await strict.on_invoke_tool(SimpleNamespace(tool_call_id="a"), "{}")
    await strict.on_invoke_tool(SimpleNamespace(tool_call_id="b"), "{}")
    assert calls == ["a", "a", "b"]


async def test_optional_real_function_tool_wrapping() -> None:
    agents = pytest.importorskip("agents")
    from agents.tool_context import ToolContext

    @agents.function_tool
    def add(a: int, b: int) -> int:
        """Add numbers."""
        return a + b

    journal = ToolJournal()
    wrapped = durable_tool(add, journal)
    assert isinstance(wrapped, agents.FunctionTool) and wrapped.name == "add"
    ctx = ToolContext(context=None, tool_name="add", tool_call_id="c1", tool_arguments='{"a":1,"b":2}')
    assert int(await wrapped.on_invoke_tool(ctx, '{"a":1,"b":2}')) == 3
    assert journal.lookup("c1", "add", "{}")[0]
