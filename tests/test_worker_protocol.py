"""Lease-protocol behaviour of the worker and client (claim_epoch, poll hints)."""
from __future__ import annotations

import json
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
import respx

from orch8 import Orch8Client
from orch8.client import decode_worker_poll
from orch8.types import WorkerPollResponse, WorkerTask
from orch8.worker import Orch8Worker, current_task, execute_task

BASE = "http://engine.test"

TASK = {
    "id": "wt-1",
    "instance_id": "inst-1",
    "block_id": "b1",
    "handler_name": "h",
    "created_at": "2026-01-01T00:00:00Z",
    "claim_epoch": 7,
    "checkpoint_seq": 2,
    "resume_checkpoint": {"cursor": 4},
}


def test_decode_poll_batch_and_legacy_array() -> None:
    batch = decode_worker_poll(
        {"tasks": [TASK], "lease_secs": 60, "heartbeat_interval_secs": 20, "poll_after_ms": 0}
    )
    assert batch.tasks[0].claim_epoch == 7
    assert batch.lease_secs == 60
    legacy = decode_worker_poll([TASK])
    assert legacy.lease_secs is None and len(legacy.tasks) == 1
    with pytest.raises(TypeError):
        decode_worker_poll({"tasks": [], "lease_secs": 0})
    with pytest.raises(TypeError):
        decode_worker_poll({"nope": []})


@respx.mock
async def test_client_echoes_claim_epoch() -> None:
    poll = respx.post(f"{BASE}/workers/tasks/poll").mock(
        return_value=httpx.Response(200, json={"tasks": [TASK], "lease_secs": 30})
    )
    complete = respx.post(f"{BASE}/workers/tasks/wt-1/complete").mock(
        return_value=httpx.Response(204)
    )
    fail = respx.post(f"{BASE}/workers/tasks/wt-1/fail").mock(
        return_value=httpx.Response(204)
    )
    hb = respx.post(f"{BASE}/workers/tasks/wt-1/heartbeat").mock(
        return_value=httpx.Response(200, json={"checkpoint_seq": 1})
    )
    async with Orch8Client(BASE) as client:
        tasks = await client.poll_tasks("h", "w1", 5)
        assert tasks[0].claim_epoch == 7
        await client.complete_task("wt-1", "w1", {"ok": 1}, claim_epoch=7)
        await client.fail_task("wt-1", "w1", "x", claim_epoch=7)
        await client.heartbeat_task("wt-1", "w1", claim_epoch=7)
    assert json.loads(poll.calls[0].request.content)["limit"] == 5
    assert json.loads(complete.calls[0].request.content)["claim_epoch"] == 7
    assert json.loads(fail.calls[0].request.content)["claim_epoch"] == 7
    assert json.loads(hb.calls[0].request.content)["claim_epoch"] == 7


def _client() -> Any:
    client = MagicMock(spec=Orch8Client)
    client.complete_task = AsyncMock()
    client.fail_task = AsyncMock()
    client.heartbeat_task = AsyncMock(return_value={"checkpoint_seq": 3})
    return client


async def test_completion_rejection_is_not_reported_as_failure() -> None:
    client = _client()
    client.complete_task.side_effect = RuntimeError("409 lease changed")
    completed: list[Any] = []

    async def handler(task: WorkerTask) -> dict[str, bool]:
        return {"ok": True}

    outcome = await execute_task(
        client,
        "w1",
        handler,
        WorkerTask.model_validate(TASK),
        on_task_complete=lambda t, o: completed.append(o),
    )
    assert outcome == "unacknowledged"
    client.fail_task.assert_not_called()
    assert completed == []


async def test_missing_handler_fails_non_retryable() -> None:
    client = _client()
    outcome = await execute_task(client, "w1", None, WorkerTask.model_validate(TASK))
    assert outcome == "failed"
    assert client.fail_task.call_args.kwargs["retryable"] is False
    assert client.fail_task.call_args.kwargs["claim_epoch"] == 7


async def test_timeout_is_retryable() -> None:
    import asyncio

    client = _client()

    async def slow(task: WorkerTask) -> None:
        await asyncio.sleep(1)

    task = WorkerTask.model_validate({**TASK, "timeout_ms": 5})
    assert await execute_task(client, "w1", slow, task) == "failed"
    assert client.fail_task.call_args.kwargs["retryable"] is True


async def test_handler_checkpoint_uses_task_context() -> None:
    client = _client()
    seen: dict[str, Any] = {}

    async def handler(task: WorkerTask) -> dict[str, Any]:
        ctx = current_task()
        assert ctx is not None
        seen["resume"] = ctx.resume_checkpoint
        seen["prefix"] = ctx.idempotency_prefix
        seen["seq"] = await ctx.checkpoint({"cursor": 5})
        return {}

    await execute_task(client, "w1", handler, WorkerTask.model_validate(TASK))
    assert seen == {"resume": {"cursor": 4}, "prefix": "inst-1:b1", "seq": 3}
    kwargs = client.heartbeat_task.call_args.kwargs
    assert kwargs["checkpoint_seq"] == 3 and kwargs["claim_epoch"] == 7
    assert current_task() is None


async def test_worker_applies_server_hints() -> None:
    client = _client()
    client.poll_task_batch = AsyncMock(
        return_value=WorkerPollResponse(
            tasks=[], lease_secs=10, heartbeat_interval_secs=8, poll_after_ms=2500
        )
    )
    worker = Orch8Worker(client, "w1", {"h": AsyncMock()}, heartbeat_interval=15)
    await worker._poll_handler("h")
    assert worker._poll_hints["h"] == 2.5
    assert worker._effective_heartbeat() == 5


async def test_queue_worker_polls_named_queue() -> None:
    client = _client()
    client.poll_task_batch_from_queue = AsyncMock(return_value=WorkerPollResponse(tasks=[]))
    worker = Orch8Worker(client, "w1", {"h": AsyncMock()}, queue="gpu")
    await worker._poll_handler("h")
    client.poll_task_batch_from_queue.assert_awaited_once_with("gpu", "h", "w1", 10)
