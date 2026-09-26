"""Tests for orch8.testing (fake engine, native wrapper, pytest fixtures)."""
from __future__ import annotations

import asyncio
import json
from typing import Any

import httpx
import pytest

from orch8 import Orch8Client, Orch8Error, Orch8Worker, current_task, workflow
from orch8.testing import FakeEngine, NativeEnvironment
from orch8.types import WorkerTask

pytest_plugins = ["orch8.testing.pytest_plugin"]


class Retryable(Exception):
    retryable = True


async def test_sequence_runs_through_real_worker_path(orch8_engine: FakeEngine, orch8_client: Orch8Client) -> None:
    calls: list[str] = []

    async def charge(task: WorkerTask) -> dict[str, Any]:
        calls.append(task.block_id)
        return {"charge_id": "ch_1", "cents": task.params["cents"]}

    async def email(task: WorkerTask) -> dict[str, Any]:
        calls.append(task.block_id)
        assert task.context["data"]["charge_id"] == "ch_1"
        return {"sent": True}

    seq = (
        workflow("checkout")
        .step("charge", "charge", {"cents": 500})
        .step("wait", "noop", delay={"duration": 3 * 24 * 3600 * 1000})
        .step("email", "email")
        .build()
    )
    start = orch8_engine.clock.now()
    inst = await orch8_engine.run(orch8_client, seq, {"charge": charge, "email": email}, input={"user": "u1"})
    assert inst.state == "completed"
    assert calls == ["charge", "email"]
    assert inst.data == {"user": "u1", "charge_id": "ch_1", "cents": 500, "sent": True}
    # Three virtual days were skipped instantly.
    assert (orch8_engine.clock.now() - start).days == 3
    fetched = await orch8_client.get_instance(inst.id)
    assert fetched.state == "completed"
    assert [o.block_id for o in await orch8_client.get_outputs(inst.id)] == ["charge", "wait", "email"]


async def test_retryable_failures_back_off_then_fail(orch8_engine: FakeEngine, orch8_client: Orch8Client) -> None:
    attempts: list[int] = []

    async def flaky(task: WorkerTask) -> None:
        attempts.append(task.attempt)
        raise Retryable("upstream 503")

    seq = workflow("r").step(
        "call", "flaky", retry={"max_attempts": 3, "initial_backoff": 1000, "max_backoff": 60_000}
    ).build()
    inst = await orch8_engine.run(orch8_client, seq, {"flaky": flaky})
    assert attempts == [0, 1, 2]
    assert inst.state == "failed" and inst.error == "upstream 503"


async def test_lease_epoch_is_enforced_and_expired_leases_reclaimed(orch8_engine: FakeEngine, orch8_client: Orch8Client) -> None:
    task = orch8_engine.enqueue_task("h", {"x": 1})
    [claimed] = await orch8_client.poll_tasks("h", "w1")
    assert claimed.claim_epoch == 1
    with pytest.raises(Orch8Error) as info:
        await orch8_client.complete_task(task.id, "w1", {}, claim_epoch=99)
    assert info.value.status == 409
    orch8_engine.advance(orch8_engine.lease_secs + 1)
    assert orch8_engine.task(task.id).state == "pending"
    [again] = await orch8_client.poll_tasks("h", "w2")
    assert again.claim_epoch == 2
    await orch8_client.complete_task(task.id, "w2", {"ok": True}, claim_epoch=2)
    assert orch8_engine.task(task.id).output == {"ok": True}


async def test_checkpoint_survives_retry(orch8_engine: FakeEngine, orch8_client: Orch8Client) -> None:
    seen: list[Any] = []

    async def resumable(task: WorkerTask) -> dict[str, Any]:
        ctx = current_task()
        assert ctx is not None
        seen.append(ctx.resume_checkpoint)
        if ctx.resume_checkpoint is None:
            await ctx.checkpoint({"page": 3})
            raise Retryable("crash after page 3")
        return {"resumed_from": ctx.resume_checkpoint["page"]}

    seq = workflow("cp").step(
        "scan", "scan", retry={"max_attempts": 2, "initial_backoff": 10, "max_backoff": 10}
    ).build()
    inst = await orch8_engine.run(orch8_client, seq, {"scan": resumable})
    # Like the engine, the retry row carries the attempt's durable checkpoint.
    assert seen == [None, {"page": 3}]
    assert inst.state == "completed" and inst.data == {"resumed_from": 3}


async def test_polling_worker_against_loopback_server() -> None:
    engine = FakeEngine()
    task = engine.enqueue_task("greet", {"name": "Ada"}, queue_name="q")
    with engine.serve() as base_url:
        async with Orch8Client(base_url + "/api/v1", tenant_id="test") as client:
            async def greet(t: WorkerTask) -> dict[str, str]:
                return {"greeting": f"hi {t.params['name']}"}

            worker = Orch8Worker(client, "w1", {"greet": greet}, poll_interval=0.01, queue="q")
            runner = asyncio.create_task(worker.start())
            for _ in range(200):
                if engine.task(task.id).state == "completed":
                    break
                await asyncio.sleep(0.01)
            await worker.stop()
            await runner
    assert engine.task(task.id).output == {"greeting": "hi Ada"}


async def test_fake_jobs_api(orch8_engine: FakeEngine, orch8_client: Orch8Client) -> None:
    job = await orch8_client.jobs.enqueue("resize", {"w": 10}, delay_ms=60_000, idempotency_key="k1")
    same = await orch8_client.jobs.enqueue("resize", {"w": 10}, idempotency_key="k1")
    assert same.id == job.id and job.status == "scheduled"

    async def resize(task: WorkerTask) -> dict[str, int]:
        return {"w": task.params["w"] * 2}

    await orch8_engine.process(orch8_client, {"resize": resize})
    done = await orch8_client.jobs.wait_for(job.id, timeout=1)
    assert done.status == "completed" and done.output == {"w": 20}
    listed = [j.id async for j in orch8_client.jobs.list(status="completed", limit=1)]
    assert listed == [job.id]
    other = await orch8_client.jobs.enqueue("resize", {"w": 1})
    cancelled = await orch8_client.jobs.cancel(other.id)
    assert cancelled is not None and cancelled.status == "cancelled"


async def test_fake_rejects_semantics_it_cannot_model(orch8_client: Orch8Client) -> None:
    seq = workflow("x").parallel("p", lambda b: b.step("a", "h")).build()
    with pytest.raises(Orch8Error) as info:
        await orch8_client.create_sequence(seq)
    assert info.value.status == 422


class _StubNative:
    def __init__(self) -> None:
        self.calls: list[tuple[str, Any]] = []

    def sequence_schema_version(self) -> int:
        return 3

    def validate_sequence_json(self, raw: str) -> str:
        self.calls.append(("validate", json.loads(raw)))
        return raw

    def run_sequence_json(self, raw: str, input_json: str, max_ticks: int) -> str:
        self.calls.append(("run", (json.loads(raw), json.loads(input_json), max_ticks)))
        return json.dumps(
            {
                "state": "completed",
                "context": {"data": {"x": 1}},
                "outputs": [{"block_id": "s", "output": {"ok": True}}],
                "ticks": 4,
            }
        )


def test_native_environment_wraps_bindings() -> None:
    stub = _StubNative()
    env = NativeEnvironment(module=stub)
    result = env.run(workflow("n").step("s", "noop").build(), {"x": 1}, max_ticks=50)
    assert result.completed and result.data == {"x": 1} and result.output("s") == {"ok": True}
    sent, inp, ticks = stub.calls[0][1]
    assert sent["tenant_id"] == "test" and sent["version"] == 1 and inp == {"x": 1} and ticks == 50
    assert env.schema_version == 3


def test_native_run_on_real_engine(orch8_native: NativeEnvironment) -> None:
    seq = (
        workflow("native")
        .step("wait", "noop", delay={"duration": 7 * 24 * 3600 * 1000})
        .step("log", "log", {"message": "done"})
        .build()
    )
    result = orch8_native.run(seq)
    assert result.completed, result.raw
    assert any(o["block_id"] == "log" for o in result.outputs)


def test_mock_transport_is_plain_httpx() -> None:
    engine = FakeEngine()
    with httpx.Client(transport=engine.transport(), base_url="http://x") as http:
        assert http.get("/health").json() == {"status": "ok"}
        assert http.get("/nope").status_code == 404
