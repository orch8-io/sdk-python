import json
from datetime import datetime, timezone

import httpx
import pytest
import respx

from orch8 import JobTimeoutError, Orch8Client

BASE = "http://engine.test"


def job(status: str = "scheduled", **extra: object) -> dict:
    return {
        "id": "job-1",
        "instance_id": "inst-1",
        "handler": "send_email",
        "status": status,
        "created_at": "2026-09-01T00:00:00Z",
        "run_at": "2026-09-01T00:00:00Z",
        **extra,
    }


@respx.mock
async def test_enqueue_sends_contract_body() -> None:
    route = respx.post(f"{BASE}/jobs").mock(return_value=httpx.Response(201, json=job()))
    async with Orch8Client(BASE) as client:
        created = await client.jobs.enqueue(
            "send_email",
            {"to": "a@b.c"},
            queue="mail",
            priority=5,
            retry={"max_attempts": 3, "initial_backoff_ms": 100},
            run_at=datetime(2026, 9, 1, 12, tzinfo=timezone.utc),
            idempotency_key="welcome:a@b.c",
            metadata={"source": "signup"},
        )
    assert created.id == "job-1" and created.status == "scheduled"
    assert json.loads(route.calls[0].request.content) == {
        "handler": "send_email",
        "payload": {"to": "a@b.c"},
        "queue": "mail",
        "priority": 5,
        "retry": {"max_attempts": 3, "initial_backoff_ms": 100},
        "run_at": "2026-09-01T12:00:00Z",
        "idempotency_key": "welcome:a@b.c",
        "metadata": {"source": "signup"},
    }


async def test_enqueue_rejects_delay_and_run_at() -> None:
    async with Orch8Client(BASE) as client:
        with pytest.raises(ValueError):
            await client.jobs.enqueue("h", delay_ms=1, run_at="2026-01-01T00:00:00Z")


@respx.mock
async def test_list_follows_cursors() -> None:
    route = respx.get(f"{BASE}/jobs").mock(
        side_effect=[
            httpx.Response(200, json={"jobs": [job()], "next_cursor": "c2"}),
            httpx.Response(200, json={"jobs": [job("completed", id="job-2")], "next_cursor": None}),
        ]
    )
    async with Orch8Client(BASE) as client:
        ids = [j.id async for j in client.jobs.list(handler="send_email", status="scheduled", limit=1)]
    assert ids == ["job-1", "job-2"]
    assert route.calls[0].request.url.params["status"] == "scheduled"
    assert route.calls[1].request.url.params["cursor"] == "c2"


@respx.mock
async def test_get_cancel_and_wait_for() -> None:
    respx.get(f"{BASE}/jobs/job-1").mock(
        side_effect=[
            httpx.Response(200, json=job("running", attempts=[{"attempt": 1, "status": "running"}])),
            httpx.Response(200, json=job("completed", output={"ok": True}, attempts=1)),
        ]
    )
    cancel = respx.delete(f"{BASE}/jobs/job-1").mock(
        return_value=httpx.Response(200, json=job("cancelled"))
    )
    async with Orch8Client(BASE) as client:
        done = await client.jobs.wait_for("job-1", poll_interval=0.001)
        assert done.status == "completed" and done.output == {"ok": True}
        cancelled = await client.jobs.cancel("job-1")
    assert cancelled is not None and cancelled.status == "cancelled"
    assert cancel.called


@respx.mock
async def test_wait_for_times_out() -> None:
    respx.get(f"{BASE}/jobs/job-1").mock(return_value=httpx.Response(200, json=job("running")))
    async with Orch8Client(BASE) as client:
        with pytest.raises(JobTimeoutError) as info:
            await client.jobs.wait_for("job-1", timeout=0.02, poll_interval=0.005)
    assert info.value.job.status == "running"
