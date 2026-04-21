"""Integration tests — client + worker together using respx."""
from __future__ import annotations

import asyncio

import httpx
import pytest
import respx

from orch8 import Orch8Client, Orch8Worker
from orch8.types import WorkerTask

BASE = "http://orch8.test"

TS = "2025-01-01T00:00:00Z"


@pytest.fixture
def client() -> Orch8Client:
    return Orch8Client(base_url=BASE, tenant_id="t-1")


# ---------------------------------------------------------------------------
# Network failure modes
# ---------------------------------------------------------------------------


@respx.mock
async def test_network_timeout(client: Orch8Client) -> None:
    route = respx.get(f"{BASE}/sequences/seq-1").mock(
        side_effect=httpx.ConnectTimeout("connection timed out")
    )
    with pytest.raises(httpx.ConnectTimeout):
        await client.get_sequence("seq-1")
    assert route.called


@respx.mock
async def test_5xx_server_error_raises_orch8_error(client: Orch8Client) -> None:
    respx.get(f"{BASE}/sequences/seq-1").mock(
        return_value=httpx.Response(503, text="service unavailable")
    )
    from orch8 import Orch8Error

    with pytest.raises(Orch8Error) as exc_info:
        await client.get_sequence("seq-1")
    assert exc_info.value.status == 503


@respx.mock
async def test_empty_200_body_returns_none(client: Orch8Client) -> None:
    """Some PATCH/POST endpoints return 200 with empty body."""
    respx.patch(f"{BASE}/instances/inst-1/state").mock(
        return_value=httpx.Response(200, content=b"")
    )
    result = await client.update_instance_state("inst-1", "completed")
    assert result is None


@respx.mock
async def test_204_no_content_returns_none(client: Orch8Client) -> None:
    respx.delete(f"{BASE}/sequences/seq-1").mock(return_value=httpx.Response(204))
    result = await client.delete_sequence("seq-1")
    assert result is None


# ---------------------------------------------------------------------------
# Context manager
# ---------------------------------------------------------------------------


@respx.mock
async def test_context_manager_closes_client() -> None:
    async with Orch8Client(base_url=BASE, tenant_id="t-1") as client:
        respx.get(f"{BASE}/health/ready").mock(return_value=httpx.Response(200))
        result = await client.health()
        assert result.status == "ok"
    # Client is closed after exiting context manager.


# ---------------------------------------------------------------------------
# Custom headers
# ---------------------------------------------------------------------------


@respx.mock
async def test_custom_headers_sent(client: Orch8Client) -> None:
    client = Orch8Client(
        base_url=BASE, tenant_id="t-1", headers={"X-Custom": "value", "Authorization": "Bearer tok"}
    )
    route = respx.get(f"{BASE}/health/ready").mock(return_value=httpx.Response(200))
    await client.health()
    assert route.calls[0].request.headers["X-Custom"] == "value"
    assert route.calls[0].request.headers["Authorization"] == "Bearer tok"
    assert route.calls[0].request.headers["X-Tenant-Id"] == "t-1"


# ---------------------------------------------------------------------------
# Worker end-to-end
# ---------------------------------------------------------------------------


@respx.mock
async def test_worker_e2e_success() -> None:
    """Full worker lifecycle with mocked API."""
    client = Orch8Client(base_url=BASE, tenant_id="t-1")
    completed: list[str] = []

    async def handler(task: WorkerTask) -> dict[str, bool]:
        completed.append(task.id)
        return {"ok": True}

    worker = Orch8Worker(
        client=client,
        worker_id="w-1",
        handlers={"test-handler": handler},
        poll_interval=0.05,
        heartbeat_interval=0.05,
    )

    task_json = {
        "id": "wt-1",
        "instance_id": "inst-1",
        "block_id": "block-1",
        "handler_name": "test-handler",
        "created_at": TS,
    }

    # First poll returns a task, subsequent polls return empty.
    respx.post(f"{BASE}/workers/tasks/poll").mock(
        side_effect=[
            httpx.Response(200, json=[task_json]),
            httpx.Response(200, json=[]),
            httpx.Response(200, json=[]),
            httpx.Response(200, json=[]),
            httpx.Response(200, json=[]),
            httpx.Response(200, json=[]),
            httpx.Response(200, json=[]),
        ]
    )
    respx.post(f"{BASE}/workers/tasks/wt-1/complete").mock(
        return_value=httpx.Response(204)
    )
    respx.post(f"{BASE}/workers/tasks/wt-1/heartbeat").mock(
        return_value=httpx.Response(204)
    )

    async def stop_soon() -> None:
        await asyncio.sleep(0.3)
        await worker.stop()

    await asyncio.gather(worker.start(), stop_soon())
    assert "wt-1" in completed


@respx.mock
async def test_worker_e2e_handler_failure() -> None:
    """Worker reports failures to the API."""
    client = Orch8Client(base_url=BASE, tenant_id="t-1")

    async def handler(task: WorkerTask) -> None:
        raise RuntimeError("handler boom")

    worker = Orch8Worker(
        client=client,
        worker_id="w-1",
        handlers={"fail-handler": handler},
        poll_interval=0.05,
    )

    task_json = {
        "id": "wt-2",
        "instance_id": "inst-1",
        "block_id": "block-1",
        "handler_name": "fail-handler",
        "created_at": TS,
    }

    respx.post(f"{BASE}/workers/tasks/poll").mock(
        side_effect=[
            httpx.Response(200, json=[task_json]),
            httpx.Response(200, json=[]),
            httpx.Response(200, json=[]),
            httpx.Response(200, json=[]),
            httpx.Response(200, json=[]),
            httpx.Response(200, json=[]),
            httpx.Response(200, json=[]),
        ]
    )
    fail_route = respx.post(f"{BASE}/workers/tasks/wt-2/fail").mock(
        return_value=httpx.Response(204)
    )

    async def stop_soon() -> None:
        await asyncio.sleep(0.3)
        await worker.stop()

    await asyncio.gather(worker.start(), stop_soon())
    assert fail_route.called
