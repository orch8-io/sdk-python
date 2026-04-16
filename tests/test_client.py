"""Tests for Orch8Client using respx to mock httpx."""
from __future__ import annotations

import httpx
import pytest
import respx

from orch8 import Orch8Client, Orch8Error
from orch8.types import FireTriggerResponse, HealthResponse, SequenceDefinition


BASE = "http://orch8.test"


@pytest.fixture
def client() -> Orch8Client:
    return Orch8Client(base_url=BASE, tenant_id="t-1")


# --- health ---


@respx.mock
async def test_health_ok(client: Orch8Client) -> None:
    respx.get(f"{BASE}/health/ready").mock(return_value=httpx.Response(200))
    result = await client.health()
    assert isinstance(result, HealthResponse)
    assert result.status == "ok"


@respx.mock
async def test_health_unavailable(client: Orch8Client) -> None:
    respx.get(f"{BASE}/health/ready").mock(return_value=httpx.Response(503))
    result = await client.health()
    assert result.status == "unavailable"


# --- tenant header ---


@respx.mock
async def test_tenant_header_sent(client: Orch8Client) -> None:
    route = respx.get(f"{BASE}/health/ready").mock(
        return_value=httpx.Response(200)
    )
    await client.health()
    assert route.calls[0].request.headers["X-Tenant-Id"] == "t-1"


# --- create_sequence ---


@respx.mock
async def test_create_sequence(client: Orch8Client) -> None:
    seq_payload = {
        "name": "my-seq",
        "blocks": [],
        "tenant_id": "t-1",
        "namespace": "default",
    }
    respx.post(f"{BASE}/sequences").mock(
        return_value=httpx.Response(201, json={
            "id": "seq-1",
            "tenant_id": "t-1",
            "namespace": "default",
            "name": "my-seq",
            "version": 1,
            "deprecated": False,
            "blocks": [],
            "created_at": "2025-01-01T00:00:00Z",
        })
    )
    result = await client.create_sequence(seq_payload)
    assert isinstance(result, SequenceDefinition)
    assert result.id == "seq-1"
    assert result.name == "my-seq"


# --- fire_trigger ---


@respx.mock
async def test_fire_trigger(client: Orch8Client) -> None:
    respx.post(f"{BASE}/triggers/my-hook/fire").mock(
        return_value=httpx.Response(
            201,
            json={
                "instance_id": "inst-42",
                "trigger": "my-hook",
                "sequence_name": "onboard",
            },
        )
    )
    result = await client.fire_trigger("my-hook", {"key": "value"})
    assert isinstance(result, FireTriggerResponse)
    assert result.instance_id == "inst-42"


# --- error handling ---


@respx.mock
async def test_404_raises_orch8_error(client: Orch8Client) -> None:
    respx.get(f"{BASE}/sequences/nope").mock(
        return_value=httpx.Response(404, text="not found")
    )
    with pytest.raises(Orch8Error) as exc_info:
        await client.get_sequence("nope")
    assert exc_info.value.status == 404
    assert "not found" in exc_info.value.body


# --- delete returns None ---


@respx.mock
async def test_delete_cron_returns_none(client: Orch8Client) -> None:
    respx.delete(f"{BASE}/cron/c-1").mock(
        return_value=httpx.Response(204)
    )
    result = await client.delete_cron("c-1")
    assert result is None
