"""Tests for Orch8Client using respx to mock httpx."""
from __future__ import annotations

import httpx
import pytest
import respx

from orch8 import Orch8Client, Orch8Error
from orch8.types import (
    AuditEntry,
    BatchCreateResponse,
    BulkResponse,
    Checkpoint,
    CircuitBreaker,
    ClusterNode,
    CronSchedule,
    ExecutionNode,
    FireTriggerResponse,
    HealthResponse,
    PluginDef,
    SequenceDefinition,
    Session,
    StepOutput,
    TaskInstance,
    TriggerDef,
    WorkerTask,
)

BASE = "http://orch8.test"

# ---------------------------------------------------------------------------
# Shared minimal payloads
# ---------------------------------------------------------------------------

TS = "2025-01-01T00:00:00Z"

SEQ_JSON = {
    "id": "seq-1",
    "tenant_id": "t-1",
    "namespace": "default",
    "name": "my-seq",
    "version": 1,
    "created_at": TS,
}

INSTANCE_JSON = {
    "id": "inst-1",
    "sequence_id": "seq-1",
    "tenant_id": "t-1",
    "namespace": "default",
    "state": "pending",
    "created_at": TS,
    "updated_at": TS,
}

CRON_JSON = {
    "id": "cron-1",
    "tenant_id": "t-1",
    "namespace": "default",
    "sequence_id": "seq-1",
    "cron_expr": "0 * * * *",
    "created_at": TS,
    "updated_at": TS,
}

TRIGGER_JSON = {
    "slug": "my-hook",
    "sequence_name": "onboard",
    "tenant_id": "t-1",
    "namespace": "default",
    "created_at": TS,
    "updated_at": TS,
}

PLUGIN_JSON = {
    "name": "my-plugin",
    "plugin_type": "handler",
    "source": "https://example.com/plugin",
    "tenant_id": "t-1",
    "created_at": TS,
    "updated_at": TS,
}

SESSION_JSON = {
    "id": "sess-1",
    "tenant_id": "t-1",
    "session_key": "user-123",
    "created_at": TS,
    "updated_at": TS,
}

CHECKPOINT_JSON = {
    "id": "cp-1",
    "instance_id": "inst-1",
    "created_at": TS,
}

WORKER_TASK_JSON = {
    "id": "wt-1",
    "instance_id": "inst-1",
    "block_id": "block-1",
    "handler_name": "my-handler",
    "created_at": TS,
}

CLUSTER_NODE_JSON = {
    "id": "node-1",
    "address": "10.0.0.1:8080",
    "state": "active",
    "last_heartbeat": TS,
}

CIRCUIT_BREAKER_JSON = {
    "handler": "my-handler",
    "state": "closed",
}

STEP_OUTPUT_JSON = {
    "id": "out-1",
    "instance_id": "inst-1",
    "block_id": "block-1",
    "created_at": TS,
}

EXECUTION_NODE_JSON = {
    "id": "en-1",
    "instance_id": "inst-1",
    "block_id": "block-1",
    "block_type": "task",
    "state": "completed",
}

AUDIT_ENTRY_JSON = {
    "timestamp": TS,
    "event": "state_changed",
}


# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def client() -> Orch8Client:
    return Orch8Client(base_url=BASE, tenant_id="t-1")


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------


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
    assert isinstance(result, HealthResponse)
    assert result.status == "unavailable"


# ---------------------------------------------------------------------------
# Headers / errors
# ---------------------------------------------------------------------------


@respx.mock
async def test_tenant_header_sent(client: Orch8Client) -> None:
    route = respx.get(f"{BASE}/health/ready").mock(return_value=httpx.Response(200))
    await client.health()
    assert route.calls[0].request.headers["X-Tenant-Id"] == "t-1"


@respx.mock
async def test_404_raises_orch8_error(client: Orch8Client) -> None:
    respx.get(f"{BASE}/sequences/nope").mock(
        return_value=httpx.Response(404, text="not found")
    )
    with pytest.raises(Orch8Error) as exc_info:
        await client.get_sequence("nope")
    assert exc_info.value.status == 404
    assert "not found" in exc_info.value.body


# ---------------------------------------------------------------------------
# Sequences
# ---------------------------------------------------------------------------


@respx.mock
async def test_create_sequence(client: Orch8Client) -> None:
    respx.post(f"{BASE}/sequences").mock(
        return_value=httpx.Response(201, json=SEQ_JSON)
    )
    result = await client.create_sequence({"name": "my-seq"})
    assert isinstance(result, SequenceDefinition)
    assert result.id == "seq-1"
    assert result.name == "my-seq"


@respx.mock
async def test_get_sequence(client: Orch8Client) -> None:
    respx.get(f"{BASE}/sequences/seq-1").mock(
        return_value=httpx.Response(200, json=SEQ_JSON)
    )
    result = await client.get_sequence("seq-1")
    assert isinstance(result, SequenceDefinition)
    assert result.id == "seq-1"


@respx.mock
async def test_get_sequence_by_name(client: Orch8Client) -> None:
    respx.get(f"{BASE}/sequences/by-name").mock(
        return_value=httpx.Response(200, json=SEQ_JSON)
    )
    result = await client.get_sequence_by_name(
        tenant_id="t-1", namespace="default", name="my-seq"
    )
    assert isinstance(result, SequenceDefinition)
    assert result.name == "my-seq"


@respx.mock
async def test_get_sequence_by_name_with_version(client: Orch8Client) -> None:
    respx.get(f"{BASE}/sequences/by-name").mock(
        return_value=httpx.Response(200, json=SEQ_JSON)
    )
    result = await client.get_sequence_by_name(
        tenant_id="t-1", namespace="default", name="my-seq", version=2
    )
    assert isinstance(result, SequenceDefinition)
    assert result.version == 1


@respx.mock
async def test_deprecate_sequence(client: Orch8Client) -> None:
    respx.post(f"{BASE}/sequences/seq-1/deprecate").mock(
        return_value=httpx.Response(204)
    )
    result = await client.deprecate_sequence("seq-1")
    assert result is None


@respx.mock
async def test_list_sequence_versions(client: Orch8Client) -> None:
    respx.get(f"{BASE}/sequences/versions").mock(
        return_value=httpx.Response(200, json=[SEQ_JSON])
    )
    result = await client.list_sequence_versions(
        tenant_id="t-1", namespace="default", name="my-seq"
    )
    assert isinstance(result, list)
    assert len(result) == 1
    assert isinstance(result[0], SequenceDefinition)
    assert result[0].id == "seq-1"


# ---------------------------------------------------------------------------
# Instances
# ---------------------------------------------------------------------------


@respx.mock
async def test_create_instance(client: Orch8Client) -> None:
    respx.post(f"{BASE}/instances").mock(
        return_value=httpx.Response(201, json=INSTANCE_JSON)
    )
    result = await client.create_instance({"sequence_id": "seq-1"})
    assert isinstance(result, TaskInstance)
    assert result.id == "inst-1"
    assert result.state == "pending"


@respx.mock
async def test_batch_create_instances(client: Orch8Client) -> None:
    respx.post(f"{BASE}/instances/batch").mock(
        return_value=httpx.Response(201, json={"created": 3})
    )
    result = await client.batch_create_instances([{}, {}, {}])
    assert isinstance(result, BatchCreateResponse)
    assert result.created == 3


@respx.mock
async def test_get_instance(client: Orch8Client) -> None:
    respx.get(f"{BASE}/instances/inst-1").mock(
        return_value=httpx.Response(200, json=INSTANCE_JSON)
    )
    result = await client.get_instance("inst-1")
    assert isinstance(result, TaskInstance)
    assert result.id == "inst-1"


@respx.mock
async def test_list_instances(client: Orch8Client) -> None:
    respx.get(f"{BASE}/instances").mock(
        return_value=httpx.Response(200, json=[INSTANCE_JSON])
    )
    result = await client.list_instances()
    assert isinstance(result, list)
    assert len(result) == 1
    assert isinstance(result[0], TaskInstance)
    assert result[0].id == "inst-1"


@respx.mock
async def test_update_instance_state(client: Orch8Client) -> None:
    respx.patch(f"{BASE}/instances/inst-1/state").mock(
        return_value=httpx.Response(204)
    )
    result = await client.update_instance_state("inst-1", "completed")
    assert result is None


@respx.mock
async def test_update_instance_state_with_next_fire(client: Orch8Client) -> None:
    respx.patch(f"{BASE}/instances/inst-1/state").mock(
        return_value=httpx.Response(204)
    )
    result = await client.update_instance_state(
        "inst-1", "scheduled", next_fire_at=TS
    )
    assert result is None


@respx.mock
async def test_update_instance_context(client: Orch8Client) -> None:
    respx.patch(f"{BASE}/instances/inst-1/context").mock(
        return_value=httpx.Response(204)
    )
    result = await client.update_instance_context("inst-1", {"key": "val"})
    assert result is None


@respx.mock
async def test_send_signal(client: Orch8Client) -> None:
    respx.post(f"{BASE}/instances/inst-1/signals").mock(
        return_value=httpx.Response(200, json={"signal_id": "sig-1"})
    )
    result = await client.send_signal("inst-1", "resume", payload={"data": 1})
    assert isinstance(result, dict)
    assert result["signal_id"] == "sig-1"


@respx.mock
async def test_get_outputs(client: Orch8Client) -> None:
    respx.get(f"{BASE}/instances/inst-1/outputs").mock(
        return_value=httpx.Response(200, json=[STEP_OUTPUT_JSON])
    )
    result = await client.get_outputs("inst-1")
    assert isinstance(result, list)
    assert len(result) == 1
    assert isinstance(result[0], StepOutput)
    assert result[0].id == "out-1"


@respx.mock
async def test_get_execution_tree(client: Orch8Client) -> None:
    respx.get(f"{BASE}/instances/inst-1/tree").mock(
        return_value=httpx.Response(200, json=[EXECUTION_NODE_JSON])
    )
    result = await client.get_execution_tree("inst-1")
    assert isinstance(result, list)
    assert len(result) == 1
    assert isinstance(result[0], ExecutionNode)
    assert result[0].id == "en-1"
    assert result[0].state == "completed"


@respx.mock
async def test_retry_instance(client: Orch8Client) -> None:
    respx.post(f"{BASE}/instances/inst-1/retry").mock(
        return_value=httpx.Response(200, json=INSTANCE_JSON)
    )
    result = await client.retry_instance("inst-1")
    assert isinstance(result, TaskInstance)
    assert result.id == "inst-1"


@respx.mock
async def test_list_checkpoints(client: Orch8Client) -> None:
    respx.get(f"{BASE}/instances/inst-1/checkpoints").mock(
        return_value=httpx.Response(200, json=[CHECKPOINT_JSON])
    )
    result = await client.list_checkpoints("inst-1")
    assert isinstance(result, list)
    assert len(result) == 1
    assert isinstance(result[0], Checkpoint)
    assert result[0].id == "cp-1"


@respx.mock
async def test_save_checkpoint(client: Orch8Client) -> None:
    respx.post(f"{BASE}/instances/inst-1/checkpoints").mock(
        return_value=httpx.Response(201, json=CHECKPOINT_JSON)
    )
    result = await client.save_checkpoint("inst-1", {"step": 3})
    assert isinstance(result, Checkpoint)
    assert result.instance_id == "inst-1"


@respx.mock
async def test_get_latest_checkpoint(client: Orch8Client) -> None:
    respx.get(f"{BASE}/instances/inst-1/checkpoints/latest").mock(
        return_value=httpx.Response(200, json=CHECKPOINT_JSON)
    )
    result = await client.get_latest_checkpoint("inst-1")
    assert isinstance(result, Checkpoint)
    assert result.id == "cp-1"


@respx.mock
async def test_prune_checkpoints(client: Orch8Client) -> None:
    respx.post(f"{BASE}/instances/inst-1/checkpoints/prune").mock(
        return_value=httpx.Response(200, json={"updated": 5})
    )
    result = await client.prune_checkpoints("inst-1", keep=2)
    assert isinstance(result, BulkResponse)
    assert result.updated == 5


@respx.mock
async def test_list_audit_log(client: Orch8Client) -> None:
    respx.get(f"{BASE}/instances/inst-1/audit").mock(
        return_value=httpx.Response(200, json=[AUDIT_ENTRY_JSON])
    )
    result = await client.list_audit_log("inst-1")
    assert isinstance(result, list)
    assert len(result) == 1
    assert isinstance(result[0], AuditEntry)
    assert result[0].event == "state_changed"


@respx.mock
async def test_bulk_update_state(client: Orch8Client) -> None:
    respx.patch(f"{BASE}/instances/bulk/state").mock(
        return_value=httpx.Response(200, json={"updated": 10})
    )
    result = await client.bulk_update_state(
        filter={"tenant_id": "t-1"}, state="cancelled"
    )
    assert isinstance(result, BulkResponse)
    assert result.updated == 10


@respx.mock
async def test_bulk_reschedule(client: Orch8Client) -> None:
    respx.patch(f"{BASE}/instances/bulk/reschedule").mock(
        return_value=httpx.Response(200, json={"updated": 4})
    )
    result = await client.bulk_reschedule(
        filter={"tenant_id": "t-1"}, offset_secs=300
    )
    assert isinstance(result, BulkResponse)
    assert result.updated == 4


@respx.mock
async def test_list_dlq(client: Orch8Client) -> None:
    respx.get(f"{BASE}/instances/dlq").mock(
        return_value=httpx.Response(200, json=[INSTANCE_JSON])
    )
    result = await client.list_dlq()
    assert isinstance(result, list)
    assert len(result) == 1
    assert isinstance(result[0], TaskInstance)
    assert result[0].id == "inst-1"


# ---------------------------------------------------------------------------
# Cron
# ---------------------------------------------------------------------------


@respx.mock
async def test_create_cron(client: Orch8Client) -> None:
    respx.post(f"{BASE}/cron").mock(
        return_value=httpx.Response(201, json=CRON_JSON)
    )
    result = await client.create_cron({"cron_expr": "0 * * * *"})
    assert isinstance(result, CronSchedule)
    assert result.id == "cron-1"
    assert result.cron_expr == "0 * * * *"


@respx.mock
async def test_list_cron(client: Orch8Client) -> None:
    respx.get(f"{BASE}/cron").mock(
        return_value=httpx.Response(200, json=[CRON_JSON])
    )
    result = await client.list_cron()
    assert isinstance(result, list)
    assert len(result) == 1
    assert isinstance(result[0], CronSchedule)
    assert result[0].id == "cron-1"


@respx.mock
async def test_get_cron(client: Orch8Client) -> None:
    respx.get(f"{BASE}/cron/cron-1").mock(
        return_value=httpx.Response(200, json=CRON_JSON)
    )
    result = await client.get_cron("cron-1")
    assert isinstance(result, CronSchedule)
    assert result.id == "cron-1"


@respx.mock
async def test_update_cron(client: Orch8Client) -> None:
    updated = {**CRON_JSON, "cron_expr": "30 * * * *"}
    respx.put(f"{BASE}/cron/cron-1").mock(
        return_value=httpx.Response(200, json=updated)
    )
    result = await client.update_cron("cron-1", {"cron_expr": "30 * * * *"})
    assert isinstance(result, CronSchedule)
    assert result.cron_expr == "30 * * * *"


@respx.mock
async def test_delete_cron_returns_none(client: Orch8Client) -> None:
    respx.delete(f"{BASE}/cron/cron-1").mock(return_value=httpx.Response(204))
    result = await client.delete_cron("cron-1")
    assert result is None


# ---------------------------------------------------------------------------
# Triggers
# ---------------------------------------------------------------------------


@respx.mock
async def test_create_trigger(client: Orch8Client) -> None:
    respx.post(f"{BASE}/triggers").mock(
        return_value=httpx.Response(201, json=TRIGGER_JSON)
    )
    result = await client.create_trigger({"slug": "my-hook"})
    assert isinstance(result, TriggerDef)
    assert result.slug == "my-hook"


@respx.mock
async def test_list_triggers(client: Orch8Client) -> None:
    respx.get(f"{BASE}/triggers").mock(
        return_value=httpx.Response(200, json=[TRIGGER_JSON])
    )
    result = await client.list_triggers()
    assert isinstance(result, list)
    assert len(result) == 1
    assert isinstance(result[0], TriggerDef)
    assert result[0].slug == "my-hook"


@respx.mock
async def test_get_trigger(client: Orch8Client) -> None:
    respx.get(f"{BASE}/triggers/my-hook").mock(
        return_value=httpx.Response(200, json=TRIGGER_JSON)
    )
    result = await client.get_trigger("my-hook")
    assert isinstance(result, TriggerDef)
    assert result.slug == "my-hook"
    assert result.sequence_name == "onboard"


@respx.mock
async def test_delete_trigger(client: Orch8Client) -> None:
    respx.delete(f"{BASE}/triggers/my-hook").mock(
        return_value=httpx.Response(204)
    )
    result = await client.delete_trigger("my-hook")
    assert result is None


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
    assert result.trigger == "my-hook"


# ---------------------------------------------------------------------------
# Plugins
# ---------------------------------------------------------------------------


@respx.mock
async def test_create_plugin(client: Orch8Client) -> None:
    respx.post(f"{BASE}/plugins").mock(
        return_value=httpx.Response(201, json=PLUGIN_JSON)
    )
    result = await client.create_plugin({"name": "my-plugin"})
    assert isinstance(result, PluginDef)
    assert result.name == "my-plugin"


@respx.mock
async def test_list_plugins(client: Orch8Client) -> None:
    respx.get(f"{BASE}/plugins").mock(
        return_value=httpx.Response(200, json=[PLUGIN_JSON])
    )
    result = await client.list_plugins()
    assert isinstance(result, list)
    assert len(result) == 1
    assert isinstance(result[0], PluginDef)
    assert result[0].name == "my-plugin"


@respx.mock
async def test_get_plugin(client: Orch8Client) -> None:
    respx.get(f"{BASE}/plugins/my-plugin").mock(
        return_value=httpx.Response(200, json=PLUGIN_JSON)
    )
    result = await client.get_plugin("my-plugin")
    assert isinstance(result, PluginDef)
    assert result.plugin_type == "handler"


@respx.mock
async def test_update_plugin(client: Orch8Client) -> None:
    updated = {**PLUGIN_JSON, "enabled": False}
    respx.patch(f"{BASE}/plugins/my-plugin").mock(
        return_value=httpx.Response(200, json=updated)
    )
    result = await client.update_plugin("my-plugin", {"enabled": False})
    assert isinstance(result, PluginDef)
    assert result.enabled is False


@respx.mock
async def test_delete_plugin(client: Orch8Client) -> None:
    respx.delete(f"{BASE}/plugins/my-plugin").mock(
        return_value=httpx.Response(204)
    )
    result = await client.delete_plugin("my-plugin")
    assert result is None


# ---------------------------------------------------------------------------
# Sessions
# ---------------------------------------------------------------------------


@respx.mock
async def test_create_session(client: Orch8Client) -> None:
    respx.post(f"{BASE}/sessions").mock(
        return_value=httpx.Response(201, json=SESSION_JSON)
    )
    result = await client.create_session({"session_key": "user-123"})
    assert isinstance(result, Session)
    assert result.id == "sess-1"
    assert result.session_key == "user-123"


@respx.mock
async def test_get_session(client: Orch8Client) -> None:
    respx.get(f"{BASE}/sessions/sess-1").mock(
        return_value=httpx.Response(200, json=SESSION_JSON)
    )
    result = await client.get_session("sess-1")
    assert isinstance(result, Session)
    assert result.id == "sess-1"


@respx.mock
async def test_get_session_by_key(client: Orch8Client) -> None:
    respx.get(f"{BASE}/sessions/by-key/t-1/user-123").mock(
        return_value=httpx.Response(200, json=SESSION_JSON)
    )
    result = await client.get_session_by_key("t-1", "user-123")
    assert isinstance(result, Session)
    assert result.session_key == "user-123"


@respx.mock
async def test_update_session_data(client: Orch8Client) -> None:
    respx.patch(f"{BASE}/sessions/sess-1/data").mock(
        return_value=httpx.Response(200, content=b"")
    )
    result = await client.update_session_data("sess-1", {"foo": "bar"})
    assert result is None


@respx.mock
async def test_update_session_state(client: Orch8Client) -> None:
    respx.patch(f"{BASE}/sessions/sess-1/state").mock(
        return_value=httpx.Response(200, content=b"")
    )
    result = await client.update_session_state("sess-1", "closed")
    assert result is None


@respx.mock
async def test_list_session_instances(client: Orch8Client) -> None:
    respx.get(f"{BASE}/sessions/sess-1/instances").mock(
        return_value=httpx.Response(200, json=[INSTANCE_JSON])
    )
    result = await client.list_session_instances("sess-1")
    assert isinstance(result, list)
    assert len(result) == 1
    assert isinstance(result[0], TaskInstance)
    assert result[0].id == "inst-1"


# ---------------------------------------------------------------------------
# Workers
# ---------------------------------------------------------------------------


@respx.mock
async def test_poll_tasks(client: Orch8Client) -> None:
    respx.post(f"{BASE}/workers/tasks/poll").mock(
        return_value=httpx.Response(200, json=[WORKER_TASK_JSON])
    )
    result = await client.poll_tasks(
        handler_name="my-handler", worker_id="w-1", limit=5
    )
    assert isinstance(result, list)
    assert len(result) == 1
    assert isinstance(result[0], WorkerTask)
    assert result[0].id == "wt-1"
    assert result[0].handler_name == "my-handler"


@respx.mock
async def test_complete_task(client: Orch8Client) -> None:
    respx.post(f"{BASE}/workers/tasks/wt-1/complete").mock(
        return_value=httpx.Response(204)
    )
    result = await client.complete_task("wt-1", "w-1", output={"result": 42})
    assert result is None


@respx.mock
async def test_fail_task(client: Orch8Client) -> None:
    respx.post(f"{BASE}/workers/tasks/wt-1/fail").mock(
        return_value=httpx.Response(204)
    )
    result = await client.fail_task("wt-1", "w-1", message="timeout", retryable=True)
    assert result is None


@respx.mock
async def test_heartbeat_task(client: Orch8Client) -> None:
    respx.post(f"{BASE}/workers/tasks/wt-1/heartbeat").mock(
        return_value=httpx.Response(204)
    )
    result = await client.heartbeat_task("wt-1", "w-1")
    assert result is None


# ---------------------------------------------------------------------------
# Cluster
# ---------------------------------------------------------------------------


@respx.mock
async def test_list_cluster_nodes(client: Orch8Client) -> None:
    respx.get(f"{BASE}/cluster/nodes").mock(
        return_value=httpx.Response(200, json=[CLUSTER_NODE_JSON])
    )
    result = await client.list_cluster_nodes()
    assert isinstance(result, list)
    assert len(result) == 1
    assert isinstance(result[0], ClusterNode)
    assert result[0].id == "node-1"
    assert result[0].state == "active"


@respx.mock
async def test_drain_node(client: Orch8Client) -> None:
    respx.post(f"{BASE}/cluster/nodes/node-1/drain").mock(
        return_value=httpx.Response(204)
    )
    result = await client.drain_node("node-1")
    assert result is None


# ---------------------------------------------------------------------------
# Circuit Breakers
# ---------------------------------------------------------------------------


@respx.mock
async def test_list_circuit_breakers(client: Orch8Client) -> None:
    respx.get(f"{BASE}/circuit-breakers").mock(
        return_value=httpx.Response(200, json=[CIRCUIT_BREAKER_JSON])
    )
    result = await client.list_circuit_breakers()
    assert isinstance(result, list)
    assert len(result) == 1
    assert isinstance(result[0], CircuitBreaker)
    assert result[0].handler == "my-handler"


@respx.mock
async def test_get_circuit_breaker(client: Orch8Client) -> None:
    respx.get(f"{BASE}/circuit-breakers/my-handler").mock(
        return_value=httpx.Response(200, json=CIRCUIT_BREAKER_JSON)
    )
    result = await client.get_circuit_breaker("my-handler")
    assert isinstance(result, CircuitBreaker)
    assert result.state == "closed"


@respx.mock
async def test_reset_circuit_breaker(client: Orch8Client) -> None:
    respx.post(f"{BASE}/circuit-breakers/my-handler/reset").mock(
        return_value=httpx.Response(204)
    )
    result = await client.reset_circuit_breaker("my-handler")
    assert result is None
