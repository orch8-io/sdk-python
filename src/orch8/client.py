"""Orch8 async management client wrapping the REST API."""
from __future__ import annotations

from typing import Any

import httpx

from .errors import Orch8Error
from .types import (
    AuditEntry,
    BatchCreateResponse,
    BulkResponse,
    Checkpoint,
    CircuitBreaker,
    ClusterNode,
    Credential,
    CronSchedule,
    ExecutionNode,
    FireTriggerResponse,
    HealthResponse,
    PluginDef,
    PoolResource,
    ResourcePool,
    SequenceDefinition,
    Session,
    StepOutput,
    TaskInstance,
    TriggerDef,
    WorkerTask,
)


class Orch8Client:
    """Async client for the Orch8 workflow-engine REST API."""

    def __init__(
        self,
        base_url: str,
        tenant_id: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        h: dict[str, str] = {"Content-Type": "application/json"}
        if tenant_id:
            h["X-Tenant-Id"] = tenant_id
        if headers:
            h.update(headers)
        self._http = httpx.AsyncClient(base_url=base_url, headers=h)

    # -- context manager --

    async def __aenter__(self) -> Orch8Client:
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    async def close(self) -> None:
        await self._http.aclose()

    # -- internal --

    async def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        resp = await self._http.request(method, path, **kwargs)
        if resp.status_code == 204:
            return None
        if resp.status_code >= 400:
            raise Orch8Error(resp.status_code, resp.text, path)
        if not resp.content:
            return None
        return resp.json()

    # ------------------------------------------------------------------ #
    # Sequences
    # ------------------------------------------------------------------ #

    async def create_sequence(self, definition: dict[str, Any]) -> SequenceDefinition:
        data = await self._request("POST", "/sequences", json=definition)
        return SequenceDefinition.model_validate(data)

    async def get_sequence(self, sequence_id: str) -> SequenceDefinition:
        data = await self._request("GET", f"/sequences/{sequence_id}")
        return SequenceDefinition.model_validate(data)

    async def get_sequence_by_name(
        self,
        tenant_id: str,
        namespace: str,
        name: str,
        version: int | None = None,
    ) -> SequenceDefinition:
        params: dict[str, Any] = {
            "tenant_id": tenant_id,
            "namespace": namespace,
            "name": name,
        }
        if version is not None:
            params["version"] = version
        data = await self._request("GET", "/sequences/by-name", params=params)
        return SequenceDefinition.model_validate(data)

    async def list_sequences(self, **filters: Any) -> list[SequenceDefinition]:
        data = await self._request("GET", "/sequences", params=filters)
        return [SequenceDefinition.model_validate(d) for d in data]

    async def delete_sequence(self, sequence_id: str) -> None:
        await self._request("DELETE", f"/sequences/{sequence_id}")

    async def migrate_instance(self, body: dict[str, Any]) -> TaskInstance:
        data = await self._request("POST", "/sequences/migrate-instance", json=body)
        return TaskInstance.model_validate(data)

    async def deprecate_sequence(self, sequence_id: str) -> None:
        await self._request("POST", f"/sequences/{sequence_id}/deprecate")

    async def list_sequence_versions(
        self, tenant_id: str, namespace: str, name: str
    ) -> list[SequenceDefinition]:
        params = {"tenant_id": tenant_id, "namespace": namespace, "name": name}
        data = await self._request("GET", "/sequences/versions", params=params)
        return [SequenceDefinition.model_validate(d) for d in data]

    # ------------------------------------------------------------------ #
    # Instances
    # ------------------------------------------------------------------ #

    async def create_instance(self, body: dict[str, Any]) -> TaskInstance:
        data = await self._request("POST", "/instances", json=body)
        return TaskInstance.model_validate(data)

    async def batch_create_instances(
        self, instances: list[dict[str, Any]]
    ) -> BatchCreateResponse:
        data = await self._request(
            "POST", "/instances/batch", json={"instances": instances}
        )
        return BatchCreateResponse.model_validate(data)

    async def get_instance(self, instance_id: str) -> TaskInstance:
        data = await self._request("GET", f"/instances/{instance_id}")
        return TaskInstance.model_validate(data)

    async def list_instances(self, **filters: Any) -> list[TaskInstance]:
        data = await self._request("GET", "/instances", params=filters)
        return [TaskInstance.model_validate(d) for d in data]

    async def update_instance_state(
        self,
        instance_id: str,
        state: str,
        next_fire_at: str | None = None,
    ) -> None:
        body: dict[str, Any] = {"state": state}
        if next_fire_at is not None:
            body["next_fire_at"] = next_fire_at
        await self._request("PATCH", f"/instances/{instance_id}/state", json=body)

    async def update_instance_context(
        self, instance_id: str, context: dict[str, Any]
    ) -> None:
        await self._request(
            "PATCH", f"/instances/{instance_id}/context", json={"context": context}
        )

    async def send_signal(
        self,
        instance_id: str,
        signal_type: str,
        payload: Any = None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {"signal_type": signal_type}
        if payload is not None:
            body["payload"] = payload
        return await self._request(
            "POST", f"/instances/{instance_id}/signals", json=body
        )

    async def get_outputs(self, instance_id: str) -> list[StepOutput]:
        data = await self._request("GET", f"/instances/{instance_id}/outputs")
        return [StepOutput.model_validate(d) for d in data]

    async def get_execution_tree(self, instance_id: str) -> list[ExecutionNode]:
        data = await self._request("GET", f"/instances/{instance_id}/tree")
        return [ExecutionNode.model_validate(d) for d in data]

    async def retry_instance(self, instance_id: str) -> TaskInstance:
        data = await self._request("POST", f"/instances/{instance_id}/retry")
        return TaskInstance.model_validate(data)

    # -- Checkpoints --

    async def list_checkpoints(self, instance_id: str) -> list[Checkpoint]:
        data = await self._request("GET", f"/instances/{instance_id}/checkpoints")
        return [Checkpoint.model_validate(d) for d in data]

    async def save_checkpoint(
        self, instance_id: str, checkpoint_data: Any
    ) -> Checkpoint:
        data = await self._request(
            "POST",
            f"/instances/{instance_id}/checkpoints",
            json={"checkpoint_data": checkpoint_data},
        )
        return Checkpoint.model_validate(data)

    async def get_latest_checkpoint(self, instance_id: str) -> Checkpoint:
        data = await self._request(
            "GET", f"/instances/{instance_id}/checkpoints/latest"
        )
        return Checkpoint.model_validate(data)

    async def prune_checkpoints(
        self, instance_id: str, keep: int
    ) -> BulkResponse:
        data = await self._request(
            "POST",
            f"/instances/{instance_id}/checkpoints/prune",
            json={"keep": keep},
        )
        return BulkResponse.model_validate(data)

    # -- Inject Blocks --

    async def inject_blocks(
        self, instance_id: str, blocks: list[dict[str, Any]]
    ) -> None:
        await self._request(
            "POST",
            f"/instances/{instance_id}/inject-blocks",
            json={"blocks": blocks},
        )

    # -- Audit --

    async def list_audit_log(self, instance_id: str) -> list[AuditEntry]:
        data = await self._request("GET", f"/instances/{instance_id}/audit")
        return [AuditEntry.model_validate(d) for d in data]

    # -- Bulk --

    async def bulk_update_state(
        self, filter: dict[str, Any], state: str
    ) -> BulkResponse:
        data = await self._request(
            "PATCH",
            "/instances/bulk/state",
            json={"filter": filter, "state": state},
        )
        return BulkResponse.model_validate(data)

    async def bulk_reschedule(
        self, filter: dict[str, Any], offset_secs: int
    ) -> BulkResponse:
        data = await self._request(
            "PATCH",
            "/instances/bulk/reschedule",
            json={"filter": filter, "offset_secs": offset_secs},
        )
        return BulkResponse.model_validate(data)

    async def list_dlq(self, **filters: Any) -> list[TaskInstance]:
        data = await self._request("GET", "/instances/dlq", params=filters)
        return [TaskInstance.model_validate(d) for d in data]

    # ------------------------------------------------------------------ #
    # Approvals
    # ------------------------------------------------------------------ #

    async def list_approvals(self, **filters: Any) -> list[TaskInstance]:
        data = await self._request("GET", "/approvals", params=filters)
        return [TaskInstance.model_validate(d) for d in data]

    # ------------------------------------------------------------------ #
    # Cron
    # ------------------------------------------------------------------ #

    async def create_cron(self, body: dict[str, Any]) -> CronSchedule:
        data = await self._request("POST", "/cron", json=body)
        return CronSchedule.model_validate(data)

    async def list_cron(self, tenant_id: str | None = None) -> list[CronSchedule]:
        params = {}
        if tenant_id is not None:
            params["tenant_id"] = tenant_id
        data = await self._request("GET", "/cron", params=params)
        return [CronSchedule.model_validate(d) for d in data]

    async def get_cron(self, cron_id: str) -> CronSchedule:
        data = await self._request("GET", f"/cron/{cron_id}")
        return CronSchedule.model_validate(data)

    async def update_cron(
        self, cron_id: str, body: dict[str, Any]
    ) -> CronSchedule:
        data = await self._request("PUT", f"/cron/{cron_id}", json=body)
        return CronSchedule.model_validate(data)

    async def delete_cron(self, cron_id: str) -> None:
        await self._request("DELETE", f"/cron/{cron_id}")

    # ------------------------------------------------------------------ #
    # Triggers
    # ------------------------------------------------------------------ #

    async def create_trigger(self, body: dict[str, Any]) -> TriggerDef:
        data = await self._request("POST", "/triggers", json=body)
        return TriggerDef.model_validate(data)

    async def list_triggers(
        self, tenant_id: str | None = None
    ) -> list[TriggerDef]:
        params = {}
        if tenant_id is not None:
            params["tenant_id"] = tenant_id
        data = await self._request("GET", "/triggers", params=params)
        return [TriggerDef.model_validate(d) for d in data]

    async def get_trigger(self, slug: str) -> TriggerDef:
        data = await self._request("GET", f"/triggers/{slug}")
        return TriggerDef.model_validate(data)

    async def delete_trigger(self, slug: str) -> None:
        await self._request("DELETE", f"/triggers/{slug}")

    async def fire_trigger(
        self, slug: str, payload: Any = None
    ) -> FireTriggerResponse:
        data = await self._request(
            "POST", f"/triggers/{slug}/fire", json=payload or {}
        )
        return FireTriggerResponse.model_validate(data)

    # ------------------------------------------------------------------ #
    # Plugins
    # ------------------------------------------------------------------ #

    async def create_plugin(self, body: dict[str, Any]) -> PluginDef:
        data = await self._request("POST", "/plugins", json=body)
        return PluginDef.model_validate(data)

    async def list_plugins(
        self, tenant_id: str | None = None
    ) -> list[PluginDef]:
        params = {}
        if tenant_id is not None:
            params["tenant_id"] = tenant_id
        data = await self._request("GET", "/plugins", params=params)
        return [PluginDef.model_validate(d) for d in data]

    async def get_plugin(self, name: str) -> PluginDef:
        data = await self._request("GET", f"/plugins/{name}")
        return PluginDef.model_validate(data)

    async def update_plugin(
        self, name: str, body: dict[str, Any]
    ) -> PluginDef:
        data = await self._request("PATCH", f"/plugins/{name}", json=body)
        return PluginDef.model_validate(data)

    async def delete_plugin(self, name: str) -> None:
        await self._request("DELETE", f"/plugins/{name}")

    # ------------------------------------------------------------------ #
    # Sessions
    # ------------------------------------------------------------------ #

    async def create_session(self, body: dict[str, Any]) -> Session:
        data = await self._request("POST", "/sessions", json=body)
        return Session.model_validate(data)

    async def get_session(self, session_id: str) -> Session:
        data = await self._request("GET", f"/sessions/{session_id}")
        return Session.model_validate(data)

    async def get_session_by_key(self, tenant_id: str, key: str) -> Session:
        data = await self._request(
            "GET", f"/sessions/by-key/{tenant_id}/{key}"
        )
        return Session.model_validate(data)

    async def update_session_data(
        self, session_id: str, data: Any
    ) -> None:
        await self._request(
            "PATCH", f"/sessions/{session_id}/data", json={"data": data}
        )

    async def update_session_state(
        self, session_id: str, state: str
    ) -> None:
        await self._request(
            "PATCH", f"/sessions/{session_id}/state", json={"state": state}
        )

    async def list_session_instances(
        self, session_id: str
    ) -> list[TaskInstance]:
        data = await self._request("GET", f"/sessions/{session_id}/instances")
        return [TaskInstance.model_validate(d) for d in data]

    # ------------------------------------------------------------------ #
    # Workers
    # ------------------------------------------------------------------ #

    async def poll_tasks(
        self,
        handler_name: str,
        worker_id: str,
        limit: int = 1,
    ) -> list[WorkerTask]:
        data = await self._request(
            "POST",
            "/workers/tasks/poll",
            json={
                "handler_name": handler_name,
                "worker_id": worker_id,
                "limit": limit,
            },
        )
        return [WorkerTask.model_validate(d) for d in data]

    async def complete_task(
        self, task_id: str, worker_id: str, output: Any
    ) -> None:
        await self._request(
            "POST",
            f"/workers/tasks/{task_id}/complete",
            json={"worker_id": worker_id, "output": output},
        )

    async def fail_task(
        self,
        task_id: str,
        worker_id: str,
        message: str,
        retryable: bool = False,
    ) -> None:
        await self._request(
            "POST",
            f"/workers/tasks/{task_id}/fail",
            json={
                "worker_id": worker_id,
                "message": message,
                "retryable": retryable,
            },
        )

    async def heartbeat_task(self, task_id: str, worker_id: str) -> None:
        await self._request(
            "POST",
            f"/workers/tasks/{task_id}/heartbeat",
            json={"worker_id": worker_id},
        )

    async def list_worker_tasks(self, **filters: Any) -> list[WorkerTask]:
        data = await self._request("GET", "/workers/tasks", params=filters)
        return [WorkerTask.model_validate(d) for d in data]

    async def get_worker_task_stats(self) -> dict[str, Any]:
        data = await self._request("GET", "/workers/tasks/stats")
        return data

    async def poll_tasks_from_queue(
        self,
        queue: str,
        worker_id: str,
        limit: int = 1,
    ) -> list[WorkerTask]:
        data = await self._request(
            "POST",
            "/workers/tasks/poll/queue",
            json={"queue": queue, "worker_id": worker_id, "limit": limit},
        )
        return [WorkerTask.model_validate(d) for d in data]

    # ------------------------------------------------------------------ #
    # Cluster
    # ------------------------------------------------------------------ #

    async def list_cluster_nodes(self) -> list[ClusterNode]:
        data = await self._request("GET", "/cluster/nodes")
        return [ClusterNode.model_validate(d) for d in data]

    async def drain_node(self, node_id: str) -> None:
        await self._request("POST", f"/cluster/nodes/{node_id}/drain")

    # ------------------------------------------------------------------ #
    # Circuit Breakers
    # ------------------------------------------------------------------ #

    async def list_circuit_breakers(self) -> list[CircuitBreaker]:
        data = await self._request("GET", "/circuit-breakers")
        return [CircuitBreaker.model_validate(d) for d in data]

    async def get_circuit_breaker(self, handler: str) -> CircuitBreaker:
        data = await self._request("GET", f"/circuit-breakers/{handler}")
        return CircuitBreaker.model_validate(data)

    async def reset_circuit_breaker(self, handler: str) -> None:
        await self._request("POST", f"/circuit-breakers/{handler}/reset")

    # ------------------------------------------------------------------ #
    # Circuit Breakers (per-tenant)
    # ------------------------------------------------------------------ #

    async def list_tenant_circuit_breakers(
        self, tenant_id: str
    ) -> list[CircuitBreaker]:
        data = await self._request(
            "GET", f"/tenants/{tenant_id}/circuit-breakers"
        )
        return [CircuitBreaker.model_validate(d) for d in data]

    async def get_tenant_circuit_breaker(
        self, tenant_id: str, handler: str
    ) -> CircuitBreaker:
        data = await self._request(
            "GET", f"/tenants/{tenant_id}/circuit-breakers/{handler}"
        )
        return CircuitBreaker.model_validate(data)

    async def reset_tenant_circuit_breaker(
        self, tenant_id: str, handler: str
    ) -> None:
        await self._request(
            "POST", f"/tenants/{tenant_id}/circuit-breakers/{handler}/reset"
        )

    # ------------------------------------------------------------------ #
    # Resource Pools
    # ------------------------------------------------------------------ #

    async def list_pools(
        self, tenant_id: str | None = None
    ) -> list[ResourcePool]:
        params: dict[str, Any] = {}
        if tenant_id is not None:
            params["tenant_id"] = tenant_id
        data = await self._request("GET", "/pools", params=params)
        return [ResourcePool.model_validate(d) for d in data]

    async def create_pool(self, body: dict[str, Any]) -> ResourcePool:
        data = await self._request("POST", "/pools", json=body)
        return ResourcePool.model_validate(data)

    async def get_pool(self, pool_id: str) -> ResourcePool:
        data = await self._request("GET", f"/pools/{pool_id}")
        return ResourcePool.model_validate(data)

    async def delete_pool(self, pool_id: str) -> None:
        await self._request("DELETE", f"/pools/{pool_id}")

    async def list_pool_resources(
        self, pool_id: str
    ) -> list[PoolResource]:
        data = await self._request("GET", f"/pools/{pool_id}/resources")
        return [PoolResource.model_validate(d) for d in data]

    async def create_pool_resource(
        self, pool_id: str, body: dict[str, Any]
    ) -> PoolResource:
        data = await self._request(
            "POST", f"/pools/{pool_id}/resources", json=body
        )
        return PoolResource.model_validate(data)

    async def update_pool_resource(
        self, pool_id: str, resource_id: str, body: dict[str, Any]
    ) -> PoolResource:
        data = await self._request(
            "PUT", f"/pools/{pool_id}/resources/{resource_id}", json=body
        )
        return PoolResource.model_validate(data)

    async def delete_pool_resource(
        self, pool_id: str, resource_id: str
    ) -> None:
        await self._request(
            "DELETE", f"/pools/{pool_id}/resources/{resource_id}"
        )

    # ------------------------------------------------------------------ #
    # Credentials
    # ------------------------------------------------------------------ #

    async def list_credentials(
        self, tenant_id: str | None = None
    ) -> list[Credential]:
        params: dict[str, Any] = {}
        if tenant_id is not None:
            params["tenant_id"] = tenant_id
        data = await self._request("GET", "/credentials", params=params)
        return [Credential.model_validate(d) for d in data]

    async def create_credential(self, body: dict[str, Any]) -> Credential:
        data = await self._request("POST", "/credentials", json=body)
        return Credential.model_validate(data)

    async def get_credential(self, credential_id: str) -> Credential:
        data = await self._request("GET", f"/credentials/{credential_id}")
        return Credential.model_validate(data)

    async def delete_credential(self, credential_id: str) -> None:
        await self._request("DELETE", f"/credentials/{credential_id}")

    async def update_credential(
        self, credential_id: str, body: dict[str, Any]
    ) -> Credential:
        data = await self._request(
            "PATCH", f"/credentials/{credential_id}", json=body
        )
        return Credential.model_validate(data)

    # ------------------------------------------------------------------ #
    # Health
    # ------------------------------------------------------------------ #

    async def health(self) -> HealthResponse:
        try:
            await self._request("GET", "/health/ready")
            return HealthResponse(status="ok")
        except Orch8Error:
            return HealthResponse(status="unavailable")
