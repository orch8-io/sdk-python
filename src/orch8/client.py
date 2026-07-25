"""Orch8 async management client wrapping the REST API."""
from __future__ import annotations

import asyncio
import inspect
import json
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4
from urllib.parse import quote

import httpx
from pydantic import BaseModel

from .errors import Orch8Error
from .types import (
    AddResourceRequest,
    ApprovalItem,
    ApprovalsResponse,
    AuditEntry,
    BatchCreateResponse,
    BulkResponse,
    Checkpoint,
    CircuitBreaker,
    ClusterNode,
    CommandPayload,
    CreateCommandRequest,
    CreateCredentialRequest,
    CreateCronRequest,
    CreateInstanceRequest,
    CreatePoolRequest,
    CreateSessionRequest,
    CreateSequenceResponse,
    CreateTriggerRequest,
    Credential,
    CronSchedule,
    DashboardResponse,
    DeviceContext,
    ExecutionNode,
    FireTriggerResponse,
    HealthResponse,
    IngestErrorRequest,
    IngestResponse,
    IngestTelemetryRequest,
    PluginDef,
    PoolResource,
    RegisterDeviceRequest,
    ResolveApprovalRequest,
    ResourcePool,
    RetryConfig,
    RequestEvent,
    ResponseEvent,
    Page,
    RollbackPolicy,
    SequenceDefinition,
    SendSignalRequest,
    Session,
    StepOutput,
    SyncRequest,
    SyncResponse,
    TaskInstance,
    TelemetryBatchItem,
    TriggerDef,
    UpdateContextRequest,
    UpdateCredentialRequest,
    UpdateCronRequest,
    UpdateResourceRequest,
    UpdateStateRequest,
    WorkerTask,
)


class Orch8Client:
    """Async client for the Orch8 workflow-engine REST API."""

    def __init__(
        self,
        base_url: str,
        tenant_id: str | None = None,
        headers: dict[str, str] | None = None,
        get_headers: Callable[
            [], Mapping[str, str] | Awaitable[Mapping[str, str]]
        ] | None = None,
        retry: RetryConfig | bool = True,
        timeout: float = 30.0,
        on_request: Callable[[RequestEvent], None] | None = None,
        on_response: Callable[[ResponseEvent], None] | None = None,
    ) -> None:
        h: dict[str, str] = {"Content-Type": "application/json"}
        if tenant_id:
            h["X-Tenant-Id"] = tenant_id
        if headers:
            h.update(headers)
        self.base_url = base_url.rstrip("/")
        self.headers = h
        self.get_headers = get_headers
        self.retry = RetryConfig() if retry is True else retry
        self.on_request = on_request
        self.on_response = on_response
        self._http = httpx.AsyncClient(
            base_url=self.base_url, headers=self.headers, timeout=timeout
        )

    # -- context manager --

    async def __aenter__(self) -> Orch8Client:
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    async def close(self) -> None:
        await self._http.aclose()

    # -- low-level API --

    @staticmethod
    def _body(value: BaseModel | dict[str, Any]) -> dict[str, Any]:
        if isinstance(value, BaseModel):
            return value.model_dump(exclude_none=True)
        return value

    @staticmethod
    def _e(segment: str) -> str:
        return quote(segment, safe="")

    async def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        normalized_method = method.upper()
        safe = normalized_method in {"GET", "HEAD"}
        config = self.retry if isinstance(self.retry, RetryConfig) else None
        max_attempts = max(1, config.max_attempts) if config and safe else 1
        last_error: Exception | None = None

        for attempt in range(1, max_attempts + 1):
            event = RequestEvent(
                method=normalized_method,
                path=path,
                attempt=attempt,
                max_attempts=max_attempts,
            )
            started_at = time.perf_counter()
            self._observe(self.on_request, event)
            try:
                request_kwargs = dict(kwargs)
                if self.get_headers:
                    dynamic = self.get_headers()
                    if inspect.isawaitable(dynamic):
                        dynamic = await dynamic
                    request_kwargs["headers"] = {
                        **request_kwargs.get("headers", {}),
                        **dict(dynamic),
                    }
                resp = await self._http.request(
                    normalized_method, path, **request_kwargs
                )
                if resp.status_code >= 400:
                    error = Orch8Error(resp.status_code, resp.text, path)
                    self._observe(
                        self.on_response,
                        ResponseEvent(
                            **event.model_dump(),
                            duration_ms=(time.perf_counter() - started_at) * 1000,
                            status=resp.status_code,
                            error=error,
                        ),
                    )
                    if (
                        self._is_retryable_status(resp.status_code)
                        and attempt < max_attempts
                    ):
                        await self._retry(error, attempt, config)
                        continue
                    raise error
                if resp.status_code == 204 or not resp.content:
                    self._observe(
                        self.on_response,
                        ResponseEvent(
                            **event.model_dump(),
                            duration_ms=(time.perf_counter() - started_at) * 1000,
                            status=resp.status_code,
                        ),
                    )
                    return None
                self._observe(
                    self.on_response,
                    ResponseEvent(
                        **event.model_dump(),
                        duration_ms=(time.perf_counter() - started_at) * 1000,
                        status=resp.status_code,
                    ),
                )
                return resp.json()
            except Orch8Error:
                raise
            except httpx.TransportError as error:
                last_error = error
                self._observe(
                    self.on_response,
                    ResponseEvent(
                        **event.model_dump(),
                        duration_ms=(time.perf_counter() - started_at) * 1000,
                        error=error,
                    ),
                )
                if attempt >= max_attempts:
                    raise
                await self._retry(error, attempt, config)

        if last_error:
            raise last_error
        raise RuntimeError("request attempts exhausted")

    @staticmethod
    def _observe(observer: Callable[[Any], None] | None, event: Any) -> None:
        try:
            if observer:
                observer(event)
        except Exception:
            # Observability must never change request behavior.
            pass

    @staticmethod
    def _is_retryable_status(status: int) -> bool:
        return status in {408, 425, 429} or status >= 500

    @staticmethod
    async def _retry(
        error: Exception, attempt: int, config: RetryConfig | None
    ) -> None:
        if not config:
            return
        if config.on_retry:
            config.on_retry(error, attempt + 1)
        delay = config.base_delay * (2 ** (attempt - 1))
        if delay > 0:
            await asyncio.sleep(delay)

    async def request(self, method: str, path: str, **kwargs: Any) -> Any:
        """Call an engine endpoint not yet covered by a convenience method.

        ``path`` must be relative to the configured engine origin.  This keeps
        authentication and tenant headers on the configured host and makes new
        engine endpoints immediately accessible as the API evolves.
        """
        if not path.startswith("/") or path.startswith("//"):
            raise ValueError("path must start with exactly one '/' character")
        return await self._request(method.upper(), path, **kwargs)

    async def request_page(
        self, path: str, params: dict[str, Any] | None = None
    ) -> Page[Any]:
        """Request a list endpoint without discarding cursor metadata."""
        data = await self.request("GET", path, params=params or {})
        if isinstance(data, list):
            return Page(items=data)
        return Page.model_validate(data)

    # ------------------------------------------------------------------ #
    # Sequences
    # ------------------------------------------------------------------ #

    async def create_sequence(self, definition: dict[str, Any]) -> CreateSequenceResponse:
        tenant_id = definition.get("tenant_id") or self.headers.get("X-Tenant-Id")
        if not tenant_id:
            raise ValueError("tenant_id is required to create a sequence")
        prepared = dict(definition)
        prepared["id"] = definition.get("id") or str(uuid4())
        prepared["tenant_id"] = tenant_id
        prepared["namespace"] = definition.get("namespace") or "default"
        prepared["version"] = definition.get("version") or 1
        prepared["deprecated"] = definition.get("deprecated", False)
        prepared["status"] = definition.get("status") or "production"
        prepared["created_at"] = definition.get("created_at") or datetime.now(
            timezone.utc
        ).isoformat().replace("+00:00", "Z")
        data = await self._request("POST", "/sequences", json=prepared)
        return CreateSequenceResponse.model_validate(data)

    async def get_sequence(self, sequence_id: str) -> SequenceDefinition:
        data = await self._request("GET", f"/sequences/{self._e(sequence_id)}")
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
        if isinstance(data, dict):
            data = data.get("items", [])
        return [SequenceDefinition.model_validate(d) for d in data]

    async def delete_sequence(self, sequence_id: str) -> None:
        await self._request("DELETE", f"/sequences/{self._e(sequence_id)}")

    async def migrate_instance(self, body: dict[str, Any]) -> TaskInstance:
        data = await self._request("POST", "/sequences/migrate-instance", json=body)
        return TaskInstance.model_validate(data)

    async def deprecate_sequence(self, sequence_id: str) -> None:
        await self._request("POST", f"/sequences/{self._e(sequence_id)}/deprecate")

    async def list_sequence_versions(
        self, tenant_id: str, namespace: str, name: str
    ) -> list[SequenceDefinition]:
        params = {"tenant_id": tenant_id, "namespace": namespace, "name": name}
        data = await self._request("GET", "/sequences/versions", params=params)
        return [SequenceDefinition.model_validate(d) for d in data]

    # ------------------------------------------------------------------ #
    # Instances
    # ------------------------------------------------------------------ #

    async def create_instance(
        self, body: dict[str, Any] | CreateInstanceRequest
    ) -> TaskInstance:
        data = await self._request(
            "POST", "/instances", json=self._prepare_instance(self._body(body))
        )
        return TaskInstance.model_validate(data)

    async def batch_create_instances(
        self, instances: list[dict[str, Any]]
    ) -> BatchCreateResponse:
        data = await self._request(
            "POST",
            "/instances/batch",
            json={"instances": [self._prepare_instance(item) for item in instances]},
        )
        return BatchCreateResponse.model_validate(data)

    async def get_instance(self, instance_id: str) -> TaskInstance:
        data = await self._request("GET", f"/instances/{self._e(instance_id)}")
        return TaskInstance.model_validate(data)

    async def list_instances(self, **filters: Any) -> list[TaskInstance]:
        data = await self._request("GET", "/instances", params=filters)
        if isinstance(data, dict):
            data = data.get("items", [])
        return [TaskInstance.model_validate(d) for d in data]

    def _prepare_instance(self, body: dict[str, Any]) -> dict[str, Any]:
        tenant_id = body.get("tenant_id") or self.headers.get("X-Tenant-Id")
        if not tenant_id:
            raise ValueError("tenant_id is required to create an instance")
        prepared = dict(body)
        prepared["tenant_id"] = tenant_id
        prepared["namespace"] = body.get("namespace") or "default"
        return prepared

    async def update_instance_state(
        self,
        instance_id: str,
        state: str | UpdateStateRequest | dict[str, Any],
        next_fire_at: str | None = None,
    ) -> None:
        if isinstance(state, (UpdateStateRequest, dict)):
            body = self._body(state)
        else:
            body = {"state": state}
            if next_fire_at is not None:
                body["next_fire_at"] = next_fire_at
        await self._request(
            "PATCH", f"/instances/{self._e(instance_id)}/state", json=body
        )

    async def update_instance_context(
        self,
        instance_id: str,
        context: dict[str, Any] | UpdateContextRequest,
    ) -> None:
        if isinstance(context, UpdateContextRequest):
            body = context.model_dump(exclude_none=True)
        else:
            body = {"context": context}
        await self._request(
            "PATCH", f"/instances/{self._e(instance_id)}/context", json=body
        )

    async def send_signal(
        self,
        instance_id: str,
        signal_type: str | SendSignalRequest | dict[str, Any],
        payload: Any = None,
    ) -> dict[str, Any]:
        if isinstance(signal_type, (SendSignalRequest, dict)):
            body = self._body(signal_type)
        else:
            body = {"signal_type": signal_type}
            if payload is not None:
                body["payload"] = payload
        return await self._request(
            "POST", f"/instances/{self._e(instance_id)}/signals", json=body
        )

    async def get_outputs(self, instance_id: str) -> list[StepOutput]:
        data = await self._request("GET", f"/instances/{self._e(instance_id)}/outputs")
        return [StepOutput.model_validate(d) for d in data]

    async def get_execution_tree(self, instance_id: str) -> list[ExecutionNode]:
        data = await self._request("GET", f"/instances/{self._e(instance_id)}/tree")
        return [ExecutionNode.model_validate(d) for d in data]

    async def retry_instance(self, instance_id: str) -> TaskInstance:
        data = await self._request("POST", f"/instances/{self._e(instance_id)}/retry")
        return TaskInstance.model_validate(data)

    async def stream_instance(
        self, instance_id: str, poll_ms: int = 500
    ) -> AsyncIterator[dict[str, Any]]:
        """SSE stream for instance state/output/done events.

        poll_ms must be between 100 and 5000 (inclusive).
        """
        async for event in self.stream_instance_events(instance_id, poll_ms=poll_ms):
            yield event["data"]

    async def stream_instance_events(
        self,
        instance_id: str,
        *,
        poll_ms: int = 500,
        last_event_id: str | None = None,
    ) -> AsyncIterator[dict[str, Any]]:
        """Stream SSE envelopes and expose IDs that can resume a later stream."""
        poll_ms = max(100, min(poll_ms, 5000))
        dynamic_headers: dict[str, str] = {"Accept": "text/event-stream"}
        if last_event_id:
            dynamic_headers["Last-Event-ID"] = last_event_id
        if self.get_headers:
            resolved = self.get_headers()
            if inspect.isawaitable(resolved):
                resolved = await resolved
            dynamic_headers.update(resolved)
        async with self._http.stream(
            "GET",
            f"/instances/{self._e(instance_id)}/stream",
            params={"poll_ms": poll_ms},
            headers=dynamic_headers,
            timeout=None,
        ) as response:
            if response.status_code >= 400:
                body = (await response.aread()).decode(errors="replace")
                raise Orch8Error(response.status_code, body, str(response.request.url.path))
            event_id: str | None = None
            event_type: str | None = None
            event_data: list[str] = []
            async for line in response.aiter_lines():
                if line.startswith("id:"):
                    event_id = line[3:].strip()
                elif line.startswith("event:"):
                    event_type = line[6:].strip()
                elif line.startswith("data:"):
                    event_data.append(line[5:].lstrip())
                elif not line and event_data:
                    raw = "\n".join(event_data)
                    event_data = []
                    if raw != "[DONE]":
                        yield {"id": event_id, "event": event_type, "data": json.loads(raw)}
                    event_id = None
                    event_type = None
            if event_data:
                raw = "\n".join(event_data)
                if raw != "[DONE]":
                    yield {"id": event_id, "event": event_type, "data": json.loads(raw)}

    # -- Checkpoints --

    async def list_checkpoints(self, instance_id: str) -> list[Checkpoint]:
        data = await self._request("GET", f"/instances/{self._e(instance_id)}/checkpoints")
        return [Checkpoint.model_validate(d) for d in data]

    async def save_checkpoint(
        self, instance_id: str, checkpoint_data: Any
    ) -> Checkpoint:
        data = await self._request(
            "POST",
            f"/instances/{self._e(instance_id)}/checkpoints",
            json={"checkpoint_data": checkpoint_data},
        )
        return Checkpoint.model_validate(data)

    async def get_latest_checkpoint(self, instance_id: str) -> Checkpoint:
        data = await self._request(
            "GET", f"/instances/{self._e(instance_id)}/checkpoints/latest"
        )
        return Checkpoint.model_validate(data)

    async def prune_checkpoints(
        self, instance_id: str, keep: int
    ) -> BulkResponse:
        data = await self._request(
            "POST",
            f"/instances/{self._e(instance_id)}/checkpoints/prune",
            json={"keep": keep},
        )
        return BulkResponse.model_validate(data)

    # -- Inject Blocks --

    async def inject_blocks(
        self, instance_id: str, blocks: list[dict[str, Any]]
    ) -> None:
        await self._request(
            "POST",
            f"/instances/{self._e(instance_id)}/inject-blocks",
            json={"blocks": blocks},
        )

    # -- Audit --

    async def list_audit_log(self, instance_id: str) -> list[AuditEntry]:
        data = await self._request("GET", f"/instances/{self._e(instance_id)}/audit")
        return [AuditEntry.model_validate(d) for d in data]

    # -- Bulk --

    async def bulk_update_state(
        self, criteria: dict[str, Any], state: str
    ) -> BulkResponse:
        data = await self._request(
            "PATCH",
            "/instances/bulk/state",
            json={"filter": criteria, "state": state},
        )
        return BulkResponse.model_validate(data)

    async def bulk_reschedule(
        self, criteria: dict[str, Any], offset_secs: int
    ) -> BulkResponse:
        data = await self._request(
            "PATCH",
            "/instances/bulk/reschedule",
            json={"filter": criteria, "offset_secs": offset_secs},
        )
        return BulkResponse.model_validate(data)

    async def list_dlq(self, **filters: Any) -> list[TaskInstance]:
        data = await self._request("GET", "/instances/dlq", params=filters)
        return [TaskInstance.model_validate(d) for d in data]

    # ------------------------------------------------------------------ #
    # Approvals
    # ------------------------------------------------------------------ #

    async def list_approvals(self, **filters: Any) -> ApprovalsResponse:
        data = await self._request("GET", "/approvals", params=filters)
        return ApprovalsResponse.model_validate(data)

    # ------------------------------------------------------------------ #
    # Cron
    # ------------------------------------------------------------------ #

    async def create_cron(
        self, body: dict[str, Any] | CreateCronRequest
    ) -> CronSchedule:
        data = await self._request("POST", "/cron", json=self._body(body))
        return CronSchedule.model_validate(data)

    async def list_cron(self, tenant_id: str | None = None) -> list[CronSchedule]:
        params = {}
        if tenant_id is not None:
            params["tenant_id"] = tenant_id
        data = await self._request("GET", "/cron", params=params)
        return [CronSchedule.model_validate(d) for d in data]

    async def get_cron(self, cron_id: str) -> CronSchedule:
        data = await self._request("GET", f"/cron/{self._e(cron_id)}")
        return CronSchedule.model_validate(data)

    async def update_cron(
        self, cron_id: str, body: dict[str, Any] | UpdateCronRequest
    ) -> CronSchedule:
        data = await self._request("PUT", f"/cron/{self._e(cron_id)}", json=self._body(body))
        return CronSchedule.model_validate(data)

    async def delete_cron(self, cron_id: str) -> None:
        await self._request("DELETE", f"/cron/{self._e(cron_id)}")

    # ------------------------------------------------------------------ #
    # Triggers
    # ------------------------------------------------------------------ #

    async def create_trigger(
        self, body: dict[str, Any] | CreateTriggerRequest
    ) -> TriggerDef:
        data = await self._request("POST", "/triggers", json=self._body(body))
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
        data = await self._request("GET", f"/triggers/{self._e(slug)}")
        return TriggerDef.model_validate(data)

    async def delete_trigger(self, slug: str) -> None:
        await self._request("DELETE", f"/triggers/{self._e(slug)}")

    async def fire_trigger(
        self, slug: str, payload: Any = None
    ) -> FireTriggerResponse:
        data = await self._request(
            "POST", f"/triggers/{self._e(slug)}/fire", json=payload or {}
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
        data = await self._request("GET", f"/plugins/{self._e(name)}")
        return PluginDef.model_validate(data)

    async def update_plugin(
        self, name: str, body: dict[str, Any]
    ) -> PluginDef:
        data = await self._request("PATCH", f"/plugins/{self._e(name)}", json=body)
        return PluginDef.model_validate(data)

    async def delete_plugin(self, name: str) -> None:
        await self._request("DELETE", f"/plugins/{self._e(name)}")

    # ------------------------------------------------------------------ #
    # Sessions
    # ------------------------------------------------------------------ #

    async def create_session(
        self, body: dict[str, Any] | CreateSessionRequest
    ) -> Session:
        data = await self._request("POST", "/sessions", json=self._body(body))
        return Session.model_validate(data)

    async def get_session(self, session_id: str) -> Session:
        data = await self._request("GET", f"/sessions/{self._e(session_id)}")
        return Session.model_validate(data)

    async def get_session_by_key(self, tenant_id: str, key: str) -> Session:
        data = await self._request(
            "GET", f"/sessions/by-key/{self._e(tenant_id)}/{self._e(key)}"
        )
        return Session.model_validate(data)

    async def update_session_data(
        self, session_id: str, data: Any
    ) -> None:
        await self._request(
            "PATCH", f"/sessions/{self._e(session_id)}/data", json={"data": data}
        )

    async def update_session_state(
        self, session_id: str, state: str
    ) -> None:
        await self._request(
            "PATCH", f"/sessions/{self._e(session_id)}/state", json={"state": state}
        )

    async def list_session_instances(
        self, session_id: str
    ) -> list[TaskInstance]:
        data = await self._request("GET", f"/sessions/{self._e(session_id)}/instances")
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
            f"/workers/tasks/{self._e(task_id)}/complete",
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
            f"/workers/tasks/{self._e(task_id)}/fail",
            json={
                "worker_id": worker_id,
                "message": message,
                "retryable": retryable,
            },
        )

    async def heartbeat_task(
        self,
        task_id: str,
        worker_id: str,
        *,
        checkpoint: Any = None,
        checkpoint_seq: int | None = None,
    ) -> dict[str, int]:
        body: dict[str, Any] = {"worker_id": worker_id}
        if checkpoint is not None:
            if checkpoint_seq is None:
                raise ValueError("checkpoint_seq is required with checkpoint")
            body["checkpoint"] = checkpoint
            body["checkpoint_seq"] = checkpoint_seq
        return await self._request(
            "POST",
            f"/workers/tasks/{self._e(task_id)}/heartbeat",
            json=body,
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
        handler_name: str,
        worker_id: str,
        limit: int = 1,
    ) -> list[WorkerTask]:
        data = await self._request(
            "POST",
            "/workers/tasks/poll/queue",
            json={"queue_name": queue, "handler_name": handler_name, "worker_id": worker_id, "limit": limit},
        )
        return [WorkerTask.model_validate(d) for d in data]

    # ------------------------------------------------------------------ #
    # Cluster
    # ------------------------------------------------------------------ #

    async def list_cluster_nodes(self) -> list[ClusterNode]:
        data = await self._request("GET", "/cluster/nodes")
        return [ClusterNode.model_validate(d) for d in data]

    async def drain_node(self, node_id: str) -> None:
        await self._request("POST", f"/cluster/nodes/{self._e(node_id)}/drain")

    # ------------------------------------------------------------------ #
    # Circuit Breakers
    # ------------------------------------------------------------------ #

    async def list_circuit_breakers(self) -> list[CircuitBreaker]:
        data = await self._request("GET", "/circuit-breakers")
        return [CircuitBreaker.model_validate(d) for d in data]

    async def get_circuit_breaker(self, handler: str) -> CircuitBreaker:
        data = await self._request("GET", f"/circuit-breakers/{self._e(handler)}")
        return CircuitBreaker.model_validate(data)

    async def reset_circuit_breaker(self, handler: str) -> None:
        await self._request("POST", f"/circuit-breakers/{self._e(handler)}/reset")

    # ------------------------------------------------------------------ #
    # Circuit Breakers (per-tenant)
    # ------------------------------------------------------------------ #

    async def list_tenant_circuit_breakers(
        self, tenant_id: str
    ) -> list[CircuitBreaker]:
        data = await self._request(
            "GET", f"/tenants/{self._e(tenant_id)}/circuit-breakers"
        )
        return [CircuitBreaker.model_validate(d) for d in data]

    async def get_tenant_circuit_breaker(
        self, tenant_id: str, handler: str
    ) -> CircuitBreaker:
        data = await self._request(
            "GET", f"/tenants/{self._e(tenant_id)}/circuit-breakers/{self._e(handler)}"
        )
        return CircuitBreaker.model_validate(data)

    async def reset_tenant_circuit_breaker(
        self, tenant_id: str, handler: str
    ) -> None:
        await self._request(
            "POST", f"/tenants/{self._e(tenant_id)}/circuit-breakers/{self._e(handler)}/reset"
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

    async def create_pool(
        self, body: dict[str, Any] | CreatePoolRequest
    ) -> ResourcePool:
        data = await self._request("POST", "/pools", json=self._body(body))
        return ResourcePool.model_validate(data)

    async def get_pool(self, pool_id: str) -> ResourcePool:
        data = await self._request("GET", f"/pools/{self._e(pool_id)}")
        return ResourcePool.model_validate(data)

    async def delete_pool(self, pool_id: str) -> None:
        await self._request("DELETE", f"/pools/{self._e(pool_id)}")

    async def list_pool_resources(
        self, pool_id: str
    ) -> list[PoolResource]:
        data = await self._request("GET", f"/pools/{self._e(pool_id)}/resources")
        return [PoolResource.model_validate(d) for d in data]

    async def create_pool_resource(
        self, pool_id: str, body: dict[str, Any] | AddResourceRequest
    ) -> PoolResource:
        data = await self._request(
            "POST", f"/pools/{self._e(pool_id)}/resources", json=self._body(body)
        )
        return PoolResource.model_validate(data)

    async def update_pool_resource(
        self, pool_id: str, resource_id: str, body: dict[str, Any] | UpdateResourceRequest
    ) -> PoolResource:
        data = await self._request(
            "PUT", f"/pools/{self._e(pool_id)}/resources/{self._e(resource_id)}", json=self._body(body)
        )
        return PoolResource.model_validate(data)

    async def delete_pool_resource(
        self, pool_id: str, resource_id: str
    ) -> None:
        await self._request(
            "DELETE", f"/pools/{self._e(pool_id)}/resources/{self._e(resource_id)}"
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

    async def create_credential(
        self, body: dict[str, Any] | CreateCredentialRequest
    ) -> Credential:
        data = await self._request("POST", "/credentials", json=self._body(body))
        return Credential.model_validate(data)

    async def get_credential(self, credential_id: str) -> Credential:
        data = await self._request("GET", f"/credentials/{self._e(credential_id)}")
        return Credential.model_validate(data)

    async def delete_credential(self, credential_id: str) -> None:
        await self._request("DELETE", f"/credentials/{self._e(credential_id)}")

    async def update_credential(
        self, credential_id: str, body: dict[str, Any] | UpdateCredentialRequest
    ) -> Credential:
        data = await self._request(
            "PATCH", f"/credentials/{self._e(credential_id)}", json=self._body(body)
        )
        return Credential.model_validate(data)

    # ------------------------------------------------------------------ #
    # Health
    # ------------------------------------------------------------------ #

    async def health(self) -> HealthResponse:
        try:
            await self._request("GET", "/health/ready")
            return HealthResponse(status="ok")
        except (Orch8Error, httpx.NetworkError, httpx.TimeoutException):
            return HealthResponse(status="unavailable")

    # ------------------------------------------------------------------ #
    # Mobile Sync
    # ------------------------------------------------------------------ #

    async def mobile_sync(
        self,
        device_id: str,
        status_updates: list[StatusUpdatePayload] | None = None,
        approval_requests: list[ApprovalRequestPayload] | None = None,
        step_delegations: list[StepDelegationPayload] | None = None,
        command_acks: list[str] | None = None,
    ) -> SyncResponse:
        body = SyncRequest(
            device_id=device_id,
            status_updates=status_updates or [],
            approval_requests=approval_requests or [],
            step_delegations=step_delegations or [],
            command_acks=command_acks or [],
        )
        data = await self._request("POST", "/mobile/sync", json=body.model_dump(exclude_none=True))
        return SyncResponse.model_validate(data)

    async def register_mobile_device(
        self,
        device_id: str,
        push_token: str | None = None,
        platform: str = "",
        app_version: str | None = None,
    ) -> None:
        body = RegisterDeviceRequest(
            device_id=device_id,
            push_token=push_token,
            platform=platform,
            app_version=app_version,
        )
        await self._request("POST", "/mobile/devices/register", json=body.model_dump(exclude_none=True))

    async def list_mobile_devices(self) -> MobileDevicesResponse:
        data = await self._request("GET", "/mobile/devices")
        return MobileDevicesResponse.model_validate(data)

    async def list_mobile_approvals(self) -> MobileApprovalsResponse:
        data = await self._request("GET", "/mobile/approvals")
        return MobileApprovalsResponse.model_validate(data)

    async def resolve_mobile_approval(
        self, id: str, output: Any = None
    ) -> None:
        body = ResolveApprovalRequest(output=output)
        await self._request(
            "POST", f"/mobile/approvals/{self._e(id)}/resolve", json=body.model_dump(exclude_none=True)
        )

    async def list_mobile_status(self) -> MobileStatusResponse:
        data = await self._request("GET", "/mobile/status")
        return MobileStatusResponse.model_validate(data)

    async def create_mobile_command(
        self,
        device_id: str,
        command_type: str,
        payload: Any = None,
    ) -> None:
        body = CreateCommandRequest(
            device_id=device_id,
            command_type=command_type,
            payload=payload,
        )
        await self._request("POST", "/mobile/commands", json=body.model_dump(exclude_none=True))

    # ------------------------------------------------------------------ #
    # Telemetry
    # ------------------------------------------------------------------ #

    async def ingest_telemetry(
        self,
        events: list[TelemetryBatchItem],
        tenant_id: str | None = None,
    ) -> IngestResponse:
        body = IngestTelemetryRequest(events=events, tenant_id=tenant_id)
        data = await self._request("POST", "/telemetry/mobile", json=body.model_dump(exclude_none=True))
        return IngestResponse.model_validate(data)

    async def ingest_telemetry_error(
        self,
        error_type: str,
        message: str,
        stack_trace: str | None = None,
        device: DeviceContext | None = None,
        tenant_id: str | None = None,
        instance_id: str | None = None,
        sequence_name: str | None = None,
    ) -> IngestResponse:
        body = IngestErrorRequest(
            error_type=error_type,
            message=message,
            stack_trace=stack_trace,
            device=device,
            tenant_id=tenant_id,
            instance_id=instance_id,
            sequence_name=sequence_name,
        )
        data = await self._request("POST", "/telemetry/mobile/errors", json=body.model_dump(exclude_none=True))
        return IngestResponse.model_validate(data)

    async def telemetry_dashboard(
        self,
        query_type: str,
        tenant_id: str | None = None,
        start_time: str | None = None,
        end_time: str | None = None,
    ) -> DashboardResponse:
        params: dict[str, Any] = {"query_type": query_type}
        if tenant_id is not None:
            params["tenant_id"] = tenant_id
        if start_time is not None:
            params["start_time"] = start_time
        if end_time is not None:
            params["end_time"] = end_time
        data = await self._request("GET", "/telemetry/mobile/dashboard", params=params)
        return DashboardResponse.model_validate(data)

    # ------------------------------------------------------------------ #
    # Rollback Policies
    # ------------------------------------------------------------------ #

    async def create_rollback_policy(
        self,
        tenant_id: str,
        sequence_name: str,
        error_rate_threshold: float,
        time_window_secs: int,
        cooldown_secs: int = 3600,
        confirmation_window_secs: int = 60,
        webhook_url: str | None = None,
    ) -> RollbackPolicy:
        body = {
            "tenant_id": tenant_id,
            "sequence_name": sequence_name,
            "error_rate_threshold": error_rate_threshold,
            "time_window_secs": time_window_secs,
            "cooldown_secs": cooldown_secs,
            "confirmation_window_secs": confirmation_window_secs,
        }
        if webhook_url is not None:
            body["webhook_url"] = webhook_url
        data = await self._request("POST", "/rollback-policies", json=body)
        return RollbackPolicy.model_validate(data)

    async def list_rollback_policies(
        self, tenant_id: str | None = None
    ) -> list[RollbackPolicy]:
        params: dict[str, Any] = {}
        if tenant_id is not None:
            params["tenant_id"] = tenant_id
        data = await self._request("GET", "/rollback-policies", params=params)
        return [RollbackPolicy.model_validate(d) for d in data] if data else []

    async def get_rollback_policy(self, name: str) -> RollbackPolicy:
        data = await self._request("GET", f"/rollback-policies/{self._e(name)}")
        return RollbackPolicy.model_validate(data)

    async def delete_rollback_policy(self, name: str) -> None:
        await self._request("DELETE", f"/rollback-policies/{self._e(name)}")
