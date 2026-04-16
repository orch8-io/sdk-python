"""Orch8 SDK types — Pydantic models matching the REST API JSON shapes."""
from __future__ import annotations

from typing import Any

from pydantic import BaseModel


# --- Core models ---


class AuditEntry(BaseModel):
    timestamp: str
    event: str
    details: Any = None


class ExecutionContext(BaseModel):
    data: dict[str, Any] = {}
    config: dict[str, Any] = {}
    audit: list[AuditEntry] = []
    runtime: dict[str, Any] = {}


class SequenceDefinition(BaseModel):
    id: str
    tenant_id: str
    namespace: str
    name: str
    version: int
    deprecated: bool = False
    blocks: list[Any] = []
    interceptors: list[Any] | None = None
    created_at: str


class TaskInstance(BaseModel):
    id: str
    sequence_id: str
    tenant_id: str
    namespace: str
    state: str
    next_fire_at: str | None = None
    priority: int = 0
    timezone: str = "UTC"
    metadata: Any = None
    context: ExecutionContext = ExecutionContext()
    concurrency_key: str | None = None
    max_concurrency: int | None = None
    idempotency_key: str | None = None
    session_id: str | None = None
    parent_instance_id: str | None = None
    created_at: str
    updated_at: str


class ExecutionNode(BaseModel):
    id: str
    instance_id: str
    block_id: str
    parent_id: str | None = None
    block_type: str
    branch_index: int | None = None
    state: str
    started_at: str | None = None
    completed_at: str | None = None


class StepOutput(BaseModel):
    id: str
    instance_id: str
    block_id: str
    output: Any = None
    output_ref: str | None = None
    output_size: int = 0
    attempt: int = 0
    error_message: str | None = None
    created_at: str


class Checkpoint(BaseModel):
    id: str
    instance_id: str
    checkpoint_data: Any = None
    created_at: str


# --- Scheduling / Triggers ---


class CronSchedule(BaseModel):
    id: str
    tenant_id: str
    namespace: str
    sequence_id: str
    cron_expr: str
    timezone: str = "UTC"
    enabled: bool = True
    metadata: Any = None
    last_triggered_at: str | None = None
    next_fire_at: str | None = None
    created_at: str
    updated_at: str


class TriggerDef(BaseModel):
    slug: str
    sequence_name: str
    version: int | None = None
    tenant_id: str
    namespace: str
    enabled: bool = True
    secret: str | None = None
    trigger_type: str = "webhook"
    config: Any = None
    created_at: str
    updated_at: str


# --- Plugins ---


class PluginDef(BaseModel):
    name: str
    plugin_type: str
    source: str
    tenant_id: str
    enabled: bool = True
    config: Any = None
    description: str | None = None
    created_at: str
    updated_at: str


# --- Sessions ---


class Session(BaseModel):
    id: str
    tenant_id: str
    session_key: str
    data: Any = None
    state: str = "active"
    created_at: str
    updated_at: str
    expires_at: str | None = None


# --- Workers ---


class WorkerTask(BaseModel):
    id: str
    instance_id: str
    block_id: str
    handler_name: str
    params: Any = None
    context: Any = None
    attempt: int = 0
    timeout_ms: int | None = None
    state: str = "pending"
    worker_id: str | None = None
    claimed_at: str | None = None
    heartbeat_at: str | None = None
    completed_at: str | None = None
    output: Any = None
    error_message: str | None = None
    error_retryable: bool | None = None
    created_at: str


# --- Cluster ---


class ClusterNode(BaseModel):
    id: str
    address: str
    state: str
    last_heartbeat: str


# --- Circuit Breaker ---


class CircuitBreaker(BaseModel):
    handler: str
    state: str
    failure_count: int = 0
    last_failure: str | None = None


# --- Response types ---


class FireTriggerResponse(BaseModel):
    instance_id: str
    trigger: str
    sequence_name: str


class BulkResponse(BaseModel):
    updated: int


class BatchCreateResponse(BaseModel):
    created: int


class HealthResponse(BaseModel):
    status: str
