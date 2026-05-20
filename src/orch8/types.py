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
    queue_name: str | None = None
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


# --- Resource Pools ---


class ResourcePool(BaseModel):
    id: str
    tenant_id: str
    name: str
    max_size: int
    current_size: int = 0
    config: Any = None
    created_at: str
    updated_at: str


class PoolResource(BaseModel):
    id: str
    pool_id: str
    resource_key: str
    state: str
    data: Any = None
    locked_by: str | None = None
    locked_at: str | None = None
    created_at: str
    updated_at: str


# --- Credentials ---


class Credential(BaseModel):
    id: str
    tenant_id: str
    name: str
    credential_type: str
    metadata: dict[str, Any] = {}
    created_at: str
    updated_at: str


# --- Mobile Sync ---


class HumanChoice(BaseModel):
    label: str
    value: str


class StatusUpdatePayload(BaseModel):
    instance_id: str
    sequence_name: str
    state: str
    current_step: str | None = None
    handler: str | None = None
    timestamp: str | None = None
    context_summary: Any = None
    steps: list[Any] = []


class ApprovalRequestPayload(BaseModel):
    instance_id: str
    block_id: str
    sequence_name: str
    prompt: str
    choices: list[HumanChoice] = []
    store_as: str | None = None
    timeout_seconds: int | None = None
    metadata: Any = None


class StepDelegationPayload(BaseModel):
    request_id: str
    instance_id: str
    block_id: str
    handler: str
    params: Any = None


class CommandPayload(BaseModel):
    id: str
    type: str
    payload: Any = None


class SyncRequest(BaseModel):
    device_id: str
    status_updates: list[StatusUpdatePayload] = []
    approval_requests: list[ApprovalRequestPayload] = []
    step_delegations: list[StepDelegationPayload] = []
    command_acks: list[str] = []


class SyncResponse(BaseModel):
    commands: list[CommandPayload] = []
    sync_interval_secs: int = 30


class RegisterDeviceRequest(BaseModel):
    device_id: str
    push_token: str | None = None
    platform: str
    app_version: str | None = None


class ResolveApprovalRequest(BaseModel):
    output: Any = None


class CreateCommandRequest(BaseModel):
    device_id: str
    command_type: str
    payload: Any = None


class MobileDevice(BaseModel):
    id: str
    device_id: str
    push_token: str | None = None
    platform: str
    app_version: str | None = None
    created_at: str


class MobileStatus(BaseModel):
    instance_id: str
    state: str
    current_step: str | None = None
    updated_at: str


class MobileDevicesResponse(BaseModel):
    items: list[MobileDevice] = []
    total: int = 0


class MobileApprovalsResponse(BaseModel):
    items: list[ApprovalItem] = []
    total: int = 0


class MobileStatusResponse(BaseModel):
    items: list[MobileStatus] = []
    total: int = 0


# --- Telemetry ---


class DeviceContext(BaseModel):
    device_id: str
    os_name: str | None = None
    os_version: str | None = None
    app_version: str | None = None
    sdk_version: str | None = None


class TelemetryBatchItem(BaseModel):
    event_type: str
    payload: Any = None
    timestamp: str | None = None
    device: DeviceContext | None = None


class IngestTelemetryRequest(BaseModel):
    events: list[TelemetryBatchItem]
    tenant_id: str | None = None


class IngestErrorRequest(BaseModel):
    error_type: str
    message: str
    stack_trace: str | None = None
    device: DeviceContext | None = None
    tenant_id: str | None = None
    instance_id: str | None = None
    sequence_name: str | None = None


class IngestResponse(BaseModel):
    accepted: int = 0


class DashboardRow(BaseModel):
    dimension: str
    count: int = 0
    percentage: float = 0.0


class DashboardResponse(BaseModel):
    rows: list[DashboardRow] = []


# --- Rollback Policies ---


class RollbackPolicy(BaseModel):
    id: int
    tenant_id: str
    sequence_name: str
    error_rate_threshold: float
    time_window_secs: int
    enabled: bool = True
    cooldown_secs: int = 3600
    confirmation_window_secs: int = 60
    webhook_url: str | None = None
    created_at: str
    updated_at: str


# --- Approvals ---


class ApprovalItem(BaseModel):
    instance_id: str
    tenant_id: str
    namespace: str
    sequence_id: str
    sequence_name: str
    block_id: str
    prompt: str
    choices: list[HumanChoice] = []
    store_as: str | None = None
    timeout_seconds: int | None = None
    escalation_handler: str | None = None
    waiting_since: str
    deadline: str | None = None
    metadata: Any = None
    allow_comment: bool = False


class ApprovalsResponse(BaseModel):
    items: list[ApprovalItem] = []
    total: int = 0


# --- Typed request payloads ---


class CreateInstanceRequest(BaseModel):
    sequence_id: str
    tenant_id: str | None = None
    namespace: str | None = None
    state: str | None = None
    priority: int | None = None
    timezone: str | None = None
    metadata: Any = None
    context: dict[str, Any] | None = None
    concurrency_key: str | None = None
    max_concurrency: int | None = None
    idempotency_key: str | None = None
    session_id: str | None = None
    parent_instance_id: str | None = None
    next_fire_at: str | None = None


class UpdateStateRequest(BaseModel):
    state: str
    next_fire_at: str | None = None


class UpdateContextRequest(BaseModel):
    context: dict[str, Any]


class SendSignalRequest(BaseModel):
    signal_type: str
    payload: Any = None


class CreateCronRequest(BaseModel):
    tenant_id: str
    namespace: str
    sequence_id: str
    cron_expr: str
    timezone: str | None = None
    enabled: bool | None = None
    metadata: Any = None


class UpdateCronRequest(BaseModel):
    cron_expr: str | None = None
    timezone: str | None = None
    enabled: bool | None = None
    metadata: Any = None


class CreateTriggerRequest(BaseModel):
    slug: str
    sequence_name: str
    tenant_id: str
    namespace: str
    version: int | None = None
    enabled: bool | None = None
    secret: str | None = None
    trigger_type: str | None = None
    config: Any = None


class CreateCredentialRequest(BaseModel):
    tenant_id: str
    name: str
    credential_type: str
    metadata: dict[str, Any] | None = None
    value: Any = None


class UpdateCredentialRequest(BaseModel):
    name: str | None = None
    credential_type: str | None = None
    metadata: dict[str, Any] | None = None
    value: Any = None


class CreateSessionRequest(BaseModel):
    tenant_id: str
    session_key: str
    data: Any = None
    state: str | None = None
    expires_at: str | None = None


class CreatePoolRequest(BaseModel):
    tenant_id: str
    name: str
    strategy: str = "round_robin"
    config: Any = None


class AddResourceRequest(BaseModel):
    resource_key: str
    name: str
    weight: int = 1
    daily_cap: int = 0
    warmup_start: str | None = None
    warmup_days: int = 0
    warmup_start_cap: int = 0
    data: Any = None


class UpdateResourceRequest(BaseModel):
    name: str | None = None
    weight: int | None = None
    enabled: bool | None = None
    daily_cap: int | None = None
    warmup_start: str | None = None
    warmup_days: int | None = None
    warmup_start_cap: int | None = None
    data: Any = None
