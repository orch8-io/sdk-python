"""A pure-Python, in-memory stand-in for the Orch8 engine HTTP API.

It implements the worker protocol (poll, queue poll, complete, fail,
heartbeat — with lease epochs), instance endpoints, a linear-step sequence
interpreter, and the jobs API, all driven by a virtual clock so tests can skip
time instead of sleeping. Use it through an in-process ``httpx`` transport
(:meth:`FakeEngine.client`) or a real loopback HTTP server
(:meth:`FakeEngine.serve`).

Scope: sequences may contain only top-level ``step`` blocks (``retry``,
``delay.duration``, ``queue_name``, ``timeout`` and ``params`` are honoured;
the built-in ``noop`` handler completes inline). For full DSL semantics use
:class:`orch8.testing.NativeEnvironment`, which runs the real engine.
"""
from __future__ import annotations

import json
import re
import threading
import uuid
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qs, unquote, urlsplit

import httpx

if TYPE_CHECKING:
    from ..client import Orch8Client
    from ..worker import Handler

_TERMINAL = {"completed", "failed", "cancelled"}
_UNSUPPORTED_STEP_KEYS = {"when", "wait_for_input", "send_window", "compensation", "cache_key"}


class FakeClock:
    """Virtual UTC clock; :meth:`advance` skips time without sleeping."""

    def __init__(self, start: datetime | None = None) -> None:
        self._now = start or datetime(2026, 1, 1, tzinfo=timezone.utc)

    def now(self) -> datetime:
        return self._now

    def advance(self, seconds: float) -> None:
        self._now += timedelta(seconds=seconds)

    def set(self, when: datetime) -> None:
        self._now = when


def _iso(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


class _HttpError(Exception):
    def __init__(self, status: int, message: str) -> None:
        self.status = status
        self.message = message
        super().__init__(message)


@dataclass
class _Response:
    status: int
    body: Any = None


@dataclass
class FakeTask:
    id: str
    instance_id: str
    block_id: str
    handler_name: str
    params: Any
    context: Any
    queue_name: str | None = None
    timeout_ms: int | None = None
    attempt: int = 0
    state: str = "pending"
    worker_id: str | None = None
    claim_epoch: int = 0
    claimed_at: datetime | None = None
    heartbeat_at: datetime | None = None
    available_at: datetime | None = None
    completed_at: datetime | None = None
    created_at: datetime | None = None
    output: Any = None
    error_message: str | None = None
    error_retryable: bool | None = None
    resume_checkpoint: Any = None
    checkpoint_seq: int = 0

    def to_json(self) -> dict[str, Any]:
        def ts(v: datetime | None) -> str | None:
            return _iso(v) if v else None

        return {
            "id": self.id,
            "instance_id": self.instance_id,
            "block_id": self.block_id,
            "handler_name": self.handler_name,
            "queue_name": self.queue_name,
            "params": self.params,
            "context": self.context,
            "attempt": self.attempt,
            "timeout_ms": self.timeout_ms,
            "state": self.state,
            "worker_id": self.worker_id,
            "claim_epoch": self.claim_epoch,
            "claimed_at": ts(self.claimed_at),
            "heartbeat_at": ts(self.heartbeat_at),
            "completed_at": ts(self.completed_at),
            "output": self.output,
            "error_message": self.error_message,
            "error_retryable": self.error_retryable,
            "resume_checkpoint": self.resume_checkpoint,
            "checkpoint_seq": self.checkpoint_seq,
            "created_at": ts(self.created_at),
        }


@dataclass
class FakeInstance:
    id: str
    sequence_id: str | None
    tenant_id: str
    namespace: str
    blocks: list[dict[str, Any]]
    data: dict[str, Any]
    state: str = "scheduled"
    cursor: int = 0
    attempts: dict[str, int] = field(default_factory=dict)
    outputs: list[dict[str, Any]] = field(default_factory=list)
    signals: list[dict[str, Any]] = field(default_factory=list)
    metadata: Any = None
    idempotency_key: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    error: str | None = None
    #: True when the instance failed after exhausting retryable attempts.
    exhausted: bool = False
    #: Wake-up time of a pending durable timer (delayed ``noop`` step).
    timer_at: datetime | None = None
    job: dict[str, Any] | None = None

    def to_json(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "sequence_id": self.sequence_id or "",
            "tenant_id": self.tenant_id,
            "namespace": self.namespace,
            "state": self.state,
            "next_fire_at": _iso(self.timer_at) if self.timer_at else None,
            "priority": 0,
            "timezone": "UTC",
            "metadata": self.metadata,
            "context": {"data": self.data, "config": {}, "audit": [], "runtime": {}},
            "idempotency_key": self.idempotency_key,
            "created_at": _iso(self.created_at) if self.created_at else None,
            "updated_at": _iso(self.updated_at) if self.updated_at else None,
        }


class FakeEngine:
    """In-memory Orch8 engine for tests. Thread-safe."""

    def __init__(
        self,
        *,
        tenant_id: str = "test",
        lease_secs: int = 30,
        heartbeat_interval_secs: int = 10,
        clock: FakeClock | None = None,
    ) -> None:
        self.tenant_id = tenant_id
        self.lease_secs = lease_secs
        self.heartbeat_interval_secs = heartbeat_interval_secs
        self.clock = clock or FakeClock()
        self.sequences: dict[str, dict[str, Any]] = {}
        self.instances: dict[str, FakeInstance] = {}
        self.tasks: dict[str, FakeTask] = {}
        self.requests: list[tuple[str, str, Any]] = []
        self._jobs_by_key: dict[str, str] = {}
        self._lock = threading.RLock()

    # ------------------------------------------------------------------ #
    # Test helpers
    # ------------------------------------------------------------------ #

    def client(self, **kwargs: Any) -> Orch8Client:
        """An :class:`Orch8Client` wired to this engine in-process."""
        from ..client import Orch8Client

        kwargs.setdefault("tenant_id", self.tenant_id)
        return Orch8Client("http://orch8.fake", transport=self.transport(), **kwargs)

    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self._httpx_handler)

    @contextmanager
    def serve(self, host: str = "127.0.0.1", port: int = 0) -> Iterator[str]:
        """Run a loopback HTTP server; yields its base URL."""
        engine = self

        class Handler(BaseHTTPRequestHandler):
            def _dispatch(self) -> None:
                length = int(self.headers.get("Content-Length") or 0)
                raw = self.rfile.read(length) if length else b""
                resp = engine.handle(self.command, self.path, raw)
                payload = b"" if resp.body is None else json.dumps(resp.body).encode()
                self.send_response(resp.status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            do_GET = do_POST = do_PATCH = do_PUT = do_DELETE = _dispatch

            def log_message(self, *args: Any) -> None:
                pass

        server = ThreadingHTTPServer((host, port), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            yield f"http://{host}:{server.server_address[1]}"
        finally:
            server.shutdown()
            server.server_close()

    def advance(self, seconds: float) -> None:
        """Skip virtual time; expired leases are reclaimed."""
        with self._lock:
            self.clock.advance(seconds)
            self._reap()

    def enqueue_task(
        self,
        handler_name: str,
        params: Any = None,
        *,
        queue_name: str | None = None,
        instance_id: str | None = None,
        block_id: str | None = None,
        context: Any = None,
        timeout_ms: int | None = None,
    ) -> FakeTask:
        """Create a pending worker task directly (no instance required)."""
        with self._lock:
            task = FakeTask(
                id=str(uuid.uuid4()),
                instance_id=instance_id or str(uuid.uuid4()),
                block_id=block_id or handler_name,
                handler_name=handler_name,
                params={} if params is None else params,
                context=context if context is not None else {"data": {}},
                queue_name=queue_name,
                timeout_ms=timeout_ms,
                created_at=self.clock.now(),
                available_at=self.clock.now(),
            )
            self.tasks[task.id] = task
            return task

    def task(self, task_id: str) -> FakeTask:
        return self.tasks[task_id]

    def instance(self, instance_id: str) -> FakeInstance:
        return self.instances[instance_id]

    def pending_tasks(self, handler_name: str | None = None) -> list[FakeTask]:
        with self._lock:
            return [
                t
                for t in self.tasks.values()
                if t.state == "pending" and handler_name in (None, t.handler_name)
            ]

    def next_available_at(self, handlers: Mapping[str, Any] | None = None) -> datetime | None:
        """Earliest future moment at which a task or timer becomes due."""
        with self._lock:
            times = [
                t.available_at
                for t in self.tasks.values()
                if t.state == "pending"
                and t.available_at
                and (handlers is None or t.handler_name in handlers)
            ]
            times += [
                i.timer_at
                for i in self.instances.values()
                if i.timer_at is not None and i.state not in _TERMINAL
            ]
            return min(times) if times else None

    async def process(
        self,
        client: Orch8Client,
        handlers: Mapping[str, Handler],
        *,
        worker_id: str = "fake-worker",
        skip_time: bool = True,
        max_tasks: int = 10_000,
    ) -> int:
        """Run every claimable task through ``handlers`` using the real SDK
        acknowledgement path. With ``skip_time`` the clock jumps to delayed or
        backed-off tasks instead of waiting. Returns the number executed."""
        from ..worker import execute_task

        executed = 0
        while executed < max_tasks:
            claimed = False
            for name, handler in handlers.items():
                for queue in {t.queue_name for t in self.pending_tasks(name)}:
                    if queue is None:
                        tasks = await client.poll_tasks(name, worker_id, limit=10)
                    else:
                        tasks = await client.poll_tasks_from_queue(queue, name, worker_id, limit=10)
                    for task in tasks:
                        claimed = True
                        executed += 1
                        await execute_task(client, worker_id, handler, task)
            if claimed:
                continue
            upcoming = self.next_available_at(handlers)
            if skip_time and upcoming is not None and upcoming > self.clock.now():
                self.advance((upcoming - self.clock.now()).total_seconds())
                continue
            break
        return executed

    async def run(
        self,
        client: Orch8Client,
        sequence: Mapping[str, Any],
        handlers: Mapping[str, Handler],
        *,
        input: Mapping[str, Any] | None = None,
    ) -> FakeInstance:
        """Create ``sequence``, start an instance and drive it to a terminal
        state with ``handlers``, skipping virtual time."""
        seq = await client.create_sequence(dict(sequence))
        inst = await client.create_instance(
            {"sequence_id": seq.id, "context": {"data": dict(input or {})}}
        )
        await self.process(client, handlers)
        return self.instances[inst.id]

    # ------------------------------------------------------------------ #
    # Transport plumbing
    # ------------------------------------------------------------------ #

    def _httpx_handler(self, request: httpx.Request) -> httpx.Response:
        resp = self.handle(request.method, str(request.url.raw_path, "ascii"), request.content)
        if resp.body is None:
            return httpx.Response(resp.status)
        return httpx.Response(resp.status, json=resp.body)

    def handle(self, method: str, raw_path: str, body: bytes) -> _Response:
        parts = urlsplit(raw_path)
        path = parts.path
        if path.startswith("/api/v1/"):
            path = path[len("/api/v1"):]
        query = {k: v[-1] for k, v in parse_qs(parts.query).items()}
        try:
            payload = json.loads(body) if body else None
        except ValueError:
            return _Response(400, {"error": "invalid JSON"})
        with self._lock:
            self.requests.append((method, path, payload))
            self._reap()
            try:
                return self._route(method.upper(), path, query, payload)
            except _HttpError as err:
                return _Response(err.status, {"error": err.message})

    _ROUTES: list[tuple[str, re.Pattern[str], str]] = [
        (m, re.compile(f"^{p}$"), fn)
        for m, p, fn in [
            ("GET", r"/health", "_health"),
            ("POST", r"/sequences", "_create_sequence"),
            ("GET", r"/sequences/(?P<id>[^/]+)", "_get_sequence"),
            ("POST", r"/instances", "_create_instance"),
            ("GET", r"/instances", "_list_instances"),
            ("GET", r"/instances/(?P<id>[^/]+)", "_get_instance"),
            ("GET", r"/instances/(?P<id>[^/]+)/outputs", "_get_outputs"),
            ("PATCH", r"/instances/(?P<id>[^/]+)/state", "_update_state"),
            ("POST", r"/instances/(?P<id>[^/]+)/signals", "_signal"),
            ("POST", r"/workers/tasks/poll", "_poll"),
            ("POST", r"/workers/tasks/poll/queue", "_poll_queue"),
            ("GET", r"/workers/tasks", "_list_tasks"),
            ("POST", r"/workers/tasks/(?P<id>[^/]+)/complete", "_complete"),
            ("POST", r"/workers/tasks/(?P<id>[^/]+)/fail", "_fail"),
            ("POST", r"/workers/tasks/(?P<id>[^/]+)/heartbeat", "_heartbeat"),
            ("POST", r"/jobs", "_create_job"),
            ("GET", r"/jobs", "_list_jobs"),
            ("GET", r"/jobs/(?P<id>[^/]+)", "_get_job"),
            ("DELETE", r"/jobs/(?P<id>[^/]+)", "_cancel_job"),
        ]
    ]

    def _route(self, method: str, path: str, query: dict[str, str], body: Any) -> _Response:
        path_matched = False
        for route_method, pattern, fn in self._ROUTES:
            match = pattern.match(path)
            if not match:
                continue
            path_matched = True
            if route_method == method:
                args = {k: unquote(v) for k, v in match.groupdict().items()}
                return getattr(self, fn)(query=query, body=body, **args)
        if path_matched:
            raise _HttpError(405, f"{method} not allowed on {path}")
        raise _HttpError(404, f"fake engine does not implement {method} {path}")

    # ------------------------------------------------------------------ #
    # Engine behaviour
    # ------------------------------------------------------------------ #

    def _reap(self) -> None:
        now = self.clock.now()
        for inst in list(self.instances.values()):
            if inst.timer_at is not None and inst.timer_at <= now and inst.state not in _TERMINAL:
                inst.timer_at = None
                self._record_output(inst, inst.blocks[inst.cursor]["id"], {}, 0)
                inst.cursor += 1
                self._advance_instance(inst)
        for task in self.tasks.values():
            if task.state != "claimed":
                continue
            last = task.heartbeat_at or task.claimed_at
            if last and now - last > timedelta(seconds=self.lease_secs):
                task.state = "pending"
                task.worker_id = None
                task.available_at = now

    def _health(self, **_: Any) -> _Response:
        return _Response(200, {"status": "ok"})

    def _create_sequence(self, body: Any, **_: Any) -> _Response:
        if not isinstance(body, dict) or not isinstance(body.get("blocks"), list):
            raise _HttpError(400, "sequence must have blocks")
        for block in body["blocks"]:
            if not isinstance(block, dict) or block.get("type") != "step":
                raise _HttpError(
                    422,
                    "fake engine supports only top-level step blocks; "
                    "use orch8.testing.NativeEnvironment for full DSL semantics",
                )
            unsupported = _UNSUPPORTED_STEP_KEYS & set(block)
            if unsupported:
                raise _HttpError(
                    422, f"fake engine does not evaluate step fields: {sorted(unsupported)}"
                )
        seq_id = body.get("id") or str(uuid.uuid4())
        self.sequences[seq_id] = {**body, "id": seq_id}
        return _Response(201, {"id": seq_id})

    def _get_sequence(self, id: str, **_: Any) -> _Response:
        if id not in self.sequences:
            raise _HttpError(404, f"sequence {id}")
        return _Response(200, self.sequences[id])

    def _new_instance(
        self,
        blocks: list[dict[str, Any]],
        data: dict[str, Any],
        *,
        sequence_id: str | None,
        tenant_id: str | None = None,
        namespace: str | None = None,
        metadata: Any = None,
        idempotency_key: str | None = None,
    ) -> FakeInstance:
        now = self.clock.now()
        inst = FakeInstance(
            id=str(uuid.uuid4()),
            sequence_id=sequence_id,
            tenant_id=tenant_id or self.tenant_id,
            namespace=namespace or "default",
            blocks=blocks,
            data=data,
            metadata=metadata,
            idempotency_key=idempotency_key,
            created_at=now,
            updated_at=now,
        )
        self.instances[inst.id] = inst
        self._advance_instance(inst)
        return inst

    def _create_instance(self, body: Any, **_: Any) -> _Response:
        if not isinstance(body, dict) or body.get("sequence_id") not in self.sequences:
            raise _HttpError(404, "sequence not found")
        seq = self.sequences[body["sequence_id"]]
        context = body.get("context") or {}
        data = context.get("data", context) if isinstance(context, dict) else {}
        inst = self._new_instance(
            [dict(b) for b in seq["blocks"]],
            dict(data),
            sequence_id=seq["id"],
            tenant_id=body.get("tenant_id"),
            namespace=body.get("namespace"),
            metadata=body.get("metadata"),
            idempotency_key=body.get("idempotency_key"),
        )
        return _Response(201, inst.to_json())

    def _list_instances(self, query: dict[str, str], **_: Any) -> _Response:
        items = [
            i.to_json()
            for i in self.instances.values()
            if query.get("state") in (None, i.state)
        ]
        return _Response(200, items)

    def _instance_or_404(self, id: str) -> FakeInstance:
        if id not in self.instances:
            raise _HttpError(404, f"instance {id}")
        return self.instances[id]

    def _get_instance(self, id: str, **_: Any) -> _Response:
        return _Response(200, self._instance_or_404(id).to_json())

    def _get_outputs(self, id: str, **_: Any) -> _Response:
        return _Response(200, self._instance_or_404(id).outputs)

    def _update_state(self, id: str, body: Any, **_: Any) -> _Response:
        inst = self._instance_or_404(id)
        state = (body or {}).get("state")
        if state == "cancelled":
            self._finish(inst, "cancelled")
            for task in self.tasks.values():
                if task.instance_id == inst.id and task.state in ("pending", "claimed"):
                    task.state = "failed"
                    task.error_message = "instance cancelled"
        elif isinstance(state, str):
            inst.state = state
        return _Response(200, inst.to_json())

    def _signal(self, id: str, body: Any, **_: Any) -> _Response:
        inst = self._instance_or_404(id)
        inst.signals.append(body or {})
        return _Response(200, {"id": str(uuid.uuid4())})

    def _advance_instance(self, inst: FakeInstance) -> None:
        """Schedule the next step or finish the instance."""
        while inst.cursor < len(inst.blocks):
            block = inst.blocks[inst.cursor]
            block_id = block["id"]
            attempt = inst.attempts.get(block_id, 0)
            available = self.clock.now()
            delay = block.get("delay") or {}
            if attempt == 0 and delay.get("duration"):
                available += timedelta(milliseconds=delay["duration"])
            if block.get("handler") == "noop":
                if available > self.clock.now():
                    inst.timer_at = available
                    inst.state = "scheduled"
                    return
                self._record_output(inst, block_id, {}, attempt)
                inst.cursor += 1
                continue
            inst.state = "running" if inst.cursor or attempt else "scheduled"
            task = FakeTask(
                id=str(uuid.uuid4()),
                instance_id=inst.id,
                block_id=block_id,
                handler_name=block["handler"],
                params=block.get("params") or {},
                context={"data": dict(inst.data)},
                queue_name=block.get("queue_name"),
                timeout_ms=block.get("timeout"),
                attempt=attempt,
                created_at=self.clock.now(),
                available_at=available,
            )
            self.tasks[task.id] = task
            return
        self._finish(inst, "completed")

    def _record_output(self, inst: FakeInstance, block_id: str, output: Any, attempt: int) -> None:
        if isinstance(output, dict):
            inst.data.update(output)
        inst.outputs.append(
            {
                "id": str(uuid.uuid4()),
                "instance_id": inst.id,
                "block_id": block_id,
                "output": output,
                "output_ref": None,
                "output_size": len(json.dumps(output)),
                "attempt": attempt,
                "created_at": _iso(self.clock.now()),
            }
        )
        inst.updated_at = self.clock.now()

    def _finish(self, inst: FakeInstance, state: str, error: str | None = None) -> None:
        inst.state = state
        inst.error = error
        inst.updated_at = self.clock.now()

    def _claim(self, body: Any, queue: str | None) -> _Response:
        if not isinstance(body, dict) or not body.get("handler_name") or not body.get("worker_id"):
            raise _HttpError(400, "handler_name and worker_id are required")
        limit = min(int(body.get("limit") or 1), 1000)
        now = self.clock.now()
        claimed: list[dict[str, Any]] = []
        for task in self.tasks.values():
            if len(claimed) >= limit:
                break
            if (
                task.state == "pending"
                and task.handler_name == body["handler_name"]
                and task.queue_name == queue
                and (task.available_at is None or task.available_at <= now)
            ):
                task.state = "claimed"
                task.worker_id = body["worker_id"]
                task.claim_epoch += 1
                task.claimed_at = now
                task.heartbeat_at = None
                claimed.append(task.to_json())
        return _Response(
            200,
            {
                "tasks": claimed,
                "lease_secs": self.lease_secs,
                "heartbeat_interval_secs": self.heartbeat_interval_secs,
                "poll_after_ms": 0 if claimed else 1000,
            },
        )

    def _poll(self, body: Any, **_: Any) -> _Response:
        return self._claim(body, None)

    def _poll_queue(self, body: Any, **_: Any) -> _Response:
        queue = (body or {}).get("queue_name")
        if not queue:
            raise _HttpError(400, "queue_name is required")
        return self._claim(body, queue)

    def _list_tasks(self, query: dict[str, str], **_: Any) -> _Response:
        return _Response(
            200,
            [
                t.to_json()
                for t in self.tasks.values()
                if query.get("state") in (None, t.state)
                and query.get("handler_name") in (None, t.handler_name)
            ],
        )

    def _leased(self, id: str, body: Any, *, allow_completed: bool = False) -> FakeTask:
        task = self.tasks.get(id)
        if task is None:
            raise _HttpError(404, f"worker_task {id}")
        body = body or {}
        same_lease = task.worker_id == body.get("worker_id") and (
            body.get("claim_epoch") is None or body.get("claim_epoch") == task.claim_epoch
        )
        if not same_lease or not (
            task.state == "claimed" or (allow_completed and task.state == "completed")
        ):
            raise _HttpError(409, "worker task lease changed")
        return task

    def _complete(self, id: str, body: Any, **_: Any) -> _Response:
        task = self._leased(id, body, allow_completed=True)
        if task.state == "completed":
            return _Response(200, task.to_json())
        output = (body or {}).get("output")
        task.state = "completed"
        task.output = output
        task.completed_at = self.clock.now()
        self._task_finished(task, ok=True, output=output)
        return _Response(200, task.to_json())

    def _fail(self, id: str, body: Any, **_: Any) -> _Response:
        task = self._leased(id, body)
        body = body or {}
        task.state = "failed"
        task.error_message = body.get("message") or body.get("error")
        task.error_retryable = bool(body.get("retryable"))
        task.completed_at = self.clock.now()
        self._task_finished(task, ok=False, retryable=task.error_retryable)
        return _Response(200, task.to_json())

    def _heartbeat(self, id: str, body: Any, **_: Any) -> _Response:
        task = self._leased(id, body)
        body = body or {}
        task.heartbeat_at = self.clock.now()
        if "checkpoint" in body:
            seq = body.get("checkpoint_seq")
            if not isinstance(seq, int) or seq <= task.checkpoint_seq:
                raise _HttpError(409, "stale checkpoint_seq")
            task.resume_checkpoint = body["checkpoint"]
            task.checkpoint_seq = seq
        return _Response(200, {"checkpoint_seq": task.checkpoint_seq})

    def _task_finished(
        self, task: FakeTask, *, ok: bool, output: Any = None, retryable: bool = False
    ) -> None:
        inst = self.instances.get(task.instance_id)
        if inst is None or inst.state in _TERMINAL:
            return
        block = inst.blocks[inst.cursor]
        if ok:
            self._record_output(inst, task.block_id, output, task.attempt)
            inst.attempts.pop(task.block_id, None)
            inst.cursor += 1
            self._advance_instance(inst)
            return
        retry = block.get("retry") or {}
        attempts = task.attempt + 1
        if retryable and attempts < int(retry.get("max_attempts") or 1):
            inst.attempts[task.block_id] = attempts
            backoff = int(retry.get("initial_backoff") or 0) * (
                float(retry.get("backoff_multiplier") or 2.0) ** (attempts - 1)
            )
            if retry.get("max_backoff"):
                backoff = min(backoff, int(retry["max_backoff"]))
            retry_task = FakeTask(
                id=str(uuid.uuid4()),
                instance_id=inst.id,
                block_id=task.block_id,
                handler_name=task.handler_name,
                params=task.params,
                context={"data": dict(inst.data)},
                queue_name=task.queue_name,
                timeout_ms=task.timeout_ms,
                attempt=attempts,
                created_at=self.clock.now(),
                available_at=self.clock.now() + timedelta(milliseconds=backoff),
                # The engine carries durable checkpoints onto the retry row.
                resume_checkpoint=task.resume_checkpoint,
                checkpoint_seq=task.checkpoint_seq,
            )
            self.tasks[retry_task.id] = retry_task
            return
        inst.exhausted = retryable
        self._finish(inst, "failed", task.error_message)

    # ------------------------------------------------------------------ #
    # Jobs
    # ------------------------------------------------------------------ #

    def _job_json(self, inst: FakeInstance) -> dict[str, Any]:
        job = inst.job or {}
        tasks = sorted(
            (t for t in self.tasks.values() if t.instance_id == inst.id),
            key=lambda t: t.attempt,
        )
        current = tasks[-1] if tasks else None
        if inst.state == "completed":
            status = "completed"
        elif inst.state == "cancelled":
            status = "cancelled"
        elif inst.state == "failed":
            status = "dead_lettered" if inst.exhausted else "failed"
        elif current is not None and current.state == "claimed":
            status = "running"
        else:
            status = "scheduled"
        return {
            "id": inst.id,
            "instance_id": inst.id,
            "handler": job.get("handler"),
            "status": status,
            "created_at": _iso(inst.created_at) if inst.created_at else None,
            "run_at": job.get("run_at"),
            "queue": job.get("queue"),
            "priority": job.get("priority"),
            "metadata": inst.metadata,
            "attempts": [
                {
                    "attempt": t.attempt + 1,
                    "status": t.state,
                    "started_at": _iso(t.claimed_at) if t.claimed_at else None,
                    "finished_at": _iso(t.completed_at) if t.completed_at else None,
                    "error": t.error_message,
                }
                for t in tasks
                if t.claimed_at
            ],
            "output": inst.outputs[-1]["output"] if inst.outputs and status == "completed" else None,
            "error": inst.error,
        }

    def _create_job(self, body: Any, **_: Any) -> _Response:
        if not isinstance(body, dict) or not body.get("handler"):
            raise _HttpError(400, "handler is required")
        key = body.get("idempotency_key")
        if key and key in self._jobs_by_key:
            return _Response(200, self._job_json(self.instances[self._jobs_by_key[key]]))
        if body.get("delay_ms") is not None and body.get("run_at") is not None:
            raise _HttpError(400, "delay_ms and run_at are mutually exclusive")
        step: dict[str, Any] = {
            "type": "step",
            "id": "job",
            "handler": body["handler"],
            "params": body.get("payload") or {},
        }
        run_at = self.clock.now()
        if body.get("delay_ms"):
            run_at += timedelta(milliseconds=int(body["delay_ms"]))
        if body.get("run_at"):
            run_at = max(run_at, datetime.fromisoformat(body["run_at"].replace("Z", "+00:00")))
        delay_ms = int((run_at - self.clock.now()).total_seconds() * 1000)
        if delay_ms > 0:
            step["delay"] = {"duration": delay_ms}
        if body.get("queue"):
            step["queue_name"] = body["queue"]
        retry = body.get("retry")
        if retry:
            step["retry"] = {
                "max_attempts": retry.get("max_attempts", 1),
                "initial_backoff": retry.get("initial_backoff_ms", 0),
                "max_backoff": retry.get("max_backoff_ms") or 0,
            }
        inst = self._new_instance(
            [step],
            {},
            sequence_id=None,
            metadata=body.get("metadata"),
            idempotency_key=key,
        )
        inst.job = {
            "handler": body["handler"],
            "run_at": _iso(run_at),
            "queue": body.get("queue"),
            "priority": body.get("priority"),
        }
        if key:
            self._jobs_by_key[key] = inst.id
        return _Response(201, self._job_json(inst))

    def _job_or_404(self, id: str) -> FakeInstance:
        inst = self.instances.get(id)
        if inst is None or inst.job is None:
            raise _HttpError(404, f"job {id}")
        return inst

    def _get_job(self, id: str, **_: Any) -> _Response:
        return _Response(200, self._job_json(self._job_or_404(id)))

    def _list_jobs(self, query: dict[str, str], **_: Any) -> _Response:
        jobs = [
            self._job_json(i)
            for i in self.instances.values()
            if i.job is not None
        ]
        jobs = [
            j
            for j in jobs
            if query.get("handler") in (None, j["handler"])
            and query.get("status") in (None, j["status"])
        ]
        start = int(query.get("cursor") or 0)
        limit = int(query.get("limit") or 50)
        page = jobs[start : start + limit]
        more = start + limit < len(jobs)
        return _Response(200, {"jobs": page, "next_cursor": str(start + limit) if more else None})

    def _cancel_job(self, id: str, **_: Any) -> _Response:
        inst = self._job_or_404(id)
        if inst.state not in _TERMINAL:
            self._update_state(id=id, body={"state": "cancelled"})
        return _Response(200, self._job_json(inst))
