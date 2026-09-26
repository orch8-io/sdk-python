"""Push-dispatch receiver: verify signed task envelopes and run the work.

A queue in ``push`` mode makes the engine ``POST`` a JSON envelope to your URL
when a task is enqueued::

    {"task_id", "instance_id", "block_id", "handler_name", "queue_name",
     "params", "context", "attempt", "timeout_ms"}

With a queue secret, the request carries ``X-Orch8-Timestamp`` (unix seconds)
and ``X-Orch8-Signature: sha256=<hex HMAC-SHA256(secret, "{ts}.{body}")>`` —
the same scheme as outbound webhooks.

The pushed task is still *pending*: completion requires a lease
(``worker_id`` + ``claim_epoch``) that the envelope does not carry. The
:class:`PushDispatcher` therefore treats a verified push as a wake-up and
claims from the envelope's queue ("claim-on-push") before executing, then
acknowledges through the normal lease protocol. Duplicate or retried pushes
simply find nothing left to claim.

This module is framework-neutral; adapters for AWS Lambda, FastAPI, Django,
ASGI and WSGI live in :mod:`orch8.integrations`.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import socket
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ValidationError

from .worker import Handler, execute_task

if TYPE_CHECKING:
    from .client import Orch8Client

logger = logging.getLogger("orch8.push")

TIMESTAMP_HEADER = "x-orch8-timestamp"
SIGNATURE_HEADER = "x-orch8-signature"
DEFAULT_TOLERANCE_SECONDS = 300


def sign_payload(secret: str, timestamp: int | str, body: bytes) -> str:
    """Return the ``sha256=<hex>`` signature the engine sends for ``body``."""
    mac = hmac.new(secret.encode(), f"{timestamp}.".encode() + body, hashlib.sha256)
    return f"sha256={mac.hexdigest()}"


def verify_push_signature(
    secret: str,
    body: bytes,
    timestamp: str | None,
    signature: str | None,
    *,
    tolerance: int = DEFAULT_TOLERANCE_SECONDS,
    now: float | None = None,
) -> bool:
    """Verify an engine push (or outbound webhook) signature.

    Uses a constant-time comparison and rejects timestamps more than
    ``tolerance`` seconds from ``now`` to bound replay.
    """
    if not secret or not timestamp or not signature:
        return False
    try:
        ts = int(timestamp.strip())
    except ValueError:
        return False
    current = time.time() if now is None else now
    if abs(current - ts) > tolerance:
        return False
    expected = sign_payload(secret, ts, body)
    provided = signature.strip()
    if not provided.startswith("sha256="):
        provided = f"sha256={provided}"
    return hmac.compare_digest(expected.encode(), provided.lower().encode())


#: Outbound webhooks use the same scheme as push dispatch.
verify_outbound_webhook_signature = verify_push_signature


class PushEnvelope(BaseModel):
    task_id: str
    instance_id: str
    block_id: str
    handler_name: str
    queue_name: str | None = None
    params: Any = None
    context: Any = None
    attempt: int = 0
    timeout_ms: int | None = None


@dataclass
class PushResponse:
    status: int
    body: dict[str, Any] = field(default_factory=dict)

    @property
    def json_bytes(self) -> bytes:
        return json.dumps(self.body).encode()


def _header(headers: Mapping[str, str] | Any, name: str) -> str | None:
    getter = getattr(headers, "get", None)
    if getter is not None:
        value = getter(name) or getter(name.title()) or getter(name.upper())
        if value is not None:
            return str(value)
    try:
        items = headers.items()
    except AttributeError:
        items = headers
    for key, value in items:
        key_s = key.decode() if isinstance(key, bytes) else str(key)
        if key_s.lower() == name:
            return value.decode() if isinstance(value, bytes) else str(value)
    return None


class PushDispatcher:
    """Framework-agnostic push receiver.

    Parameters
    ----------
    client:
        Client used to claim and acknowledge tasks.
    handlers:
        Handler name to async handler, as for :class:`~orch8.Orch8Worker`.
    secret:
        The queue's signing secret. Defaults to ``$ORCH8_PUSH_SECRET``.
    allow_unsigned:
        Accept pushes without a signature. Only for queues configured without
        a secret on a trusted network; off by default.
    claim_limit:
        Tasks to claim per push (default 1).
    """

    def __init__(
        self,
        client: Orch8Client,
        handlers: Mapping[str, Handler],
        *,
        secret: str | None = None,
        worker_id: str | None = None,
        allow_unsigned: bool = False,
        tolerance: int = DEFAULT_TOLERANCE_SECONDS,
        claim_limit: int = 1,
        heartbeat_interval: float = 15.0,
    ) -> None:
        self.client = client
        self.handlers = dict(handlers)
        self.secret = secret if secret is not None else os.environ.get("ORCH8_PUSH_SECRET")
        if not self.secret and not allow_unsigned:
            raise ValueError(
                "a push secret is required (pass secret= or set ORCH8_PUSH_SECRET); "
                "use allow_unsigned=True only for unsigned queues on trusted networks"
            )
        self.allow_unsigned = allow_unsigned
        self.worker_id = worker_id or f"push-{socket.gethostname()}-{os.getpid()}"
        self.tolerance = tolerance
        self.claim_limit = max(1, claim_limit)
        self.heartbeat_interval = heartbeat_interval

    def verify(self, headers: Mapping[str, str] | Any, body: bytes) -> bool:
        signature = _header(headers, SIGNATURE_HEADER)
        if not self.secret:
            return self.allow_unsigned and signature is None
        return verify_push_signature(
            self.secret,
            body,
            _header(headers, TIMESTAMP_HEADER),
            signature,
            tolerance=self.tolerance,
        )

    async def handle(self, headers: Mapping[str, str] | Any, body: bytes) -> PushResponse:
        """Verify, claim and execute. Never raises for request problems."""
        if not self.verify(headers, body):
            return PushResponse(401, {"error": "invalid signature"})
        try:
            envelope = PushEnvelope.model_validate_json(body)
        except ValidationError:
            return PushResponse(400, {"error": "invalid push envelope"})
        handler = self.handlers.get(envelope.handler_name)
        if handler is None:
            return PushResponse(404, {"error": f"no handler for {envelope.handler_name}"})
        try:
            if envelope.queue_name:
                batch = await self.client.poll_task_batch_from_queue(
                    envelope.queue_name,
                    envelope.handler_name,
                    self.worker_id,
                    self.claim_limit,
                )
            else:
                batch = await self.client.poll_task_batch(
                    handler_name=envelope.handler_name,
                    worker_id=self.worker_id,
                    limit=self.claim_limit,
                )
        except Exception:
            logger.exception("claim-on-push failed for task %s", envelope.task_id)
            # 5xx lets the engine retry the push.
            return PushResponse(503, {"error": "claim failed"})
        heartbeat = self.heartbeat_interval
        if batch.heartbeat_interval_secs:
            heartbeat = min(heartbeat, batch.heartbeat_interval_secs)
        if batch.lease_secs:
            heartbeat = min(heartbeat, batch.lease_secs / 2)
        results: dict[str, str] = {}
        for task in batch.tasks:
            results[task.id] = await execute_task(
                self.client,
                self.worker_id,
                self.handlers.get(task.handler_name),
                task,
                heartbeat_interval=heartbeat,
            )
        return PushResponse(
            200,
            {"task_id": envelope.task_id, "claimed": len(batch.tasks), "results": results},
        )
