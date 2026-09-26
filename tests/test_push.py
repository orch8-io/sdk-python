"""Push-dispatch verification, claim-on-push, and framework adapters."""
from __future__ import annotations

import base64
import io
import json
import time
from typing import Any

import httpx
import pytest

from orch8.integrations.asgi import PushASGIApp
from orch8.integrations.aws_lambda import lambda_handler
from orch8.integrations.wsgi import push_wsgi_app
from orch8.push import PushDispatcher, sign_payload, verify_push_signature
from orch8.testing import FakeEngine
from orch8.types import WorkerTask

SECRET = "shhh"


def test_signature_matches_engine_scheme() -> None:
    # Vector computed with the engine's webhooks::sign (HMAC-SHA256 over "{ts}.{body}").
    import hashlib
    import hmac

    body = b'{"task_id":"t"}'
    expected = hmac.new(b"k", b"1700000000." + body, hashlib.sha256).hexdigest()
    assert sign_payload("k", 1700000000, body) == f"sha256={expected}"


def test_verify_rejects_tampering_staleness_and_garbage() -> None:
    body = b'{"a":1}'
    ts = int(time.time())
    sig = sign_payload(SECRET, ts, body)
    assert verify_push_signature(SECRET, body, str(ts), sig)
    assert verify_push_signature(SECRET, body, str(ts), sig.removeprefix("sha256="))
    assert not verify_push_signature(SECRET, b'{"a":2}', str(ts), sig)
    assert not verify_push_signature("other", body, str(ts), sig)
    assert not verify_push_signature(SECRET, body, str(ts - 301), sign_payload(SECRET, ts - 301, body))
    assert not verify_push_signature(SECRET, body, "not-a-number", sig)
    assert not verify_push_signature(SECRET, body, None, sig)
    assert not verify_push_signature("", body, str(ts), sig)


def _setup() -> tuple[FakeEngine, PushDispatcher, FakeTask_, list[str]]:
    engine = FakeEngine()
    task = engine.enqueue_task("resize", {"w": 4}, queue_name="images")
    seen: list[str] = []

    async def resize(t: WorkerTask) -> dict[str, int]:
        seen.append(t.id)
        return {"w": t.params["w"] * 2}

    dispatcher = PushDispatcher(engine.client(), {"resize": resize}, secret=SECRET, worker_id="push-1")
    return engine, dispatcher, task, seen


FakeTask_ = Any


def _envelope(task: Any) -> bytes:
    return json.dumps(
        {
            "task_id": task.id,
            "instance_id": task.instance_id,
            "block_id": task.block_id,
            "handler_name": task.handler_name,
            "queue_name": task.queue_name,
            "params": task.params,
            "context": {},
            "attempt": 0,
            "timeout_ms": None,
        }
    ).encode()


def _signed_headers(body: bytes) -> dict[str, str]:
    ts = int(time.time())
    return {"X-Orch8-Timestamp": str(ts), "X-Orch8-Signature": sign_payload(SECRET, ts, body)}


async def test_claim_on_push_executes_and_acknowledges() -> None:
    engine, dispatcher, task, seen = _setup()
    body = _envelope(task)
    result = await dispatcher.handle(_signed_headers(body), body)
    assert result.status == 200 and result.body["claimed"] == 1
    assert seen == [task.id]
    assert engine.task(task.id).state == "completed"
    assert engine.task(task.id).output == {"w": 8}
    # A retried push finds nothing left to claim.
    again = await dispatcher.handle(_signed_headers(body), body)
    assert again.status == 200 and again.body["claimed"] == 0


async def test_rejections() -> None:
    engine, dispatcher, task, _ = _setup()
    body = _envelope(task)
    assert (await dispatcher.handle({}, body)).status == 401
    bad = _signed_headers(body)
    bad["X-Orch8-Signature"] = "sha256=" + "0" * 64
    assert (await dispatcher.handle(bad, body)).status == 401
    junk = b'{"nope": true}'
    assert (await dispatcher.handle(_signed_headers(junk), junk)).status == 400
    other = json.loads(body)
    other["handler_name"] = "unknown"
    raw = json.dumps(other).encode()
    assert (await dispatcher.handle(_signed_headers(raw), raw)).status == 404
    assert engine.task(task.id).state == "pending"


def test_secret_is_required_unless_explicitly_unsigned(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ORCH8_PUSH_SECRET", raising=False)
    engine = FakeEngine()
    with pytest.raises(ValueError):
        PushDispatcher(engine.client(), {})
    unsigned = PushDispatcher(engine.client(), {}, allow_unsigned=True)
    assert unsigned.verify({}, b"{}")
    assert not unsigned.verify({"x-orch8-signature": "sha256=00"}, b"{}")
    monkeypatch.setenv("ORCH8_PUSH_SECRET", SECRET)
    assert PushDispatcher(engine.client(), {}).secret == SECRET


def test_lambda_adapter_function_url_event() -> None:
    engine, dispatcher, task, _ = _setup()
    body = _envelope(task)
    headers = {k.lower(): v for k, v in _signed_headers(body).items()}
    event = {
        "version": "2.0",
        "headers": headers,
        "body": base64.b64encode(body).decode(),
        "isBase64Encoded": True,
    }
    response = lambda_handler(dispatcher)(event, None)
    assert response["statusCode"] == 200
    assert json.loads(response["body"])["claimed"] == 1
    assert engine.task(task.id).state == "completed"
    unsigned = lambda_handler(dispatcher)({"headers": {}, "body": body.decode()}, None)
    assert unsigned["statusCode"] == 401


async def test_asgi_app() -> None:
    engine, dispatcher, task, _ = _setup()
    body = _envelope(task)
    transport = httpx.ASGITransport(app=PushASGIApp(dispatcher))
    async with httpx.AsyncClient(transport=transport, base_url="http://app") as http:
        assert (await http.get("/")).status_code == 405
        response = await http.post("/", content=body, headers=_signed_headers(body))
    assert response.status_code == 200
    assert engine.task(task.id).state == "completed"


def test_wsgi_app() -> None:
    engine, dispatcher, task, _ = _setup()
    body = _envelope(task)
    environ = {
        "REQUEST_METHOD": "POST",
        "CONTENT_LENGTH": str(len(body)),
        "wsgi.input": io.BytesIO(body),
        **{f"HTTP_{k.upper().replace('-', '_')}": v for k, v in _signed_headers(body).items()},
    }
    statuses: list[str] = []
    out = push_wsgi_app(dispatcher)(environ, lambda status, headers: statuses.append(status))
    assert statuses == ["200 OK"] and json.loads(b"".join(out))["claimed"] == 1
    assert engine.task(task.id).state == "completed"


def test_fastapi_router() -> None:
    fastapi = pytest.importorskip("fastapi")
    from fastapi.testclient import TestClient

    from orch8.integrations.fastapi import push_router

    engine, dispatcher, task, _ = _setup()
    app = fastapi.FastAPI()
    app.include_router(push_router(dispatcher, path="/orch8/push"))
    body = _envelope(task)
    with TestClient(app) as http:
        assert http.post("/orch8/push", content=body).status_code == 401
        response = http.post("/orch8/push", content=body, headers=_signed_headers(body))
    assert response.status_code == 200
    assert engine.task(task.id).state == "completed"


def test_django_view() -> None:
    pytest.importorskip("django")
    from django.conf import settings

    if not settings.configured:
        settings.configure(DEBUG=True, ALLOWED_HOSTS=["*"], SECRET_KEY="test")
    import django

    django.setup()
    from django.test import RequestFactory

    from orch8.integrations.django import push_view

    engine, dispatcher, task, _ = _setup()
    view = push_view(dispatcher)
    body = _envelope(task)
    factory = RequestFactory()
    assert view(factory.get("/p")).status_code == 405
    request = factory.post(
        "/p",
        data=body,
        content_type="application/json",
        headers=_signed_headers(body),
    )
    response = view(request)
    assert response.status_code == 200 and json.loads(response.content)["claimed"] == 1
    assert engine.task(task.id).state == "completed"
