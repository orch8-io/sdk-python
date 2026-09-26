"""AWS Lambda adapter (API Gateway REST/HTTP APIs and Lambda Function URLs).

::

    from orch8 import Orch8Client
    from orch8.push import PushDispatcher
    from orch8.integrations.aws_lambda import lambda_handler

    dispatcher = PushDispatcher(Orch8Client(ENGINE_URL), {"resize": resize})
    handler = lambda_handler(dispatcher)   # configure as the Lambda handler

No AWS package is required: events are plain dictionaries.
"""
from __future__ import annotations

import base64
from collections.abc import Callable, Mapping
from typing import Any

from ..push import PushDispatcher
from ._loop import LoopThread, shared_loop


def _body(event: Mapping[str, Any]) -> bytes:
    raw = event.get("body") or ""
    if event.get("isBase64Encoded"):
        return base64.b64decode(raw)
    return raw.encode() if isinstance(raw, str) else bytes(raw)


def _headers(event: Mapping[str, Any]) -> dict[str, str]:
    headers: dict[str, str] = {}
    for key, values in (event.get("multiValueHeaders") or {}).items():
        if values:
            headers[key.lower()] = values[-1]
    for key, value in (event.get("headers") or {}).items():
        if value is not None:
            headers[key.lower()] = value
    return headers


def lambda_handler(
    dispatcher: PushDispatcher, *, loop: LoopThread | None = None
) -> Callable[[Mapping[str, Any], Any], dict[str, Any]]:
    """Return a Lambda handler function that runs ``dispatcher``."""
    runner = loop or shared_loop

    def handle(event: Mapping[str, Any], context: Any = None) -> dict[str, Any]:
        result = runner.run(dispatcher.handle(_headers(event), _body(event)))
        return {
            "statusCode": result.status,
            "headers": {"Content-Type": "application/json"},
            "body": result.json_bytes.decode(),
        }

    return handle
