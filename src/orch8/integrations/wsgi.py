"""Plain WSGI app for push dispatch (Flask, Bottle, gunicorn, ...)."""
from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import Any

from ..push import PushDispatcher
from ._loop import LoopThread, shared_loop

_REASONS = {200: "OK", 400: "Bad Request", 401: "Unauthorized", 404: "Not Found",
            405: "Method Not Allowed", 413: "Payload Too Large", 503: "Service Unavailable"}


def push_wsgi_app(
    dispatcher: PushDispatcher,
    *,
    loop: LoopThread | None = None,
    max_body_bytes: int = 10 * 1024 * 1024,
) -> Callable[[dict[str, Any], Callable[..., Any]], Iterable[bytes]]:
    runner = loop or shared_loop

    def app(environ: dict[str, Any], start_response: Callable[..., Any]) -> Iterable[bytes]:
        def reply(status: int, body: bytes) -> list[bytes]:
            start_response(
                f"{status} {_REASONS.get(status, 'Error')}",
                [("Content-Type", "application/json"), ("Content-Length", str(len(body)))],
            )
            return [body]

        if environ.get("REQUEST_METHOD") != "POST":
            return reply(405, b'{"error":"method not allowed"}')
        length = int(environ.get("CONTENT_LENGTH") or 0)
        if length > max_body_bytes:
            return reply(413, b'{"error":"payload too large"}')
        body = environ["wsgi.input"].read(length) if length else b""
        headers = {
            key[5:].replace("_", "-").lower(): value
            for key, value in environ.items()
            if key.startswith("HTTP_")
        }
        result = runner.run(dispatcher.handle(headers, body))
        return reply(result.status, result.json_bytes)

    return app
