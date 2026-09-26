"""Plain ASGI app for push dispatch (mount under Starlette, Litestar, etc.)."""
from __future__ import annotations

from collections.abc import Awaitable, Callable, MutableMapping
from typing import Any

from ..push import PushDispatcher

Scope = MutableMapping[str, Any]
Receive = Callable[[], Awaitable[MutableMapping[str, Any]]]
Send = Callable[[MutableMapping[str, Any]], Awaitable[None]]


class PushASGIApp:
    def __init__(self, dispatcher: PushDispatcher, *, max_body_bytes: int = 10 * 1024 * 1024) -> None:
        self.dispatcher = dispatcher
        self.max_body_bytes = max_body_bytes

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            return
        if scope["method"] != "POST":
            await self._reply(send, 405, b'{"error":"method not allowed"}')
            return
        chunks: list[bytes] = []
        size = 0
        while True:
            message = await receive()
            chunk = message.get("body", b"")
            size += len(chunk)
            if size > self.max_body_bytes:
                await self._reply(send, 413, b'{"error":"payload too large"}')
                return
            chunks.append(chunk)
            if not message.get("more_body"):
                break
        result = await self.dispatcher.handle(scope.get("headers") or [], b"".join(chunks))
        await self._reply(send, result.status, result.json_bytes)

    @staticmethod
    async def _reply(send: Send, status: int, body: bytes) -> None:
        await send(
            {
                "type": "http.response.start",
                "status": status,
                "headers": [(b"content-type", b"application/json")],
            }
        )
        await send({"type": "http.response.body", "body": body})
