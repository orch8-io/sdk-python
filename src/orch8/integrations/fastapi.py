"""FastAPI router for push dispatch::

    app.include_router(push_router(dispatcher, path="/orch8/push"))

Requires ``fastapi`` (``pip install "orch8-io-sdk[fastapi]"``).
"""
# No ``from __future__ import annotations``: FastAPI resolves the route
# signature at runtime and ``Request`` is imported lazily below.
from typing import Any

from ..push import PushDispatcher


def push_router(dispatcher: PushDispatcher, *, path: str = "/orch8/push", **router_kwargs: Any) -> Any:
    from fastapi import APIRouter, Request, Response

    router = APIRouter(**router_kwargs)

    @router.post(path, include_in_schema=False)
    async def orch8_push(request: Request) -> Response:
        # The signature covers the exact bytes, so read the raw body.
        body = await request.body()
        result = await dispatcher.handle(request.headers, body)
        return Response(
            content=result.json_bytes, status_code=result.status, media_type="application/json"
        )

    return router
