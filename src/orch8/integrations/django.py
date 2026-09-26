"""Django view for push dispatch::

    urlpatterns = [path("orch8/push", push_view(dispatcher))]

The view is synchronous and CSRF-exempt (requests are authenticated by the
HMAC signature). Work runs on a persistent SDK event loop, so it behaves the
same under WSGI and ASGI deployments. Requires ``django``.
"""
from __future__ import annotations

from collections.abc import Callable
from typing import Any

from ..push import PushDispatcher
from ._loop import LoopThread, shared_loop


def push_view(dispatcher: PushDispatcher, *, loop: LoopThread | None = None) -> Callable[..., Any]:
    from django.http import HttpResponse, HttpResponseNotAllowed
    from django.views.decorators.csrf import csrf_exempt

    runner = loop or shared_loop

    @csrf_exempt
    def orch8_push(request: Any) -> Any:
        if request.method != "POST":
            return HttpResponseNotAllowed(["POST"])
        result = runner.run(dispatcher.handle(request.headers, request.body))
        return HttpResponse(result.json_bytes, status=result.status, content_type="application/json")

    return orch8_push
