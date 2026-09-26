"""A long-lived event loop on a daemon thread for synchronous hosts.

Sync entry points (AWS Lambda, WSGI, Django sync views) must not create a new
event loop per request: the SDK's pooled ``httpx.AsyncClient`` is bound to the
loop it first ran on. Every call is submitted to one persistent loop instead.
"""
from __future__ import annotations

import asyncio
import threading
from collections.abc import Coroutine
from typing import Any, TypeVar

T = TypeVar("T")


class LoopThread:
    def __init__(self) -> None:
        self._loop: asyncio.AbstractEventLoop | None = None
        self._lock = threading.Lock()

    def _ensure(self) -> asyncio.AbstractEventLoop:
        with self._lock:
            if self._loop is None or self._loop.is_closed():
                loop = asyncio.new_event_loop()
                thread = threading.Thread(
                    target=loop.run_forever, name="orch8-push-loop", daemon=True
                )
                thread.start()
                self._loop = loop
            return self._loop

    def run(self, coro: Coroutine[Any, Any, T], timeout: float | None = None) -> T:
        return asyncio.run_coroutine_threadsafe(coro, self._ensure()).result(timeout)


shared_loop = LoopThread()
