"""Orch8 polling worker — runs task handlers using asyncio."""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Awaitable, Callable

from .client import Orch8Client
from .types import WorkerTask

logger = logging.getLogger("orch8.worker")

Handler = Callable[[WorkerTask], Awaitable[Any]]


class Orch8Worker:
    """Long-running polling worker that claims and executes tasks.

    Parameters
    ----------
    client:
        An ``Orch8Client`` used to poll / complete / fail tasks.
    worker_id:
        Unique identifier for this worker instance.
    handlers:
        Mapping of handler name to an async callable that processes the task
        and returns the output value.
    poll_interval:
        Seconds between poll cycles (default 2).
    heartbeat_interval:
        Seconds between heartbeats for in-progress tasks (default 30).
    max_concurrent:
        Maximum number of tasks processed concurrently (default 5).
    """

    def __init__(
        self,
        client: Orch8Client,
        worker_id: str,
        handlers: dict[str, Handler],
        *,
        poll_interval: float = 2.0,
        heartbeat_interval: float = 30.0,
        max_concurrent: int = 5,
        circuit_breaker_check: bool = False,
        on_task_complete: Callable[[WorkerTask, Any], None] | None = None,
        on_task_fail: Callable[[WorkerTask, Exception], None] | None = None,
    ) -> None:
        self.client = client
        self.worker_id = worker_id
        self.handlers = handlers
        self.poll_interval = poll_interval
        self.heartbeat_interval = heartbeat_interval
        self._max_concurrent = max_concurrent
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._in_flight = 0
        self._running = False
        self._tasks: set[asyncio.Task[None]] = set()
        self._circuit_breaker_check = circuit_breaker_check
        self._on_task_complete = on_task_complete
        self._on_task_fail = on_task_fail
        self._backoff: dict[str, float] = {}

    async def start(self) -> None:
        """Start the polling loop. Blocks until :meth:`stop` is called."""
        self._running = True
        logger.info("worker %s starting (handlers=%s)", self.worker_id, list(self.handlers))
        try:
            poll_loops = [
                asyncio.create_task(self._poll_loop(name))
                for name in self.handlers
            ]
            await asyncio.gather(*poll_loops)
        finally:
            self._running = False

    async def _poll_loop(self, handler_name: str) -> None:
        """Independent polling loop for a single handler."""
        while self._running:
            await self._poll_handler(handler_name)
            interval = self._backoff.get(handler_name, self.poll_interval)
            await asyncio.sleep(interval)

    async def stop(self, timeout: float = 30.0) -> None:
        """Signal the worker to stop and wait for in-flight tasks."""
        self._running = False
        if self._tasks:
            logger.info("waiting for %d in-flight tasks", len(self._tasks))
            _, pending = await asyncio.wait(self._tasks, timeout=timeout)
            for t in pending:
                t.cancel()

    async def _poll_handler(self, handler_name: str) -> None:
        remaining = self._max_concurrent - self._in_flight
        if remaining <= 0:
            return

        # Circuit breaker check
        if self._circuit_breaker_check:
            try:
                cb = await self.client.get_circuit_breaker(handler_name)
                if cb.state == "open":
                    logger.debug(
                        "circuit breaker open for %s, skipping poll",
                        handler_name,
                    )
                    return
            except Exception:
                logger.warning(
                    "circuit breaker check failed for %s, polling anyway",
                    handler_name,
                )

        try:
            tasks = await self.client.poll_tasks(
                handler_name=handler_name,
                worker_id=self.worker_id,
                limit=remaining,
            )
        except Exception:
            logger.exception("poll error for handler %s", handler_name)
            # Exponential backoff on poll failure
            current = self._backoff.get(handler_name, self.poll_interval)
            self._backoff[handler_name] = min(current * 2, 30.0)
            return

        # Reset backoff on successful poll
        self._backoff.pop(handler_name, None)

        for task in tasks:
            await self._semaphore.acquire()
            self._in_flight += 1
            t = asyncio.create_task(self._execute(task))
            t.add_done_callback(self._tasks.discard)
            self._tasks.add(t)

    async def _execute(self, task: WorkerTask) -> None:
        heartbeat_handle: asyncio.Task[None] | None = None
        try:
            handler = self.handlers[task.handler_name]
            heartbeat_handle = asyncio.create_task(self._heartbeat_loop(task.id))
            output = await handler(task)
            await self.client.complete_task(task.id, self.worker_id, output)
            if self._on_task_complete is not None:
                try:
                    self._on_task_complete(task, output)
                except Exception:
                    logger.exception("on_task_complete callback error for task %s", task.id)
        except Exception as exc:
            logger.exception("task %s failed", task.id)
            if self._on_task_fail is not None:
                try:
                    self._on_task_fail(task, exc)
                except Exception:
                    logger.exception("on_task_fail callback error for task %s", task.id)
            try:
                await self.client.fail_task(
                    task.id, self.worker_id, str(exc), retryable=False
                )
            except Exception:
                logger.exception("failed to report failure for task %s", task.id)
        finally:
            if heartbeat_handle is not None:
                heartbeat_handle.cancel()
            self._in_flight -= 1
            self._semaphore.release()

    async def _heartbeat_loop(self, task_id: str) -> None:
        while True:
            await asyncio.sleep(self.heartbeat_interval)
            try:
                await self.client.heartbeat_task(task_id, self.worker_id)
            except Exception:
                logger.exception("heartbeat error for task %s", task_id)
