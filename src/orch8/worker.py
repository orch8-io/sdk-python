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
    ) -> None:
        self.client = client
        self.worker_id = worker_id
        self.handlers = handlers
        self.poll_interval = poll_interval
        self.heartbeat_interval = heartbeat_interval
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._running = False
        self._tasks: set[asyncio.Task[None]] = set()

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
            await asyncio.sleep(self.poll_interval)

    async def stop(self, timeout: float = 30.0) -> None:
        """Signal the worker to stop and wait for in-flight tasks."""
        self._running = False
        if self._tasks:
            logger.info("waiting for %d in-flight tasks", len(self._tasks))
            _, pending = await asyncio.wait(self._tasks, timeout=timeout)
            for t in pending:
                t.cancel()

    async def _poll_handler(self, handler_name: str) -> None:
        try:
            tasks = await self.client.poll_tasks(
                handler_name=handler_name,
                worker_id=self.worker_id,
                limit=self._semaphore._value,  # noqa: SLF001 — remaining capacity
            )
        except Exception:
            logger.exception("poll error for handler %s", handler_name)
            return

        for task in tasks:
            await self._semaphore.acquire()
            t = asyncio.create_task(self._execute(task))
            self._tasks.add(t)
            t.add_done_callback(self._tasks.discard)

    async def _execute(self, task: WorkerTask) -> None:
        heartbeat_handle: asyncio.Task[None] | None = None
        try:
            handler = self.handlers[task.handler_name]
            heartbeat_handle = asyncio.create_task(self._heartbeat_loop(task.id))
            output = await handler(task)
            await self.client.complete_task(task.id, self.worker_id, output)
        except Exception as exc:
            logger.exception("task %s failed", task.id)
            try:
                await self.client.fail_task(
                    task.id, self.worker_id, str(exc), retryable=False
                )
            except Exception:
                logger.exception("failed to report failure for task %s", task.id)
        finally:
            if heartbeat_handle is not None:
                heartbeat_handle.cancel()
            self._semaphore.release()

    async def _heartbeat_loop(self, task_id: str) -> None:
        while True:
            await asyncio.sleep(self.heartbeat_interval)
            try:
                await self.client.heartbeat_task(task_id, self.worker_id)
            except Exception:
                logger.exception("heartbeat error for task %s", task_id)
