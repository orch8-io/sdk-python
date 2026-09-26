"""Orch8 polling worker — runs task handlers using asyncio.

The worker follows the engine lease protocol: every heartbeat, completion and
failure echoes the task's ``claim_epoch``; server poll hints
(``poll_after_ms``, ``heartbeat_interval_secs``, ``lease_secs``) bound the
polling and heartbeat cadence. A rejected or ambiguous acknowledgement is left
for lease recovery rather than contradicted with a failure report.
"""
from __future__ import annotations

import asyncio
import contextvars
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from .client import Orch8Client
from .types import WorkerTask

logger = logging.getLogger("orch8.worker")

Handler = Callable[[WorkerTask], Awaitable[Any]]


@dataclass
class TaskContext:
    """Runtime view of the task a handler is executing.

    Obtain it inside a handler with :func:`current_task`. ``checkpoint`` stores
    durable progress through the heartbeat endpoint; a retried attempt receives
    the last stored value as :attr:`resume_checkpoint`.
    """

    task: WorkerTask
    client: Orch8Client
    worker_id: str
    _seq: int = field(default=0, repr=False)

    def __post_init__(self) -> None:
        self._seq = self.task.checkpoint_seq

    @property
    def resume_checkpoint(self) -> Any:
        return self.task.resume_checkpoint

    @property
    def idempotency_prefix(self) -> str:
        """Stable across retries of the same step: ``{instance_id}:{block_id}``."""
        return f"{self.task.instance_id}:{self.task.block_id}"

    async def checkpoint(self, data: Any) -> int:
        """Durably record ``data`` for this task; returns the new sequence."""
        next_seq = self._seq + 1
        reply = await self.client.heartbeat_task(
            self.task.id,
            self.worker_id,
            checkpoint=data,
            checkpoint_seq=next_seq,
            claim_epoch=self.task.claim_epoch,
        )
        seq = reply.get("checkpoint_seq") if isinstance(reply, dict) else None
        self._seq = seq if isinstance(seq, int) else next_seq
        self.task.resume_checkpoint = data
        self.task.checkpoint_seq = self._seq
        return self._seq


_current: contextvars.ContextVar[TaskContext | None] = contextvars.ContextVar(
    "orch8_current_task", default=None
)


def current_task() -> TaskContext | None:
    """Return the task context of the running handler, if any."""
    return _current.get()


async def execute_task(
    client: Orch8Client,
    worker_id: str,
    handler: Handler | None,
    task: WorkerTask,
    *,
    heartbeat_interval: float = 15.0,
    on_task_complete: Callable[[WorkerTask, Any], None] | None = None,
    on_task_fail: Callable[[WorkerTask, Exception], None] | None = None,
) -> str:
    """Run one claimed task and acknowledge it with its lease epoch.

    Returns ``"completed"``, ``"failed"`` or ``"unacknowledged"`` (the
    acknowledgement itself was rejected; the lease reaper recovers the task).
    """
    if handler is None:
        try:
            await client.fail_task(
                task.id,
                worker_id,
                f'no handler registered for "{task.handler_name}"',
                retryable=False,
                claim_epoch=task.claim_epoch,
            )
            return "failed"
        except Exception:
            logger.exception("failed to report missing handler for task %s", task.id)
            return "unacknowledged"

    heartbeat = asyncio.create_task(
        _heartbeat_loop(client, worker_id, task, heartbeat_interval)
    )
    token = _current.set(TaskContext(task=task, client=client, worker_id=worker_id))
    try:
        try:
            if task.timeout_ms and task.timeout_ms > 0:
                output = await asyncio.wait_for(
                    handler(task), timeout=task.timeout_ms / 1000
                )
            else:
                output = await handler(task)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if isinstance(exc, asyncio.TimeoutError):
                message = "task timed out"
                retryable = True
            else:
                message = str(exc) or type(exc).__name__
                retryable = bool(getattr(exc, "retryable", False))
            logger.warning("task %s failed: %s", task.id, message)
            _notify(on_task_fail, task, exc)
            try:
                await client.fail_task(
                    task.id,
                    worker_id,
                    message,
                    retryable=retryable,
                    claim_epoch=task.claim_epoch,
                )
            except Exception:
                logger.exception("failed to report failure for task %s", task.id)
                return "unacknowledged"
            return "failed"
        try:
            await client.complete_task(
                task.id,
                worker_id,
                {} if output is None else output,
                claim_epoch=task.claim_epoch,
            )
        except Exception:
            # Never contradict an ambiguous completion with a failure report.
            logger.exception("completion of task %s was not acknowledged", task.id)
            return "unacknowledged"
        _notify(on_task_complete, task, output)
        return "completed"
    finally:
        _current.reset(token)
        heartbeat.cancel()


def _notify(callback: Callable[[WorkerTask, Any], None] | None, task: WorkerTask, value: Any) -> None:
    if callback is None:
        return
    try:
        callback(task, value)
    except Exception:
        logger.exception("worker lifecycle callback error for task %s", task.id)


async def _heartbeat_loop(
    client: Orch8Client, worker_id: str, task: WorkerTask, interval: float
) -> None:
    try:
        while True:
            await asyncio.sleep(interval)
            try:
                await client.heartbeat_task(
                    task.id, worker_id, claim_epoch=task.claim_epoch
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("heartbeat error for task %s", task.id)
    except asyncio.CancelledError:
        logger.debug("heartbeat loop for task %s cancelled", task.id)
        raise


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
        Minimum seconds between poll cycles (default 1).
    heartbeat_interval:
        Maximum seconds between heartbeats (default 15); lowered to the
        server's advertised interval or half the lease when those are shorter.
    max_concurrent:
        Maximum number of tasks processed concurrently (default 10).
    queue:
        Optional named queue to claim from instead of the default queue.
    """

    def __init__(
        self,
        client: Orch8Client,
        worker_id: str,
        handlers: dict[str, Handler],
        *,
        poll_interval: float = 1.0,
        heartbeat_interval: float = 15.0,
        max_concurrent: int = 10,
        circuit_breaker_check: bool = False,
        queue: str | None = None,
        on_task_complete: Callable[[WorkerTask, Any], None] | None = None,
        on_task_fail: Callable[[WorkerTask, Exception], None] | None = None,
    ) -> None:
        self.client = client
        self.worker_id = worker_id
        self.handlers = handlers
        self.poll_interval = poll_interval
        self.heartbeat_interval = heartbeat_interval
        self.queue = queue
        self._max_concurrent = max_concurrent
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._in_flight = 0
        self._running = False
        self._tasks: set[asyncio.Task[None]] = set()
        self._poll_tasks: set[asyncio.Task[None]] = set()
        self._circuit_breaker_check = circuit_breaker_check
        self._on_task_complete = on_task_complete
        self._on_task_fail = on_task_fail
        self._backoff: dict[str, float] = {}
        self._poll_hints: dict[str, float] = {}
        self._heartbeat_hints: dict[str, float] = {}

    def stats(self) -> dict[str, Any]:
        """Return the language-neutral worker runtime snapshot."""
        return {
            "running": self._running,
            "in_flight": self._in_flight,
            "available_slots": self._max_concurrent - self._in_flight,
            "handlers": list(self.handlers),
        }

    async def start(self) -> None:
        """Start the polling loop. Blocks until :meth:`stop` is called."""
        self._running = True
        logger.info(
            "worker %s starting (handlers=%s)", self.worker_id, list(self.handlers)
        )
        try:
            poll_loops = [
                asyncio.create_task(self._poll_loop(name))
                for name in self.handlers
            ]
            self._poll_tasks.update(poll_loops)
            results = await asyncio.gather(*poll_loops, return_exceptions=True)
            for result in results:
                if isinstance(result, BaseException) and not isinstance(
                    result, asyncio.CancelledError
                ):
                    logger.error("poll loop terminated with error: %r", result)
        finally:
            self._running = False
            self._poll_tasks.clear()

    async def _poll_loop(self, handler_name: str) -> None:
        """Independent polling loop for a single handler."""
        try:
            while self._running:
                await self._poll_handler(handler_name)
                interval = self._backoff.get(handler_name)
                if interval is None:
                    interval = max(
                        self.poll_interval, self._poll_hints.get(handler_name, 0.0)
                    )
                await asyncio.sleep(interval)
        except asyncio.CancelledError:
            logger.debug("poll loop for %s cancelled", handler_name)
            raise

    async def stop(self, timeout: float = 30.0) -> None:
        """Signal the worker to stop and wait for in-flight tasks."""
        self._running = False
        for t in list(self._poll_tasks):
            if not t.done():
                t.cancel()
        if self._poll_tasks:
            await asyncio.wait(self._poll_tasks, timeout=timeout)
        if self._tasks:
            logger.info("waiting for %d in-flight tasks", len(self._tasks))
            _, pending = await asyncio.wait(self._tasks, timeout=timeout)
            for t in pending:
                t.cancel()

    def _effective_heartbeat(self) -> float:
        return min([self.heartbeat_interval, *self._heartbeat_hints.values()])

    async def _poll_handler(self, handler_name: str) -> None:
        remaining = self._max_concurrent - self._in_flight
        if remaining <= 0:
            return

        if self._circuit_breaker_check:
            try:
                cb = await self.client.get_circuit_breaker(handler_name)
                if cb.state == "open":
                    logger.debug(
                        "circuit breaker open for %s, skipping poll", handler_name
                    )
                    return
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.warning(
                    "circuit breaker check failed for %s, polling anyway",
                    handler_name,
                )

        try:
            if self.queue:
                batch = await self.client.poll_task_batch_from_queue(
                    self.queue, handler_name, self.worker_id, remaining
                )
            else:
                batch = await self.client.poll_task_batch(
                    handler_name=handler_name,
                    worker_id=self.worker_id,
                    limit=remaining,
                )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("poll error for handler %s", handler_name)
            current = self._backoff.get(handler_name, self.poll_interval)
            self._backoff[handler_name] = min(current * 2, 30.0)
            return

        self._backoff.pop(handler_name, None)
        self._poll_hints[handler_name] = (batch.poll_after_ms or 0) / 1000
        hints = [
            v
            for v in (
                batch.heartbeat_interval_secs,
                batch.lease_secs / 2 if batch.lease_secs else None,
            )
            if v
        ]
        if hints:
            self._heartbeat_hints[handler_name] = min(hints)

        for task in batch.tasks:
            await self._semaphore.acquire()
            self._in_flight += 1
            t = asyncio.create_task(self._execute(task))
            t.add_done_callback(self._tasks.discard)
            self._tasks.add(t)

    async def _execute(self, task: WorkerTask) -> None:
        try:
            await execute_task(
                self.client,
                self.worker_id,
                self.handlers.get(task.handler_name),
                task,
                heartbeat_interval=self._effective_heartbeat(),
                on_task_complete=self._on_task_complete,
                on_task_fail=self._on_task_fail,
            )
        finally:
            self._in_flight -= 1
            self._semaphore.release()
