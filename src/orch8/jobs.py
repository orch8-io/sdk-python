"""Background-jobs client (``client.jobs``).

A job is a single durable handler invocation with retries, delay and
idempotency, backed by an engine instance. Endpoints: ``POST /jobs``,
``GET /jobs/{id}``, ``GET /jobs``, ``DELETE /jobs/{id}``.
"""
from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator, Mapping
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Literal
from urllib.parse import quote

from pydantic import BaseModel, ConfigDict

if TYPE_CHECKING:
    from .client import Orch8Client

JobStatus = Literal[
    "scheduled", "running", "completed", "failed", "cancelled", "dead_lettered"
]
TERMINAL_JOB_STATUSES: frozenset[str] = frozenset(
    {"completed", "failed", "cancelled", "dead_lettered"}
)


class JobRetry(BaseModel):
    max_attempts: int
    initial_backoff_ms: int
    max_backoff_ms: int | None = None


class JobAttempt(BaseModel):
    model_config = ConfigDict(extra="allow")

    attempt: int | None = None
    status: str | None = None
    started_at: str | None = None
    finished_at: str | None = None
    error: Any = None


class Job(BaseModel):
    model_config = ConfigDict(extra="allow")

    id: str
    instance_id: str
    handler: str
    status: JobStatus
    created_at: str
    run_at: str | None = None
    attempts: list[JobAttempt] | int | None = None
    output: Any = None
    error: Any = None

    @property
    def done(self) -> bool:
        return self.status in TERMINAL_JOB_STATUSES


class JobPage(BaseModel):
    jobs: list[Job]
    next_cursor: str | None = None


class JobTimeoutError(TimeoutError):
    def __init__(self, job: Job) -> None:
        self.job = job
        super().__init__(f"job {job.id} still {job.status} after wait timeout")


def _rfc3339(value: datetime | str) -> str:
    if isinstance(value, str):
        return value
    if value.tzinfo is None:
        raise ValueError("run_at must be timezone-aware")
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


class JobsClient:
    """Enqueue, inspect, list, cancel and await background jobs."""

    def __init__(self, client: Orch8Client) -> None:
        self._client = client

    async def enqueue(
        self,
        handler: str,
        payload: Any = None,
        *,
        queue: str | None = None,
        priority: int | None = None,
        retry: JobRetry | Mapping[str, Any] | None = None,
        delay_ms: int | None = None,
        run_at: datetime | str | None = None,
        idempotency_key: str | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> Job:
        if delay_ms is not None and run_at is not None:
            raise ValueError("pass either delay_ms or run_at, not both")
        body: dict[str, Any] = {"handler": handler, "payload": {} if payload is None else payload}
        if queue is not None:
            body["queue"] = queue
        if priority is not None:
            body["priority"] = priority
        if retry is not None:
            body["retry"] = (
                retry.model_dump(exclude_none=True)
                if isinstance(retry, JobRetry)
                else JobRetry.model_validate(retry).model_dump(exclude_none=True)
            )
        if delay_ms is not None:
            if delay_ms < 0:
                raise ValueError("delay_ms must be >= 0")
            body["delay_ms"] = delay_ms
        if run_at is not None:
            body["run_at"] = _rfc3339(run_at)
        if idempotency_key is not None:
            body["idempotency_key"] = idempotency_key
        if metadata is not None:
            body["metadata"] = dict(metadata)
        data = await self._client.request("POST", "/jobs", json=body)
        return Job.model_validate(data)

    async def get(self, job_id: str) -> Job:
        data = await self._client.request("GET", f"/jobs/{quote(job_id, safe='')}")
        return Job.model_validate(data)

    async def list_page(
        self,
        *,
        handler: str | None = None,
        status: JobStatus | None = None,
        limit: int | None = None,
        cursor: str | None = None,
    ) -> JobPage:
        params = {
            k: v
            for k, v in {
                "handler": handler,
                "status": status,
                "limit": limit,
                "cursor": cursor,
            }.items()
            if v is not None
        }
        data = await self._client.request("GET", "/jobs", params=params)
        if isinstance(data, list):
            return JobPage(jobs=[Job.model_validate(j) for j in data])
        if isinstance(data, dict) and "jobs" not in data and "items" in data:
            data = {"jobs": data["items"], "next_cursor": data.get("next_cursor")}
        return JobPage.model_validate(data)

    async def list(
        self,
        *,
        handler: str | None = None,
        status: JobStatus | None = None,
        limit: int | None = None,
        cursor: str | None = None,
    ) -> AsyncIterator[Job]:
        """Iterate every matching job, following ``next_cursor`` pages.

        ``limit`` is the page size sent to the server.
        """
        while True:
            page = await self.list_page(
                handler=handler, status=status, limit=limit, cursor=cursor
            )
            for job in page.jobs:
                yield job
            if not page.next_cursor or not page.jobs:
                return
            cursor = page.next_cursor

    async def cancel(self, job_id: str) -> Job | None:
        data = await self._client.request("DELETE", f"/jobs/{quote(job_id, safe='')}")
        return Job.model_validate(data) if isinstance(data, dict) else None

    async def wait_for(
        self,
        job_id: str,
        *,
        timeout: float | None = 60.0,
        poll_interval: float = 0.5,
        max_poll_interval: float = 5.0,
    ) -> Job:
        """Poll until the job is terminal; raises :class:`JobTimeoutError`."""
        deadline = None if timeout is None else time.monotonic() + timeout
        interval = poll_interval
        while True:
            job = await self.get(job_id)
            if job.done:
                return job
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise JobTimeoutError(job)
                sleep_for = min(interval, remaining)
            else:
                sleep_for = interval
            await asyncio.sleep(sleep_for)
            interval = min(interval * 1.5, max_poll_interval)
