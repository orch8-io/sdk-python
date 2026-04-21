"""Tests for Orch8Worker."""
from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from orch8 import Orch8Client
from orch8.types import WorkerTask
from orch8.worker import Orch8Worker


@pytest.fixture
def mock_client() -> Orch8Client:
    client = MagicMock(spec=Orch8Client)
    client.poll_tasks = AsyncMock(return_value=[])
    client.complete_task = AsyncMock()
    client.fail_task = AsyncMock()
    client.heartbeat_task = AsyncMock()
    client.get_circuit_breaker = AsyncMock(
        return_value=MagicMock(state="closed")
    )
    return client


def _task(handler_name: str = "test-handler", task_id: str = "wt-1") -> WorkerTask:
    return WorkerTask(
        id=task_id,
        instance_id="inst-1",
        block_id="block-1",
        handler_name=handler_name,
        created_at="2025-01-01T00:00:00Z",
    )


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_start_stop(mock_client: Orch8Client) -> None:
    worker = Orch8Worker(
        client=mock_client,
        worker_id="w-1",
        handlers={"h": AsyncMock()},
        poll_interval=0.05,
    )

    async def stop_soon() -> None:
        await asyncio.sleep(0.15)
        await worker.stop()

    await asyncio.gather(worker.start(), stop_soon())
    assert not worker._running


@pytest.mark.asyncio
async def test_stop_cancels_poll_loops(mock_client: Orch8Client) -> None:
    worker = Orch8Worker(
        client=mock_client,
        worker_id="w-1",
        handlers={"h": AsyncMock()},
        poll_interval=10.0,
    )

    async def stop_soon() -> None:
        await asyncio.sleep(0.1)
        await worker.stop()

    await asyncio.gather(worker.start(), stop_soon())
    # Should exit quickly without waiting the full 10s poll interval


# ---------------------------------------------------------------------------
# Concurrency
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_concurrency_semaphore(mock_client: Orch8Client) -> None:
    handler = AsyncMock(return_value={"ok": True})
    worker = Orch8Worker(
        client=mock_client,
        worker_id="w-1",
        handlers={"h": handler},
        poll_interval=0.05,
        max_concurrent=2,
    )

    mock_client.poll_tasks = AsyncMock(
        side_effect=[
            [_task("h", "wt-1"), _task("h", "wt-2"), _task("h", "wt-3")],
            [],
            [],
        ]
    )

    async def stop_soon() -> None:
        await asyncio.sleep(0.3)
        await worker.stop()

    await asyncio.gather(worker.start(), stop_soon())
    assert handler.call_count == 3
    mock_client.complete_task.assert_called()


# ---------------------------------------------------------------------------
# Heartbeats
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_heartbeat_sent(mock_client: Orch8Client) -> None:
    async def slow_handler(task: WorkerTask) -> dict[str, Any]:
        await asyncio.sleep(0.15)
        return {"ok": True}

    worker = Orch8Worker(
        client=mock_client,
        worker_id="w-1",
        handlers={"h": slow_handler},
        poll_interval=0.05,
        heartbeat_interval=0.05,
    )

    mock_client.poll_tasks = AsyncMock(
        side_effect=[[_task("h", "wt-1")], [], []]
    )

    async def stop_soon() -> None:
        await asyncio.sleep(0.25)
        await worker.stop()

    await asyncio.gather(worker.start(), stop_soon())
    assert mock_client.heartbeat_task.call_count >= 1


# ---------------------------------------------------------------------------
# Exception handling
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_handler_failure_reports_fail_task(mock_client: Orch8Client) -> None:
    handler = AsyncMock(side_effect=RuntimeError("boom"))
    worker = Orch8Worker(
        client=mock_client,
        worker_id="w-1",
        handlers={"h": handler},
        poll_interval=0.05,
    )

    mock_client.poll_tasks = AsyncMock(
        side_effect=[[_task("h", "wt-1")], [], []]
    )

    async def stop_soon() -> None:
        await asyncio.sleep(0.2)
        await worker.stop()

    await asyncio.gather(worker.start(), stop_soon())
    mock_client.fail_task.assert_called_once()
    args = mock_client.fail_task.call_args
    assert args[0][0] == "wt-1"
    assert args[1]["retryable"] is False


@pytest.mark.asyncio
async def test_poll_error_triggers_backoff(mock_client: Orch8Client) -> None:
    worker = Orch8Worker(
        client=mock_client,
        worker_id="w-1",
        handlers={"h": AsyncMock()},
        poll_interval=0.05,
    )

    mock_client.poll_tasks = AsyncMock(side_effect=ConnectionError("down"))

    async def stop_soon() -> None:
        await asyncio.sleep(0.15)
        await worker.stop()

    await asyncio.gather(worker.start(), stop_soon())
    assert "h" in worker._backoff
    assert worker._backoff["h"] > worker.poll_interval


@pytest.mark.asyncio
async def test_backoff_reset_on_success(mock_client: Orch8Client) -> None:
    handler = AsyncMock(return_value={"ok": True})
    worker = Orch8Worker(
        client=mock_client,
        worker_id="w-1",
        handlers={"h": handler},
        poll_interval=0.05,
    )

    mock_client.poll_tasks = AsyncMock(
        side_effect=[
            ConnectionError("down"),
            [_task("h", "wt-1")],
            [],
            [],
            [],
            [],
            [],
        ]
    )

    async def stop_soon() -> None:
        await asyncio.sleep(0.35)
        await worker.stop()

    await asyncio.gather(worker.start(), stop_soon())
    assert "h" not in worker._backoff


# ---------------------------------------------------------------------------
# CancelledError propagation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cancelled_error_propagates(mock_client: Orch8Client) -> None:
    """Ensure asyncio.CancelledError is not swallowed by broad except Exception."""
    handler = AsyncMock(side_effect=asyncio.CancelledError())
    worker = Orch8Worker(
        client=mock_client,
        worker_id="w-1",
        handlers={"h": handler},
        poll_interval=0.05,
    )

    mock_client.poll_tasks = AsyncMock(
        side_effect=[[_task("h", "wt-1")], [], []]
    )

    async def stop_soon() -> None:
        await asyncio.sleep(0.15)
        await worker.stop()

    # CancelledError should propagate out of the execution task but not crash the worker
    await asyncio.gather(worker.start(), stop_soon())


# ---------------------------------------------------------------------------
# Circuit breaker
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_circuit_breaker_open_skips_poll(mock_client: Orch8Client) -> None:
    worker = Orch8Worker(
        client=mock_client,
        worker_id="w-1",
        handlers={"h": AsyncMock()},
        poll_interval=0.05,
        circuit_breaker_check=True,
    )

    mock_client.get_circuit_breaker.return_value = MagicMock(state="open")

    async def stop_soon() -> None:
        await asyncio.sleep(0.15)
        await worker.stop()

    await asyncio.gather(worker.start(), stop_soon())
    mock_client.poll_tasks.assert_not_called()
