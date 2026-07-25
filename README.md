# orch8-io-sdk

Python SDK for the [Orch8](https://orch8.io) workflow engine.

## Installation

```bash
pip install orch8-io-sdk
```

Requires Python 3.10+.

Version 0.3 understands the current Orch8 0.7-dev response contract for sequence lifecycle
metadata and resumable worker checkpoints. New or experimental engine routes
are immediately available through the safe low-level `request()` method.

## Quick Start

```python
import asyncio
from orch8 import Orch8Client

async def main():
    async with Orch8Client("https://api.orch8.io", tenant_id="my-tenant") as client:
        # Create a sequence
        seq = await client.create_sequence({
            "name": "my-sequence",
            "namespace": "default",
            "blocks": [],
        })
        print(f"Created sequence: {seq.id}")

        # Start an instance
        inst = await client.create_instance({
            "sequence_id": seq.id,
            "context": {"user_id": "123"},
        })
        print(f"Started instance: {inst.id}")

asyncio.run(main())
```

```python
engine_info = await client.request("GET", "/info")
```

Safe requests retry transient `408`, `425`, `429`, and `5xx` responses up to
three times. Each attempt has a 30-second timeout. `get_headers` is evaluated
for every attempt, so expiring credentials can be refreshed without rebuilding
the client.

```python
from orch8 import Orch8Client, RetryConfig

client = Orch8Client(
    "https://api.orch8.io",
    get_headers=lambda: {"Authorization": f"Bearer {get_token()}"},
    retry=RetryConfig(max_attempts=3, base_delay=0.25),
    timeout=30,
)
```

Request observers, cursor-preserving pagination, and resumable SSE are exposed
without bypassing authentication:

```python
client = Orch8Client(
    "https://api.orch8.io",
    on_response=lambda event: record_latency(event.duration_ms),
)
page = await client.request_page("/instances", {"limit": 50})

async for event in client.stream_instance_events(
    instance_id, last_event_id=saved_cursor
):
    saved_cursor = event["id"]
    consume(event["data"])
```

Resource IDs are URL-encoded as individual path segments. `ORCH8_ROUTES` and
`ORCH8_API_VERSION` are generated from the engine OpenAPI contract. Worker
defaults match Node and Go (1s polling, 15s heartbeat, concurrency 10), enforce
task timeouts, and expose `worker.stats()`.

## Worker

Run a polling worker that claims and executes tasks:

```python
import asyncio
from orch8 import Orch8Client, Orch8Worker

async def handle_email(task):
    print(f"Sending email to {task.params['to']}")
    return {"sent": True}

async def main():
    client = Orch8Client("https://api.orch8.io", tenant_id="my-tenant")
    worker = Orch8Worker(
        client=client,
        worker_id="worker-1",
        handlers={"send-email": handle_email},
        max_concurrent=10,
    )
    await worker.start()  # blocks until worker.stop() is called

asyncio.run(main())
```

## Error Handling

```python
from orch8 import Orch8Error

try:
    await client.get_instance("non-existent")
except Orch8Error as exc:
    print(f"API error {exc.status} on {exc.path}")
```

## Development

```bash
# Install in editable mode with dev dependencies
pip install -e ".[dev]"

# Run tests
pytest
```
