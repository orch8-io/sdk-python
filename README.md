# orch8-sdk

Python SDK for the [Orch8](https://orch8.io) workflow engine.

## Installation

```bash
pip install orch8-sdk
```

Requires Python 3.10+.

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
