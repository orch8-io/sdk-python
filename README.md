# orch8-io-sdk

Python SDK for the [Orch8](https://orch8.io) workflow engine.

## Installation

```bash
pip install orch8-io-sdk
# optional integrations
pip install "orch8-io-sdk[fastapi]"        # push-dispatch router
pip install "orch8-io-sdk[django]"         # push-dispatch view
pip install "orch8-io-sdk[langgraph]"      # durable LangGraph turns
pip install "orch8-io-sdk[openai-agents]"  # durable OpenAI Agents tools
```

Requires Python 3.10+.

Version 0.7 tracks the Orch8 0.7 sequence contract, matching the Node SDK:
sagas and per-step compensation, conditional steps (`when` guards), filtered
retries (`retry_if`, `non_retryable_codes`), output schemas, local-time delays
(`fire_at_local` + `timezone`), and bounded loop history (`retain_iterations`).
Workers follow the engine lease protocol (`claim_epoch` on every
acknowledgement, server poll/heartbeat hints). Portable continuity APIs are
available under `client.continuity`. New or experimental engine routes are
reachable through the authenticated low-level `request()` method.

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

## Code-first workflow DSL

```python
from orch8 import workflow

checkout = (
    workflow("checkout")
    .step("charge", "charge", {"customer_id": "cus_123", "cents": 2500})
    .parallel(
        "notify",
        lambda branch: branch.step("email", "send-email", {"template": "receipt"}),
        lambda branch: branch.step("audit", "write-audit", {}),
    )
    .build()
)
```

The builder covers all eleven block types and emits the same JSON accepted by
`create_sequence`; `build()` validates it against the 0.7 contract (unknown
fields, duplicate ids, retry and loop bounds). Step options are typed
(`orch8.StepOptions`):

```python
from orch8 import delay, retry_policy, workflow

onboarding = (
    workflow("onboarding")
    .input_schema({"type": "object", "required": ["email"]})
    .step(
        "charge", "charge", {"cents": 2500},
        when='data.plan == "pro"',
        retry=retry_policy(3, 500, 10_000, non_retryable_codes=["card_declined"]),
        output_schema={"type": "object", "required": ["charge_id"]},
        compensation={"handler": "refund"},
    )
    .delay(delay(fire_at_local="2026-03-09T09:00:00", timezone="America/New_York"))
    .loop("poll", "data.pending", lambda b: b.step("check", "check"), retain_iterations=10)
    .on_failure(lambda b: b.step("alert", "alert"))
    .build()
)
```

`validate_sequence(definition, native=True)` additionally applies the
engine's own strict validator when the native bindings are installed.

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

The worker echoes each task's `claim_epoch` on heartbeat, completion, and
failure, respects the server's `poll_after_ms`, and heartbeats no less often
than the advertised interval or half the lease. A rejected or ambiguous
completion is left for lease recovery rather than reported as a failure.
Pass `queue="name"` to claim from a named queue. Use
`client.poll_task_batch()` for the lease hints in custom loops.

Inside a handler, `current_task()` exposes durable checkpoints that survive
retries and lease reclaims:

```python
from orch8 import current_task

async def scan(task):
    ctx = current_task()
    page = (ctx.resume_checkpoint or {}).get("page", 0)
    for page in range(page, 100):
        await process(page, idempotency_key=f"{ctx.idempotency_prefix}:{page}")
        await ctx.checkpoint({"page": page + 1})
    return {"pages": 100}
```

## Background jobs

`client.jobs` enqueues single durable handler invocations with retries,
delays and idempotency (requires an engine with the `/jobs` API):

```python
job = await client.jobs.enqueue(
    "send_email", {"to": "ada@example.com"},
    retry={"max_attempts": 5, "initial_backoff_ms": 1_000},
    delay_ms=60_000, idempotency_key="welcome:ada",
)
done = await client.jobs.wait_for(job.id, timeout=300)
async for failed in client.jobs.list(status="failed"):
    print(failed.id, failed.error)
await client.jobs.cancel(job.id)
```

## Push dispatch

Queues in push mode make the engine POST a signed envelope to your endpoint.
`PushDispatcher` verifies `X-Orch8-Signature` (HMAC-SHA256 over
`"{timestamp}.{body}"`, constant-time, 5-minute replay window), then claims
from the envelope's queue and runs the handler with the normal lease
protocol — a pushed task is still pending, and the envelope carries no lease,
so a verified push acts as a wake-up. Duplicate pushes find nothing to claim.

```python
from orch8.push import PushDispatcher

dispatcher = PushDispatcher(client, {"resize": resize}, secret=os.environ["ORCH8_PUSH_SECRET"])

# AWS Lambda (API Gateway v1/v2 or Function URL) — no AWS dependency
from orch8.integrations.aws_lambda import lambda_handler
handler = lambda_handler(dispatcher)

# FastAPI
from orch8.integrations.fastapi import push_router
app.include_router(push_router(dispatcher, path="/orch8/push"))

# Django
from orch8.integrations.django import push_view
urlpatterns = [path("orch8/push", push_view(dispatcher))]
```

`orch8.integrations.asgi.PushASGIApp` and `orch8.integrations.wsgi.push_wsgi_app`
cover other frameworks; `verify_push_signature()` is available on its own and
also verifies outbound webhooks, which use the same scheme.

## Testing

`orch8.testing.FakeEngine` is a pure-Python engine stand-in: worker poll,
queue poll, complete, fail and heartbeat with lease epochs and reaping;
instances running linear `step` sequences with retries, delays and durable
checkpoints; and the jobs API — all on a virtual clock that skips time.

```python
# conftest.py
pytest_plugins = ["orch8.testing.pytest_plugin"]

# test_checkout.py
async def test_checkout(orch8_engine, orch8_client):
    inst = await orch8_engine.run(orch8_client, checkout, {"charge": charge})
    assert inst.state == "completed"
```

`orch8_engine.serve()` exposes the same fake over loopback HTTP. For full DSL
semantics on the real engine, `orch8.testing.NativeEnvironment` (fixture
`orch8_native`) runs sequences in-memory with time skipping through the
`orch8-engine-native` PyO3 bindings, built from `engine/packages/python-native`
with maturin (not yet published to PyPI).

## Durable AI agents

Adapters follow the engine's framework-adapter contract: portable state only,
one framework turn per worker task, checkpoints at turn boundaries.

```python
from orch8.ai import turn_loop
from orch8.ai.langgraph import langgraph_turn_handler
from orch8.ai.openai_agents import openai_agent_turn_handler

handlers = {
    "support_turn": langgraph_turn_handler(graph, portable_keys={"messages"}),
    "billing_turn": openai_agent_turn_handler(billing_agent),
}
support = turn_loop(workflow("support"), "chat", "support_turn", max_turns=20).build()
```

The OpenAI Agents adapter journals every function-tool result by
`tool_call_id` in the task's durable checkpoint, so a retried turn reuses it
instead of repeating the side effect; `tool_idempotency_key()` gives tools a
stable key to pass to providers.

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
uv sync --extra dev
uv run pytest
```
