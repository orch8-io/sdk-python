import json

import httpx
import respx

from orch8 import Orch8Client

BASE = "http://engine.test"


@respx.mock
async def test_continuity_paths_encode_segments_and_queries() -> None:
    get = respx.get(f"{BASE}/continuity/executions/a%2Fb").mock(
        return_value=httpx.Response(200, json={"id": "a/b"})
    )
    post = respx.post(f"{BASE}/continuity/handoffs/h1/accept").mock(
        return_value=httpx.Response(200, json={"state": "accepted"})
    )
    verify = respx.get(f"{BASE}/continuity/executions/e1/provenance/verify").mock(
        return_value=httpx.Response(200, json={"valid": True})
    )
    async with Orch8Client(BASE) as client:
        assert (await client.continuity.get_execution("a/b", "t1"))["id"] == "a/b"
        await client.continuity.accept_handoff("h1", {"runtime_id": "r"})
        await client.continuity.verify_provenance("e1", "t1")
    assert get.calls[0].request.url.params["tenant_id"] == "t1"
    assert json.loads(post.calls[0].request.content) == {"runtime_id": "r"}
    assert "expected_head" not in verify.calls[0].request.url.params
