"""Typed-path wrappers for the engine's portable-continuity control plane.

Request and response bodies stay open JSON objects: the engine publishes the
full schema through OpenAPI and this experimental surface evolves faster than
the stable workflow primitives. Endpoints not wrapped here remain reachable
through :meth:`Orch8Client.request`.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal
from urllib.parse import quote

if TYPE_CHECKING:
    from .client import Orch8Client

JsonObject = dict[str, Any]


def _s(value: str) -> str:
    return quote(value, safe="")


def _q(**values: Any) -> dict[str, Any]:
    return {k: v for k, v in values.items() if v is not None}


class ContinuityClient:
    """Exposed as ``client.continuity``."""

    def __init__(self, client: Orch8Client) -> None:
        self._client = client

    async def _post(self, path: str, body: JsonObject) -> Any:
        return await self._client.request("POST", path, json=body)

    async def _get(self, path: str, **params: Any) -> Any:
        return await self._client.request("GET", path, params=_q(**params))

    # -- executions & runtimes --

    async def create_execution(self, body: JsonObject) -> JsonObject:
        return await self._post("/continuity/executions", body)

    async def get_execution(self, id: str, tenant_id: str) -> JsonObject:
        return await self._get(f"/continuity/executions/{_s(id)}", tenant_id=tenant_id)

    async def list_locations(self, id: str, tenant_id: str) -> list[JsonObject]:
        return await self._get(
            f"/continuity/executions/{_s(id)}/locations", tenant_id=tenant_id
        )

    async def register_runtime(self, body: JsonObject) -> JsonObject:
        return await self._post("/runtimes/register", body)

    async def list_runtimes(self, tenant_id: str) -> list[JsonObject]:
        return await self._get("/runtimes", tenant_id=tenant_id)

    async def choose_placement(self, id: str, body: JsonObject) -> JsonObject:
        return await self._post(f"/continuity/executions/{_s(id)}/placement", body)

    # -- handoffs & capsules --

    async def preview_handoff(self, id: str, body: JsonObject) -> JsonObject:
        return await self._post(f"/continuity/executions/{_s(id)}/handoff-preview", body)

    async def create_handoff(self, body: JsonObject) -> JsonObject:
        return await self._post("/continuity/handoffs", body)

    async def get_handoff(self, id: str, tenant_id: str) -> JsonObject:
        return await self._get(f"/continuity/handoffs/{_s(id)}", tenant_id=tenant_id)

    async def export_handoff(self, id: str, body: JsonObject) -> JsonObject:
        return await self._post(f"/continuity/handoffs/{_s(id)}/export", body)

    async def attach_device_capsule(self, id: str, body: JsonObject) -> JsonObject:
        return await self._post(
            f"/continuity/handoffs/{_s(id)}/attach-device-capsule", body
        )

    async def accept_handoff(self, id: str, body: JsonObject) -> JsonObject:
        return await self._post(f"/continuity/handoffs/{_s(id)}/accept", body)

    async def reject_handoff(self, id: str, body: JsonObject) -> JsonObject:
        return await self._post(f"/continuity/handoffs/{_s(id)}/reject", body)

    async def resume_handoff(self, id: str, body: JsonObject) -> JsonObject:
        return await self._post(f"/continuity/handoffs/{_s(id)}/resume", body)

    async def revoke_handoff(self, id: str, body: JsonObject) -> JsonObject:
        return await self._post(f"/continuity/handoffs/{_s(id)}/revoke", body)

    async def import_capsule(self, body: JsonObject) -> JsonObject:
        return await self._post("/continuity/capsules/import", body)

    async def issue_grant(self, body: JsonObject) -> JsonObject:
        return await self._post("/continuity/grants", body)

    async def consume_grant(self, body: JsonObject) -> JsonObject:
        return await self._post("/continuity/grants/consume", body)

    # -- effects, provenance, checkpoints --

    async def list_effects(self, id: str, tenant_id: str) -> list[JsonObject]:
        return await self._get(
            f"/continuity/executions/{_s(id)}/effects", tenant_id=tenant_id
        )

    async def resolve_effect(self, id: str, body: JsonObject) -> JsonObject:
        return await self._post(f"/continuity/effects/{_s(id)}/resolve", body)

    async def list_provenance(self, id: str, tenant_id: str) -> list[JsonObject]:
        return await self._get(
            f"/continuity/executions/{_s(id)}/provenance", tenant_id=tenant_id
        )

    async def record_provenance(self, id: str, body: JsonObject) -> JsonObject:
        return await self._post(f"/continuity/executions/{_s(id)}/provenance", body)

    async def verify_provenance(
        self, id: str, tenant_id: str, expected_head: str | None = None
    ) -> JsonObject:
        return await self._get(
            f"/continuity/executions/{_s(id)}/provenance/verify",
            tenant_id=tenant_id,
            expected_head=expected_head,
        )

    async def list_checkpoints(
        self, id: str, tenant_id: str, limit: int | None = None
    ) -> list[JsonObject]:
        return await self._get(
            f"/continuity/executions/{_s(id)}/checkpoints",
            tenant_id=tenant_id,
            limit=limit,
        )

    async def get_checkpoint(
        self, id: str, checkpoint_id: str, tenant_id: str
    ) -> JsonObject:
        return await self._get(
            f"/continuity/executions/{_s(id)}/checkpoints/{_s(checkpoint_id)}",
            tenant_id=tenant_id,
        )

    # -- compensations --

    async def preview_compensation(self, id: str, body: JsonObject) -> JsonObject:
        return await self._post(
            f"/continuity/executions/{_s(id)}/compensations/preview", body
        )

    async def create_compensation(self, id: str, body: JsonObject) -> JsonObject:
        return await self._post(f"/continuity/executions/{_s(id)}/compensations", body)

    async def get_compensation(self, id: str, tenant_id: str) -> JsonObject:
        return await self._get(f"/continuity/compensations/{_s(id)}", tenant_id=tenant_id)

    async def claim_compensation(self, id: str, body: JsonObject) -> JsonObject:
        return await self._post(f"/continuity/compensations/{_s(id)}/claim", body)

    async def compensation_step(
        self,
        id: str,
        effect_id: str,
        action: Literal["complete", "fail", "verify"],
        body: JsonObject,
    ) -> JsonObject:
        return await self._post(
            f"/continuity/compensations/{_s(id)}/steps/{_s(effect_id)}/{action}", body
        )

    # -- budgets & attention --

    async def reserve_budget(self, id: str, body: JsonObject) -> JsonObject:
        return await self._post(
            f"/continuity/executions/{_s(id)}/budget-reservations", body
        )

    async def list_budget_reservations(self, id: str, tenant_id: str) -> list[JsonObject]:
        return await self._get(
            f"/continuity/executions/{_s(id)}/budget-reservations", tenant_id=tenant_id
        )

    async def create_attention_task(self, body: JsonObject) -> JsonObject:
        return await self._post("/continuity/attention", body)

    async def decide_attention_task(self, id: str, body: JsonObject) -> JsonObject:
        return await self._post(f"/continuity/attention/{_s(id)}/decide", body)
