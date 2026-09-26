"""Fluent, dependency-free builder for the Orch8 workflow JSON DSL."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any

from typing_extensions import Unpack

from .dsl import DelaySpec, StepOptions, validate_sequence

Block = dict[str, Any]
Branch = Callable[["WorkflowBuilder"], None]


class WorkflowBuilder:
    def __init__(self, name: str, namespace: str = "default") -> None:
        if not name:
            raise ValueError("workflow name cannot be empty")
        self.name = name
        self.namespace = namespace
        self._items: list[Block] = []
        self._extra: Block = {}

    def step(
        self,
        id: str,
        handler: str,
        params: Mapping[str, Any] | None = None,
        **options: Unpack[StepOptions],
    ) -> WorkflowBuilder:
        """Append a step. Options include ``when`` guards, ``retry`` (with
        ``retry_if`` / ``non_retryable_codes`` filters), ``output_schema``,
        local-time ``delay`` and per-step ``compensation``."""
        self._items.append(
            {"type": "step", "id": id, "handler": handler, "params": dict(params or {}), **options}
        )
        return self

    def delay(self, spec: DelaySpec, id: str | None = None) -> WorkflowBuilder:
        """Append a durable wait as a ``noop`` step carrying ``spec``."""
        block_id = id or f"delay_{len(self._items)}"
        self._items.append(
            {"type": "step", "id": block_id, "handler": "noop", "params": {}, "delay": dict(spec)}
        )
        return self

    def parallel(self, id: str, *branches: Branch) -> WorkflowBuilder:
        self._items.append(
            {"type": "parallel", "id": id, "branches": [self._branch(fn) for fn in branches]}
        )
        return self

    def race(self, id: str, *branches: Branch, semantics: str | None = None) -> WorkflowBuilder:
        block: Block = {"type": "race", "id": id, "branches": [self._branch(fn) for fn in branches]}
        if semantics is not None:
            block["semantics"] = semantics
        self._items.append(block)
        return self

    def loop(
        self,
        id: str,
        condition: str,
        body: Branch,
        *,
        max_iterations: int = 1000,
        retain_iterations: int | None = None,
        **options: Any,
    ) -> WorkflowBuilder:
        """Append a loop; ``retain_iterations`` bounds stored iteration history."""
        if retain_iterations is not None:
            options["retain_iterations"] = retain_iterations
        self._items.append(
            {
                "type": "loop",
                "id": id,
                "condition": condition,
                "body": self._branch(body),
                "max_iterations": max_iterations,
                **options,
            }
        )
        return self

    def for_each(
        self,
        id: str,
        collection: str,
        body: Branch,
        *,
        item_var: str = "item",
        retain_iterations: int | None = None,
        **options: Any,
    ) -> WorkflowBuilder:
        if retain_iterations is not None:
            options["retain_iterations"] = retain_iterations
        self._items.append(
            {
                "type": "for_each",
                "id": id,
                "collection": collection,
                "item_var": item_var,
                "body": self._branch(body),
                **options,
            }
        )
        return self

    def router(
        self,
        id: str,
        routes: Sequence[tuple[str, Branch]],
        *,
        default: Branch | None = None,
    ) -> WorkflowBuilder:
        block: Block = {
            "type": "router",
            "id": id,
            "routes": [
                {"condition": condition, "blocks": self._branch(branch)}
                for condition, branch in routes
            ],
        }
        if default is not None:
            block["default"] = self._branch(default)
        self._items.append(block)
        return self

    def try_catch(
        self,
        id: str,
        try_block: Branch,
        catch_block: Branch,
        *,
        finally_block: Branch | None = None,
    ) -> WorkflowBuilder:
        block: Block = {
            "type": "try_catch",
            "id": id,
            "try_block": self._branch(try_block),
            "catch_block": self._branch(catch_block),
        }
        if finally_block is not None:
            block["finally_block"] = self._branch(finally_block)
        self._items.append(block)
        return self

    def sub_sequence(
        self, id: str, sequence_name: str, *, version: int | None = None, input: Any = None
    ) -> WorkflowBuilder:
        block: Block = {"type": "sub_sequence", "id": id, "sequence_name": sequence_name}
        if version is not None:
            block["version"] = version
        if input is not None:
            block["input"] = input
        self._items.append(block)
        return self

    def ab_split(
        self, id: str, variants: Sequence[tuple[str, int, Branch]]
    ) -> WorkflowBuilder:
        self._items.append(
            {
                "type": "ab_split",
                "id": id,
                "variants": [
                    {"name": name, "weight": weight, "blocks": self._branch(branch)}
                    for name, weight, branch in variants
                ],
            }
        )
        return self

    def cancellation_scope(self, id: str, body: Branch) -> WorkflowBuilder:
        self._items.append(
            {"type": "cancellation_scope", "id": id, "blocks": self._branch(body)}
        )
        return self

    def saga(
        self,
        id: str,
        steps: Sequence[tuple[str, Branch, Branch | None]],
    ) -> WorkflowBuilder:
        resolved = []
        for step_id, action, compensation in steps:
            actions = self._branch(action)
            compensations = self._branch(compensation) if compensation else []
            if len(actions) != 1 or len(compensations) > 1:
                raise ValueError("each saga action must have one block and compensation at most one")
            item: Block = {"id": step_id, "action": actions[0]}
            if compensations:
                item["compensation"] = compensations[0]
            resolved.append(item)
        self._items.append({"type": "saga", "id": id, "steps": resolved})
        return self

    def raw(self, block: Mapping[str, Any]) -> WorkflowBuilder:
        self._items.append(dict(block))
        return self

    def on_failure(self, body: Branch) -> WorkflowBuilder:
        """Best-effort cleanup blocks run when the instance fails."""
        self._extra["on_failure"] = self._branch(body)
        return self

    def on_cancel(self, body: Branch) -> WorkflowBuilder:
        self._extra["on_cancel"] = self._branch(body)
        return self

    def input_schema(self, schema: Mapping[str, Any]) -> WorkflowBuilder:
        """JSON Schema that instance input is validated against."""
        self._extra["input_schema"] = dict(schema)
        return self

    def build(self, *, validate: bool = True) -> Block:
        """Return the ``create_sequence`` payload, validated by default."""
        payload: Block = {
            "name": self.name,
            "namespace": self.namespace,
            "blocks": list(self._items),
            **self._extra,
        }
        if validate:
            validate_sequence(payload)
        return payload

    def _branch(self, callback: Branch) -> list[Block]:
        inner = WorkflowBuilder("_inner", self.namespace)
        callback(inner)
        return inner._items


def workflow(name: str, namespace: str = "default") -> WorkflowBuilder:
    return WorkflowBuilder(name, namespace)
