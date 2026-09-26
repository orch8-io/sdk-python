"""Typed shapes and dependency-free validation for the Orch8 0.7 sequence DSL.

These mirror the engine's ``orch8-types`` sequence model (and the Node SDK's
Zod schemas). Durations are integer milliseconds on the wire.
"""
from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any, Literal

from typing_extensions import NotRequired, TypedDict


class DelaySpec(TypedDict):
    duration: int
    business_days_only: NotRequired[bool]
    jitter: NotRequired[int]
    holidays: NotRequired[list[str]]
    #: Local wall-clock target, e.g. ``"2026-03-08T02:30:00"``.
    fire_at_local: NotRequired[str]
    #: IANA zone used for ``fire_at_local`` and business-day arithmetic.
    timezone: NotRequired[str]


class RetryPolicy(TypedDict):
    max_attempts: int
    initial_backoff: int
    max_backoff: int
    backoff_multiplier: NotRequired[float]
    #: Expression evaluated against the error; retry only when truthy.
    retry_if: NotRequired[str]
    #: Error codes that are never retried.
    non_retryable_codes: NotRequired[list[str]]


class SendWindow(TypedDict, total=False):
    start_hour: int
    end_hour: int
    days: list[int]


class HumanChoice(TypedDict):
    label: str
    value: str


class HumanInputDef(TypedDict, total=False):
    prompt: str
    timeout: int
    escalation_handler: str
    choices: list[HumanChoice]
    store_as: str
    allow_comment: bool


class EscalationDef(TypedDict):
    handler: str
    params: NotRequired[Any]


class StepCompensation(TypedDict):
    handler: str
    params: NotRequired[Any]
    depends_on: NotRequired[list[str]]
    verification: NotRequired[Literal["handler_result", "provider_receipt", "manual"]]


class StepOptions(TypedDict, total=False):
    delay: DelaySpec
    retry: RetryPolicy
    timeout: int
    rate_limit_key: str
    send_window: SendWindow
    context_access: dict[str, Any]
    cancellable: bool
    wait_for_input: HumanInputDef
    queue_name: str
    deadline: int
    on_deadline_breach: EscalationDef
    fallback_handler: str
    cache_key: str
    #: JSON Schema the step output must satisfy.
    output_schema: Any
    #: Guard expression; the step is skipped when it evaluates falsy.
    when: str
    compensation: StepCompensation


def retry_policy(
    max_attempts: int,
    initial_backoff: int,
    max_backoff: int,
    *,
    backoff_multiplier: float | None = None,
    retry_if: str | None = None,
    non_retryable_codes: list[str] | None = None,
) -> RetryPolicy:
    """Build a retry policy, including 0.7 error filters."""
    policy: RetryPolicy = {
        "max_attempts": max_attempts,
        "initial_backoff": initial_backoff,
        "max_backoff": max_backoff,
    }
    if backoff_multiplier is not None:
        policy["backoff_multiplier"] = backoff_multiplier
    if retry_if is not None:
        policy["retry_if"] = retry_if
    if non_retryable_codes is not None:
        policy["non_retryable_codes"] = list(non_retryable_codes)
    return policy


def delay(
    duration: int = 0,
    *,
    fire_at_local: str | None = None,
    timezone: str | None = None,
    business_days_only: bool | None = None,
    jitter: int | None = None,
    holidays: list[str] | None = None,
) -> DelaySpec:
    """Build a durable delay; ``fire_at_local`` + ``timezone`` targets local time."""
    spec: DelaySpec = {"duration": duration}
    if fire_at_local is not None:
        spec["fire_at_local"] = fire_at_local
    if timezone is not None:
        spec["timezone"] = timezone
    if business_days_only is not None:
        spec["business_days_only"] = business_days_only
    if jitter is not None:
        spec["jitter"] = jitter
    if holidays is not None:
        spec["holidays"] = list(holidays)
    return spec


# --------------------------------------------------------------------------- #
# Validation
# --------------------------------------------------------------------------- #

_STEP_KEYS = {"type", "id", "handler", "params", *StepOptions.__annotations__}
_BLOCK_KEYS: dict[str, set[str]] = {
    "step": _STEP_KEYS,
    "parallel": {"type", "id", "branches"},
    "race": {"type", "id", "branches", "semantics"},
    "loop": {
        "type", "id", "condition", "body", "max_iterations", "break_on",
        "continue_on_error", "poll_interval", "retain_iterations",
    },
    "for_each": {
        "type", "id", "collection", "item_var", "body", "max_iterations",
        "retain_iterations",
    },
    "router": {"type", "id", "routes", "default"},
    "try_catch": {"type", "id", "try_block", "catch_block", "finally_block"},
    "sub_sequence": {"type", "id", "sequence_name", "version", "input"},
    "ab_split": {"type", "id", "variants"},
    "cancellation_scope": {"type", "id", "blocks"},
    "saga": {"type", "id", "steps"},
}
MAX_ITERATIONS = 100_000


class SequenceValidationError(ValueError):
    """Raised when a sequence definition violates the 0.7 DSL contract."""


def _non_negative_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _check_retry(path: str, retry: Any) -> None:
    if retry is None:
        return
    if not isinstance(retry, Mapping):
        raise SequenceValidationError(f"{path}.retry must be an object")
    attempts = retry.get("max_attempts")
    if not isinstance(attempts, int) or isinstance(attempts, bool) or attempts < 1:
        raise SequenceValidationError(f"{path}.retry.max_attempts must be a positive integer")
    for key in ("initial_backoff", "max_backoff"):
        if not _non_negative_int(retry.get(key)):
            raise SequenceValidationError(f"{path}.retry.{key} must be integer milliseconds")
    if "retry_if" in retry and not (isinstance(retry["retry_if"], str) and retry["retry_if"]):
        raise SequenceValidationError(f"{path}.retry.retry_if must be a non-empty expression")
    codes = retry.get("non_retryable_codes")
    if codes is not None and not (
        isinstance(codes, list) and all(isinstance(c, str) and c for c in codes)
    ):
        raise SequenceValidationError(f"{path}.retry.non_retryable_codes must be strings")


def _check_delay(path: str, spec: Any) -> None:
    if spec is None:
        return
    if not isinstance(spec, Mapping) or not _non_negative_int(spec.get("duration")):
        raise SequenceValidationError(f"{path}.delay.duration must be integer milliseconds")
    if "fire_at_local" in spec and not isinstance(spec["fire_at_local"], str):
        raise SequenceValidationError(f"{path}.delay.fire_at_local must be a string")


def _check_blocks(path: str, blocks: Any, seen: set[str]) -> None:
    if not isinstance(blocks, list):
        raise SequenceValidationError(f"{path} must be a list of blocks")
    for index, block in enumerate(blocks):
        _check_block(f"{path}[{index}]", block, seen)


def _check_block(path: str, block: Any, seen: set[str]) -> None:
    if not isinstance(block, Mapping):
        raise SequenceValidationError(f"{path} must be an object")
    kind = block.get("type")
    allowed = _BLOCK_KEYS.get(kind)  # type: ignore[arg-type]
    if allowed is None:
        raise SequenceValidationError(f"{path}.type {kind!r} is not a known block type")
    block_id = block.get("id")
    if not isinstance(block_id, str) or not block_id:
        raise SequenceValidationError(f"{path}.id must be a non-empty string")
    if block_id in seen:
        raise SequenceValidationError(f"duplicate block id {block_id!r}")
    seen.add(block_id)
    unknown = set(block) - allowed
    if unknown:
        raise SequenceValidationError(
            f"{path} ({kind}) has unknown fields: {', '.join(sorted(unknown))}"
        )
    here = f"{path}<{block_id}>"
    if kind == "step":
        if not isinstance(block.get("handler"), str) or not block["handler"]:
            raise SequenceValidationError(f"{here}.handler is required")
        _check_retry(here, block.get("retry"))
        _check_delay(here, block.get("delay"))
        if "when" in block and not isinstance(block["when"], str):
            raise SequenceValidationError(f"{here}.when must be an expression string")
        comp = block.get("compensation")
        if comp is not None and not (isinstance(comp, Mapping) and comp.get("handler")):
            raise SequenceValidationError(f"{here}.compensation.handler is required")
    elif kind in ("parallel", "race"):
        branches = block.get("branches")
        if not isinstance(branches, list):
            raise SequenceValidationError(f"{here}.branches must be a list")
        for i, branch in enumerate(branches):
            _check_blocks(f"{here}.branches[{i}]", branch, seen)
        if kind == "race" and block.get("semantics") not in (
            None, "first_to_resolve", "first_to_succeed",
        ):
            raise SequenceValidationError(f"{here}.semantics is invalid")
    elif kind in ("loop", "for_each"):
        _check_blocks(f"{here}.body", block.get("body"), seen)
        limit = block.get("max_iterations")
        if limit is not None and not (
            isinstance(limit, int) and 1 <= limit <= MAX_ITERATIONS
        ):
            raise SequenceValidationError(
                f"{here}.max_iterations must be between 1 and {MAX_ITERATIONS}"
            )
        retain = block.get("retain_iterations")
        if retain is not None and not _non_negative_int(retain):
            raise SequenceValidationError(f"{here}.retain_iterations must be >= 0")
        key = "condition" if kind == "loop" else "collection"
        if not isinstance(block.get(key), str):
            raise SequenceValidationError(f"{here}.{key} is required")
    elif kind == "router":
        for i, route in enumerate(block.get("routes") or []):
            if not isinstance(route, Mapping) or not isinstance(route.get("condition"), str):
                raise SequenceValidationError(f"{here}.routes[{i}].condition is required")
            _check_blocks(f"{here}.routes[{i}].blocks", route.get("blocks"), seen)
        if "default" in block:
            _check_blocks(f"{here}.default", block["default"], seen)
    elif kind == "try_catch":
        for key in ("try_block", "catch_block"):
            _check_blocks(f"{here}.{key}", block.get(key), seen)
        if "finally_block" in block:
            _check_blocks(f"{here}.finally_block", block["finally_block"], seen)
    elif kind == "ab_split":
        for i, variant in enumerate(block.get("variants") or []):
            weight = variant.get("weight") if isinstance(variant, Mapping) else None
            if not _non_negative_int(weight):
                raise SequenceValidationError(f"{here}.variants[{i}].weight must be >= 0")
            _check_blocks(f"{here}.variants[{i}].blocks", variant.get("blocks"), seen)
    elif kind == "cancellation_scope":
        _check_blocks(f"{here}.blocks", block.get("blocks"), seen)
    elif kind == "saga":
        steps = block.get("steps")
        if not isinstance(steps, list) or not steps:
            raise SequenceValidationError(f"{here}.steps must be a non-empty list")
        for i, step in enumerate(steps):
            if not isinstance(step, Mapping) or "action" not in step:
                raise SequenceValidationError(f"{here}.steps[{i}].action is required")
            _check_block(f"{here}.steps[{i}].action", step["action"], seen)
            if step.get("compensation") is not None:
                _check_block(f"{here}.steps[{i}].compensation", step["compensation"], seen)
    elif kind == "sub_sequence":
        if not isinstance(block.get("sequence_name"), str):
            raise SequenceValidationError(f"{here}.sequence_name is required")


def validate_sequence(definition: Mapping[str, Any], *, native: bool = False) -> None:
    """Validate an authoring payload for ``create_sequence``.

    With ``native=True`` the engine's own strict validator from the optional
    ``orch8-engine-native`` package is also applied.
    """
    if not isinstance(definition.get("name"), str) or not definition["name"]:
        raise SequenceValidationError("name must be a non-empty string")
    seen: set[str] = set()
    _check_blocks("blocks", definition.get("blocks"), seen)
    for key in ("on_failure", "on_cancel"):
        if key in definition:
            _check_blocks(key, definition[key], seen)
    if native:
        from .testing.native import load_native

        module = load_native()
        payload = {
            "id": "00000000-0000-0000-0000-000000000000",
            "tenant_id": "validate",
            "namespace": "default",
            "version": 1,
            "created_at": "2026-01-01T00:00:00Z",
            **definition,
        }
        try:
            module.validate_sequence_json(json.dumps(payload))
        except ValueError as exc:
            raise SequenceValidationError(str(exc)) from exc
