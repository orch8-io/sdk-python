from orch8 import workflow


def test_builder_covers_nested_and_ab_split_blocks() -> None:
    definition = (
        workflow("campaign")
        .step("prepare", "prepare", {"audience": "new"})
        .parallel(
            "fanout",
            lambda branch: branch.step("email", "send-email", {"template": "welcome"}),
            lambda branch: branch.ab_split(
                "copy",
                [
                    ("a", 50, lambda variant: variant.step("a", "render", {"copy": "A"})),
                    ("b", 50, lambda variant: variant.step("b", "render", {"copy": "B"})),
                ],
            ),
        )
        .build()
    )
    assert definition["name"] == "campaign"
    assert definition["blocks"][1]["branches"][1][0]["type"] == "ab_split"


def test_saga_rejects_multiple_action_blocks() -> None:
    try:
        workflow("bad").saga(
            "saga",
            [("one", lambda branch: branch.step("a", "noop").step("b", "noop"), None)],
        )
    except ValueError as error:
        assert "one block" in str(error)
    else:
        raise AssertionError("invalid saga was accepted")


def test_builder_emits_07_contract_fields() -> None:
    import pytest

    from orch8 import SequenceValidationError, delay, retry_policy

    definition = (
        workflow("onboard")
        .input_schema({"type": "object", "required": ["email"]})
        .step(
            "charge",
            "charge",
            {"cents": 100},
            when='data.plan == "pro"',
            retry=retry_policy(
                3, 500, 10_000, retry_if='error.code != "card_declined"',
                non_retryable_codes=["card_declined"],
            ),
            output_schema={"type": "object", "required": ["charge_id"]},
            compensation={"handler": "refund", "verification": "provider_receipt"},
        )
        .delay(delay(fire_at_local="2026-03-08T09:00:00", timezone="America/New_York"))
        .loop("poll", "data.pending", lambda b: b.step("check", "check"), retain_iterations=5)
        .saga(
            "booking",
            [("hotel", lambda b: b.step("book", "book"), lambda b: b.step("cancel", "cancel"))],
        )
        .on_failure(lambda b: b.step("alert", "alert"))
        .build()
    )
    step = definition["blocks"][0]
    assert step["when"] == 'data.plan == "pro"'
    assert step["retry"]["non_retryable_codes"] == ["card_declined"]
    assert step["output_schema"]["required"] == ["charge_id"]
    assert definition["blocks"][1]["delay"] == {
        "duration": 0,
        "fire_at_local": "2026-03-08T09:00:00",
        "timezone": "America/New_York",
    }
    assert definition["blocks"][2]["retain_iterations"] == 5
    assert definition["blocks"][3]["steps"][0]["compensation"]["id"] == "cancel"
    assert definition["on_failure"][0]["id"] == "alert"
    assert definition["input_schema"]["required"] == ["email"]

    with pytest.raises(SequenceValidationError, match="unknown fields"):
        workflow("x").step("a", "h", bogus=1).build()  # type: ignore[call-arg]
    with pytest.raises(SequenceValidationError, match="duplicate"):
        workflow("x").step("a", "h").step("a", "h").build()
    with pytest.raises(SequenceValidationError, match="max_attempts"):
        workflow("x").step("a", "h", retry={"max_attempts": 0, "initial_backoff": 1, "max_backoff": 1}).build()
    with pytest.raises(SequenceValidationError, match="max_iterations"):
        workflow("x").loop("l", "true", lambda b: b.step("s", "h"), max_iterations=100_001).build()
