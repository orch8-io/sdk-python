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
