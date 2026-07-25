import json
from pathlib import Path

from orch8 import ORCH8_API_VERSION, ORCH8_ROUTES


def test_generated_contract_and_transport_fixture() -> None:
    fixture = json.loads(
        (Path(__file__).parents[1] / "testdata" / "transport.json").read_text()
    )
    assert ORCH8_API_VERSION == "1.0.0"
    assert len(ORCH8_ROUTES) > 100
    assert any(route["path"] == "/instances/{id}/stream" for route in ORCH8_ROUTES)
    assert fixture["defaults"]["max_attempts"] == 3
