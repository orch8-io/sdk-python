"""Orch8 SDK errors."""


class Orch8Error(Exception):
    def __init__(self, status: int, body: str, path: str) -> None:
        self.status = status
        self.body = body
        self.path = path
        super().__init__(f"Orch8 API error {status} on {path}: {body}")
