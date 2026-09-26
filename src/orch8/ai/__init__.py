"""Durable AI-agent adapters.

Both adapters follow the engine's framework-adapter contract
(``docs/FRAMEWORK_ADAPTERS.md``): only allowlisted, JSON-portable state is
stored in ``context.data``; one framework turn runs as one worker task;
tool calls are idempotent and journaled; state is checkpointed at turn
boundaries; and output is ordinary JSON.

* :mod:`orch8.ai.langgraph` — ``pip install "orch8-io-sdk[langgraph]"``
* :mod:`orch8.ai.openai_agents` — ``pip install "orch8-io-sdk[openai-agents]"``

Frameworks are imported lazily; nothing here requires them at import time.
"""
from ._portable import PortableStateError, to_portable, turn_loop

__all__ = ["PortableStateError", "to_portable", "turn_loop"]
