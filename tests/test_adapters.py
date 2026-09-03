import asyncio

from orch8 import durable_agent_handler


class Graph:
    async def ainvoke(self, value, config):
        return value, config


def test_langgraph_thread_is_instance_id() -> None:
    result = asyncio.run(
        durable_agent_handler(Graph())({"params": {"text": "hi"}, "instance_id": "inst-1"})
    )
    assert result == ({"text": "hi"}, {"configurable": {"thread_id": "inst-1"}})
