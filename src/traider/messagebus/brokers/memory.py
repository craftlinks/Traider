from __future__ import annotations
from typing import Any
from collections import defaultdict
import asyncio
from traider.messagebus.protocol import MessageBroker


class InMemoryBroker(MessageBroker):
    def __init__(self):
        self.channels: dict[str, list[asyncio.Queue[Any]]] = defaultdict(
            list[asyncio.Queue[Any]]
        )

    async def publish(self, channel_name: str, message: Any) -> None:
        for queue in self.channels.get(channel_name, []):
            await queue.put(message)

    async def subscribe(self, channel_name: str) -> asyncio.Queue[Any]:
        queue = asyncio.Queue()
        self.channels[channel_name].append(queue)
        return queue

    def unsubscribe(self, channel_name: str, queue: asyncio.Queue[Any]) -> None:
        if queue in self.channels.get(channel_name, []):
            self.channels[channel_name].remove(queue)
