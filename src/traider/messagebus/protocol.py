# mypy expects TypeVar from typing, not ast
from __future__ import annotations

from typing import Any, Protocol
import asyncio

class MessageBroker(Protocol):
    
    async def publish(self, channel_name: str, message: Any) -> None:
        ...

    async def subscribe(self, channel_name: str) -> asyncio.Queue[Any]:
        ...

    def unsubscribe(self, channel_name: str, queue: asyncio.Queue[Any]) -> None:
        ...