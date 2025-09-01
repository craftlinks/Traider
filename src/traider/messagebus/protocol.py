# mypy expects TypeVar from typing, not ast
from __future__ import annotations

from typing import Any, Protocol, Literal, TypeVar
from enum import Enum
import asyncio
from traider.yfinance import EarningsEvent, PressRelease
from typing import overload
from traider.messagebus.channels import Channel

T_msg = TypeVar("T_msg", EarningsEvent,  PressRelease)

class MessageBroker(Protocol):
    
    @overload
    async def publish(self, channel_name: Literal[Channel.EARNINGS], message: EarningsEvent) -> None:
        ...

    @overload
    async def publish(self, channel_name: Literal[Channel.PRESS_RELEASE], message: PressRelease) -> None:
        ...

    @overload
    async def subscribe(self, channel_name: Literal[Channel.EARNINGS]) -> asyncio.Queue[EarningsEvent]:
        ...

    @overload
    async def subscribe(self, channel_name: Literal[Channel.PRESS_RELEASE]) -> asyncio.Queue[PressRelease]:
        ...


    def unsubscribe(self, channel_name: Channel, queue: asyncio.Queue[EarningsEvent | PressRelease]) -> None:
        ...