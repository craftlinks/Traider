import asyncio
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from enum import Enum
import inspect
import os
import pprint
import signal
import time
from typing import Any, Awaitable, Callable, Literal, Protocol, TypeVar, overload

from traider.yfinance import EarningsEvent, PressRelease

import logging
logging.basicConfig(level=logging.INFO)

MAX_EVENTS = 5

class Channel(str, Enum):
    EARNINGS = "earnings"
    PRESS_RELEASE = "press_release"


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

    async def publish(self, channel_name: Channel, message: Any) -> None:
        ...

    async def subscribe(self, channel_name: Channel) -> asyncio.Queue[Any]:
        ...

    def unsubscribe(self, channel_name: Channel, queue: asyncio.Queue[Any]) -> None:
        ...


class InMemoryBroker(MessageBroker):
    def __init__(self):
        self.channels = defaultdict(list[asyncio.Queue[Any]])
    
    async def publish(self, channel_name: Channel, message: Any) -> None:
        for queue in self.channels[channel_name]:
            await queue.put(message)
        
    async def subscribe(self, channel_name: Channel) -> asyncio.Queue[Any]:
       queue = asyncio.Queue()
       self.channels[channel_name].append(queue)
       return queue
    
    def unsubscribe(self, channel_name: Channel, queue: asyncio.Queue[Any]) -> None:
        if queue in self.channels[channel_name]:
            self.channels[channel_name].remove(queue)

async def worker_loop(worker_name: str, queue: asyncio.Queue[Any], shutdown_event: asyncio.Event, worker_fn: Callable[[Any], Awaitable[None]]):
    while True:
        try:
            message = await asyncio.wait_for(queue.get(), timeout=1)
            await worker_fn(message)
        except asyncio.TimeoutError:
            # Queue is empty, check if we should shutdown
            if shutdown_event.is_set():
                logging.debug(f"Shutting down {worker_name}...")
                break
            continue

def _cpu_bound_worker_fn(message: EarningsEvent) -> bool:
    pprint.pprint(
        f"worker 2: Also received earnings event {message.id} for company: {message.company_name}"
    )
    time.sleep(2)
    return message.id > MAX_EVENTS


async def cpu_bound_worker_1(
    process_pool: ProcessPoolExecutor,
    broker: MessageBroker,
    shutdown_event: asyncio.Event,
    startup_barrier: asyncio.Barrier,
):
    """Delegates events to the CPU-bound process pool."""
    queue = await broker.subscribe(Channel.EARNINGS)

    await startup_barrier.wait()

    loop = asyncio.get_running_loop()

    async def process_event(event: Any):
        should_unsubscribe = await loop.run_in_executor(process_pool, _cpu_bound_worker_fn, event)
        # Check if shutdown was triggered while we were blocked in the executor
        if shutdown_event.is_set():
            logging.debug(f"[CPU-MANAGER] Shutdown triggered. Discarding result.")
            return

        if should_unsubscribe:
            broker.unsubscribe(Channel.EARNINGS, queue)

    await worker_loop("cpu_bound_worker_1", queue, shutdown_event, process_event)


async def earnings_producer(msg_broker: MessageBroker, shutdown_event: asyncio.Event, startup_barrier: asyncio.Barrier):
    
    await startup_barrier.wait()
    
    earnings_event_id = 1
    
    while not shutdown_event.is_set():
        if earnings_event_id > MAX_EVENTS:
            shutdown_event.set()
            break
        
        # get earnings from database
        earnings_event = EarningsEvent(
            id=earnings_event_id,
            ticker="AAPL",
            company_name="Apple",
            event_name="Earnings",
            time_type="after_hours",
            earnings_call_time=datetime.now(),
            eps_estimate=1.0,
            eps_actual=1.0,
            eps_surprise=1.0,
            eps_surprise_percent=1.0,
            market_cap=1.0,
        )

        await msg_broker.publish(Channel.EARNINGS, earnings_event)

        earnings_event_id += 1
        await asyncio.sleep(1)

    logging.debug("Producer shutting down...")


async def earnings_press_release_poller(company_name: str, ticker: str):
    pass


async def start_earnings_press_release_poller(msg_broker: MessageBroker, shutdown_event: asyncio.Event, startup_barrier: asyncio.Barrier):
    
    queue = await msg_broker.subscribe(Channel.EARNINGS)

    await startup_barrier.wait()
    
    async def worker_fn(message: EarningsEvent):
        pprint.pprint(f"worker 1: Received earnings event for {message.company_name}")
        # start long running process to poll press release
        asyncio.create_task(earnings_press_release_poller(message.company_name, message.ticker))
        if message.id > MAX_EVENTS:
            msg_broker.unsubscribe(Channel.EARNINGS, queue)
            shutdown_event.set()
            return
        await asyncio.sleep(1)
    
    await worker_loop("press_release_worker_1", queue, shutdown_event, worker_fn)


async def main():
    
    msg_broker = InMemoryBroker()
    shutdown_event = asyncio.Event()
    startup_barrier = asyncio.Barrier(parties=3)
    cpu_cores = os.cpu_count() or 1
    
    loop = asyncio.get_running_loop()

    for sig in [signal.SIGINT, signal.SIGTERM]:
        loop.add_signal_handler(sig, shutdown_event.set)

    
    with ProcessPoolExecutor(max_workers=cpu_cores) as process_pool:
        await asyncio.gather(
            earnings_producer(msg_broker, shutdown_event, startup_barrier),
            press_release_producer(msg_broker, shutdown_event, startup_barrier),
            cpu_bound_worker_1(process_pool, msg_broker, shutdown_event, startup_barrier)
        )
    

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.debug("Shutting down...")