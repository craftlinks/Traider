import asyncio
# ---------------------------------------------------------------------------
# Standard library imports
# ---------------------------------------------------------------------------
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
import os
import pprint
import signal
import time
from typing import Any, Optional

from traider.yfinance import EarningsEvent
# Use the shared message-bus infrastructure
from traider.messagebus.channels import Channel
from traider.messagebus.protocol import MessageBroker
from traider.messagebus.brokers.memory import InMemoryBroker
from traider.messagebus.router import MessageRouter

import logging
logging.basicConfig(level=logging.INFO)

MAX_EVENTS = 5

def _cpu_bound_worker_fn(message: EarningsEvent) -> None:
    pprint.pprint(
        f"worker 2: Also received earnings event {message.id} for company: {message.company_name}"
    )
    time.sleep(2)
    return None


# ---------------------------------------------------------------------------
# Message-bus router and worker definitions
# ---------------------------------------------------------------------------


msg_broker: MessageBroker = InMemoryBroker()
router = MessageRouter(msg_broker)

# A global process pool for CPU-bound work – will be initialised in main().
process_pool_global: ProcessPoolExecutor | None = None


@router.route(listen_to=Channel.EARNINGS)
async def press_release_worker(event: EarningsEvent, shutdown_event: asyncio.Event) -> EarningsEvent | None:
    """Kick off a poller to retrieve the press-release that follows *event*."""

    pprint.pprint(f"[press_release_worker] Triggering poller for {event.company_name}")
    # simulate background task
    asyncio.create_task(earnings_press_release_poller(event.company_name, event.ticker, shutdown_event))

    # Example of reacting to global shutdown
    if shutdown_event.is_set():
        return None
    return None


@router.route(listen_to=Channel.EARNINGS)
async def cpu_heavy_worker(event: EarningsEvent, shutdown_event: asyncio.Event) -> EarningsEvent | None:
    """Handle CPU-bound work in a separate process pool."""

    assert process_pool_global is not None  # Should be initialised in *main*.
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(process_pool_global, _cpu_bound_worker_fn, event)
    if shutdown_event.is_set():
        return None
    return event


@router.route(publish_to=Channel.EARNINGS)
async def earnings_producer(broker: MessageBroker, shutdown_event: asyncio.Event, startup_barrier: asyncio.Barrier):
    """Generate dummy earnings events until MAX_EVENTS then trigger shutdown."""

    await startup_barrier.wait()  # Ensure all subscribers are ready

    for event_id in range(1, MAX_EVENTS + 1):
        if shutdown_event.is_set():
            break

        earnings_event = EarningsEvent(
            id=event_id,
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

        await broker.publish(Channel.EARNINGS, earnings_event)
        await asyncio.sleep(1)

    shutdown_event.set()
    logging.debug("Producer shutting down...")


async def earnings_press_release_poller(company_name: str, ticker: str, shutdown_event: asyncio.Event):
    pass


async def main() -> None:

    global process_pool_global

    shutdown_event = asyncio.Event()

    # Handle graceful shutdown via SIGINT / SIGTERM.
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, shutdown_event.set)

    cpu_cores = os.cpu_count() or 1
    process_pool_global = ProcessPoolExecutor(max_workers=cpu_cores)


    # Barrier must cover subscribers + producers
    worker_count = router.node_count
    startup_barrier = asyncio.Barrier(parties=worker_count)

    async with asyncio.TaskGroup() as tg:
        tg.create_task(router.run(shutdown_event, startup_barrier))

    # Clean-up once all tasks are done.
    process_pool_global.shutdown(wait=True)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.debug("Shutting down...")