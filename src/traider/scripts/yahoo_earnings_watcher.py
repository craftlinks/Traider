import asyncio
from datetime import date, datetime
import argparse

import logging

from src.traider.platforms.yahoo.main import EarningsEvent
from traider.interfaces.queue_sink import AsyncQueueSink
from traider.platforms.pollers.yahoo_earnings_poller import YahooEarningsPoller

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments.

    Returns
    -------
    argparse.Namespace
        Parsed arguments with an optional ``date`` attribute.
    """

    parser = argparse.ArgumentParser(
        description="Watch Yahoo earnings calendar for a specific date (default: today) and interval (default: 1 minute)."
    )
    parser.add_argument(
        "--date",
        type=str,
        help="Date in ISO format YYYY-MM-DD to fetch earnings for (defaults to today).",
    )
    parser.add_argument(
        "--interval",
        type=int,
        help="Interval in hours to poll for earnings (default: 1 hour).",
    )
    return parser.parse_args()


# Use an asynchronous queue to avoid blocking the event-loop.
earnings_queue: asyncio.Queue[EarningsEvent] = asyncio.Queue(maxsize=1000)

sink = AsyncQueueSink(earnings_queue)


stop_event = asyncio.Event()

# ---------------------------------------------------------------------------
# Earnings worker (consumer)
# ---------------------------------------------------------------------------


async def earnings_worker() -> None:
    """Background consumer that logs each :class:`EarningsEvent`."""

    while not stop_event.is_set():
        try:
            # Await an item – this *yields* control while the queue is empty and
            # therefore does *not* hog the event-loop.
            earnings_event: EarningsEvent = await earnings_queue.get()

            logger.info(
                "Ticker: %s | Call Time: %s | EPS Estimate: %s | EPS Actual: %s | EPS Surprise: %s | Surprise %%: %s",
                earnings_event.ticker,
                earnings_event.earnings_call_time,
                earnings_event.eps_estimate,
                earnings_event.eps_actual,
                earnings_event.eps_surprise,
                earnings_event.eps_surprise_percent,
            )
        except Exception as exc:
            logger.error("[Worker] Error while processing earnings event: %s", exc)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


async def main() -> None:
    args = parse_args()

    if args.date:
        try:
            poll_date = datetime.strptime(args.date, "%Y-%m-%d").date()
        except ValueError as err:
            raise SystemExit(f"Invalid --date '{args.date}': {err}")
    else:
        poll_date = date.today()

    if args.interval:
        interval = args.interval
    else:
        interval = 60

    poller = YahooEarningsPoller(date=poll_date, interval=interval)
    poller.set_sink(sink)

    # Start the asynchronous polling loop as a background task inside the
    # currently running event-loop instead of spawning an extra thread.
    task = asyncio.create_task(poller.async_polling_loop())  # type: ignore[attr-defined]
    logger.info("Started %s as asyncio task.", poller.get_poller_name())

    earnings_worker_task = asyncio.create_task(earnings_worker())

    # Wait for task completion to keep main alive. The task itself runs an
    # infinite loop, so this effectively blocks until cancellation.
    await task
    await earnings_worker_task


if __name__ == "__main__":
    asyncio.run(main())
