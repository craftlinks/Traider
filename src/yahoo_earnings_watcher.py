import asyncio
from dataclasses import dataclass
from datetime import date, datetime
import argparse
import queue

from dotenv.main import logger

from src.traider.platforms.yahoo.main import EarningsEvent
from traider.interfaces.queue_sink import QueueSink
from traider.platforms.pollers.yahoo_earnings_poller import YahooEarningsPoller


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments.

    Returns
    -------
    argparse.Namespace
        Parsed arguments with an optional ``date`` attribute.
    """

    parser = argparse.ArgumentParser(
        description="Watch Yahoo earnings calendar for a specific date (default: today)."
    )
    parser.add_argument(
        "--date",
        type=str,
        help="Date in ISO format YYYY-MM-DD to fetch earnings for (defaults to today).",
    )

    return parser.parse_args()


earnings_queue: queue.Queue[EarningsEvent] = queue.Queue(maxsize=1000)
sink = QueueSink(earnings_queue)


stop_event = asyncio.Event()

async def earnings_worker() -> None:
    while not stop_event.is_set():
        try:
            earnings_event: EarningsEvent = earnings_queue.get_nowait()
        except queue.Empty:
            continue
        except Exception as e:
            logger.error(f"Error getting earnings event: {e}")
            continue

        try:
            logger.info(f"Earnings event: {earnings_event}")
        except Exception as e:
            logger.error(f"Error logging earnings event: {e}")
            continue





async def main() -> None:
    args = parse_args()

    if args.date:
        try:
            poll_date = datetime.strptime(args.date, "%Y-%m-%d").date()
        except ValueError as err:
            raise SystemExit(f"Invalid --date '{args.date}': {err}")
    else:
        poll_date = date.today()

    poller = YahooEarningsPoller(date=poll_date)
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

# TODO Geert fix the queue sink!