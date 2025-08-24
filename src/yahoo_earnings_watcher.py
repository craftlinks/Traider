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

async def earnings_worker(earnings_event: EarningsEvent) -> None:
    logger.info(f"Earnings event: {earnings_event}")




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

    # Wait for task completion to keep main alive. The task itself runs an
    # infinite loop, so this effectively blocks until cancellation.
    await task


if __name__ == "__main__":
    asyncio.run(main())