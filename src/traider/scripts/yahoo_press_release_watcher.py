import asyncio
from datetime import date, datetime
import argparse

import logging

from traider.interfaces.queue_sink import AsyncQueueSink
from traider.platforms.pollers.yahoo_earnings_poller import YahooEarningsPoller
from traider.platforms.pollers.yahoo_press_release_poller import YahooPressReleasePoller
from traider.platforms.yahoo.main import PressRelease, YahooFinance

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

yf = YahooFinance()



def parse_args() -> argparse.Namespace:
    """Parse command-line arguments.

    Returns
    -------
    argparse.Namespace
        Parsed arguments with an optional ``date`` attribute.
    """

    parser = argparse.ArgumentParser(
        description="Watch Yahoo press releases for a specific tickers and interval (default: 1 minute)."
    )
    parser.add_argument(
        "-t",
        "--tickers",
        nargs="+",
        dest="tickers",
        help="One or more tickers to fetch press releases for (e.g., AAPL MSFT TSLA).",
    )
    parser.add_argument(
        "--interval",
        type=int,
        help="Interval in seconds to poll for press releases (default: 1 second).",
    )
    return parser.parse_args()


# Use an asynchronous queue to avoid blocking the event-loop.
press_release_queue: asyncio.Queue[PressRelease] = asyncio.Queue(maxsize=1000)

sink = AsyncQueueSink(press_release_queue)


stop_event = asyncio.Event()

# ---------------------------------------------------------------------------
# Earnings worker (consumer)
# ---------------------------------------------------------------------------

async def press_release_worker() -> None:
    """Background consumer that logs each :class:`PressRelease`."""

    while not stop_event.is_set():
        try:
            # Await an item – this *yields* control while the queue is empty and
            # therefore does *not* hog the event-loop.
            press_release: PressRelease = await press_release_queue.get()

            # TODO: if not an earnings report, skip

            logger.info(
                "Ticker: %s | Title: %s | URL: %s | Type: %s | Pub Date: %s | Display Time: %s | Company Name: %s",
                press_release.ticker,
                press_release.title,
                press_release.url,
                press_release.type,
                press_release.pub_date,
                press_release.display_time,
                press_release.company_name,
            )

            press_release.raw_html = yf.get_press_release_content(press_release.url)

            yf.save_press_release_to_db(press_release)


        except Exception as exc:
            logger.error("[Worker] Error while processing press release: %s", exc)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main() -> None:
    args = parse_args()


    # Resolve list of tickers – default to a representative set when none provided.
    tickers: list[str] = args.tickers if args.tickers else [
        "AAPL",
        "MSFT",
        "GOOG",
        "AMZN",
        "TSLA",
    ]

    if args.interval:
        interval = args.interval
    else:
        interval = 1

    poller = YahooPressReleasePoller(tickers=tickers, interval=interval)
    poller.set_sink(sink)



    # Start the asynchronous polling loop as a background task inside the
    # currently running event-loop instead of spawning an extra thread.
    task = asyncio.create_task(poller.async_polling_loop())  # type: ignore[attr-defined]
    logger.info("Started %s as asyncio task.", poller.get_poller_name())

    press_release_worker_task = asyncio.create_task(press_release_worker())

    # Wait for task completion to keep main alive. The task itself runs an
    # infinite loop, so this effectively blocks until cancellation.
    await task
    await press_release_worker_task

if __name__ == "__main__":
    asyncio.run(main())