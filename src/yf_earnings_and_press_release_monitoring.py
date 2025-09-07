import asyncio
from datetime import date
from enum import Enum
from typing import Dict, Optional, Final
import httpx
from traider.messagebus.protocol import MessageBroker
from traider.messagebus.brokers.memory import InMemoryBroker
from traider.messagebus.router import MessageRouter
from dspy.signatures import Signature, InputField, OutputField
from dspy import Predict, LM, configure
import os
from dotenv import load_dotenv
import logging
from logging.handlers import RotatingFileHandler

# Shared Discord client instance (initialized lazily)
_discord_client: httpx.AsyncClient | None = None

# Track tickers for which polling was cancelled because an earnings report was found.
processed_tickers: set[str] = set()
# Store last processed press‐release ID per ticker
last_press_release_id: Dict[str, str] = {}
import traider.yfinance as yf

# SETUP
# ----- Replace default logging with console, file and Discord handlers -----
load_dotenv()  # make sure environment variables are loaded

LOG_FORMAT: Final = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"

class DiscordWebhookHandler(logging.Handler):
    """Send log records to a Discord channel via webhook."""
    def __init__(self, webhook_url: str, level: int = logging.ERROR) -> None:
        super().__init__(level)
        self.webhook_url = webhook_url
        self.client = httpx.Client(timeout=5.0)

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            self.client.post(self.webhook_url, json={"content": msg})
        except Exception:
            # Ensure logging errors do not crash the application
            self.handleError(record)

root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)

# Console handler
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter(LOG_FORMAT))
root_logger.addHandler(console_handler)

# Rotating file handler
file_handler = RotatingFileHandler("monitoring.log", maxBytes=2_000_000, backupCount=3, encoding="utf-8")
file_handler.setFormatter(logging.Formatter(LOG_FORMAT))
root_logger.addHandler(file_handler)

# Discord handler (only if webhook URL is present)
discord_webhook_url = os.getenv("DISCORD_LOGS_WEBHOOK_URL")
if discord_webhook_url:
    discord_handler = DiscordWebhookHandler(discord_webhook_url, level=logging.WARNING)
    discord_handler.setFormatter(logging.Formatter(LOG_FORMAT))
    root_logger.addHandler(discord_handler)

logger = logging.getLogger(__name__)

# Silence noisy third-party libraries
for name in ("httpx", "httpcore", "LiteLLM"):
    logging.getLogger(name).setLevel(logging.ERROR)

EARNINGS_PRODUCER_INTERVAL = 60*60
PRESS_RELEASE_POLL_INTERVAL = 10
# ---------------------------------------------------------------------------
# Concurrency limits for outbound Yahoo Finance requests
# ---------------------------------------------------------------------------

# Maximum number of concurrent requests we allow to Yahoo Finance.  Adjust as
# needed depending on observed rate-limits / latency.
MAX_CONCURRENT_YF_REQUESTS: Final = 5

# A shared semaphore that guards all outbound Yahoo requests so that at most
# *MAX_CONCURRENT_YF_REQUESTS* run in parallel.  It is created once at import
# time and reused by all polling tasks.
_yf_request_semaphore: asyncio.Semaphore = asyncio.Semaphore(MAX_CONCURRENT_YF_REQUESTS)

# Small random jitter helper to avoid large synchronous request waves.  Jitter
# is a fraction of the base poll interval so overall cadence remains similar.
from random import random as _rand


def _jitter(base: float) -> float:
    """Return a random float in the range [0, base/4)."""

    return _rand() * base / 4


# Track active polling tasks per ticker to avoid spawning duplicates
earnings_polling_tasks: Dict[str, asyncio.Task] = {}

# ---------------------------------------------------------------------------
# Channel definitions
# ---------------------------------------------------------------------------


class Channel(str, Enum):
    EARNINGS = "earnings"
    PRESS_RELEASE = "press_release"
    PRESS_RELEASE_CONTENT = "press_release_content"
    LLM_EARNINGS_REPORT = "llm_earnings_report"


# ---------------------------------------------------------------------------
# Message Bus Setup
# ---------------------------------------------------------------------------

msg_broker: MessageBroker = InMemoryBroker()
router = MessageRouter(msg_broker)

# ---------------------------------------------------------------------------
# LLM Setup
# ---------------------------------------------------------------------------

openai_api_key = os.getenv("OPENAI_API_KEY")
lm = LM(model="gpt-4.1-mini", api_key=openai_api_key)
configure(lm=lm)


class PressReleaseJudgement(Signature):
    """
    Judge whether the article is an earnings report and whether it is a BUY or SELL.
    If it is not an earnings report, return `is_earnings_report=False` and `judgement=''` and `judgement_score=0`.
    If it is an earnings report, return a score between -10 (SELL) and 10 (BUY) for the BUY or SELL judgement.
    """

    article_text: str = InputField(desc="The text of the article")
    is_earnings_report: bool = OutputField(
        desc="Indicate whether the article is an earnings report"
    )
    judgement: Optional[str] = OutputField(
        desc="Explain the reasoning for your judgement"
    )
    judgement_score: Optional[int] = OutputField(
        desc="A score between -10 and 10 for the BUY or SELL judgement"
    )


press_release_judgement = Predict(PressReleaseJudgement)

# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Earnings Producer
# ---------------------------------------------------------------------------


@router.route(publish_to=Channel.EARNINGS)
async def earnings_producer(router: MessageRouter):
    await router.wait_until_ready()
    while True:
        logger.info(f"Producing earnings events for {date.today()} ...")
        earnings: list[yf.EarningsEvent] = await yf.get_earnings(date.today())
        if earnings:
            await asyncio.gather(
                *(router.broker.publish(Channel.EARNINGS, e) for e in earnings)
            )
        await asyncio.sleep(EARNINGS_PRODUCER_INTERVAL)


# ---------------------------------------------------------------------------
# Earnings Consumer
# ---------------------------------------------------------------------------
async def poll_for_press_release(router: MessageRouter, ticker: str):
    logger.debug(f"Polling for press release for {ticker}")
    while True:
        try:
            # Limit the number of concurrent outbound requests using the shared
            # semaphore.  The `async with` block ensures that we acquire a
            # "slot" before performing the HTTP request and release it
            # immediately afterwards, regardless of success or failure.
            async with _yf_request_semaphore:
                press_release: yf.PressRelease | None = (
                    await yf.get_latest_press_release(ticker)
                )
            # Compose a deterministic *key* from URL + publication date (hashing not necessary)
            if press_release is not None:
                id_ = f"{press_release.url}|{press_release.pub_date}"
                # Skip duplicates using the LRU set (maintains max 10k ids)
                if last_press_release_id.get(ticker) == id_:
                    # Already seen -> skip
                    logger.debug(
                        "Press release %s for %s already processed – skipping",
                        id_,
                        ticker,
                    )
                else:
                    # New ID – publish
                    await router.broker.publish(Channel.PRESS_RELEASE, press_release)
                    last_press_release_id[ticker] = id_
        except Exception:
            logger.exception("Error while polling press release for %s", ticker)

        # Allow cancellation between polling cycles
        try:
            # Add a small random jitter so that many polling tasks started at
            # the same time do not keep hitting Yahoo in perfect lock-step.
            await asyncio.sleep(PRESS_RELEASE_POLL_INTERVAL + _jitter(PRESS_RELEASE_POLL_INTERVAL))
        except asyncio.CancelledError:
            logger.debug("Polling task for %s cancelled", ticker)
            raise


@router.route(listen_to=Channel.EARNINGS)
async def earnings_consumer(router: MessageRouter, earning: yf.EarningsEvent):
    logger.debug(
        f"Received earnings event for {earning.ticker} at {earning.earnings_call_time}"
    )
    # If we have already completed polling for this ticker (earnings report found), skip spawning a new task
    if earning.ticker in processed_tickers:
        logger.debug(
            f"Polling for {earning.ticker} was already completed – skipping new polling task"
        )
        return

    # If we already have an active polling task for this ticker, skip spawning a new one
    existing_task = earnings_polling_tasks.get(earning.ticker)
    if existing_task is not None and not existing_task.done():
        logger.debug(f"Polling task for {earning.ticker} is already running – skipping")
        return
    logger.info(
        f"Received new earnings event for {earning.ticker} at {earning.earnings_call_time}"
    )
    logger.info(f"Spawning press-release polling task for {earning.ticker}")

    task = router.spawn_task(
        poll_for_press_release(router, earning.ticker), ttl=3600 * 24
    )
    earnings_polling_tasks[earning.ticker] = task

    # Remove task from the registry once it completes or is cancelled so that
    # future earnings events can start a fresh polling cycle if needed.
    def _cleanup(_task: asyncio.Task, *, ticker: str = earning.ticker):
        earnings_polling_tasks.pop(ticker, None)

    task.add_done_callback(_cleanup)


# ---------------------------------------------------------------------------
# Save Earnings to DB
# ---------------------------------------------------------------------------


@router.route(listen_to=Channel.EARNINGS)
async def save_earnings_to_db(router: MessageRouter, earning: yf.EarningsEvent):
    logger.debug(f"Saving earnings event for {earning.ticker} to db")
    id = await earning.to_db()
    if id is None:
        logger.warning(f"Failed to save earnings event for {earning.ticker} to db")
        return
    logger.debug(f"Saved earnings event for {earning.ticker} to db with id {id}")


# ---------------------------------------------------------------------------
# Process Press Release
# ---------------------------------------------------------------------------


@router.route(listen_to=Channel.PRESS_RELEASE, publish_to=Channel.PRESS_RELEASE_CONTENT)
async def process_press_release(router: MessageRouter, press_release: yf.PressRelease):
    logger.info(f"Processing press release for {press_release.ticker}")
    press_release.text_content = await yf.get_press_release_content(press_release.url)

    id = await press_release.to_db()
    if id is None:
        logger.warning(f"Failed to save press release for {press_release.ticker} to db")
        return
    logger.debug(f"Saved press release for {press_release.ticker} to db with id {id}")
    return press_release


# ---------------------------------------------------------------------------
# Judge Press Release
# ---------------------------------------------------------------------------


@router.route(listen_to=Channel.PRESS_RELEASE_CONTENT)
async def judge_press_release(router: MessageRouter, press_release: yf.PressRelease):
    logger.debug(f"Judging press release for {press_release.ticker}")
    judgement = await press_release_judgement.acall(
        article_text=press_release.text_content
    )
    if judgement.is_earnings_report:
        msg = {
            "ticker": press_release.ticker,
            "judgement": judgement,
        }
        # If we have confirmed this press release is an earnings report, we can stop
        # the ongoing polling task for this ticker to avoid unnecessary network calls.
        task = earnings_polling_tasks.get(press_release.ticker)
        if task is not None and not task.done():
            logger.info(
                "Cancelling polling task for %s – earnings report found",
                press_release.ticker,
            )
            processed_tickers.add(press_release.ticker)
            task.cancel()
        await router.broker.publish(Channel.LLM_EARNINGS_REPORT, msg)
    else:
        logger.debug(
            f"Press release for {press_release.ticker} is not an earnings report"
        )


# ---------------------------------------------------------------------------
# Publish Earnings Report
# ---------------------------------------------------------------------------


async def send_discord_message(message: str):
    """Send *message* to Discord via webhook.

    A single httpx.AsyncClient is reused for efficiency.  If the webhook URL is
    missing, the function becomes a no-op and simply logs the event so the
    caller does not need to perform additional checks.
    """

    # Retrieve the webhook URL only once at import time.
    webhook_url: str | None = os.getenv("DISCORD_WEBHOOK_URL")

    if not webhook_url:
        logger.debug("Discord webhook URL not set – skipping message: %s", message)
        return

    # Lazily create (and reuse) a shared AsyncClient.
    global _discord_client
    if _discord_client is None:
        _discord_client = httpx.AsyncClient(timeout=10.0)

    try:
        response = await _discord_client.post(webhook_url, json={"content": message})
        response.raise_for_status()
    except Exception:
        # Log once; do not raise so that callers are unaffected by Discord issues.
        logger.exception("Failed to send Discord message")


@router.route(listen_to=Channel.LLM_EARNINGS_REPORT)
async def publish_earnings_report(router: MessageRouter, judgement: dict):
    logger.debug(f"Publishing earnings report for {judgement['ticker']}")
    if judgement["judgement"].is_earnings_report:
        logger.info(f"\n{judgement['judgement'].judgement}")
        logger.info(
            f"{judgement['ticker']} SELL/BUY SCORE: {judgement['judgement'].judgement_score}"
        )
        message = f"{judgement['ticker']} SELL/BUY SCORE: {judgement['judgement'].judgement_score}\n{judgement['judgement'].judgement}"
        await send_discord_message(message)
    else:
        logger.debug(
            f"Press release for {judgement['ticker']} is not an earnings report"
        )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


async def main():
    async with asyncio.TaskGroup() as tg:
        tg.create_task(router.run())


if __name__ == "__main__":
    asyncio.run(main())
