import asyncio
from datetime import date
from enum import Enum
import hashlib
from traider.messagebus.protocol import MessageBroker
from traider.messagebus.brokers.memory import InMemoryBroker
from traider.messagebus.router import MessageRouter
from typing import Optional, Dict
from dspy.signatures import Signature, InputField, OutputField
from dspy import Predict, LM, configure
import os
from dotenv import load_dotenv

from traider.platforms.cache.in_memory_cache_helper import FixedSizeLRUSet
import traider.yfinance as yf
import logging
from collections import OrderedDict

# SETUP
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
for name in ("httpx", "httpcore", "LiteLLM"):
    logging.getLogger(name).setLevel(logging.ERROR)
load_dotenv()
EARNINGS_PRODUCER_INTERVAL = 10
PRESS_RELEASE_POLL_INTERVAL = 10

# Track active polling tasks per ticker to avoid spawning duplicates
earnings_polling_tasks: Dict[str, asyncio.Task] = {}
# Track tickers for which polling was cancelled because an earnings report was found.
processed_tickers: set[str] = set()
# Single instance used by the monitoring loop
seen_press_release_ids = FixedSizeLRUSet(max_items=10_000)

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
    Judgement if the article is an earnings report and whether it is a BUY or SELL judgement.
    If it is not an earnings report, return `is_earnings_report=False` and `judgement=''` and `judgement_score=0`.
    If it is an earnings report, return a score between -10 (SELL) and 10 (BUY) for the BUY or SELL judgement.
    """

    article_text: str = InputField(desc="The text of the article")
    is_earnings_report: bool = OutputField(desc="Indicate whether the article is an earnings report")
    judgement: Optional[str] = OutputField(desc="Explain the reasoning for your judgement")
    judgement_score: Optional[int] = OutputField(desc="A score between -10 and 10 for the BUY or SELL judgement")

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
        for earning in earnings:
            await router.broker.publish(Channel.EARNINGS, earning)
        await asyncio.sleep(EARNINGS_PRODUCER_INTERVAL)
    


# ---------------------------------------------------------------------------
# Earnings Consumer
# ---------------------------------------------------------------------------

@router.route(listen_to=Channel.EARNINGS)
async def earnings_consumer(router: MessageRouter, earning: yf.EarningsEvent):
    
    async def poll_for_press_release(router: MessageRouter, ticker: str):
        logger.debug(f"Polling for press release for {ticker}")
        while True:
            try:
                press_release: yf.PressRelease | None = await yf.get_latest_press_release(ticker)
                # generate unique id for the press release based on the url and the pub date
                if press_release is not None:
                    id_ = hashlib.sha256(f"{press_release.url}{press_release.pub_date}".encode()).hexdigest()
                    # Skip duplicates using the LRU set (maintains max 10k ids)
                    if seen_press_release_ids.add(id_):
                        # Already seen -> skip
                        logger.debug("Press release %s for %s already processed – skipping", id_, ticker)
                    else:
                        # New ID – publish
                        await router.broker.publish(Channel.PRESS_RELEASE, press_release)
            except Exception:  # noqa: BLE001
                logger.exception("Error while polling press release for %s", ticker)

            # Allow cancellation between polling cycles
            try:
                await asyncio.sleep(PRESS_RELEASE_POLL_INTERVAL)
            except asyncio.CancelledError:
                logger.debug("Polling task for %s cancelled", ticker)
                raise
    
    logger.debug(f"Received earnings event for {earning.ticker} at {earning.earnings_call_time}")
    # If we have already completed polling for this ticker (earnings report found), skip spawning a new task
    if earning.ticker in processed_tickers:
        logger.debug(f"Polling for {earning.ticker} was already completed – skipping new polling task")
        return

    # If we already have an active polling task for this ticker, skip spawning a new one
    existing_task = earnings_polling_tasks.get(earning.ticker)
    if existing_task is not None and not existing_task.done():
        logger.debug(f"Polling task for {earning.ticker} is already running – skipping")
        return
    logger.info(f"Received new earnings event for {earning.ticker} at {earning.earnings_call_time}")
    logger.info(f"Spawning press-release polling task for {earning.ticker}")

    task = router.spawn_task(poll_for_press_release(router, earning.ticker), ttl=3600*24)
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
    logger.debug(f"Saving press release for {press_release.ticker} to db")
    id = await press_release.to_db()
    if id is None:
        logger.warning(f"Failed to save initial press release for {press_release.ticker} to db")
        return 
    press_release.text_content = await yf.get_press_release_content(press_release.url)
    id = await press_release.to_db()
    if id is None:
        logger.warning(f"Failed to save processed press release for {press_release.ticker} to db")
        return
    logger.debug(f"Saved press release for {press_release.ticker} to db with id {id}")
    return press_release

# ---------------------------------------------------------------------------
# Judge Press Release
# ---------------------------------------------------------------------------

@router.route(listen_to=Channel.PRESS_RELEASE_CONTENT)
async def judge_press_release(router: MessageRouter, press_release: yf.PressRelease):
    logger.debug(f"Judging press release for {press_release.ticker}")
    judgement = await press_release_judgement.acall(article_text=press_release.text_content)
    if judgement.is_earnings_report:
        msg = {
            "ticker": press_release.ticker,
            "judgement": judgement,
        }
        # If we have confirmed this press release is an earnings report, we can stop
        # the ongoing polling task for this ticker to avoid unnecessary network calls.
        task = earnings_polling_tasks.get(press_release.ticker)
        if task is not None and not task.done():
            logger.info("Cancelling polling task for %s – earnings report found", press_release.ticker)
            processed_tickers.add(press_release.ticker)
            task.cancel()
        await router.broker.publish(Channel.LLM_EARNINGS_REPORT, msg)
    else:
        logger.debug(f"Press release for {press_release.ticker} is not an earnings report")

# ---------------------------------------------------------------------------
# Publish Earnings Report
# ---------------------------------------------------------------------------

@router.route(listen_to=Channel.LLM_EARNINGS_REPORT)
async def publish_earnings_report(router: MessageRouter, judgement: dict):
    logger.debug(f"Publishing earnings report for {judgement['ticker']}")
    if judgement['judgement'].is_earnings_report:
        logger.info(f"\n{judgement['judgement'].judgement}")
        logger.info(f"{judgement['ticker']} SELL/BUY SCORE: {judgement['judgement'].judgement_score}")
    else:
        logger.debug(f"Press release for {judgement['ticker']} is not an earnings report")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main():
    async with asyncio.TaskGroup() as tg:
        tg.create_task(router.run())

if __name__ == "__main__":
    asyncio.run(main())