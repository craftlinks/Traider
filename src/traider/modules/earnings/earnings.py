import asyncio
from datetime import date, timedelta
from crawlee import Request
from modules.earnings.crawlee import crawler
from traider.platforms.yahoo.main import YahooFinance
from traider.db.database import get_db_connection, create_tables
from .helpers import (
    fetch_urls_from_db,
    get_earnings_for_date_range,
    get_earnings_tickers_for_date_range,
    save_earnings_data,
    save_profile_to_db,
)
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("earnings_collection.log")],
)
for name in (
    "LiteLLM",
    "openai",
    "httpx",
    "dspy",
    "crawlee.crawlers._playwright._playwright_crawler",
):
    logging.getLogger(name).setLevel(logging.ERROR)

# Optional: stop them from bubbling to the root logger if root is INFO
logging.getLogger("LiteLLM").propagate = False
logging.getLogger("openai").propagate = False
logging.getLogger("httpx").propagate = False
logging.getLogger("dspy").propagate = False


# Suppress noisy Crawlee 403 SessionError stack traces specifically
class SuppressCrawlee403Filter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:  # type: ignore[override]
        message = record.getMessage()
        if "Assuming the session is blocked based on HTTP status code 403" in message:
            # Try to extract URL from message like: "Request to https://... failed ..."
            url_snippet = None
            try:
                if "Request to " in message:
                    start = message.find("Request to ") + len("Request to ")
                    end = message.find(" failed", start)
                    if end == -1:
                        end = None
                    url_snippet = message[start:end].strip()
            except Exception:
                url_snippet = None

            if url_snippet:
                logger.info(
                    "Crawler blocked (403) - suppressed stacktrace: %s", url_snippet
                )
            else:
                logger.info("Crawler blocked (403) - suppressed stacktrace")
            return False
        # Also check exception info text if present
        if record.exc_info and record.exc_info[1]:
            try:
                exc_text = str(record.exc_info[1])
                if "HTTP status code 403" in exc_text and "SessionError" in exc_text:
                    logger.info("Crawler blocked (403) - suppressed stacktrace")
                    return False
            except Exception:
                pass

        # Suppress noisy retries/unknown-host errors and replace with concise log
        is_retries_msg = "failed and reached maximum retries" in message
        contains_unknown_host = "NS_ERROR_UNKNOWN_HOST" in message or (
            record.exc_info
            and record.exc_info[1]
            and "NS_ERROR_UNKNOWN_HOST" in str(record.exc_info[1])
        )
        if is_retries_msg or contains_unknown_host:
            # Extract URL if present in message pattern: "Request to <url> failed ..."
            url_snippet = None
            try:
                if "Request to " in message:
                    start = message.find("Request to ") + len("Request to ")
                    end = message.find(" failed", start)
                    if end == -1:
                        end = None
                    url_snippet = message[start:end].strip()
            except Exception:
                url_snippet = None

            reason = "unknown host" if contains_unknown_host else "retries exceeded"
            if url_snippet:
                logger.info(
                    "Crawler navigation issue (%s) - suppressed stacktrace: %s",
                    reason,
                    url_snippet,
                )
            else:
                logger.info(
                    "Crawler navigation issue (%s) - suppressed stacktrace", reason
                )
            return False
        return True


# Attach the filter to Crawlee loggers that emit the message
_crawlee_playwright_logger = logging.getLogger(
    "crawlee.crawlers._playwright._playwright_crawler"
)
_crawlee_playwright_logger.addFilter(SuppressCrawlee403Filter())
_crawlee_basic_logger = logging.getLogger("crawlee.crawlers._basic._basic_crawler")
_crawlee_basic_logger.addFilter(SuppressCrawlee403Filter())


async def earnings_main():
    logger.info("Initializing database…")
    create_tables()

    # Run crawler to get press releases urls, standby mode
    crawler_task = asyncio.create_task(crawler.run())

    # ---- Step 1: Get earnings data and save to database ----

    today = date.today()
    yesterday = today - timedelta(days=1)
    tomorrow = today + timedelta(days=1)

    yf = YahooFinance()
    all_earnings_df = get_earnings_for_date_range(yesterday, tomorrow, yf)

    if all_earnings_df.empty:
        return

    try:
        with get_db_connection() as db_conn:
            save_earnings_data(all_earnings_df, db_conn)
    except Exception as e:
        logger.error(f"Failed to save earnings data to database: {e}")
        raise

    # ---- Step 2: Retrieve Press Releases urls for companies with earnings data ----
    with get_db_connection() as db_conn:
        tickers = get_earnings_tickers_for_date_range(db_conn, tomorrow, tomorrow)
        logger.info(f"Found {len(tickers)} tickers with earnings data for today")
        logger.info(tickers)

    if len(tickers) == 0:
        logger.info("No tickers with earnings data for today")
        return

    # Fetch and store missing press releases urls and homepage urls
    # TODO Geert: cleanup this code
    with get_db_connection() as db_conn:
        for ticker in tickers:
            press_releases_urls = fetch_urls_from_db(db_conn, [ticker], "press-release")

            if not press_releases_urls:
                logger.info(
                    f"No press releases urls found in our database for {ticker}, fetching homepage url"
                )
                homepage_urls = fetch_urls_from_db(db_conn, [ticker], "website")
                if homepage_urls:
                    logger.info(
                        f"Homepage urls found in our databasefor {ticker}: {homepage_urls}"
                    )
                else:
                    logger.info(
                        f"No homepage urls found in our database for {ticker}, fetching from Yahoo Finance"
                    )
                    profile = yf.get_profile(ticker)
                    if profile:
                        save_profile_to_db(ticker, profile, db_conn)
                        homepage_urls = profile.website_url
                        if not homepage_urls:
                            logger.error(
                                f"No homepage url found for {ticker} in Yahoo Finance, skipping"
                            )
                            continue
                        else:
                            logger.info(
                                f"Homepage url found for {ticker} in Yahoo Finance: {homepage_urls}"
                            )
                    else:
                        logger.error(
                            f"No profile found for {ticker} in Yahoo Finance, skipping"
                        )
                        continue
                try:
                    request = Request.from_url(
                        homepage_urls[0],
                        label="press_releases",
                        user_data={"ticker": ticker},
                    )
                    await crawler.add_requests([request])
                    logger.info(
                        f"Added homepage url for {ticker} to crawler for searching press releases urls"
                    )
                except Exception as e:
                    logger.error(f"Crawler execution failed: {e}")
                    continue
            else:
                logger.info(
                    f"Press releases urls found in our database for {ticker}: {press_releases_urls}: GO POLLING!!"
                )

    await asyncio.sleep(20)
    # When current_concurrency is == 0 the crawler should stop
    while crawler._autoscaled_pool.current_concurrency > 0:
        await asyncio.sleep(10)
    crawler.stop()
    await crawler_task
    logger.info("Crawler task completed")

    # with get_db_connection() as db_conn:
    #     for ticker in tickers:
    #         press_releases_urls = fetch_urls_from_db(db_conn, [ticker], "press-release")
    #         if not press_releases_urls:
    #             logger.warning(f"No press releases urls found in our database for {ticker}, skipping")
    #             continue
    #         else:
    #             logger.info(f"Press releases urls found in our database for {ticker}: {press_releases_urls}")

    # TODO Geert: poll the press releases urls for earnings now


if __name__ == "__main__":
    asyncio.run(earnings_main())
