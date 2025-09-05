#!/usr/bin/env python3
"""
Script to collect ticker symbols from companies with scheduled earnings releases for today,
then crawl their company websites to collect all URLs and save them to individual files.

This script combines functionality from:
- yahoo_earnings_calendar.py: to fetch today's earnings data
- discover_pr_urls.py: to crawl company websites and extract URLs
"""

import argparse
import asyncio
import logging
from datetime import date
from pathlib import Path
from typing import Iterator, Optional, List, override
from collections import defaultdict
from urllib.parse import urlparse, ParseResult

from crawlee.browsers import (
    BrowserPool,
    PlaywrightBrowserController,
    PlaywrightBrowserPlugin,
)
from crawlee import ConcurrencySettings, Request
import tldextract
from crawlee.crawlers import PlaywrightCrawler, PlaywrightCrawlingContext
from yarl import URL

# Import existing modules

from traider.db.database import get_db_connection, create_tables

from camoufox import AsyncNewBrowser


logger = logging.getLogger(__name__)

# Default output directory for company URL files
OUTPUT_DIR = Path(__file__).resolve().parents[3] / "storage" / "company_urls"
OUTPUT_DIR.mkdir(exist_ok=True)

# Keywords to filter URLs (investor relations, news, press releases, etc.)
DEFAULT_KEYWORDS = [
    "invest",
    "event",
    "news",
    "filing",
    "press-release",
    "press release",
]

# Locks to serialize per-ticker file writes when crawling concurrently
_ticker_file_locks: dict[str, asyncio.Lock] = {}


def _get_ticker_lock(ticker: str) -> asyncio.Lock:
    """Return a process-wide asyncio.Lock for the given ticker."""
    # setdefault ensures only one Lock per ticker
    lock = _ticker_file_locks.setdefault(ticker, asyncio.Lock())
    return lock


class CamoufoxPlugin(PlaywrightBrowserPlugin):
    """Example browser plugin that uses Camoufox browser,
    but otherwise keeps the functionality of PlaywrightBrowserPlugin.
    """

    @override
    async def new_browser(self) -> PlaywrightBrowserController:
        if not self._playwright:
            raise RuntimeError("Playwright browser plugin is not initialized.")

        return PlaywrightBrowserController(
            browser=await AsyncNewBrowser(
                self._playwright, **self._browser_launch_options
            ),
            # Increase, if camoufox can handle it in your use case.
            max_open_pages_per_browser=1,
            # This turns off the crawlee header_generation. Camoufox has its own.
            header_generator=None,
        )


def get_company_website_from_db(ticker: str) -> Optional[str]:
    """Get company website URL from database for a given ticker."""
    logger.debug(f"Fetching website for ticker: {ticker}")

    try:
        with get_db_connection() as conn:
            cursor = conn.execute(
                "SELECT url FROM urls WHERE company_ticker = ? AND url_type = 'website'",
                (ticker.upper(),),
            )
            url_row = cursor.fetchone()
            website_url = url_row["url"] if url_row else None

            if website_url:
                logger.info(f"Found website for {ticker}: {website_url}")
            else:
                logger.warning(f"No website found in database for ticker: {ticker}")

            return website_url
    except Exception as e:
        logger.error(f"Error fetching website for {ticker}: {e}")
        return None


def _check_enqueue_strategy(
    strategy: str,
    *,
    target_url: ParseResult,
    origin_url: ParseResult,
) -> bool:
    """Check if a URL matches the enqueue_strategy."""
    if strategy == "all":
        return True

    if origin_url.hostname is None or target_url.hostname is None:
        logger.warning(
            f"Skipping enqueue: Missing hostname in origin_url = {origin_url.geturl()} or "
            f"target_url = {target_url.geturl()}"
        )
        return False

    if strategy == "same-hostname":
        return target_url.hostname == origin_url.hostname

    if strategy == "same-domain":
        origin_domain = tldextract.extract(origin_url.hostname).domain
        target_domain = tldextract.extract(target_url.hostname).domain
        return origin_domain == target_domain

    if strategy == "same-origin":
        return (
            target_url.hostname == origin_url.hostname
            and target_url.scheme == origin_url.scheme
            and target_url.port == origin_url.port
        )

    return False


def _filter_links_iterator(
    request_iterator: Iterator[str],
    origin_url: str,
    enqueue_strategy: str,
    limit: int | None = None,
    keywords: Optional[list[str]] = None,
) -> Iterator[str]:
    """Filter requests based on the enqueue strategy and URL patterns."""
    parsed_origin_url = urlparse(origin_url)

    if enqueue_strategy == "all" and not parsed_origin_url.hostname:
        logger.warning(
            f"Skipping enqueue: Missing hostname in origin_url = {origin_url}."
        )
        return

    # Emit a `warning` message to the log, only once per call
    warning_flag = True

    for url in request_iterator:
        target_url = url
        parsed_target_url = urlparse(target_url)

        if (
            warning_flag
            and enqueue_strategy != "all"
            and not parsed_target_url.hostname
        ):
            logger.warning(
                f"Skipping enqueue url: Missing hostname in target_url = {target_url}."
            )
            warning_flag = False

        if _check_enqueue_strategy(
            enqueue_strategy, target_url=parsed_target_url, origin_url=parsed_origin_url
        ):
            # Check if URL (excluding scheme) contains any of the keywords (case-insensitive)
            # This includes both domain and path
            url_without_scheme = (
                target_url.lower().replace("https://", "").replace("http://", "")
            )
            if keywords:
                has_keyword = any(
                    keyword.lower() in url_without_scheme for keyword in keywords
                )
            else:
                has_keyword = True

            if has_keyword:
                logger.debug(f"URL contains keyword: {target_url}")
                yield url

                limit = limit - 1 if limit is not None else None
                if limit and limit <= 0:
                    break
            else:
                logger.debug(f"URL does not contain keywords, skipping: {target_url}")


async def extract_company_urls(
    context: PlaywrightCrawlingContext,
    selector: str = "a",
    keywords: Optional[list[str]] = None,
) -> list[str]:
    """Extract URLs from a company webpage."""
    elements = await context.page.query_selector_all(selector)

    links_iterator: Iterator[str] = iter(
        [
            url
            for element in elements
            if (url := await element.get_attribute("href")) is not None
        ]
    )

    links_iterator = to_absolute_url_iterator(
        context.request.loaded_url or context.request.url, links_iterator
    )

    filtered_links_iterator = _filter_links_iterator(
        links_iterator, context.request.url, "same-domain", keywords=keywords
    )

    return list(set(filtered_links_iterator))


async def crawl_company_website(
    ticker: str,
    website_url: str,
    keywords: Optional[list[str]] = None,
    max_requests: int = 5,
    max_concurrency: int = 10,
) -> List[str]:
    """Crawl a company website and collect all URLs."""
    logger.info(f"Starting to crawl {ticker} website: {website_url}")

    collected_urls = []
    crawler = PlaywrightCrawler(
        max_requests_per_crawl=max_requests,
        browser_pool=BrowserPool(plugins=[CamoufoxPlugin()]),
    )

    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        url = context.request.url
        logger.info(f"Processing page: {url}")

        try:
            title = await context.page.title()
            logger.info(f"Page title: {title}")
        except Exception as e:
            logger.error(f"Failed to get page title for {url}: {e}")
            return

        try:
            await context.infinite_scroll()
            logger.debug(f"Completed infinite scroll on {url}")
        except Exception as e:
            logger.error(f"Failed to perform infinite scroll on {url}: {e}")

        try:
            links = await extract_company_urls(context, selector="a", keywords=keywords)
            logger.info(f"Extracted {len(links)} unique links from {url}")
            collected_urls.extend(links)
        except Exception as e:
            logger.error(f"Failed to extract links from {url}: {e}")

    try:
        await crawler.run([website_url])
        logger.info(f"Successfully crawled {website_url} for {ticker}")

        # Remove duplicates and sort
        unique_urls = sorted(list(set(collected_urls)))
        logger.info(f"Collected {len(unique_urls)} unique URLs for {ticker}")
        return unique_urls

    except Exception as e:
        logger.error(f"Crawler failed for {ticker} ({website_url}): {e}")
        return []


# ---------------------------------------------------------------------------
# New implementation that uses a SINGLE crawler instance to process multiple
# company websites concurrently. The crawler relies on the `user_data` field of
# `Request` objects to keep track of which ticker the request belongs to. This
# allows us to aggregate the extracted URLs per-company and write them to disk
# once the crawl is finished.
# ---------------------------------------------------------------------------


async def crawl_company_websites(
    initial_requests: List[Request],
    *,
    keywords: Optional[list[str]] = None,
) -> dict[str, list[str]]:
    """Crawl multiple company websites in a single crawler run.

    Parameters
    ----------
    initial_requests
        A list of dictionaries each containing at minimum the *url* of the
        company website and *ticker* in the *user_data* field, e.g.::

            {
                "url": "https://example.com",
                "user_data": {"ticker": "EXM", "company_name": "Example Inc."}
            }

    keywords, max_requests, max_concurrency
        Passed through to the underlying :pyclass:`~crawlee.crawlers.PlaywrightCrawler`.

    Returns
    -------
    dict[str, list[str]]
        Mapping of ticker symbols to the list of unique URLs that were
        discovered during the crawl.
    """

    # Mapping <ticker> -> set(urls)
    collected_urls: dict[str, set[str]] = defaultdict(set)

    concurrency_settings = ConcurrencySettings(
        # Start with 8 concurrent tasks, as long as resources are available.
        desired_concurrency=10,
        # Maintain a minimum of 5 concurrent tasks to ensure steady crawling.
        min_concurrency=4,
        # Limit the maximum number of concurrent tasks to 10 to prevent
        # overloading the system.
        max_concurrency=10,
    )

    crawler = PlaywrightCrawler(
        browser_pool=BrowserPool(plugins=[CamoufoxPlugin()]),
        concurrency_settings=concurrency_settings,
    )

    # The router keeps processing pages and stores results in `collected_urls`.
    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:  # noqa: D401
        """Handle a single page within the crawler."""

        ticker: str | None = context.request.user_data.get("ticker")  # type: ignore[attr-defined]
        company_name: str | None = context.request.user_data.get("company_name")  # type: ignore[attr-defined]

        if ticker is None:
            logger.warning(
                "Request missing 'ticker' in user_data – skipping page %s",
                context.request.url,
            )
            return

        url = context.request.url
        logger.info("[%s] Processing page: %s", ticker, url)

        # Best-effort scroll in case the page is lazy-loaded.
        try:
            await context.infinite_scroll()
        except (
            Exception
        ) as e:  # pragma: no-cover – network issues are expected occasionally
            logger.error("[%s] Infinite scroll failed on %s: %s", ticker, url, e)

        try:
            links = await extract_company_urls(context, selector="a", keywords=keywords)

            # Only write to disk if we discovered new URLs for this ticker
            before_count = len(collected_urls[ticker])
            collected_urls[ticker].update(links)
            after_count = len(collected_urls[ticker])

            logger.info(
                "[%s] Extracted %d links on this page; total unique now %d",
                ticker,
                len(links),
                after_count,
            )

            if after_count > before_count:
                # Serialize writes per ticker to avoid concurrent file access
                async with _get_ticker_lock(ticker):
                    # Persist the current snapshot immediately
                    save_urls_to_file(
                        ticker, sorted(collected_urls[ticker]), company_name
                    )
        except Exception as e:  # pragma: no-cover
            logger.error("[%s] Failed to extract links from %s: %s", ticker, url, e)

    # Kick off the crawl with all start URLs at once.
    try:
        await crawler.run(initial_requests)
    except Exception as e:  # pragma: no-cover
        logger.error("Crawler run failed: %s", e)

    # Convert the sets to sorted lists for downstream consumption.
    return {ticker: sorted(urls) for ticker, urls in collected_urls.items()}


def save_urls_to_file(
    ticker: str, urls: List[str], company_name: Optional[str] = None
) -> None:
    """Save collected URLs to a file for the company."""
    filename = f"{ticker}_urls.txt"
    filepath = OUTPUT_DIR / filename

    try:
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(f"# URLs collected for {ticker}")
            if company_name:
                f.write(f" - {company_name}")
            f.write(f"\n# Collection date: {date.today()}")
            f.write(f"\n# Total URLs: {len(urls)}\n")
            f.write("=" * 80 + "\n\n")

            for url in urls:
                f.write(f"{url}\n")

        logger.info(f"Saved {len(urls)} URLs to {filepath}")
        print(f"✓ Saved {len(urls)} URLs for {ticker} to {filepath}")

    except Exception as e:
        logger.error(f"Failed to save URLs for {ticker}: {e}")
        print(f"✗ Failed to save URLs for {ticker}: {e}")


def get_company_name_from_db(ticker: str) -> Optional[str]:
    """Get company name from database for a given ticker."""
    try:
        with get_db_connection() as conn:
            cursor = conn.execute(
                "SELECT company_name FROM companies WHERE ticker = ?", (ticker.upper(),)
            )
            company_row = cursor.fetchone()
            return company_row["company_name"] if company_row else None
    except Exception as e:
        logger.error(f"Error fetching company name for {ticker}: {e}")
        return None


async def process_earnings_companies(
    tickers: List[str],
    keywords: Optional[list[str]] = None,
) -> None:
    """Process all companies with earnings today and collect their URLs using a **single** crawler run."""

    logger.info("Processing %d companies with earnings today", len(tickers))

    # ------------------------------------------------------------------
    # Build the list of *initial_requests* for the crawler.
    # ------------------------------------------------------------------
    initial_requests: list[Request] = []
    ticker_to_company_name: dict[str, Optional[str]] = {}
    failed_crawls = 0  # Will be incremented for tickers we cannot crawl.

    for ticker in tickers:
        website_url = get_company_website_from_db(ticker)
        if not website_url:
            logger.warning("No website found for %s – skipping", ticker)
            failed_crawls += 1
            continue

        company_name = get_company_name_from_db(ticker)
        ticker_to_company_name[ticker] = company_name

        initial_requests.append(
            Request.from_url(  # Use the recommended constructor.
                url=website_url,
                user_data={
                    "ticker": ticker,
                    "company_name": company_name,
                },
            )
        )

    if not initial_requests:
        logger.warning("No valid company websites to crawl – aborting process.")
        return

    # ------------------------------------------------------------------
    # Run the crawler **once** for all companies.
    # ------------------------------------------------------------------
    crawl_results = await crawl_company_websites(
        initial_requests,
        keywords=keywords,
    )

    # ------------------------------------------------------------------
    # Print a summary only. Files are written incrementally during the crawl.
    # ------------------------------------------------------------------
    total_companies = len(tickers)
    num_with_website = len(initial_requests)
    successful_crawls = sum(1 for _, urls in crawl_results.items() if urls)
    failed_crawls_total = failed_crawls + (num_with_website - successful_crawls)

    logger.info(
        "Processing complete: %d successful, %d failed",
        successful_crawls,
        failed_crawls_total,
    )

    print("\nProcessing Summary:")
    print(f"  Total companies: {total_companies}")
    print(f"  Successful crawls: {successful_crawls}")
    print(f"  Failed crawls: {failed_crawls_total}")
    print(f"  Output directory: {OUTPUT_DIR}")


async def main():
    """Main function to run the earnings URL collection process."""
    parser = argparse.ArgumentParser(
        description="Collect ticker symbols from companies with today's earnings and crawl their websites"
    )
    parser.add_argument(
        "--keywords",
        nargs="*",
        default=DEFAULT_KEYWORDS,
        help="Keywords to filter URLs (default: investor relations keywords)",
    )
    parser.add_argument(
        "--no-keywords",
        action="store_true",
        help="Collect all URLs without keyword filtering",
    )

    args = parser.parse_args()

    logger.info("Starting earnings company URL collection process")

    # Ensure database tables exist
    try:
        create_tables()
        logger.info("Database tables verified")
    except Exception as e:
        logger.error(f"Failed to create/verify database tables: {e}")
        return

    # Get today's earnings tickers
    tickers = get_todays_earnings_tickers()
    if not tickers:
        logger.warning("No companies with earnings today found")
        print("No companies with earnings scheduled for today.")
        return

    # Set keywords
    keywords = None if args.no_keywords else args.keywords

    # Process all companies
    await process_earnings_companies(
        tickers=tickers,
        keywords=keywords,
    )


if __name__ == "__main__":
    asyncio.run(main())
