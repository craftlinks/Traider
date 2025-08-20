import argparse
from ast import List
import logging
import sqlite3
from pathlib import Path
import asyncio
from typing import Iterator, Optional
from yarl import URL
from urllib.parse import ParseResult, urlparse
from typing_extensions import assert_never

import tldextract
from crawlee.crawlers import PlaywrightCrawler, PlaywrightCrawlingContext

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('crawler.log')
    ]
)
logger = logging.getLogger(__name__)

TICKERS = ['LWLG']

# Database path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
DATABASE_FILE = PROJECT_ROOT / "storage" / "trading_platform.db"

def get_db_connection():
    """Return a new SQLite connection with row access by column name."""
    conn = sqlite3.connect(DATABASE_FILE)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def get_company_info_with_website(ticker: str):
    """Get company name and website URL for a given ticker."""
    logger.info(f"Fetching company info for ticker: {ticker}")
    with get_db_connection() as conn:
        # Get company info
        cursor = conn.execute(
            "SELECT company_name FROM companies WHERE ticker = ?",
            (ticker.upper(),)
        )
        company_row = cursor.fetchone()

        if not company_row:
            return None, None

        company_name = company_row['company_name']

        # Get website URL
        cursor = conn.execute(
            "SELECT url FROM urls WHERE company_ticker = ? AND url_type = 'website'",
            (ticker.upper(),)
        )
        url_row = cursor.fetchone()
        website_url = url_row['url'] if url_row else None

        return company_name, website_url

def convert_to_absolute_url(base_url: str, relative_url: str) -> str:
    """Convert a relative URL to an absolute URL using a base URL."""
    return str(URL(base_url).join(URL(relative_url)))

def is_url_absolute(url: str) -> bool:
    """Check if a URL is absolute."""
    url_parsed = URL(url)

    # We don't use .absolute because in yarl.URL, it is always True for links that start with '//'
    return bool(url_parsed.scheme) and bool(url_parsed.raw_authority)

def to_absolute_url_iterator(base_url: str, urls: Iterator[str]) -> Iterator[str]:
    """Convert an iterator of relative URLs to absolute URLs using a base URL."""
    for url in urls:
        if is_url_absolute(url):
            yield url
        else:
            yield convert_to_absolute_url(base_url, url)

def _check_enqueue_strategy(
    strategy: str,
    *,
    target_url: ParseResult,
    origin_url: ParseResult,
) -> bool:
    """Check if a URL matches the enqueue_strategy."""
    if strategy == 'all':
        return True

    if origin_url.hostname is None or target_url.hostname is None:
        logger.warning(
            f'Skipping enqueue: Missing hostname in origin_url = {origin_url.geturl()} or '
            f'target_url = {target_url.geturl()}'
        )
        return False

    if strategy == 'same-hostname':
        return target_url.hostname == origin_url.hostname

    if strategy == 'same-domain':
        origin_domain = tldextract.extract(origin_url.hostname).domain
        target_domain = tldextract.extract(target_url.hostname).domain
        return origin_domain == target_domain

    if strategy == 'same-origin':
        return (
            target_url.hostname == origin_url.hostname
            and target_url.scheme == origin_url.scheme
            and target_url.port == origin_url.port
        )

    return False

DEFAULT_KEYWORDS = ['invest', 'event', 'news', 'filing', 'press-release', 'press release']

def _filter_links_iterator(
        request_iterator: Iterator[str], origin_url: str, enqueue_strategy: str, limit: int | None = None,
        keywords: Optional[list[str]] = None) -> Iterator[str]:
        """Filter requests based on the enqueue strategy and URL patterns."""
        parsed_origin_url = urlparse(origin_url)

        if enqueue_strategy == 'all' and not parsed_origin_url.hostname:
            logger.warning(f'Skipping enqueue: Missing hostname in origin_url = {origin_url}.')
            return

        # Emit a `warning` message to the log, only once per call
        warning_flag = True

        for url in request_iterator:
            target_url = url
            parsed_target_url = urlparse(target_url)

            if warning_flag and enqueue_strategy != 'all' and not parsed_target_url.hostname:
                logger.warning(f'Skipping enqueue url: Missing hostname in target_url = {target_url}.')
                warning_flag = False

            if _check_enqueue_strategy(
                enqueue_strategy, target_url=parsed_target_url, origin_url=parsed_origin_url
            ):
                # Check if URL (excluding scheme) contains any of the keywords (case-insensitive)
                # This includes both domain and path
                url_without_scheme = target_url.lower().replace('https://', '').replace('http://', '')
                if keywords:
                    has_keyword = any(keyword.lower() in url_without_scheme for keyword in keywords)
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

async def extract_links(context: PlaywrightCrawlingContext, selector: str = 'a', keywords: Optional[list[str]] = None) -> list[str]:
    # https://github.com/apify/crawlee-python/blob/08ab74a73ce7e01072132364ffb40c913f07e226/src/crawlee/crawlers/_playwright/_playwright_crawler.py#L364

    elements = await context.page.query_selector_all(selector)

    links_iterator: Iterator[str] = iter(
                [url for element in elements if (url := await element.get_attribute('href')) is not None]
            )
    links_iterator = to_absolute_url_iterator(context.request.loaded_url or context.request.url, links_iterator)

    filtered_links_iterator = _filter_links_iterator(links_iterator, context.request.url, 'same-domain', keywords=keywords)

    return list(set(filtered_links_iterator))


async def run_crawler(websites: list[str], keywords: Optional[list[str]] = None):
    """Run the crawler on the provided website URLs."""
    logger.info(f"Starting crawler with {len(websites)} websites to process")
    logger.info(f"Websites to crawl: {websites}")

    crawler = PlaywrightCrawler(max_requests_per_crawl=10)

    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        url = context.request.url
        logger.info(f"Processing page: {url}")

        try:
            title = await context.page.title()
            logger.info(f'Page title: {title}')
        except Exception as e:
            logger.error(f'Failed to get page title for {url}: {e}')
            return

        try:
            await context.infinite_scroll()
            logger.debug(f"Completed infinite scroll on {url}")
        except Exception as e:
            logger.error(f'Failed to perform infinite scroll on {url}: {e}')

        try:
            links = await extract_links(context, selector='a', keywords=keywords)
            logger.info(f"Extracted {len(links)} unique links from {url}")
            for i, link in enumerate(links):  # Log first 5 links to avoid spam
                logger.info(f"Link {i+1}: {link}")
        except Exception as e:
            logger.error(f'Failed to extract links from {url}: {e}')

        # if we want twe can create Request objects and enqueue them
        # requests = [Request(url=link) for link in links]
        # await context.enqueue_links(requests=requests)

    try:
        logger.info("Starting crawler execution...")
        await crawler.run(websites)
        logger.info("Crawler execution completed successfully")
    except Exception as e:
        logger.error(f"Crawler execution failed: {e}")
        raise

if __name__ == "__main__":
    logger.info("Starting discover_pr_urls script")
    parser = argparse.ArgumentParser(description="Fetch company name and website URL for ticker(s)")
    parser.add_argument("ticker", nargs="?", help="Stock ticker symbol (e.g., AAPL, MSFT, LWLG). If not provided, uses default TICKERS list.")

    args = parser.parse_args()

    # Use CLI argument if provided, otherwise use default TICKERS list
    tickers_to_process = [args.ticker.upper()] if args.ticker else TICKERS
    logger.info(f"Processing tickers: {tickers_to_process}")

    # Get company info and website URL for each ticker
    websites = []
    for ticker in tickers_to_process:
        logger.info(f"Processing ticker: {ticker}")
        company_name, website_url = get_company_info_with_website(ticker)
        if website_url:
            websites.append(website_url)

    asyncio.run(run_crawler(websites, keywords=None))