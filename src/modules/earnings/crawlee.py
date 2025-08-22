import asyncio
from pathlib import Path
from crawlee import ConcurrencySettings
from crawlee.browsers import PlaywrightBrowserPlugin, PlaywrightBrowserController, BrowserPool
from crawlee.crawlers import PlaywrightCrawler, PlaywrightCrawlingContext
import logging
from typing import Iterator, Optional, override
from dspy import Prediction
from traider.llm.press_release_urls import select_press_release_url
from yarl import URL
from urllib.parse import ParseResult, urlparse
import tldextract
from camoufox import AsyncNewBrowser
import aiofiles
from traider.db.data_manager import add_url
logger = logging.getLogger(__name__)

DEFAULT_KEYWORDS = ['invest', 'event', 'news', 'filing', 'press-release', 'press release']
# Default output directory for company URL files
OUTPUT_DIR = Path(__file__).resolve().parents[3] / "storage" / "company_urls"
OUTPUT_DIR.mkdir(exist_ok=True)

# Locks to serialize per-ticker file writes when crawling concurrently
_ticker_file_locks: dict[str, asyncio.Lock] = {}

def _get_ticker_lock(ticker: str) -> asyncio.Lock:
    """Return a process-wide asyncio.Lock for the given ticker."""
    # setdefault ensures only one Lock per ticker
    lock = _ticker_file_locks.setdefault(ticker, asyncio.Lock())
    return lock

async def save_urls_to_file(ticker: str, urls: list[str]) -> None:
    """Save collected URLs to a file for the company."""
    filename = f"{ticker}_press_release_url.txt"
    filepath = OUTPUT_DIR / filename

    try:
        async with aiofiles.open(filepath, 'w', encoding='utf-8') as f:

            for url in urls:
                await f.write(f"{url}\n")

        logger.info(f"Saved {len(urls)} URLs to {filepath}")

    except Exception as e:
        logger.error(f"Failed to save URLs for {ticker}: {e}")


class CamoufoxPlugin(PlaywrightBrowserPlugin):
    """Example browser plugin that uses Camoufox browser,
    but otherwise keeps the functionality of PlaywrightBrowserPlugin.
    """

    @override
    async def new_browser(self) -> PlaywrightBrowserController:
        if not self._playwright:
            raise RuntimeError('Playwright browser plugin is not initialized.')

        return PlaywrightBrowserController(
            browser=await AsyncNewBrowser(
                self._playwright, **self._browser_launch_options
            ),
            # Increase, if camoufox can handle it in your use case.
            max_open_pages_per_browser=1,
            # This turns off the crawlee header_generation. Camoufox has its own.
            header_generator=None,
        )
concurrency_settings = ConcurrencySettings(
        # Start with 8 concurrent tasks, as long as resources are available.
        desired_concurrency=10,
        # Maintain a minimum of 5 concurrent tasks to ensure steady crawling.
        min_concurrency=4,
        # Limit the maximum number of concurrent tasks to 10 to prevent
        # overloading the system.
        max_concurrency=10,
    )

crawler = PlaywrightCrawler(concurrency_settings=concurrency_settings, browser_pool=BrowserPool(plugins=[CamoufoxPlugin()]), keep_alive=True)

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

def _filter_links_iterator(
        request_iterator: Iterator[str],
        origin_url: str,
        enqueue_strategy: str,
        limit: int | None = None,
        keywords: Optional[list[str]] = None
) -> Iterator[str]:
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

def convert_to_absolute_url(base_url: str, relative_url: str) -> str:
    """Convert a relative URL to an absolute URL using a base URL."""
    return str(URL(base_url).join(URL(relative_url)))

def is_url_absolute(url: str) -> bool:
    """Check if a URL is absolute."""
    url_parsed = URL(url)
    return bool(url_parsed.scheme) and bool(url_parsed.raw_authority)

def to_absolute_url_iterator(base_url: str, urls: Iterator[str]) -> Iterator[str]:
    """Convert an iterator of relative URLs to absolute URLs using a base URL."""
    for url in urls:
        if is_url_absolute(url):
            yield url
        else:
            yield convert_to_absolute_url(base_url, url)

async def extract_company_urls(
    context: PlaywrightCrawlingContext,
    selector: str = 'a',
    keywords: Optional[list[str]] = None
) -> list[str]:
    """Extract URLs from a company webpage."""
    elements = await context.page.query_selector_all(selector)

    links_iterator: Iterator[str] = iter(
        [url for element in elements if (url := await element.get_attribute('href')) is not None]
    )

    links_iterator = to_absolute_url_iterator(
        context.request.loaded_url or context.request.url,
        links_iterator
    )

    filtered_links_iterator = _filter_links_iterator(
        links_iterator,
        context.request.url,
        'same-domain',
        keywords=keywords
    )

    return list(set(filtered_links_iterator))


@crawler.router.default_handler
async def request_handler(context: PlaywrightCrawlingContext) -> None:  # noqa: D401
    """Handle a single page within the crawler."""

    ticker: str = context.request.user_data.get("ticker")  # type: ignore[attr-defined]

    url = context.request.url

    if context.request.label == 'STOP':
        logger.info("Stopping crawler")
        crawler.stop()
        return

    logger.info("[%s] Processing page: %s", ticker, url)

    # wait until the page is loaded
    await context.page.wait_for_load_state("networkidle")


    await context.infinite_scroll()


    
    links = await extract_company_urls(context, selector="a", keywords=DEFAULT_KEYWORDS)

    if len(links) == 0:
        logger.info("[%s] No links found on this page", ticker)
        return

    response: Prediction = await select_press_release_url.acall(input_urls=links)

    logger.info("[%s] - Selected Press release URL: %s", ticker, response.output_url)

    # Serialize writes per ticker to avoid concurrent file access
    async with _get_ticker_lock(ticker):
        # Persist the current snapshot immediately
        await save_urls_to_file(ticker, [response.output_url])

        # Persist the press release URL in the database
        try:
            add_url(company_ticker=ticker, url_type="press-release", url=response.output_url)
            logger.info("[%s] Press release URL persisted to DB: %s", ticker, response.output_url)
        except Exception as db_exc:  # pragma: no-cover
            logger.error("[%s] Failed to persist press release URL to DB: %s", ticker, db_exc)