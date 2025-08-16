from __future__ import annotations

"""Playwright-based Crawlee crawler used by the FastAPI web server.

The crawler is configured with ``keep_alive=True`` so that it can
stay idle and react to requests coming from the HTTP API. Each processed
request captures a screenshot of the requested page using Playwright and
stores it in the local ``KeyValueStore``. The result (key under which the
screenshot was stored) is propagated back to the API via an ``asyncio.Future``
placed in the application ``state``.
"""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import TypedDict

from fastapi import FastAPI

from crawlee.crawlers import PlaywrightCrawler, PlaywrightCrawlingContext
from crawlee.storages import KeyValueStore

__all__ = ["lifespan"]


class State(TypedDict):
    """Objects kept in FastAPI application state."""

    crawler: PlaywrightCrawler
    requests_to_results: dict[str, asyncio.Future[dict[str, str]]]


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[State]:
    """FastAPI lifespan context that initialises and tears down the crawler."""

    # Dictionary used to bridge a crawling result back to the HTTP handler
    # via a unique-key -> Future mapping.
    requests_to_results: dict[str, asyncio.Future[dict[str, str]]] = {}

    # Open the default key-value store where screenshots will be recorded.
    kvs = await KeyValueStore.open()

    # Create the crawler instance ------------------------------------------
    crawler = PlaywrightCrawler(
        keep_alive=True,  # keep the crawler running between requests
        headless=True,  # run browser in headless mode
        browser_type="chromium",
        max_requests_per_crawl=10_000,  # practically unlimited
    )

    # Register the default request handler ---------------------------------
    @crawler.router.default_handler  # type: ignore[call-arg]
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        """Handle a single webpage request and take a screenshot."""

        context.log.info("Processing %s …", context.request.url)

        # Attempt to capture a screenshot; fallback to empty bytes on error.
        try:
            screenshot = await context.page.screenshot()
        except Exception as exc:  # pragma: no cover – defensive
            context.log.exception("Failed to capture screenshot: %s", exc)
            screenshot = b""

        # Derive a filename-like key from the URL – "google.com" -> "google.com.png".
        url_part = context.request.url.rstrip("/").split("/")[-1] or "index"
        kv_key = f"screenshot-{url_part}.png"

        # Persist the screenshot in the KV store so it can be inspected later.
        await kvs.set_value(key=kv_key, value=screenshot, content_type="image/png")

        # Fulfil the awaiting Future so the API call can return.
        future = requests_to_results.get(context.request.unique_key)
        if future is not None and not future.done():
            future.set_result({"screenshot_key": kv_key, "bytes": str(len(screenshot))})

    # ---------------------------------------------------------------------
    # Start the crawler in the background without blocking the lifespan init.
    # ---------------------------------------------------------------------
    crawler.log.info("Starting PlaywrightCrawler (%s)", app.title)
    run_task = asyncio.create_task(crawler.run([]))

    # Expose objects via FastAPI application state so request handlers can access them.
    app.state.crawler = crawler  # type: ignore[attr-defined]
    app.state.requests_to_results = requests_to_results  # type: ignore[attr-defined]

    # Yield state to the FastAPI application --------------------------------
    yield {"crawler": crawler, "requests_to_results": requests_to_results}

    # ---------------------------------------------------------------------
    # Teardown – gracefully stop the crawler before the application exits.
    # ---------------------------------------------------------------------
    crawler.stop()
    await run_task
