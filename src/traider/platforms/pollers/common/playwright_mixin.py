from __future__ import annotations

"""Mixin adding JavaScript-rendered HTML fetching via Crawlee PlaywrightCrawler.

Designed to be combined with synchronous *Poller classes that already run in
background threads.  The mix-in spins up a dedicated asyncio event-loop in that
thread, starts a single :class:`crawlee.crawlers.PlaywrightCrawler` with
``keep_alive=True`` and exposes a blocking :py:meth:`fetch_html` helper that
returns fully-rendered page HTML.

Usage::

    class MyPoller(PlaywrightMixin, HTMLPoller):
        def __init__(self):
            config = PollerConfig.from_env("MY", ...)
            super().__init__(MY_LIST_URL, config)

        def fetch_data(self):
            return self.fetch_html(self.list_url)
"""

import asyncio
import threading
from concurrent.futures import Future, TimeoutError
from typing import Dict
from uuid import uuid4

import crawlee
from crawlee.crawlers import PlaywrightCrawler, PlaywrightCrawlingContext
from crawlee.errors import SessionError

import logging
logger = logging.getLogger(__name__)

__all__ = ["PlaywrightMixin"]


class PlaywrightMixin:
    """Provide blocking ``fetch_html(url)`` using a dedicated PlaywrightCrawler."""

    _crawler: PlaywrightCrawler | None = None
    _loop: asyncio.AbstractEventLoop | None = None
    _thread: threading.Thread | None = None
    _result_futures: Dict[str, Future[str]]

    # ---------------------------------------------------------------------
    # Initialisation helpers
    # ---------------------------------------------------------------------
    def _ensure_crawler_started(self) -> None:
        """Start the background event-loop and crawler once per instance."""
        if self._crawler is not None:
            return

        # One dictionary per poller instance mapping request unique_key → Future
        self._result_futures = {}

        # Create a fresh event-loop confined to the background thread --------
        self._loop = asyncio.new_event_loop()
        loop = self._loop  # Local non-optional reference for type checkers

        def _thread_entry() -> None:  # Runs in background thread
            asyncio.set_event_loop(loop)
            loop.run_forever()

        self._thread = threading.Thread(target=_thread_entry, daemon=True, name=f"{self.__class__.__name__}-PlaywrightLoop")
        self._thread.start()

        # ------------------------------------------------------------------
        # Create the PlaywrightCrawler *inside* the new loop so it is bound to
        # that loop for its lifetime and register the HTML capturing handler.
        # ------------------------------------------------------------------

        async def _init_crawler() -> None:
            self._crawler = PlaywrightCrawler(
                keep_alive=True,
                headless=False,
                browser_type="chromium",
                max_request_retries=0,  # Fail fast on request-level errors
                use_session_pool=False,  # Disable session rotation on 403 errors
            )

            @self._crawler.router.default_handler  # type: ignore[call-arg]
            async def _handler(context: PlaywrightCrawlingContext) -> None:  # noqa: WPS430
                # If a selector is provided, wait for it to appear before getting content.
                # This is crucial for pages with client-side redirects or JS-based bot checks.
                wait_for_selector = context.request.user_data.get("wait_for_selector")
                if isinstance(wait_for_selector, str):
                    try:
                        await context.page.wait_for_selector(wait_for_selector, timeout=15000)
                    except Exception:
                        # Log the timeout but proceed to grab content anyway, which might
                        # be a "Forbidden" page that is useful for debugging.
                        context.log.warning(
                            "Timed out waiting for selector '%s' at URL: %s",
                            wait_for_selector,
                            context.request.url,
                        )

                html = await context.page.content()
                fut = self._result_futures.pop(context.request.unique_key, None)
                if fut and not fut.done():
                    fut.set_result(html)

            async def _error_handler(context: PlaywrightCrawlingContext, exc: Exception) -> None:
                """Handle crawler errors and propagate them to the waiting Future."""
                fut = self._result_futures.pop(context.request.unique_key, None)
                if fut and not fut.done():
                    fut.set_exception(exc)

            self._crawler.error_handler = _error_handler  # type: ignore[assignment]

            # Start the crawler but don't await its completion (it keeps running)
            asyncio.create_task(self._crawler.run([]))

        # Schedule the init coroutine and wait until it finishes so that
        # self._crawler is ready before we return.
        init_fut = asyncio.run_coroutine_threadsafe(_init_crawler(), self._loop)
        init_fut.result()

    def fetch_html(  # noqa: WPS231
        self, url: str, timeout: float = 30.0, wait_for_selector: str | None = None
    ) -> str:
        """Synchronously fetch rendered HTML for *url* within *timeout* seconds."""
        self._ensure_crawler_started()
        assert self._crawler is not None and self._loop is not None  # for mypy
        crawler = self._crawler
        loop = self._loop

        unique_key = uuid4().hex
        result_fut: Future[str] = Future()
        self._result_futures[unique_key] = result_fut

        async def _queue() -> None:
            request = crawlee.Request.from_url(url, unique_key=unique_key)
            if wait_for_selector:
                request.user_data["wait_for_selector"] = wait_for_selector
            await crawler.add_requests([request])

        enqueue_fut = asyncio.run_coroutine_threadsafe(_queue(), loop)
        enqueue_fut.result()  # wait until the request got queued

        try:
            return result_fut.result(timeout=timeout)
        except (TimeoutError, SessionError) as exc:
            logger.warning("Failed to fetch URL '%s' with Playwright: %s", url, exc)
            return ""  # Return empty HTML on failure to prevent poller crash
        except Exception as exc:
            logger.exception("An unexpected error occurred in Playwright fetch for '%s'", url)
            return ""
