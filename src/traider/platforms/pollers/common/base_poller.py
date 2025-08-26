"""Base classes for all pollers using template method pattern."""
from __future__ import annotations

import os
import logging
import random
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Callable

import requests
from dotenv import load_dotenv, find_dotenv
from requests import Response, Session

from .poller_utils import build_session, filter_new_items
from traider.platforms.cache import get_shared_cache
import pandas as pd
from traider.interfaces.cache import CacheInterface

# Use find_dotenv to locate the nearest .env starting from the CWD and moving up
# This is more reliable when poller modules live in nested sub-packages but are
# executed from an arbitrary working directory (e.g., with ``python -m``).
_DOTENV_PATH = find_dotenv(usecwd=True) or find_dotenv()
if _DOTENV_PATH:
    load_dotenv(_DOTENV_PATH)
else:
    # For debugging: you can uncomment the next line to verify where we looked.
    # print("[DEBUG] .env not found starting from CWD or module path")
    pass


logger = logging.getLogger(__name__)

@dataclass
class BaseItem:
    """Base item with common fields across all pollers."""
    id: str
    title: str
    url: str
    timestamp: datetime | None = None
    summary: str | None = None
    article_text: str | None = None

    @staticmethod
    def parse_iso_utc(ts: str) -> datetime:
        """Parse an ISO-8601 timestamp (e.g. '2025-03-01T14:07:23Z') into a
        timezone-aware UTC datetime instance.
        """
        return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc)


@dataclass
class PollerConfig:
    """Configuration for pollers with sensible defaults."""
    polling_interval_seconds: int
    user_agent: str
    min_request_interval_sec: float
    jitter_fraction: float = 0.1
    skip_extraction: bool = False
    timing_enabled: bool = False
    article_timeout_seconds: float = 8.0

    @classmethod
    def from_env(
        cls, 
        prefix: str, 
        default_interval: int = 3, 
        default_user_agent: str = "TraderPoller/1.0 admin@example.com",
        default_min_interval: float = 0.25
    ) -> "PollerConfig":
        """Create config from environment variables with given prefix."""
        return cls(
            polling_interval_seconds=int(os.getenv(f"{prefix}_POLL_INTERVAL", default_interval)),
            user_agent=os.getenv(f"{prefix}_USER_AGENT", default_user_agent),
            min_request_interval_sec=float(os.getenv(f"{prefix}_MIN_REQUEST_INTERVAL_SEC", default_min_interval)),
            jitter_fraction=float(os.getenv(f"{prefix}_JITTER_FRACTION", 0.1)),
            skip_extraction=os.getenv(f"{prefix}_SKIP_EXTRACTION", "0").lower() in ("1", "true", "yes", "y"),
            timing_enabled=os.getenv(f"{prefix}_TIMING", "0").lower() in ("1", "true", "yes", "y"),
            article_timeout_seconds=float(os.getenv(f"{prefix}_ARTICLE_TIMEOUT_SEC", 8.0)),
        )


class BasePoller(ABC):
    """Abstract base class for all pollers implementing the template method pattern."""
    
    def __init__(
        self,
        config: PollerConfig,
        *,
        cache: CacheInterface | None = None,
        use_cloudscraper: bool = False,
        extra_headers: dict | None = None,
    ):
        """BasePoller constructor.

        Parameters
        ----------
        config:
            Poller runtime configuration.
        cache:
            Optional dedicated cache instance.  When *None*, falls back to the
            process-wide :pyfunc:`traider.platforms.cache.get_shared_cache`.
        use_cloudscraper / extra_headers:
            Passed straight to :pyfunc:`build_session`.
        """

        self.config = config
        # Per-poller cache (defaults to global shared cache)
        self.cache = cache if cache is not None else get_shared_cache()
        self.session = build_session(
            config.user_agent, 
            config.min_request_interval_sec, 
            use_cloudscraper,
            extra_headers
        )
        self.current_interval = float(config.polling_interval_seconds)
        
        # Cache headers for conditional requests
        self.feed_etag: Optional[str] = None
        self.feed_last_modified: Optional[str] = None
        # Optional sink to emit processed items to a downstream consumer
        self._sink: Callable[[str, BaseItem], None] | None = None

    @abstractmethod
    def get_poller_name(self) -> str:
        """Return the name of this poller for logging."""
        pass

    @abstractmethod
    def fetch_data(self) -> Response | Dict[str, Any] | pd.DataFrame:
        """Fetch data from the source. Return Response or parsed dict."""
        pass

    @abstractmethod
    def parse_items(self, data: Response | Dict[str, Any] | pd.DataFrame) -> List[BaseItem]:
        """Parse the fetched data into BaseItem objects."""
        pass

    @abstractmethod
    def extract_article_text(self, item: BaseItem) -> str | None:
        """Extract full article text from an item's URL."""
        pass

    def set_sink(self, sink: Callable) -> None:
        """Register a sink callable to receive processed items.

        The sink will be called with (poller_name, item) for each new item.
        """
        self._sink = sink

    def handle_new_items(self, new_items: List[BaseItem]) -> None:
        """Process and display new items."""
        if not new_items:
            return
        
        for item in new_items:
            article_text: str | None = None
            if not self.config.skip_extraction:
                try:
                    t0 = time.monotonic()
                    article_text = self.extract_article_text(item)
                    t1 = time.monotonic()
                    
                    if self.config.timing_enabled:
                        fetch_ms = (t1 - t0) * 1000.0
                        logger.debug("[TIMING] Article fetch: %.1f ms", fetch_ms)

                    
                except Exception as article_exc:
                    logger.exception("[ARTICLE] Error while fetching article: %s", article_exc)

                item.article_text = article_text
            
            # Emit to sink if configured
            if self._sink is not None:
                try:
                    self._sink(self.get_poller_name(), item)
                except Exception as sink_exc:
                    logger.exception("[SINK] Error while emitting item: %s", sink_exc)

    def handle_no_new_items(self) -> None:
        """Handle case when no new items are detected."""
        logger.debug("No new items detected. Checking again in %.1fs...", self.current_interval)

    def handle_error(self, exc: Exception) -> None:
        """Handle errors during polling."""
        if isinstance(exc, requests.exceptions.RequestException):
            logger.error("Could not connect to %s. %s", self.get_poller_name(), exc, exc_info=exc)
        else:
            logger.error("An unexpected error occurred: %s", exc, exc_info=exc)
        
    def normalize_timestamp_to_utc_z(self, dt_text: str) -> datetime | None:
        """Normalize timestamp string to a timezone-aware UTC datetime."""
        try:
            txt = dt_text.strip()
            dt = datetime.fromisoformat(txt.replace("Z", "+00:00"))
            return dt.astimezone(timezone.utc)
        except Exception:
            return None

    def print_startup_info(self) -> None:
        """Log startup information."""
        logger.info("Starting %s Poller...", self.get_poller_name())
        logger.info(
            "Polling interval: %ds | User-Agent: %s | Min request interval: %.3fs | Jitter: ±%d%%",
            self.config.polling_interval_seconds,
            self.config.user_agent,
            self.config.min_request_interval_sec,
            int(self.config.jitter_fraction * 100),
        )

        if "example.com" in self.config.user_agent:
            logger.warning("Your User-Agent appears to be a placeholder. Consider setting a real contact.")

    def run_polling_loop(self) -> None:
        """Main polling loop using template method pattern."""
        self.print_startup_info()
        
        while True:
            new_items: List[BaseItem] = []
            try:
                data = self.fetch_data()
                logger.debug("Fetched data: %s", data)
                items = self.parse_items(data)
                new_items = filter_new_items(items, self.cache)
                
                if new_items:
                    self.handle_new_items(new_items)
                    self.current_interval = float(self.config.polling_interval_seconds)
                else:
                    self.handle_no_new_items()
                    
            except Exception as exc:
                self.handle_error(exc)

            # Sleep with jitter
            jitter = 1.0 + random.uniform(-self.config.jitter_fraction, self.config.jitter_fraction)
            sleep_for = max(1.0, self.current_interval * jitter)
            time.sleep(sleep_for)

    def run(
        self,
        polling_interval_seconds: Optional[int] = None,
        user_agent: Optional[str] = None,
    ) -> None:
        """Run the poller with optional parameter overrides."""
        if polling_interval_seconds is not None:
            self.config.polling_interval_seconds = polling_interval_seconds
            self.current_interval = float(polling_interval_seconds)
        if user_agent is not None:
            self.config.user_agent = user_agent
            
        self.run_polling_loop()