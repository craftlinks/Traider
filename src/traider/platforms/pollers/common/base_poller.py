"""Base classes for all pollers using template method pattern."""
from __future__ import annotations

import os
import random
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

import requests
from dotenv import load_dotenv, find_dotenv
from requests import Response, Session

from .poller_utils import build_session, filter_new_items

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


@dataclass(frozen=True)
class BaseItem:
    """Base item with common fields across all pollers."""
    id: str
    title: str
    url: str
    timestamp: str | None = None
    summary: str | None = None


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
        use_cloudscraper: bool = False,
        extra_headers: dict | None = None
    ):
        self.config = config
        self.seen_ids: Set[str] = set()
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

    @abstractmethod
    def get_poller_name(self) -> str:
        """Return the name of this poller for logging."""
        pass

    @abstractmethod
    def fetch_data(self) -> Response | Dict[str, Any]:
        """Fetch data from the source. Return Response or parsed dict."""
        pass

    @abstractmethod
    def parse_items(self, data: Response | Dict[str, Any]) -> List[BaseItem]:
        """Parse the fetched data into BaseItem objects."""
        pass

    @abstractmethod
    def extract_article_text(self, item: BaseItem) -> str | None:
        """Extract full article text from an item's URL."""
        pass

    def display_item(self, item: BaseItem) -> None:
        """Display an item's information. Can be overridden for custom display."""
        print(f"  -> Title: {item.title}")
        if item.timestamp:
            print(f"     TIMESTAMP: {item.timestamp}")
        if item.summary:
            print(f"     SUMMARY: {item.summary[:160]}")
        print(f"     URL: {item.url}")

    def display_article_text(self, item: BaseItem, article_text: str | None) -> None:
        """Display article text preview. Can be overridden for custom display."""
        if article_text:
            preview = article_text[:300].replace("\n", " ")
            print("   [ARTICLE] Extracted text. Preview:")
            print(f"     {preview}...")
        else:
            print("   [ARTICLE] No extractable text found.")

    def handle_new_items(self, new_items: List[BaseItem]) -> None:
        """Process and display new items."""
        if not new_items:
            return
            
        print(f"[{time.ctime()}] Detected {len(new_items)} new {self.get_poller_name()} item(s):")
        
        for item in new_items:
            self.display_item(item)
            
            if not self.config.skip_extraction:
                try:
                    t0 = time.monotonic()
                    article_text = self.extract_article_text(item)
                    t1 = time.monotonic()
                    
                    if self.config.timing_enabled:
                        fetch_ms = (t1 - t0) * 1000.0
                        print(f"     [TIMING] Article fetch: {fetch_ms:.1f} ms")
                    
                    self.display_article_text(item, article_text)
                except Exception as article_exc:
                    print(f"   [ARTICLE] Error while fetching article: {article_exc}")
            print("")

    def handle_no_new_items(self) -> None:
        """Handle case when no new items are detected."""
        print(f"[{time.ctime()}] No new items detected. Checking again in {self.current_interval:.1f}s...")

    def handle_error(self, exc: Exception) -> None:
        """Handle errors during polling."""
        if isinstance(exc, requests.exceptions.RequestException):
            print(f"[{time.ctime()}] ERROR: Could not connect to {self.get_poller_name()}. {exc}")
        else:
            print(f"[{time.ctime()}] An unexpected error occurred: {exc}")
        
    def normalize_timestamp_to_utc_z(self, dt_text: str) -> str | None:
        """Normalize timestamp string to UTC Z ISO format."""
        try:
            txt = dt_text.strip()
            dt = datetime.fromisoformat(txt.replace("Z", "+00:00"))
            dt_utc = dt.astimezone(timezone.utc)
            return dt_utc.replace(microsecond=0).isoformat().replace("+00:00", "Z")
        except Exception:
            return None

    def print_startup_info(self) -> None:
        """Print startup information."""
        print(f"Starting {self.get_poller_name()} Poller...")
        print("-" * (len(self.get_poller_name()) + 20))
        print(
            f"Polling interval: {self.config.polling_interval_seconds}s | "
            f"User-Agent: {self.config.user_agent} | "
            f"Min request interval: {self.config.min_request_interval_sec:.3f}s | "
            f"Jitter: ±{int(self.config.jitter_fraction*100)}%"
        )

        if "example.com" in self.config.user_agent:
            print(f"[WARN] Your User-Agent appears to be a placeholder. Consider setting a real contact.")

    def run_polling_loop(self) -> None:
        """Main polling loop using template method pattern."""
        self.print_startup_info()
        
        while True:
            new_items: List[BaseItem] = []
            try:
                data = self.fetch_data()
                items = self.parse_items(data)
                new_items = filter_new_items(items, self.seen_ids)
                
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