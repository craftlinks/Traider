"""Specialized base classes for different types of pollers."""
from __future__ import annotations

import xml.etree.ElementTree as ET
from abc import abstractmethod
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from requests import Response

from .base_poller import BaseItem, BasePoller
from .poller_utils import extract_primary_text_from_html


class FeedPoller(BasePoller):
    """Base class for RSS/Atom feed pollers."""
    
    def __init__(self, feed_url: str, *args, **kwargs):
        self.feed_url = feed_url
        super().__init__(*args, **kwargs)
        # Override accept header for feeds
        self.session.headers.update({
            "Accept": "application/atom+xml, application/xml, application/rss+xml;q=0.9, */*;q=0.8"
        })

    def fetch_data(self) -> Response:
        """Fetch feed with conditional headers for caching."""
        headers: dict[str, str] = {}
        if self.feed_etag:
            headers["If-None-Match"] = self.feed_etag
        if self.feed_last_modified:
            headers["If-Modified-Since"] = self.feed_last_modified

        response = self.session.get(self.feed_url, headers=headers, timeout=15)
        
        if response.status_code == 304:
            # Handle 304 Not Modified
            print(f"[{datetime.now().strftime('%c')}] Feed not modified (304). Next check in {self.config.polling_interval_seconds}s...")
            return response
        
        response.raise_for_status()
        
        # Update cache headers
        self.feed_etag = response.headers.get("ETag")
        self.feed_last_modified = response.headers.get("Last-Modified")
        
        return response

    @abstractmethod
    def parse_feed_entries(self, xml_content: bytes) -> List[BaseItem]:
        """Parse XML feed entries into BaseItem objects."""
        pass

    def parse_items(self, data: Response | Dict[str, Any]) -> List[BaseItem]:
        """Parse the response into items."""
        if not isinstance(data, Response):
            raise TypeError(f"FeedPoller.parse_items expects Response, got {type(data)}")
        if data.status_code == 304:
            return []  # No new items
        return self.parse_feed_entries(data.content)

    def normalize_rfc822_to_utc_z(self, dt_text: str) -> str | None:
        """Parse RFC-822 date (RSS format) to UTC Z ISO format."""
        try:
            from email.utils import parsedate_to_datetime
            dt = parsedate_to_datetime(dt_text.strip())
            dt_utc = dt.astimezone(timezone.utc)
            return dt_utc.replace(microsecond=0).isoformat().replace("+00:00", "Z")
        except Exception:
            return None

    def extract_article_text(self, item: BaseItem) -> str | None:
        """Extract article text from URL."""
        headers = {"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"}
        response = self.session.get(item.url, headers=headers, timeout=self.config.article_timeout_seconds)
        response.raise_for_status()
        return extract_primary_text_from_html(response.text)


class AtomFeedPoller(FeedPoller):
    """Specialized poller for Atom feeds."""
    
    def parse_feed_entries(self, xml_content: bytes) -> List[BaseItem]:
        """Parse Atom XML entries."""
        root = ET.fromstring(xml_content)
        ns = {"atom": "http://www.w3.org/2005/Atom"}
        
        entries = root.findall("atom:entry", ns)
        items = []
        
        for entry in entries:
            id_el = entry.find("atom:id", ns)
            title_el = entry.find("atom:title", ns)
            link_el = entry.find("atom:link[@rel='alternate']", ns)
            if link_el is None:
                link_el = entry.find("atom:link", ns)
            
            pub_el = entry.find("atom:published", ns)
            upd_el = entry.find("atom:updated", ns)
            
            # Element truthiness is False when element has no children in Python 3.12.
            # Use explicit None checks instead of all([...]) to avoid skipping valid entries.
            if id_el is None or title_el is None or link_el is None:
                continue
                
            entry_id = (id_el.text or "").strip()
            title = (title_el.text or "").strip()
            href = link_el.get("href", "").strip()
            
            if not entry_id or not title or not href:
                continue
            
            timestamp = None
            if pub_el is not None and pub_el.text:
                timestamp = self.normalize_timestamp_to_utc_z(pub_el.text)
            elif upd_el is not None and upd_el.text:
                timestamp = self.normalize_timestamp_to_utc_z(upd_el.text)
            
            items.append(BaseItem(
                id=entry_id,
                title=title, 
                url=href,
                timestamp=timestamp
            ))
        
        return items


class RSSFeedPoller(FeedPoller):
    """Specialized poller for RSS feeds."""
    
    def parse_feed_entries(self, xml_content: bytes) -> List[BaseItem]:
        """Parse RSS XML entries."""
        root = ET.fromstring(xml_content)
        items_xml = root.findall("./channel/item")
        items = []
        
        for item_xml in items_xml:
            guid_el = item_xml.find("guid")
            title_el = item_xml.find("title")
            link_el = item_xml.find("link")
            pub_el = item_xml.find("pubDate")
            desc_el = item_xml.find("description")
            
            # Use GUID or link as ID
            entry_id = None
            if guid_el is not None and guid_el.text:
                entry_id = guid_el.text.strip()
            elif link_el is not None and link_el.text:
                entry_id = link_el.text.strip()
            
            # Similar to Atom feed parsing: avoid relying on Element truthiness. Ensure
            # elements exist and have non-empty text before proceeding.
            if title_el is None or link_el is None or not entry_id:
                continue

            title_text = (title_el.text or "").strip()
            href_text = (link_el.text or "").strip()

            if not title_text or not href_text:
                continue
            
            timestamp = None
            if pub_el is not None and pub_el.text:
                timestamp = self.normalize_rfc822_to_utc_z(pub_el.text)
            
            summary = None
            if desc_el is not None and desc_el.text:
                summary = desc_el.text.strip()
            
            items.append(BaseItem(
                id=entry_id,
                title=title_text,
                url=href_text,
                timestamp=timestamp,
                summary=summary
            ))
        
        return items


class HTMLPoller(BasePoller):
    """Base class for HTML scraping pollers."""
    
    def __init__(self, list_url: str, container_patterns: List[str] | None = None, *args, **kwargs):
        self.list_url = list_url
        self.container_patterns = container_patterns or []
        super().__init__(*args, **kwargs)

    def fetch_data(self) -> Response:
        """Fetch HTML page."""
        headers = {"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"}
        response = self.session.get(self.list_url, headers=headers, timeout=15)
        response.raise_for_status()
        return response

    @abstractmethod
    def parse_html_items(self, html: str) -> List[BaseItem]:
        """Parse HTML content into BaseItem objects."""
        pass

    def parse_items(self, data: Response | Dict[str, Any]) -> List[BaseItem]:
        """Parse the HTML response into items."""
        if not isinstance(data, Response):
            raise TypeError(f"HTMLPoller.parse_items expects Response, got {type(data)}")
        return self.parse_html_items(data.text)

    def extract_article_text(self, item: BaseItem) -> str | None:
        """Extract article text from URL."""
        headers = {"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"}
        response = self.session.get(item.url, headers=headers, timeout=self.config.article_timeout_seconds)
        response.raise_for_status()
        return extract_primary_text_from_html(response.text, self.container_patterns)


class APIPoller(BasePoller):
    """Base class for JSON API pollers."""
    
    def __init__(self, api_url: str, *args, **kwargs):
        self.api_url = api_url
        super().__init__(*args, **kwargs)
        # Override accept header for JSON APIs
        self.session.headers.update({
            "Accept": "application/json, text/plain, */*"
        })

    def fetch_data(self) -> Dict[str, Any]:
        """Fetch JSON data from API."""
        response = self.session.get(self.api_url, timeout=15)
        response.raise_for_status()
        return response.json()

    @abstractmethod  
    def parse_api_items(self, data: Dict[str, Any]) -> List[BaseItem]:
        """Parse JSON data into BaseItem objects."""
        pass

    def parse_items(self, data: Response | Dict[str, Any]) -> List[BaseItem]:
        """Parse the JSON data into items."""
        if isinstance(data, Response):
            raise TypeError(f"APIPoller.parse_items expects Dict, got {type(data)}")
        return self.parse_api_items(data)

    def extract_article_text(self, item: BaseItem) -> str | None:
        """Extract article text from URL."""
        headers = {"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"}
        response = self.session.get(item.url, headers=headers, timeout=self.config.article_timeout_seconds)
        response.raise_for_status()
        return extract_primary_text_from_html(response.text)