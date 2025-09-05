"""Globe Newswire Atom feed poller - refactored version."""

from __future__ import annotations

import xml.etree.ElementTree as ET
from dataclasses import dataclass
import logging
from datetime import datetime, timezone
from typing import List, Optional, Sequence, Tuple

from .common.base_poller import BaseItem, PollerConfig
from .common.specialized_pollers import AtomFeedPoller

logger = logging.getLogger(__name__)


# Configuration
ATOM_FEED_URL: str = "https://www.globenewswire.com/AtomFeed/orgclass/1/feedTitle/GlobeNewswire%20-%20News%20about%20Public%20Companies"


@dataclass
class GlobeNewswireItem(BaseItem):
    """Globe Newswire specific item with Dublin Core metadata."""

    updated_utc: datetime | None = None
    modified_utc: str | None = None
    identifier: str | None = None
    language: str | None = None
    publisher: str | None = None
    contributor: str | None = None
    subjects: Sequence[str] | None = None
    keywords: Sequence[str] | None = None
    categories: Sequence[Tuple[str, Optional[str]]] | None = None
    content_html: str | None = None


class GlobeNewswirePoller(AtomFeedPoller):
    """Globe Newswire Atom feed poller with Dublin Core support."""

    def __init__(self):
        config = PollerConfig.from_env(
            "GNW",
            default_interval=3,
            default_user_agent="TraderGNWWatcher/1.0 admin@example.com",
            default_min_interval=0.25,
        )
        super().__init__(ATOM_FEED_URL, config)

    def get_poller_name(self) -> str:
        return "GlobeNewswire"

    def parse_feed_entries(self, xml_content: bytes) -> List[BaseItem]:
        """Parse Globe Newswire Atom XML with Dublin Core extensions."""
        root = ET.fromstring(xml_content)
        ns = {
            "atom": "http://www.w3.org/2005/Atom",
            "dc": "http://purl.org/dc/elements/1.1/",
        }

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

            # Dublin Core elements
            modified_el = entry.find("dc:modified", ns)
            identifier_el = entry.find("dc:identifier", ns)
            lang_el = entry.find("dc:language", ns)
            publisher_el = entry.find("dc:publisher", ns)
            contrib_el = entry.find("dc:contributor", ns)
            subject_els = entry.findall("dc:subject", ns)
            keyword_els = entry.findall("dc:keyword", ns)

            # Avoid Element truthiness which is False when element has no children in
            # Python 3.12+. Use explicit None comparison instead.
            if id_el is None or title_el is None or link_el is None:
                continue

            entry_id = (id_el.text or "").strip()
            title = (title_el.text or "").strip()
            href = link_el.get("href", "").strip()

            if not entry_id or not title or not href:
                continue

            # Parse timestamps
            published_utc = None
            if pub_el is not None and pub_el.text:
                published_utc = self.normalize_timestamp_to_utc_z(pub_el.text)

            updated_utc = None
            if upd_el is not None and upd_el.text:
                updated_utc = self.normalize_timestamp_to_utc_z(upd_el.text)

            modified_utc = None
            if modified_el is not None and modified_el.text:
                try:
                    from email.utils import parsedate_to_datetime

                    dt = parsedate_to_datetime(modified_el.text.strip())
                    dt_utc = dt.astimezone(timezone.utc)
                    modified_utc = (
                        dt_utc.replace(microsecond=0).isoformat().replace("+00:00", "Z")
                    )
                except Exception:
                    modified_utc = None

            # Collect subjects and keywords
            subjects = [
                sel.text.strip() for sel in subject_els if sel.text and sel.text.strip()
            ] or None
            keywords = [
                kel.text.strip() for kel in keyword_els if kel.text and kel.text.strip()
            ] or None

            # Categories with term and scheme
            category_els = entry.findall("atom:category", ns)
            categories = []
            for cel in category_els:
                term = cel.get("term")
                scheme = cel.get("scheme")
                if term:
                    categories.append((term, scheme))
            categories = categories or None

            # Content HTML
            content_el = entry.find("atom:content", ns)
            content_html = (
                content_el.text if content_el is not None and content_el.text else None
            )

            items.append(
                GlobeNewswireItem(
                    id=entry_id,
                    title=title,
                    url=href,
                    timestamp=published_utc,
                    updated_utc=updated_utc,
                    modified_utc=modified_utc,
                    identifier=identifier_el.text.strip()
                    if identifier_el is not None and identifier_el.text
                    else None,
                    language=lang_el.text.strip()
                    if lang_el is not None and lang_el.text
                    else None,
                    publisher=publisher_el.text.strip()
                    if publisher_el is not None and publisher_el.text
                    else None,
                    contributor=contrib_el.text.strip()
                    if contrib_el is not None and contrib_el.text
                    else None,
                    subjects=subjects,
                    keywords=keywords,
                    categories=categories,
                    content_html=content_html,
                )
            )

        return items


def run_poller(
    polling_interval_seconds: int | None = None,
    user_agent: str | None = None,
) -> None:
    """Run the Globe Newswire poller."""
    poller = GlobeNewswirePoller()
    poller.run(polling_interval_seconds, user_agent)


if __name__ == "__main__":
    run_poller()
