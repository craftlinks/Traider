"""Common utilities and base classes for pollers."""

from .base_poller import BasePoller, BaseItem, PollerConfig
from .poller_utils import (
    build_session,
    strip_tags,
    extract_primary_text_from_html,
    filter_new_items,
    ThrottledHTTPAdapter,
)
from .specialized_pollers import (
    FeedPoller,
    AtomFeedPoller,
    RSSFeedPoller,
    HTMLPoller,
    APIPoller,
)

__all__ = [
    # Base classes
    "BasePoller",
    "BaseItem", 
    "PollerConfig",
    # Utilities
    "build_session",
    "strip_tags",
    "extract_primary_text_from_html",
    "filter_new_items",
    "ThrottledHTTPAdapter",
    # Specialized pollers
    "FeedPoller",
    "AtomFeedPoller",
    "RSSFeedPoller", 
    "HTMLPoller",
    "APIPoller",
]