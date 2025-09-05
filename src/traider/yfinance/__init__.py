# In traider/yfinance/__init__.py

"""Public interface for the Yahoo Finance scraping utilities."""

from ._models import Profile, EarningsEvent, PressRelease
from ._api import (
    initialize,
    get_profile,
    get_earnings,
    get_earnings_for_date_range,
    get_press_releases,
    get_latest_press_release,
    get_press_release_content,
)

# You can optionally also define __all__ here for maximum clarity
__all__ = [
    # --- Public Functions ---
    "initialize",
    "get_profile",
    "get_earnings",
    "get_earnings_for_date_range",
    "get_press_releases",
    "get_latest_press_release",
    "get_press_release_content",
    # --- Public Dataclasses (Return Types) ---
    "Profile",
    "EarningsEvent",
    "PressRelease",
]
