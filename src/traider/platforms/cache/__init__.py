"""Caching backends package.

Currently provides a DiskCache-based persistent, size-bounded implementation that
conforms to :class:`traider.interfaces.cache.CacheInterface`.

Additional back-ends can be added here later (e.g. Redis, in-memory, etc.).
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Optional

from traider.interfaces.cache import CacheInterface


# Module-level singleton ----------------------------------------------------

_shared_cache: Optional[CacheInterface] = None

_parsed_cli = False


def _apply_cli_flags_once() -> None:
    """Inspect ``sys.argv`` for cache-related flags and translate to env vars."""
    global _parsed_cli  # pylint: disable=global-statement
    if _parsed_cli:
        return

    mapping = {
        "--no-cache": "TRAIDER_NO_CACHE",
        "--clear-cache": "TRAIDER_CLEAR_CACHE",
    }

    remaining_args: list[str] = []
    for arg in sys.argv:
        if arg in mapping:
            os.environ[mapping[arg]] = "1"
        else:
            remaining_args.append(arg)

    # Optionally mutate sys.argv to remove handled flags so that downstream
    # argparse parsers ignore them.
    sys.argv[:] = remaining_args
    _parsed_cli = True


def get_shared_cache() -> CacheInterface:  # noqa: D401
    """Return process-wide shared cache instance.

    The first call initialises a :class:`DiskCacheBackend` in
    ``$HOME/.traider_cache`` (overridable via *TRAIDER_CACHE_DIR* env var).
    A maximum of 50 000 items is enforced by default but can be changed by
    setting *TRAIDER_CACHE_MAX_ITEMS*.

    Environment flags
    -----------------
    TRAIDER_NO_CACHE
        If set to ``1|true|yes``, returns a dummy in-memory cache that is not
        persisted and imposes no size limit.
    TRAIDER_CLEAR_CACHE
        If set, the cache directory is wiped at startup (before returning the
        instance).
    """

    global _shared_cache  # pylint: disable=global-statement
    if _shared_cache is not None:
        return _shared_cache

    _apply_cli_flags_once()

    def _truthy(env_value: str | None) -> bool:
        return str(env_value).lower() in {"1", "true", "yes", "y"}

    if _truthy(os.getenv("TRAIDER_NO_CACHE")):
        from collections import OrderedDict

        class _InMemoryCache(CacheInterface):
            """Non-persistent fallback cache."""

            def __init__(self, max_items: int | None = None):
                self._data: OrderedDict[str, object] = OrderedDict()
                self._max_items = max_items

            # Mapping ---------------------------------------------------
            def __getitem__(self, key: str):
                return self._data[key]

            def __setitem__(self, key: str, value):  # type: ignore[override]
                self._data.pop(key, None)
                self._data[key] = value
                if self._max_items and len(self._data) > self._max_items:
                    self._data.popitem(last=False)

            def __delitem__(self, key: str):
                del self._data[key]

            def __iter__(self):  # type: ignore[override]
                return reversed(list(self._data.keys()))

            def __len__(self):
                return len(self._data)

            # Lifecycle
            def close(self):  # noqa: D401
                self._data.clear()

        _shared_cache = _InMemoryCache(max_items=int(os.getenv("TRAIDER_CACHE_MAX_ITEMS", "50000")))
        return _shared_cache

    # Persistent DiskCacheBackend ---------------------------------------
    cache_dir = Path(os.getenv("TRAIDER_CACHE_DIR", str(Path.home() / ".traider_cache"))).expanduser()
    max_items = int(os.getenv("TRAIDER_CACHE_MAX_ITEMS", "50000"))
    size_limit_env = os.getenv("TRAIDER_CACHE_SIZE_LIMIT")
    size_limit = int(size_limit_env) if size_limit_env is not None else None

    from .disk_cache_backend import DiskCacheBackend

    cache = DiskCacheBackend(directory=cache_dir, max_items=max_items, size_limit=size_limit)

    if _truthy(os.getenv("TRAIDER_CLEAR_CACHE")):
        cache.clear()

    _shared_cache = cache
    return _shared_cache

from .disk_cache_backend import DiskCacheBackend

__all__: list[str] = [
    "DiskCacheBackend",
]
