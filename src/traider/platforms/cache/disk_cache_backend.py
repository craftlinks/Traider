from __future__ import annotations

"""Disk-backed implementation of :class:`traider.interfaces.cache.CacheInterface`.

This class is a thin adapter around the excellent `diskcache.Cache` which adds

* maximum *item* count control in addition to the underlying *byte* size limit
* conformance to our abstract `CacheInterface` contract

A single instance can be shared safely across **threads** and **processes**.

Example
-------
```python
from traider.platforms.cache import DiskCacheBackend
cache = DiskCacheBackend(directory="~/.traider_cache", max_items=50_000)
if cache.add("ABC-20250101-001"):
    process_item()
...
cache.close()
```
"""

import atexit
import contextlib
from pathlib import Path
from typing import Any, Iterator, Optional

from diskcache import Cache as _DiskCache

from traider.interfaces.cache import CacheInterface

__all__: list[str] = ["DiskCacheBackend"]


class DiskCacheBackend(CacheInterface):
    """Persistent LRU cache based on *diskcache*.

    Parameters
    ----------
    directory:
        Directory where cache files live.  Will be created if missing.
    max_items:
        Optional hard limit on the **number** of items.  When *None*, only the
        *size_limit* of the underlying `diskcache.Cache` applies (bytes).
    size_limit:
        Maximum on-disk **byte** size.  Passed straight to `diskcache.Cache`.
    cull_limit:
        Controls how aggressively `diskcache` removes items when the size limit
        is exceeded.  Same semantics as upstream (default: ``10``).
    """

    def __init__(
        self,
        directory: str | Path = "cache",
        *,
        max_items: Optional[int] = None,
        size_limit: Optional[int] = None,
        cull_limit: int = 10,
    ) -> None:
        directory = Path(directory).expanduser().absolute()
        directory.mkdir(parents=True, exist_ok=True)

        # If `size_limit` is `None`, default to a very large number so that item
        # count (max_items) becomes the primary eviction mechanism.
        _size_limit = int(size_limit) if size_limit is not None else 2**63 - 1

        self._cache = _DiskCache(directory, size_limit=_size_limit, cull_limit=cull_limit)
        self._max_items = max_items
        # Ensure the cache is flushed on interpreter exit (best-effort)
        atexit.register(self.close)

    # ------------------------------------------------------------------
    # Mapping interface
    # ------------------------------------------------------------------

    def __getitem__(self, key: str) -> Any:  # noqa: D401
        return self._cache[key]

    def __setitem__(self, key: str, value: Any) -> None:  # noqa: D401
        self._cache[key] = value
        # Enforce item-count limit (if configured) using LRU strategy.
        # pyright's stubs for `diskcache` mark `__len__` as returning `Unknown`,
        # so we silence the type checker for the `len()` calls below.
        if self._max_items is not None and len(self._cache) > self._max_items:  # type: ignore[arg-type]
            # Pop least-recently used items until within limit.
            while len(self._cache) > self._max_items:  # type: ignore[arg-type]
                # Remove least-recently-used key (first key returned by
                # `iterkeys()`)
                oldest_key = next(self._cache.iterkeys())  # type: ignore[attr-defined]
                del self._cache[oldest_key]

    def __delitem__(self, key: str) -> None:  # noqa: D401
        del self._cache[key]

    def __iter__(self) -> Iterator[str]:  # noqa: D401
        # diskcache iterates over keys in LRU → MRU order by default.  We reverse
        # to get MRU → LRU as required by the interface.
        return reversed(list(self._cache.iterkeys()))  # type: ignore[arg-type]

    def __len__(self) -> int:  # noqa: D401
        return len(self._cache)  # type: ignore[arg-type]

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def close(self) -> None:  # noqa: D401
        """Flush to disk and close underlying resources."""
        if hasattr(self, "_cache") and self._cache is not None:  # pragma: no cover
            with contextlib.suppress(Exception):
                self._cache.close()
