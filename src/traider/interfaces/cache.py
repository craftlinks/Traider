from __future__ import annotations

"""Cache interface module.

Defines an abstract interface for a persistent, size-limited, thread-safe cache.  A concrete
implementation (e.g. based on `diskcache`) must provide the persistence and
LRU/size-capping behaviour.

Design goals
============
1. Global Singleton – consumers should retrieve the cache via an application-level
   accessor rather than instantiating it directly.  That accessor is outside the
   scope of this interface; here we only define the behaviour of an individual
   cache instance.
2. Key–value semantics – while the primary use-case is the “have we seen this ID
   before?” membership test, supporting arbitrary *values* makes the cache more
   general-purpose and enables TTL / metadata storage.
3. Thread-safety – every method must be safe to call concurrently.  Most
   back-ends achieve this with internal locks or file locks; the interface
   treats this as an implementation detail.
"""

import contextlib
from abc import ABC, abstractmethod
from typing import Any, Iterable, Iterator, MutableMapping, Optional


class CacheInterface(ABC, MutableMapping[str, Any]):
    """Abstract base class for a persistent, size-limited cache.

    A concrete implementation must guarantee:
        * Thread-safety for all public methods.
        * Persistence across process restarts **unless** `clear()` has been
          called or the user opted-out via CLI flags.
        * Automatic eviction when the configured *item* limit (and/or *byte*
          limit) is exceeded.  The eviction strategy SHOULD be
          least-recent-used (LRU) but may be configurable.
    """

    # ---------------------------------------------------------------------
    # Basic Mapping API – subclasses MUST implement these.
    # ---------------------------------------------------------------------

    @abstractmethod
    def __getitem__(self, key: str) -> Any:  # noqa: Dunder method documented via ABC
        """Retrieve *value* for *key* or raise ``KeyError`` if missing."""

    @abstractmethod
    def __setitem__(self, key: str, value: Any) -> None:  # noqa: See class docstring
        """Insert or overwrite *key* with *value* (updates *recency*)."""

    @abstractmethod
    def __delitem__(self, key: str) -> None:
        """Remove *key* and its value from the cache."""

    @abstractmethod
    def __iter__(self) -> Iterator[str]:
        """Iterate over cache *keys* in **descending** recency order (MRU → …)."""

    @abstractmethod
    def __len__(self) -> int:
        """Return the current number of items in the cache (after evictions)."""

    # ------------------------------------------------------------------
    # Convenience helpers (non-abstract) – may be overridden for speed.
    # ------------------------------------------------------------------

    def add(self, key: str, value: Any | None = True) -> bool:
        """Add *key* only if it does not yet exist.

        Returns ``True`` if the key was inserted, ``False`` otherwise.  Useful
        for *seen-id* semantics where the *value* is irrelevant.
        """
        if key in self:
            return False
        self[key] = value
        return True

    def get_or_add(self, key: str, default: Any | None = True) -> Any:
        """Return existing value or insert *default* atomically and return it."""
        try:
            return self[key]
        except KeyError:
            self[key] = default
            return default

    # ------------------------------------------------------------------
    # Lifecycle management
    # ------------------------------------------------------------------

    @abstractmethod
    def close(self) -> None:
        """Flush pending writes and free underlying resources."""

    def clear(self) -> None:  # pylint: disable=arguments-differ
        """Remove **all** items from the cache (persisted empty)."""
        keys: list[str] = list(self.keys())
        for k in keys:
            del self[k]

    # ------------------------------------------------------------------
    # Context-manager sugar
    # ------------------------------------------------------------------

    def __enter__(self) -> "CacheInterface":  # noqa: Dunder method documented via ABC
        return self

    def __exit__(self, exc_type, exc, tb):  # noqa: D401
        # Persist on normal exit as well as when an exception bubbles.
        # The concrete implementation decides whether to swallow errors.
        with contextlib.suppress(Exception):
            self.close()
        return False  # never suppress exceptions
