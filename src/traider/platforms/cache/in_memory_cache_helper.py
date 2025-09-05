from collections import OrderedDict


class FixedSizeLRUSet:
    """A lightweight fixed-size *LRU* set.

    Maintains *O(1)* membership checks and enforces an upper bound on the
    number of stored elements by evicting the least-recently *seen* entry once
    the capacity is exceeded.
    """

    __slots__ = ("_store", "_max_items")

    def __init__(self, max_items: int = 10_000):
        if max_items <= 0:
            raise ValueError("max_items must be positive")
        self._store: OrderedDict[str, None] = OrderedDict()
        self._max_items = max_items

    def __contains__(self, item: str) -> bool:  # noqa: D401
        return item in self._store

    def add(self, item: str) -> bool:
        """Add *item*, returning *False* if it was new, *True* otherwise."""

        if item in self._store:
            # Refresh recency
            self._store.move_to_end(item)
            return True

        self._store[item] = None
        if len(self._store) > self._max_items:
            # Evict the oldest entry (FIFO order of OrderedDict)
            self._store.popitem(last=False)
        return False


__all__: list[str] = ["FixedSizeLRUSet"]
