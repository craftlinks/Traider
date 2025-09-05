from __future__ import annotations

from dataclasses import dataclass
from typing import Optional
import queue
import time

from .base_poller import BaseItem


@dataclass(frozen=True)
class NewsEvent:
    """Envelope for a news item emitted by a poller.

    Attributes:
        source: Logical name of the poller (e.g., "PR Newswire").
        item: The parsed item metadata.
        article_text: Optional extracted article/body text.
        received_at: Wall-clock timestamp when enqueued.
    """

    source: str
    item: BaseItem
    received_at: float = time.time()


class QueueSink:
    """Callable sink that enqueues NewsEvent objects into a Queue.

    By default it will block briefly when the queue is full to provide
    backpressure instead of dropping events.
    """

    def __init__(
        self, q: queue.Queue[NewsEvent], block_timeout_seconds: float = 0.25
    ) -> None:
        self._q = q
        self._timeout = max(0.0, float(block_timeout_seconds))

    def __call__(self, source: str, item: BaseItem) -> None:
        try:
            self._q.put(NewsEvent(source=source, item=item), timeout=self._timeout)
        except queue.Full:
            # Timed out while waiting for space; drop silently.
            pass
