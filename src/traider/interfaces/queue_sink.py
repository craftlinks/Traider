from __future__ import annotations

import queue
import asyncio
from typing import Generic, TypeVar

T = TypeVar("T")


class QueueSink(Generic[T]):
    """A callable sink that enqueues objects of type ``T`` into a ``queue.Queue``.

    The sink provides a thin wrapper around :pymod:`queue.Queue.put` that can be
    passed to producers/pollers which then simply call the sink with the object
    they wish to enqueue.  By parameterising the class with a :pydata:`~typing.TypeVar`,
    static type checkers (``mypy``, ``pyright``) can validate that producers and
    consumers agree on the payload type while the runtime code remains entirely
    agnostic of what *T* actually is.

    Parameters
    ----------
    q
        The target queue that will receive the objects.
    block_timeout_seconds
        Maximum time (in seconds) to block while the queue is full before the
        item is silently dropped.  Set to ``0`` or a negative value to perform a
        non-blocking ``put``.
    """

    def __init__(self, q: queue.Queue[T], block_timeout_seconds: float = 0.25) -> None:
        # Store a typed queue; the runtime object is the same as an untyped one.
        self._q: queue.Queue[T] = q
        # Ensure non-negative timeout.
        self._timeout: float = max(0.0, float(block_timeout_seconds))

    def __call__(self, *args: T) -> None:  # noqa: D401  (simple verb phrase is fine)
        """Enqueue an object into the underlying queue.

        The call signature is flexible in order to interoperate with different
        producer conventions:

        1. ``sink(obj)`` – legacy/stand-alone usage where the *obj* is provided
           directly.
        2. ``sink(poller_name, obj)`` – newer convention used by
           :pyclass:`~traider.platforms.pollers.common.base_poller.BasePoller`
           where the first argument is a *str* naming the poller.  In this case
           the *poller_name* is **ignored** because the queue consumer usually
           cares only about the actual payload object.
        """

        if not args:
            # Nothing to enqueue – silently ignore to match previous behaviour.
            return

        # Determine the payload depending on the provided arguments.
        if len(args) == 1:
            payload = args[0]
        elif len(args) == 2 and isinstance(args[0], str):
            # Signature (poller_name, obj) – ignore the name.
            payload = args[1]
        else:
            # Fallback: treat the *last* argument as payload to be generic.
            payload = args[-1]

        try:
            # ``queue.Queue`` already handles locking; we just forward the call.
            self._q.put(payload, timeout=self._timeout)
        except queue.Full:
            # If the queue is full and we time out, we drop the item silently to
            # avoid blocking the producer indefinitely.
            pass


# ---------------------------------------------------------------------------
# Asynchronous variant
# ---------------------------------------------------------------------------


class AsyncQueueSink(Generic[T]):
    """A callable sink that enqueues objects into an :class:`asyncio.Queue`.

    This mirrors :class:`QueueSink` but targets :class:`asyncio.Queue` and uses
    the non-blocking :py:meth:`asyncio.Queue.put_nowait` method so the producer
    is *never* blocked, even inside synchronous contexts.

    The flexible call signature allows the same usage patterns accepted by
    :class:`QueueSink`:

    1. ``sink(obj)`` – enqueue *obj* directly.
    2. ``sink(poller_name, obj)`` – ignore *poller_name* and enqueue *obj*.

    If the queue is full the item is silently dropped, matching the behaviour of
    the synchronous variant.
    """

    def __init__(self, q: asyncio.Queue[T]) -> None:
        self._q: asyncio.Queue[T] = q

    def __call__(self, *args: T) -> None:  # noqa: D401 – simple verb phrase is fine
        if not args:
            return

        if len(args) == 1:
            payload = args[0]
        elif len(args) == 2 and isinstance(args[0], str):
            payload = args[1]  # type: ignore[assignment]
        else:
            payload = args[-1]  # type: ignore[assignment]

        try:
            self._q.put_nowait(payload)  # type: ignore[arg-type]
        except asyncio.QueueFull:
            # Drop silently if queue is at capacity.
            pass
