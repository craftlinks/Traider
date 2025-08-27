from __future__ import annotations

from typing import Any, Callable, Protocol, TypeVar

# Define a generic TypeVar for the item that a poller yields.
# This could be a BaseItem, an EarningsEvent, or any other dataclass/object.
ItemT = TypeVar("ItemT", covariant=True)

class Poller(Protocol[ItemT]):
    """
    A protocol defining the essential contract for any poller.

    A Poller is an object that can be run to periodically produce items of a
    specific type (`ItemT`). It can be configured with a 'sink' to send
    these items to a downstream consumer.
    """

    @property
    def name(self) -> str:
        """A unique name for the poller, used for logging and identification."""
        ...

    def set_sink(self, sink: Callable[[str, ItemT], Any]) -> None:
        """
        Register a sink callable to receive newly produced items.

        The sink will be called with (poller_name, item) for each new item.
        The sink's return value is ignored.
        """
        ...

    def run(self) -> None:
        """
        Start the poller's main execution loop.

        This method should block and run indefinitely until the process is
        terminated.
        """
        ...