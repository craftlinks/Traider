import asyncio
import logging
# ---------------------------------------------------------------------------
# typing imports
# ---------------------------------------------------------------------------
from typing import Callable, Optional, Any, Coroutine, cast

from .protocol import MessageBroker
from .channels import Channel

logger = logging.getLogger(__name__)

# Handler signature is flexible: first arg is the message, followed by any
# additional contextual parameters (shutdown_event, startup_barrier, ...).
# It may return an optional message to be forwarded.
MessageHandler = Callable[..., Coroutine[Any, Any, Optional[Any]]]

class MessageRouter:
    """
    The main orchestrator for defining and running a message-driven application.
    """
    def __init__(self, broker: MessageBroker):
        self.broker = broker
        # Registry contains tuples of input channel, message handler(co-routine), and output channel(optional)
        self._registry: list[tuple[Channel, MessageHandler, Optional[Channel]]] = []
        self._producers: list[Callable[..., Coroutine[Any, Any, Any]]] = []
        self._shutdown_event: Optional[asyncio.Event] = None
        self._startup_barrier: Optional[asyncio.Barrier] = None
        self._extra_args: Any = ()

    def route(self, listen_to: Channel, publish_to: Optional[Channel] = None) -> Callable[[MessageHandler], MessageHandler]:
        """Decorator to register a function as a processing node in the data flow."""
        def decorator(func: MessageHandler) -> MessageHandler:
            self._registry.append((listen_to, func, publish_to))
            return func
        return decorator

    # ---------------------------------------------------------------------
    # Producer decorator
    # ---------------------------------------------------------------------

    def producer(self) -> Callable[[Callable[..., Coroutine[Any, Any, Any]]], Callable[..., Coroutine[Any, Any, Any]]]:
        """Decorator to register an async function as a producer-only node.

        The decorated coroutine receives the broker as its first argument so it
        can publish messages freely. Additional *extra_args, shutdown_event,
        etc. are injected the same way as for normal handlers.
        """

        def decorator(func: Callable[..., Coroutine[Any, Any, Any]]):
            self._producers.append(func)
            return func

        return decorator

    async def _run_worker(self, listen_to: Channel, handler: MessageHandler, publish_to: Optional[Channel]):
        queue = await self.broker.subscribe(listen_to)
        # Signal readiness if a startup barrier is in use.
        if self._startup_barrier is not None:
            await self._startup_barrier.wait()
        while True:
            try:
                message = await asyncio.wait_for(queue.get(), timeout=1)
                if self._shutdown_event is not None:
                    result = await handler(message, self._shutdown_event, *self._extra_args)
                else:
                    result = await handler(message, *self._extra_args)
                if result is not None and publish_to is not None:
                    await self.broker.publish(publish_to, cast(Any, result))
            except asyncio.TimeoutError:
                # No message arrived within timeout. If a shutdown has been
                # signalled, exit the worker.
                if self._shutdown_event is not None and self._shutdown_event.is_set():
                    break
                continue
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception(f"Error in handler for channel '{listen_to.value}'")

    async def run(self, shutdown_event: asyncio.Event | None = None, startup_barrier: asyncio.Barrier | None = None, *extra_args: Any):
        # Store extra args so _run_worker can inject them into every handler
        self._extra_args = extra_args
        self._shutdown_event = shutdown_event
        self._startup_barrier = startup_barrier
        if not self._registry:
            logger.warning("No routes registered. Nothing to do.")
            return
        async with asyncio.TaskGroup() as tg:
            # Start subscribers (workers)
            for listen_to, handler, publish_to in self._registry:
                tg.create_task(self._run_worker(listen_to, handler, publish_to))

            # Start producers
            for producer in self._producers:
                tg.create_task(producer(self.broker, self._shutdown_event, *self._extra_args))