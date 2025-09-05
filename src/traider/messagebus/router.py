# ---------------------------------------------------------------------------
# Standard library imports
# ---------------------------------------------------------------------------
import asyncio
import logging
from datetime import timedelta
import signal
# ---------------------------------------------------------------------------
# typing imports
# ---------------------------------------------------------------------------
from typing import Callable, Optional, Any, Coroutine, cast
from typing import Tuple

from .protocol import MessageBroker
from .channels import Channel

logger = logging.getLogger(__name__)

# Handler signature is flexible: first arg is the message, followed by any
# additional contextual parameters (shutdown_event, startup_barrier, ...).
# It may return an optional message to be forwarded.
MessageHandler = Callable[..., Coroutine[Any, Any, Optional[Any]]]
ProducerHandler = Callable[..., Coroutine[Any, Any, Any]]

class MessageRouter:
    """
    The main orchestrator for defining and running a message-driven application.
    """
    def __init__(self, broker: MessageBroker):
        self.broker = broker
        # Registry tuples: (listen_channel, handler, publish_channel, ttl)
        self._registry: list[tuple[Channel, MessageHandler, Optional[Channel], Optional[float | int | timedelta]]] = []
        self._producers: list[Tuple[ProducerHandler, Optional[Channel], Optional[float | int | timedelta]]] = []
        self._shutdown_event: Optional[asyncio.Event] = None
        # Internal barrier used to coordinate initial startup of all nodes.
        self._startup_barrier: Optional[asyncio.Barrier] = None  # not exposed to user
        self._extra_args: Any = ()
        # Event that is set once all initially registered nodes have passed the
        # startup barrier and the system is ready to process/publish messages.
        self._ready_event: asyncio.Event = asyncio.Event()
        # Reference to the TaskGroup created in `run()`. This allows workers
        # to spawn additional background tasks that will be tied to the same
        # lifecycle (i.e. they are cancelled automatically on shutdown).
        self._tg: Optional[asyncio.TaskGroup] = None

    # ------------------------------------------------------------------
    # Public helpers
    # ------------------------------------------------------------------

    @property
    def worker_count(self) -> int:
        """Number of subscriber workers registered (excluding producers)."""

        return len(self._registry)

    @property
    def producer_count(self) -> int:
        """Number of producer-only nodes registered."""

        return len(self._producers)

    @property
    def node_count(self) -> int:
        """Total number of nodes (workers + producers) managed by the router."""

        return self.worker_count + self.producer_count

    def route(
        self,
        listen_to: Optional[Channel] = None,
        publish_to: Optional[Channel] = None,
        *,
        ttl: float | int | timedelta | None = None,
    ) -> Callable[[Callable[..., Coroutine[Any, Any, Any]]], Callable[..., Coroutine[Any, Any, Any]]]:
        """Decorate a coroutine as a subscriber or producer.

        • If *listen_to* is provided, the function is treated as a subscriber
          (as before).
        • If *listen_to* is None and *publish_to* is provided, the function is
          treated as a producer-only node; its return value (if not None) will
          be published to *publish_to*.
        """

        def decorator(func: Callable[..., Coroutine[Any, Any, Any]]):
            if listen_to is None:
                if publish_to is None:
                    raise ValueError("Producer route requires publish_to channel")
                self._producers.append((func, publish_to, ttl))
            else:
                self._registry.append((listen_to, func, publish_to, ttl))
            return func
        return decorator

    async def _run_worker(self, listen_to: Channel, handler: MessageHandler, publish_to: Optional[Channel]):
        queue = await self.broker.subscribe(listen_to)
        # Signal readiness if a startup barrier is in use.
        if self._startup_barrier is not None:
            await self._startup_barrier.wait()
            # First coroutine after barrier trips sets readiness flag.
            if not self._ready_event.is_set():
                self._ready_event.set()
        while True:
            try:
                message = await asyncio.wait_for(queue.get(), timeout=1)
                if self._shutdown_event is not None:
                    result = await handler(self, message, *self._extra_args)
                else:
                    result = await handler(self, message, *self._extra_args)
                if result is not None and publish_to is not None:
                    await self.broker.publish(publish_to, result)
            except asyncio.TimeoutError:
                # No message arrived within timeout. If a shutdown has been
                # signalled, exit the worker.
                if self._shutdown_event is not None and self._shutdown_event.is_set():
                    break
                continue
            except asyncio.CancelledError:
                logger.debug(f"Worker for channel '{listen_to.value}' cancelled")
                break
            except Exception:
                logger.exception(f"Error in handler for channel '{listen_to.value}'")

    async def run(
        self,
        shutdown_event: asyncio.Event | None = None,
        *extra_args: Any,
        install_signal_handlers: bool = True,
    ) -> None:
        # Store extra args so _run_worker can inject them into every handler
        self._extra_args = extra_args
        # Create a shutdown event if none provided.
        self._shutdown_event = shutdown_event or asyncio.Event()

        # Optionally handle SIGINT / SIGTERM automatically.
        if install_signal_handlers:
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGINT, signal.SIGTERM):
                try:
                    loop.add_signal_handler(sig, self._shutdown_event.set)
                except (NotImplementedError, RuntimeError):
                    # Some platforms or running in threads may not allow it.
                    pass

        # Create an internal barrier covering all *registered* nodes (workers + producers)
        self._startup_barrier = asyncio.Barrier(parties=self.node_count) if self.node_count > 0 else None

        if not self._registry and not self._producers:
            logger.warning("No routes registered. Nothing to do.")
            return
        async with asyncio.TaskGroup() as tg:
            loop = asyncio.get_running_loop()
            # Expose the TaskGroup so that workers/producers can create
            # child-tasks that are properly supervised.
            self._tg = tg

            # Start subscribers (workers)
            for listen_to, handler, publish_to, ttl in self._registry:
                task = tg.create_task(self._run_worker(listen_to, handler, publish_to))
                if ttl is not None:
                    seconds = ttl.total_seconds() if isinstance(ttl, timedelta) else float(ttl)
                    loop.call_later(seconds, task.cancel)
            # Start producers
            for producer, out_channel, ttl in self._producers:
                task = tg.create_task(self._run_producer(producer, out_channel))
                if ttl is not None:
                    seconds = ttl.total_seconds() if isinstance(ttl, timedelta) else float(ttl)
                    loop.call_later(seconds, task.cancel)

            # If there is no startup barrier, the system is ready immediately.
            if self._startup_barrier is None:
                self._ready_event.set()

        # Clear reference once the TaskGroup exits (either normally or via error)
        self._tg = None

    # ------------------------------------------------------------------
    # Readiness helpers
    # ------------------------------------------------------------------

    async def wait_until_ready(self) -> None:
        """Block until the router's initial startup is complete."""

        await self._ready_event.wait()

    # ------------------------------------------------------------------
    # Shutdown helper
    # ------------------------------------------------------------------

    def request_shutdown(self) -> None:
        """Signal graceful shutdown to all router tasks."""

        if self._shutdown_event is not None:
            self._shutdown_event.set()

    # ------------------------------------------------------------------
    # Public API for dynamic task creation
    # ------------------------------------------------------------------

    def spawn_task(
        self,
        coro: Coroutine[Any, Any, Any],
        *,
        ttl: float | int | timedelta | None = None,
    ) -> asyncio.Task[Any]:
        """Schedule *coro* in the router's TaskGroup with optional lifetime.

        Parameters
        ----------
        coro
            Coroutine to execute.
        ttl
            Maximum lifetime. If *None* the task runs until completion or
            router shutdown.  If a number, interpreted as seconds.  A
            ``datetime.timedelta`` is also accepted.
        """

        if self._tg is None:
            raise RuntimeError("MessageRouter is not running – cannot spawn tasks")

        task = self._tg.create_task(coro)

        if ttl is not None:
            # Convert to float seconds
            if isinstance(ttl, timedelta):
                ttl_seconds = ttl.total_seconds()
            else:
                ttl_seconds = float(ttl)

            loop = asyncio.get_running_loop()

            def _cancel_task(t: asyncio.Task[Any] = task):
                if not t.done():
                    t.cancel()

            loop.call_later(ttl_seconds, _cancel_task)

        return task

    # ---------------------------------------------------------------------
    # Internal helpers
    # ---------------------------------------------------------------------

    async def _run_producer(
        self,
        producer_fn: Callable[..., Coroutine[Any, Any, Any]],
        publish_to: Optional[Channel],
    ) -> None:
        """Run a producer coroutine and forward its (non-None) result."""

        # Ensure producers also participate in the startup synchronisation so
        # the barrier trips once *all* initial nodes are ready.
        if self._startup_barrier is not None:
            await self._startup_barrier.wait()
            if not self._ready_event.is_set():
                self._ready_event.set()

        result = await producer_fn(
            self,  # pass router for convenience
            *self._extra_args,
        )

        # Producer waited on startup_barrier inside itself; ensure ready_event is set.
        if self._startup_barrier is not None and not self._ready_event.is_set():
            self._ready_event.set()
        if result is not None and publish_to is not None:
            await self.broker.publish(publish_to, result)