import asyncio
import signal
from traider.messagebus.protocol import MessageBroker
from traider.messagebus.brokers.memory import InMemoryBroker
from traider.messagebus.router import MessageRouter


msg_broker: MessageBroker = InMemoryBroker()
router = MessageRouter(msg_broker)

async def main():
    shutdown_event = asyncio.Event()

    # Handle graceful shutdown via SIGINT / SIGTERM.
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, shutdown_event.set)