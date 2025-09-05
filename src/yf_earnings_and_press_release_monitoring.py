from traider.messagebus.protocol import MessageBroker
from traider.messagebus.brokers.memory import InMemoryBroker
from traider.messagebus.router import MessageRouter


msg_broker: MessageBroker = InMemoryBroker()
router = MessageRouter(msg_broker)

async def main():