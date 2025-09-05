import asyncio
from datetime import date
from enum import Enum
from traider.messagebus.protocol import MessageBroker
from traider.messagebus.brokers.memory import InMemoryBroker
from traider.messagebus.router import MessageRouter

import traider.yfinance as yf
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Channel(str, Enum):
    EARNINGS = "earnings"
    PRESS_RELEASE = "press_release"


msg_broker: MessageBroker = InMemoryBroker()
router = MessageRouter(msg_broker)


EARNINGS_PRODUCER_INTERVAL = 60*60


@router.route(publish_to=Channel.EARNINGS)
async def earnings_producer(router: MessageRouter):
    await router.wait_until_ready()
    try:
        while True:
            earnings: list[yf.EarningsEvent] = await yf.get_earnings(date.today())
            for earning in earnings:
                await router.broker.publish(Channel.EARNINGS, earning)
            await asyncio.sleep(EARNINGS_PRODUCER_INTERVAL)
    except asyncio.CancelledError:
        logger.info("Earnings producer cancelled")
        raise asyncio.CancelledError
    except Exception as e:
        logger.error(f"Error getting earnings: {e}")
        raise e
    



@router.route(listen_to=Channel.EARNINGS)
async def earnings_consumer(router: MessageRouter, earning: yf.EarningsEvent):
    try:
        logger.info(f"Received earnings event for {earning.ticker} at {earning.earnings_call_time}")
    except asyncio.CancelledError:
        logger.info("Earnings consumer cancelled")
        raise asyncio.CancelledError
    except Exception as e:
        logger.error(f"Error getting earnings: {e}")
        raise e


async def main():
    async with asyncio.TaskGroup() as tg:
        tg.create_task(router.run())

if __name__ == "__main__":
    asyncio.run(main())