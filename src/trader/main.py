import logging
import time
from decimal import Decimal

from trader.models import Contract, Order, OrderAction, OrderType, SecurityType
from trader.interfaces.trading_platform import TradingPlatform
from trader.platforms.interactive_brokers import InteractiveBrokersPlatform

def main():
    """
    A simple application to test the InteractiveBrokersPlatform.
    """
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Instantiate the platform
    ib_platform: TradingPlatform = InteractiveBrokersPlatform()

    # Connection details for a paper trading account
    host = "127.0.0.1"
    port = 4002  # 7497 for TWS Paper, 4002 for IBG Paper
    client_id = 1

    # Connect to the platform
    ib_platform.connect(host, port, client_id)

    # Allow time for connection to be established
    logging.info("Connecting...")
    time.sleep(3) 

    # Get and print the account summary
    logging.info("Fetching account summary...")
    account_summary = ib_platform.get_account_summary()
    logging.info("Account Summary: %s", account_summary)

    # Place a buy order
    logging.info("Placing a buy order...")
    contract = Contract(symbol="AAPL", sec_type=SecurityType.STOCK, currency="USD", exchange="SMART")
    order = Order(action=OrderAction.BUY, order_type=OrderType.MARKET, quantity=Decimal("1"))
    ib_platform.buy(contract, order)
    logging.info("Buy order placed.")

    time.sleep(5)  # Allow time for order to be processed

    # Place a sell order
    logging.info("Placing a sell order...")
    contract = Contract(symbol="AAPL", sec_type=SecurityType.STOCK, currency="USD", exchange="SMART")
    order = Order(action=OrderAction.SELL, order_type=OrderType.MARKET, quantity=Decimal("1"))
    ib_platform.sell(contract, order)
    logging.info("Sell order placed.")

    time.sleep(5)  # Allow time for order to be processed

    # Disconnect from the platform
    ib_platform.disconnect()
    logging.info("Disconnected.")

if __name__ == "__main__":
    main()
