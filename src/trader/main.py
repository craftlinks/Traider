import logging
import time

from trader.models import Contract, Order, OrderAction, OrderType, SecurityType
from trader.interfaces.trading_platform import TradingPlatform
from trader.platforms.interactive_brokers import InteractiveBrokersPlatform

def main():
    """
    A simple application to test the InteractiveBrokersPlatform.
    """
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.getLogger('ibapi.utils').setLevel(logging.WARNING)

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

    # Check for open orders
    logging.info("Checking for open orders...")
    open_orders = ib_platform.get_open_orders()
    if open_orders:
        logging.info("Found open orders: %s", open_orders)
        for order in open_orders:
            if order.order_id:
                logging.info("Canceling order %s", order.order_id)
                ib_platform.cancel_order(order.order_id)
        logging.info("Canceled all open orders.")
        time.sleep(3) # Give some time for the cancellations to process
    else:
        logging.info("No open orders found.")

    # Get and print the account summary
    logging.info("Fetching account summary...")
    account_summary = ib_platform.get_account_summary()
    logging.info("Account Summary: %s", account_summary)

    # Place a buy order
    logging.info("Placing a buy order...")
    contract = Contract(symbol="AAPL", sec_type=SecurityType.STOCK, currency="USD", exchange="SMART")
    order = Order(contract=contract, action=OrderAction.BUY, order_type=OrderType.MARKET, quantity=1.0, outside_rth=False)
    ib_platform.buy(contract, order)
    logging.info("Buy order placed.")

    time.sleep(5)  # Allow time for order to be processed

    # Modify the order
    logging.info("Modifying the buy order...")
    open_orders = ib_platform.get_open_orders()
    if open_orders:
        for o in open_orders:
            if o.order_id and o.action == OrderAction.BUY:
                logging.info("Modifying order %s", o.order_id)
                o.outside_rth = True
                ib_platform.modify_order(o.order_id, o)
        logging.info("Order modified.")
    else:
        logging.info("No open orders found to modify.")

        time.sleep(5)  # Allow time for order to be processed

    # Place a sell order
    logging.info("Placing a sell order...")
    contract = Contract(symbol="AAPL", sec_type=SecurityType.STOCK, currency="USD", exchange="SMART")
    order = Order(contract=contract, action=OrderAction.SELL, order_type=OrderType.MARKET, quantity=1.0, outside_rth=False)
    ib_platform.sell(contract, order)
    logging.info("Sell order placed.")

    time.sleep(5)  # Allow time for order to be processed

    # Modify the sell order
    logging.info("Modifying the sell order...")
    open_orders = ib_platform.get_open_orders()
    if open_orders:
        for o in open_orders:
            if o.order_id and o.action == OrderAction.SELL:
                logging.info("Modifying order %s", o.order_id)
                o.outside_rth = True
                ib_platform.modify_order(o.order_id, o)
        logging.info("Order modified.")
    else:
        logging.info("No open orders found to modify.")

    time.sleep(20)  # Allow time for order to be processed

    # Disconnect from the platform
    ib_platform.disconnect()
    logging.info("Disconnected.")

if __name__ == "__main__":
    main()

