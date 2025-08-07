import time
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from trader.interfaces.trading_platform import TradingPlatform
from trader.platforms.interactive_brokers import InteractiveBrokersPlatform

def main():
    """
    A simple application to test the InteractiveBrokersPlatform.
    """
    # Instantiate the platform
    ib_platform: TradingPlatform = InteractiveBrokersPlatform()

    # Connection details for a paper trading account
    host = "127.0.0.1"
    port = 4002  # 7497 for TWS Paper, 4002 for IBG Paper
    client_id = 1

    # Connect to the platform
    ib_platform.connect(host, port, client_id)

    # Allow time for connection to be established
    print("Connecting...")
    time.sleep(3) 

    # Get and print the account summary
    print("Fetching account summary...")
    account_summary = ib_platform.get_account_summary()
    print("Account Summary:", account_summary)

    # Disconnect from the platform
    ib_platform.disconnect()
    print("Disconnected.")


if __name__ == "__main__":
    main()
