from __future__ import annotations

"""Demo application showcasing how to combine Alpaca market data with
Interactive Brokers order execution.

Workflow:
1. Connect to Interactive Brokers TWS / Gateway (paper trading)
2. Fetch the latest AAPL quote from Alpaca to determine a reasonable limit
   price and BUY 10 shares of AAPL via IB.
3. Wait until the order is reported as filled.
4. Stream real-time AAPL trades & quotes from Alpaca for ~2 seconds.
5. Fetch another quote snapshot and SELL the 10 shares via a LIMIT order.
6. Gracefully shut down all connections.

Note
----
* This script assumes that the Interactive Brokers TWS / Gateway is running
  locally on port 7497 (paper trading). Adjust the host/port/client_id as
  needed.
* Alpaca API credentials must be available as environment variables
  `ALPACA_API_KEY` and `ALPACA_SECRET_KEY` or provided via a .env file.
"""

import logging
import time
import threading
from datetime import datetime
from dotenv import load_dotenv

from alpaca.data.enums import DataFeed

from traider.models import (
    Contract,
    Order,
    OrderAction,
    OrderType,
    TimeInForce,
    Trade,
    Quote,
)
from traider.platforms.brokers.interactive_brokers import InteractiveBrokersPlatform
from traider.platforms.market_data.alpaca import AlpacaMarketData
from traider.platforms.pollers import (
    AccessNewswirePoller,
    BusinessWirePoller,
    GlobeNewswirePoller,
    NewsroomPoller,
    PRNewswirePoller,
    SECPoller,
    PollerConfig,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def wait_for_order_fill(platform: InteractiveBrokersPlatform, order_id: int, timeout: float = 30.0) -> bool:
    """Wait for a specific order to fill using platform-native events."""
    did_fill = platform.wait_for_fill(order_id, timeout_seconds=timeout)
    status = platform.get_order_status(order_id)
    if did_fill:
        logger.info("Order %s filled.", order_id)
    else:
        logger.warning("Order %s not filled within %.0fs (last status: %s)", order_id, timeout, status)
    return did_fill


def run_pollers_in_background():
    """Initializes and runs all pollers in background threads."""
    poller_classes = [
        # AccessNewswirePoller,
        # BusinessWirePoller,
        # GlobeNewswirePoller,
        # NewsroomPoller,
        # PRNewswirePoller,
        SECPoller,
    ]

    threads = []
    for poller_cls in poller_classes:
        poller = poller_cls()

        thread = threading.Thread(target=poller.run, daemon=True)
        thread.start()
        threads.append(thread)
        logger.info("Started %s in a background thread.", poller_cls.__name__)

    return threads


# ---------------------------------------------------------------------------
# Main demo workflow
# ---------------------------------------------------------------------------

def main() -> None:
    """Entry-point for the demo."""

    load_dotenv()  # Load Alpaca credentials from .env if present

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    # Suppress noisy INFO logs from ibapi internals
    for noisy_logger in ("ibapi", "ibapi.utils", "ibapi.client", "ibapi.wrapper", "trader.platforms.interactive_brokers", "alpaca.data.live.websocket"):
        nl = logging.getLogger(noisy_logger)
        nl.setLevel(logging.WARNING)
        nl.propagate = False

    # Start news pollers in the background
    run_pollers_in_background()

    symbol = "AAPL"
    quantity = 10

    # Instantiate helpers ----------------------------------------------------
    market_data = AlpacaMarketData(feed=DataFeed.IEX)
    ib = InteractiveBrokersPlatform()

    # Connect to Interactive Brokers ----------------------------------------
    logger.info("Connecting to Interactive Brokers ...")
    ib.connect(host="127.0.0.1", port=7497, client_id=1)
    logger.info("Connected to IB.")

    try:
        # ------------------------------------------------------------------
        # BUY phase
        # ------------------------------------------------------------------
        quote_before = market_data.get_latest_quote(symbol)
        if quote_before is None:
            raise RuntimeError(f"Could not retrieve quote for {symbol} – aborting.")

        buy_limit = quote_before.ask_price  # Pay the current ask
        logger.info("Placing BUY LIMIT order for %d %s @ %.2f", quantity, symbol, buy_limit)

        contract = Contract(symbol=symbol)
        buy_order = Order(
            contract=contract,
            action=OrderAction.BUY,
            order_type=OrderType.LIMIT,
            quantity=quantity,
            time_in_force=TimeInForce.DAY,
            limit_price=buy_limit,
        )

        buy_order_id = ib.buy(contract, buy_order)
        wait_for_order_fill(ib, buy_order_id, timeout=30.0)

        # ------------------------------------------------------------------
        # Streaming snapshot
        # ------------------------------------------------------------------
        logger.info("Streaming live %s trades/quotes for 2 seconds ...", symbol)

        def on_trade(trade: Trade) -> None:
            logger.info(
                "TRADE %s | %.2f x %s",
                trade.timestamp.isoformat(timespec="seconds"),
                trade.price,
                trade.size,
            )

        def on_quote(quote: Quote) -> None:
            logger.info(
                "QUOTE %s | bid %.2f x %d / ask %.2f x %d",
                quote.timestamp.isoformat(timespec="seconds"),
                quote.bid_price,
                quote.bid_size,
                quote.ask_price,
                quote.ask_size,
            )

        market_data.subscribe_trades(symbol, on_trade)
        market_data.subscribe_quotes(symbol, on_quote)
        time.sleep(2.0)
        market_data.unsubscribe_trades(symbol)
        market_data.unsubscribe_quotes(symbol)

        # ------------------------------------------------------------------
        # SELL phase
        # ------------------------------------------------------------------
        quote_after = market_data.get_latest_quote(symbol)
        sell_limit = quote_after.bid_price if quote_after else buy_limit
        logger.info("Placing SELL LIMIT order for %d %s @ %.2f", quantity, symbol, sell_limit)

        sell_order = Order(
            contract=contract,
            action=OrderAction.SELL,
            order_type=OrderType.LIMIT,
            quantity=quantity,
            time_in_force=TimeInForce.DAY,
            limit_price=sell_limit,
        )

        sell_order_id = ib.sell(contract, sell_order)
        wait_for_order_fill(ib, sell_order_id, timeout=30.0)

    finally:
        # Clean-up -----------------------------------------------------------
        logger.info("Shutting down ...")
        # Close market data stream first
        market_data.close()

        # Cancel any remaining open orders before disconnecting
        try:
            remaining_open = ib.get_open_orders()
            if remaining_open:
                ids = [o.order_id for o in remaining_open if o.order_id is not None]
                logger.info("Canceling %d open order(s): %s", len(ids), ids)
                for order in remaining_open:
                    if order.order_id is not None:
                        ib.cancel_order(order.order_id)
                # Give IB a moment to process cancellations and verify
                time.sleep(1.0)
                still_open = ib.get_open_orders()
                if still_open:
                    logger.warning("Some orders still reported open after cancel: %s",
                                   [o.order_id for o in still_open if o.order_id is not None])
        except Exception as exc:
            logger.exception("Error during open order cancellation: %s", exc)

        ib.disconnect()
        logger.info("Done.")


if __name__ == "__main__":
    main()
