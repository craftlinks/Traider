import datetime
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.ticktype import TickTypeEnum
from ibapi.contract import ComboLeg
from ibapi.tag_value import TagValue

import time
import threading
import argparse

class IBApp(EClient, EWrapper):
  def __init__(self):
    EClient.__init__(self, self)

  def connectAck(self):
    print("Connected to TWS")
    print('--------------------------------')
  
  def nextValidId(self, orderId):
    self.orderId = orderId
  
  def nextId(self):
    self.orderId += 1
    return self.orderId

  def error(self, reqId, errorTime, errorCode, errorString, advancedOrderRejectJson=""):
    print(f"reqId: {reqId}, errorCode: {errorCode}, errorString: {errorString}, orderReject: {advancedOrderRejectJson}")

  def contractDetails(self, reqId, contractDetails: ContractDetails):
    contract_details = vars(contractDetails)
    print("\n".join(f"{name}: {value}" for name,value in contract_details.items()))
    # print(contractDetails.contract)

  def contractDetailsEnd(self, reqId):
    print("End of contract details")
    self.disconnect()

  def openOrder(self, orderId, contract, order, orderState):
    print(f"Open order: {orderId}, contract: {contract}, order: {order}, orderState: {orderState}")

    print('--------------------------------')
  
  def orderStatus(self, orderId, status, filled, remaining, avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice):
    print(f"Order status: {orderId}, status: {status}, filled: {filled}, remaining: {remaining}, avgFillPrice: {avgFillPrice}, permId: {permId}, parentId: {parentId}, lastFillPrice: {lastFillPrice}, clientId: {clientId}, whyHeld: {whyHeld}, mktCapPrice: {mktCapPrice}")

    print('--------------------------------')

  def execDetails(self, reqId, contract, execution):
    print(f"Exec details: {reqId}, contract: {contract}, execution: {execution}")

    print('--------------------------------')


  def currentTime(self, time):
    print(f"Current time: {datetime.datetime.fromtimestamp(time)}")


def parse_arguments():
    """Parse command line arguments for platform selection."""
    parser = argparse.ArgumentParser(description='Interactive Brokers API Client')
    
    # Define platform choices with their corresponding ports
    platforms = {
        'tws_live': 7496,
        'tws_paper': 7497,
        'ibg_live': 4001,
        'ibg_paper': 4002
    }
    
    parser.add_argument(
        '--platform', 
        choices=platforms.keys(),
        default='ibg_paper',
        help='Platform to connect to. choose from: tws_live, tws_paper, ibg_live, ibg_paper (default: ibg_paper)'
    )
    
    args = parser.parse_args()
    return platforms[args.platform]



# Future
# mycontract.symbol = "ES"
# mycontract.secType = "FUT"
# mycontract.currency = "USD"
# mycontract.exchange = "CME"
# mycontract.lastTradeDateOrContractMonth = 202412

# Option
# mycontract.symbol = "SPX"
# mycontract.secType = "OPT"
# mycontract.currency = "USD"
# mycontract.exchange = "SMART"
# mycontract.lastTradeDateOrContractMonth = 202412
# mycontract.right = "P"
# mycontract.tradingClass = "SPXW"
# mycontract.strike = 5300




def main():

  # Parse command line arguments and get the port
  port = parse_arguments()

  app = IBApp()
  app.connect("127.0.0.1", port, 0) # Requires IB Gateway/TWS to be running on your local machine
  if not app.isConnected():
    print("Failed to connect to TWS")
    return
  threading.Thread(target=app.run).start()
  time.sleep(1)
  print("Connected to TWS")
  print('--------------------------------')

  mycontract = Contract()
  # Stock
  mycontract.symbol = "AAPL"
  mycontract.secType = "STK" # Stock
  mycontract.currency = "USD"
  mycontract.exchange = "SMART" # or "NASDAQ" SMART MaxRebate, SMART PreferRebate, SMART VRebate (fastest, higher fee)
  mycontract.primaryExchange = "NASDAQ"

  # app.reqContractDetails(app.nextId(), mycontract)
  # app.reqCurrentTime()

  # app.reqMarketDataType(1)
  # app.reqMktData(app.nextId(), mycontract, "232", False, False, [])

  # Bracket order setup
  parent_order = Order()
  parent_order.action = "BUY"
  parent_order.orderId = app.nextId()
  parent_order.lmtPrice = 137
  parent_order.transmit = False
  parent_order.totalQuantity = Decimal(10.0)
  parent_order.orderType = "LMT"

 
  profit_taker = Order()
  profit_taker.orderId = parent_order.orderId +1
  profit_taker.parentId = parent_order.orderId
  profit_taker.action = "SELL"
  profit_taker.totalQuantity = Decimal(10.0)
  profit_taker.tif = "GTC" # Good Till Canceled
  profit_taker.orderType = "LMT"
  profit_taker.lmtPrice = 140
  profit_taker.transmit = False

  stop_loss = Order()
  stop_loss.orderId = parent_order.orderId +2
  stop_loss.parentId = parent_order.orderId
  stop_loss.action = "SELL"
  stop_loss.totalQuantity = Decimal(10.0)
  stop_loss.orderType = "STP"
  stop_loss.auxPrice = 125
  stop_loss.transmit = True

  app.placeOrder(app.nextId(), mycontract, parent_order)
  app.placeOrder(app.nextId(), mycontract, profit_taker)
  app.placeOrder(app.nextId(), mycontract, stop_loss)

  time.sleep(5)

  
  
  time.sleep(5)
  app.disconnect()


if __name__ == "__main__":
    main()