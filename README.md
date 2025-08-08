# Traider

An AI-powered automated trading system designed to interface with various brokerage platforms.

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

### Prerequisites

*   Python 3.12+
*   [uv](https://github.com/astral-sh/uv) (a fast Python package installer and resolver)
*   Git

### Development Setup

Follow these steps to set up a development environment. This will install the project in "editable" mode, meaning any changes you make to the source code will be immediately effective without needing to reinstall.

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/your-username/Traider.git
    cd Traider
    ```

2.  **Create the virtual environment:**
    ```bash
    uv venv
    ```

3.  **Install dependencies and the project in editable mode:**
    This command will create a virtual environment, install all required dependencies from the `uv.lock` file, and make the local `traider` package available for import.
    ```bash
    uv pip install -e .
    ```

3.  **Run the simple demo application:**
    ```bash
    uv run -m traider.simple_demo
    ```

### Production Setup

For a production or non-development environment, you can install the project directly from the lockfile for a consistent, reproducible deployment.

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/your-username/Traider.git
    cd Traider
    ```

2.  **Sync the environment:**
    This command creates a virtual environment and installs the exact versions of all dependencies as specified in `uv.lock`.
    ```bash
    uv sync
    ```

3.  **Run the simple demo application:**
    ```bash
    uv run -m traider.simple_demo
    ```

## Platform Setup

### Interactive Brokers IB Gateway

The Traider application can connect to Interactive Brokers using the IB Gateway. The IB Gateway provides access to the Interactive Brokers trading system through an API, which this application uses to manage trades. You can find more about the TWS API [here](https://www.interactivebrokers.com/campus/trading-lessons/what-is-the-tws-api/).

Before running the application with the Interactive Brokers platform, you need to install and run the IB Gateway.

**Download IB Gateway:**

You can download the stable version of IB Gateway from the [Interactive Brokers website](https://www.interactivebrokers.com/en/trading/ibgateway-stable.php).

**Installation Instructions:**

Follow the instructions for your operating system.

#### Linux

1.  Download the installer (e.g., `ibgateway-stable-standalone-linux-x64.sh`).
2.  Open a terminal and navigate to the download directory.
3.  Make the installer executable:
    ```bash
    chmod u+x ibgateway-stable-standalone-linux-x64.sh
    ```
4.  Run the installer:
    ```bash
    ./ibgateway-stable-standalone-linux-x64.sh
    ```
5.  Follow the setup wizard to complete the installation.

#### Windows

1.  Download the 64-bit or 32-bit installer.
2.  Run the downloaded installer.
3.  Follow the setup wizard to complete the installation.

#### macOS

1.  Download the installer for macOS.
2.  Open the downloaded file and follow the on-screen instructions to install.

After installation, you will need to run IB Gateway and log in with your Interactive Brokers account credentials.

### Alpaca Market Data & Trading

The project includes a wrapper around the **Alpaca Market Data API** that can fetch both snapshot and streaming data.  
Alpaca requires an API key-pair which you obtain from the Alpaca dashboard.

1. **Create a `.env` file** in the project root (already git-ignored) and add:

   ```shell
   ALPACA_API_KEY="<your-key>"
   ALPACA_SECRET_KEY="<your-secret>"
   ```

2. **Choose a data feed**

   Alpaca offers several real-time feeds.  You select the feed when you construct `AlpacaMarketData`.

   | Feed enum                | Cost / Access                        | What you get                                             |
   | ------------------------ | ------------------------------------ | -------------------------------------------------------- |
   | `DataFeed.IEX`          | **Free**                             | IEX quotes/trades for listed stocks                      |
   | `DataFeed.DELAYED_SIP`  | **Free (15-min delayed)**            | Consolidated (SIP) data delayed by 15 minutes            |
   | `DataFeed.SIP`          | **Paid subscription**                | Full SIP real-time market depth & trades                 |

   Example:

   ```python
   from alpaca.data.enums import DataFeed
   from trader.platforms import AlpacaMarketData

   md = AlpacaMarketData(feed=DataFeed.IEX)          # default & free
   # md = AlpacaMarketData(feed=DataFeed.SIP)        # requires paid plan
   # md = AlpacaMarketData(feed=DataFeed.DELAYED_SIP)
   ```

3. **Snapshot vs. streaming**

   • *Snapshot* methods (`get_latest_trade`, `get_latest_quote`) are synchronous and hit Alpaca’s REST endpoints.  
   • *Streaming* methods (`subscribe_trades`, `subscribe_quotes`) start a background WebSocket thread and invoke your handler callbacks directly.

4. **Common errors**

   *`insufficient subscription`* – you attempted to use `DataFeed.SIP` without a paid plan.  Switch to `DataFeed.IEX` or upgrade your Alpaca account.

Refer to Alpaca’s official docs for more details:  
<https://docs.alpaca.markets/docs/real-time-stock-pricing-data>
