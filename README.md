# Trader

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
    git clone https://github.com/your-username/Trader.git
    cd Trader
    ```

2.  **Create the virtual environment:**
    ```bash
    uv venv
    ```

3.  **Install dependencies and the project in editable mode:**
    This command will create a virtual environment, install all required dependencies from the `uv.lock` file, and make the local `trader` package available for import.
    ```bash
    uv pip install -e .
    ```

3.  **Run the application:**
    ```bash
    uv run -m trader.main
    ```

### Production Setup

For a production or non-development environment, you can install the project directly from the lockfile for a consistent, reproducible deployment.

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/your-username/Trader.git
    cd Trader
    ```

2.  **Sync the environment:**
    This command creates a virtual environment and installs the exact versions of all dependencies as specified in `uv.lock`.
    ```bash
    uv sync
    ```

3.  **Run the application:**
    ```bash
    uv run -m trader.main
    ```

## Platform Setup

### Interactive Brokers IB Gateway

The Trader application can connect to Interactive Brokers using the IB Gateway. The IB Gateway provides access to the Interactive Brokers trading system through an API, which this application uses to manage trades. You can find more about the TWS API [here](https://www.interactivebrokers.com/campus/trading-lessons/what-is-the-tws-api/).

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
