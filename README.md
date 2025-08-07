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