import sqlite3
import logging
from pathlib import Path
from typing import Optional

# Configure module-level logger
logger = logging.getLogger(__name__)

# Compute the database file path relative to the project root. The project root is three
# levels above this file (src/traider/db/database.py -> project_root).
_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_STORAGE_DIR = _PROJECT_ROOT / "storage"
_STORAGE_DIR.mkdir(exist_ok=True)

# Name of the SQLite database file
DATABASE_FILE: Path = _STORAGE_DIR / "trading_platform.db"


def get_db_connection() -> sqlite3.Connection:  # noqa: D401 (imperative mood preferred by project)
    """Return a new SQLite connection with row access by column name.

    Foreign-key constraints are enabled for every connection.
    """
    conn = sqlite3.connect(DATABASE_FILE, check_same_thread=False)
    # Enable accessing columns by name: row["column_name"]
    conn.row_factory = sqlite3.Row  # type: ignore [assignment]
    # Enforce foreign-key constraints
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def create_tables(conn: Optional[sqlite3.Connection] = None) -> None:  # noqa: D401
    """Create all database tables if they do not already exist."""
    if conn is None:
        conn = get_db_connection()

    cursor = conn.cursor()

    # --- Companies Table ---

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS companies (
            ticker TEXT PRIMARY KEY,
            cik TEXT,
            company_name TEXT NOT NULL,
            sector TEXT,
            industry TEXT
        );
        """
    )

    # --- Exchanges Table ---
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS exchanges (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL
        );
        """
    )

    # --- Company-Exchanges Linking Table ---
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS company_exchanges (
            company_ticker TEXT NOT NULL,
            exchange_id INTEGER NOT NULL,
            PRIMARY KEY (company_ticker, exchange_id),
            FOREIGN KEY (company_ticker) REFERENCES companies (ticker) ON DELETE CASCADE,
            FOREIGN KEY (exchange_id) REFERENCES exchanges (id) ON DELETE CASCADE
        );
        """
    )

    # --- URLs Table ---
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS urls (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_ticker TEXT NOT NULL,
            url_type TEXT NOT NULL,
            url TEXT NOT NULL,
            FOREIGN KEY (company_ticker) REFERENCES companies (ticker) ON DELETE CASCADE,
            UNIQUE (company_ticker, url_type)
        );
        """
    )

    # --- Earnings Reports Table ---
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS earnings_reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_ticker TEXT NOT NULL,
            report_datetime TEXT NOT NULL,
            fiscal_quarter INTEGER,
            fiscal_year INTEGER,
            event_name TEXT,
            time_type TEXT,
            eps_estimate REAL,
            reported_eps REAL,
            surprise_percentage REAL,
            market_cap INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(company_ticker, report_datetime),
            FOREIGN KEY (company_ticker) REFERENCES companies (ticker) ON DELETE CASCADE
        );
        """
    )

    # Trigger to auto-update the 'updated_at' timestamp whenever a row changes
    cursor.execute(
        """
        CREATE TRIGGER IF NOT EXISTS update_earnings_reports_updated_at
        AFTER UPDATE ON earnings_reports
        FOR EACH ROW
        BEGIN
            UPDATE earnings_reports
            SET updated_at = CURRENT_TIMESTAMP
            WHERE id = OLD.id;
        END;
        """
    )

    # Index for quicker look-ups of a company's earnings history
    cursor.execute("DROP INDEX IF EXISTS idx_earnings_ticker_date;")
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_earnings_ticker_datetime ON earnings_reports (company_ticker, report_datetime DESC);"
    )

    # --- Press Releases Table ---
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS press_releases (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_ticker TEXT NOT NULL,
            title TEXT NOT NULL,
            url TEXT NOT NULL UNIQUE,
            type TEXT,
            pub_date TEXT,
            display_time TEXT,
            company_name TEXT,
            raw_html TEXT,
            text_content TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (company_ticker) REFERENCES companies (ticker) ON DELETE CASCADE
        );
        """
    )

    # Trigger to auto-update the 'updated_at' timestamp whenever a press release row changes
    cursor.execute(
        """
        CREATE TRIGGER IF NOT EXISTS update_press_releases_updated_at
        AFTER UPDATE ON press_releases
        FOR EACH ROW
        BEGIN
            UPDATE press_releases
            SET updated_at = CURRENT_TIMESTAMP
            WHERE id = OLD.id;
        END;
        """
    )

    # Index for quicker look-ups of press releases by company and date (descending)
    cursor.execute("DROP INDEX IF EXISTS idx_press_releases_ticker_date;")
    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_press_releases_ticker_date
        ON press_releases (company_ticker, pub_date DESC);
        """
    )

    conn.commit()
    logger.info("Database tables created or verified successfully at %s", DATABASE_FILE)


if __name__ == "__main__":
    create_tables()
