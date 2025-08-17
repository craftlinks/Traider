import sqlite3
import logging
from pathlib import Path

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
    conn = sqlite3.connect(DATABASE_FILE)
    # Enable accessing columns by name: row["column_name"]
    conn.row_factory = sqlite3.Row  # type: ignore [assignment]
    # Enforce foreign-key constraints
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def create_tables() -> None:  # noqa: D401
    """Create all database tables if they do not already exist."""
    with get_db_connection() as conn:
        cursor = conn.cursor()

        # --- Companies Table ---
        # For development simplicity: if the companies table exists with an old
        # schema (e.g. UNIQUE constraint on cik) drop and recreate. This avoids
        # migration complexity at this stage.
        cursor.execute(
            """
            DROP TABLE IF EXISTS companies;
            """
        )
        cursor.execute(
            """
            CREATE TABLE companies (
                ticker TEXT PRIMARY KEY,
                cik TEXT NOT NULL,
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

        conn.commit()
        logger.info("Database tables created or verified successfully at %s", DATABASE_FILE)


if __name__ == "__main__":
    create_tables()
