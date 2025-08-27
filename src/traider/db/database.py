# traider/db/database.py

import sqlite3
import logging
from pathlib import Path
from typing import Optional

from appdirs import user_data_dir

# Configure module-level logger
logger = logging.getLogger(__name__)

_APP_NAME = "Traider"
_APP_AUTHOR = "CraftLinks"

_DATA_DIR = Path(user_data_dir(_APP_NAME, _APP_AUTHOR))
_DATA_DIR.mkdir(parents=True, exist_ok=True)
DATABASE_FILE: Path = _DATA_DIR / "trading_platform.db"

_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_STORAGE_DIR = _PROJECT_ROOT / "storage"
_STORAGE_DIR.mkdir(exist_ok=True)


# Path to the new schema file
SCHEMA_FILE = Path(__file__).parent / "schema.sql"


def get_db_connection() -> sqlite3.Connection:
    """Return a new SQLite connection with row access by column name."""
    conn = sqlite3.connect(DATABASE_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    # The PRAGMA for foreign keys is now in the schema file,
    # but it's harmless and good practice to set it per-connection anyway.
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def create_tables(conn: Optional[sqlite3.Connection] = None) -> None:
    """
    Create all database tables by executing the schema.sql script.
    
    This function is idempotent due to the 'IF NOT EXISTS' clauses in the schema.
    """
    owns_connection = conn is None
    if conn is None:
        conn = get_db_connection()

    try:
        logger.info("Applying database schema from %s", SCHEMA_FILE)
        with open(SCHEMA_FILE, "r") as f:
            schema_sql = f.read()
        
        # Use executescript to run all statements in the .sql file
        conn.cursor().executescript(schema_sql)
        conn.commit()
        logger.info("Database tables created or verified successfully.")
    except sqlite3.Error as e:
        logger.error("Failed to apply database schema: %s", e)
        if owns_connection:
            conn.rollback() # Rollback if we own the connection
        raise
    finally:
        if owns_connection:
            conn.close()


if __name__ == "__main__":
    create_tables()