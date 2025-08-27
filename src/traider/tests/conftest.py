# in traider/tests/conftest.py

import sqlite3
import pytest
from pathlib import Path

@pytest.fixture
def db_connection():
    """
    Pytest fixture to set up a fresh, in-memory SQLite database for each test.
    
    The database schema is loaded from the project's 'schema.sql' file,
    ensuring the test database structure is identical to the production one.
    """
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    # --- Apply the database schema from the single source of truth ---
    # Adjust this path if your tests directory is located differently
    schema_path = Path(__file__).parent.parent / "db" / "schema.sql"
    with open(schema_path, "r") as f:
        schema_sql = f.read()
    
    conn.cursor().executescript(schema_sql)
    conn.commit()

    yield conn

    conn.close()