# in traider/tests/conftest.py

import sqlite3
import pytest
from pathlib import Path

from traider.db import database, crud

@pytest.fixture
def tmp_db(tmp_path: Path):
    """
    Pytest fixture that creates a temporary, file-based SQLite database for
    each test function, ensuring test isolation.
    
    It temporarily overrides the main DATABASE_FILE path and yields the path
    to the temporary database file.
    """
    # Point the database module to a temporary file for the duration of the test
    original_db_path = database.DATABASE_FILE
    tmp_db_file = tmp_path / "test_db.sqlite"
    database.DATABASE_FILE = tmp_db_file
    
    # Ensure the schema is created in our new temporary database
    crud.create_tables()

    yield tmp_db_file

    # Restore the original database path after the test completes
    database.DATABASE_FILE = original_db_path

@pytest.fixture
def db_connection(tmp_db):
    """
    Depends on the tmp_db fixture to get a connection to the temporary database.
    This fixture is useful for the test function itself to set up and assert state.
    """
    conn = sqlite3.connect(tmp_db)
    conn.row_factory = sqlite3.Row
    yield conn
    conn.close()