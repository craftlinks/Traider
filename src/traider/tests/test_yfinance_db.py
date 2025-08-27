# in traider/tests/test_yfinance_db.py

from datetime import datetime
import pytest


from traider.yfinance import EarningsEvent

# Assume the path to your get_db_connection function
# Adjust this path to match your project structure.
DB_CONNECTION_PATH = "traider.db.database.get_db_connection"

def test_earnings_event_to_db(db_connection, monkeypatch):
    """
    GIVEN an EarningsEvent instance
    WHEN its to_db() method is called
    THEN the event data should be correctly inserted into the database.
    """
    # 1. ARRANGE
    # Monkeypatch the get_db_connection function. Any code that calls it
    # will now receive our in-memory test DB connection instead of the real one.
    monkeypatch.setattr(DB_CONNECTION_PATH, lambda: db_connection)

    # Create a sample data object
    test_event = EarningsEvent(
        id=-1,  # Assuming ID is not used for insertion
        ticker="TEST",
        company_name="Test Corp",
        event_name="Q4 2023",
        time_type="BMO", # Before Market Open
        earnings_call_time=datetime.fromisoformat("2023-12-31T12:00:00+00:00"),
        eps_estimate=1.25,
        eps_actual=1.30,
        eps_surprise=0.05,
        eps_surprise_percent=4.0,
        market_cap=100_000_000.0,
    )

    # 2. ACT
    # Call the method we want to test. It will internally use our monkeypatched
    # connection. Note: we don't pass a connection, so it creates its own.
    row_id = test_event.to_db()

    # 3. ASSERT
    assert row_id is not None
    assert row_id > 0

    # Verify the data was written correctly by querying the database directly
    cursor = db_connection.cursor()
    cursor.execute(
        "SELECT * FROM earnings_reports WHERE company_ticker = ?", ("TEST",)
    )
    row = cursor.fetchone()

    assert row is not None
    assert row["company_ticker"] == "TEST"
    assert row["eps_estimate"] == 1.25
    assert row["reported_eps"] == 1.30
    assert row["market_cap"] == 100_000_000.0
    
    # Check that the company was also created in the companies table
    company_row = db_connection.execute(
        "SELECT * FROM companies WHERE ticker = ?", ("TEST",)
    ).fetchone()
    
    assert company_row is not None
    assert company_row["company_name"] == "Test Corp"


def test_profile_to_db_updates_company(db_connection, monkeypatch):
    """
    GIVEN an existing company in the DB
    WHEN a Profile object is saved with to_db()
    THEN the company's sector and industry should be updated.
    """
    from traider.yfinance import Profile
    # Note: the local import of add_url in your Profile.to_db means we
    # might need to patch that too if it has side effects we want to control.
    # For now, let's assume it's okay or we can patch it as well.
    monkeypatch.setattr(DB_CONNECTION_PATH, lambda: db_connection)
    monkeypatch.setattr("traider.db.data_manager.add_url", lambda **kwargs: None) # Mock this out

    # ARRANGE: Pre-populate the database with a company
    db_connection.execute(
        "INSERT INTO companies (ticker, company_name) VALUES (?, ?)",
        ("NVDA", "NVIDIA Corporation")
    )
    db_connection.commit()

    test_profile = Profile(
        website_url="https://nvidia.com",
        sector="Technology",
        industry="Semiconductors"
    )

    # ACT
    test_profile.to_db(ticker="NVDA")

    # ASSERT
    updated_company = db_connection.execute(
        "SELECT sector, industry FROM companies WHERE ticker = ?", ("NVDA",)
    ).fetchone()

    assert updated_company is not None
    assert updated_company["sector"] == "Technology"
    assert updated_company["industry"] == "Semiconductors"