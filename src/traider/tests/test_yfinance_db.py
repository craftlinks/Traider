# in traider/tests/test_yfinance_db.py

from datetime import datetime
import pytest_asyncio


from traider.yfinance import EarningsEvent

# Assume the path to your get_db_connection function
# Adjust this path to match your project structure.
DB_CONNECTION_PATH = "traider.db.database.get_db_connection"

async def test_earnings_event_to_db(db_connection):
    """
    GIVEN an EarningsEvent instance
    WHEN its to_db() method is called
    THEN the event data should be correctly inserted into the database.
    """
    # 1. ARRANGE
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
    # The to_db method will now use the temporary database file configured
    # in the conftest.py setup.
    row_id = await test_event.to_db()

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


async def test_profile_to_db_updates_company(db_connection):
    """
    GIVEN an existing company in the DB
    WHEN a Profile object is saved with to_db()
    THEN the company's sector and industry should be updated.
    """
    from traider.yfinance import Profile
    # Note: the local import of add_url in your Profile.to_db means we
    # might need to patch that too if it has side effects we want to control.
    # For now, let's assume it's okay or we can patch it as well.

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
    await test_profile.to_db(ticker="NVDA")

    # ASSERT
    updated_company = db_connection.execute(
        "SELECT sector, industry FROM companies WHERE ticker = ?", ("NVDA",)
    ).fetchone()

    assert updated_company is not None
    assert updated_company["sector"] == "Technology"
    assert updated_company["industry"] == "Semiconductors"


async def test_press_release_to_db(db_connection):
    """
    GIVEN a PressRelease instance
    WHEN its to_db() method is called
    THEN the event data should be correctly inserted into the database.
    """
    # 1. ARRANGE
    from traider.yfinance import PressRelease

    # Pre-populate the database with a company for the foreign key constraint
    db_connection.execute(
        "INSERT INTO companies (ticker, company_name) VALUES (?, ?)",
        ("TEST", "Test Corp")
    )
    db_connection.commit()

    # Create a sample data object
    test_release = PressRelease(
        ticker="TEST",
        title="Test Title",
        url="https://test.com/release",
        type="8-K",
        pub_date="2023-01-01",
        display_time="10:00 AM",
        company_name="Test Corp",
        raw_html="<html><body>Test</body></html>",
        text_content="Test"
    )

    # 2. ACT
    row_id = await test_release.to_db()

    # 3. ASSERT
    assert row_id is not None
    assert row_id > 0

    # Verify the data was written correctly
    cursor = db_connection.cursor()
    cursor.execute(
        "SELECT * FROM press_releases WHERE company_ticker = ?", ("TEST",)
    )
    row = cursor.fetchone()

    assert row is not None
    assert row["company_ticker"] == "TEST"
    assert row["title"] == "Test Title"
    assert row["url"] == "https://test.com/release"
    assert row["type"] == "8-K"
    assert row["pub_date"] == "2023-01-01"
    assert row["display_time"] == "10:00 AM"
    assert row["company_name"] == "Test Corp"
    assert row["raw_html"] == "<html><body>Test</body></html>"
    assert row["text_content"] == "Test"


# ---------------------------------------------------------------------------
# .from_db() helpers
# ---------------------------------------------------------------------------

def test_earnings_event_from_db(db_connection):
    """
    GIVEN an earnings_reports row in the DB
    WHEN EarningsEvent.from_db() is called with that row
    THEN it should return a correctly populated EarningsEvent instance.
    """
    from traider.yfinance import EarningsEvent

    # ARRANGE – pre-populate supporting company & earnings data
    db_connection.execute(
        "INSERT INTO companies (ticker, company_name) VALUES (?, ?)",
        ("ACME", "Acme Corp"),
    )
    db_connection.execute(
        """
        INSERT INTO earnings_reports (
            company_ticker, report_datetime, event_name, time_type,
            eps_estimate, reported_eps, surprise_percentage, market_cap
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "ACME",
            "2024-03-31T13:00:00+00:00",
            "Q1 2024",
            "AMC",  # After Market Close
            0.42,
            0.50,
            19.05,
            1_500_000_000,
        ),
    )
    row = db_connection.execute(
        "SELECT * FROM earnings_reports WHERE company_ticker = ?", ("ACME",)
    ).fetchone()

    # ACT
    event = EarningsEvent.from_db(row)

    # ASSERT
    assert event.ticker == "ACME"
    assert event.event_name == "Q1 2024"
    assert event.time_type == "AMC"
    assert event.earnings_call_time == "2024-03-31T13:00:00+00:00"
    assert event.eps_estimate == 0.42
    assert event.eps_actual == 0.50
    # from_db computes surprise as difference; allow float precision margin
    assert abs(event.eps_surprise - (0.50 - 0.42)) < 1e-9
    assert event.eps_surprise_percent == 19.05
    assert event.market_cap == 1_500_000_000


def test_profile_from_db(db_connection):
    """
    GIVEN a companies row (with optional sector/industry) and a related website URL
    WHEN Profile.from_db() is called with a joined row
    THEN it should return a correctly populated Profile instance.
    """
    from traider.yfinance import Profile

    # ARRANGE – insert baseline company record
    db_connection.execute(
        "INSERT INTO companies (ticker, company_name, sector, industry) VALUES (?, ?, ?, ?)",
        ("NVDA", "NVIDIA Corporation", "Technology", "Semiconductors"),
    )
    # insert website url into urls table
    db_connection.execute(
        "INSERT INTO urls (company_ticker, url_type, url) VALUES (?, ?, ?)",
        ("NVDA", "website", "https://nvidia.com"),
    )

    # Create a row that has website_url, sector and industry columns.
    row = db_connection.execute(
        """
        SELECT (
            SELECT url FROM urls WHERE company_ticker = companies.ticker AND url_type = 'website'
        )             AS website_url,
               sector AS sector,
               industry AS industry
        FROM companies
        WHERE ticker = ?
        """,
        ("NVDA",),
    ).fetchone()

    # ACT
    profile = Profile.from_db(row)

    # ASSERT
    assert profile.website_url == "https://nvidia.com"
    assert profile.sector == "Technology"
    assert profile.industry == "Semiconductors"


def test_press_release_from_db(db_connection):
    """
    GIVEN a press_releases row in the DB
    WHEN PressRelease.from_db() is called with that row
    THEN it should return a correctly populated PressRelease instance.
    """
    from traider.yfinance import PressRelease

    # ARRANGE – ensure company exists and insert press release
    db_connection.execute(
        "INSERT INTO companies (ticker, company_name) VALUES (?, ?)",
        ("ABC", "ABC Corp"),
    )
    db_connection.execute(
        """
        INSERT INTO press_releases (
            company_ticker, title, url, type, pub_date, display_time,
            company_name, raw_html, text_content
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "ABC",
            "Big News",
            "https://abc.com/pr",
            "News",
            "2024-04-01",
            "09:00 AM",
            "ABC Corp",
            "<html>news</html>",
            "news text",
        ),
    )

    row = db_connection.execute(
        "SELECT * FROM press_releases WHERE company_ticker = ?", ("ABC",)
    ).fetchone()

    # ACT
    pr = PressRelease.from_db(row)

    # ASSERT
    assert pr.ticker == "ABC"
    assert pr.title == "Big News"
    assert pr.url == "https://abc.com/pr"
    assert pr.type == "News"
    assert pr.pub_date == "2024-04-01"
    assert pr.display_time == "09:00 AM"
    assert pr.company_name == "ABC Corp"
    assert pr.raw_html == "<html>news</html>"
    assert pr.text_content == "news text"