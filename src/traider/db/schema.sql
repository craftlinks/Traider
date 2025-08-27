-- traider/db/schema.sql

-- Use PRAGMA settings to enforce foreign key constraints for data integrity.
PRAGMA foreign_keys = ON;

-- --- Companies Table ---
-- Stores core information about each company being tracked.
CREATE TABLE IF NOT EXISTS companies (
    ticker TEXT PRIMARY KEY,
    cik TEXT,
    company_name TEXT NOT NULL,
    sector TEXT,
    industry TEXT
);

-- --- Exchanges Table ---
-- Stores the stock exchanges (e.g., NASDAQ, NYSE).
CREATE TABLE IF NOT EXISTS exchanges (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL
);

-- --- Company-Exchanges Linking Table ---
-- A many-to-many relationship linking companies to the exchanges they are listed on.
CREATE TABLE IF NOT EXISTS company_exchanges (
    company_ticker TEXT NOT NULL,
    exchange_id INTEGER NOT NULL,
    PRIMARY KEY (company_ticker, exchange_id),
    FOREIGN KEY (company_ticker) REFERENCES companies (ticker) ON DELETE CASCADE,
    FOREIGN KEY (exchange_id) REFERENCES exchanges (id) ON DELETE CASCADE
);

-- --- URLs Table ---
-- Stores various URLs related to a company, such as its official website.
CREATE TABLE IF NOT EXISTS urls (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    company_ticker TEXT NOT NULL,
    url_type TEXT NOT NULL,
    url TEXT NOT NULL,
    FOREIGN KEY (company_ticker) REFERENCES companies (ticker) ON DELETE CASCADE,
    UNIQUE (company_ticker, url_type)
);

-- --- Earnings Reports Table ---
-- Stores historical and upcoming earnings report data.
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

-- Index for quickly querying earnings reports by company and date.
CREATE INDEX IF NOT EXISTS idx_earnings_ticker_datetime 
ON earnings_reports (company_ticker, report_datetime DESC);

-- Trigger to automatically update the 'updated_at' timestamp on row changes.
CREATE TRIGGER IF NOT EXISTS update_earnings_reports_updated_at
AFTER UPDATE ON earnings_reports
FOR EACH ROW
BEGIN
    UPDATE earnings_reports
    SET updated_at = CURRENT_TIMESTAMP
    WHERE id = OLD.id;
END;


-- --- Press Releases Table ---
-- Stores metadata and content of press releases.
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

-- Index for quickly querying press releases by company and date.
CREATE INDEX IF NOT EXISTS idx_press_releases_ticker_date
ON press_releases (company_ticker, pub_date DESC);

-- Trigger to automatically update the 'updated_at' timestamp on row changes.
CREATE TRIGGER IF NOT EXISTS update_press_releases_updated_at
AFTER UPDATE ON press_releases
FOR EACH ROW
BEGIN
    UPDATE press_releases
    SET updated_at = CURRENT_TIMESTAMP
    WHERE id = OLD.id;
END;