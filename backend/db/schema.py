"""
SQLite schema and database initialization for BRVM Trading Desk.
Tables: stocks, daily_quotes, indices, portfolio_transactions, capital_flows,
        watchlist, alerts, news_cache, corporate_events
"""
import aiosqlite
import json
import os

# DATA_DIR can be overridden via env var (e.g. persistent disk on Render)
_data_dir = os.environ.get("DATA_DIR", os.path.join(os.path.dirname(__file__), "..", "..", "data"))
DB_PATH = os.path.join(_data_dir, "brvm.db")

# Seed file is always relative to the repo (committed to git)
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_SEED_PATH = os.path.join(_REPO_ROOT, "data", "seed.json")

SCHEMA_SQL = """
-- Référentiel des titres cotés
CREATE TABLE IF NOT EXISTS stocks (
    ticker TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    sector TEXT NOT NULL,
    country TEXT DEFAULT 'CI',
    description TEXT,
    shares_outstanding REAL,
    updated_at TEXT
);

-- Cours journaliers
CREATE TABLE IF NOT EXISTS daily_quotes (
    ticker TEXT NOT NULL,
    date TEXT NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL NOT NULL,
    volume REAL DEFAULT 0,
    value REAL DEFAULT 0,
    change_pct REAL DEFAULT 0,
    source TEXT,
    PRIMARY KEY (ticker, date)
);

-- Indices BRVM
CREATE TABLE IF NOT EXISTS indices (
    index_name TEXT NOT NULL,
    date TEXT NOT NULL,
    value REAL NOT NULL,
    change_pct REAL DEFAULT 0,
    ytd_pct REAL DEFAULT 0,
    PRIMARY KEY (index_name, date)
);

-- Transactions du portefeuille
CREATE TABLE IF NOT EXISTS portfolio_transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    type TEXT NOT NULL CHECK(type IN ('BUY', 'SELL')),
    ticker TEXT NOT NULL,
    asset_type TEXT DEFAULT 'ACTION',
    date TEXT NOT NULL,
    price REAL NOT NULL,
    quantity INTEGER NOT NULL,
    fees REAL DEFAULT 0,
    catalyst TEXT,
    notes TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

-- Apports de capital
CREATE TABLE IF NOT EXISTS capital_flows (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT NOT NULL,
    amount REAL NOT NULL,
    notes TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

-- Watchlist
CREATE TABLE IF NOT EXISTS watchlist (
    ticker TEXT PRIMARY KEY,
    priority TEXT DEFAULT 'Warm' CHECK(priority IN ('Hot', 'Warm', 'Cold')),
    notes TEXT,
    added_at TEXT DEFAULT (datetime('now'))
);

-- Alertes
CREATE TABLE IF NOT EXISTS alerts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    type TEXT NOT NULL,
    ticker TEXT,
    condition TEXT NOT NULL,
    target_value REAL,
    message TEXT,
    is_active INTEGER DEFAULT 1,
    triggered_at TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

-- Cache d'actualités
CREATE TABLE IF NOT EXISTS news_cache (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    source TEXT,
    url TEXT UNIQUE,
    summary TEXT,
    tickers_mentioned TEXT,
    published_at TEXT,
    fetched_at TEXT DEFAULT (datetime('now'))
);

-- Événements corporate
CREATE TABLE IF NOT EXISTS corporate_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker TEXT,
    event_type TEXT NOT NULL,
    event_date TEXT NOT NULL,
    description TEXT,
    source TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

-- Fondamentaux (repris du screener)
CREATE TABLE IF NOT EXISTS fundamentals (
    ticker TEXT PRIMARY KEY,
    sector TEXT,
    period TEXT,
    year TEXT,
    is_bank INTEGER DEFAULT 0,
    shares_outstanding REAL,
    dividend REAL,
    eps_prev REAL,
    eps_n2 REAL,
    equity REAL,
    net_income REAL,
    total_assets REAL,
    total_debt REAL,
    eps_stability TEXT,
    pnb REAL,
    bank_result REAL,
    credit_outstanding REAL,
    client_deposits REAL,
    per REAL,
    market_cap REAL,
    updated_at TEXT
);

-- Screeners sauvegardés
CREATE TABLE IF NOT EXISTS saved_screeners (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    criteria TEXT NOT NULL,
    created_at TEXT DEFAULT (datetime('now'))
);

-- Index pour performance
CREATE INDEX IF NOT EXISTS idx_quotes_date ON daily_quotes(date DESC);
CREATE INDEX IF NOT EXISTS idx_quotes_ticker ON daily_quotes(ticker);
CREATE INDEX IF NOT EXISTS idx_transactions_ticker ON portfolio_transactions(ticker);
CREATE INDEX IF NOT EXISTS idx_transactions_date ON portfolio_transactions(date DESC);
CREATE INDEX IF NOT EXISTS idx_news_date ON news_cache(published_at DESC);
"""


async def init_db():
    """Initialize database with schema, then seed reference data if empty."""
    os.makedirs(_data_dir, exist_ok=True)
    async with aiosqlite.connect(DB_PATH) as db:
        # Enable WAL mode for better concurrent access
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA busy_timeout=5000")
        await db.executescript(SCHEMA_SQL)
        await db.commit()
    await _seed_if_empty()


async def _seed_if_empty():
    """Seed stocks + fundamentals from seed.json on first run (empty DB)."""
    if not os.path.exists(_SEED_PATH):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        count = (await (await db.execute("SELECT COUNT(*) FROM fundamentals")).fetchone())[0]
        if count > 0:
            return  # Already seeded
        with open(_SEED_PATH, encoding="utf-8") as f:
            seed = json.load(f)
        # Insert stocks
        for s in seed.get("stocks", []):
            await db.execute(
                """INSERT OR IGNORE INTO stocks (ticker, name, sector, country, description, shares_outstanding, updated_at)
                   VALUES (?,?,?,?,?,?,?)""",
                (s["ticker"], s["name"], s["sector"], s.get("country", "CI"),
                 s.get("description"), s.get("shares_outstanding"), s.get("updated_at")),
            )
        # Insert fundamentals
        for fu in seed.get("fundamentals", []):
            await db.execute(
                """INSERT OR IGNORE INTO fundamentals
                   (ticker, sector, period, year, is_bank, shares_outstanding, dividend, eps_prev, eps_n2,
                    equity, net_income, total_assets, total_debt, eps_stability, pnb, bank_result,
                    credit_outstanding, client_deposits, per, market_cap, updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (fu["ticker"], fu.get("sector"), fu.get("period"), fu.get("year"), fu.get("is_bank", 0),
                 fu.get("shares_outstanding"), fu.get("dividend"), fu.get("eps_prev"), fu.get("eps_n2"),
                 fu.get("equity"), fu.get("net_income"), fu.get("total_assets"), fu.get("total_debt"),
                 fu.get("eps_stability"), fu.get("pnb"), fu.get("bank_result"),
                 fu.get("credit_outstanding"), fu.get("client_deposits"),
                 fu.get("per"), fu.get("market_cap"), fu.get("updated_at")),
            )
        await db.commit()


async def get_db():
    """Get database connection."""
    db = await aiosqlite.connect(DB_PATH, timeout=30)
    await db.execute("PRAGMA journal_mode=WAL")
    await db.execute("PRAGMA busy_timeout=5000")
    db.row_factory = aiosqlite.Row
    return db
