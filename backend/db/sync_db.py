"""
Synchronous SQLite wrapper for Streamlit.
Streamlit runs in a regular (non-async) context, so we use sqlite3 directly.
"""
import json
import logging
import os
import sqlite3
from typing import Any

from backend.db.schema import SCHEMA_SQL

logger = logging.getLogger(__name__)

# DATA_DIR can be overridden via env var (same logic as schema.py)
_data_dir = os.environ.get("DATA_DIR", os.path.join(os.path.dirname(__file__), "..", "..", "data"))
DB_PATH = os.path.join(_data_dir, "brvm.db")

# Seed file is always relative to the repo root
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_SEED_PATH = os.path.join(_REPO_ROOT, "data", "seed.json")


def _get_conn() -> sqlite3.Connection:
    """Return a new sqlite3 connection with row_factory set."""
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def query(sql: str, params: tuple = ()) -> list[dict]:
    """Execute a SELECT and return all rows as a list of dicts."""
    conn = _get_conn()
    try:
        cur = conn.execute(sql, params)
        rows = cur.fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def query_one(sql: str, params: tuple = ()) -> dict | None:
    """Execute a SELECT and return the first row as dict, or None."""
    conn = _get_conn()
    try:
        cur = conn.execute(sql, params)
        row = cur.fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def execute(sql: str, params: tuple = ()) -> None:
    """Execute a single write statement (INSERT/UPDATE/DELETE)."""
    conn = _get_conn()
    try:
        conn.execute(sql, params)
        conn.commit()
    finally:
        conn.close()


def executemany(sql: str, params_list: list[tuple]) -> None:
    """Execute a write statement for multiple rows."""
    conn = _get_conn()
    try:
        conn.executemany(sql, params_list)
        conn.commit()
    finally:
        conn.close()


def init_db_sync() -> None:
    """
    Create schema tables if they do not exist, then seed stocks and
    fundamentals from data/seed.json if the fundamentals table is empty.
    """
    os.makedirs(_data_dir, exist_ok=True)
    conn = _get_conn()
    try:
        conn.executescript(SCHEMA_SQL)
        conn.commit()
    finally:
        conn.close()

    _seed_if_empty()


def _seed_if_empty() -> None:
    """Seed stocks + fundamentals from seed.json on first run."""
    if not os.path.exists(_SEED_PATH):
        logger.warning(f"Seed file not found: {_SEED_PATH}")
        return

    row = query_one("SELECT COUNT(*) AS cnt FROM fundamentals")
    count = row["cnt"] if row else 0
    if count > 0:
        return  # Already seeded

    logger.info("Seeding database from seed.json …")

    with open(_SEED_PATH, encoding="utf-8") as f:
        seed: dict = json.load(f)

    conn = _get_conn()
    try:
        for s in seed.get("stocks", []):
            conn.execute(
                """INSERT OR IGNORE INTO stocks
                   (ticker, name, sector, country, description, shares_outstanding, updated_at)
                   VALUES (?,?,?,?,?,?,?)""",
                (
                    s["ticker"], s["name"], s["sector"],
                    s.get("country", "CI"), s.get("description"),
                    s.get("shares_outstanding"), s.get("updated_at"),
                ),
            )

        for fu in seed.get("fundamentals", []):
            conn.execute(
                """INSERT OR IGNORE INTO fundamentals
                   (ticker, sector, period, year, is_bank, shares_outstanding,
                    dividend, eps_prev, eps_n2, equity, net_income, total_assets,
                    total_debt, eps_stability, pnb, bank_result, credit_outstanding,
                    client_deposits, per, market_cap, updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    fu["ticker"], fu.get("sector"), fu.get("period"), fu.get("year"),
                    fu.get("is_bank", 0), fu.get("shares_outstanding"),
                    fu.get("dividend"), fu.get("eps_prev"), fu.get("eps_n2"),
                    fu.get("equity"), fu.get("net_income"), fu.get("total_assets"),
                    fu.get("total_debt"), fu.get("eps_stability"), fu.get("pnb"),
                    fu.get("bank_result"), fu.get("credit_outstanding"),
                    fu.get("client_deposits"), fu.get("per"), fu.get("market_cap"),
                    fu.get("updated_at"),
                ),
            )

        conn.commit()
        logger.info(
            f"Seeded {len(seed.get('stocks', []))} stocks and "
            f"{len(seed.get('fundamentals', []))} fundamentals."
        )
    finally:
        conn.close()
