"""
FastAPI routes for BRVM Trading Desk.
"""
import json
import logging
from datetime import datetime, date
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from backend.db.schema import get_db, DB_PATH
from backend.models.reference import TICKERS_BRVM, SECTORS, PER_SECTORIELS
from backend.scrapers.courses import (
    fetch_all_quotes, fetch_history, fetch_indices,
    fetch_richbourse_news, fetch_richbourse_dividends,
)
from backend.scrapers.news import fetch_news
from backend.scrapers.fundamentals import fetch_fundamentals, compute_ratios
from backend.scrapers.technicals import calc_all_indicators

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api")


# ══════════════════════════════════════════════════════════════
# MODELS
# ══════════════════════════════════════════════════════════════

class TransactionIn(BaseModel):
    type: str  # BUY or SELL
    ticker: str
    asset_type: str = "ACTION"  # ACTION | FCP | OBLIGATION
    date: str
    price: float
    quantity: int
    fees: Optional[float] = None
    catalyst: Optional[str] = None
    notes: Optional[str] = None

class CapitalFlowIn(BaseModel):
    date: str
    amount: float
    notes: Optional[str] = None

class WatchlistIn(BaseModel):
    ticker: str
    priority: str = "Warm"
    notes: Optional[str] = None

class AlertIn(BaseModel):
    type: str
    ticker: Optional[str] = None
    condition: str
    target_value: Optional[float] = None
    message: Optional[str] = None

class SavedScreenerIn(BaseModel):
    name: str
    criteria: dict

class FundamentalsIn(BaseModel):
    period: Optional[str] = None          # ex: "2023", "S1-2024"
    year: Optional[int] = None
    is_bank: Optional[bool] = False
    shares_outstanding: Optional[float] = None  # nombre de titres
    dividend: Optional[float] = None            # dividende/action (FCFA)
    eps: Optional[float] = None                 # BPA exercice courant
    eps_prev: Optional[float] = None            # BPA exercice précédent
    eps_n2: Optional[float] = None              # BPA N-2
    equity: Optional[float] = None             # capitaux propres (M FCFA)
    net_income: Optional[float] = None         # résultat net (M FCFA)
    total_assets: Optional[float] = None       # total bilan (M FCFA)
    total_debt: Optional[float] = None         # dettes totales (M FCFA)
    market_cap: Optional[float] = None         # capitalisation (M FCFA)
    per: Optional[float] = None               # PER constaté
    # Bank-specific
    pnb: Optional[float] = None              # Produit Net Bancaire (M FCFA)
    bank_result: Optional[float] = None      # Résultat brut d'exploitation
    credit_outstanding: Optional[float] = None  # Encours crédits (M FCFA)
    client_deposits: Optional[float] = None     # Dépôts clients (M FCFA)


# ══════════════════════════════════════════════════════════════
# MARKET DATA
# ══════════════════════════════════════════════════════════════

@router.get("/stocks")
async def get_stocks():
    """Get reference data for all BRVM stocks."""
    return [
        {"ticker": tk, "name": info[0], "sector": info[1], "country": info[2]}
        for tk, info in TICKERS_BRVM.items()
    ]


@router.get("/quotes")
async def get_quotes(refresh: bool = False):
    """
    Get latest quotes for all stocks.
    If refresh=true, fetch from web sources. Otherwise return from DB.
    """
    db = await get_db()
    try:
        if refresh:
            quotes = fetch_all_quotes()
            if quotes:
                today = datetime.now().strftime("%Y-%m-%d")
                import math
                for q in quotes:
                    # Skip quotes with invalid price (NaN, None, 0, negative)
                    px = q.get("price")
                    if px is None or not isinstance(px, (int, float)) or math.isnan(px) or px <= 0:
                        continue
                    await db.execute(
                        """INSERT OR REPLACE INTO daily_quotes
                           (ticker, date, open, high, low, close, volume, value, change_pct, source)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (q["ticker"], today,
                         q.get("open"), q.get("high"), q.get("low"),
                         q["price"],
                         q.get("volume") or 0, q.get("value") or 0,
                         q.get("change_pct") or 0, q.get("source", "unknown"))
                    )
                await db.commit()

        # Get latest quotes from DB, enriched with reference data
        rows = await db.execute_fetchall("""
            SELECT dq.ticker, dq.date, dq.close as price, dq.change_pct,
                   dq.volume, dq.value, dq.source,
                   dq.open, dq.high, dq.low
            FROM daily_quotes dq
            INNER JOIN (
                SELECT ticker, MAX(date) as max_date
                FROM daily_quotes GROUP BY ticker
            ) latest ON dq.ticker = latest.ticker AND dq.date = latest.max_date
            ORDER BY dq.ticker
        """)

        result = []
        seen_tickers = set()
        for row in rows:
            r = dict(row)
            seen_tickers.add(r["ticker"])
            ref = TICKERS_BRVM.get(r["ticker"])
            if ref:
                r["name"] = ref[0]
                r["sector"] = ref[1]
                r["country"] = ref[2]
            else:
                r["name"] = r["ticker"]
                r["sector"] = "Autre"
                r["country"] = "?"

            # Get 3-month high/low from history
            hist = await db.execute_fetchall(
                """SELECT MIN(close) as low_3m, MAX(close) as high_3m
                   FROM daily_quotes WHERE ticker = ?
                   AND date >= date('now', '-90 days')""",
                (r["ticker"],)
            )
            if hist and hist[0]["low_3m"]:
                r["low_3m"] = hist[0]["low_3m"]
                r["high_3m"] = hist[0]["high_3m"]
                if r["high_3m"] != r["low_3m"]:
                    r["range_3m_pct"] = round(
                        (r["price"] - r["low_3m"]) / (r["high_3m"] - r["low_3m"]) * 100, 1)
                else:
                    r["range_3m_pct"] = 50.0
            else:
                r["low_3m"] = r["price"]
                r["high_3m"] = r["price"]
                r["range_3m_pct"] = 50.0

            # Get PER from fundamentals
            fund = await db.execute_fetchall(
                "SELECT per, market_cap FROM fundamentals WHERE ticker = ?",
                (r["ticker"],)
            )
            if fund and fund[0]["per"]:
                r["per"] = fund[0]["per"]
                r["market_cap"] = fund[0]["market_cap"]
            else:
                r["per"] = None
                r["market_cap"] = None

            # Compute value (transaction value in FCFA) when not provided by source
            if not r.get("value") and r.get("volume") and r.get("price"):
                r["value"] = round(r["volume"] * r["price"], 0)

            result.append(r)

        # Add reference tickers that have no DB data (inactive/suspended stocks)
        for tk, ref in TICKERS_BRVM.items():
            if tk not in seen_tickers:
                result.append({
                    "ticker": tk,
                    "name": ref[0],
                    "sector": ref[1],
                    "country": ref[2],
                    "price": None,
                    "date": None,
                    "change_pct": None,
                    "volume": 0,
                    "value": 0,
                    "source": "reference_only",
                    "low_3m": None,
                    "high_3m": None,
                    "range_3m_pct": None,
                    "per": None,
                    "market_cap": None,
                    "inactive": True,
                })

        return result
    finally:
        await db.close()


@router.get("/quotes/refresh")
async def refresh_quotes():
    """Force refresh quotes from web sources and return them."""
    return await get_quotes(refresh=True)


@router.get("/indices")
async def get_indices(refresh: bool = False):
    """Get BRVM index values."""
    db = await get_db()
    try:
        if refresh:
            indices = fetch_indices()
            today = datetime.now().strftime("%Y-%m-%d")
            for idx in indices:
                await db.execute(
                    """INSERT OR REPLACE INTO indices
                       (index_name, date, value, change_pct)
                       VALUES (?, ?, ?, ?)""",
                    (idx["name"], today, idx["value"], idx["change_pct"])
                )
            await db.commit()

        rows = await db.execute_fetchall("""
            SELECT i.index_name as name, i.date, i.value, i.change_pct, i.ytd_pct
            FROM indices i
            INNER JOIN (
                SELECT index_name, MAX(date) as max_date
                FROM indices GROUP BY index_name
            ) latest ON i.index_name = latest.index_name AND i.date = latest.max_date
            ORDER BY i.index_name
        """)
        return [dict(r) for r in rows]
    finally:
        await db.close()


@router.get("/history/{ticker}")
async def get_history(ticker: str, days: int = 365):
    """Get historical data + technical indicators for a ticker.
    days: number of trading sessions to return (max 1825 = 5 years)."""
    tk = ticker.upper().strip()
    days = max(30, min(days, 1825))  # clamp between 30 and 5 years

    # Try DB first — always query all available data, then limit
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            """SELECT date, open, high, low, close, volume, change_pct
               FROM daily_quotes WHERE ticker = ?
               AND date >= date('now', ? || ' days')
               ORDER BY date ASC""",
            (tk, -days)
        )
    finally:
        await db.close()

    import pandas as pd

    # Build DB dataframe if we have enough data
    df_db = pd.DataFrame()
    if rows:
        df_db = pd.DataFrame([dict(r) for r in rows])
        df_db["date"] = pd.to_datetime(df_db["date"])
        for col in ["open", "high", "low", "close", "volume", "change_pct"]:
            df_db[col] = pd.to_numeric(df_db[col], errors="coerce")
        df_db = df_db.sort_values("date").reset_index(drop=True)
        # Compute synthetic OHLC where missing: open = close / (1 + change_pct/100)
        missing_ohlc = df_db["open"].isna() | df_db["high"].isna() | df_db["low"].isna()
        if missing_ohlc.any():
            chg = df_db.loc[missing_ohlc, "change_pct"].fillna(0) / 100
            df_db.loc[missing_ohlc, "open"] = (df_db.loc[missing_ohlc, "close"] / (1 + chg)).round(0)
            df_db.loc[missing_ohlc, "high"] = df_db.loc[missing_ohlc, ["open", "close"]].max(axis=1)
            df_db.loc[missing_ohlc, "low"]  = df_db.loc[missing_ohlc, ["open", "close"]].min(axis=1)

    # Determine if we need to scrape for more history
    # Trigger scrape if: oldest DB date is less than 60% of the requested span
    need_scrape = True
    if not df_db.empty:
        oldest = df_db["date"].min()
        cutoff = pd.Timestamp.now() - pd.Timedelta(days=days)
        coverage_days = (pd.Timestamp.now() - oldest).days
        need_scrape = coverage_days < (days * 0.6)

    if need_scrape:
        # Fetch from web scraper (tries 5y, 2y, 1y from sikafinance + richbourse date ranges)
        df_scraped = fetch_history(tk, days)
        if not df_scraped.empty:
            # Save all scraped history to DB for future use
            db2 = await get_db()
            try:
                for _, row in df_scraped.iterrows():
                    await db2.execute(
                        """INSERT OR IGNORE INTO daily_quotes
                           (ticker, date, open, high, low, close, volume, source)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                        (tk,
                         row["date"].strftime("%Y-%m-%d") if hasattr(row["date"], "strftime") else str(row["date"]),
                         row.get("open"), row.get("high"), row.get("low"),
                         row["close"], row.get("volume", 0), "scraper")
                    )
                await db2.commit()
            finally:
                await db2.close()
            # Merge scraped + DB (DB has recent data from scheduler, scraper has historical)
            cutoff = pd.Timestamp.now() - pd.Timedelta(days=days)
            df_scraped = df_scraped[df_scraped["date"] >= cutoff]
            if not df_db.empty:
                combined = pd.concat([df_db, df_scraped], ignore_index=True)
                df = (combined.drop_duplicates(subset=["date"])
                              .sort_values("date").reset_index(drop=True))
            else:
                df = df_scraped
        elif not df_db.empty:
            df = df_db
        else:
            return {"ticker": tk, "data": [], "indicators": {}}
    else:
        df = df_db

    indicators = calc_all_indicators(df)
    return {
        "ticker": tk,
        "indicators": indicators,
    }


@router.get("/top-movers")
async def get_top_movers():
    """Get top gainers, losers, and most traded."""
    db = await get_db()
    try:
        # Latest date
        row = await db.execute_fetchall("SELECT MAX(date) as d FROM daily_quotes")
        if not row or not row[0]["d"]:
            return {"gainers": [], "losers": [], "most_traded": []}

        latest_date = row[0]["d"]

        rows = await db.execute_fetchall(
            """SELECT ticker, close as price, change_pct, volume, value
               FROM daily_quotes WHERE date = ?
               ORDER BY change_pct DESC""",
            (latest_date,)
        )

        all_data = []
        for r in rows:
            d = dict(r)
            ref = TICKERS_BRVM.get(d["ticker"])
            d["name"] = ref[0] if ref else d["ticker"]
            d["sector"] = ref[1] if ref else "Autre"
            all_data.append(d)

        # Sort for each category
        gainers = sorted(all_data, key=lambda x: x["change_pct"] or 0, reverse=True)[:5]
        losers = sorted(all_data, key=lambda x: x["change_pct"] or 0)[:5]
        most_traded = sorted(all_data, key=lambda x: x["volume"] or 0, reverse=True)[:5]

        return {
            "date": latest_date,
            "gainers": gainers,
            "losers": losers,
            "most_traded": most_traded,
        }
    finally:
        await db.close()


@router.get("/sectors")
async def get_sectors():
    """Get sector breakdown with aggregated data."""
    db = await get_db()
    try:
        row = await db.execute_fetchall("SELECT MAX(date) as d FROM daily_quotes")
        if not row or not row[0]["d"]:
            return []

        latest_date = row[0]["d"]
        rows = await db.execute_fetchall(
            "SELECT ticker, close, change_pct, volume, value FROM daily_quotes WHERE date = ?",
            (latest_date,)
        )

        sectors = {}
        for r in rows:
            d = dict(r)
            ref = TICKERS_BRVM.get(d["ticker"])
            sector = ref[1] if ref else "Autre"
            if sector not in sectors:
                sectors[sector] = {"name": sector, "stocks": [], "total_volume": 0,
                                   "total_value": 0, "avg_change": 0, "count": 0}
            sectors[sector]["stocks"].append({
                "ticker": d["ticker"],
                "name": ref[0] if ref else d["ticker"],
                "price": d["close"],
                "change_pct": d["change_pct"],
                "volume": d["volume"],
                "value": d["value"],
            })
            sectors[sector]["total_volume"] += (d["volume"] or 0)
            sectors[sector]["total_value"] += (d["value"] or 0)
            sectors[sector]["avg_change"] += (d["change_pct"] or 0)
            sectors[sector]["count"] += 1

        for s in sectors.values():
            if s["count"] > 0:
                s["avg_change"] = round(s["avg_change"] / s["count"], 2)

        return list(sectors.values())
    finally:
        await db.close()


# ══════════════════════════════════════════════════════════════
# PORTFOLIO
# ══════════════════════════════════════════════════════════════

@router.get("/portfolio/positions")
async def get_positions():
    """Get current open positions with P&L."""
    db = await get_db()
    try:
        # Get all transactions
        txns = await db.execute_fetchall(
            "SELECT * FROM portfolio_transactions ORDER BY date ASC"
        )

        # Calculate positions using FIFO
        holdings = {}
        for t in txns:
            t = dict(t)
            tk = t["ticker"]
            if tk not in holdings:
                holdings[tk] = {"quantity": 0, "total_cost": 0, "entries": [],
                            "asset_type": t.get("asset_type", "ACTION")}

            if t["type"] == "BUY":
                holdings[tk]["quantity"] += t["quantity"]
                holdings[tk]["total_cost"] += t["price"] * t["quantity"] + (t["fees"] or 0)
                holdings[tk]["entries"].append(t)
            elif t["type"] == "SELL":
                holdings[tk]["quantity"] -= t["quantity"]
                if holdings[tk]["quantity"] > 0:
                    holdings[tk]["total_cost"] *= (holdings[tk]["quantity"] /
                                                    (holdings[tk]["quantity"] + t["quantity"]))

        # Build positions with current prices
        positions = []
        for tk, h in holdings.items():
            if h["quantity"] <= 0:
                continue

            pru = h["total_cost"] / h["quantity"] if h["quantity"] > 0 else 0

            # Get current price
            price_row = await db.execute_fetchall(
                "SELECT close FROM daily_quotes WHERE ticker = ? ORDER BY date DESC LIMIT 1",
                (tk,)
            )
            current_price = price_row[0]["close"] if price_row else pru

            # Entry date (earliest buy still in position)
            entry_date = h["entries"][0]["date"] if h["entries"] else None
            days_held = (datetime.now() - datetime.strptime(entry_date, "%Y-%m-%d")).days if entry_date else 0

            pnl = (current_price - pru) * h["quantity"]
            pnl_pct = (current_price / pru - 1) * 100 if pru > 0 else 0

            ref = TICKERS_BRVM.get(tk)
            positions.append({
                "ticker": tk,
                "name": ref[0] if ref else tk,
                "sector": ref[1] if ref else "Autre",
                "asset_type": h.get("asset_type", "ACTION"),
                "entry_date": entry_date,
                "pru": round(pru, 0),
                "quantity": h["quantity"],
                "current_price": current_price,
                "market_value": current_price * h["quantity"],
                "cost_basis": h["total_cost"],
                "pnl": round(pnl, 0),
                "pnl_pct": round(pnl_pct, 2),
                "days_held": days_held,
                "time_stop_warning": days_held > 28,
                "stop_loss_warning": pnl_pct < -5,
            })

        # Calculate weights
        total_value = sum(p["market_value"] for p in positions)
        for p in positions:
            p["weight_pct"] = round(p["market_value"] / total_value * 100, 1) if total_value > 0 else 0

        return positions
    finally:
        await db.close()


@router.get("/portfolio/trades")
async def get_trades():
    """Get closed trade history."""
    db = await get_db()
    try:
        txns = await db.execute_fetchall(
            "SELECT * FROM portfolio_transactions ORDER BY date DESC"
        )
        result = []
        for t in txns:
            d = dict(t)
            ref = TICKERS_BRVM.get(d["ticker"])
            d["name"] = ref[0] if ref else d["ticker"]
            result.append(d)
        return result
    finally:
        await db.close()


@router.post("/portfolio/transaction")
async def add_transaction(txn: TransactionIn):
    """Add a buy or sell transaction."""
    db = await get_db()
    try:
        fees = txn.fees
        if fees is None:
            fees = round(txn.price * txn.quantity * 0.018, 0)

        await db.execute(
            """INSERT INTO portfolio_transactions
               (type, ticker, asset_type, date, price, quantity, fees, catalyst, notes)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (txn.type.upper(), txn.ticker.upper(), txn.asset_type.upper(),
             txn.date, txn.price, txn.quantity, fees, txn.catalyst, txn.notes)
        )
        await db.commit()
        return {"status": "ok", "message": f"Transaction {txn.type} {txn.ticker} enregistrée"}
    finally:
        await db.close()


@router.delete("/portfolio/transaction/{txn_id}")
async def delete_transaction(txn_id: int):
    """Delete a single transaction by ID."""
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            "SELECT id FROM portfolio_transactions WHERE id = ?", (txn_id,)
        )
        if not rows:
            raise HTTPException(status_code=404, detail="Transaction introuvable")
        await db.execute("DELETE FROM portfolio_transactions WHERE id = ?", (txn_id,))
        await db.commit()
        return {"status": "ok", "message": f"Transaction #{txn_id} supprimée"}
    finally:
        await db.close()


@router.post("/portfolio/capital-flow")
async def add_capital_flow(flow: CapitalFlowIn):
    """Record a capital deposit/withdrawal."""
    db = await get_db()
    try:
        await db.execute(
            "INSERT INTO capital_flows (date, amount, notes) VALUES (?, ?, ?)",
            (flow.date, flow.amount, flow.notes)
        )
        await db.commit()
        return {"status": "ok"}
    finally:
        await db.close()


@router.get("/portfolio/capital-flows")
async def get_capital_flows():
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            "SELECT * FROM capital_flows ORDER BY date DESC"
        )
        return [dict(r) for r in rows]
    finally:
        await db.close()


@router.get("/portfolio/metrics")
async def get_portfolio_metrics():
    """Calculate portfolio performance metrics."""
    db = await get_db()
    try:
        positions = await get_positions()
        txns = await db.execute_fetchall(
            "SELECT * FROM portfolio_transactions ORDER BY date ASC"
        )
        flows = await db.execute_fetchall(
            "SELECT * FROM capital_flows ORDER BY date ASC"
        )

        # Realized P&L from closed trades
        sells = [dict(t) for t in txns if t["type"] == "SELL"]
        realized_pnl = 0
        wins = 0
        losses = 0
        total_win = 0
        total_loss = 0
        holding_days = []

        # Simplified: each sell is matched against average cost
        for s in sells:
            # This is a simplified calculation
            realized_pnl += s.get("quantity", 0) * s.get("price", 0) - (s.get("fees", 0) or 0)

        # Unrealized P&L
        unrealized_pnl = sum(p["pnl"] for p in positions)

        # Total capital deployed
        total_invested = sum(p["cost_basis"] for p in positions)
        total_market_value = sum(p["market_value"] for p in positions)

        # Cash from capital flows
        total_capital = sum(dict(f)["amount"] for f in flows) if flows else 100000

        cash = total_capital - total_invested + realized_pnl

        # Build allocation breakdown
        by_asset = {}
        for p in positions:
            at = p.get("asset_type", "ACTION")
            if at not in by_asset:
                by_asset[at] = {"value": 0, "sectors": {}}
            by_asset[at]["value"] += p["market_value"]
            if at == "ACTION":
                sec = p.get("sector", "Autre")
                by_asset[at]["sectors"][sec] = by_asset[at]["sectors"].get(sec, 0) + p["market_value"]

        allocation = []
        for at, data in by_asset.items():
            allocation.append({
                "type": at,
                "value": round(data["value"], 0),
                "sectors": [{"sector": k, "value": round(v, 0)}
                            for k, v in sorted(data["sectors"].items(), key=lambda x: -x[1])]
            })
        # Add cash
        allocation.append({"type": "CASH", "value": round(cash, 0), "sectors": []})

        return {
            "realized_pnl": round(realized_pnl, 0),
            "unrealized_pnl": round(unrealized_pnl, 0),
            "total_pnl": round(realized_pnl + unrealized_pnl, 0),
            "total_capital": round(total_capital, 0),
            "invested": round(total_invested, 0),
            "cash": round(cash, 0),
            "market_value": round(total_market_value, 0),
            "portfolio_value": round(total_market_value + cash, 0),
            "return_pct": round((realized_pnl + unrealized_pnl) / total_capital * 100, 2) if total_capital > 0 else 0,
            "num_positions": len(positions),
            "num_trades": len(list(txns)),
            "positions": positions,
            "allocation": allocation,
        }
    finally:
        await db.close()


# ══════════════════════════════════════════════════════════════
# WATCHLIST
# ══════════════════════════════════════════════════════════════

@router.get("/watchlist")
async def get_watchlist():
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            "SELECT * FROM watchlist ORDER BY priority, ticker"
        )
        result = []
        for r in rows:
            d = dict(r)
            ref = TICKERS_BRVM.get(d["ticker"])
            d["name"] = ref[0] if ref else d["ticker"]
            d["sector"] = ref[1] if ref else "Autre"
            result.append(d)
        return result
    finally:
        await db.close()


@router.post("/watchlist")
async def add_to_watchlist(item: WatchlistIn):
    db = await get_db()
    try:
        await db.execute(
            "INSERT OR REPLACE INTO watchlist (ticker, priority, notes) VALUES (?, ?, ?)",
            (item.ticker.upper(), item.priority, item.notes)
        )
        await db.commit()
        return {"status": "ok"}
    finally:
        await db.close()


@router.delete("/watchlist/{ticker}")
async def remove_from_watchlist(ticker: str):
    db = await get_db()
    try:
        await db.execute("DELETE FROM watchlist WHERE ticker = ?", (ticker.upper(),))
        await db.commit()
        return {"status": "ok"}
    finally:
        await db.close()


# ══════════════════════════════════════════════════════════════
# ALERTS
# ══════════════════════════════════════════════════════════════

@router.get("/alerts")
async def get_alerts():
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            "SELECT * FROM alerts WHERE is_active = 1 ORDER BY created_at DESC"
        )
        return [dict(r) for r in rows]
    finally:
        await db.close()


@router.post("/alerts")
async def create_alert(alert: AlertIn):
    db = await get_db()
    try:
        await db.execute(
            """INSERT INTO alerts (type, ticker, condition, target_value, message)
               VALUES (?, ?, ?, ?, ?)""",
            (alert.type, alert.ticker, alert.condition,
             alert.target_value, alert.message)
        )
        await db.commit()
        return {"status": "ok"}
    finally:
        await db.close()


@router.delete("/alerts/{alert_id}")
async def delete_alert(alert_id: int):
    db = await get_db()
    try:
        await db.execute("UPDATE alerts SET is_active = 0 WHERE id = ?", (alert_id,))
        await db.commit()
        return {"status": "ok"}
    finally:
        await db.close()


# ══════════════════════════════════════════════════════════════
# SCREENER
# ══════════════════════════════════════════════════════════════

@router.get("/screener")
async def run_screener(
    sector: Optional[str] = None,
    min_change: Optional[float] = None,
    max_change: Optional[float] = None,
    min_volume: Optional[float] = None,
    min_range_52w: Optional[float] = None,
    max_range_52w: Optional[float] = None,
    min_upside: Optional[float] = None,
    max_upside: Optional[float] = None,
    max_per: Optional[float] = None,
    min_div_yield: Optional[float] = None,
    min_roe: Optional[float] = None,
    has_dividend: Optional[bool] = None,
    # Quality of earnings
    min_net_margin: Optional[float] = None,
    flag_exceptional: Optional[bool] = None,
    # Liquidity
    min_days_traded_30d: Optional[float] = None,
    min_avg_vol_20d: Optional[float] = None,
    # Balance sheet
    max_debt_equity: Optional[float] = None,
    # Context
    country: Optional[str] = None,
    max_report_age_months: Optional[float] = None,
):
    """
    Multi-criteria screener combining technical + fundamental filters.
    Technical: sector, change, volume, 52w range.
    Fundamental: PER max, dividend yield min, ROE min, has_dividend.
    """
    quotes = await get_quotes()

    # Load all fundamentals once
    db = await get_db()
    try:
        fund_rows = await db.execute_fetchall("SELECT * FROM fundamentals")
        fund_map = {}
        for row in fund_rows:
            r = dict(row)
            # Get current price for ratio computation
            price_row = await db.execute_fetchall(
                "SELECT close FROM daily_quotes WHERE ticker = ? ORDER BY date DESC LIMIT 1",
                (r["ticker"],)
            )
            price = price_row[0]["close"] if price_row else None
            fund_map[r["ticker"]] = compute_ratios(r, price)
    finally:
        await db.close()

    # Pre-compute liquidity stats for all tickers
    db2 = await get_db()
    try:
        liq_rows = await db2.execute_fetchall(
            """SELECT ticker,
                      COUNT(DISTINCT date) as days_traded_30d,
                      AVG(volume)          as avg_vol_20d
               FROM daily_quotes
               WHERE date >= date('now', '-30 days')
               GROUP BY ticker"""
        )
        liq_map = {r["ticker"]: dict(r) for r in liq_rows}
    finally:
        await db2.close()

    results = []
    for q in quotes:
        if q.get("inactive"):
            continue

        # ── Technical filters ──────────────────────────
        if sector and q.get("sector") != sector:
            continue
        if min_change is not None and (q.get("change_pct") or 0) < min_change:
            continue
        if max_change is not None and (q.get("change_pct") or 0) > max_change:
            continue
        if min_volume is not None and (q.get("volume") or 0) < min_volume:
            continue
        if min_range_52w is not None and (q.get("range_3m_pct") or 50) < min_range_52w:
            continue
        if max_range_52w is not None and (q.get("range_3m_pct") or 50) > max_range_52w:
            continue

        # ── Fundamental enrichment (needed before filters) ─────
        fund = fund_map.get(q["ticker"], {})
        per = fund.get("per") or q.get("per")
        div_yield = fund.get("div_yield")
        roe = fund.get("roe")
        dividend = fund.get("dividend")
        q["per"] = per
        q["div_yield"] = div_yield
        q["roe"] = roe
        q["dividend"] = dividend
        q["pbr"] = fund.get("pbr")
        q["eps"] = fund.get("eps_prev")
        q["market_cap"] = fund.get("market_cap") or q.get("market_cap")

        # Upside/Downside vs sector fair value (EPS × PER sectoriel benchmark)
        eps_val = fund.get("eps_prev")
        sector_name = q.get("sector", "")
        bench_per = PER_SECTORIELS.get(sector_name)
        cur_price = q.get("price")
        if eps_val and eps_val > 0 and bench_per and cur_price and cur_price > 0:
            fair_value = eps_val * bench_per
            q["upside_pct"] = round((fair_value - cur_price) / cur_price * 100, 1)
            q["fair_value"] = round(fair_value)
        else:
            q["upside_pct"] = None
            q["fair_value"] = None

        # ── Fundamental filters (applied after enrichment) ────
        if max_per is not None:
            if per is None or per <= 0 or per > max_per:
                continue
        if min_div_yield is not None:
            if div_yield is None or div_yield < min_div_yield:
                continue
        if min_roe is not None:
            if roe is None or roe < min_roe:
                continue
        if has_dividend is True:
            if not (dividend and dividend > 0):
                continue
        if min_upside is not None and (q.get("upside_pct") is None or q["upside_pct"] < min_upside):
            continue
        if max_upside is not None and (q.get("upside_pct") is None or q["upside_pct"] > max_upside):
            continue

        # ── New filters ────────────────────────────────────
        # Net margin (net_income / pnb for banks, net_income / total_assets proxy otherwise)
        ni = fund.get("net_income")
        pnb = fund.get("pnb")
        net_margin = None
        if ni is not None and pnb and pnb > 0:
            net_margin = round(ni / pnb * 100, 1)
        elif ni is not None and fund.get("total_assets") and fund["total_assets"] > 0:
            net_margin = round(ni / fund["total_assets"] * 100 * 10, 1)  # rough proxy
        q["net_margin"] = net_margin
        if min_net_margin is not None and (net_margin is None or net_margin < min_net_margin):
            continue

        # Flag exceptional result: RN > 1.5 × normal (proxy: net_income > 2 × equity * avg_roe)
        if flag_exceptional:
            equity_v = fund.get("equity")
            if ni and equity_v and equity_v > 0:
                roe_v = ni / equity_v
                q["flag_exceptional"] = roe_v > 0.45  # ROE > 45% likely exceptional
            else:
                q["flag_exceptional"] = False

        # Liquidity
        liq = liq_map.get(q["ticker"], {})
        days_30d = liq.get("days_traded_30d") or 0
        avg_vol_20d = liq.get("avg_vol_20d") or 0
        q["days_traded_30d"] = int(days_30d)
        q["avg_vol_20d"] = round(avg_vol_20d, 0)
        if min_days_traded_30d is not None and days_30d < min_days_traded_30d:
            continue
        if min_avg_vol_20d is not None and avg_vol_20d < min_avg_vol_20d:
            continue

        # Balance sheet
        debt_eq = fund.get("debt_equity")
        if max_debt_equity is not None and (debt_eq is None or debt_eq > max_debt_equity):
            continue

        # Country
        if country and q.get("country") != country:
            continue

        # Report age (months since last annual/semi-annual report)
        if max_report_age_months is not None:
            fund_year = fund.get("year")
            fund_period = fund.get("period", "12M")
            if fund_year:
                from datetime import datetime
                base_month = {"12M": 12, "9M": 9, "6M": 6, "3M": 3}.get(str(fund_period), 12)
                report_date = datetime(int(fund_year), base_month, 30)
                age_months = (datetime.now() - report_date).days / 30.44
                if age_months > max_report_age_months:
                    continue

        # Regulated sector badge
        REGULATED = {"Services Financiers", "Télécommunications", "Énergie", "Services Publics"}
        q["is_regulated"] = q.get("sector") in REGULATED

        results.append(q)

    return results


# ══════════════════════════════════════════════════════════════
# FUNDAMENTALS
# ══════════════════════════════════════════════════════════════

@router.get("/fundamentals/{ticker}")
async def get_fundamentals(ticker: str, refresh: bool = False):
    """
    Get fundamental data for a ticker.
    Returns stored data + computed ratios (PER, PBR, ROE, Div Yield, etc.)
    refresh=true tries to scrape from web sources.
    """
    tk = ticker.upper().strip()
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            "SELECT * FROM fundamentals WHERE ticker = ?", (tk,)
        )

        # Refresh from web if requested or no data in DB
        if refresh or not rows:
            ref = TICKERS_BRVM.get(tk)
            sector = ref[1] if ref else ""
            scraped = fetch_fundamentals(tk, sector)
            if scraped and len(scraped) > 2:   # more than just ticker + is_bank
                await db.execute("""
                    INSERT OR REPLACE INTO fundamentals
                    (ticker, sector, period, year, is_bank, shares_outstanding,
                     dividend, eps_prev, eps_n2, equity, net_income, total_assets,
                     total_debt, pnb, bank_result, credit_outstanding,
                     client_deposits, per, market_cap, updated_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,datetime('now'))
                """, (
                    tk,
                    sector,
                    scraped.get("period"),
                    scraped.get("year"),
                    1 if scraped.get("is_bank") else 0,
                    scraped.get("shares_outstanding"),
                    scraped.get("dividend"),
                    scraped.get("eps") or scraped.get("eps_prev"),
                    scraped.get("eps_n2"),
                    scraped.get("equity"),
                    scraped.get("net_income"),
                    scraped.get("total_assets"),
                    scraped.get("total_debt"),
                    scraped.get("pnb"),
                    scraped.get("bank_result"),
                    scraped.get("credit_outstanding"),
                    scraped.get("client_deposits"),
                    scraped.get("per"),
                    scraped.get("market_cap"),
                ))
                await db.commit()
                rows = await db.execute_fetchall(
                    "SELECT * FROM fundamentals WHERE ticker = ?", (tk,)
                )

        if not rows:
            ref = TICKERS_BRVM.get(tk)
            return {
                "ticker": tk,
                "name": ref[0] if ref else tk,
                "sector": ref[1] if ref else "",
                "no_data": True,
            }

        fund = dict(rows[0])

        # Get current price
        price_row = await db.execute_fetchall(
            "SELECT close FROM daily_quotes WHERE ticker = ? ORDER BY date DESC LIMIT 1",
            (tk,)
        )
        price = price_row[0]["close"] if price_row else None

        # Compute derived ratios
        fund = compute_ratios(fund, price)

        # Enrich with reference data
        ref = TICKERS_BRVM.get(tk)
        fund["name"] = ref[0] if ref else tk
        fund["current_price"] = price
        if ref:
            sector = ref[1]
            fund["sector_per_benchmark"] = PER_SECTORIELS.get(sector)
            per = fund.get("per")
            bench = PER_SECTORIELS.get(sector)
            if per and bench and per > 0:
                fund["per_vs_sector"] = round((bench - per) / bench * 100, 1)

        return fund
    finally:
        await db.close()


@router.post("/fundamentals/{ticker}")
async def save_fundamentals(ticker: str, data: FundamentalsIn):
    """
    Save / update fundamental data manually for a ticker.
    Used when scraping fails or data needs correction.
    """
    tk = ticker.upper().strip()
    ref = TICKERS_BRVM.get(tk)
    sector = ref[1] if ref else ""

    db = await get_db()
    try:
        # eps_prev stores the "current" EPS (label kept for DB compat)
        eps_val = data.eps or data.eps_prev

        await db.execute("""
            INSERT OR REPLACE INTO fundamentals
            (ticker, sector, period, year, is_bank, shares_outstanding,
             dividend, eps_prev, eps_n2, equity, net_income, total_assets,
             total_debt, pnb, bank_result, credit_outstanding,
             client_deposits, per, market_cap, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,datetime('now'))
        """, (
            tk, sector,
            data.period, data.year,
            1 if data.is_bank else 0,
            data.shares_outstanding,
            data.dividend,
            eps_val,
            data.eps_n2,
            data.equity,
            data.net_income,
            data.total_assets,
            data.total_debt,
            data.pnb,
            data.bank_result,
            data.credit_outstanding,
            data.client_deposits,
            data.per,
            data.market_cap,
        ))
        await db.commit()
        return {"status": "ok", "ticker": tk}
    finally:
        await db.close()


@router.delete("/fundamentals/{ticker}")
async def delete_fundamentals(ticker: str):
    """Delete fundamental data for a ticker (force re-scrape or re-entry)."""
    db = await get_db()
    try:
        await db.execute("DELETE FROM fundamentals WHERE ticker = ?", (ticker.upper(),))
        await db.commit()
        return {"status": "ok"}
    finally:
        await db.close()


@router.get("/screener/saved")
async def get_saved_screeners():
    db = await get_db()
    try:
        rows = await db.execute_fetchall("SELECT * FROM saved_screeners ORDER BY name")
        return [dict(r) for r in rows]
    finally:
        await db.close()


@router.post("/screener/save")
async def save_screener(screener: SavedScreenerIn):
    db = await get_db()
    try:
        await db.execute(
            "INSERT OR REPLACE INTO saved_screeners (name, criteria) VALUES (?, ?)",
            (screener.name, json.dumps(screener.criteria))
        )
        await db.commit()
        return {"status": "ok"}
    finally:
        await db.close()


# ══════════════════════════════════════════════════════════════
# SYSTEM
# ══════════════════════════════════════════════════════════════

@router.get("/status")
async def get_status():
    """System status."""
    db = await get_db()
    try:
        quote_count = await db.execute_fetchall("SELECT COUNT(*) as c FROM daily_quotes")
        latest_date = await db.execute_fetchall("SELECT MAX(date) as d FROM daily_quotes")
        stock_count = await db.execute_fetchall("SELECT COUNT(DISTINCT ticker) as c FROM daily_quotes")
        return {
            "status": "ok",
            "db_path": DB_PATH,
            "total_quotes": quote_count[0]["c"],
            "unique_stocks": stock_count[0]["c"],
            "latest_date": latest_date[0]["d"],
            "timestamp": datetime.now().isoformat(),
        }
    finally:
        await db.close()


# ══════════════════════════════════════════════════════════════
# NEWS & CALENDAR
# ══════════════════════════════════════════════════════════════

@router.get("/news")
async def get_news(refresh: bool = False, limit: int = 30):
    """Get latest market news (from cache or fresh scrape)."""
    db = await get_db()
    try:
        if refresh:
            articles = fetch_news(limit)
            if articles:
                for a in articles:
                    await db.execute(
                        """INSERT OR IGNORE INTO news_cache
                           (title, source, url, summary, published_at)
                           VALUES (?, ?, ?, ?, ?)""",
                        (a["title"], a["source"], a.get("url") or "",
                         a.get("summary") or "", a.get("published_at") or "")
                    )
                await db.commit()

        rows = await db.execute_fetchall(
            """SELECT id, title, source, url, summary, published_at, fetched_at
               FROM news_cache ORDER BY fetched_at DESC LIMIT ?""",
            (limit,)
        )

        if not rows and not refresh:
            # Auto-fetch if cache empty
            articles = fetch_news(limit)
            result = []
            for a in articles:
                await db.execute(
                    """INSERT OR IGNORE INTO news_cache
                       (title, source, url, summary, published_at)
                       VALUES (?, ?, ?, ?, ?)""",
                    (a["title"], a["source"], a.get("url") or "",
                     a.get("summary") or "", a.get("published_at") or "")
                )
                result.append(a)
            await db.commit()
            return result

        return [dict(r) for r in rows]
    finally:
        await db.close()


@router.get("/weekly-summary")
async def get_weekly_summary():
    """
    Weekly market digest: performance of all tickers over the last 5 trading days,
    sector breakdown, top movers of the week, and news summary.
    """
    db = await get_db()
    try:
        # Get the latest date in DB and 5 trading days back
        row = await db.execute_fetchall("SELECT MAX(date) as d FROM daily_quotes")
        if not row or not row[0]["d"]:
            return {"error": "no data", "stocks": [], "gainers": [], "losers": []}
        latest_date = row[0]["d"]

        # Get all tickers with close on latest date and 5 sessions ago
        rows = await db.execute_fetchall(
            """SELECT ticker, close as price, change_pct, volume, value
               FROM daily_quotes WHERE date = ?""",
            (latest_date,)
        )
        # Get close 5 sessions ago per ticker
        rows5 = await db.execute_fetchall(
            """SELECT ticker, close as price_5d
               FROM daily_quotes
               WHERE (ticker, date) IN (
                   SELECT ticker, MAX(date) FROM daily_quotes
                   WHERE date < date(?, '-4 days')
                   GROUP BY ticker
               )""",
            (latest_date,)
        )
        price_5d = {r["ticker"]: r["price_5d"] for r in rows5}

        stocks = []
        for r in rows:
            d = dict(r)
            tk = d["ticker"]
            ref = TICKERS_BRVM.get(tk)
            d["name"] = ref[0] if ref else tk
            d["sector"] = ref[1] if ref else "Autre"
            p5 = price_5d.get(tk)
            d["var_week"] = round((d["price"] - p5) / p5 * 100, 2) if p5 and p5 > 0 else None
            stocks.append(d)

        # Sort for gainers/losers of the week
        with_var = [s for s in stocks if s["var_week"] is not None]
        gainers = sorted(with_var, key=lambda x: x["var_week"], reverse=True)[:10]
        losers  = sorted(with_var, key=lambda x: x["var_week"])[:10]
        most_active = sorted(stocks, key=lambda x: x.get("volume") or 0, reverse=True)[:10]

        # Sector performance (average var_week per sector)
        sector_perf = {}
        for s in with_var:
            sec = s["sector"]
            if sec not in sector_perf:
                sector_perf[sec] = {"sum": 0, "cnt": 0, "volume": 0}
            sector_perf[sec]["sum"] += s["var_week"]
            sector_perf[sec]["cnt"] += 1
            sector_perf[sec]["volume"] += s.get("volume") or 0
        sectors = [
            {"sector": k, "avg_var": round(v["sum"] / v["cnt"], 2), "volume": v["volume"]}
            for k, v in sector_perf.items() if v["cnt"] > 0
        ]
        sectors.sort(key=lambda x: x["avg_var"], reverse=True)

        # Market stats
        positive = sum(1 for s in with_var if s["var_week"] > 0)
        negative = sum(1 for s in with_var if s["var_week"] < 0)
        neutral  = sum(1 for s in with_var if s["var_week"] == 0)
        total_value = sum(s.get("value") or 0 for s in stocks)

        return {
            "date": latest_date,
            "stocks": stocks,
            "gainers": gainers,
            "losers": losers,
            "most_active": most_active,
            "sectors": sectors,
            "stats": {
                "positive": positive,
                "negative": negative,
                "neutral": neutral,
                "total": len(with_var),
                "total_value_week": total_value,
            },
        }
    finally:
        await db.close()


@router.get("/calendar")
async def get_calendar(refresh: bool = False):
    """Get dividend/corporate events calendar."""
    db = await get_db()
    try:
        if refresh:
            divs = fetch_richbourse_dividends()
            if divs:
                for d in divs:
                    await db.execute(
                        """INSERT OR IGNORE INTO corporate_events
                           (ticker, event_type, event_date, description, source)
                           VALUES (?, ?, ?, ?, ?)""",
                        (d["ticker"], "DIVIDEND",
                         d.get("ex_date") or d.get("payment_date") or "",
                         f"Dividende: {d.get('dividend')} FCFA | Rendement: {d.get('yield_pct')}%",
                         d.get("source", "richbourse"))
                    )
                await db.commit()

        rows = await db.execute_fetchall(
            """SELECT id, ticker, event_type, event_date, description, source, created_at
               FROM corporate_events
               ORDER BY event_date DESC
               LIMIT 100"""
        )

        if not rows and not refresh:
            divs = fetch_richbourse_dividends()
            result = []
            for d in divs:
                await db.execute(
                    """INSERT OR IGNORE INTO corporate_events
                       (ticker, event_type, event_date, description, source)
                       VALUES (?, ?, ?, ?, ?)""",
                    (d["ticker"], "DIVIDEND",
                     d.get("ex_date") or d.get("payment_date") or "",
                     f"Dividende: {d.get('dividend')} FCFA | Rendement: {d.get('yield_pct')}%",
                     d.get("source", "richbourse"))
                )
                ref = TICKERS_BRVM.get(d["ticker"])
                result.append({
                    **d,
                    "name": ref[0] if ref else d["ticker"],
                    "event_type": "DIVIDEND",
                })
            await db.commit()
            return result

        out = []
        for r in rows:
            d = dict(r)
            ref = TICKERS_BRVM.get(d.get("ticker", ""))
            d["name"] = ref[0] if ref else d.get("ticker", "")
            out.append(d)
        return out
    finally:
        await db.close()


# ══════════════════════════════════════════════════════════════
# PORTFOLIO EQUITY CURVE
# ══════════════════════════════════════════════════════════════

@router.get("/portfolio/equity-curve")
async def get_equity_curve():
    """
    Reconstruct portfolio equity value over time using stored daily_quotes.
    Returns {dates: [...], values: [...], invested: [...], pnl: [...]}.
    """
    import pandas as pd
    import math

    db = await get_db()
    try:
        # Get transactions sorted by date
        txns = await db.execute_fetchall(
            "SELECT * FROM portfolio_transactions ORDER BY date ASC"
        )
        flows = await db.execute_fetchall(
            "SELECT * FROM capital_flows ORDER BY date ASC"
        )

        if not txns and not flows:
            return {"dates": [], "values": [], "invested": [], "pnl": []}

        # Determine date range
        all_dates_str = []
        for t in txns:
            all_dates_str.append(dict(t)["date"])
        for f in flows:
            all_dates_str.append(dict(f)["date"])

        start_date = min(all_dates_str)
        today_str = datetime.now().strftime("%Y-%m-%d")

        # Get all tickers involved
        tickers = list(set(dict(t)["ticker"] for t in txns))

        # Fetch historical prices for all tickers
        price_data = {}  # {ticker: {date: close}}
        for tk in tickers:
            rows = await db.execute_fetchall(
                """SELECT date, close FROM daily_quotes
                   WHERE ticker = ? AND date >= ?
                   ORDER BY date ASC""",
                (tk, start_date)
            )
            price_data[tk] = {dict(r)["date"]: dict(r)["close"] for r in rows}

        # Get all trading dates (union of all quote dates >= start_date)
        date_rows = await db.execute_fetchall(
            """SELECT DISTINCT date FROM daily_quotes
               WHERE date >= ? ORDER BY date ASC""",
            (start_date,)
        )
        all_dates = [dict(r)["date"] for r in date_rows]

        if not all_dates:
            return {"dates": [], "values": [], "invested": [], "pnl": []}

        # Build capital flows timeline
        flow_by_date = {}
        for f in flows:
            fd = dict(f)
            flow_by_date[fd["date"]] = flow_by_date.get(fd["date"], 0) + fd["amount"]

        # Simulate portfolio day by day
        holdings = {}       # {ticker: {qty, avg_cost}}
        cumulative_capital = 0
        cumulative_invested = 0

        curve_dates = []
        curve_values = []
        curve_invested = []
        curve_pnl = []

        txn_list = [dict(t) for t in txns]
        txn_by_date = {}
        for t in txn_list:
            txn_by_date.setdefault(t["date"], []).append(t)

        # Fill price gaps: carry forward last known price
        def get_price(tk, d):
            pd_dict = price_data.get(tk, {})
            if d in pd_dict:
                return pd_dict[d]
            # Find last available price before or on d
            prior = [v for k, v in pd_dict.items() if k <= d]
            return prior[-1] if prior else None

        for d in all_dates:
            # Apply capital flows
            if d in flow_by_date:
                cumulative_capital += flow_by_date[d]

            # Apply transactions
            for t in txn_by_date.get(d, []):
                tk = t["ticker"]
                qty = t["quantity"]
                price = t["price"]
                fees = t.get("fees") or 0

                if t["type"] == "BUY":
                    if tk not in holdings:
                        holdings[tk] = {"qty": 0, "cost": 0}
                    holdings[tk]["qty"] += qty
                    holdings[tk]["cost"] += price * qty + fees
                    cumulative_invested += price * qty + fees
                elif t["type"] == "SELL":
                    if tk in holdings and holdings[tk]["qty"] > 0:
                        sell_ratio = min(qty / holdings[tk]["qty"], 1.0)
                        recovered = holdings[tk]["cost"] * sell_ratio
                        holdings[tk]["qty"] -= qty
                        holdings[tk]["cost"] -= recovered
                        cumulative_invested -= recovered
                        cumulative_capital += price * qty - fees

            # Calculate portfolio market value
            market_value = 0
            cost_basis = 0
            for tk, h in holdings.items():
                if h["qty"] <= 0:
                    continue
                px = get_price(tk, d)
                if px:
                    market_value += px * h["qty"]
                else:
                    market_value += h["cost"]  # fallback to cost if no price
                cost_basis += h["cost"]

            # Cash = capital - invested + realized gains
            cash = max(0, cumulative_capital - cumulative_invested)
            total_value = market_value + cash

            curve_dates.append(d)
            curve_values.append(round(total_value, 0))
            curve_invested.append(round(cumulative_capital, 0))
            curve_pnl.append(round(total_value - cumulative_capital, 0))

        return {
            "dates": curve_dates,
            "values": curve_values,
            "invested": curve_invested,
            "pnl": curve_pnl,
        }
    finally:
        await db.close()


# ══════════════════════════════════════════════════════════════
# TECHNICAL SCORING
# ══════════════════════════════════════════════════════════════

@router.get("/scores/{ticker}")
async def get_scores(ticker: str):
    """
    Compute Value/Quality/Momentum/Technical scores for a ticker.
    Synthetic scoring inspired from screener v6.0.
    """
    tk = ticker.upper().strip()
    hist_data = await get_history(tk)
    ind = hist_data.get("indicators", {})

    if not ind:
        return {"ticker": tk, "scores": {}}

    scores = {}

    # ── Momentum score (0-100) ──────────────────────────────
    # Based on: RSI position, price vs EMA, var_1m, var_3m, range_52w
    mom = 0
    rsi = ind.get("rsi") or 50
    # RSI between 50-70 is bullish momentum
    if 50 < rsi <= 70:
        mom += 30
    elif rsi > 70:
        mom += 15  # overextended
    elif 40 < rsi <= 50:
        mom += 10

    # Price vs EMA
    px = ind.get("current_price") or 0
    ema20 = ind.get("ema20") or px
    if px and ema20 and px > ema20:
        mom += 20

    # Short-term momentum
    var_1m = ind.get("var_1m") or 0
    var_3m = ind.get("var_3m") or 0
    if var_1m > 5:
        mom += 25
    elif var_1m > 2:
        mom += 15
    elif var_1m > 0:
        mom += 8

    if var_3m > 10:
        mom += 25
    elif var_3m > 5:
        mom += 15
    elif var_3m > 0:
        mom += 8

    # 52-week range position
    range_52w = ind.get("range_52w_pct") or 50
    if range_52w > 80:
        mom += 0  # near ATH, mean reversion risk
    elif 60 < range_52w <= 80:
        mom += 20
    elif 40 < range_52w <= 60:
        mom += 10

    scores["momentum"] = min(100, mom)

    # ── Technical score (0-100) ─────────────────────────────
    # Based on: BB position, RSI zone, EMA crossover
    tech = 0
    bb_upper = ind.get("bb_upper") or px
    bb_lower = ind.get("bb_lower") or px
    bb_mid = ind.get("bb_middle") or px

    if px and bb_upper and bb_lower and bb_upper != bb_lower:
        bb_pos = (px - bb_lower) / (bb_upper - bb_lower)
        if 0.4 < bb_pos < 0.7:
            tech += 30  # middle band = healthy
        elif 0.7 <= bb_pos < 0.9:
            tech += 20  # upper half, bullish
        elif bb_pos >= 0.9:
            tech += 10  # near upper band, potential reversal
        elif 0.1 < bb_pos < 0.4:
            tech += 15

    # RSI not overbought/oversold
    if 40 <= rsi <= 60:
        tech += 30
    elif 60 < rsi <= 70:
        tech += 20
    elif 30 <= rsi < 40:
        tech += 15  # approaching oversold, potential bounce
    elif rsi < 30:
        tech += 25  # oversold bounce potential

    # EMA alignment
    sma50 = ind.get("sma50") or 0
    if px and ema20 and sma50:
        if px > ema20 > sma50:
            tech += 40  # perfect bullish alignment
        elif px > ema20:
            tech += 25
        elif ema20 > sma50:
            tech += 15

    scores["technical"] = min(100, tech)

    # ── Value + Quality scores ───────────────────────────────
    db = await get_db()
    try:
        fund_rows = await db.execute_fetchall(
            "SELECT * FROM fundamentals WHERE ticker = ?", (tk,)
        )
        ref = TICKERS_BRVM.get(tk)
        sector = ref[1] if ref else "Autre"
        sector_per_bench = PER_SECTORIELS.get(sector, 15)

        if fund_rows:
            fund = compute_ratios(dict(fund_rows[0]), px)
            per = fund.get("per")
            roe = fund.get("roe")
            div_yield = fund.get("div_yield")
            debt_equity = fund.get("debt_equity")
            pbr = fund.get("pbr")

            # Value score — based on PER vs sector + PBR + dividend
            val = 50
            if per and per > 0:
                discount = (sector_per_bench - per) / sector_per_bench * 100
                if discount > 30: val = 90
                elif discount > 20: val = 80
                elif discount > 10: val = 68
                elif discount > 0: val = 56
                elif discount > -10: val = 42
                elif discount > -20: val = 30
                else: val = 15
            if pbr and pbr > 0:
                if pbr < 1: val = min(100, val + 10)
                elif pbr > 3: val = max(0, val - 10)
            if div_yield and div_yield > 5: val = min(100, val + 10)
            elif div_yield and div_yield > 3: val = min(100, val + 5)
            scores["value"] = val

            # Quality score — based on ROE + debt
            qual = 50
            if roe is not None:
                if roe > 20: qual = 90
                elif roe > 15: qual = 80
                elif roe > 10: qual = 65
                elif roe > 5: qual = 52
                elif roe > 0: qual = 40
                else: qual = 20
            if debt_equity is not None:
                if debt_equity < 0.3: qual = min(100, qual + 10)
                elif debt_equity > 1.5: qual = max(0, qual - 15)
            scores["quality"] = qual
            scores["has_fundamentals"] = True
            scores["fund_summary"] = {
                "per": per,
                "roe": roe,
                "div_yield": div_yield,
                "pbr": pbr,
                "debt_equity": debt_equity,
            }
        else:
            # No fundamentals — use range proxy
            val = 75 if range_52w < 20 else (60 if range_52w < 40 else (30 if range_52w > 80 else 50))
            scores["value"] = val
            scores["quality"] = 50
            scores["has_fundamentals"] = False
    finally:
        await db.close()

    # ── Global score (weighted: Momentum 35%, Technical 35%, Value 20%, Quality 10%)
    scores["global"] = round(
        scores["momentum"] * 0.35 +
        scores["technical"] * 0.35 +
        scores.get("value", 50) * 0.20 +
        scores.get("quality", 50) * 0.10
    )

    # ── Labels ──────────────────────────────────────────────
    def label(s):
        if s >= 70: return "Fort"
        if s >= 50: return "Moyen"
        return "Faible"

    _score_keys = ("momentum", "technical", "value", "quality", "global")
    scores["labels"] = {k: label(scores[k]) for k in _score_keys if k in scores}
    scores["labels"]["has_fundamentals"] = "Oui" if scores.get("has_fundamentals") else "Non"
    scores["rsi"] = rsi
    scores["rsi_period"] = ind.get("rsi_period", 14)
    scores["var_1w"] = ind.get("var_1w")
    scores["var_1m"] = ind.get("var_1m")
    scores["var_3m"] = ind.get("var_3m")
    scores["liquidity_pct"] = ind.get("liquidity_pct")

    return {"ticker": tk, "scores": scores}
