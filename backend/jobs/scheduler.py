"""
Scheduled jobs for periodic data refresh.
Runs during BRVM trading hours: 9h00-15h30 GMT, Mon-Fri.
Supports both AsyncIOScheduler (FastAPI) and BackgroundScheduler (Streamlit).
"""
import logging
from datetime import datetime

from apscheduler.triggers.cron import CronTrigger

from backend.scrapers.courses import fetch_all_quotes, fetch_indices

logger = logging.getLogger(__name__)

_scheduler = None


def _refresh_market_data_sync():
    """Sync version — used by BackgroundScheduler (Streamlit)."""
    from backend.db.sync_db import execute, executemany
    now = datetime.utcnow()
    if now.weekday() >= 5:
        return
    if now.hour < 9 or (now.hour == 15 and now.minute > 30) or now.hour > 15:
        return

    logger.info("Refreshing market data (sync)...")
    today = now.strftime("%Y-%m-%d")

    try:
        quotes = fetch_all_quotes()
        if quotes:
            params = [
                (q["ticker"], today,
                 q.get("open"), q.get("high"), q.get("low"),
                 q["price"],
                 q.get("volume") or 0, q.get("value") or 0,
                 q.get("change_pct") or 0, q.get("source", "unknown"))
                for q in quotes if q.get("price") and q["price"] > 0
            ]
            executemany(
                """INSERT OR REPLACE INTO daily_quotes
                   (ticker, date, open, high, low, close, volume, value, change_pct, source)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                params,
            )
            logger.info(f"Updated {len(params)} quotes")
    except Exception as e:
        logger.error(f"Error refreshing quotes: {e}")

    try:
        indices = fetch_indices()
        if indices:
            params = [
                (idx["name"], today, idx["value"], idx["change_pct"])
                for idx in indices
            ]
            executemany(
                """INSERT OR REPLACE INTO indices
                   (index_name, date, value, change_pct)
                   VALUES (?, ?, ?, ?)""",
                params,
            )
            logger.info(f"Updated {len(params)} indices")
    except Exception as e:
        logger.error(f"Error refreshing indices: {e}")


async def _refresh_market_data_async():
    """Async version — used by AsyncIOScheduler (FastAPI)."""
    from backend.db.schema import get_db
    now = datetime.utcnow()
    if now.weekday() >= 5:
        return
    if now.hour < 9 or (now.hour == 15 and now.minute > 30) or now.hour > 15:
        return

    logger.info("Refreshing market data (async)...")
    today = now.strftime("%Y-%m-%d")

    try:
        quotes = fetch_all_quotes()
        if quotes:
            db = await get_db()
            try:
                for q in quotes:
                    if not q.get("price") or q["price"] <= 0:
                        continue
                    await db.execute(
                        """INSERT OR REPLACE INTO daily_quotes
                           (ticker, date, open, high, low, close, volume, value, change_pct, source)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (q["ticker"], today,
                         q.get("open"), q.get("high"), q.get("low"),
                         q["price"],
                         q.get("volume") or 0, q.get("value") or 0,
                         q.get("change_pct") or 0, q.get("source", "unknown")),
                    )
                await db.commit()
                logger.info(f"Updated {len(quotes)} quotes")
            finally:
                await db.close()
    except Exception as e:
        logger.error(f"Error refreshing quotes: {e}")

    try:
        indices = fetch_indices()
        if indices:
            db = await get_db()
            try:
                for idx in indices:
                    await db.execute(
                        """INSERT OR REPLACE INTO indices
                           (index_name, date, value, change_pct)
                           VALUES (?, ?, ?, ?)""",
                        (idx["name"], today, idx["value"], idx["change_pct"]),
                    )
                await db.commit()
            finally:
                await db.close()
    except Exception as e:
        logger.error(f"Error refreshing indices: {e}")


def start_scheduler():
    """Start BackgroundScheduler (thread-based, works in Streamlit)."""
    global _scheduler
    if _scheduler and _scheduler.running:
        return

    from apscheduler.schedulers.background import BackgroundScheduler
    _scheduler = BackgroundScheduler(timezone="GMT")
    _scheduler.add_job(
        _refresh_market_data_sync,
        CronTrigger(
            day_of_week="mon-fri",
            hour="9-15",
            minute="*/15",
            timezone="GMT",
        ),
        id="refresh_market_data",
        replace_existing=True,
    )
    _scheduler.start()
    logger.info("BackgroundScheduler started — refreshing every 15 min during trading hours")


def start_async_scheduler():
    """Start AsyncIOScheduler (for FastAPI/uvicorn context)."""
    global _scheduler
    if _scheduler and _scheduler.running:
        return

    from apscheduler.schedulers.asyncio import AsyncIOScheduler
    _scheduler = AsyncIOScheduler(timezone="GMT")
    _scheduler.add_job(
        _refresh_market_data_async,
        CronTrigger(
            day_of_week="mon-fri",
            hour="9-15",
            minute="*/15",
            timezone="GMT",
        ),
        id="refresh_market_data",
        replace_existing=True,
    )
    _scheduler.start()
    logger.info("AsyncIOScheduler started — refreshing every 15 min during trading hours")


def stop_scheduler():
    global _scheduler
    if _scheduler and _scheduler.running:
        _scheduler.shutdown()
        logger.info("Scheduler stopped")
