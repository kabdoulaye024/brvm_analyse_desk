"""
Scheduled jobs for periodic data refresh.
Runs during BRVM trading hours: 9h00-15h30 GMT, Mon-Fri.
"""
import logging
from datetime import datetime
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from backend.scrapers.courses import fetch_all_quotes, fetch_indices
from backend.db.schema import get_db

logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler(timezone="GMT")


async def refresh_market_data():
    """Fetch and store latest quotes + indices."""
    now = datetime.utcnow()
    # Only during trading hours (Mon-Fri, 9h-15h30 GMT)
    if now.weekday() >= 5:
        return
    if now.hour < 9 or (now.hour == 15 and now.minute > 30) or now.hour > 15:
        return

    logger.info("Refreshing market data...")
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
                         q.get("open"),  q.get("high"), q.get("low"),
                         q["price"],
                         q.get("volume") or 0, q.get("value") or 0,
                         q.get("change_pct") or 0, q.get("source", "unknown"))
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
                        (idx["name"], today, idx["value"], idx["change_pct"])
                    )
                await db.commit()
                logger.info(f"Updated {len(indices)} indices")
            finally:
                await db.close()
    except Exception as e:
        logger.error(f"Error refreshing indices: {e}")


def start_scheduler():
    """Start the background scheduler."""
    # Every 15 minutes during trading hours
    scheduler.add_job(
        refresh_market_data,
        CronTrigger(
            day_of_week="mon-fri",
            hour="9-15",
            minute="*/15",
            timezone="GMT",
        ),
        id="refresh_market_data",
        replace_existing=True,
    )
    scheduler.start()
    logger.info("Scheduler started — refreshing every 15 min during trading hours")


def stop_scheduler():
    if scheduler.running:
        scheduler.shutdown()
