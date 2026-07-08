"""
Rafraîchissement des données de marché avant screening.

Réutilise les scrapers du desk (cascade richbourse / brvm.org / sikafinance)
pour :
  1. insérer les cotations du jour (fetch_all_quotes) ;
  2. combler les trous d'historique par ticker (fetch_history) ;
  3. mettre à jour les indices (fetch_indices).

Chaque étape est best-effort : un échec réseau n'arrête jamais la chaîne.
Utilisable seul : python -m brvm_screener.refresh_data
"""
import os
import sqlite3
import sys
from datetime import datetime, timedelta

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from backend.models.reference import TICKERS_BRVM              # noqa: E402
from backend.scrapers.courses import (fetch_all_quotes,        # noqa: E402
                                      fetch_history, fetch_indices)

from . import config

UPSERT_QUOTE = """INSERT OR REPLACE INTO daily_quotes
    (ticker, date, open, high, low, close, volume, value, change_pct, source)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
INSERT_QUOTE = UPSERT_QUOTE.replace("INSERT OR REPLACE", "INSERT OR IGNORE")


def _today() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def refresh_quotes_today(db: sqlite3.Connection) -> int:
    """Cotations du jour pour tout l'univers (une seule page scrapée)."""
    quotes = fetch_all_quotes()
    params = [(q["ticker"], _today(), q.get("open"), q.get("high"), q.get("low"),
               q["price"], q.get("volume") or 0, q.get("value") or 0,
               q.get("change_pct") or 0, q.get("source", "launcher"))
              for q in quotes if q.get("price") and q["price"] > 0]
    if params:
        db.executemany(UPSERT_QUOTE, params)
        db.commit()
    return len(params)


def backfill_gaps(db: sqlite3.Connection, max_stale_days: int = 3,
                  nb_history: int = 120) -> int:
    """
    Pour chaque ticker dont la dernière cotation date de plus de
    `max_stale_days` jours, récupère l'historique récent et insère les
    dates manquantes (INSERT OR IGNORE : ne réécrit jamais l'existant).
    """
    # On exclut la date du jour : les cotations fraîchement insérées par
    # refresh_quotes_today ne doivent pas masquer un trou d'historique.
    last_dates = dict(db.execute(
        "SELECT ticker, MAX(date) FROM daily_quotes WHERE date < ? GROUP BY ticker",
        (_today(),)).fetchall())
    cutoff = (datetime.now() - timedelta(days=max_stale_days)).strftime("%Y-%m-%d")
    stale = [t for t in TICKERS_BRVM if (last_dates.get(t) or "1900") < cutoff]
    if not stale:
        print("  Historique déjà à jour pour tous les titres.")
        return 0

    print(f"  {len(stale)} titres à combler (dernière cotation < {cutoff})...")
    inserted = 0
    for i, t in enumerate(stale, 1):
        try:
            hist = fetch_history(t, nb=nb_history)
            if hist.empty:
                continue
            hist["date"] = pd.to_datetime(hist["date"]).dt.strftime("%Y-%m-%d")
            floor = last_dates.get(t) or "1900-01-01"
            hist = hist[hist["date"] > floor]
            params = [(t, r["date"], r.get("open"), r.get("high"), r.get("low"),
                       r["close"], r.get("volume") or 0, r.get("value") or 0,
                       r.get("change_pct") or 0, "backfill")
                      for _, r in hist.iterrows() if pd.notna(r["close"])]
            if params:
                cur = db.executemany(INSERT_QUOTE, params)
                inserted += cur.rowcount
                db.commit()
        except Exception as e:                      # réseau capricieux : on continue
            print(f"    {t}: échec ({str(e)[:60]})")
        if i % 10 == 0:
            print(f"    ... {i}/{len(stale)} titres traités")
    return inserted


def refresh_indices(db: sqlite3.Connection) -> int:
    """Valeurs du jour des indices BRVM (l'historique long vient de export_brvm30.R)."""
    indices = fetch_indices()
    params = [(ix["name"], _today(), ix["value"], ix.get("change_pct") or 0)
              for ix in indices if ix.get("value")]
    if params:
        db.executemany("INSERT OR REPLACE INTO indices "
                       "(index_name, date, value, change_pct) VALUES (?, ?, ?, ?)",
                       params)
        db.commit()
    return len(params)


def main() -> None:
    print("── Rafraîchissement des données de marché ──────────────────────────")
    with sqlite3.connect(config.SQLITE_DB) as db:
        for label, fn in (("Cotations du jour", refresh_quotes_today),
                          ("Comblement des trous d'historique", backfill_gaps),
                          ("Indices", refresh_indices)):
            try:
                n = fn(db)
                print(f"  ✓ {label} : {n} lignes")
            except Exception as e:
                print(f"  ✗ {label} : échec ({str(e)[:80]}) — on continue")


if __name__ == "__main__":
    main()
