"""
Chargement des données du screener.

Priorité : fichiers déposés dans brvm_screener/data/ (csv ou xlsx).
Repli    : base SQLite du desk (data/brvm.db) — prix, fondamentaux, événements.

Toutes les fonctions tolèrent les données manquantes : elles renvoient des
DataFrames éventuellement vides et consignent des avertissements dans `warnings`.
"""
import os
import re
import sqlite3
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from backend.models.reference import TICKERS_BRVM  # noqa: E402

from . import config


def _read_input(name: str) -> pd.DataFrame | None:
    """Lit data/<name>.xlsx ou data/<name>.csv si présent, sinon None."""
    for ext, reader in ((".xlsx", pd.read_excel), (".csv", pd.read_csv)):
        path = os.path.join(config.DATA_DIR, name + ext)
        if os.path.exists(path):
            df = reader(path)
            df.columns = [str(c).strip().lower() for c in df.columns]
            return df
    return None


def _sql(query: str, params: tuple = ()) -> pd.DataFrame:
    if not os.path.exists(config.SQLITE_DB):
        return pd.DataFrame()
    with sqlite3.connect(config.SQLITE_DB) as db:
        return pd.read_sql_query(query, db, params=params)


# ── Prix journaliers ─────────────────────────────────────────────────────────

def load_daily_prices(warnings: list) -> pd.DataFrame:
    """Colonnes de sortie : date, ticker, company_name, sector, close, volume, value_traded."""
    df = _read_input("daily_prices")
    if df is not None:
        warnings.append("Prix : fichier local data/daily_prices utilisé.")
    else:
        df = _sql("SELECT ticker, date, close, volume, value AS value_traded "
                  "FROM daily_quotes ORDER BY ticker, date")
        if df.empty:
            raise FileNotFoundError(
                "Aucune donnée de prix : ni data/daily_prices.(xlsx|csv) ni base SQLite.")

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date", "ticker", "close"])
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["volume"] = pd.to_numeric(df.get("volume"), errors="coerce").fillna(0)
    if "value_traded" not in df.columns or df["value_traded"].isna().all():
        df["value_traded"] = df["close"] * df["volume"]
    df["value_traded"] = pd.to_numeric(df["value_traded"], errors="coerce")
    df["value_traded"] = df["value_traded"].fillna(df["close"] * df["volume"])

    # Nom de société et secteur depuis le référentiel si absents du fichier.
    ref_name = {t: v[0] for t, v in TICKERS_BRVM.items()}
    ref_sector = {t: v[1] for t, v in TICKERS_BRVM.items()}
    if "company_name" not in df.columns:
        df["company_name"] = df["ticker"].map(ref_name)
    if "sector" not in df.columns:
        df["sector"] = df["ticker"].map(ref_sector)
    df["company_name"] = df["company_name"].fillna(df["ticker"])
    df["sector"] = df["sector"].fillna("Inconnu")
    return df.sort_values(["ticker", "date"]).reset_index(drop=True)


# ── Benchmark BRVM 30 ────────────────────────────────────────────────────────

def load_benchmark(daily: pd.DataFrame, warnings: list) -> tuple[pd.Series, str]:
    """
    Série hebdomadaire du benchmark. Essaie le BRVM 30 réel ; si l'historique
    est trop court pour la force relative 26 semaines, construit un indice
    synthétique équipondéré à partir de l'univers (repli documenté).
    """
    need_weeks = config.PARAMS["relative_strength_lookback_weeks"] + 4

    bench = None
    df = _read_input("brvm30")
    if df is not None and {"date", "close"} <= set(df.columns):
        bench = df.rename(columns={"close": "value"})
    else:
        db = _sql("SELECT date, value FROM indices WHERE index_name LIKE 'BRVM%30%' ORDER BY date")
        if not db.empty:
            bench = db
    if bench is not None:
        bench["date"] = pd.to_datetime(bench["date"], errors="coerce")
        weekly = (bench.dropna(subset=["date"]).set_index("date")["value"]
                  .resample("W-FRI").last().dropna())
        if len(weekly) >= need_weeks:
            return weekly, "BRVM30"
        warnings.append(
            f"BRVM 30 : seulement {len(weekly)} semaines d'historique "
            f"(minimum {need_weeks}) — benchmark synthétique équipondéré utilisé.")

    # Indice synthétique : moyenne équipondérée des variations hebdo des titres.
    px = (daily.set_index("date").groupby("ticker")["close"]
          .resample("W-FRI").last().unstack(level=0).ffill())
    rets = px.pct_change(fill_method=None).mean(axis=1).fillna(0)
    synth = 100 * (1 + rets).cumprod()
    return synth, "SYNTHETIC_EQUAL_WEIGHT"


# ── Fondamentaux ─────────────────────────────────────────────────────────────

def load_fundamentals(warnings: list) -> pd.DataFrame:
    """
    Une ligne par ticker (dernier exercice connu).
    Fusionne fundamentals_dataset (workbook, riche) et la table legacy
    fundamentals (dividende, EPS N-1, PNB, market cap) quand pas de fichier local.
    """
    df = _read_input("fundamentals")
    if df is not None:
        warnings.append("Fondamentaux : fichier local data/fundamentals utilisé.")
        df = df.sort_values(["ticker", "year"]).groupby("ticker", as_index=False).last()
        return df

    ds = _sql("SELECT ticker, company, sector, is_bank, fiscal_year AS year, revenue, "
              "ebit AS operating_income, ebitda, net_income, equity, total_assets, "
              "total_debt, cash, shares_outstanding, eps, dividend_ps AS dividend_per_share, "
              "pnb, coverage_flags FROM fundamentals_dataset")
    legacy = _sql("SELECT ticker, is_bank AS is_bank_l, dividend AS dividend_l, "
                  "eps_prev, eps_n2, pnb AS pnb_l, per AS per_l, market_cap, "
                  "shares_outstanding AS shares_l FROM fundamentals")
    if ds.empty and legacy.empty:
        warnings.append("Fondamentaux : aucune source disponible — Graham Score en mode dégradé.")
        return pd.DataFrame(columns=["ticker"])

    df = ds.merge(legacy, on="ticker", how="outer")
    # Complète depuis la table legacy quand le workbook est lacunaire.
    df["dividend_per_share"] = df["dividend_per_share"].fillna(df.get("dividend_l"))
    df["eps"] = df["eps"].fillna(df.get("eps_prev"))
    df["eps_prior"] = df.get("eps_n2")          # EPS exercice précédent (proxy croissance)
    df["pnb"] = df["pnb"].fillna(df.get("pnb_l"))
    df["shares_outstanding"] = df["shares_outstanding"].fillna(df.get("shares_l"))
    df["is_bank"] = df["is_bank"].fillna(df.get("is_bank_l")).fillna(0).astype(int)

    ref_sector = {t: v[1] for t, v in TICKERS_BRVM.items()}
    df["sector"] = df["sector"].fillna(df["ticker"].map(ref_sector)).fillna("Inconnu")
    return df.drop(columns=[c for c in df.columns if c.endswith("_l")], errors="ignore")


# ── Événements d'entreprise ──────────────────────────────────────────────────

def _parse_event_date(s) -> pd.Timestamp | None:
    if s is None or (isinstance(s, float) and np.isnan(s)):
        return None
    s = str(s).strip()
    if not s or "inconnu" in s.lower():
        return None
    m = re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", s)
    if m:
        return pd.Timestamp(int(m.group(3)), int(m.group(2)), int(m.group(1)))
    try:
        return pd.Timestamp(s)
    except (ValueError, TypeError):
        return None


def load_corporate_events(warnings: list) -> pd.DataFrame:
    """Optionnel : le Catalyst Score fonctionne en mode dégradé si vide."""
    df = _read_input("corporate_events")
    if df is None:
        df = _sql("SELECT ticker, event_type, event_date, description AS event_description, "
                  "dividend_amount, yield_pct FROM corporate_events")
    if df.empty:
        warnings.append("Événements : aucune donnée — Catalyst Score en mode dégradé.")
        return pd.DataFrame(columns=["ticker", "event_type", "event_date",
                                     "dividend_amount", "yield_pct"])
    df["event_date_parsed"] = df["event_date"].map(_parse_event_date)
    return df


# ── Classification halal ─────────────────────────────────────────────────────

def load_halal(fundamentals: pd.DataFrame, warnings: list) -> pd.DataFrame:
    """
    Fichier local prioritaire ; sinon heuristique documentée :
      - banques / financières conventionnelles      -> Non-compatible (riba)
      - tabac / brasserie / loterie                 -> Non-compatible (activité)
      - non-financière, dette/actif <= 33 %          -> Compatible
      - données insuffisantes ou dette/actif > 33 %  -> À vérifier
    """
    df = _read_input("halal_classification")
    if df is not None:
        warnings.append("Halal : fichier local data/halal_classification utilisé.")
        return df[["ticker", "halal_status"]]

    rows = []
    fund = fundamentals.set_index("ticker") if not fundamentals.empty else pd.DataFrame()
    for t, (name, sector, _c) in TICKERS_BRVM.items():
        if t in config.HALAL_EXCLUDED_ACTIVITY:
            status = "Non-compatible"
        elif sector == config.FINANCIAL_SECTOR:
            status = "Non-compatible"
        else:
            debt = fund["total_debt"].get(t) if "total_debt" in fund else None
            assets = fund["total_assets"].get(t) if "total_assets" in fund else None
            if debt is not None and assets and not pd.isna(debt) and not pd.isna(assets) and assets > 0:
                status = ("Compatible" if debt / assets <= config.HALAL_MAX_DEBT_TO_ASSETS
                          else "À vérifier")
            else:
                status = "À vérifier"
        rows.append({"ticker": t, "halal_status": status})
    warnings.append("Halal : classification heuristique (secteur + dette/actif) — "
                    "déposer data/halal_classification.xlsx pour la remplacer.")
    return pd.DataFrame(rows)


# ── Point d'entrée ───────────────────────────────────────────────────────────

def load_all() -> dict:
    """Charge tout et renvoie {daily, benchmark, benchmark_name, fundamentals, events, halal, warnings, as_of}."""
    warnings: list[str] = []
    daily = load_daily_prices(warnings)
    benchmark, bench_name = load_benchmark(daily, warnings)
    fundamentals = load_fundamentals(warnings)
    events = load_corporate_events(warnings)
    halal = load_halal(fundamentals, warnings)
    as_of = daily["date"].max()   # date d'analyse = dernière cotation disponible
    return {"daily": daily, "benchmark": benchmark, "benchmark_name": bench_name,
            "fundamentals": fundamentals, "events": events, "halal": halal,
            "warnings": warnings, "as_of": as_of}
