"""
BRVM fundamentals scraper.
Sources: sikafinance.com (company page) → richbourse.com (fiche société)
Returns: EPS, equity, net_income, total_assets, dividend, shares_outstanding, etc.
"""
import logging
import re
from io import StringIO
from typing import Optional

import requests
from bs4 import BeautifulSoup
import pandas as pd

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
    "Accept-Language": "fr-FR,fr;q=0.9",
}

# French financial label synonyms → canonical field name
LABEL_MAP = {
    "bpa":                    "eps",
    "benefice par action":    "eps",
    "bénéfice par action":    "eps",
    "résultat par action":    "eps",
    "resultat par action":    "eps",
    "earning per share":      "eps",
    "eps":                    "eps",

    "dividende par action":   "dividend",
    "dividende":              "dividend",
    "div":                    "dividend",

    "résultat net":           "net_income",
    "resultat net":           "net_income",
    "bénéfice net":           "net_income",
    "benefice net":           "net_income",
    "profit net":             "net_income",

    "capitaux propres":       "equity",
    "fonds propres":          "equity",
    "capital propre":         "equity",

    "total bilan":            "total_assets",
    "total actif":            "total_assets",
    "total de l'actif":       "total_assets",
    "actif total":            "total_assets",
    "bilan total":            "total_assets",

    "dettes totales":         "total_debt",
    "dettes financières":     "total_debt",
    "dettes financieres":     "total_debt",
    "endettement net":        "total_debt",

    "nombre d'actions":       "shares_outstanding",
    "nombre d actions":       "shares_outstanding",
    "actions en circulation": "shares_outstanding",
    "titres en circulation":  "shares_outstanding",
    "nombre de titres":       "shares_outstanding",

    "capitalisation":         "market_cap",
    "capitalisation boursiere": "market_cap",
    "capitalisation boursière": "market_cap",

    "per":                    "per",
    "price earning ratio":    "per",
    "p/e":                    "per",
    "p/e ratio":              "per",

    # Bank-specific
    "pnb":                    "pnb",
    "produit net bancaire":   "pnb",
    "résultat brut exploitation": "bank_result",
    "resultat brut exploitation": "bank_result",
    "encours crédit":         "credit_outstanding",
    "encours credits":        "credit_outstanding",
    "credits clientele":      "credit_outstanding",
    "dépots clientèle":       "client_deposits",
    "depots clientele":       "client_deposits",
    "ressources clientele":   "client_deposits",
}

BANK_SECTORS = {"Services Financiers"}


def _safe_get(url: str, timeout: int = 20) -> Optional[requests.Response]:
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout, verify=False)
        if r.status_code == 200:
            return r
    except Exception as e:
        logger.debug(f"GET {url}: {e}")
    return None


def _to_float(v, default=None):
    if v is None:
        return default
    s = (str(v).replace(" ", "").replace("\xa0", "").replace("\u202f", "")
         .replace("\u2009", "").replace(",", ".").replace("%", "").strip())
    # Remove trailing K/M for now — handled separately
    try:
        return float(s)
    except Exception:
        return default


def _parse_scale(text: str) -> float:
    """Parse values like '1 234 000', '12.4 Mds', '450 M' → float (base FCFA)."""
    if not text:
        return None
    s = (text.replace("\xa0", "").replace("\u202f", "").replace(" ", "")
              .replace(",", ".").strip())
    multiplier = 1.0
    if s.upper().endswith("MDS") or s.upper().endswith("MDS"):
        s = s[:-3]; multiplier = 1e9
    elif s.upper().endswith("MD"):
        s = s[:-2]; multiplier = 1e9
    elif s.upper().endswith("M"):
        s = s[:-1]; multiplier = 1e6
    elif s.upper().endswith("K"):
        s = s[:-1]; multiplier = 1e3
    try:
        return float(s) * multiplier
    except Exception:
        return None


def _normalize_label(raw: str) -> str:
    return (raw.lower()
               .replace("\xa0", " ")
               .replace("\u202f", " ")
               .strip()
               .rstrip(":"))


def _extract_from_table(table_el) -> dict:
    """Extract key→value pairs from a 2-column HTML table."""
    result = {}
    rows = table_el.find_all("tr")
    for tr in rows:
        cells = tr.find_all(["td", "th"])
        if len(cells) < 2:
            continue
        label_raw = cells[0].get_text(strip=True)
        value_raw = cells[-1].get_text(strip=True)

        label = _normalize_label(label_raw)
        field = None
        for key, canonical in LABEL_MAP.items():
            if key in label:
                field = canonical
                break
        if field:
            val = _parse_scale(value_raw) or _to_float(value_raw)
            if val is not None:
                result[field] = val
    return result


# ═══════════════════════════════════════════════════════
# SOURCE 1: sikafinance.com/marches/titres/{TICKER}
# ═══════════════════════════════════════════════════════

def _fetch_sikafinance_fundamentals(ticker: str) -> dict:
    urls = [
        f"https://www.sikafinance.com/marches/titres/{ticker}",
        f"https://www.sikafinance.com/marches/cotations/{ticker}",
    ]
    data = {}
    for url in urls:
        resp = _safe_get(url)
        if not resp or len(resp.text) < 500:
            continue
        try:
            soup = BeautifulSoup(resp.text, "html.parser")

            # Try all tables
            for table in soup.find_all("table"):
                extracted = _extract_from_table(table)
                data.update({k: v for k, v in extracted.items() if v is not None})

            # Try definition lists
            for dl in soup.find_all(["dl", "div"]):
                dts = dl.find_all("dt")
                dds = dl.find_all("dd")
                for dt, dd in zip(dts, dds):
                    label = _normalize_label(dt.get_text())
                    for key, canonical in LABEL_MAP.items():
                        if key in label:
                            val = _parse_scale(dd.get_text(strip=True))
                            if val is not None:
                                data[canonical] = val
                            break

            if data:
                logger.info(f"sikafinance fundamentals for {ticker}: {list(data.keys())}")
                return data
        except Exception as e:
            logger.debug(f"sikafinance fundamentals parse error for {ticker}: {e}")
    return data


# ═══════════════════════════════════════════════════════
# SOURCE 2: richbourse.com company fiche
# ═══════════════════════════════════════════════════════

def _fetch_richbourse_fundamentals(ticker: str) -> dict:
    urls = [
        f"https://www.richbourse.com/common/societe/{ticker}",
        f"https://www.richbourse.com/common/variation/fiche/{ticker}",
        f"https://www.richbourse.com/common/variation/historique/{ticker}",
    ]
    data = {}
    for url in urls:
        resp = _safe_get(url)
        if not resp or len(resp.text) < 500:
            continue
        try:
            soup = BeautifulSoup(resp.text, "html.parser")
            for table in soup.find_all("table"):
                extracted = _extract_from_table(table)
                data.update({k: v for k, v in extracted.items() if v is not None})
            if data:
                logger.info(f"richbourse fundamentals for {ticker}: {list(data.keys())}")
                return data
        except Exception as e:
            logger.debug(f"richbourse fundamentals parse error for {ticker}: {e}")
    return data


# ═══════════════════════════════════════════════════════
# PUBLIC API
# ═══════════════════════════════════════════════════════

def fetch_fundamentals(ticker: str, sector: str = "") -> dict:
    """
    Try to scrape fundamental data from available sources.
    Returns a dict with whatever fields were found.
    May be empty if nothing found — caller should use manual input as fallback.
    """
    tk = ticker.upper().strip()
    data = {}

    # Try sikafinance first
    d1 = _fetch_sikafinance_fundamentals(tk)
    data.update(d1)

    # Fill gaps from richbourse
    d2 = _fetch_richbourse_fundamentals(tk)
    for k, v in d2.items():
        if k not in data:
            data[k] = v

    # Tag source
    if data:
        data["source"] = "scraped"
    data["ticker"] = tk
    data["is_bank"] = 1 if sector in BANK_SECTORS else 0

    return data


def compute_ratios(fund: dict, current_price: float) -> dict:
    """
    Compute derived financial ratios from raw fundamental data + current price.
    All values returned as floats or None.
    """
    out = dict(fund)
    price = current_price or 0

    eps = fund.get("eps") or fund.get("eps_prev")
    equity = fund.get("equity")  # in millions FCFA
    shares = fund.get("shares_outstanding")
    dividend = fund.get("dividend")
    net_income = fund.get("net_income")  # in millions FCFA
    total_debt = fund.get("total_debt")
    total_assets = fund.get("total_assets")
    market_cap = fund.get("market_cap")

    # PER: ALWAYS recompute from live price + EPS (never use stored PER)
    if price and eps and eps > 0:
        out["per"] = round(price / eps, 1)
    elif not out.get("per"):
        out["per"] = None  # no data

    # Book Value per Share & PBR
    if shares and shares > 0 and equity:
        # equity usually in millions of FCFA
        scale = 1e6 if equity < 1e6 else 1  # normalize
        bvps = (equity * scale) / shares
        out["bvps"] = round(bvps, 0)
        if price and bvps > 0:
            out["pbr"] = round(price / bvps, 2)

    # Market Cap: ALWAYS recompute from live price × shares
    if price and shares and shares > 0:
        out["market_cap"] = round(price * shares / 1e6, 2)  # in MFCFA

    # Dividend yield: ALWAYS recompute from live price
    if price and price > 0 and dividend and dividend > 0:
        out["div_yield"] = round(dividend / price * 100, 2)

    # ROE
    if equity and equity > 0 and net_income is not None:
        out["roe"] = round(net_income / equity * 100, 1)

    # Payout ratio
    if eps and eps > 0 and dividend and dividend > 0:
        out["payout_ratio"] = round(dividend / eps * 100, 1)

    # Debt/Equity
    if equity and equity > 0 and total_debt is not None:
        out["debt_equity"] = round(total_debt / equity, 2)

    # Debt/Assets
    if total_assets and total_assets > 0 and total_debt is not None:
        out["debt_assets"] = round(total_debt / total_assets * 100, 1)

    # EPS growth (if both years available)
    eps_prev = fund.get("eps_prev")
    eps_n2 = fund.get("eps_n2")
    if eps_prev and eps_n2 and eps_n2 > 0:
        out["eps_growth"] = round((eps_prev / eps_n2 - 1) * 100, 1)

    return out
