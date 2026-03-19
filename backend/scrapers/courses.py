"""
BRVM market data scrapers.
Cascade: richbourse → brvm.org → sikafinance → madisinvest
Ported and adapted from the existing screener v6.0.
All requests route through CF_WORKER_URL proxy when configured (bypasses IP blocks).
"""
import logging
import os
import re
from datetime import datetime, timedelta
from io import StringIO
from typing import Optional

import requests
import urllib3
from bs4 import BeautifulSoup
import pandas as pd

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

try:
    import cloudscraper
    _scraper = cloudscraper.create_scraper(browser={"browser": "chrome", "platform": "windows", "mobile": False})
except Exception:
    _scraper = None

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Cache-Control": "max-age=0",
}
RICHBOURSE_BASE = "https://www.richbourse.com"
MADISINVEST_API  = "https://www.madisinvest.com/api/stocks"
BRVM_ORG_EN      = "https://www.brvm.org/en/cours-actions/0/status/200"
SIKAFINANCE_AAZ  = "https://www.sikafinance.com/marches/aaz"

_NULS = {"", "-", "–", "—", "N/D", "N/A", "nd", "na", "nc", "n/c", "null", "none"}

# ── Cloudflare Worker proxy ──────────────────────────────────────────────────
# When CF_WORKER_URL is set (Streamlit secret or env var), all GET requests
# are relayed through the Worker so Streamlit Cloud IPs never hit scrapers.
def _cf_worker_url() -> str:
    try:
        import streamlit as st
        return st.secrets.get("CF_WORKER_URL", os.environ.get("CF_WORKER_URL", ""))
    except Exception:
        return os.environ.get("CF_WORKER_URL", "")


def _to_float(v, default=None):
    s = (str(v).replace(" ", "").replace("\u202f", "").replace("\u2009", "")
         .replace("\xa0", "").replace(",", ".").replace("%", "").strip())
    if s.lower() in _NULS:
        return default
    try:
        return float(s)
    except Exception:
        return default


def _safe_get(url: str, timeout: int = 20, params: dict = None, **kwargs) -> Optional[requests.Response]:
    """
    Universal GET with proxy support:
    - With CF_WORKER_URL: relay through Cloudflare Worker (bypasses IP blocks)
    - Without: cloudscraper for richbourse, plain requests for others
    """
    try:
        cf_worker = _cf_worker_url()
        if cf_worker:
            # Encode params into URL before relaying
            target = url
            if params:
                from urllib.parse import urlencode
                sep = "&" if "?" in target else "?"
                target = target + sep + urlencode(params)
            relay = f"{cf_worker}/?url={requests.utils.quote(target, safe='')}"
            resp = requests.get(relay, timeout=timeout)
        elif "richbourse.com" in url and _scraper:
            resp = _scraper.get(url, timeout=timeout, verify=False, params=params, **kwargs)
        else:
            resp = requests.get(url, headers=HEADERS, timeout=timeout, verify=False,
                                params=params, **kwargs)
        if resp.status_code == 200:
            return resp
        logger.warning(f"GET {url}: HTTP {resp.status_code}")
    except Exception as e:
        logger.warning(f"GET {url}: {e}")
    return None


# ══════════════════════════════════════════════════════════════
# FETCH ALL QUOTES (bulk) — for Market Overview
# ══════════════════════════════════════════════════════════════

def fetch_all_quotes() -> list[dict]:
    """
    Fetch all BRVM stock quotes at once.
    Cascade (best coverage first):
    1. brvm.org/en  — 47 tickers, OHLCV, official source, most reliable
    2. richbourse   — 43 tickers, change_pct + value
    3. madisinvest  — 47 tickers JSON API (fast fallback)
    4. sikafinance  — AAZ page with full OHLCV
    Returns list of dicts with: ticker, price, change_pct, volume, value, open, high, low
    """
    cf = _cf_worker_url()
    if cf:
        logger.info(f"fetch_all_quotes: using CF proxy {cf[:60]}")
    else:
        logger.warning("fetch_all_quotes: CF_WORKER_URL not set — direct requests (blocked on Streamlit Cloud)")

    # Source 1: brvm.org English endpoint — 47 tickers with full OHLCV
    resp = _safe_get(BRVM_ORG_EN, timeout=15)
    if resp:
        try:
            results = _parse_brvm_org_en(resp.text)
            if len(results) >= 40:
                logger.info(f"brvm.org/en: {len(results)} quotes fetched (OHLCV)")
                return results
            else:
                logger.warning(f"brvm.org/en: only {len(results)} quotes parsed (response {len(resp.text)} chars)")
        except Exception as e:
            logger.warning(f"brvm.org/en parse error: {e}")
    else:
        logger.warning("brvm.org/en: no response")

    # Source 2: richbourse
    for url in [
        f"{RICHBOURSE_BASE}/common/variation/index",
        f"{RICHBOURSE_BASE}/common/variation/index/veille/tout",
    ]:
        resp = _safe_get(url, timeout=15)
        if not resp or len(resp.text) < 500:
            logger.warning(f"richbourse: bad response from {url} (len={len(resp.text) if resp else 0})")
            continue
        try:
            results = _parse_richbourse_all(resp.text)
            if results:
                logger.info(f"richbourse: {len(results)} quotes fetched")
                return results
            else:
                logger.warning(f"richbourse: 0 quotes parsed from {url}")
        except Exception as e:
            logger.warning(f"richbourse parse error: {e}")

    # Source 3: madisinvest JSON API
    try:
        results = _fetch_madisinvest_quotes()
        if results:
            logger.info(f"madisinvest: {len(results)} quotes fetched")
            return results
        else:
            logger.warning("madisinvest: 0 quotes returned")
    except Exception as e:
        logger.warning(f"madisinvest error: {e}")

    # Source 4: sikafinance AAZ page
    resp = _safe_get(SIKAFINANCE_AAZ, timeout=15)
    if resp:
        try:
            results = _parse_sikafinance_aaz(resp.text)
            if results:
                logger.info(f"sikafinance/aaz: {len(results)} quotes fetched")
                return results
            else:
                logger.warning(f"sikafinance/aaz: 0 quotes parsed (response {len(resp.text)} chars)")
        except Exception as e:
            logger.warning(f"sikafinance/aaz parse error: {e}")
    else:
        logger.warning("sikafinance/aaz: no response")

    logger.error("fetch_all_quotes: ALL sources failed — no data returned")
    return []


def _parse_richbourse_all(html: str) -> list[dict]:
    """Parse richbourse variation table for all stocks."""
    results = []
    try:
        dfs = pd.read_html(StringIO(html), flavor="lxml")
    except Exception:
        return results

    COL_TK = ["symbole", "ticker", "code", "valeur", "titre", "action", "sigle"]
    COL_PX = ["coursactuel", "actuel", "derniercours", "cours", "cloture", "clôture", "close", "prix"]
    COL_VR = ["variation", "var", "évolution", "evolution"]
    COL_VL = ["volume", "vol", "quantite", "quantité", "titreséchangés"]
    COL_VA = ["valeur", "montant", "valeurtransigee", "valeurtransigée"]

    for df in dfs:
        df.columns = [str(c).strip().lower().replace(" ", "").replace("\xa0", "")
                       .replace("é", "e").replace("è", "e").replace("ê", "e")
                       for c in df.columns]

        col_tk = next((c for c in df.columns if any(k in c for k in COL_TK)), None)
        col_px = next((c for c in df.columns if any(k in c for k in COL_PX)), None)
        col_vr = next((c for c in df.columns if any(k in c for k in COL_VR)), None)
        col_vl = next((c for c in df.columns if any(k in c for k in COL_VL)), None)
        col_va = next((c for c in df.columns if any(k in c for k in COL_VA)), None)

        if not col_tk or not col_px:
            continue

        for _, row in df.iterrows():
            tk = str(row[col_tk]).strip().upper()
            if len(tk) < 3 or len(tk) > 6 or not tk.isalpha():
                continue
            px = _to_float(row[col_px])
            if not px or px <= 0 or (isinstance(px, float) and (px != px)):  # NaN check
                continue
            results.append({
                "ticker": tk,
                "price": px,
                "change_pct": _to_float(row[col_vr], 0.0) if col_vr else 0.0,
                "volume": _to_float(row[col_vl], 0) if col_vl else 0,
                "value": _to_float(row[col_va], 0) if col_va else 0,
                "source": "richbourse",
            })
        if results:
            break

    return results


def _parse_brvm_org_en(html: str) -> list[dict]:
    """
    Parse brvm.org/en/cours-actions/0/status/200 — the most reliable source.
    Returns 47 tickers with OHLCV + change_pct.
    Columns: Symbol | Name | Volume | Previous price | Opening price | Closing price | Change (%)
    """
    results = []
    try:
        dfs = pd.read_html(StringIO(html), thousands=" ", flavor="lxml")
    except Exception:
        return results

    for df in dfs:
        if len(df) < 10 or len(df.columns) < 6:
            continue
        # Normalize column names
        cols = [str(c).strip().lower() for c in df.columns]
        # Identify columns by keywords
        idx_tk    = next((i for i, c in enumerate(cols) if any(k in c for k in ["symbol","symbole","ticker","code"])), None)
        idx_prev  = next((i for i, c in enumerate(cols) if "previous" in c or "veille" in c or "prev" in c), None)
        idx_open  = next((i for i, c in enumerate(cols) if "open" in c or "ouvert" in c), None)
        idx_close = next((i for i, c in enumerate(cols) if "clos" in c or "dernier" in c or "closing" in c), None)
        idx_vol   = next((i for i, c in enumerate(cols) if "vol" in c), None)
        idx_chg   = next((i for i, c in enumerate(cols) if "change" in c or "variat" in c or "%" in c), None)

        if idx_tk is None or idx_close is None:
            continue

        for _, row in df.iterrows():
            vals = list(row)
            tk = str(vals[idx_tk]).strip().upper().replace(" ", "")
            # Validate ticker format
            if not re.match(r'^[A-Z]{3,6}$', tk):
                continue

            close  = _to_float(vals[idx_close])
            if not close or close <= 0:
                continue
            prev   = _to_float(vals[idx_prev])   if idx_prev  is not None else None
            open_  = _to_float(vals[idx_open])   if idx_open  is not None else None
            vol    = _to_float(vals[idx_vol])     if idx_vol   is not None else 0
            # change_pct: brvm.org EN stores as float e.g. "1.54" or "1,54"
            # But sometimes stores as absolute price change — compute from prev if needed
            raw_chg = _to_float(vals[idx_chg])  if idx_chg is not None else None
            if raw_chg is not None and prev and prev > 0:
                # If |raw_chg| > 50, it's a price change, not a percentage
                if abs(raw_chg) > 50:
                    chg = round((close - prev) / prev * 100, 2) if prev else 0.0
                else:
                    chg = raw_chg
            elif prev and prev > 0:
                chg = round((close - prev) / prev * 100, 2)
            else:
                chg = 0.0

            results.append({
                "ticker": tk,
                "price":  close,
                "open":   open_,
                "high":   None,   # brvm.org EN doesn't provide high
                "low":    None,
                "prev_close": prev,
                "change_pct": chg,
                "volume": vol,
                "value":  0,
                "source": "brvm.org",
            })
        if len(results) >= 40:
            break

    return results


def _parse_sikafinance_aaz(html: str) -> list[dict]:
    """
    Parse sikafinance.com/marches/aaz — returns 48 stocks with full OHLCV.
    Columns: Nom | Ouverture | +Haut | +Bas | Volume(titres) | Volume(XOF) | Dernier | Variation
    Maps names to tickers via TICKERS_BRVM reference.
    """
    from backend.models.reference import TICKERS_BRVM
    # Build name→ticker map (lowercase, stripped)
    name_to_ticker = {}
    for tk, info in TICKERS_BRVM.items():
        name_to_ticker[info[0].lower().strip()] = tk

    results = []
    try:
        dfs = pd.read_html(StringIO(html), thousands=" ", flavor="lxml")
    except Exception:
        return results

    for df in dfs:
        cols = [str(c).strip().lower() for c in df.columns]
        if "ouverture" not in " ".join(cols) and "dernier" not in " ".join(cols):
            continue
        if len(df) < 10:
            continue

        idx_name = next((i for i, c in enumerate(cols) if "nom" in c), None)
        idx_open = next((i for i, c in enumerate(cols) if "ouvert" in c), None)
        idx_high = next((i for i, c in enumerate(cols) if "haut" in c), None)
        idx_low  = next((i for i, c in enumerate(cols) if "bas" in c), None)
        idx_vol  = next((i for i, c in enumerate(cols) if "titres" in c), None)
        idx_val  = next((i for i, c in enumerate(cols) if "xof" in c or "montant" in c), None)
        idx_cls  = next((i for i, c in enumerate(cols) if "dernier" in c or "clos" in c), None)
        idx_chg  = next((i for i, c in enumerate(cols) if "variat" in c or "%" in c), None)

        if idx_cls is None:
            continue

        for _, row in df.iterrows():
            vals = list(row)
            # Resolve ticker from name
            raw_name = str(vals[idx_name]).strip() if idx_name is not None else ""
            # Try exact match then partial
            tk = name_to_ticker.get(raw_name.lower())
            if not tk:
                for ref_name, ref_tk in name_to_ticker.items():
                    if raw_name.lower()[:8] in ref_name or ref_name[:8] in raw_name.lower():
                        tk = ref_tk
                        break
            if not tk:
                continue

            close = _to_float(vals[idx_cls])
            if not close or close <= 0:
                continue

            results.append({
                "ticker": tk,
                "price":  close,
                "open":   _to_float(vals[idx_open]) if idx_open is not None else None,
                "high":   _to_float(vals[idx_high]) if idx_high is not None else None,
                "low":    _to_float(vals[idx_low])  if idx_low  is not None else None,
                "change_pct": _to_float(vals[idx_chg], 0.0) if idx_chg is not None else 0.0,
                "volume": _to_float(vals[idx_vol], 0) if idx_vol is not None else 0,
                "value":  _to_float(vals[idx_val], 0) if idx_val is not None else 0,
                "source": "sikafinance",
            })
        if results:
            break

    return results


def _parse_brvm_org_all(html: str) -> list[dict]:
    """Parse brvm.org stock listing (French, legacy fallback)."""
    results = []
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table")

    for table in tables:
        rows = table.find_all("tr")
        if len(rows) < 3:
            continue

        for tr in rows[1:]:
            cells = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
            if len(cells) < 4:
                continue

            # Find ticker-like cell (3-5 uppercase letters)
            tk = None
            for c in cells:
                clean = c.strip().upper()
                if re.match(r'^[A-Z]{3,6}$', clean):
                    tk = clean
                    break
            if not tk:
                continue

            # Extract numbers
            nums = [_to_float(c) for c in cells]
            prices = [v for v in nums if v and v > 50]
            if not prices:
                continue

            px = prices[0]
            var = next((v for v in nums if v is not None and -30 < v < 30 and v != px), 0.0)
            vol = next((v for v in nums if v is not None and v >= 0 and v != px and v != var), 0)

            results.append({
                "ticker": tk,
                "price": px,
                "change_pct": var,
                "volume": vol,
                "value": 0,
                "source": "brvm.org",
            })

    return results


def _parse_sikafinance_all(html: str) -> list[dict]:
    """Parse sikafinance cotations page."""
    results = []
    try:
        dfs = pd.read_html(StringIO(html), flavor="lxml")
    except Exception:
        return results

    for df in dfs:
        df.columns = [str(c).strip().lower().replace(" ", "").replace("\xa0", "") for c in df.columns]
        col_tk = next((c for c in df.columns if any(k in c for k in ["symbole", "ticker", "code", "valeur"])), None)
        col_px = next((c for c in df.columns if any(k in c for k in ["cours", "dernier", "close", "prix"])), None)
        if not col_tk or not col_px:
            continue

        col_vr = next((c for c in df.columns if any(k in c for k in ["variation", "var", "%"])), None)
        col_vl = next((c for c in df.columns if "vol" in c), None)

        for _, row in df.iterrows():
            tk = str(row[col_tk]).strip().upper()
            if len(tk) < 3 or len(tk) > 6:
                continue
            px = _to_float(row[col_px])
            if not px or px <= 0:
                continue
            results.append({
                "ticker": tk,
                "price": px,
                "change_pct": _to_float(row[col_vr], 0.0) if col_vr else 0.0,
                "volume": _to_float(row[col_vl], 0) if col_vl else 0,
                "value": 0,
                "source": "sikafinance",
            })
        if results:
            break

    return results


def _parse_madisinvest_volume(v) -> float:
    """Parse volume strings like '0.40K', '1.2M' to float."""
    if v is None:
        return 0.0
    s = str(v).strip().replace(",", ".")
    try:
        if s.upper().endswith("K"):
            return float(s[:-1]) * 1_000
        elif s.upper().endswith("M"):
            return float(s[:-1]) * 1_000_000
        return float(s)
    except Exception:
        return 0.0


def _fetch_madisinvest_quotes() -> list[dict]:
    """Fetch all BRVM quotes from madisinvest.com JSON API."""
    resp = _safe_get(MADISINVEST_API, timeout=15)
    if not resp:
        return []
    try:
        j = resp.json()
        data = j.get("data", []) if isinstance(j, dict) else j
        results = []
        for item in (data or []):
            tk = str(item.get("symbol") or "").strip().upper()
            if len(tk) < 3 or len(tk) > 6 or not tk.isalpha():
                continue
            px = _to_float(item.get("lastPrice"))
            if not px or px <= 0:
                continue
            results.append({
                "ticker": tk,
                "price": px,
                "change_pct": _to_float(item.get("periodVariation"), 0.0),
                "volume": _parse_madisinvest_volume(item.get("volume")),
                "value": 0,
                "source": "madisinvest",
            })
        return results
    except Exception as e:
        logger.warning(f"madisinvest JSON parse error: {e}")
        return []


# ══════════════════════════════════════════════════════════════
# FETCH HISTORICAL DATA — for charts and technical indicators
# ══════════════════════════════════════════════════════════════

def fetch_history(ticker: str, nb: int = 365) -> pd.DataFrame:
    """
    Fetch historical daily data. Cascade (priority: most history first):
    1. richbourse   — 25+ years via pagination (page=N, 20 rows/page)
    2. sikafinance  — fallback JSON endpoint
    3. brvm.org     — recent sessions supplement
    Merges all sources and returns up to `nb` most recent points.
    """
    tk = ticker.upper().strip()

    # Primary: richbourse pagination (25+ years available)
    df_rich = _fetch_richbourse_hist(tk, nb)

    # Supplement: sikafinance if richbourse fails
    df_sika = pd.DataFrame()
    if df_rich.empty:
        df_sika = _fetch_sikafinance_hist(tk)

    # Supplement: brvm.org recent sessions
    df_brvm = _fetch_brvm_org_hist(tk)

    # Merge all available sources
    fragments = [d for d in [df_rich, df_sika, df_brvm] if not d.empty]
    if fragments:
        merged = (pd.concat(fragments, ignore_index=True)
                    .drop_duplicates(subset=["date"])
                    .dropna(subset=["date", "close"])
                    .sort_values("date")
                    .reset_index(drop=True))
        if len(merged) >= 5:
            return merged.tail(nb).reset_index(drop=True)

    return pd.DataFrame()


def _parse_hist_html(html: str) -> pd.DataFrame:
    """Parse historical data HTML table."""
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if table is None:
        return pd.DataFrame()

    rows = table.find_all("tr")
    if len(rows) < 2:
        return pd.DataFrame()

    header_cells = [th.get_text(strip=True).lower()
                    .replace("\xa0", "").replace("\u202f", "").replace(" ", "")
                    for th in rows[0].find_all(["th", "td"])]

    def _find(keywords, default):
        for i, h in enumerate(header_cells):
            if any(k in h for k in keywords):
                return i
        return default

    idx_date = _find(["date", "jour", "séance", "seance"], 0)
    idx_close = _find(["ajust", "cours", "close", "cloture", "clôture", "prix"], 3)
    idx_vol = _find(["vol"], 4)
    idx_high = _find(["haut", "high", "max"], -1)
    idx_low = _find(["bas", "low", "min"], -1)
    idx_open = _find(["ouverture", "open", "ouv"], -1)

    data = []
    for tr in rows[1:]:
        cells = tr.find_all("td")
        if len(cells) <= max(idx_close, idx_date):
            continue
        row = {
            "date": cells[idx_date].get_text(strip=True),
            "close": cells[idx_close].get_text(strip=True),
            "volume": cells[idx_vol].get_text(strip=True) if len(cells) > idx_vol >= 0 else "0",
        }
        if idx_open >= 0 and len(cells) > idx_open:
            row["open"] = cells[idx_open].get_text(strip=True)
        if idx_high >= 0 and len(cells) > idx_high:
            row["high"] = cells[idx_high].get_text(strip=True)
        if idx_low >= 0 and len(cells) > idx_low:
            row["low"] = cells[idx_low].get_text(strip=True)
        data.append(row)

    if not data:
        return pd.DataFrame()

    def _clean(s):
        return pd.to_numeric(
            s.str.replace(r"[\xa0\s\u202f\u2009]", "", regex=True)
             .str.replace(",", ".", regex=False),
            errors="coerce")

    df = pd.DataFrame(data)
    df["date"] = pd.to_datetime(df["date"], dayfirst=True, errors="coerce")
    df["close"] = _clean(df["close"])
    df["volume"] = _clean(df["volume"]) if "volume" in df.columns else 0
    for col in ["open", "high", "low"]:
        if col in df.columns:
            df[col] = _clean(df[col])

    return df.dropna(subset=["date", "close"]).sort_values("date").reset_index(drop=True)


def _parse_richbourse_hist_html(html: str) -> pd.DataFrame:
    """Parse richbourse historique HTML table into a DataFrame."""
    try:
        dfs = pd.read_html(StringIO(html), thousands=" ", flavor="lxml")
    except Exception:
        return pd.DataFrame()

    for df in dfs:
        if len(df) < 3:
            continue
        cols = [str(c).lower() for c in df.columns]
        idx_date  = next((i for i, c in enumerate(cols) if "date" in c), None)
        idx_close = next((i for i, c in enumerate(cols)
                          if any(k in c for k in ["cours ajusté", "cours ajuste",
                                                   "cours normal", "ajust", "close", "cours"])), None)
        idx_vol   = next((i for i, c in enumerate(cols)
                          if any(k in c for k in ["volume ajusté", "volume ajuste",
                                                   "volume normal", "vol"])), None)
        idx_chg   = next((i for i, c in enumerate(cols)
                          if "variation" in c or "var" in c or "%" in c), None)
        if idx_date is None or idx_close is None:
            continue

        rows = []
        for _, row in df.iterrows():
            date_raw = str(row.iloc[idx_date]).strip()
            close = _to_float(row.iloc[idx_close])
            if not close or close <= 0:
                continue
            dt = pd.to_datetime(date_raw, dayfirst=True, errors="coerce")
            if pd.isna(dt):
                continue
            r = {
                "date": dt,
                "close": close,
                "volume": _to_float(row.iloc[idx_vol], 0) if idx_vol is not None else 0,
            }
            if idx_chg is not None:
                r["change_pct"] = _to_float(row.iloc[idx_chg], 0.0)
            rows.append(r)

        if rows:
            df_out = pd.DataFrame(rows)
            if "change_pct" in df_out.columns:
                chg = df_out["change_pct"].fillna(0) / 100
                df_out["open"] = (df_out["close"] / (1 + chg)).round(0)
                df_out["high"] = df_out[["open", "close"]].max(axis=1)
                df_out["low"]  = df_out[["open", "close"]].min(axis=1)
            return df_out

    return pd.DataFrame()


def _fetch_richbourse_hist(tk: str, nb: int = 365) -> pd.DataFrame:
    """
    Historical data from richbourse.com.
    Strategy (v6.0):
      1. Default URL (no params) — returns most recent ~20 rows
      2. Date-range GET params — 3 rolling windows covering nb days
      3. Date-range POST fallback (direct, no proxy) for each window
    All routed through CF_WORKER proxy when configured.
    """
    url = f"{RICHBOURSE_BASE}/common/variation/historique/{tk}"
    frames = []

    # Step 1: default fetch
    resp = _safe_get(url, timeout=20)
    if resp and "<table" in resp.text.lower():
        df0 = _parse_richbourse_hist_html(resp.text)
        if not df0.empty:
            frames.append(df0)
            logger.debug(f"richbourse hist default: {len(df0)} pts for {tk}")

    # Step 2: date-range windows
    today = datetime.now()
    # Build overlapping windows to cover nb days
    windows = []
    step_days = 180
    end = today
    total_days = nb + 90  # buffer
    while total_days > 0:
        start = end - timedelta(days=step_days)
        windows.append((start, end))
        end = start + timedelta(days=10)  # small overlap
        total_days -= step_days

    for d_start, d_end in windows:
        params = {
            "action": tk,
            "periode": "Journalière",
            "date_debut": d_start.strftime("%Y-%m-%d"),
            "date_fin": d_end.strftime("%Y-%m-%d"),
        }
        resp = _safe_get(url, timeout=20, params=params)
        if resp and "<table" in resp.text.lower():
            df_i = _parse_richbourse_hist_html(resp.text)
            if not df_i.empty:
                frames.append(df_i)
                continue
        # POST fallback (direct, bypasses proxy — last resort)
        try:
            post_data = {
                "action": tk,
                "periode": "Journalière",
                "date_debut": d_start.strftime("%d/%m/%Y"),
                "date_fin": d_end.strftime("%d/%m/%Y"),
            }
            r = requests.post(url, data=post_data, headers=HEADERS, timeout=20, verify=False)
            if r.status_code == 200 and "<table" in r.text.lower():
                df_i = _parse_richbourse_hist_html(r.text)
                if not df_i.empty:
                    frames.append(df_i)
        except Exception:
            pass

    if not frames:
        logger.warning(f"richbourse hist: no data for {tk}")
        return pd.DataFrame()

    merged = (pd.concat(frames, ignore_index=True)
                .drop_duplicates(subset=["date"])
                .dropna(subset=["date", "close"])
                .sort_values("date")
                .reset_index(drop=True))
    logger.info(f"richbourse hist: {len(merged)} pts for {tk}")
    return merged


def _fetch_sikafinance_hist(tk: str) -> pd.DataFrame:
    """Historical data from sikafinance.com."""
    urls = [
        f"https://www.sikafinance.com/charts/gethistory?symbol={tk}&period=5y",
        f"https://www.sikafinance.com/charts/gethistory?symbol={tk}&period=2y",
        f"https://www.sikafinance.com/charts/gethistory?symbol={tk}&period=1y",
    ]
    for url in urls:
        resp = _safe_get(url, timeout=15)
        if not resp:
            continue
        try:
            j = resp.json()
            rows = []
            items = j if isinstance(j, list) else next(
                (v for v in j.values() if isinstance(v, list) and len(v) > 5), [])
            for item in items:
                if not isinstance(item, dict):
                    continue
                d = item.get("date") or item.get("t") or item.get("Date")
                c = item.get("close") or item.get("c") or item.get("Close")
                v = item.get("volume") or item.get("v") or 0
                o = item.get("open") or item.get("o") or None
                h = item.get("high") or item.get("h") or None
                lo = item.get("low") or item.get("l") or None
                if d and c:
                    rows.append({"date": d, "close": c, "volume": v,
                                 "open": o, "high": h, "low": lo})
            if rows:
                df = pd.DataFrame(rows)
                df["date"] = pd.to_datetime(df["date"], errors="coerce")
                for col in ["close", "volume", "open", "high", "low"]:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors="coerce")
                df = df.dropna(subset=["date", "close"]).sort_values("date")
                if len(df) >= 20:
                    return df.reset_index(drop=True)
        except Exception:
            pass

        # Fallback HTML
        if "<table" in resp.text.lower():
            df_h = _parse_hist_html(resp.text)
            if len(df_h) >= 20:
                return df_h

    return pd.DataFrame()


def _fetch_brvm_org_hist(tk: str) -> pd.DataFrame:
    """Historical data from brvm.org."""
    urls = [
        f"https://www.brvm.org/en/cours/show/{tk}/0/BVRM",
        f"https://www.brvm.org/fr/cours/show/{tk}/0/BVRM",
        f"https://www.brvm.org/en/cours-actions/historique/{tk}",
    ]
    for url in urls:
        resp = _safe_get(url, timeout=15)
        if resp and "<table" in resp.text.lower():
            df_h = _parse_hist_html(resp.text)
            if len(df_h) >= 20:
                return df_h
    return pd.DataFrame()


# ══════════════════════════════════════════════════════════════
# FETCH INDICES
# ══════════════════════════════════════════════════════════════

def fetch_indices() -> list[dict]:
    """Fetch BRVM index values."""
    results = []

    # Primary: parse indices from brvm.org EN quotes page (returns 200, Table 2 has BRVM-C/30/PRES)
    resp = _safe_get(BRVM_ORG_EN, timeout=15)
    if resp:
        try:
            dfs = pd.read_html(StringIO(resp.text), thousands=" ", flavor="lxml")
            for df in dfs:
                if len(df.columns) < 3:
                    continue
                # Look for the Market Activities table (contains BRVM-C, BRVM-30, BRVM-PRES)
                first_col = str(df.columns[0]).lower()
                if "market" not in first_col and "activit" not in first_col:
                    continue
                for _, row in df.iterrows():
                    name = str(row.iloc[0]).strip()
                    if "brvm" not in name.lower():
                        continue
                    # col 1 = value, col 2 = change_pct
                    val = _to_float(row.iloc[1]) if len(row) > 1 else None
                    chg = _to_float(row.iloc[2]) if len(row) > 2 else None
                    if val and val > 10:
                        results.append({
                            "name": name,
                            "value": val,
                            "change_pct": chg or 0.0,
                            "source": "brvm.org",
                        })
                if results:
                    break
        except Exception as e:
            logger.warning(f"brvm.org EN indices parse error: {e}")

    # Fallback: brvm.org FR indices page (returns 404 but has valid HTML)
    if not results:
        try:
            import requests as _req
            r = _req.get("https://www.brvm.org/fr/cours-indices/0",
                         headers=HEADERS, timeout=15, verify=False)
            dfs = pd.read_html(StringIO(r.text), thousands=" ", flavor="lxml")
            for df in dfs:
                if len(df.columns) < 3:
                    continue
                first_col = str(df.columns[0]).lower()
                if "activit" not in first_col and "market" not in first_col:
                    continue
                for _, row in df.iterrows():
                    name = str(row.iloc[0]).strip()
                    if "brvm" not in name.lower():
                        continue
                    val = _to_float(row.iloc[1])
                    chg = _to_float(row.iloc[2]) if len(row) > 2 else None
                    if val and val > 10:
                        results.append({
                            "name": name, "value": val,
                            "change_pct": chg or 0.0, "source": "brvm.org",
                        })
                if results:
                    break
        except Exception as e:
            logger.warning(f"brvm.org FR indices parse error: {e}")

    # Try richbourse
    if not results:
        resp = _safe_get(f"{RICHBOURSE_BASE}/common/indices", timeout=15)
        if resp:
            try:
                dfs = pd.read_html(StringIO(resp.text), flavor="lxml")
                for df in dfs:
                    df.columns = [str(c).strip().lower() for c in df.columns]
                    for _, row in df.iterrows():
                        name = str(row.iloc[0]).strip()
                        if "brvm" not in name.lower():
                            continue
                        nums = [_to_float(row.iloc[i]) for i in range(1, len(row))]
                        vals = [v for v in nums if v and v > 10]
                        if vals:
                            results.append({
                                "name": name,
                                "value": vals[0],
                                "change_pct": next((v for v in nums if v is not None and -20 < v < 20 and v != vals[0]), 0.0),
                                "source": "richbourse",
                            })
            except Exception as e:
                logger.warning(f"richbourse indices parse error: {e}")

    return results


# ══════════════════════════════════════════════════════════════
# FETCH NEWS — richbourse actualites
# ══════════════════════════════════════════════════════════════

def fetch_richbourse_news(limit: int = 30) -> list[dict]:
    """
    Scrape latest news articles from richbourse.com/common/actualite/index.
    Returns list of dicts: title, url, published_at, source, summary.
    """
    url = f"{RICHBOURSE_BASE}/common/actualite/index"
    resp = _safe_get(url, timeout=20)
    if not resp:
        return []

    items = []
    try:
        soup = BeautifulSoup(resp.text, "html.parser")
        # Try multiple selectors that richbourse might use for article listings
        articles = (
            soup.find_all("article") or
            soup.find_all(class_=re.compile(r"article|news|actualite|post", re.I)) or
            soup.select(".list-group-item") or
            []
        )

        # Also scan for <a> tags with descriptive text as fallback
        if not articles:
            links = soup.find_all("a", href=re.compile(r"actualite|article|news", re.I))
            for lnk in links[:limit * 2]:
                title = lnk.get_text(strip=True)
                if len(title) < 15:
                    continue
                href = lnk.get("href", "")
                if href.startswith("/"):
                    href = RICHBOURSE_BASE + href
                parent_text = lnk.find_parent()
                pub_date = ""
                if parent_text:
                    date_el = parent_text.find(class_=re.compile(r"date|time", re.I))
                    pub_date = date_el.get_text(strip=True) if date_el else ""
                items.append({
                    "title": title,
                    "url": href,
                    "summary": "",
                    "published_at": pub_date,
                    "source": "richbourse",
                })
        else:
            for a in articles[:limit * 2]:
                title_el = (
                    a.find(["h1", "h2", "h3", "h4", "h5"]) or
                    a.find(class_=re.compile(r"title|titre|heading", re.I)) or
                    a.find("a")
                )
                if not title_el:
                    continue
                title = title_el.get_text(strip=True)
                if len(title) < 10:
                    continue

                link_el = a.find("a", href=True)
                href = ""
                if link_el:
                    href = link_el["href"]
                    if href.startswith("/"):
                        href = RICHBOURSE_BASE + href

                date_el = a.find(["time", "span", "div"],
                                  class_=re.compile(r"date|time|pub", re.I))
                pub_date = date_el.get_text(strip=True) if date_el else ""

                summary_el = a.find("p")
                summary = summary_el.get_text(strip=True)[:300] if summary_el else ""

                items.append({
                    "title": title,
                    "url": href,
                    "summary": summary,
                    "published_at": pub_date,
                    "source": "richbourse",
                })

        # Deduplicate by title
        seen = set()
        unique = []
        for it in items:
            if it["title"] not in seen and len(it["title"]) > 10:
                seen.add(it["title"])
                unique.append(it)
        return unique[:limit]

    except Exception as e:
        logger.warning(f"richbourse news parse error: {e}")
        return []


# ══════════════════════════════════════════════════════════════
# TICKER RESOLVER — company name → BRVM ticker
# ══════════════════════════════════════════════════════════════

def _resolve_ticker_from_name(raw_name: str) -> str:
    """
    Map a company name (as returned by richbourse dividend page) to its
    proper BRVM ticker.
    Combines: explicit alias table → TICKERS_BRVM exact/partial match → fallback.
    """
    from backend.models.reference import TICKERS_BRVM

    if not raw_name:
        return ""
    name_up = raw_name.strip().upper()

    # 1. Direct ticker match (e.g. richbourse already writes the ticker)
    if name_up in TICKERS_BRVM:
        return name_up

    # 2. Explicit alias table (common alternative names on richbourse/sikafinance)
    ALIASES = {
        # Telecoms
        "SONATEL": "SNTS", "SONATEL SENEGAL": "SNTS",
        "ORANGE CI": "ORAC", "ORANGE COTE D'IVOIRE": "ORAC", "ORANGE": "ORAC",
        "ONATEL": "ONTBF", "ONATEL BF": "ONTBF", "ONATEL BURKINA": "ONTBF",
        "ETI": "ETIT", "ECOBANK TOGO": "ETIT", "ECOBANK TRANSNATIONAL": "ETIT",
        # Banks — BOA family
        "BOA BENIN": "BOAB", "BOA SENEGAL": "BOAS", "BOA BURKINA": "BOABF",
        "BOA CI": "BOAC", "BOA MALI": "BOAM", "BOA NIGER": "BOAN",
        "BANK OF AFRICA BENIN": "BOAB", "BANK OF AFRICA SENEGAL": "BOAS",
        "BANK OF AFRICA BURKINA": "BOABF", "BANK OF AFRICA CI": "BOAC",
        "BANK OF AFRICA MALI": "BOAM", "BANK OF AFRICA NIGER": "BOAN",
        # Other banks
        "SGBCI": "SGBC", "SGB": "SGBC", "SGB CI": "SGBC",
        "BICICI": "BICC", "BICI CI": "BICC",
        "NSIA BANQUE": "NSBC", "NSIA CI": "NSBC",
        "CORIS BANK": "CBIBF", "CORIS BF": "CBIBF",
        "SIB": "SIBC", "SOCIETE IVOIRIENNE DE BANQUE": "SIBC",
        "ORAGROUP": "ORGT",
        # Industry & consumer
        "SICABLE": "CABC", "SICABL": "CABC",
        "NESTLE CI": "NTLC", "NESTLE": "NTLC",
        "SOLIBRA": "SLBC",
        "SITAB": "STBC",
        "FILTISAC": "FTSC",
        "PALM CI": "PALC", "PALM": "PALC",
        "SAPH": "SPHC",
        "SOGB": "SOGC",
        "SUCRIVOIRE": "SCRC",
        "UNILEVER CI": "UNLC", "UNILEVER": "UNLC",
        "UNIWAX": "UNXC",
        "SMB CI": "SMBC", "SMB": "SMBC",
        "CFAO": "CFAC", "CFAO MOTORS": "CFAC",
        "SETAO": "STAC",
        "BERNABE": "BNBC",
        # Energy
        "TOTAL CI": "TTLC", "TOTAL COTE D'IVOIRE": "TTLC",
        "TOTAL SENEGAL": "TTLS",
        "VIVO ENERGY": "SHEC", "VIVO CI": "SHEC",
        "SODECI": "SDCC",
        "CIE": "CIEC", "CIE CI": "CIEC",
        # Others
        "ECOBANK CI": "ECOC", "ECOBANK COTE D'IVOIRE": "ECOC",
        "TRACTAFRIC": "PRSC", "TRACTAFRIC MOTORS": "PRSC",
        "NEI CEDA": "NEIC", "NEI-CEDA": "NEIC",
        "SICOR": "SICC",
        "SERVAIR": "ABJC",
        "ALIOS FINANCE": "SAFC", "SAFCA": "SAFC",
        "LOTERIE BENIN": "LNBB", "LNBB": "LNBB",
        "AFRICA GLOBAL LOGISTICS": "SDSC", "AGL": "SDSC",
        "ERIUM": "SIVC",
        "EVIOSYS": "SEMC", "SIEM": "SEMC",
    }
    if name_up in ALIASES:
        return ALIASES[name_up]

    # 3. Partial alias match (starts with key)
    for alias, tk in ALIASES.items():
        if name_up.startswith(alias) or alias.startswith(name_up):
            return tk

    # 4. Scan TICKERS_BRVM reference names for containment
    best_tk, best_score = "", 0
    for tk, info in TICKERS_BRVM.items():
        ref = info[0].upper()
        # Score = number of characters in common words (min 3 chars)
        words = [w for w in name_up.split() if len(w) >= 4]
        score = sum(4 for w in words if w in ref)
        if score > best_score:
            best_score, best_tk = score, tk

    if best_score >= 4:
        return best_tk

    # 5. Last resort: first token if it looks like a ticker
    m = re.match(r'^([A-Z]{3,6})\b', name_up)
    return m.group(1) if m else ""


# ══════════════════════════════════════════════════════════════
# FETCH DIVIDENDS — richbourse dividend calendar
# ══════════════════════════════════════════════════════════════

def fetch_richbourse_dividends(limit: int = 60) -> list[dict]:
    """
    Scrape dividend calendar from richbourse.com/common/dividende/index.
    Returns list of dicts: ticker, dividend, ex_date, payment_date, yield_pct.
    """
    url = f"{RICHBOURSE_BASE}/common/dividende/index"
    resp = _safe_get(url, timeout=20)
    if not resp:
        return []

    items = []
    try:
        dfs = pd.read_html(StringIO(resp.text), flavor="lxml")
    except Exception:
        dfs = []

    for df in dfs:
        if df.empty or len(df.columns) < 3:
            continue
        df.columns = [
            str(c).strip().lower()
            .replace(" ", "").replace("\xa0", "")
            .replace("é", "e").replace("è", "e").replace("ê", "e")
            for c in df.columns
        ]
        col_tk = next((c for c in df.columns if any(k in c for k in
            ["symbole", "ticker", "code", "valeur", "titre", "action", "societe"])), None)
        col_div = next((c for c in df.columns if any(k in c for k in
            ["dividende", "montant", "div", "distribution"])), None)
        col_date = next((c for c in df.columns if any(k in c for k in
            ["date", "detachement", "exdate", "ex", "coupon", "detach"])), None)
        col_pay = next((c for c in df.columns if any(k in c for k in
            ["paiement", "payment", "versement", "mise"])), None)
        col_yield = next((c for c in df.columns if any(k in c for k in
            ["rendement", "yield", "taux", "rdt"])), None)

        if not col_tk:
            continue

        for _, row in df.iterrows():
            raw_tk = str(row[col_tk]).strip().upper()
            company_name = str(row.get(col_tk, raw_tk)).strip()
            # Resolve proper BRVM ticker from company name
            tk = _resolve_ticker_from_name(raw_tk)
            if not tk:
                continue

            items.append({
                "ticker": tk,
                "company": str(row.get(col_tk, tk)).strip(),
                "dividend": _to_float(row[col_div]) if col_div else None,
                "ex_date": str(row[col_date]).strip() if col_date else "",
                "payment_date": str(row[col_pay]).strip() if col_pay else "",
                "yield_pct": _to_float(row[col_yield]) if col_yield else None,
                "source": "richbourse",
            })
        if items:
            break

    # HTML fallback
    if not items:
        try:
            soup = BeautifulSoup(resp.text, "html.parser")
            for table in soup.find_all("table"):
                rows = table.find_all("tr")
                if len(rows) < 2:
                    continue
                for tr in rows[1:]:
                    cells = [td.get_text(strip=True) for td in tr.find_all("td")]
                    if len(cells) < 2:
                        continue
                    tk = None
                    for c in cells:
                        m = re.match(r'^([A-Z]{3,6})(?:\s|$|-)', c.strip().upper())
                        if m:
                            tk = m.group(1)
                            break
                    if not tk:
                        continue
                    items.append({
                        "ticker": tk,
                        "company": cells[0] if cells else tk,
                        "dividend": _to_float(cells[1]) if len(cells) > 1 else None,
                        "ex_date": cells[2] if len(cells) > 2 else "",
                        "payment_date": cells[3] if len(cells) > 3 else "",
                        "yield_pct": _to_float(cells[4]) if len(cells) > 4 else None,
                        "source": "richbourse",
                    })
        except Exception as e:
            logger.warning(f"richbourse dividend HTML parse error: {e}")

    return items[:limit]
