"""
BRVM Trading Desk — Screener page
Multi-criteria fundamental + technical stock screener.
"""
import pandas as pd
import streamlit as st

from backend.db.sync_db import query
from backend.models.reference import SECTORS, TICKERS_BRVM, PER_SECTORIELS
from backend.scrapers.fundamentals import compute_ratios

st.set_page_config(page_title="Screener", page_icon="🔍", layout="wide")

st.title("🔍 Screener BRVM")


# ── Data ──────────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def _load_screener_data() -> pd.DataFrame:
    rows = query(
        """
        SELECT q.ticker,
               s.name,
               s.sector,
               s.country,
               q.close,
               q.change_pct,
               q.volume,
               f.shares_outstanding,
               f.dividend,
               f.eps_prev,
               f.eps_n2,
               f.equity,
               f.net_income,
               f.total_assets,
               f.total_debt,
               f.market_cap,
               f.per AS stored_per,
               f.is_bank
        FROM daily_quotes q
        LEFT JOIN stocks s ON q.ticker = s.ticker
        LEFT JOIN fundamentals f ON q.ticker = f.ticker
        WHERE q.date = (
            SELECT MAX(date) FROM daily_quotes dq2 WHERE dq2.ticker = q.ticker
        )
        ORDER BY q.ticker
        """
    )
    df = pd.DataFrame(rows) if rows else pd.DataFrame()
    if df.empty:
        return df
    for c in ["close", "change_pct", "volume", "dividend", "eps_prev", "eps_n2",
              "equity", "net_income", "total_assets", "total_debt",
              "market_cap", "stored_per", "shares_outstanding"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def _enrich_with_ratios(df: pd.DataFrame) -> pd.DataFrame:
    """Add computed ratio columns to the dataframe."""
    records = df.to_dict("records")
    enriched = []
    for rec in records:
        price = rec.get("close")
        if not price or price <= 0:
            enriched.append(rec)
            continue
        ratios = compute_ratios(rec, price)
        rec.update({
            "per": ratios.get("per"),
            "div_yield": ratios.get("div_yield"),
            "roe": ratios.get("roe"),
            "market_cap": ratios.get("market_cap") or rec.get("market_cap"),
            "debt_equity": ratios.get("debt_equity"),
            "eps_growth": ratios.get("eps_growth"),
        })
        enriched.append(rec)
    return pd.DataFrame(enriched)


def _compute_score(row: pd.Series) -> float:
    """
    Composite score 0–100:
      Momentum   25%: change_pct (clipped ±7.5%)
      Technical  25%: RSI not available at row level → use price vs 52w range proxy
      Value      20%: PER vs sector benchmark
      Quality    30%: ROE (10%) + dividend yield (10%) + EPS growth (10%)
    """
    score = 0.0

    # Momentum (25%): normalise change_pct from -7.5 to +7.5
    chg = row.get("change_pct") or 0.0
    momentum = min(max((chg + 7.5) / 15.0, 0), 1) * 25
    score += momentum

    # Technical proxy (25%): reward positive change + volume presence
    vol = row.get("volume") or 0
    tech = 12.5  # neutral baseline
    if chg > 0:
        tech += 6.25
    if vol > 1000:
        tech += 6.25
    score += tech

    # Value (20%): PER vs sector benchmark
    per = row.get("per")
    sector = row.get("sector", "")
    sector_per = PER_SECTORIELS.get(sector, 15.0)
    if per and per > 0:
        # Under-valued (PER below sector) scores higher
        ratio = sector_per / per
        value_score = min(ratio, 2.0) / 2.0 * 20
        score += value_score
    else:
        score += 10  # neutral if no PER data

    # Quality (30%)
    roe = row.get("roe")
    div_yield = row.get("div_yield")
    eps_growth = row.get("eps_growth")

    # ROE (10%): 20%+ ROE gets full marks
    if roe and roe > 0:
        roe_score = min(roe / 20.0, 1.0) * 10
        score += roe_score
    else:
        score += 5  # neutral

    # Dividend yield (10%): 5%+ yield gets full marks
    if div_yield and div_yield > 0:
        div_score = min(div_yield / 5.0, 1.0) * 10
        score += div_score
    else:
        score += 5  # neutral

    # EPS growth (10%): 10%+ growth gets full marks
    if eps_growth and eps_growth > 0:
        growth_score = min(eps_growth / 10.0, 1.0) * 10
        score += growth_score
    else:
        score += 5  # neutral

    return round(score, 1)


# ── Sidebar filters ───────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("Filtres")

    selected_sectors = st.multiselect("Secteur", SECTORS, default=SECTORS)

    st.subheader("Variation (%)")
    var_min = st.number_input("Var min", value=-2.0, step=0.5)
    var_max = st.number_input("Var max", value=5.0, step=0.5)

    st.subheader("Valorisation")
    per_max = st.number_input("PER max", value=30.0, step=1.0, help="Laisser à 30 pour ne pas filtrer")
    div_min = st.number_input("Rendement div. min (%)", value=0.0, step=0.5)

    st.subheader("Qualité")
    roe_min = st.number_input("ROE min (%)", value=0.0, step=1.0)
    vol_min = st.number_input("Volume min", value=0, step=100)

    only_dividend = st.checkbox("Versement dividende uniquement")

    if st.button("🔄 Actualiser"):
        st.cache_data.clear()
        st.rerun()

# ── Build screener ────────────────────────────────────────────────────────────────
df_raw = _load_screener_data()

if df_raw.empty:
    st.info(
        "Aucune donnée disponible. Les cours seront chargés lors de la prochaine "
        "synchronisation (~5 min)."
    )
    st.stop()

df = _enrich_with_ratios(df_raw)

# Apply filters
mask = pd.Series([True] * len(df), index=df.index)

if selected_sectors:
    mask &= df["sector"].isin(selected_sectors)

mask &= (df["change_pct"].isna()) | (df["change_pct"].between(var_min, var_max))

if per_max < 30.0:
    mask &= df["per"].isna() | (df["per"] <= per_max)

if div_min > 0:
    mask &= df["div_yield"].notna() & (df["div_yield"] >= div_min)

if roe_min > 0:
    mask &= df["roe"].notna() & (df["roe"] >= roe_min)

if vol_min > 0:
    mask &= (df["volume"].fillna(0) >= vol_min)

if only_dividend:
    mask &= df["dividend"].notna() & (df["dividend"] > 0)

df_filtered = df[mask].copy()

# Compute scores
df_filtered["score"] = df_filtered.apply(_compute_score, axis=1)
df_filtered = df_filtered.sort_values("score", ascending=False).reset_index(drop=True)

# ── Display ────────────────────────────────────────────────────────────────────────
st.metric("Titres correspondant à vos critères", len(df_filtered))

if df_filtered.empty:
    st.warning("Aucun titre ne correspond aux critères sélectionnés. Élargissez vos filtres.")
    st.stop()

st.divider()

# Build display table
cols_to_show = ["score", "ticker", "name", "sector", "close", "change_pct",
                "volume", "per", "div_yield", "roe", "market_cap"]
df_show = df_filtered[[c for c in cols_to_show if c in df_filtered.columns]].copy()

df_show = df_show.rename(columns={
    "score": "Score",
    "ticker": "Ticker",
    "name": "Société",
    "sector": "Secteur",
    "close": "Cours (FCFA)",
    "change_pct": "Var %",
    "volume": "Volume",
    "per": "PER",
    "div_yield": "Div %",
    "roe": "ROE %",
    "market_cap": "MktCap (MFCFA)",
})


def _color_score(val):
    if pd.isna(val):
        return ""
    if val >= 70:
        return "color: #26a69a; font-weight: bold"
    if val >= 50:
        return "color: #ff9800"
    return "color: #ef5350"


def _color_var(val):
    if pd.isna(val):
        return ""
    if val > 0:
        return "color: #26a69a"
    if val < 0:
        return "color: #ef5350"
    return ""


styled = df_show.style.map(
    _color_score, subset=["Score"] if "Score" in df_show.columns else []
).map(
    _color_var, subset=["Var %"] if "Var %" in df_show.columns else []
)

st.dataframe(
    styled,
    use_container_width=True,
    hide_index=True,
    height=500,
    column_config={
        "Score": st.column_config.NumberColumn("Score", format="%.1f"),
        "Cours (FCFA)": st.column_config.NumberColumn("Cours (FCFA)", format="%,.0f"),
        "Var %": st.column_config.NumberColumn("Var %", format="%.2f%%"),
        "Volume": st.column_config.NumberColumn("Volume", format="%,d"),
        "PER": st.column_config.NumberColumn("PER", format="%.1f"),
        "Div %": st.column_config.NumberColumn("Div %", format="%.2f%%"),
        "ROE %": st.column_config.NumberColumn("ROE %", format="%.1f%%"),
        "MktCap (MFCFA)": st.column_config.NumberColumn("MktCap (MFCFA)", format="%,.1f"),
    },
)

st.divider()

st.subheader("Analyser un titre")
selected_ticker = st.selectbox(
    "Sélectionner un titre pour l'analyse",
    options=df_filtered["ticker"].tolist(),
    format_func=lambda t: f"{t} — {TICKERS_BRVM.get(t, (t,))[0]}",
)

if st.button("Voir l'analyse technique →"):
    st.session_state["selected_ticker"] = selected_ticker
    st.switch_page("pages/2_Analyse.py")
