"""
BRVM Trading Desk — Marché page
Full market overview with filterable, sortable quotes table.
"""
import pandas as pd
import streamlit as st

from backend.db.sync_db import query
from backend.models.reference import SECTORS

st.set_page_config(page_title="Marché", page_icon="📊", layout="wide")

st.title("📊 Vue du Marché")


# ── Data ─────────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def _load_market() -> pd.DataFrame:
    rows = query(
        """
        SELECT q.ticker, s.name, s.sector, s.country,
               q.close, q.change_pct, q.volume, q.value, q.date,
               f.market_cap, f.shares_outstanding, f.dividend, f.per
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
    for c in ["close", "change_pct", "volume", "value", "market_cap", "per", "dividend"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    # Recompute market_cap from live price × shares if not stored
    mask = df["market_cap"].isna() & df["shares_outstanding"].notna() & df["close"].notna()
    df.loc[mask, "market_cap"] = (
        df.loc[mask, "close"] * df.loc[mask, "shares_outstanding"] / 1e6
    )
    return df


# ── Sidebar filters ───────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("Filtres")
    selected_sectors = st.multiselect(
        "Secteur",
        options=SECTORS,
        default=SECTORS,
        help="Filtrer par secteur d'activité",
    )
    var_min = st.number_input("Variation min (%)", value=-10.0, step=0.5)
    var_max = st.number_input("Variation max (%)", value=10.0, step=0.5)
    vol_min = st.number_input("Volume min", value=0, step=100)

    if st.button("🔄 Actualiser"):
        st.cache_data.clear()
        st.rerun()

# ── Load + filter ─────────────────────────────────────────────────────────────────
df = _load_market()

if df.empty:
    st.info(
        "Aucune donnée de cours disponible. "
        "Les données seront chargées lors de la première synchronisation (~5 min)."
    )
    st.stop()

df_filtered = df.copy()

if selected_sectors:
    df_filtered = df_filtered[df_filtered["sector"].isin(selected_sectors)]

if "change_pct" in df_filtered.columns:
    df_filtered = df_filtered[
        (df_filtered["change_pct"].isna()) |
        (df_filtered["change_pct"].between(var_min, var_max))
    ]

if "volume" in df_filtered.columns:
    df_filtered = df_filtered[
        (df_filtered["volume"].isna()) |
        (df_filtered["volume"] >= vol_min)
    ]

# ── Metrics row ───────────────────────────────────────────────────────────────────
n_total = len(df_filtered)
total_vol_mfcfa = df_filtered["value"].fillna(0).sum() / 1e6
n_advancing = int((df_filtered["change_pct"].fillna(0) > 0).sum())
n_declining = int((df_filtered["change_pct"].fillna(0) < 0).sum())

m1, m2, m3, m4 = st.columns(4)
m1.metric("Titres affichés", n_total)
m2.metric("Volume (MFCFA)", f"{total_vol_mfcfa:,.1f}")
m3.metric("En hausse", n_advancing)
m4.metric("En baisse", n_declining)

st.divider()

# ── Main quotes table ─────────────────────────────────────────────────────────────
st.subheader("Cours des titres BRVM")

display_cols = {
    "ticker": "Ticker",
    "name": "Société",
    "sector": "Secteur",
    "country": "Pays",
    "close": "Cours (FCFA)",
    "change_pct": "Var %",
    "volume": "Volume",
    "market_cap": "Mkt Cap (MFCFA)",
    "per": "PER",
    "date": "Date",
}

df_show = df_filtered[[c for c in display_cols if c in df_filtered.columns]].copy()
df_show = df_show.rename(columns=display_cols)

if "Var %" in df_show.columns:
    df_show["Var %"] = pd.to_numeric(df_show["Var %"], errors="coerce")

col_config = {
    "Cours (FCFA)": st.column_config.NumberColumn(
        "Cours (FCFA)", format="%,.0f"
    ),
    "Var %": st.column_config.NumberColumn(
        "Var %", format="%.2f%%"
    ),
    "Volume": st.column_config.NumberColumn(
        "Volume", format="%,d"
    ),
    "Mkt Cap (MFCFA)": st.column_config.NumberColumn(
        "Mkt Cap (MFCFA)", format="%,.1f"
    ),
    "PER": st.column_config.NumberColumn(
        "PER", format="%.1f"
    ),
}


def _color_var(val):
    if pd.isna(val):
        return ""
    if val > 0:
        return "color: #26a69a; font-weight: bold"
    if val < 0:
        return "color: #ef5350; font-weight: bold"
    return ""


styled = df_show.style.map(_color_var, subset=["Var %"] if "Var %" in df_show.columns else [])

st.dataframe(
    styled,
    use_container_width=True,
    hide_index=True,
    column_config=col_config,
    height=600,
)

if "date" in df_filtered.columns and not df_filtered["date"].dropna().empty:
    latest = df_filtered["date"].dropna().max()
    st.caption(f"Données au : {latest} · {n_total} titres affichés")
