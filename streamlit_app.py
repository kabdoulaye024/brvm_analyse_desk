"""
BRVM Trading Desk — Home Page
Streamlit entry point. Displays market overview, indices, top movers.
"""
import logging

import pandas as pd
import streamlit as st

from backend.db.sync_db import init_db_sync, query
from backend.jobs.scheduler import start_scheduler, _do_initial_sync

logging.basicConfig(level=logging.INFO)

st.set_page_config(
    page_title="BRVM Trading Desk",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Bootstrap (runs once per Streamlit server process) ─────────────────────────
@st.cache_resource
def _init():
    """Initialise DB schema + seed data, then launch background scheduler."""
    try:
        init_db_sync()
    except Exception as e:
        logging.getLogger(__name__).error(f"DB init error: {e}")
    try:
        start_scheduler()
    except Exception as e:
        logging.getLogger(__name__).error(f"Scheduler start error: {e}")


_init()


# ── Data helpers ────────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def _load_quotes() -> pd.DataFrame:
    rows = query(
        """
        SELECT q.ticker, s.name, s.sector,
               q.close, q.change_pct, q.volume, q.date
        FROM daily_quotes q
        LEFT JOIN stocks s ON q.ticker = s.ticker
        WHERE q.date = (
            SELECT MAX(date) FROM daily_quotes dq2 WHERE dq2.ticker = q.ticker
        )
        ORDER BY q.ticker
        """
    )
    return pd.DataFrame(rows) if rows else pd.DataFrame()


@st.cache_data(ttl=300)
def _load_indices() -> pd.DataFrame:
    rows = query(
        """
        SELECT i.index_name, i.value, i.change_pct
        FROM indices i
        INNER JOIN (
            SELECT index_name, MAX(date) AS md
            FROM indices
            GROUP BY index_name
        ) m ON i.index_name = m.index_name AND i.date = m.md
        ORDER BY i.index_name
        """
    )
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def _fmt_change(val: float) -> str:
    if val is None:
        return "—"
    sign = "+" if val > 0 else ""
    return f"{sign}{val:.2f}%"


# ── Page layout ─────────────────────────────────────────────────────────────────
st.title("📈 BRVM Trading Desk")
st.caption("Bourse Régionale des Valeurs Mobilières · Abidjan, Côte d'Ivoire")

# Refresh button
col_refresh, col_force, _ = st.columns([1, 2, 7])
with col_refresh:
    if st.button("🔄 Actualiser"):
        st.cache_data.clear()
        st.rerun()
with col_force:
    if st.button("⚡ Forcer synchronisation"):
        with st.spinner("Synchronisation en cours…"):
            _do_initial_sync()
        st.cache_data.clear()
        st.rerun()

st.divider()

# ── Row 1: Index metrics ────────────────────────────────────────────────────────
df_idx = _load_indices()

DISPLAY_INDICES = ["BRVM Composite", "BRVM 30", "BRVM Prestige", "BRVM Principal"]

if df_idx.empty:
    st.info(
        "Synchronisation initiale en cours… Les données apparaîtront dans quelques secondes. "
        "Si le chargement prend plus de 30 s, cliquez sur **⚡ Forcer synchronisation**."
    )
else:
    metric_cols = st.columns(len(DISPLAY_INDICES))
    for col, idx_name in zip(metric_cols, DISPLAY_INDICES):
        row = df_idx[df_idx["index_name"] == idx_name]
        if row.empty:
            col.metric(idx_name, "—", "—")
        else:
            val = float(row.iloc[0]["value"])
            chg = float(row.iloc[0]["change_pct"])
            col.metric(
                label=idx_name,
                value=f"{val:,.2f}",
                delta=f"{chg:+.2f}%",
                delta_color="normal",
            )

st.divider()

# ── Load quote data ─────────────────────────────────────────────────────────────
df_q = _load_quotes()

if df_q.empty:
    st.info(
        "Aucune donnée de cours disponible encore. "
        "Cliquez sur **⚡ Forcer synchronisation** pour récupérer les données immédiatement."
    )
    st.stop()

# Ensure numeric types
for col_name in ["close", "change_pct", "volume"]:
    if col_name in df_q.columns:
        df_q[col_name] = pd.to_numeric(df_q[col_name], errors="coerce")

df_q = df_q.dropna(subset=["close"])

# ── Row 2: Top movers ────────────────────────────────────────────────────────────
st.subheader("Meilleures et pires performances du jour")

df_valid = df_q.dropna(subset=["change_pct"]).copy()
df_gainers = df_valid.nlargest(5, "change_pct")[
    ["ticker", "name", "close", "change_pct", "volume"]
].copy()
df_losers = df_valid.nsmallest(5, "change_pct")[
    ["ticker", "name", "close", "change_pct", "volume"]
].copy()

col_gain, col_lose = st.columns(2)

def _style_change(val):
    color = "#26a69a" if val > 0 else "#ef5350" if val < 0 else "#fafafa"
    return f"color: {color}; font-weight: bold"

with col_gain:
    st.markdown("**Top 5 Hausses**")
    df_gainers_display = df_gainers.rename(columns={
        "ticker": "Ticker", "name": "Société",
        "close": "Cours (FCFA)", "change_pct": "Var %", "volume": "Volume"
    })
    df_gainers_display["Var %"] = df_gainers_display["Var %"].map(lambda x: f"+{x:.2f}%")
    df_gainers_display["Cours (FCFA)"] = df_gainers_display["Cours (FCFA)"].map(
        lambda x: f"{x:,.0f}"
    )
    df_gainers_display["Volume"] = df_gainers_display["Volume"].map(
        lambda x: f"{int(x):,}" if pd.notna(x) else "—"
    )
    st.dataframe(
        df_gainers_display,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Var %": st.column_config.TextColumn("Var %"),
        },
    )

with col_lose:
    st.markdown("**Top 5 Baisses**")
    df_losers_display = df_losers.rename(columns={
        "ticker": "Ticker", "name": "Société",
        "close": "Cours (FCFA)", "change_pct": "Var %", "volume": "Volume"
    })
    df_losers_display["Var %"] = df_losers_display["Var %"].map(lambda x: f"{x:.2f}%")
    df_losers_display["Cours (FCFA)"] = df_losers_display["Cours (FCFA)"].map(
        lambda x: f"{x:,.0f}"
    )
    df_losers_display["Volume"] = df_losers_display["Volume"].map(
        lambda x: f"{int(x):,}" if pd.notna(x) else "—"
    )
    st.dataframe(
        df_losers_display,
        use_container_width=True,
        hide_index=True,
    )

st.divider()

# ── Row 3: Market summary ────────────────────────────────────────────────────────
st.subheader("Résumé du marché")

n_up = int((df_valid["change_pct"] > 0).sum())
n_down = int((df_valid["change_pct"] < 0).sum())
n_flat = int((df_valid["change_pct"] == 0).sum())
total_vol = int(df_q["volume"].fillna(0).sum())
n_total = len(df_q)

s1, s2, s3, s4, s5 = st.columns(5)
s1.metric("Titres cotés", n_total)
s2.metric("En hausse", n_up, delta=None)
s3.metric("En baisse", n_down, delta=None)
s4.metric("Stables", n_flat, delta=None)
s5.metric("Volume total", f"{total_vol:,}")

# Latest data date
if "date" in df_q.columns and not df_q["date"].dropna().empty:
    latest_date = df_q["date"].dropna().max()
    st.caption(f"Dernière mise à jour des cours : {latest_date}")
