"""
BRVM Trading Desk — Analyse Technique page
Full interactive Plotly candlestick/HA chart with Bollinger Bands, RSI, Volume.
"""
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

from backend.db.sync_db import query, query_one
from backend.models.reference import TICKERS_BRVM
from backend.scrapers.courses import fetch_history
from backend.scrapers.technicals import calc_bollinger, calc_rsi
from backend.scrapers.fundamentals import compute_ratios

st.set_page_config(page_title="Analyse", page_icon="📈", layout="wide")

st.title("📈 Analyse Technique")

# ── Sidebar controls ──────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("Paramètres")

    ticker_options = sorted(TICKERS_BRVM.keys())
    # Allow pre-selection from session state (e.g. from screener page link)
    default_ticker = st.session_state.get("selected_ticker", ticker_options[0])
    if default_ticker not in ticker_options:
        default_ticker = ticker_options[0]

    ticker = st.selectbox(
        "Titre",
        options=ticker_options,
        index=ticker_options.index(default_ticker),
        format_func=lambda t: f"{t} — {TICKERS_BRVM[t][0]}",
    )
    st.session_state["selected_ticker"] = ticker

    period = st.radio("Période", ["1M", "3M", "6M", "1A"], horizontal=True, index=1)
    chart_type = st.radio("Type", ["Chandeliers", "Heikin-Ashi"], horizontal=True)
    show_bb = st.checkbox("Bollinger Bands", value=True)
    show_volume = st.checkbox("Volume", value=True)
    show_rsi = st.checkbox("RSI (14)", value=True)

    if st.button("🔄 Actualiser"):
        st.cache_data.clear()
        st.rerun()

# ── Data loading ──────────────────────────────────────────────────────────────────
DAYS_MAP = {"1M": 30, "3M": 90, "6M": 180, "1A": 365}
days = DAYS_MAP[period]


@st.cache_data(ttl=300)
def _load_ohlcv(ticker: str, days: int) -> pd.DataFrame:
    rows = query(
        """
        SELECT date, open, high, low, close, volume
        FROM daily_quotes
        WHERE ticker = ?
          AND date >= date('now', ? || ' days')
        ORDER BY date ASC
        """,
        (ticker, f"-{days}"),
    )
    df = pd.DataFrame(rows) if rows else pd.DataFrame()
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"])
    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


@st.cache_data(ttl=300)
def _load_fundamentals(ticker: str) -> dict:
    row = query_one("SELECT * FROM fundamentals WHERE ticker = ?", (ticker,))
    return dict(row) if row else {}


@st.cache_data(ttl=300)
def _load_latest_quote(ticker: str) -> dict:
    row = query_one(
        """
        SELECT close, change_pct, volume, date
        FROM daily_quotes
        WHERE ticker = ?
        ORDER BY date DESC
        LIMIT 1
        """,
        (ticker,),
    )
    return dict(row) if row else {}


df = _load_ohlcv(ticker, days)


@st.cache_data(ttl=300)
def _load_live_history(ticker: str, days: int) -> tuple[pd.DataFrame, str]:
    try:
        live = fetch_history(ticker, days)
        if live is not None and not live.empty:
            live = live.copy()
            live["date"] = pd.to_datetime(live["date"])
            for c in ["open", "high", "low", "close", "volume"]:
                if c in live.columns:
                    live[c] = pd.to_numeric(live[c], errors="coerce")
            return live, ""
    except Exception as e:
        return pd.DataFrame(), str(e)
    return pd.DataFrame(), "fetch_history returned None or empty"


# Fall back to live scraper when DB has fewer than 20 rows
_live_err = ""
if len(df) < 20:
    with st.spinner(f"Récupération des données historiques pour {ticker}…"):
        live, _live_err = _load_live_history(ticker, days)
        if not live.empty:
            df = live

if df.empty or len(df) < 3:
    ticker_name = TICKERS_BRVM.get(ticker, (ticker,))[0]
    st.info(
        f"Pas assez de données historiques pour **{ticker} — {ticker_name}** "
        f"sur la période sélectionnée. Essayez une période plus longue ou revenez "
        "après la prochaine synchronisation."
    )
    with st.expander("🔍 Debug — détails d'erreur"):
        import os
        try:
            import streamlit as _st
            cf = _st.secrets.get("CF_WORKER_URL", os.environ.get("CF_WORKER_URL", ""))
        except Exception:
            cf = os.environ.get("CF_WORKER_URL", "")
        st.write(f"**CF_WORKER_URL configuré:** `{'oui — ' + cf[:40] if cf else 'non'}`")
        if _live_err:
            st.write(f"**Erreur fetch_history:** `{_live_err}`")
        else:
            st.write("fetch_history n'a retourné aucune donnée (pas d'exception)")
    st.stop()

# Fill missing OHLC from close if needed
for col_name in ["open", "high", "low"]:
    if col_name not in df.columns or df[col_name].isna().all():
        df[col_name] = df["close"]
    else:
        df[col_name] = df[col_name].fillna(df["close"])

df = df.dropna(subset=["close"]).reset_index(drop=True)

# ── Heikin-Ashi computation ───────────────────────────────────────────────────────
def _heikin_ashi(df: pd.DataFrame) -> pd.DataFrame:
    ha = df.copy()
    ha["ha_close"] = (df["open"] + df["high"] + df["low"] + df["close"]) / 4
    ha_open = [0.0] * len(df)
    ha_open[0] = (df["open"].iloc[0] + df["close"].iloc[0]) / 2
    for i in range(1, len(df)):
        ha_open[i] = (ha_open[i - 1] + ha["ha_close"].iloc[i - 1]) / 2
    ha["ha_open"] = ha_open
    ha["ha_high"] = ha[["high", "ha_open", "ha_close"]].max(axis=1)
    ha["ha_low"] = ha[["low", "ha_open", "ha_close"]].min(axis=1)
    return ha


# ── Indicators ────────────────────────────────────────────────────────────────────
close_series = df["close"].reset_index(drop=True)

bb_data = None
if show_bb and len(df) >= 20:
    bb_data = calc_bollinger(close_series, period=20, std_dev=2.0)

rsi_series = None
if show_rsi and len(df) >= 14:
    rsi_series = calc_rsi(close_series, period=14)

# ── Build subplot layout ──────────────────────────────────────────────────────────
n_rows = 1
row_heights = [0.6]

vol_row = None
rsi_row = None

if show_volume:
    n_rows += 1
    vol_row = n_rows
    row_heights.append(0.15)

if show_rsi:
    n_rows += 1
    rsi_row = n_rows
    row_heights.append(0.25)

# Normalize heights so they sum to 1
total = sum(row_heights)
row_heights = [h / total for h in row_heights]

fig = make_subplots(
    rows=n_rows,
    cols=1,
    shared_xaxes=True,
    vertical_spacing=0.03,
    row_heights=row_heights,
)

# ── Row 1: Price chart ────────────────────────────────────────────────────────────
if chart_type == "Heikin-Ashi":
    ha_df = _heikin_ashi(df)
    o_col, h_col, l_col, c_col = "ha_open", "ha_high", "ha_low", "ha_close"
else:
    ha_df = df
    o_col, h_col, l_col, c_col = "open", "high", "low", "close"

fig.add_trace(
    go.Candlestick(
        x=df["date"],
        open=ha_df[o_col],
        high=ha_df[h_col],
        low=ha_df[l_col],
        close=ha_df[c_col],
        name=ticker,
        increasing=dict(line=dict(color="#26a69a"), fillcolor="#26a69a"),
        decreasing=dict(line=dict(color="#ef5350"), fillcolor="#ef5350"),
        showlegend=False,
    ),
    row=1,
    col=1,
)

# Bollinger Bands overlay
if bb_data is not None:
    upper = bb_data["upper"]
    mid = bb_data["middle"]
    lower = bb_data["lower"]
    dates = df["date"].tolist()

    fig.add_trace(
        go.Scatter(
            x=dates,
            y=upper,
            name="BB Supérieure",
            line=dict(color="#ff9800", width=1, dash="dash"),
            opacity=0.8,
        ),
        row=1,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=dates,
            y=mid,
            name="BB Milieu",
            line=dict(color="#ff9800", width=1),
            opacity=0.8,
        ),
        row=1,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=dates,
            y=lower,
            name="BB Inférieure",
            line=dict(color="#ff9800", width=1, dash="dash"),
            opacity=0.8,
            fill="tonexty",
            fillcolor="rgba(255,152,0,0.05)",
        ),
        row=1,
        col=1,
    )

# ── Volume row ────────────────────────────────────────────────────────────────────
if show_volume and vol_row is not None and "volume" in df.columns:
    vol = df["volume"].fillna(0)
    # Color bars green/red by candle direction
    closes = ha_df[c_col].values
    opens = ha_df[o_col].values
    vol_colors = [
        "#26a69a" if closes[i] >= opens[i] else "#ef5350"
        for i in range(len(df))
    ]
    fig.add_trace(
        go.Bar(
            x=df["date"],
            y=vol,
            name="Volume",
            marker_color=vol_colors,
            showlegend=False,
            opacity=0.7,
        ),
        row=vol_row,
        col=1,
    )
    fig.update_yaxes(
        title_text="Volume",
        row=vol_row,
        col=1,
        gridcolor="#1e2130",
        color="#fafafa",
    )

# ── RSI row ───────────────────────────────────────────────────────────────────────
if show_rsi and rsi_row is not None and rsi_series is not None:
    rsi_vals = rsi_series.tolist()
    fig.add_trace(
        go.Scatter(
            x=df["date"],
            y=rsi_vals,
            name="RSI(14)",
            line=dict(color="#ce93d8", width=1.5),
        ),
        row=rsi_row,
        col=1,
    )
    # Overbought / oversold lines
    fig.add_hline(
        y=70,
        line=dict(color="#ef5350", width=1, dash="dash"),
        row=rsi_row,
        col=1,
    )
    fig.add_hline(
        y=30,
        line=dict(color="#26a69a", width=1, dash="dash"),
        row=rsi_row,
        col=1,
    )
    fig.update_yaxes(
        title_text="RSI",
        range=[0, 100],
        row=rsi_row,
        col=1,
        gridcolor="#1e2130",
        color="#fafafa",
    )

# ── Global layout ─────────────────────────────────────────────────────────────────
ticker_name = TICKERS_BRVM.get(ticker, (ticker,))[0]

fig.update_layout(
    paper_bgcolor="#0e1117",
    plot_bgcolor="#0e1117",
    font=dict(color="#fafafa", size=12),
    height=650,
    margin=dict(l=0, r=0, t=20, b=0),
    legend=dict(
        bgcolor="#1a1d24",
        bordercolor="#2d3748",
        borderwidth=1,
        font=dict(color="#fafafa"),
    ),
    hovermode="x unified",
)

fig.update_xaxes(
    rangeslider_visible=False,
    gridcolor="#1e2130",
    color="#fafafa",
    showspikes=True,
    spikecolor="#fafafa",
    spikethickness=1,
)

fig.update_yaxes(
    gridcolor="#1e2130",
    color="#fafafa",
    row=1,
    col=1,
)

st.plotly_chart(fig, use_container_width=True)

st.divider()

# ── Info panels ───────────────────────────────────────────────────────────────────
col_fund, col_tech = st.columns(2)

latest_quote = _load_latest_quote(ticker)
fund_raw = _load_fundamentals(ticker)
current_price = latest_quote.get("close") or (float(df["close"].iloc[-1]) if not df.empty else None)

with col_fund:
    st.subheader("Données fondamentales")
    ratios = compute_ratios(fund_raw, current_price) if (fund_raw and current_price) else {}

    cours_val = f"{current_price:,.0f} FCFA" if current_price else "—"
    change_val = latest_quote.get("change_pct")
    change_str = f"{change_val:+.2f}%" if change_val is not None else "—"
    per_val = ratios.get("per")
    div_yield = ratios.get("div_yield")
    roe_val = ratios.get("roe")
    mkt_cap = ratios.get("market_cap")

    fund_data = {
        "Cours": cours_val,
        "Variation": change_str,
        "PER": f"{per_val:.1f}x" if per_val else "—",
        "Rendement div.": f"{div_yield:.2f}%" if div_yield else "—",
        "ROE": f"{roe_val:.1f}%" if roe_val else "—",
        "Capitalisation": f"{mkt_cap:,.0f} MFCFA" if mkt_cap else "—",
    }

    for label, value in fund_data.items():
        c1, c2 = st.columns([2, 3])
        c1.markdown(f"**{label}**")
        c2.markdown(value)

with col_tech:
    st.subheader("Signaux techniques")

    if rsi_series is not None and not rsi_series.dropna().empty:
        rsi_last = float(rsi_series.dropna().iloc[-1])
        if rsi_last >= 70:
            rsi_signal = f"🔴 Suracheté ({rsi_last:.1f})"
        elif rsi_last <= 30:
            rsi_signal = f"🟢 Survendu ({rsi_last:.1f})"
        else:
            rsi_signal = f"⚪ Neutre ({rsi_last:.1f})"
    else:
        rsi_last = None
        rsi_signal = "— (données insuffisantes)"

    # 52-week high / low
    close_52w = df["close"].dropna()
    high_52w = float(close_52w.max()) if not close_52w.empty else None
    low_52w = float(close_52w.min()) if not close_52w.empty else None

    if high_52w and low_52w and current_price:
        range_pct = (
            (current_price - low_52w) / (high_52w - low_52w) * 100
            if high_52w != low_52w
            else 50.0
        )
        range_str = f"{range_pct:.0f}% du range (bas {low_52w:,.0f} / haut {high_52w:,.0f})"
    else:
        range_str = "—"

    # Bollinger position
    if bb_data is not None and current_price:
        bb_upper_last = next((v for v in reversed(bb_data["upper"]) if v is not None), None)
        bb_lower_last = next((v for v in reversed(bb_data["lower"]) if v is not None), None)
        if bb_upper_last and bb_lower_last and bb_upper_last != bb_lower_last:
            bb_pos = (current_price - bb_lower_last) / (bb_upper_last - bb_lower_last) * 100
            if bb_pos >= 90:
                bb_signal = f"🔴 Proche bande sup. ({bb_pos:.0f}%)"
            elif bb_pos <= 10:
                bb_signal = f"🟢 Proche bande inf. ({bb_pos:.0f}%)"
            else:
                bb_signal = f"⚪ Position neutre ({bb_pos:.0f}%)"
        else:
            bb_signal = "—"
    else:
        bb_signal = "— (BB désactivé ou données insuffisantes)"

    tech_data = {
        "RSI(14)": rsi_signal,
        "Range période": range_str,
        "Position BB": bb_signal,
        "Nb. séances": f"{len(df)} jours",
        "Période": period,
        "Type de chart": chart_type,
    }

    for label, value in tech_data.items():
        c1, c2 = st.columns([2, 3])
        c1.markdown(f"**{label}**")
        c2.markdown(value)
