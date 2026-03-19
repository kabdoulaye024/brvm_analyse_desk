"""
BRVM Trading Desk — Intelligence Marché page
News feed, dividend calendar, weekly market summary.
"""
from datetime import datetime, timedelta

import pandas as pd
import streamlit as st

from backend.db.sync_db import query
from backend.scrapers.news import fetch_news
from backend.scrapers.courses import fetch_richbourse_dividends

st.set_page_config(page_title="Intelligence", page_icon="📰", layout="wide")

st.title("📰 Intelligence Marché")

# ── Tabs ──────────────────────────────────────────────────────────────────────────
tab_news, tab_div, tab_weekly = st.tabs(
    ["Actualités", "Calendrier Dividendes", "Résumé Hebdomadaire"]
)


# ── Tab 1: News ───────────────────────────────────────────────────────────────────
@st.cache_data(ttl=1800)
def _load_news(limit: int = 30) -> list[dict]:
    try:
        return fetch_news(limit=limit)
    except Exception as e:
        return []


@st.cache_data(ttl=3600)
def _load_dividends_db() -> pd.DataFrame:
    rows = query(
        """
        SELECT ce.ticker, s.name, ce.event_date, ce.description, ce.source
        FROM corporate_events ce
        LEFT JOIN stocks s ON ce.ticker = s.ticker
        WHERE ce.event_type = 'dividend'
        ORDER BY ce.event_date DESC
        """
    )
    return pd.DataFrame(rows) if rows else pd.DataFrame()


@st.cache_data(ttl=3600)
def _load_dividends_live() -> list[dict]:
    try:
        return fetch_richbourse_dividends()
    except Exception:
        return []


@st.cache_data(ttl=300)
def _load_weekly_quotes() -> pd.DataFrame:
    five_days_ago = (datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%d")
    rows = query(
        """
        SELECT q.ticker, s.name, q.date, q.close, q.change_pct, q.volume
        FROM daily_quotes q
        LEFT JOIN stocks s ON q.ticker = s.ticker
        WHERE q.date >= ?
        ORDER BY q.ticker, q.date ASC
        """,
        (five_days_ago,),
    )
    df = pd.DataFrame(rows) if rows else pd.DataFrame()
    if not df.empty:
        for c in ["close", "change_pct", "volume"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


SOURCE_BADGES = {
    "brvm.org": ("🏛️", "#1565c0"),
    "richbourse": ("📊", "#4527a0"),
    "richbourse.com": ("📊", "#4527a0"),
}

with tab_news:
    col_refresh, _ = st.columns([1, 9])
    with col_refresh:
        if st.button("🔄 Actualiser", key="refresh_news"):
            _load_news.clear()
            st.rerun()

    with st.spinner("Chargement des actualités…"):
        news_items = _load_news(limit=30)

    if not news_items:
        st.info(
            "Aucune actualité disponible pour le moment. "
            "Vérifiez votre connexion internet ou revenez dans quelques instants."
        )
    else:
        st.markdown(f"**{len(news_items)} actualités récentes**")
        st.divider()

        for item in news_items:
            title = item.get("title", "Sans titre")
            source = item.get("source", "")
            pub_date = item.get("published_at", "")
            summary = item.get("summary", "")
            url = item.get("url", "")

            icon, badge_color = SOURCE_BADGES.get(source, ("📰", "#37474f"))

            with st.expander(f"{icon} {title}", expanded=False):
                meta_parts = []
                if source:
                    meta_parts.append(f"**Source :** {source}")
                if pub_date:
                    meta_parts.append(f"**Date :** {pub_date}")
                if meta_parts:
                    st.markdown("   |   ".join(meta_parts))

                if summary:
                    st.markdown(summary)

                if url:
                    st.markdown(f"[Lire l'article complet →]({url})")


# ── Tab 2: Dividend Calendar ──────────────────────────────────────────────────────
with tab_div:
    col_refresh2, _ = st.columns([1, 9])
    with col_refresh2:
        if st.button("🔄 Actualiser", key="refresh_div"):
            _load_dividends_db.clear()
            _load_dividends_live.clear()
            st.rerun()

    df_div_db = _load_dividends_db()

    if not df_div_db.empty:
        st.subheader("Événements dividendes")
        df_div_display = df_div_db.rename(columns={
            "ticker": "Ticker",
            "name": "Société",
            "event_date": "Date",
            "description": "Description",
            "source": "Source",
        })
        st.dataframe(df_div_display, use_container_width=True, hide_index=True)
    else:
        # Try live fetch
        with st.spinner("Récupération du calendrier dividendes…"):
            live_divs = _load_dividends_live()

        if live_divs:
            df_div_live = pd.DataFrame(live_divs)
            st.subheader(f"Calendrier des dividendes ({len(df_div_live)} entrées)")
            st.dataframe(df_div_live, use_container_width=True, hide_index=True)
        else:
            st.info(
                "Aucune donnée de dividendes disponible. "
                "Les données seront récupérées lors de la prochaine synchronisation."
            )


# ── Tab 3: Weekly Summary ─────────────────────────────────────────────────────────
with tab_weekly:
    df_week = _load_weekly_quotes()

    if df_week.empty:
        st.info("Aucune donnée hebdomadaire disponible.")
    else:
        st.subheader("Performance de la semaine")

        # For each ticker, compute week performance: latest close vs earliest close in period
        weekly_perf = []
        for tkr, grp in df_week.groupby("ticker"):
            grp_sorted = grp.sort_values("date")
            if len(grp_sorted) < 2:
                continue
            price_start = float(grp_sorted["close"].iloc[0])
            price_end = float(grp_sorted["close"].iloc[-1])
            if price_start <= 0:
                continue
            perf = (price_end / price_start - 1) * 100
            name = grp_sorted["name"].iloc[-1] if "name" in grp_sorted.columns else tkr
            total_vol = grp_sorted["volume"].fillna(0).sum()
            weekly_perf.append({
                "ticker": tkr,
                "name": name,
                "prix_début": round(price_start, 0),
                "prix_fin": round(price_end, 0),
                "perf_semaine": round(perf, 2),
                "volume_total": int(total_vol),
                "nb_séances": len(grp_sorted),
            })

        if not weekly_perf:
            st.info("Données insuffisantes pour calculer les performances hebdomadaires.")
        else:
            df_perf = pd.DataFrame(weekly_perf).sort_values("perf_semaine", ascending=False)

            # Summary metrics
            n_dates = df_week["date"].nunique()
            total_vol_week = int(df_week["volume"].fillna(0).sum())
            n_up_week = int((df_perf["perf_semaine"] > 0).sum())
            n_down_week = int((df_perf["perf_semaine"] < 0).sum())
            best_perf = df_perf.iloc[0]["perf_semaine"] if not df_perf.empty else 0
            worst_perf = df_perf.iloc[-1]["perf_semaine"] if not df_perf.empty else 0

            wm1, wm2, wm3, wm4 = st.columns(4)
            wm1.metric("Séances analysées", n_dates)
            wm2.metric("Volume total", f"{total_vol_week:,}")
            wm3.metric("En hausse", n_up_week)
            wm4.metric("En baisse", n_down_week)

            st.divider()

            col_best, col_worst = st.columns(2)

            with col_best:
                st.markdown("**Top 5 Hausses de la semaine**")
                df_gainers_w = df_perf.head(5)[["ticker", "name", "prix_fin", "perf_semaine"]].copy()
                df_gainers_w = df_gainers_w.rename(columns={
                    "ticker": "Ticker", "name": "Société",
                    "prix_fin": "Cours (FCFA)", "perf_semaine": "Var semaine %"
                })

                def _style_pos(v):
                    return "color: #26a69a; font-weight: bold" if v > 0 else ""

                st.dataframe(
                    df_gainers_w.style.map(_style_pos, subset=["Var semaine %"]),
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "Cours (FCFA)": st.column_config.NumberColumn("Cours (FCFA)", format="%,.0f"),
                        "Var semaine %": st.column_config.NumberColumn("Var semaine %", format="%.2f%%"),
                    },
                )

            with col_worst:
                st.markdown("**Top 5 Baisses de la semaine**")
                df_losers_w = df_perf.tail(5).sort_values("perf_semaine")[
                    ["ticker", "name", "prix_fin", "perf_semaine"]
                ].copy()
                df_losers_w = df_losers_w.rename(columns={
                    "ticker": "Ticker", "name": "Société",
                    "prix_fin": "Cours (FCFA)", "perf_semaine": "Var semaine %"
                })

                def _style_neg(v):
                    return "color: #ef5350; font-weight: bold" if v < 0 else ""

                st.dataframe(
                    df_losers_w.style.map(_style_neg, subset=["Var semaine %"]),
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "Cours (FCFA)": st.column_config.NumberColumn("Cours (FCFA)", format="%,.0f"),
                        "Var semaine %": st.column_config.NumberColumn("Var semaine %", format="%.2f%%"),
                    },
                )

            st.divider()
            st.subheader("Tableau complet")

            df_perf_display = df_perf.rename(columns={
                "ticker": "Ticker", "name": "Société",
                "prix_début": "Ouverture sem.", "prix_fin": "Cours actuel",
                "perf_semaine": "Perf. semaine %",
                "volume_total": "Volume total", "nb_séances": "Séances",
            })

            def _style_perf(v):
                if v > 0:
                    return "color: #26a69a"
                if v < 0:
                    return "color: #ef5350"
                return ""

            st.dataframe(
                df_perf_display.style.map(_style_perf, subset=["Perf. semaine %"]),
                use_container_width=True,
                hide_index=True,
                height=400,
                column_config={
                    "Ouverture sem.": st.column_config.NumberColumn("Ouverture sem.", format="%,.0f"),
                    "Cours actuel": st.column_config.NumberColumn("Cours actuel", format="%,.0f"),
                    "Perf. semaine %": st.column_config.NumberColumn("Perf. semaine %", format="%.2f%%"),
                    "Volume total": st.column_config.NumberColumn("Volume total", format="%,d"),
                },
            )
