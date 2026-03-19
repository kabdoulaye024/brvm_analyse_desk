"""
BRVM Trading Desk — Portefeuille page
Portfolio tracking: positions, P&L, transactions, capital flows.
"""
from datetime import date

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from backend.db.sync_db import execute, query, query_one
from backend.models.reference import TICKERS_BRVM

st.set_page_config(page_title="Portefeuille", page_icon="💼", layout="wide")

st.title("💼 Portefeuille")


# ── Data loaders ─────────────────────────────────────────────────────────────────
@st.cache_data(ttl=60)
def _load_transactions() -> pd.DataFrame:
    rows = query(
        """
        SELECT id, type, ticker, date, price, quantity, fees, notes, created_at
        FROM portfolio_transactions
        ORDER BY date DESC, created_at DESC
        """
    )
    return pd.DataFrame(rows) if rows else pd.DataFrame(
        columns=["id", "type", "ticker", "date", "price", "quantity", "fees", "notes", "created_at"]
    )


@st.cache_data(ttl=60)
def _load_capital_flows() -> pd.DataFrame:
    rows = query(
        "SELECT id, date, amount, notes, created_at FROM capital_flows ORDER BY date DESC"
    )
    return pd.DataFrame(rows) if rows else pd.DataFrame(
        columns=["id", "date", "amount", "notes", "created_at"]
    )


@st.cache_data(ttl=60)
def _load_prices() -> dict[str, float]:
    rows = query(
        """
        SELECT ticker, close FROM daily_quotes
        WHERE (ticker, date) IN (
            SELECT ticker, MAX(date) FROM daily_quotes GROUP BY ticker
        )
        """
    )
    return {r["ticker"]: float(r["close"]) for r in rows if r["close"]}


@st.cache_data(ttl=300)
def _load_stock_info() -> dict[str, dict]:
    rows = query("SELECT ticker, name, sector FROM stocks")
    return {r["ticker"]: {"name": r["name"], "sector": r["sector"]} for r in rows}


# ── Portfolio computation ─────────────────────────────────────────────────────────
def _compute_portfolio(
    df_tx: pd.DataFrame,
    df_cf: pd.DataFrame,
    prices: dict,
    stock_info: dict,
) -> tuple[list[dict], dict]:
    """
    Returns (positions_list, summary_dict).
    positions_list: one row per open position.
    summary_dict: total_invested, total_market_value, cash, total_pnl, total_pnl_pct.
    """
    if df_tx.empty:
        total_invested = float(df_cf["amount"].sum()) if not df_cf.empty else 0.0
        return [], {
            "total_invested": total_invested,
            "total_market_value": 0.0,
            "cash": total_invested,
            "total_pnl": 0.0,
            "total_pnl_pct": 0.0,
            "n_positions": 0,
        }

    for col in ["price", "quantity", "fees"]:
        df_tx[col] = pd.to_numeric(df_tx[col], errors="coerce").fillna(0)

    tickers_in_portfolio = df_tx["ticker"].unique().tolist()
    positions = []
    total_buy_cost = 0.0
    total_sell_proceeds = 0.0

    for tkr in tickers_in_portfolio:
        sub = df_tx[df_tx["ticker"] == tkr]
        buys = sub[sub["type"] == "BUY"]
        sells = sub[sub["type"] == "SELL"]

        total_buy_qty = buys["quantity"].sum()
        total_sell_qty = sells["quantity"].sum()
        net_qty = total_buy_qty - total_sell_qty

        if net_qty <= 0:
            continue

        # Weighted average buy price
        buy_cost = (buys["price"] * buys["quantity"]).sum() + buys["fees"].sum()
        avg_buy = buy_cost / total_buy_qty if total_buy_qty > 0 else 0.0
        sell_proceeds = (sells["price"] * sells["quantity"]).sum() - sells["fees"].sum()

        total_buy_cost += buy_cost
        total_sell_proceeds += sell_proceeds

        current_price = prices.get(tkr)
        # Fall back to last transaction price if no market data
        if not current_price:
            current_price = float(sub["price"].iloc[0]) if not sub.empty else avg_buy

        market_value = net_qty * current_price
        cost_basis = avg_buy * net_qty
        pnl = market_value - cost_basis
        pnl_pct = (pnl / cost_basis * 100) if cost_basis > 0 else 0.0

        info = stock_info.get(tkr, {})
        positions.append({
            "ticker": tkr,
            "name": info.get("name", TICKERS_BRVM.get(tkr, (tkr,))[0]),
            "sector": info.get("sector", TICKERS_BRVM.get(tkr, ("", "Inconnu"))[1]
                               if len(TICKERS_BRVM.get(tkr, ())) > 1 else "Inconnu"),
            "quantity": int(net_qty),
            "avg_buy_price": round(avg_buy, 0),
            "current_price": round(current_price, 0),
            "market_value": round(market_value, 0),
            "pnl": round(pnl, 0),
            "pnl_pct": round(pnl_pct, 2),
        })

    total_invested = float(df_cf["amount"].sum()) if not df_cf.empty else 0.0
    total_market_value = sum(p["market_value"] for p in positions)
    cash = total_invested - total_buy_cost + total_sell_proceeds
    total_portfolio = total_market_value + cash
    total_pnl = total_portfolio - total_invested
    total_pnl_pct = (total_pnl / total_invested * 100) if total_invested > 0 else 0.0

    summary = {
        "total_invested": round(total_invested, 0),
        "total_market_value": round(total_market_value, 0),
        "total_portfolio": round(total_portfolio, 0),
        "cash": round(cash, 0),
        "total_pnl": round(total_pnl, 0),
        "total_pnl_pct": round(total_pnl_pct, 2),
        "n_positions": len(positions),
    }

    return positions, summary


# ── Load data ─────────────────────────────────────────────────────────────────────
df_tx = _load_transactions()
df_cf = _load_capital_flows()
prices = _load_prices()
stock_info = _load_stock_info()

positions, summary = _compute_portfolio(df_tx, df_cf, prices, stock_info)
df_pos = pd.DataFrame(positions) if positions else pd.DataFrame()

# ── Row 1: Summary metrics ────────────────────────────────────────────────────────
m1, m2, m3, m4 = st.columns(4)

total_val = summary.get("total_portfolio", 0)
total_pnl = summary.get("total_pnl", 0)
cash_val = summary.get("cash", 0)
n_pos = summary.get("n_positions", 0)

m1.metric("Valeur totale", f"{total_val:,.0f} FCFA")
pnl_sign = "+" if total_pnl >= 0 else ""
m2.metric(
    "P&L total",
    f"{pnl_sign}{total_pnl:,.0f} FCFA",
    delta=f"{pnl_sign}{summary.get('total_pnl_pct', 0):.2f}%",
    delta_color="normal",
)
m3.metric("Cash disponible", f"{cash_val:,.0f} FCFA")
m4.metric("Positions actives", n_pos)

st.divider()

# ── Row 2: Positions + allocation chart ──────────────────────────────────────────
if not df_pos.empty:
    col_pos, col_chart = st.columns([6, 4])

    with col_pos:
        st.subheader("Positions ouvertes")

        df_display = df_pos.rename(columns={
            "ticker": "Ticker",
            "name": "Société",
            "sector": "Secteur",
            "quantity": "Qté",
            "avg_buy_price": "Px Achat",
            "current_price": "Cours",
            "market_value": "Valeur (FCFA)",
            "pnl": "P&L (FCFA)",
            "pnl_pct": "P&L %",
        })

        def _color_pnl(val):
            if val > 0:
                return "color: #26a69a; font-weight: bold"
            if val < 0:
                return "color: #ef5350; font-weight: bold"
            return ""

        styled = df_display.style.map(
            _color_pnl,
            subset=["P&L (FCFA)", "P&L %"] if "P&L (FCFA)" in df_display.columns else [],
        )

        st.dataframe(
            styled,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Px Achat": st.column_config.NumberColumn("Px Achat", format="%,.0f"),
                "Cours": st.column_config.NumberColumn("Cours", format="%,.0f"),
                "Valeur (FCFA)": st.column_config.NumberColumn("Valeur (FCFA)", format="%,.0f"),
                "P&L (FCFA)": st.column_config.NumberColumn("P&L (FCFA)", format="%,.0f"),
                "P&L %": st.column_config.NumberColumn("P&L %", format="%.2f%%"),
            },
        )

    with col_chart:
        st.subheader("Allocation")

        # Build sunburst data: sector → ticker
        sunburst_df = df_pos[["ticker", "sector", "market_value"]].copy()
        sunburst_df = sunburst_df[sunburst_df["market_value"] > 0]

        if not sunburst_df.empty:
            fig_sb = px.sunburst(
                sunburst_df,
                path=["sector", "ticker"],
                values="market_value",
                color="sector",
                color_discrete_sequence=px.colors.qualitative.Set3,
            )
            fig_sb.update_layout(
                paper_bgcolor="#0e1117",
                plot_bgcolor="#0e1117",
                font=dict(color="#fafafa"),
                margin=dict(l=0, r=0, t=0, b=0),
                height=350,
            )
            fig_sb.update_traces(textinfo="label+percent entry")
            st.plotly_chart(fig_sb, use_container_width=True)
        else:
            st.info("Aucune position avec valeur positive.")

    st.divider()

# ── Tabs ──────────────────────────────────────────────────────────────────────────
tab_pos, tab_add_tx, tab_add_cap, tab_hist = st.tabs(
    ["Positions", "Ajouter Transaction", "Apports Capital", "Historique"]
)

ticker_list = sorted(TICKERS_BRVM.keys())

with tab_pos:
    if df_pos.empty:
        st.info("Aucune position ouverte. Ajoutez des transactions dans l'onglet dédié.")
    else:
        st.dataframe(
            df_pos,
            use_container_width=True,
            hide_index=True,
        )

with tab_add_tx:
    st.subheader("Enregistrer une transaction")
    with st.form("add_transaction", clear_on_submit=True):
        fc1, fc2 = st.columns(2)
        with fc1:
            tx_type = st.selectbox("Type", ["BUY", "SELL"])
            tx_ticker = st.selectbox(
                "Titre",
                options=ticker_list,
                format_func=lambda t: f"{t} — {TICKERS_BRVM[t][0]}",
            )
            tx_date = st.date_input("Date", value=date.today())
        with fc2:
            tx_price = st.number_input("Prix (FCFA)", min_value=0.0, step=10.0, format="%.0f")
            tx_qty = st.number_input("Quantité", min_value=1, step=1)
            tx_fees = st.number_input("Frais (FCFA)", min_value=0.0, step=100.0, format="%.0f")
        tx_notes = st.text_input("Notes (optionnel)")

        submitted = st.form_submit_button("Enregistrer")
        if submitted:
            if tx_price <= 0:
                st.error("Le prix doit être supérieur à 0.")
            else:
                execute(
                    """INSERT INTO portfolio_transactions
                       (type, ticker, date, price, quantity, fees, notes)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (tx_type, tx_ticker, str(tx_date), tx_price, tx_qty, tx_fees, tx_notes),
                )
                st.success(f"Transaction {tx_type} {tx_qty} × {tx_ticker} enregistrée.")
                _load_transactions.clear()
                st.rerun()

with tab_add_cap:
    st.subheader("Enregistrer un apport de capital")
    with st.form("add_capital", clear_on_submit=True):
        cap_date = st.date_input("Date", value=date.today())
        cap_amount = st.number_input("Montant (FCFA)", min_value=0.0, step=10000.0, format="%.0f")
        cap_notes = st.text_input("Notes (optionnel)")

        submitted_cap = st.form_submit_button("Enregistrer")
        if submitted_cap:
            if cap_amount <= 0:
                st.error("Le montant doit être supérieur à 0.")
            else:
                execute(
                    "INSERT INTO capital_flows (date, amount, notes) VALUES (?, ?, ?)",
                    (str(cap_date), cap_amount, cap_notes),
                )
                st.success(f"Apport de {cap_amount:,.0f} FCFA enregistré.")
                _load_capital_flows.clear()
                st.rerun()

    if not df_cf.empty:
        st.divider()
        st.subheader("Historique des apports")
        df_cf_display = df_cf[["date", "amount", "notes", "created_at"]].copy()
        df_cf_display = df_cf_display.rename(columns={
            "date": "Date", "amount": "Montant (FCFA)", "notes": "Notes", "created_at": "Saisi le"
        })
        st.dataframe(
            df_cf_display,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Montant (FCFA)": st.column_config.NumberColumn("Montant (FCFA)", format="%,.0f"),
            },
        )

with tab_hist:
    st.subheader("Historique des transactions")

    if df_tx.empty:
        st.info("Aucune transaction enregistrée.")
    else:
        df_tx_display = df_tx.copy()
        df_tx_display = df_tx_display.rename(columns={
            "id": "ID", "type": "Type", "ticker": "Ticker", "date": "Date",
            "price": "Prix", "quantity": "Qté", "fees": "Frais",
            "notes": "Notes", "created_at": "Créé le",
        })

        def _color_type(val):
            if val == "BUY":
                return "color: #26a69a; font-weight: bold"
            return "color: #ef5350; font-weight: bold"

        styled_hist = df_tx_display.style.map(_color_type, subset=["Type"])

        st.dataframe(
            styled_hist,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Prix": st.column_config.NumberColumn("Prix", format="%,.0f"),
                "Frais": st.column_config.NumberColumn("Frais", format="%,.0f"),
            },
        )

        st.divider()
        st.subheader("Supprimer une transaction")
        del_id = st.number_input(
            "ID de la transaction à supprimer", min_value=1, step=1, format="%d"
        )
        if st.button("Supprimer", type="secondary"):
            existing = query_one(
                "SELECT id FROM portfolio_transactions WHERE id = ?", (int(del_id),)
            )
            if existing:
                execute("DELETE FROM portfolio_transactions WHERE id = ?", (int(del_id),))
                st.success(f"Transaction #{del_id} supprimée.")
                _load_transactions.clear()
                st.rerun()
            else:
                st.error(f"Transaction #{del_id} introuvable.")
