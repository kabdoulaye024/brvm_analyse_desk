"""
Score final, décision d'investissement, règles de vente, commentaire court.

Final = 40 % Graham + 30 % Technical + 20 % Catalyst + 10 % Liquidity (§15).
Décision avec garde-fous (§16), hiérarchie de vente (§17), sizing (§18).
"""
import pandas as pd

from . import config

P = config.PARAMS


def final_score(row: pd.Series) -> float:
    return round(
        P["graham_weight"] * row["graham_score"]
        + P["technical_weight"] * row["technical_score"]
        + P["catalyst_weight"] * row["catalyst_score"]
        + P["liquidity_weight"] * row["liquidity_execution_score"], 1)


def decide(row: pd.Series) -> str:
    """Seuils §16 + règles complémentaires (Graham/Catalyst/Technique/liquidité C)."""
    s = row["final_score"]
    graham, tech, cat = row["graham_score"], row["technical_score"], row["catalyst_score"]
    klass = row["liquidity_class"]

    # Stage 4 = déclin Weinstein : jamais une recommandation d'achat,
    # quel que soit le score (cohérent avec la règle de vente §17).
    if row.get("stage_weinstein") == "Stage 4":
        rs = row.get("rs_26w_vs_brvm30")
        confirmed = pd.notna(rs) and rs < 0
        if confirmed:
            return "Surveillance simple" if s >= P["simple_watchlist_threshold"] else "Éviter"
        return ("Watchlist prioritaire" if s >= P["priority_watchlist_threshold"]
                else ("Surveillance simple" if s >= P["simple_watchlist_threshold"] else "Éviter"))

    if s >= P["strong_buy_threshold"]:
        # Achat fort seulement si Graham solide ET technique confirmé, jamais sur titre C.
        if (graham >= P["strong_buy_min_graham"]
                and tech >= P["strong_buy_min_technical"] and klass != "C"):
            return "Achat fort"
        return "Achat progressif"
    if s >= P["progressive_buy_threshold"]:
        return "Achat progressif"
    if s >= P["priority_watchlist_threshold"]:
        # Graham + Catalyst élevés peuvent déclencher l'achat progressif
        # même si le signal Weinstein est encore imparfait.
        if (graham >= P["progressive_min_graham"] and cat >= P["progressive_min_catalyst"]):
            return "Achat progressif"
        return "Watchlist prioritaire"
    if s >= P["simple_watchlist_threshold"]:
        return "Surveillance simple"
    return "Éviter"


def sell_signal(row: pd.Series) -> str:
    """Hiérarchie §17 : 1) thèse fondamentale cassée, 2) Stage 4, 3) valorisation tendue."""
    reasons = []

    # 1. Vente fondamentale prioritaire.
    fund_broken = []
    if pd.notna(row.get("eps_growth")) and row["eps_growth"] < -0.20:
        fund_broken.append("résultat en forte baisse")
    if pd.notna(row.get("payout_ratio")) and row["payout_ratio"] > 1.0:
        fund_broken.append("payout insoutenable")
    if pd.notna(row.get("eps")) and row["eps"] < 0:
        fund_broken.append("pertes")
    if fund_broken:
        reasons.append("VENTE FONDAMENTALE : " + ", ".join(fund_broken))

    # 2. Vente technique : Stage 4 confirmé.
    rs = row.get("rs_26w_vs_brvm30")
    if row.get("stage_weinstein") == "Stage 4" and pd.notna(rs) and rs < 0:
        reasons.append("Stage 4 confirmé : sortie progressive")
    elif row.get("stage_weinstein") == "Stage 4":
        reasons.append("Stage 4 : alléger")

    # 3. Vente de valorisation : prise de profit partielle.
    if (pd.notna(row.get("per")) and pd.notna(row.get("per_sector_median"))
            and row["per"] > 1.8 * row["per_sector_median"]
            and (pd.isna(row.get("dividend_yield")) or row["dividend_yield"] < 0.03)):
        reasons.append("Valorisation tendue vs secteur : prise de profit partielle")

    return " | ".join(reasons) if reasons else "—"


def short_comment(row: pd.Series) -> str:
    """Thèse en une phrase (§21)."""
    bits = []
    g, tech, cat = row["graham_score"], row["technical_score"], row["catalyst_score"]
    bits.append("Graham solide" if g >= 65 else ("Graham correct" if g >= 50 else "fondamentaux insuffisants"))
    if cat >= 75:
        bits.append("catalyseur fort")
    elif cat >= 60:
        bits.append("catalyseur exploitable")
    stage = row.get("stage_weinstein", "Indéterminé")
    if stage == "Stage 2":
        bits.append("Stage 2" + (" (breakout)" if row.get("new_high_26w") else ""))
    elif stage == "Stage 1":
        bits.append("Stage 1 (base)")
    elif stage == "Stage 4":
        bits.append("Stage 4 : éviter/sortir")
    elif tech < 40:
        bits.append("signal technique faible")
    bits.append(f"liquidité {row['liquidity_class']}")
    if row.get("liquidity_class") == "C":
        bits.append("petite ligne, ordre limite")
    if row.get("stale_price_flag"):
        bits.append("cours ancien")
    return "; ".join(bits).capitalize() + "."


def run(df: pd.DataFrame) -> pd.DataFrame:
    """Ajoute final_score, decision, sell_signal, short_comment."""
    df = df.copy()
    df["final_score"] = df.apply(final_score, axis=1)
    df["decision"] = df.apply(decide, axis=1)
    df["sell_signal"] = df.apply(sell_signal, axis=1)
    df["short_comment"] = df.apply(short_comment, axis=1)
    df["rank"] = df["final_score"].rank(ascending=False, method="min").astype(int)
    return df.sort_values("final_score", ascending=False)
