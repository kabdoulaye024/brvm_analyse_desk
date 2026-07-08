"""
Classification de liquidité A/B/C et Liquidity / Execution Score /100.

La liquidité ne réduit jamais l'univers : elle module la taille de position,
la fiabilité du signal technique et les règles d'exécution (§4, §14).
"""
import pandas as pd

from . import config

P = config.PARAMS


def classify(weekly: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Mesures sur la fenêtre `liquidity_lookback_weeks` :
      - trading_frequency : part des séances de la fenêtre où le titre a coté
        (jours cotés / ~5 jours par semaine observée) ;
      - avg_weekly_value : montant moyen échangé par semaine (FCFA).
    """
    look = P["liquidity_lookback_weeks"]
    rows = []
    for ticker, w in weekly.items():
        win = w.iloc[-look:]
        n_weeks = max(len(win), 1)
        freq = float(win["trading_days"].sum() / (n_weeks * 5))
        avg_value = float(win["value_traded"].mean())
        weeks_traded = int((win["trading_days"] > 0).sum())

        if (freq >= P["liquidity_A_min_trading_frequency"]
                and avg_value >= P["liquidity_A_min_weekly_value"]):
            klass = "A"
        elif (freq >= P["liquidity_B_min_trading_frequency"]
                and avg_value >= P["liquidity_B_min_weekly_value"]):
            klass = "B"
        else:
            klass = "C"

        rows.append({"ticker": ticker, "liquidity_class": klass,
                     "trading_frequency": round(freq, 3),
                     "avg_weekly_value": round(avg_value),
                     "weeks_traded_lookback": weeks_traded,
                     "lookback_weeks": n_weeks})
    return pd.DataFrame(rows).set_index("ticker")


def execution_score(liq: pd.DataFrame, stale_flag: pd.Series) -> pd.Series:
    """
    Score /100 : fréquence de cotation (40), montant moyen (40),
    régularité récente (10), pénalité prix périmé (-10), bonus classe (10).
    """
    scores = {}
    for t, r in liq.iterrows():
        s = 0.0
        s += min(r["trading_frequency"] / P["liquidity_A_min_trading_frequency"], 1.0) * 40
        s += min(r["avg_weekly_value"] / P["liquidity_A_min_weekly_value"], 1.0) * 40
        s += min(r["weeks_traded_lookback"] / max(r["lookback_weeks"], 1), 1.0) * 10
        s += {"A": 10, "B": 5, "C": 0}[r["liquidity_class"]]
        if bool(stale_flag.get(t, False)):
            s -= 10
        scores[t] = round(max(min(s, 100), 0))
    return pd.Series(scores, name="liquidity_execution_score")


def execution_plan(r: pd.Series) -> dict:
    """Règles d'exécution et de sizing par classe (§16, §18)."""
    klass = r["liquidity_class"]
    if klass == "A":
        return {"max_position_size": f"{P['max_position_A']:.0%}",
                "suggested_entry_tranches": "2-3",
                "order_type": "Limite recommandé",
                "execution_note": "Entrée en 2-3 tranches ; taille max 15-20%.",
                "risk_note": "Liquidité correcte pour la BRVM ; slippage limité."}
    if klass == "B":
        return {"max_position_size": f"{P['max_position_B']:.0%}",
                "suggested_entry_tranches": "3-4",
                "order_type": "Limite obligatoire",
                "execution_note": "Entrée en 3-4 tranches ; confirmation technique renforcée.",
                "risk_note": "Semi-liquide : étaler les ordres, surveiller le carnet."}
    return {"max_position_size": f"{P['max_position_C']:.0%}",
            "suggested_entry_tranches": "4+ (lente)",
            "order_type": "Limite obligatoire",
            "execution_note": "Ordre limite obligatoire ; entrée lente ; taille réduite",
            "risk_note": "Illiquide : horizon long terme obligatoire, pas de trading court terme."}
