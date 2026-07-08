"""
Catalyst Score /100 — potentiel de rerating BRVM (§13).

Blocs : résultat net YoY 25, dividende attendu 25, rendement implicite 20,
proximité d'annonce 15, réaction de marché encore limitée 15.
Mode dégradé : si un bloc n'est pas mesurable, le score est renormalisé sur
les blocs disponibles et la confiance est signalée dans le commentaire.
"""
import pandas as pd

from . import config

EVENT_WINDOW_DAYS = 90     # proximité d'annonce considérée comme catalyseur


def _dividend_info(events: pd.DataFrame, ticker: str) -> dict:
    """Dernier événement dividende connu pour un titre (montant, rendement, date)."""
    if events.empty:
        return {}
    ev = events[(events["ticker"] == ticker)
                & (events["event_type"].str.upper().str.contains("DIV", na=False))]
    if ev.empty:
        return {}
    ev = ev.sort_values("event_date_parsed", na_position="first").iloc[-1]
    return {"amount": ev.get("dividend_amount"), "yield_pct": ev.get("yield_pct"),
            "date": ev.get("event_date_parsed"), "type": ev.get("event_type")}


def run(ratios: pd.DataFrame, events: pd.DataFrame, tech: pd.DataFrame,
        as_of: pd.Timestamp) -> pd.DataFrame:
    """Catalyst Score par ticker. `ratios` vient de fundamentals.compute_ratios."""
    rows = []
    for t in tech.index:
        r = ratios.loc[t] if t in ratios.index else pd.Series(dtype=object)
        earned, avail, notes = 0.0, 0.0, []

        # 1) Résultat net en hausse YoY (25) — proxy croissance EPS.
        g = r.get("eps_growth")
        ni_yoy = None
        if g is not None and pd.notna(g):
            avail += 25
            ni_yoy = g
            earned += 25 if g >= 0.10 else (15 if g >= 0.0 else 0)
        else:
            notes.append("croissance RN inconnue")

        # 2) Dividende attendu en hausse ou maintien probable (25).
        div = _dividend_info(events, t)
        dps_hist = r.get("dividend_per_share")
        expected_change = None
        if div.get("amount") is not None and pd.notna(div.get("amount")):
            avail += 25
            if dps_hist is not None and pd.notna(dps_hist) and dps_hist > 0:
                chg = div["amount"] / dps_hist - 1
                expected_change = chg
                earned += 25 if chg >= 0.03 else (20 if chg >= -0.03 else 5)
            else:
                expected_change = 0.0
                earned += 18                     # dividende annoncé, historique inconnu
        elif dps_hist is not None and pd.notna(dps_hist) and dps_hist > 0:
            avail += 25
            earned += 12                          # payeur régulier, pas d'annonce connue
            notes.append("pas d'annonce dividende récente")
        else:
            notes.append("aucune info dividende")

        # 3) Rendement implicite attractif (20).
        imp_yield = None
        price = r.get("price")
        if div.get("amount") is not None and pd.notna(div.get("amount")) \
                and price is not None and pd.notna(price) and price > 0:
            imp_yield = float(div["amount"]) / float(price)
        elif r.get("dividend_yield") is not None and pd.notna(r.get("dividend_yield")):
            imp_yield = float(r["dividend_yield"])
        if imp_yield is not None:
            avail += 20
            earned += 20 if imp_yield >= 0.07 else (14 if imp_yield >= 0.05
                                                    else (7 if imp_yield >= 0.03 else 0))

        # 4) Proximité d'annonce résultats / dividende / AG (15).
        next_event_date, event_type = None, None
        if div.get("date") is not None and pd.notna(div.get("date")):
            next_event_date, event_type = div["date"], div.get("type", "DIVIDEND")
            avail += 15
            delta = abs((div["date"] - as_of).days)
            earned += 15 if delta <= EVENT_WINDOW_DAYS else (7 if delta <= 180 else 0)
        else:
            notes.append("calendrier d'annonces incomplet")

        # 5) Réaction de marché encore limitée (15) : catalyseur pas déjà dans les cours.
        perf4 = tech.loc[t, "perf_4w"] if "perf_4w" in tech.columns else None
        has_catalyst = (imp_yield or 0) >= 0.05 or (ni_yoy or 0) >= 0.05
        if perf4 is not None and pd.notna(perf4):
            avail += 15
            if has_catalyst and perf4 < 0.05:
                earned += 15
            elif has_catalyst and perf4 < 0.12:
                earned += 8

        score = round(earned / avail * 100) if avail > 0 else 0
        # Mode dégradé : on renormalise, mais un score élevé ne peut pas être
        # revendiqué avec peu d'information -> plafond selon la couverture.
        if avail < 50:
            score = min(score, 60)      # au mieux "catalyseur exploitable"
        elif avail < 75:
            score = min(score, 80)
        confidence = "haute" if avail >= 75 else ("moyenne" if avail >= 50 else "faible (mode dégradé)")

        if score >= 75:
            label = "catalyseur fort"
        elif score >= 60:
            label = "catalyseur exploitable"
        elif score >= 45:
            label = "catalyseur modéré"
        else:
            label = "pas de catalyseur clair"
        comment = f"{label} (confiance {confidence})"
        if notes:
            comment += " — " + "; ".join(notes[:2])

        rows.append({"ticker": t, "catalyst_score": score,
                     "catalyst_coverage": round(avail / 100, 2),
                     "net_income_yoy_growth": ni_yoy,
                     "expected_dividend_change": expected_change,
                     "implied_dividend_yield": imp_yield,
                     "next_event_date": (next_event_date.date().isoformat()
                                         if next_event_date is not None and pd.notna(next_event_date) else None),
                     "event_type": event_type,
                     "market_reaction_score": (15 if (perf4 is not None and pd.notna(perf4)
                                                      and has_catalyst and perf4 < 0.05) else 0),
                     "catalyst_comment": comment,
                     "incomplete_event_data": avail < 75})
    return pd.DataFrame(rows).set_index("ticker")
