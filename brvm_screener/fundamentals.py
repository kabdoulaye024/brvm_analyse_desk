"""
Graham Score /100 — Quality Value + Dividend Value, adapté BRVM.

Deux grilles : banques/financières et non-financières (§11).
Chaque critère est noté seulement si les données existent ; le score est
renormalisé sur les points disponibles, avec plafond si la couverture est
trop faible. Les seuils absolus sont nuancés par la médiane sectorielle (§12).
"""
import numpy as np
import pandas as pd

from . import config

M = config.MILLIONS
COVERAGE_CAP = 60          # plafond du score si couverture des critères < 60 %
MIN_COVERAGE = 0.60


def _safe_div(a, b):
    if a is None or b is None or pd.isna(a) or pd.isna(b) or b == 0:
        return None
    return a / b


def compute_ratios(fund: pd.DataFrame, prices: pd.Series) -> pd.DataFrame:
    """Ratios par titre. Agrégats en MFCFA, per-share et cours en FCFA."""
    rows = []
    for _, f in fund.iterrows():
        t = f["ticker"]
        price = prices.get(t)
        shares = f.get("shares_outstanding")
        equity = f.get("equity")
        ni = f.get("net_income")

        bvps = _safe_div(equity * M if pd.notna(equity) else None, shares)
        eps = f.get("eps")
        per = _safe_div(price, eps)
        pb = _safe_div(price, bvps)
        roe = _safe_div(ni, equity)
        roa = _safe_div(ni, f.get("total_assets"))
        dps = f.get("dividend_per_share")
        dy = _safe_div(dps, price)
        payout = _safe_div(dps, eps)
        net_margin = _safe_div(ni, f.get("revenue"))
        debt_to_equity = _safe_div(f.get("total_debt"), equity)
        ebitda = f.get("ebitda")
        net_debt = (f.get("total_debt") - (f.get("cash") or 0)
                    if pd.notna(f.get("total_debt")) else None)
        nd_ebitda = _safe_div(net_debt, ebitda if pd.notna(ebitda) else None)
        eps_growth = _safe_div(eps - f.get("eps_prior") if pd.notna(f.get("eps_prior"))
                               and pd.notna(eps) else None, abs(f.get("eps_prior")) or None) \
            if pd.notna(f.get("eps_prior")) else None

        rows.append({"ticker": t, "sector": f.get("sector", "Inconnu"),
                     "is_bank": int(f.get("is_bank", 0)), "price": price,
                     "per": per, "pb": pb, "roe": roe, "roa": roa,
                     "dividend_yield": dy, "payout_ratio": payout,
                     "net_margin": net_margin, "debt_to_equity": debt_to_equity,
                     "net_debt_ebitda": nd_ebitda, "eps": eps,
                     "eps_prior": f.get("eps_prior"), "eps_growth": eps_growth,
                     "dividend_per_share": dps})
    return pd.DataFrame(rows).set_index("ticker")


def _sector_medians(ratios: pd.DataFrame) -> pd.DataFrame:
    """Médianes sectorielles (repli sur la médiane marché si secteur < 3 titres)."""
    cols = ["per", "pb", "dividend_yield", "roe"]
    med_global = ratios[cols].median()
    med = ratios.groupby("sector")[cols].agg(["median", "count"])
    out = {}
    for sector in ratios["sector"].unique():
        row = {}
        for c in cols:
            n = med.loc[sector, (c, "count")]
            row[c] = (med.loc[sector, (c, "median")] if n >= 3 else med_global[c])
        out[sector] = row
    return pd.DataFrame(out).T


def _grade(value, full, partial, reverse=False):
    """3 points d'ancrage : plein / partiel / zéro. reverse=True si plus petit = mieux."""
    if value is None or pd.isna(value):
        return None
    if reverse:
        return 1.0 if value <= full else (0.5 if value <= partial else 0.0)
    return 1.0 if value >= full else (0.5 if value >= partial else 0.0)


def _score_bank(r: pd.Series, med: pd.Series) -> tuple[float, float, list]:
    """Grille bancaire : P/B vs ROE 20, ROE 20, dividende 30, croissance 15, risque 10, qualité 5."""
    earned, available, notes = 0.0, 0.0, []

    # P/B raisonnable relativement au ROE et au secteur (20).
    if pd.notna(r["pb"]) and pd.notna(r["roe"]):
        available += 20
        rel = r["pb"] / max(med["pb"], 0.1)
        good_combo = (r["pb"] <= 1.0 and r["roe"] >= 0.10) or (r["roe"] >= 0.18 and r["pb"] <= 2.0)
        earned += 20 if good_combo else (12 if rel <= 1.0 else (6 if rel <= 1.3 else 0))
    else:
        notes.append("P/B ou ROE manquant")

    # ROE élevé (20). Le levier bancaire n'est pas pénalisé (structurel).
    g = _grade(r["roe"], 0.15, 0.08)
    if g is not None:
        available += 20
        earned += g * 20

    # Dividende soutenable et régulier (30).
    if pd.notna(r["dividend_yield"]):
        available += 30
        pts = 0.0
        pts += min(r["dividend_yield"] / 0.07, 1.0) * 15          # rendement
        if pd.notna(r["payout_ratio"]):
            pts += 15 if 0.25 <= r["payout_ratio"] <= 0.80 else \
                   (8 if r["payout_ratio"] <= 1.0 else 0)          # soutenabilité
        else:
            pts += 7
        earned += pts
    else:
        notes.append("dividende inconnu")

    # Croissance PNB / résultat net (15) — proxy croissance EPS.
    g = _grade(r["eps_growth"], 0.05, 0.0)
    if g is not None:
        available += 15
        earned += g * 15
    else:
        notes.append("croissance non mesurable")

    # Coût du risque / solvabilité (10) — rarement publié en base : ignoré si absent.
    notes.append("coût du risque/solvabilité non disponibles")

    # Qualité / régularité (5) : bénéfices positifs sur les 2 exercices connus.
    if pd.notna(r["eps"]) and pd.notna(r["eps_prior"]):
        available += 5
        earned += 5 if (r["eps"] > 0 and r["eps_prior"] > 0) else 0

    return earned, available, notes


def _score_nonfin(r: pd.Series, med: pd.Series) -> tuple[float, float, list]:
    """Grille non-financière : valorisation 20, rentabilité 20, dividende 30, dette 20, régularité 10."""
    earned, available, notes = 0.0, 0.0, []

    # Valorisation raisonnable (20) : PER et P/B vs secteur.
    val_pts, val_avail = 0.0, 0.0
    if pd.notna(r["per"]) and r["per"] > 0:
        val_avail += 12
        rel = r["per"] / max(med["per"], 1)
        val_pts += 12 if rel <= 0.85 else (8 if rel <= 1.1 else (4 if rel <= 1.4 else 0))
    if pd.notna(r["pb"]):
        val_avail += 8
        # P/B faible sans rentabilité n'est pas un plus ; P/B élevé + ROE fort acceptable.
        roe_ok = pd.notna(r["roe"]) and r["roe"] >= 0.10
        if r["pb"] <= med["pb"] and (roe_ok or pd.isna(r["roe"])):
            val_pts += 8
        elif r["pb"] <= med["pb"] * 1.3 or roe_ok:
            val_pts += 4
    if val_avail == 0:
        notes.append("valorisation non mesurable")
    earned += val_pts; available += val_avail

    # Rentabilité et marges (20).
    prof_pts, prof_avail = 0.0, 0.0
    g = _grade(r["roe"], 0.15, 0.08)
    if g is not None:
        prof_avail += 12; prof_pts += g * 12
    g = _grade(r["net_margin"], 0.10, 0.05)
    if g is not None:
        prof_avail += 8; prof_pts += g * 8
    if prof_avail == 0:
        notes.append("rentabilité non mesurable")
    earned += prof_pts; available += prof_avail

    # Dividende durable (30) : rendement + payout soutenable.
    if pd.notna(r["dividend_yield"]):
        available += 30
        pts = min(r["dividend_yield"] / 0.06, 1.0) * 15
        if pd.notna(r["payout_ratio"]):
            pts += 15 if 0.30 <= r["payout_ratio"] <= 0.80 else \
                   (8 if r["payout_ratio"] <= 1.0 else 0)   # payout > 100 % = risque
        else:
            pts += 7
        earned += pts
    else:
        notes.append("dividende inconnu")

    # Dette maîtrisée (20) : net debt/EBITDA prioritaire, sinon dette/fonds propres.
    if pd.notna(r["net_debt_ebitda"]):
        available += 20
        g = _grade(r["net_debt_ebitda"], 1.5, 3.0, reverse=True)
        earned += (g or 0) * 20
    elif pd.notna(r["debt_to_equity"]):
        available += 20
        g = _grade(r["debt_to_equity"], 0.5, 1.0, reverse=True)
        earned += (g or 0) * 20
    else:
        notes.append("endettement inconnu")

    # Régularité des résultats (10) : croissance EPS et bénéfices positifs.
    if pd.notna(r["eps_growth"]):
        available += 10
        pos = r["eps"] > 0 and (r["eps_prior"] or 0) > 0
        earned += (10 if r["eps_growth"] >= 0 and pos else (5 if pos else 0))
    else:
        notes.append("historique de résultats insuffisant")

    return earned, available, notes


def run(fund: pd.DataFrame, prices: pd.Series) -> pd.DataFrame:
    """Graham Score /100 + commentaire + couverture, indexé par ticker."""
    if fund.empty:
        return pd.DataFrame(columns=["graham_score", "graham_comment"])
    ratios = compute_ratios(fund, prices)
    med = _sector_medians(ratios)

    out = []
    for t, r in ratios.iterrows():
        m = med.loc[r["sector"]] if r["sector"] in med.index else med.mean()
        earned, avail, notes = (_score_bank(r, m) if r["is_bank"]
                                else _score_nonfin(r, m))
        coverage = avail / 100.0
        score = round(earned / avail * 100) if avail > 0 else 0
        if coverage < MIN_COVERAGE:
            score = min(score, COVERAGE_CAP)
            notes.insert(0, f"couverture {coverage:.0%} — score plafonné à {COVERAGE_CAP}")
        comment = "Banque : " if r["is_bank"] else ""
        strong = []
        if pd.notna(r["dividend_yield"]) and r["dividend_yield"] >= 0.06:
            strong.append(f"rendement {r['dividend_yield']:.1%}")
        if pd.notna(r["roe"]) and r["roe"] >= 0.15:
            strong.append(f"ROE {r['roe']:.0%}")
        if pd.notna(r["per"]) and 0 < r["per"] <= m["per"]:
            strong.append(f"PER {r['per']:.1f} < secteur")
        comment += ("points forts : " + ", ".join(strong) if strong else "profil moyen")
        if notes:
            comment += " | limites : " + "; ".join(notes[:3])
        out.append({"ticker": t, "graham_score": score,
                    "graham_coverage": round(coverage, 2),
                    "graham_comment": comment})
    res = pd.DataFrame(out).set_index("ticker")
    return ratios.join(res)
