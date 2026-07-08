"""
Screener hybride BRVM — point d'entrée.

Graham (40 %) + Weinstein (30 %) + Catalyst (20 %) + Liquidité (10 %),
halal flexible, export Excel multi-onglets.

Usage :
  python -m brvm_screener.main            # depuis la racine du projet
  python -m brvm_screener.main strict     # halal_mode : off | flexible | strict
"""
import os
import sys

import pandas as pd

if __package__ is None or __package__ == "":          # exécution directe du fichier
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from brvm_screener import (catalyst, config, data_loader, fundamentals,
                               halal, liquidity, reporting, scoring, technical)
else:
    from . import (catalyst, config, data_loader, fundamentals, halal,
                   liquidity, reporting, scoring, technical)


def build_data_quality(master: pd.DataFrame, fund: pd.DataFrame) -> pd.DataFrame:
    """Onglet 06 : complétude, fraîcheur et niveau de confiance par titre."""
    fund_tickers = set(fund["ticker"]) if not fund.empty else set()
    rows = []
    for t, r in master.iterrows():
        missing_price = bool(r.get("history_weeks", 0) < config.PARAMS["ma_weeks"])
        missing_fund = t not in fund_tickers or (r.get("graham_coverage") or 0) < 0.6
        stale = bool(r.get("stale_price_flag", False))
        incomplete_events = bool(r.get("incomplete_event_data", True))

        problems = sum([missing_price, missing_fund, stale, incomplete_events])
        confidence = ["Haute", "Bonne", "Moyenne", "Faible", "Très faible"][min(problems, 4)]

        warn = []
        if missing_price:
            warn.append("historique de prix court (MM30 fragile)")
        if missing_fund:
            warn.append("fondamentaux incomplets")
        if stale:
            warn.append(f"dernier échange il y a {int(r.get('weeks_since_trade', 0))} semaines")
        if incomplete_events:
            warn.append("calendrier d'événements incomplet")
        rows.append({"ticker": t,
                     "missing_price_data": missing_price,
                     "missing_fundamental_data": missing_fund,
                     "stale_price_flag": stale,
                     "incomplete_event_data": incomplete_events,
                     "confidence_level": confidence,
                     "warning_message": " ; ".join(warn) if warn else "—"})
    return pd.DataFrame(rows).set_index("ticker")


def run(halal_mode: str | None = None) -> tuple[pd.DataFrame, str]:
    if halal_mode:
        config.PARAMS["halal_mode"] = halal_mode

    # 1. Données ------------------------------------------------------------
    data = data_loader.load_all()
    daily, bench = data["daily"], data["benchmark"]
    print(f"Données : {daily['ticker'].nunique()} titres, "
          f"{daily['date'].min().date()} → {data['as_of'].date()} | "
          f"benchmark = {data['benchmark_name']}")
    for w in data["warnings"]:
        print(f"  ⚠ {w}")

    # 2. Hebdomadaire + liquidité --------------------------------------------
    weekly = technical.build_weekly(daily)
    liq = liquidity.classify(weekly)

    # 3. Technique (plafonnée par la liquidité) ------------------------------
    tech = technical.run(weekly, bench, liq["liquidity_class"])
    liq["liquidity_execution_score"] = liquidity.execution_score(liq, tech["stale_price_flag"])

    # 4. Graham ---------------------------------------------------------------
    graham = fundamentals.run(data["fundamentals"], tech["close"])

    # 5. Catalyst -------------------------------------------------------------
    cat = catalyst.run(graham, data["events"], tech, data["as_of"])

    # 6. Fusion ----------------------------------------------------------------
    names = daily.groupby("ticker")[["company_name", "sector"]].last()
    master = (names.join(tech).join(liq)
              .join(graham.drop(columns=["sector"], errors="ignore")).join(cat))
    for col, default in (("graham_score", 0), ("catalyst_score", 0),
                         ("technical_score", 0), ("liquidity_execution_score", 0)):
        master[col] = master[col].fillna(default)
    med_per = master.groupby("sector")["per"].transform("median")
    master["per_sector_median"] = med_per

    # 7. Score final, décision, vente, sizing ---------------------------------
    master = scoring.run(master)
    plans = master.apply(liquidity.execution_plan, axis=1)
    master = master.join(pd.DataFrame(list(plans), index=master.index))

    # 8. Halal -----------------------------------------------------------------
    hal = halal.apply(master["final_score"], data["halal"])
    master = master.join(hal)
    if config.PARAMS["halal_mode"] == "strict":
        strict_zero = master["halal_adjusted_score"] == 0
        master.loc[strict_zero, "decision"] = "Exclu (halal strict)"

    # 9. Qualité des données + export ------------------------------------------
    quality = build_data_quality(master, data["fundamentals"])
    meta = {"as_of": data["as_of"].date().isoformat(),
            "benchmark": data["benchmark_name"],
            "halal_mode": config.PARAMS["halal_mode"],
            "n_tickers": len(master),
            "warnings": " | ".join(data["warnings"])}
    os.makedirs(config.OUTPUT_DIR, exist_ok=True)
    path = reporting.export(master, quality, meta)

    # 10. Synthèse console -------------------------------------------------------
    print(f"\n{'='*100}\n SCREENER HYBRIDE BRVM — {len(master)} titres | "
          f"halal_mode={config.PARAMS['halal_mode']} | as-of {meta['as_of']}\n{'='*100}")
    hdr = (f"{'#':>2} {'TK':6} {'Secteur':22} {'Liq':3} {'Stage':7} {'Grah':>4} "
           f"{'Tech':>4} {'Cata':>4} {'Exec':>4} {'FINAL':>5}  {'Décision':22} {'Halal'}")
    print(hdr); print("-" * len(hdr))
    for t, r in master.head(20).iterrows():
        print(f"{r['rank']:>2} {t:6} {r['sector'][:22]:22} {r['liquidity_class']:3} "
              f"{r['stage_weinstein'][:7]:7} {r['graham_score']:>4.0f} "
              f"{r['technical_score']:>4.0f} {r['catalyst_score']:>4.0f} "
              f"{r['liquidity_execution_score']:>4.0f} {r['final_score']:>5.1f}  "
              f"{r['decision'][:22]:22} {r['halal_status']}")
    counts = master["decision"].value_counts()
    print("\nRépartition : " + " | ".join(f"{k}: {v}" for k, v in counts.items()))
    print(f"\n📊 Rapport Excel : {path}")
    return master, path


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else None
    run(mode)
