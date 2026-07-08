"""
Halal flexible (§19) — classification sans exclusion automatique.

Modes :
  off      : aucun ajustement.
  flexible : score conservé, statut affiché, alerte si Non-compatible / À vérifier.
  strict   : score mis à zéro si Non-compatible ; À vérifier visible mais signalé.
"""
import pandas as pd

from . import config


def apply(final_scores: pd.Series, halal: pd.DataFrame) -> pd.DataFrame:
    mode = config.PARAMS["halal_mode"]
    status = halal.set_index("ticker")["halal_status"] if not halal.empty else pd.Series(dtype=object)

    rows = []
    for t, score in final_scores.items():
        st = status.get(t, "À vérifier")
        adjusted, alert = score, ""
        if mode == "strict" and st == "Non-compatible":
            adjusted = 0
            alert = "EXCLU (mode strict)"
        elif mode in ("flexible", "strict"):
            if st == "Non-compatible":
                alert = "⚠ Non-compatible halal"
            elif st == "À vérifier":
                alert = "À vérifier (analyse charia requise)"
        rows.append({"ticker": t, "halal_status": st,
                     "halal_adjusted_score": adjusted, "halal_alert": alert})
    return pd.DataFrame(rows).set_index("ticker")
