"""
Analyse technique Weinstein adaptée BRVM.

Hebdomadaire, MM30 semaines, pente, force relative vs BRVM 30,
Volume Confirmation Score, Stage 1-4, Technical Score /100 plafonné
par la classe de liquidité, Technical Reliability Score.
"""
import numpy as np
import pandas as pd

from . import config

P = config.PARAMS


# ── Construction hebdomadaire ────────────────────────────────────────────────

def build_weekly(daily: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """
    Par ticker : weekly_close (dernier cours coté, propagé pour la continuité
    de la MM30), weekly_volume, weekly_value, trading_days, weeks_since_trade.
    On ne fabrique jamais de prix : la propagation sert uniquement au calcul,
    et `weeks_since_trade` mesure l'ancienneté réelle du dernier échange.
    """
    out = {}
    for ticker, grp in daily.groupby("ticker"):
        g = grp.set_index("date").sort_index()
        w = pd.DataFrame({
            "close_raw": g["close"].resample("W-FRI").last(),
            "volume": g["volume"].resample("W-FRI").sum(),
            "value_traded": g["value_traded"].resample("W-FRI").sum(),
            "trading_days": g["close"].resample("W-FRI").count(),
        })
        w["close"] = w["close_raw"].ffill()
        w = w[w["close"].notna()]                      # avant la première cotation
        traded = w["trading_days"] > 0
        # Nombre de semaines écoulées depuis la dernière semaine cotée.
        grp_id = traded.cumsum()
        w["weeks_since_trade"] = w.groupby(grp_id).cumcount()
        out[ticker] = w
    return out


# ── Indicateurs par titre ────────────────────────────────────────────────────

def _pct_change(series: pd.Series, weeks: int) -> float | None:
    if len(series) <= weeks or series.iloc[-1 - weeks] in (0, None):
        return None
    past = series.iloc[-1 - weeks]
    if pd.isna(past) or past == 0:
        return None
    return float(series.iloc[-1] / past - 1)


def compute_indicators(weekly: dict[str, pd.DataFrame],
                       benchmark: pd.Series) -> pd.DataFrame:
    """Une ligne par ticker : MM30, pente, RS 26 semaines, plus haut 26s, staleness."""
    ma_w, slope_w = P["ma_weeks"], P["ma_slope_lookback_weeks"]
    rs_w = P["relative_strength_lookback_weeks"]
    bench_perf = _pct_change(benchmark, rs_w)

    rows = []
    for ticker, w in weekly.items():
        close = w["close"]
        last = float(close.iloc[-1])
        ma30 = close.rolling(ma_w, min_periods=max(8, ma_w // 2)).mean()
        ma_now = float(ma30.iloc[-1]) if not pd.isna(ma30.iloc[-1]) else None
        ma_past = (float(ma30.iloc[-1 - slope_w])
                   if len(ma30) > slope_w and not pd.isna(ma30.iloc[-1 - slope_w]) else None)
        slope = (ma_now - ma_past) if (ma_now is not None and ma_past is not None) else None
        slope_pct = (slope / ma_past) if (slope is not None and ma_past) else None

        perf_26w = _pct_change(close, rs_w)
        rs_26w = (perf_26w - bench_perf) if (perf_26w is not None and bench_perf is not None) else None

        hi_26w = float(close.iloc[-P["breakout_lookback_weeks"]:].max())
        stale = int(w["weeks_since_trade"].iloc[-1])

        rows.append({
            "ticker": ticker,
            "close": last,
            "ma30w": round(ma_now, 2) if ma_now is not None else None,
            "ma30w_slope": round(slope, 2) if slope is not None else None,
            "ma30w_slope_pct": slope_pct,
            "price_above_ma30": bool(last > ma_now) if ma_now is not None else None,
            "distance_to_ma30": (last / ma_now - 1) if ma_now else None,
            "rs_26w_vs_brvm30": rs_26w,
            "perf_26w": perf_26w,
            "perf_4w": _pct_change(close, 4),
            "new_high_26w": bool(last >= hi_26w * 0.999),
            "weeks_since_trade": stale,
            "stale_price_flag": stale > P["stale_weeks_threshold"],
            "history_weeks": len(close),
        })
    return pd.DataFrame(rows).set_index("ticker")


# ── Volume Confirmation Score /20 ────────────────────────────────────────────

def volume_confirmation(w: pd.DataFrame) -> int:
    """Bonus de confirmation, jamais une condition obligatoire (§8)."""
    score = 0
    val = w["value_traded"]
    if len(val) >= 12:
        avg4, avg12 = val.iloc[-4:].mean(), val.iloc[-12:].mean()
        if avg12 > 0 and avg4 > avg12:
            score += 8
        if (w["trading_days"].iloc[-12:] > 0).sum() >= 8:
            score += 5
        med12 = val.iloc[-12:].median()
        rets = w["close"].pct_change(fill_method=None)
        recent_up = rets.iloc[-4:].sum() > 0
        if recent_up and val.iloc[-4:].mean() > med12:
            score += 5
        # Absence de gap artificiel : pas de saut > 15 % sur une semaine quasi sans échange.
        jumps = rets.iloc[-12:].abs() > 0.15
        thin = val.iloc[-12:] < max(med12, 1) * 0.25
        if not (jumps & thin).any():
            score += 2
    return score


# ── Stage Weinstein ──────────────────────────────────────────────────────────

def classify_stage(r: pd.Series) -> str:
    """Stage 1-4 selon prix vs MM30, pente et force relative (§9)."""
    if r["ma30w"] is None or pd.isna(r["ma30w"]):
        return "Indéterminé"
    above = bool(r["price_above_ma30"])
    slope = r["ma30w_slope_pct"]
    slope_up = slope is not None and slope > 0.001      # > +0.1 % sur 4 semaines
    slope_down = slope is not None and slope < -0.001
    rs = r["rs_26w_vs_brvm30"]
    rs_pos = rs is not None and rs > 0
    rs_neg = rs is not None and rs < -0.03

    if above and slope_up and rs_pos:
        return "Stage 2"
    if not above and slope_down and (rs is None or rs < 0):
        return "Stage 4"
    if above and not slope_up:
        return "Stage 3" if rs_neg else "Stage 1"
    if not above and not slope_down:
        return "Stage 1"
    return "Stage 3" if above else "Stage 4"


# ── Technical Score /100 ─────────────────────────────────────────────────────

_STAGE_POINTS = {"Stage 2": 20, "Stage 1": 12, "Stage 3": 5, "Stage 4": 0, "Indéterminé": 6}


def technical_score(r: pd.Series, vol_score_20: int, liquidity_class: str) -> dict:
    """
    Blocs (§10) : prix vs MM30 20 pts, pente 20, force relative 25,
    stage 20, volume 15. Plafond A/B/C ensuite.
    """
    pts_price = 0
    if r["price_above_ma30"]:
        dist = r["distance_to_ma30"] or 0
        pts_price = 20 if dist <= 0.15 else 14          # trop étendu = léger malus
    elif r["distance_to_ma30"] is not None and r["distance_to_ma30"] > -0.03:
        pts_price = 8                                    # juste sous la MM30

    slope = r["ma30w_slope_pct"]
    if slope is None:
        pts_slope = 5
    elif slope > 0.01:
        pts_slope = 20
    elif slope > 0.001:
        pts_slope = 14
    elif slope > -0.001:
        pts_slope = 8                                    # MM30 plate
    else:
        pts_slope = 0

    rs = r["rs_26w_vs_brvm30"]
    if rs is None:
        pts_rs = 8
    elif rs > 0.10:
        pts_rs = 25
    elif rs > 0.03:
        pts_rs = 18
    elif rs > -0.03:
        pts_rs = 10
    else:
        pts_rs = 0

    stage = r["stage_weinstein"]
    pts_stage = _STAGE_POINTS.get(stage, 6)
    if stage == "Stage 2" and r["new_high_26w"]:
        pts_stage = 20
    elif stage == "Stage 2":
        pts_stage = 17

    pts_vol = round(vol_score_20 * 15 / 20)              # /20 ramené sur 15 pts

    raw = pts_price + pts_slope + pts_rs + pts_stage + pts_vol
    cap = P[f"technical_cap_{liquidity_class}"]
    capped = min(raw, cap)

    # Fiabilité du signal : liquidité + fraîcheur du prix + profondeur d'historique.
    reliability = {"A": 100, "B": 75, "C": 50}[liquidity_class]
    if r["stale_price_flag"]:
        reliability -= 20
    if r["history_weeks"] < P["ma_weeks"] + P["ma_slope_lookback_weeks"]:
        reliability -= 15
    reliability = max(reliability, 10)

    return {"technical_score_raw": raw, "technical_score": capped,
            "technical_reliability_score": reliability,
            "pts_price": pts_price, "pts_slope": pts_slope,
            "pts_rs": pts_rs, "pts_stage": pts_stage, "pts_volume": pts_vol}


def run(weekly: dict[str, pd.DataFrame], benchmark: pd.Series,
        liquidity_class: pd.Series) -> pd.DataFrame:
    """Pipeline technique complet -> DataFrame indexé par ticker."""
    ind = compute_indicators(weekly, benchmark)
    ind["volume_confirmation_score"] = [volume_confirmation(weekly[t]) for t in ind.index]
    ind["stage_weinstein"] = ind.apply(classify_stage, axis=1)
    scores = ind.apply(
        lambda r: technical_score(r, r["volume_confirmation_score"],
                                  liquidity_class.get(r.name, "C")), axis=1)
    return ind.join(pd.DataFrame(list(scores), index=ind.index))
