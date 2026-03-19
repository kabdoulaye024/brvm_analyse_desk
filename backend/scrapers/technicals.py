"""
Technical indicators calculation.
Ported from screener v6.0 — RSI adaptatif, Bollinger, EMA, SMA.
"""
import numpy as np
import pandas as pd


def calc_sma(close: pd.Series, period: int) -> pd.Series:
    return close.rolling(period).mean()


def calc_ema(close: pd.Series, period: int) -> pd.Series:
    return close.ewm(span=period, adjust=False).mean()


def calc_bollinger(close: pd.Series, period: int = 20, std_dev: float = 2.0) -> dict:
    mid = close.rolling(period).mean()
    std = close.rolling(period).std(ddof=1)
    return {
        "upper": (mid + std_dev * std).tolist(),
        "middle": mid.tolist(),
        "lower": (mid - std_dev * std).tolist(),
    }


def calc_rsi(close: pd.Series, period: int = 14) -> pd.Series:
    """Wilder RSI (EWM com = period - 1)."""
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = (-delta.clip(upper=0))
    avg_gain = gain.ewm(com=period - 1, adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(com=period - 1, adjust=False, min_periods=period).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def calc_rsi_adaptive(close: pd.Series, volume: pd.Series = None) -> tuple:
    """
    Adaptive RSI for illiquid markets (BRVM).
    Liquidity >= 80% → RSI(14)
    Liquidity 50-80% → RSI(21)
    Liquidity < 50%  → RSI(28)
    Returns (rsi_series, period, liquidity_pct)
    """
    if volume is not None:
        vol = pd.to_numeric(volume, errors="coerce").fillna(0)
        n_active = (vol.tail(60) > 0).sum()
        ratio = n_active / min(60, len(vol))
    else:
        ratio = 1.0

    if ratio >= 0.80:
        period = 14
    elif ratio >= 0.50:
        period = 21
    else:
        period = 28

    rsi = calc_rsi(close, period)
    return rsi, period, round(ratio * 100, 0)


def calc_all_indicators(df: pd.DataFrame) -> dict:
    """
    Calculate all technical indicators from daily OHLCV data.
    Returns dict with indicator values for the latest point + full series.
    """
    if df.empty or len(df) < 20:
        return {}

    close = df["close"].astype(float).reset_index(drop=True)
    volume = df["volume"] if "volume" in df.columns else None

    # EMA
    ema20 = calc_ema(close, 20)
    sma20 = calc_sma(close, 20)
    sma50 = calc_sma(close, 50)

    # RSI adaptive
    rsi_series, rsi_period, liq_pct = calc_rsi_adaptive(close, volume)

    # Bollinger
    bb = calc_bollinger(close, 20, 2.0)
    bb_mid = sma20
    bb_std = close.rolling(20).std(ddof=1)
    bb_upper = bb_mid + 2 * bb_std
    bb_lower = bb_mid - 2 * bb_std

    # Variations
    def var_period(n):
        lag = min(n, len(close) - 1)
        if lag <= 0:
            return 0.0
        return round((close.iloc[-1] / close.iloc[-lag - 1] - 1) * 100, 2)

    var_1w = var_period(5)
    var_1m = var_period(21)
    var_3m = var_period(63)

    # Volume average (active sessions)
    vol_avg_20 = 0.0
    if volume is not None:
        vols = pd.to_numeric(volume, errors="coerce").fillna(0)
        pos = vols[vols > 0]
        if len(pos) >= 5:
            vol_avg_20 = float(pos.tail(20).mean())

    # 52-week high/low
    close_52w = close.tail(min(252, len(close)))
    high_52w = float(close_52w.max())
    low_52w = float(close_52w.min())
    current = float(close.iloc[-1])
    range_52w_pct = ((current - low_52w) / (high_52w - low_52w) * 100
                     if high_52w != low_52w else 50.0)

    def _safe(v):
        """Convert NaN/Inf to None for JSON serialization."""
        if v is None:
            return None
        f = float(v)
        if np.isnan(f) or np.isinf(f):
            return None
        return f

    def _safe_round(v, decimals=0):
        s = _safe(v)
        return round(s, decimals) if s is not None else None

    def _clean_series(s):
        """Convert a pandas Series or list to JSON-safe list (NaN → None)."""
        if isinstance(s, pd.Series):
            return [_safe(x) for x in s.tolist()]
        return [_safe(x) for x in s]

    return {
        "current_price": current,
        "rsi": _safe_round(rsi_series.iloc[-1], 1),
        "rsi_period": rsi_period,
        "liquidity_pct": liq_pct,
        "ema20": _safe_round(ema20.iloc[-1]),
        "sma20": _safe_round(sma20.iloc[-1]),
        "sma50": _safe_round(sma50.iloc[-1]) if len(close) >= 50 else None,
        "bb_upper": _safe_round(bb_upper.iloc[-1]),
        "bb_lower": _safe_round(bb_lower.iloc[-1]),
        "bb_middle": _safe_round(bb_mid.iloc[-1]),
        "var_1w": var_1w,
        "var_1m": var_1m,
        "var_3m": var_3m,
        "vol_avg_20d": round(vol_avg_20, 0),
        "high_52w": high_52w,
        "low_52w": low_52w,
        "range_52w_pct": round(range_52w_pct, 1),
        "nb_points": len(close),
        # Full series for charting (NaN → None)
        "series": {
            "dates": df["date"].dt.strftime("%Y-%m-%d").tolist() if "date" in df.columns else [],
            "close": close.tolist(),
            "open": _clean_series(df["open"]) if "open" in df.columns and df["open"].notna().any() else [],
            "high": _clean_series(df["high"]) if "high" in df.columns and df["high"].notna().any() else [],
            "low": _clean_series(df["low"]) if "low" in df.columns and df["low"].notna().any() else [],
            "volume": df["volume"].fillna(0).tolist() if "volume" in df.columns else [],
            "ema20": _clean_series(ema20),
            "sma50": _clean_series(sma50) if len(close) >= 50 else [],
            "rsi": _clean_series(rsi_series),
            "bb_upper": _clean_series(bb_upper),
            "bb_lower": _clean_series(bb_lower),
            "bb_middle": _clean_series(bb_mid),
        },
    }
