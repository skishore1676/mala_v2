"""
Market regime classifier — shared vocabulary for post-hoc per-regime
slicing of M1/M4/M5 research artifacts.

Public API:

    classify_range(start, end) -> dict[date, MarketRegime]
    classify(target_date)      -> MarketRegime

MarketRegime has three categorical fields:

    vix_band      : "low" / "mid" / "high"
    spy_trend_20d : "up"  / "flat" / "down"
    session_type  : "normal" / "opex"

SPY closes are computed from the local Parquet 1-min cache (no extra
Polygon call needed). VIX is optional — if the Polygon subscription does
not include indices, vix_band defaults to "mid".

**Purpose in v2**: regime is observational, not a gate. It enriches
detail CSVs so post-hoc analysis can answer: "did this pass only in
high-VIX?" or "was OPEX distorting results?".
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Literal, Optional

import polars as pl

from src.config import settings

logger = logging.getLogger(__name__)


# ── Types ─────────────────────────────────────────────────────────────────────

VixBand = Literal["low", "mid", "high"]
SpyTrend = Literal["up", "flat", "down"]
SessionType = Literal["normal", "opex"]


@dataclass(frozen=True)
class MarketRegime:
    trading_date: date
    vix_band: VixBand
    spy_trend_20d: SpyTrend
    session_type: SessionType
    # Raw values for debugging
    vix_close: Optional[float]
    spy_close: float
    spy_sma20: float
    spy_trend_slope_pct: float

    @property
    def regime_key(self) -> str:
        return f"vix={self.vix_band}__spy={self.spy_trend_20d}__session={self.session_type}"


# ── Thresholds ────────────────────────────────────────────────────────────────

VIX_LOW_CEILING = 15.0
VIX_HIGH_FLOOR = 22.0
SPY_TREND_FLAT_THRESHOLD_PCT_PER_DAY = 0.05


# ── Pure classification logic (unit-testable, no I/O) ────────────────────────

def classify_vix_band(vix_close: Optional[float]) -> VixBand:
    if vix_close is None:
        return "mid"
    if vix_close < VIX_LOW_CEILING:
        return "low"
    if vix_close > VIX_HIGH_FLOOR:
        return "high"
    return "mid"


def classify_spy_trend(
    sma20_today: float, sma20_five_days_ago: float
) -> tuple[SpyTrend, float]:
    if sma20_five_days_ago == 0:
        return "flat", 0.0
    total_pct = (sma20_today / sma20_five_days_ago - 1.0) * 100.0
    slope_pct_per_day = total_pct / 5.0
    if slope_pct_per_day > SPY_TREND_FLAT_THRESHOLD_PCT_PER_DAY:
        return "up", slope_pct_per_day
    if slope_pct_per_day < -SPY_TREND_FLAT_THRESHOLD_PCT_PER_DAY:
        return "down", slope_pct_per_day
    return "flat", slope_pct_per_day


def _is_third_friday(d: date) -> bool:
    if d.weekday() != 4:
        return False
    return (d.day - 1) // 7 + 1 == 3


def classify_session_type(target_date: date) -> SessionType:
    if _is_third_friday(target_date):
        return "opex"
    return "normal"


# ── SPY data from local cache ─────────────────────────────────────────────────

def _spy_daily_closes_from_local(
    start: date, end: date
) -> dict[date, float]:
    """Compute daily closes from the local 1-min Parquet cache for SPY."""
    from src.chronos.storage import LocalStorage
    storage = LocalStorage()
    raw = storage.load_bars("SPY", start, end)
    if raw.is_empty():
        return {}
    daily = (
        raw.with_columns(pl.col("timestamp").dt.date().alias("_d"))
        .group_by("_d")
        .agg(pl.col("close").last())
        .sort("_d")
    )
    return {row["_d"]: float(row["close"]) for row in daily.iter_rows(named=True)}


def _vix_closes_from_polygon(
    start: date, end: date
) -> dict[date, float]:
    """Fetch VIX daily closes from Polygon. Returns {} on any failure."""
    import requests
    key = settings.polygon_api_key
    if not key:
        return {}
    url = (
        f"https://api.polygon.io/v2/aggs/ticker/I:VIX"
        f"/range/1/day/{start.isoformat()}/{end.isoformat()}"
    )
    try:
        resp = requests.get(
            url,
            params={"adjusted": "true", "sort": "asc", "limit": 50_000, "apiKey": key},
            timeout=20.0,
        )
        resp.raise_for_status()
        bars = resp.json().get("results") or []
    except Exception as exc:
        logger.warning("VIX fetch failed, defaulting to 'mid': %s", exc)
        return {}
    out: dict[date, float] = {}
    for bar in bars:
        d = datetime.utcfromtimestamp(bar["t"] / 1000).date()
        out[d] = float(bar["c"])
    return out


# ── Core classifier ───────────────────────────────────────────────────────────

def classify_range(
    start: date,
    end: date,
    *,
    lookback_buffer_days: int = 35,
) -> dict[date, MarketRegime]:
    """Classify every trading day in [start, end].

    One call — computes SPY closes from local Parquet cache, VIX from
    Polygon (optional, falls back to 'mid' if unavailable).
    """
    window_start = start - timedelta(days=lookback_buffer_days)

    spy_closes_map = _spy_daily_closes_from_local(window_start, end)
    if not spy_closes_map:
        logger.warning("No local SPY data found for regime classification")
        return {}

    vix_closes_map = _vix_closes_from_polygon(window_start, end)
    if not vix_closes_map:
        logger.debug("VIX unavailable, using vix_band='mid' for all days")

    trading_days = sorted(spy_closes_map.keys())
    closes_list = [spy_closes_map[d] for d in trading_days]

    out: dict[date, MarketRegime] = {}
    for idx, d in enumerate(trading_days):
        if d < start or d > end:
            continue
        if idx < 20 or idx < 25:
            continue  # not enough history for 20-day SMA + 5-day lag

        sma20_today = sum(closes_list[idx - 19: idx + 1]) / 20
        sma20_lag5  = sum(closes_list[idx - 24: idx - 4]) / 20
        trend_label, slope = classify_spy_trend(sma20_today, sma20_lag5)

        vix_close = vix_closes_map.get(d)
        out[d] = MarketRegime(
            trading_date=d,
            vix_band=classify_vix_band(vix_close),
            spy_trend_20d=trend_label,
            session_type=classify_session_type(d),
            vix_close=vix_close,
            spy_close=closes_list[idx],
            spy_sma20=sma20_today,
            spy_trend_slope_pct=slope,
        )

    return out


def classify(target_date: date) -> MarketRegime:
    """Classify a single trading day (convenience wrapper)."""
    window_start = target_date - timedelta(days=35)
    results = classify_range(window_start, target_date)
    if not results:
        raise RuntimeError(
            f"No SPY data available up to {target_date}. "
            "Ensure local Parquet cache contains SPY data."
        )
    latest_key = max(k for k in results if k <= target_date)
    return results[latest_key]
