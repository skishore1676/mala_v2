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

SPY closes are computed from the local Parquet 1-min cache. VIX is read
from a local cache, refreshed from Cboe first, and falls back to FRED; if
VIX is unavailable, vix_band defaults to "mid".

**Purpose in v2**: regime is observational, not a gate. It enriches
detail CSVs so post-hoc analysis can answer: "did this pass only in
high-VIX?" or "was OPEX distorting results?".
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
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


VIX_CACHE_PATH = Path(__file__).resolve().parent.parent.parent / "data" / "_vix_daily.parquet"


def _load_vix_cache() -> dict[date, float]:
    """Read locally cached VIX daily closes. Returns {} if no cache."""
    if not VIX_CACHE_PATH.exists():
        return {}
    try:
        df = pl.read_parquet(VIX_CACHE_PATH)
        return {
            row["date"]: float(row["close"])
            for row in df.iter_rows(named=True)
        }
    except Exception:
        return {}


def _save_vix_cache(data: dict[date, float]) -> None:
    """Merge new VIX data into the local Parquet cache."""
    existing = _load_vix_cache()
    existing.update(data)
    if not existing:
        return
    rows = [{"date": d, "close": c} for d, c in sorted(existing.items())]
    df = pl.DataFrame(rows).with_columns(pl.col("date").cast(pl.Date))
    VIX_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(VIX_CACHE_PATH)
    logger.debug("VIX cache updated: %d days → %s", len(rows), VIX_CACHE_PATH)


def _vix_from_cboe_csv() -> dict[date, float]:
    """Download full VIX history from CBOE. Returns {} on failure."""
    import csv
    import io
    import requests

    url = "https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv"
    try:
        resp = requests.get(url, timeout=30.0)
        resp.raise_for_status()
    except Exception as exc:
        logger.debug("CBOE VIX fetch failed: %s", exc.__class__.__name__)
        return {}

    out: dict[date, float] = {}
    reader = csv.DictReader(io.StringIO(resp.text))
    for row in reader:
        try:
            d = datetime.strptime(row["DATE"].strip(), "%m/%d/%Y").date()
            out[d] = float(row["CLOSE"])
        except (KeyError, ValueError):
            continue
    return out


def _vix_from_fred_csv() -> dict[date, float]:
    """Download VIX from FRED (no API key needed for CSV). Returns {} on failure."""
    import csv
    import io
    import requests

    url = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=VIXCLS"
    try:
        resp = requests.get(url, timeout=30.0)
        resp.raise_for_status()
    except Exception as exc:
        logger.debug("FRED VIX fetch failed: %s", exc.__class__.__name__)
        return {}

    out: dict[date, float] = {}
    reader = csv.DictReader(io.StringIO(resp.text))
    for row in reader:
        try:
            d = date.fromisoformat(row["DATE"].strip())
            val = row["VIXCLS"].strip()
            if val == "." or not val:
                continue
            out[d] = float(val)
        except (KeyError, ValueError):
            continue
    return out


def _fetch_vix_closes(start: date, end: date) -> dict[date, float]:
    """Fetch VIX daily closes using a fallback chain: cache → CBOE → FRED.

    Newly downloaded data is merged into the local Parquet cache.
    Returns {} if all sources fail (caller defaults to vix_band='mid').
    """
    # 1. Check cache first
    cached = _load_vix_cache()
    if cached:
        in_range = {d: v for d, v in cached.items() if start <= d <= end}
        # If cache covers most of the range, use it
        if len(in_range) > 0:
            # Check if cache is reasonably fresh (has data within 7 days of end)
            latest_cached = max(cached.keys())
            if latest_cached >= end - timedelta(days=7):
                logger.debug("VIX from cache: %d days in range", len(in_range))
                return in_range

    # 2. Try CBOE (full history CSV, free, no key needed)
    fresh = _vix_from_cboe_csv()
    if not fresh:
        # 3. Try FRED (CSV endpoint, free, no key needed)
        fresh = _vix_from_fred_csv()

    if fresh:
        _save_vix_cache(fresh)
        in_range = {d: v for d, v in fresh.items() if start <= d <= end}
        logger.debug("VIX fetched: %d total, %d in range", len(fresh), len(in_range))
        return in_range

    # 4. Fall back to stale cache if we have anything
    if cached:
        in_range = {d: v for d, v in cached.items() if start <= d <= end}
        if in_range:
            logger.debug("VIX from stale cache: %d days in range", len(in_range))
            return in_range

    logger.warning("VIX unavailable from all sources, defaulting to 'mid'")
    return {}


# ── Core classifier ───────────────────────────────────────────────────────────

def classify_range(
    start: date,
    end: date,
    *,
    lookback_buffer_days: int = 35,
) -> dict[date, MarketRegime]:
    """Classify every trading day in [start, end].

    One call — computes SPY closes from local Parquet cache and VIX from
    local/Cboe/FRED sources, falling back to 'mid' if unavailable.
    """
    window_start = start - timedelta(days=lookback_buffer_days)

    spy_closes_map = _spy_daily_closes_from_local(window_start, end)
    if not spy_closes_map:
        logger.warning("No local SPY data found for regime classification")
        return {}

    vix_closes_map = _fetch_vix_closes(window_start, end)
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
