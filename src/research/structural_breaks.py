"""Structural-break detector for Big Ideas shadow research."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date
from typing import Any

import polars as pl

from src.time_utils import et_date_expr, et_timestamp_expr


@dataclass(slots=True)
class StructuralBreakRow:
    symbol: str
    date: str
    close: float
    atr_20d: float | None
    weekly_reclaim_long: bool
    weekly_reclaim_short: bool
    daily_range_expansion: bool
    intraday_volume_acceptance_long: bool
    intraday_volume_acceptance_short: bool
    confluence_score_long: int
    confluence_score_short: int
    notes: str = ""

    def to_feed_row(self) -> dict[str, Any]:
        row = asdict(self)
        row["intraday_volume_acceptance"] = (
            self.intraday_volume_acceptance_long or self.intraday_volume_acceptance_short
        )
        row["confluence_score"] = max(self.confluence_score_long, self.confluence_score_short)
        return row


def daily_ohlcv_from_minutes(minute_bars: pl.DataFrame) -> pl.DataFrame:
    """Collapse minute bars into daily OHLCV."""

    required = {"timestamp", "open", "high", "low", "close", "volume"}
    missing = required - set(minute_bars.columns)
    if missing:
        raise ValueError(f"daily OHLCV requires columns: {sorted(missing)}")
    if minute_bars.is_empty():
        return pl.DataFrame()
    return (
        minute_bars.with_columns(et_date_expr("timestamp").alias("trade_date"))
        .sort(["trade_date", "timestamp"])
        .group_by("trade_date", maintain_order=True)
        .agg([
            pl.col("open").first().alias("open"),
            pl.col("high").max().alias("high"),
            pl.col("low").min().alias("low"),
            pl.col("close").last().alias("close"),
            pl.col("volume").sum().alias("volume"),
        ])
    )


def add_atr(daily_bars: pl.DataFrame, *, window: int = 20) -> pl.DataFrame:
    """Add ATR over daily bars."""

    required = {"trade_date", "high", "low", "close"}
    missing = required - set(daily_bars.columns)
    if missing:
        raise ValueError(f"ATR requires columns: {sorted(missing)}")
    return (
        daily_bars.sort("trade_date")
        .with_columns(pl.col("close").shift(1).alias("_prev_close"))
        .with_columns(
            pl.max_horizontal(
                pl.col("high") - pl.col("low"),
                (pl.col("high") - pl.col("_prev_close")).abs(),
                (pl.col("low") - pl.col("_prev_close")).abs(),
            ).alias("_true_range")
        )
        .with_columns(
            pl.col("_true_range").rolling_mean(window_size=window, min_samples=max(2, window // 2)).alias("atr_20d")
        )
        .drop(["_prev_close", "_true_range"])
    )


def latest_structural_break(
    daily_bars: pl.DataFrame,
    *,
    symbol: str,
    minute_bars: pl.DataFrame | None = None,
    as_of: date | str | None = None,
    reclaim_window: int = 20,
    range_atr_mult: float = 1.5,
) -> StructuralBreakRow | None:
    """Compute the latest structural-break row for one symbol."""

    if daily_bars.is_empty():
        return None
    df = _normalize_daily_dates(daily_bars)
    if as_of is not None:
        target = date.fromisoformat(as_of) if isinstance(as_of, str) else as_of
        df = df.filter(pl.col("trade_date") <= target)
    if df.height < max(5, reclaim_window // 2):
        return None

    df = (
        add_atr(df)
        .with_columns([
            pl.col("close").rolling_max(window_size=reclaim_window, min_samples=5).shift(1).alias("_prior_high_close"),
            pl.col("close").rolling_min(window_size=reclaim_window, min_samples=5).shift(1).alias("_prior_low_close"),
        ])
        .with_columns([
            (pl.col("close") > pl.col("_prior_high_close")).fill_null(False).alias("_weekly_reclaim_long"),
            (pl.col("close") < pl.col("_prior_low_close")).fill_null(False).alias("_weekly_reclaim_short"),
            (((pl.col("high") - pl.col("low")) / pl.col("atr_20d")) >= range_atr_mult)
            .fill_null(False)
            .alias("_daily_range_expansion"),
        ])
    )
    latest = df.tail(1).to_dicts()[0]
    trade_date = latest["trade_date"]
    if isinstance(trade_date, date):
        trade_date_text = trade_date.isoformat()
    else:
        trade_date_text = str(trade_date)

    acceptance_long, acceptance_short = _intraday_acceptance(minute_bars, trade_date) if minute_bars is not None else (False, False)
    weekly_long = bool(latest["_weekly_reclaim_long"])
    weekly_short = bool(latest["_weekly_reclaim_short"])
    range_expansion = bool(latest["_daily_range_expansion"])
    long_score = int(weekly_long) + int(range_expansion) + int(acceptance_long)
    short_score = int(weekly_short) + int(range_expansion) + int(acceptance_short)
    notes = []
    if weekly_long:
        notes.append("first close above trailing 20-session close high")
    if weekly_short:
        notes.append("first close below trailing 20-session close low")
    if range_expansion:
        notes.append(f"daily range >= {range_atr_mult:g}x ATR")
    if acceptance_long or acceptance_short:
        notes.append("breakout hour at/after highest-volume hour")
    return StructuralBreakRow(
        symbol=symbol.upper(),
        date=trade_date_text,
        close=float(latest["close"]),
        atr_20d=float(latest["atr_20d"]) if latest.get("atr_20d") is not None else None,
        weekly_reclaim_long=weekly_long,
        weekly_reclaim_short=weekly_short,
        daily_range_expansion=range_expansion,
        intraday_volume_acceptance_long=acceptance_long,
        intraday_volume_acceptance_short=acceptance_short,
        confluence_score_long=long_score,
        confluence_score_short=short_score,
        notes="; ".join(notes),
    )


def _normalize_daily_dates(daily_bars: pl.DataFrame) -> pl.DataFrame:
    if "trade_date" in daily_bars.columns:
        return daily_bars.sort("trade_date")
    if "date" in daily_bars.columns:
        return daily_bars.rename({"date": "trade_date"}).sort("trade_date")
    if "timestamp" in daily_bars.columns:
        return daily_bars.with_columns(et_date_expr("timestamp").alias("trade_date")).sort("trade_date")
    raise ValueError("daily bars must include trade_date, date, or timestamp")


def _intraday_acceptance(minute_bars: pl.DataFrame | None, trade_date: date | str) -> tuple[bool, bool]:
    if minute_bars is None or minute_bars.is_empty():
        return False, False
    df = minute_bars.with_columns([
        et_date_expr("timestamp").alias("trade_date"),
        et_timestamp_expr("timestamp").dt.hour().alias("_hour"),
    ]).filter(pl.col("trade_date") == trade_date)
    if df.is_empty():
        return False, False

    hourly = df.group_by("_hour").agg(pl.col("volume").sum().alias("_hour_volume")).sort("_hour")
    highest_volume_hour = int(hourly.sort("_hour_volume", descending=True).head(1).to_dicts()[0]["_hour"])
    high_row = df.sort(["high", "timestamp"], descending=[True, False]).head(1).to_dicts()[0]
    low_row = df.sort(["low", "timestamp"], descending=[False, False]).head(1).to_dicts()[0]
    long_acceptance = int(high_row["_hour"]) >= highest_volume_hour
    short_acceptance = int(low_row["_hour"]) >= highest_volume_hour
    return long_acceptance, short_acceptance
