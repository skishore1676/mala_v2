"""Causal ET-session daily-bar construction for classical-pattern research."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from math import ceil
import hashlib

import polars as pl

from src.time_utils import et_date_expr, et_time_expr
from src.trading_calendar import expected_rth_minutes, nyse_market_close

from .contracts import SessionDefinition


REQUIRED_OHLCV_COLUMNS = ("timestamp", "open", "high", "low", "close", "volume")


def build_rth_daily_bars(
    source_bars: pl.DataFrame,
    *,
    symbol: str,
    session: SessionDefinition,
    require_complete: bool = True,
) -> pl.DataFrame:
    """Aggregate stored UTC bars into regular-session daily OHLCV.

    `visible_at` is the final source-bar timestamp included in the daily close.
    All grouping is by America/New_York date, never by UTC calendar date.
    """

    _validate_source_bars(source_bars)
    if source_bars.is_empty():
        return empty_daily_bars()

    enriched = (
        source_bars.select(REQUIRED_OHLCV_COLUMNS)
        .sort("timestamp")
        .with_columns(
            et_date_expr("timestamp").alias("session_date"),
            et_time_expr("timestamp").alias("session_time"),
        )
        .with_columns(
            pl.col("session_date")
            .map_elements(nyse_market_close, return_dtype=pl.Time)
            .alias("expected_market_close"),
            pl.col("session_date")
            .map_elements(expected_rth_minutes, return_dtype=pl.Int64)
            .alias("expected_rth_minutes"),
        )
        .filter(
            (pl.col("session_time") >= session.market_open)
            & (pl.col("session_time") < pl.col("expected_market_close"))
        )
    )
    if enriched.is_empty():
        return empty_daily_bars()

    daily = (
        enriched.group_by("session_date", maintain_order=True)
        .agg(
            pl.col("timestamp").max().alias("visible_at"),
            pl.col("open").first().cast(pl.Float64).alias("open"),
            pl.col("high").max().cast(pl.Float64).alias("high"),
            pl.col("low").min().cast(pl.Float64).alias("low"),
            pl.col("close").last().cast(pl.Float64).alias("close"),
            pl.col("volume").sum().cast(pl.Float64).alias("volume"),
            pl.len().cast(pl.Int64).alias("source_bar_count"),
            pl.col("session_time").min().alias("first_source_time"),
            pl.col("session_time").max().alias("last_source_time"),
            pl.col("expected_market_close").first(),
            pl.col("expected_rth_minutes").first(),
        )
        .with_columns(
            pl.lit(symbol.upper()).alias("symbol"),
            (
                (
                    pl.col("source_bar_count")
                    >= pl.col("expected_rth_minutes").map_elements(
                        lambda minutes: ceil(
                            session.minimum_source_bars * minutes / 390
                        ),
                        return_dtype=pl.Int64,
                    )
                )
                & (pl.col("first_source_time") <= time(9, 31))
                & (
                    pl.col("last_source_time")
                    >= pl.col("expected_market_close").map_elements(
                        lambda close: (
                            datetime.combine(date.min, close) - timedelta(minutes=1)
                        ).time(),
                        return_dtype=pl.Time,
                    )
                )
            ).alias("complete_session"),
        )
        .select(
            "session_date",
            "visible_at",
            "symbol",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "source_bar_count",
            "complete_session",
        )
        .sort("session_date")
    )
    _validate_daily_ohlc(daily)
    if require_complete:
        daily = daily.filter(pl.col("complete_session"))
    return daily


def normalize_daily_input(
    daily_bars: pl.DataFrame,
    *,
    symbol: str,
) -> pl.DataFrame:
    """Normalize an already-daily fixture/provider frame.

    This adapter is for deterministic fixtures and independently licensed daily
    feeds.  Production minute caches should use :func:`build_rth_daily_bars` so
    session completeness remains auditable.
    """

    required = {"open", "high", "low", "close", "volume"}
    missing = required - set(daily_bars.columns)
    if missing:
        raise ValueError(f"Daily input is missing columns: {sorted(missing)}")
    if "session_date" not in daily_bars.columns and "timestamp" not in daily_bars.columns:
        raise ValueError("Daily input needs session_date or timestamp.")

    frame = daily_bars.clone()
    if "session_date" not in frame.columns:
        frame = frame.with_columns(pl.col("timestamp").dt.date().alias("session_date"))
    else:
        frame = frame.with_columns(pl.col("session_date").cast(pl.Date))

    if "visible_at" not in frame.columns:
        if "timestamp" in frame.columns:
            frame = frame.with_columns(pl.col("timestamp").alias("visible_at"))
        else:
            frame = frame.with_columns(
                (
                    pl.col("session_date").cast(pl.Datetime("us"))
                    + pl.duration(hours=16)
                )
                .dt.replace_time_zone("America/New_York")
                .dt.convert_time_zone("UTC")
                .alias("visible_at")
            )

    frame = (
        frame.with_columns(
            pl.lit(symbol.upper()).alias("symbol"),
            pl.col("open").cast(pl.Float64),
            pl.col("high").cast(pl.Float64),
            pl.col("low").cast(pl.Float64),
            pl.col("close").cast(pl.Float64),
            pl.col("volume").cast(pl.Float64),
            pl.lit(1).cast(pl.Int64).alias("source_bar_count"),
            pl.lit(True).alias("complete_session"),
        )
        .select(
            "session_date",
            "visible_at",
            "symbol",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "source_bar_count",
            "complete_session",
        )
        .sort("session_date")
    )
    if frame.get_column("session_date").n_unique() != len(frame):
        raise ValueError("Daily input contains duplicate session dates.")
    _validate_daily_ohlc(frame)
    return frame


def hash_daily_bars(daily_bars: pl.DataFrame) -> str:
    """Return a stable hash over the research-relevant daily-bar columns."""

    columns = [
        "session_date",
        "visible_at",
        "symbol",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "source_bar_count",
        "complete_session",
    ]
    missing = set(columns) - set(daily_bars.columns)
    if missing:
        raise ValueError(f"Cannot hash daily bars without columns: {sorted(missing)}")
    encoded = daily_bars.select(columns).sort("session_date").write_csv().encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def empty_daily_bars() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "session_date": pl.Date,
            "visible_at": pl.Datetime("us"),
            "symbol": pl.String,
            "open": pl.Float64,
            "high": pl.Float64,
            "low": pl.Float64,
            "close": pl.Float64,
            "volume": pl.Float64,
            "source_bar_count": pl.Int64,
            "complete_session": pl.Boolean,
        }
    )


def _validate_source_bars(source_bars: pl.DataFrame) -> None:
    missing = set(REQUIRED_OHLCV_COLUMNS) - set(source_bars.columns)
    if missing:
        raise ValueError(f"Source bars are missing columns: {sorted(missing)}")
    if source_bars.is_empty():
        return
    if source_bars.get_column("timestamp").n_unique() != len(source_bars):
        raise ValueError("Source bars contain duplicate timestamps.")
    if source_bars.select(
        pl.any_horizontal([pl.col(c).is_null() for c in REQUIRED_OHLCV_COLUMNS]).any()
    ).item():
        raise ValueError("Source bars contain null OHLCV values.")
    numeric = ("open", "high", "low", "close", "volume")
    if source_bars.select(
        pl.any_horizontal([~pl.col(c).cast(pl.Float64).is_finite() for c in numeric]).any()
    ).item():
        raise ValueError("Source bars contain non-finite OHLCV values.")
    invalid = source_bars.filter(
        (pl.col("high") < pl.max_horizontal("open", "close", "low"))
        | (pl.col("low") > pl.min_horizontal("open", "close", "high"))
        | (pl.col("volume") < 0)
    )
    if not invalid.is_empty():
        raise ValueError("Source bars contain invalid OHLCV relationships.")


def _validate_daily_ohlc(daily: pl.DataFrame) -> None:
    if daily.is_empty():
        return
    required = ("session_date", "visible_at", "symbol", "open", "high", "low", "close", "volume")
    if daily.select(
        pl.any_horizontal([pl.col(c).is_null() for c in required]).any()
    ).item():
        raise ValueError("Daily bars contain null required values.")
    numeric = ("open", "high", "low", "close", "volume")
    if daily.select(
        pl.any_horizontal([~pl.col(c).cast(pl.Float64).is_finite() for c in numeric]).any()
    ).item():
        raise ValueError("Daily bars contain non-finite OHLCV values.")
    invalid = daily.filter(
        (pl.col("high") < pl.max_horizontal("open", "close", "low"))
        | (pl.col("low") > pl.min_horizontal("open", "close", "high"))
        | (pl.col("volume") < 0)
    )
    if not invalid.is_empty():
        bad_dates = invalid.get_column("session_date").to_list()
        raise ValueError(f"Invalid daily OHLCV relationships for sessions: {bad_dates}")


def _time_tuple(value: time) -> tuple[int, int]:
    """Compatibility helper retained for tests and call sites."""

    return value.hour, value.minute
