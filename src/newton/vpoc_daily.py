"""Daily VPOC helpers for compression and structural-state research."""

from __future__ import annotations

import polars as pl

from src.time_utils import et_date_expr


def daily_vpoc(minute_bars: pl.DataFrame, *, price_round: float = 0.25) -> pl.DataFrame:
    """Collapse minute bars into a per-session volume point of control."""

    required = {"timestamp", "close", "volume"}
    missing = required - set(minute_bars.columns)
    if missing:
        raise ValueError(f"daily VPOC requires columns: {sorted(missing)}")
    if price_round <= 0:
        raise ValueError("price_round must be positive")
    if minute_bars.is_empty():
        return pl.DataFrame()

    df = minute_bars.with_columns([
        et_date_expr("timestamp").alias("trade_date"),
        ((pl.col("close") / price_round).round(0) * price_round).alias("_price_bin"),
    ])
    by_price = df.group_by(["trade_date", "_price_bin"]).agg(pl.col("volume").sum().alias("_bin_volume"))
    ranked = by_price.sort(["trade_date", "_bin_volume", "_price_bin"], descending=[False, True, False])
    return ranked.group_by("trade_date", maintain_order=True).agg([
        pl.col("_price_bin").first().alias("vpoc"),
        pl.col("_bin_volume").first().alias("vpoc_volume"),
    ])


def add_vpoc_drift(
    daily_features: pl.DataFrame,
    *,
    window: int = 7,
    min_sessions: int = 5,
) -> pl.DataFrame:
    """Add monotonic VPOC drift direction over the trailing window."""

    if "vpoc" not in daily_features.columns:
        raise ValueError("daily_features must include vpoc")
    if window < 2 or min_sessions < 2:
        raise ValueError("window and min_sessions must be at least 2")

    df = daily_features.sort("trade_date")
    return df.with_columns([
        pl.col("vpoc").diff().rolling_sum(window_size=window, min_samples=min_sessions).alias("vpoc_drift"),
        (pl.col("vpoc").diff() > 0)
        .cast(pl.Int8)
        .rolling_sum(window_size=window, min_samples=min_sessions)
        .alias("vpoc_up_sessions"),
        (pl.col("vpoc").diff() < 0)
        .cast(pl.Int8)
        .rolling_sum(window_size=window, min_samples=min_sessions)
        .alias("vpoc_down_sessions"),
    ])
