"""Closing-auction proxy features derived from cached minute bars."""

from __future__ import annotations

from datetime import time

import polars as pl

from src.time_utils import et_date_expr, et_time_expr


_MARKET_OPEN = time(9, 30)
_MARKET_CLOSE = time(16, 0)


def auction_proxy_features(minute_bars: pl.DataFrame, *, lookback_days: int = 30) -> pl.DataFrame:
    """Return per-session 16:00/last-minute volume proxy features.

    Polygon minute aggregates usually include the closing auction in the last
    minute bar. This helper keeps the first research pass cheap and explicit.
    """

    required = {"timestamp", "open", "high", "low", "close", "volume"}
    missing = required - set(minute_bars.columns)
    if missing:
        raise ValueError(f"auction proxy requires columns: {sorted(missing)}")
    if minute_bars.is_empty():
        return pl.DataFrame()

    df = minute_bars.with_columns([
        et_date_expr("timestamp").alias("trade_date"),
        et_time_expr("timestamp").alias("_et_time"),
    ]).sort(["trade_date", "timestamp"])

    regular_session = df.filter(
        (pl.col("_et_time") >= _MARKET_OPEN)
        & (pl.col("_et_time") <= _MARKET_CLOSE)
    )
    session_df = regular_session if not regular_session.is_empty() else df

    daily = session_df.group_by("trade_date", maintain_order=True).agg([
        pl.col("open").first().alias("open"),
        pl.col("high").max().alias("high"),
        pl.col("low").min().alias("low"),
        pl.col("close").last().alias("close"),
        pl.col("volume").sum().alias("day_volume"),
    ])

    last_minute = (
        session_df
        .group_by("trade_date", maintain_order=True)
        .agg(pl.col("volume").last().alias("last_minute_volume"))
    )
    if last_minute.is_empty():
        last_minute = df.group_by("trade_date", maintain_order=True).agg(
            pl.col("volume").last().alias("last_minute_volume")
        )

    return (
        daily.join(last_minute, on="trade_date", how="left")
        .with_columns([
            (pl.col("last_minute_volume") / pl.col("day_volume")).alias("last_minute_share"),
            ((pl.col("close") - pl.col("low")) / (pl.col("high") - pl.col("low"))).fill_nan(0.5).alias(
                "close_in_range_pct"
            ),
        ])
        .with_columns(
            pl.col("last_minute_share")
            .rolling_quantile(quantile=0.8, window_size=lookback_days, min_samples=1)
            .shift(1)
            .alias("last_minute_share_p80_30d")
        )
        .with_columns(
            (pl.col("last_minute_share") >= pl.col("last_minute_share_p80_30d"))
            .fill_null(False)
            .alias("last_minute_share_pctile_30d_ge_80")
        )
    )
