from __future__ import annotations

from datetime import datetime, timedelta

import polars as pl

from src.strategy.market_impulse import MarketImpulseStrategy


def _frame(rows: list[dict]) -> pl.DataFrame:
    base_time = datetime(2025, 1, 2, 14, 35)
    data = []
    for index, row in enumerate(rows):
        high = row["high"]
        low = row["low"]
        close = row["close"]
        vma = row.get("vma_10", 100.0)
        bar_range = high - low
        data.append(
            {
                "timestamp": base_time + timedelta(minutes=index),
                "open": row.get("open", close),
                "high": high,
                "low": low,
                "close": close,
                "volume": row.get("volume", 1000.0),
                "vma_10": vma,
                "impulse_regime_5m": row.get("regime", "bullish"),
                "close_location": ((close - low) / bar_range) if bar_range > 0 else 0.5,
                "vma_excursion_pct": row.get(
                    "vma_excursion_pct",
                    max(max(vma - low, 0.0), max(high - vma, 0.0)) / vma,
                ),
            }
        )
    return pl.DataFrame(data)


def test_same_bar_shallow_reclaim_fires_only_within_excursion_limit() -> None:
    strategy = MarketImpulseStrategy(
        entry_mode="same_bar_shallow_reclaim",
        max_vma_excursion_pct=0.001,
    )
    result = strategy.generate_signals(
        _frame([
            {"high": 100.2, "low": 99.95, "close": 100.10},
            {"high": 100.2, "low": 99.70, "close": 100.10},
        ])
    )

    assert result["signal"].to_list() == [True, False]
    assert result["signal_direction"].to_list() == ["long", None]


def test_deep_same_bar_reclaim_blocked_by_threshold() -> None:
    strategy = MarketImpulseStrategy(
        entry_mode="same_bar_shallow_reclaim",
        max_vma_excursion_pct=0.0004,
    )
    result = strategy.generate_signals(
        _frame([{"high": 100.2, "low": 99.95, "close": 100.10}])
    )

    assert result["signal"].to_list() == [False]


def test_delayed_reclaim_fires_on_reclaim_bar_inside_window() -> None:
    strategy = MarketImpulseStrategy(
        entry_mode="delayed_reclaim",
        reclaim_window_bars=2,
        min_bars_after_pierce=1,
        max_vma_excursion_pct=0.01,
    )
    result = strategy.generate_signals(
        _frame([
            {"high": 100.0, "low": 99.8, "close": 99.9},
            {"high": 100.3, "low": 99.9, "close": 100.2},
        ])
    )

    assert result["signal"].to_list() == [False, True]
    assert result["signal_direction"].to_list() == [None, "long"]


def test_delayed_reclaim_does_not_fire_after_expiry() -> None:
    strategy = MarketImpulseStrategy(
        entry_mode="delayed_reclaim",
        reclaim_window_bars=1,
        min_bars_after_pierce=1,
        max_vma_excursion_pct=0.01,
    )
    result = strategy.generate_signals(
        _frame([
            {"high": 100.0, "low": 99.8, "close": 99.9},
            {"high": 99.95, "low": 99.7, "close": 99.8},
            {"high": 100.3, "low": 99.9, "close": 100.2},
        ])
    )

    assert result["signal"].to_list() == [False, False, False]


def test_close_location_reclaim_handles_long_and_short_symmetrically() -> None:
    strategy = MarketImpulseStrategy(
        entry_mode="close_location_reclaim",
        min_close_location=0.8,
    )
    result = strategy.generate_signals(
        _frame([
            {"high": 100.2, "low": 99.8, "close": 100.15, "regime": "bullish"},
            {"high": 100.2, "low": 99.8, "close": 99.85, "regime": "bearish"},
            {"high": 100.2, "low": 99.8, "close": 100.05, "regime": "bullish"},
            {"high": 100.2, "low": 99.8, "close": 99.95, "regime": "bearish"},
        ])
    )

    assert result["signal"].to_list() == [True, True, False, False]
    assert result["signal_direction"].to_list() == ["long", "short", None, None]


def test_push_through_signals_on_confirmation_bar() -> None:
    strategy = MarketImpulseStrategy(
        entry_mode="continuation_confirmation",
        confirmation_window_bars=2,
        confirmation_type="break_reclaim_high_low",
        max_vma_excursion_pct=0.01,
    )
    result = strategy.generate_signals(
        _frame([
            {"high": 100.2, "low": 99.9, "close": 100.1, "regime": "bullish"},
            {"high": 100.25, "low": 100.0, "close": 100.15, "regime": "bullish"},
        ])
    )

    assert result["signal"].to_list() == [False, True]
    assert result["signal_direction"].to_list() == [None, "long"]

