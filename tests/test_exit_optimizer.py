from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import polars as pl

from src.research.exit_optimizer import optimize_underlying_exit
from src.strategy.market_impulse import MarketImpulseStrategy


def test_optimize_underlying_exit_canonicalizes_display_strategy_key() -> None:
    strategy = MarketImpulseStrategy(
        entry_buffer_minutes=3,
        entry_window_minutes=60,
        regime_timeframe="1h",
    )
    timestamps = [
        datetime(2026, 3, day, 13, 35, tzinfo=timezone.utc) + timedelta(minutes=i)
        for day in (16, 17)
        for i in range(35)
    ]
    close = [100.0 + i * 0.05 for i in range(len(timestamps))]
    frame = pl.DataFrame(
        {
            "timestamp": timestamps,
            "ticker": ["SPY"] * len(timestamps),
            "open": [c - 0.02 for c in close],
            "high": [c + 0.12 for c in close],
            "low": [c - 0.12 for c in close],
            "close": close,
            "volume": [1000.0] * len(timestamps),
            "vma_10": [c - 0.05 for c in close],
            "impulse_regime_1h": ["bullish"] * len(timestamps),
        }
    )

    result = optimize_underlying_exit(
        strategy_key="Market Impulse (Cross & Reclaim)",
        symbol="SPY",
        direction="long",
        strategy=strategy,
        enriched_frame=frame,
        holdout_start=date(2026, 3, 16),
        holdout_end=date(2026, 3, 17),
        catastrophe_exit_params={"stop_loss_pct": 0.45, "hard_flat_time_et": "15:55"},
    )

    assert result is not None
    assert result.strategy_key == "market_impulse"
    assert result.thesis_exit_anchor == "underlying"
    assert result.catastrophe_exit_anchor == "option_premium"
    assert result.selected_metrics["trade_count"] >= 1
    assert any(
        candidate.thesis_exit_policy == "trailing_vma_underlying"
        for candidate in result.candidate_policies
    )
    assert any(
        candidate.thesis_exit_policy == "atr_trailing_underlying"
        for candidate in result.candidate_policies
    )
    assert any(
        candidate.thesis_exit_policy == "ma_trailing_underlying"
        for candidate in result.candidate_policies
    )
    assert any(
        candidate.thesis_exit_policy == "hold_to_eod_underlying"
        for candidate in result.candidate_policies
    )


def test_optimize_underlying_exit_ignores_combined_direction() -> None:
    result = optimize_underlying_exit(
        strategy_key="Market Impulse (Cross & Reclaim)",
        symbol="SPY",
        direction="combined",
        strategy=MarketImpulseStrategy(),
        enriched_frame=pl.DataFrame({"timestamp": []}),
        holdout_start=date(2026, 3, 16),
        holdout_end=date(2026, 3, 17),
    )

    assert result is None
