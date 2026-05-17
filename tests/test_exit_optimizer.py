from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import polars as pl

from src.oracle.trade_simulator import SimulationResult, Trade
from src.research.exit_optimizer import (
    ExitPolicyEvaluation,
    _dte_bucket_for_minutes,
    _option_adjusted_metrics,
    _sort_key,
    optimize_underlying_exit,
)
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
    assert any(
        candidate.policy_name.startswith("fixed_rr_underlying_scalp:")
        for candidate in result.candidate_policies
    )
    assert "pnl_per_bar" in result.selected_metrics
    assert "avg_bars_held" in result.selected_metrics
    assert result.selection_metric == "option_adjusted_expectancy_pct"
    assert "option_adjusted_expectancy_pct" in result.selected_metrics
    assert "median_minutes_held" in result.selected_metrics


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


def test_option_theta_penalty_uses_wall_clock_minutes_not_bar_count() -> None:
    entry = datetime(2026, 3, 16, 14, 30, tzinfo=timezone.utc)
    exit_ = entry + timedelta(minutes=30)
    one_minute_like = SimulationResult(
        trades=[
            Trade(entry, exit_, "long", 100.0, 101.0, "take_profit_underlying", pnl=1.0, bars_held=30)
        ]
    )
    five_minute_like = SimulationResult(
        trades=[
            Trade(entry, exit_, "long", 100.0, 101.0, "take_profit_underlying", pnl=1.0, bars_held=6)
        ]
    )
    fifteen_minute_like = SimulationResult(
        trades=[
            Trade(entry, exit_, "long", 100.0, 101.0, "take_profit_underlying", pnl=1.0, bars_held=2)
        ]
    )

    theta_values = {
        _option_adjusted_metrics(result)["theta_penalty_pct"]
        for result in (one_minute_like, five_minute_like, fifteen_minute_like)
    }

    assert len(theta_values) == 1
    assert theta_values == {0.092308}


def test_fast_lower_win_rate_exit_can_beat_slow_higher_underlying_expectancy() -> None:
    fast = ExitPolicyEvaluation(
        policy_name="fast",
        thesis_exit_policy="fixed_rr_underlying",
        metrics={
            "option_adjusted_expectancy_pct": 0.20,
            "pnl_pct_per_minute": 0.04,
            "target_hit_within_30_minutes": 0.55,
            "median_minutes_held": 15,
            "profit_factor": 1.2,
            "win_rate": 0.55,
            "expectancy": 0.1,
        },
    )
    slow = ExitPolicyEvaluation(
        policy_name="slow",
        thesis_exit_policy="hold_to_eod_underlying",
        metrics={
            "option_adjusted_expectancy_pct": 0.10,
            "pnl_pct_per_minute": 0.005,
            "target_hit_within_30_minutes": 0.10,
            "median_minutes_held": 260,
            "profit_factor": 2.0,
            "win_rate": 0.65,
            "expectancy": 0.4,
        },
    )

    assert _sort_key(fast) > _sort_key(slow)


def test_late_day_zero_to_three_dte_hold_receives_larger_theta_penalty() -> None:
    morning_entry = datetime(2026, 3, 16, 13, 30, tzinfo=timezone.utc)  # 09:30 ET
    late_entry = datetime(2026, 3, 16, 19, 15, tzinfo=timezone.utc)  # 15:15 ET
    morning = SimulationResult(
        trades=[Trade(morning_entry, morning_entry + timedelta(minutes=30), "long", 100, 101, "take_profit", pnl=1, bars_held=30)]
    )
    late = SimulationResult(
        trades=[Trade(late_entry, late_entry + timedelta(minutes=30), "long", 100, 101, "take_profit", pnl=1, bars_held=30)]
    )

    assert _option_adjusted_metrics(late)["theta_penalty_pct"] > _option_adjusted_metrics(morning)["theta_penalty_pct"]


def test_dte_bucket_mapping_boundaries() -> None:
    assert _dte_bucket_for_minutes(30) == (0, 3, 1.2)
    assert _dte_bucket_for_minutes(120) == (3, 7, 0.6)
    assert _dte_bucket_for_minutes(390) == (7, 14, 0.3)
    assert _dte_bucket_for_minutes(391) == (14, 21, 0.15)


def test_negative_option_adjusted_expectancy_sets_not_trade_ready() -> None:
    entry = datetime(2026, 3, 16, 14, 30, tzinfo=timezone.utc)
    result = SimulationResult(
        trades=[Trade(entry, entry + timedelta(minutes=30), "long", 100, 99.9, "stop_loss", pnl=-0.1, bars_held=30)]
    )

    metrics = _option_adjusted_metrics(result)

    assert metrics["option_adjusted_expectancy_pct"] < 0
    assert metrics["option_trade_ready"] is False
