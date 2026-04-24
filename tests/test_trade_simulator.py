"""Tests for pluggable trade-simulator exit policies."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import polars as pl
import pytest

from src.oracle.trade_simulator import (
    AtrTrailingExitPolicy,
    FixedRewardRiskExitPolicy,
    HoldToEodExitPolicy,
    MovingAverageCrossoverExitPolicy,
    MovingAverageTrailingExitPolicy,
    TimeStopExitPolicy,
    TradeSimulator,
    VmaTrailingExitPolicy,
)


def _base_trade_frame() -> pl.DataFrame:
    timestamps = [datetime(2025, 1, 2, 9, 30) + timedelta(minutes=i) for i in range(5)]
    return pl.DataFrame(
        {
            "timestamp": timestamps,
            "close": [100.0, 101.0, 98.0, 98.0, 98.0],
            "high": [100.0, 101.0, 99.0, 98.5, 98.5],
            "low": [100.0, 100.0, 97.0, 97.5, 97.5],
            "signal": [True, False, False, False, False],
            "signal_direction": ["long", None, None, None, None],
        }
    )


def test_trade_simulator_defaults_to_vma_trailing_exit() -> None:
    df = _base_trade_frame().with_columns(pl.Series("vma_10_5m", [99.0, 100.0, 100.0, 100.0, 100.0]))

    result = TradeSimulator().simulate(df)

    assert result.total_trades == 1
    trade = result.trades[0]
    assert trade.exit_reason == "vma_stop"
    assert trade.exit_price == 98.0
    assert trade.pnl == -2.0


def test_trade_simulator_can_use_fixed_rr_without_vma_columns() -> None:
    timestamps = [datetime(2025, 1, 2, 9, 30) + timedelta(minutes=i) for i in range(4)]
    df = pl.DataFrame(
        {
            "timestamp": timestamps,
            "close": [100.0, 101.5, 101.0, 100.5],
            "high": [100.0, 102.5, 101.2, 100.8],
            "low": [100.0, 99.8, 100.7, 100.2],
            "signal": [True, False, False, False],
            "signal_direction": ["long", None, None, None],
        }
    )

    result = TradeSimulator(
        exit_policy=FixedRewardRiskExitPolicy(stop_loss=1.0, reward_multiple=2.0)
    ).simulate(df)

    assert result.total_trades == 1
    trade = result.trades[0]
    assert trade.exit_reason == "take_profit"
    assert trade.exit_price == 102.0
    assert trade.pnl == 2.0
    assert trade.vma_5m_at_entry == 0.0


def test_trade_simulator_honors_entry_delay_and_min_hold() -> None:
    timestamps = [datetime(2025, 1, 2, 9, 30) + timedelta(minutes=i) for i in range(5)]
    df = pl.DataFrame(
        {
            "timestamp": timestamps,
            "close": [100.0, 101.0, 101.0, 99.0, 99.0],
            "high": [100.0, 103.0, 103.0, 101.0, 99.5],
            "low": [100.0, 100.5, 100.5, 99.5, 98.5],
            "signal": [True, False, False, False, False],
            "signal_direction": ["long", None, None, None, None],
        }
    )

    result = TradeSimulator(
        entry_delay_bars=1,
        min_hold_bars=2,
        exit_policy=FixedRewardRiskExitPolicy(stop_loss=1.0, reward_multiple=1.0),
    ).simulate(df)

    assert result.total_trades == 1
    trade = result.trades[0]
    assert trade.entry_time == timestamps[1]
    assert trade.exit_reason == "stop_loss"
    assert trade.exit_price == 100.0
    assert trade.pnl == -1.0


def test_fixed_rr_policy_validates_configuration() -> None:
    with pytest.raises(ValueError, match="stop_loss must be positive"):
        FixedRewardRiskExitPolicy(stop_loss=0.0)

    with pytest.raises(ValueError, match="reward_multiple must be positive"):
        FixedRewardRiskExitPolicy(stop_loss=1.0, reward_multiple=0.0)


def test_atr_trailing_policy_trails_after_favorable_move() -> None:
    timestamps = [datetime(2025, 1, 2, 9, 30) + timedelta(minutes=i) for i in range(5)]
    df = pl.DataFrame(
        {
            "timestamp": timestamps,
            "close": [100.0, 101.0, 103.0, 101.0, 101.0],
            "high": [100.0, 101.0, 103.0, 101.2, 101.0],
            "low": [100.0, 100.8, 102.8, 100.8, 100.8],
            "atr_14_exit": [1.0] * 5,
            "signal": [True, False, False, False, False],
            "signal_direction": ["long", None, None, None, None],
        }
    )

    result = TradeSimulator(
        exit_policy=AtrTrailingExitPolicy(atr_col="atr_14_exit", atr_multiple=1.0)
    ).simulate(df)

    assert result.total_trades == 1
    trade = result.trades[0]
    assert trade.exit_reason == "atr_trailing_stop"
    assert trade.exit_price == 102.0
    assert trade.pnl == 2.0


def test_atr_trailing_policy_resets_between_simulations() -> None:
    timestamps = [datetime(2025, 1, 2, 9, 30) + timedelta(minutes=i) for i in range(5)]
    df = pl.DataFrame(
        {
            "timestamp": timestamps,
            "close": [100.0, 101.0, 103.0, 101.0, 101.0],
            "high": [100.0, 101.0, 103.0, 101.2, 101.0],
            "low": [100.0, 100.8, 102.8, 100.8, 100.8],
            "atr_14_exit": [1.0] * 5,
            "signal": [True, False, False, False, False],
            "signal_direction": ["long", None, None, None, None],
        }
    )
    simulator = TradeSimulator(
        exit_policy=AtrTrailingExitPolicy(atr_col="atr_14_exit", atr_multiple=1.0)
    )

    first = simulator.simulate(df)
    second = simulator.simulate(df)

    assert first.trades[0].exit_price == second.trades[0].exit_price == 102.0


def test_moving_average_trailing_policy_exits_on_close_cross() -> None:
    df = _base_trade_frame().with_columns(
        pl.Series("ema_20_exit", [99.0, 100.5, 100.5, 100.5, 100.5])
    )

    result = TradeSimulator(
        exit_policy=MovingAverageTrailingExitPolicy(ma_col="ema_20_exit")
    ).simulate(df)

    assert result.total_trades == 1
    assert result.trades[0].exit_reason == "ma_trailing_stop"


def test_moving_average_crossover_policy_exits_on_stack_flip() -> None:
    df = _base_trade_frame().with_columns([
        pl.Series("ema_8_exit", [101.0, 101.0, 99.0, 99.0, 99.0]),
        pl.Series("ema_20_exit", [100.0, 100.0, 100.0, 100.0, 100.0]),
    ])

    result = TradeSimulator(
        exit_policy=MovingAverageCrossoverExitPolicy(
            fast_ma_col="ema_8_exit", slow_ma_col="ema_20_exit"
        )
    ).simulate(df)

    assert result.total_trades == 1
    assert result.trades[0].exit_reason == "ma_crossover_exit"


def test_time_stop_and_hold_to_eod_policies() -> None:
    timestamps = [datetime(2025, 1, 2, 14, 30, tzinfo=timezone.utc) + timedelta(minutes=i) for i in range(4)]
    df = pl.DataFrame(
        {
            "timestamp": timestamps,
            "close": [100.0, 101.0, 102.0, 103.0],
            "high": [100.0, 101.0, 102.0, 103.0],
            "low": [100.0, 101.0, 102.0, 103.0],
            "signal": [True, False, False, False],
            "signal_direction": ["long", None, None, None],
        }
    )

    time_stop = TradeSimulator(
        exit_policy=TimeStopExitPolicy(exit_time=datetime(2025, 1, 2, 9, 32).time())
    ).simulate(df)
    hold_to_eod = TradeSimulator(exit_policy=HoldToEodExitPolicy()).simulate(df)

    assert time_stop.trades[0].exit_reason == "time_stop_0932"
    assert time_stop.trades[0].exit_price == 102.0
    assert hold_to_eod.trades[0].exit_reason == "eod"
    assert hold_to_eod.trades[0].exit_price == 103.0


def test_vma_trailing_policy_requires_vma_column() -> None:
    with pytest.raises(ValueError, match="requires columns"):
        TradeSimulator(exit_policy=VmaTrailingExitPolicy(vma_col="custom_vma")).simulate(
            _base_trade_frame()
        )
