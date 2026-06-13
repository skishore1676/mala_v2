"""Tests for the Wave 1 exit profile families (exit_profiles + new policies)."""

import datetime as dt

import polars as pl

from src.oracle.trade_simulator import (
    BarSnapshot,
    HighWaterGivebackExitPolicy,
    NoProgressTimeStopExitPolicy,
    OpenTrade,
    ProfileExitPolicy,
    TradeSimulator,
)
from src.research.exit_optimizer import _policy_candidates
from src.research.exit_profiles import (
    EXIT_PROFILES,
    PROFILE_BY_STRATEGY,
    assigned_profile,
    build_profile_exit_policy,
)


def _bar(idx, high, low, close, bar_time=dt.time(10, 0)):
    return BarSnapshot(
        idx=idx, timestamp=None, close=close, high=high, low=low,
        bar_time=bar_time, trade_date=dt.date(2026, 1, 2), values={},
    )


def _trade(direction="long", entry_price=100.0, entry_idx=0):
    return OpenTrade(
        entry_idx=entry_idx, entry_time=None, direction=direction,
        entry_price=entry_price, entry_date=dt.date(2026, 1, 2), entry_values={},
    )


# ── ProfileExitPolicy ─────────────────────────────────────────────────────────

def test_profile_partial_then_target2_is_blended():
    p = ProfileExitPolicy(profile_name="T", stop_loss_pct=0.01, target_1_r=1.0,
                          target_2_r=2.0, target_1_quantity=0.5)
    tr = _trade()
    # risk=1.0 -> T1=101, T2=102. Bar 1 banks the 50% partial at T1.
    assert p.should_exit(tr, _bar(1, high=101.0, low=100.0, close=100.5)) is None
    d = p.should_exit(tr, _bar(2, high=102.0, low=101.0, close=101.8))
    assert d is not None and d.reason == "take_profit_underlying"
    assert abs(d.exit_price - 101.5) < 1e-9  # 0.5*101 + 0.5*102


def test_profile_initial_stop():
    p = ProfileExitPolicy(profile_name="T", stop_loss_pct=0.01, target_1_quantity=0.5)
    d = p.should_exit(_trade(), _bar(1, high=100.2, low=98.5, close=99.0))
    assert d.reason == "stop_loss_underlying" and abs(d.exit_price - 99.0) < 1e-9


def test_profile_breakeven_stop_after_partial():
    p = ProfileExitPolicy(profile_name="T", stop_loss_pct=0.01, target_1_r=1.0,
                          target_2_r=2.0, target_1_quantity=0.5)
    tr = _trade()
    assert p.should_exit(tr, _bar(1, high=101.0, low=100.0, close=100.5)) is None
    d = p.should_exit(tr, _bar(2, high=100.5, low=99.5, close=99.8))
    assert d.reason == "breakeven_stop" and abs(d.exit_price - 100.5) < 1e-9


def test_profile_giveback():
    p = ProfileExitPolicy(profile_name="T", stop_loss_pct=0.01, target_1_r=5.0,
                          target_2_r=9.0, target_1_quantity=0.0,
                          giveback_arm_r=0.5, giveback_retrace_frac=0.5)
    tr = _trade()
    assert p.should_exit(tr, _bar(1, high=100.8, low=100.0, close=100.4)) is None
    d = p.should_exit(tr, _bar(2, high=100.5, low=100.2, close=100.3))
    assert d.reason == "high_water_giveback" and abs(d.exit_price - 100.3) < 1e-9


def test_profile_no_progress():
    p = ProfileExitPolicy(profile_name="T", stop_loss_pct=0.01, target_1_r=5.0,
                          target_2_r=9.0, target_1_quantity=0.0,
                          no_progress_bars=3, no_progress_floor_r=0.25)
    tr = _trade()
    assert p.should_exit(tr, _bar(1, 100.1, 100.0, 100.05)) is None
    assert p.should_exit(tr, _bar(2, 100.1, 100.0, 100.05)) is None
    d = p.should_exit(tr, _bar(3, 100.1, 100.0, 100.05))
    assert d.reason == "no_progress"


def test_profile_short_initial_stop():
    p = ProfileExitPolicy(profile_name="T", stop_loss_pct=0.01, target_1_quantity=0.5)
    d = p.should_exit(_trade(direction="short"), _bar(1, high=101.5, low=100.0, close=101.0))
    assert d.reason == "stop_loss_underlying" and abs(d.exit_price - 101.0) < 1e-9


# ── Standalone families ─────────────────────────────────────────────────────────

def test_high_water_giveback_standalone():
    p = HighWaterGivebackExitPolicy(arm_pct=0.005, retrace_frac=0.5)
    tr = _trade()
    assert p.should_exit(tr, _bar(1, high=100.6, low=100.0, close=100.4)) is None
    d = p.should_exit(tr, _bar(2, high=100.5, low=100.2, close=100.2))
    assert d.reason == "high_water_giveback" and abs(d.exit_price - 100.2) < 1e-9


def test_no_progress_standalone():
    p = NoProgressTimeStopExitPolicy(no_progress_bars=3, min_favorable_pct=0.005)
    tr = _trade()
    assert p.should_exit(tr, _bar(1, 100.1, 100.0, 100.05)) is None
    assert p.should_exit(tr, _bar(2, 100.1, 100.0, 100.05)) is None
    assert p.should_exit(tr, _bar(3, 100.1, 100.0, 100.05)).reason == "no_progress"


# ── Profile registry + optimizer wiring ─────────────────────────────────────────

def test_exit_profiles_registry():
    assert set(EXIT_PROFILES) == {
        "FLASH_REVERSAL", "EXHAUSTION_REVERSAL", "TREND_CONTINUATION", "RANGE_EXPANSION",
    }
    pol = build_profile_exit_policy("flash_reversal")
    assert pol.profile_name == "FLASH_REVERSAL"
    assert pol.policy_name == "profile:flash_reversal"
    assert assigned_profile("intraday_mean_reversion_extremes") == "FLASH_REVERSAL"
    assert PROFILE_BY_STRATEGY["compression_expansion_breakout"] == "RANGE_EXPANSION"


def test_optimizer_includes_profile_candidates():
    candidates = _policy_candidates(strategy_key="elastic_band_reversion", strategy=object())
    names = {c.name for c in candidates}
    for profile in (
        "profile:flash_reversal", "profile:exhaustion_reversal",
        "profile:trend_continuation", "profile:range_expansion",
    ):
        assert profile in names


def test_profile_runs_through_simulator_with_blended_partial():
    timestamps = [dt.datetime(2025, 1, 2, 9, 30) + dt.timedelta(minutes=i) for i in range(4)]
    df = pl.DataFrame({
        "timestamp": timestamps,
        "close": [100.0, 100.5, 101.8, 101.0],
        "high":  [100.0, 101.0, 102.0, 101.5],
        "low":   [100.0, 100.0, 101.0, 100.5],
        "signal": [True, False, False, False],
        "signal_direction": ["long", None, None, None],
    })
    sim = TradeSimulator(exit_policy=ProfileExitPolicy(
        profile_name="T", stop_loss_pct=0.01, target_1_r=1.0,
        target_2_r=2.0, target_1_quantity=0.5,
    ))
    result = sim.simulate(df)
    assert result.total_trades == 1
    trade = result.trades[0]
    # Bar 1 banks 50% at T1=101; bar 2 exits the runner at T2=102 -> blended 101.5.
    assert trade.exit_reason == "take_profit_underlying"
    assert abs(trade.exit_price - 101.5) < 1e-9
    assert abs(trade.pnl - 1.5) < 1e-9
