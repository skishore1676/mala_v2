"""Tests for the S4 option-translation scorer."""

import datetime as dt

import polars as pl

from src.research.option_translation import (
    annualized_realized_vol,
    score_profile_band,
    score_profile_on_options,
    _iv,
)


def _frame(closes):
    n = len(closes)
    ts = [dt.datetime(2025, 1, 2, 9, 30) + dt.timedelta(minutes=i) for i in range(n)]
    return pl.DataFrame({
        "timestamp": ts,
        "close": closes,
        "high": [c + 0.05 for c in closes],
        "low": [c - 0.05 for c in closes],
        "signal": [i == 0 for i in range(n)],
        "signal_direction": ["short"] + [None] * (n - 1),
    })


def test_put_profits_when_underlying_drops():
    closes = [100.0 - min(i, 15) * 0.3 for i in range(30)]  # ~4.5% drop then flat
    res = score_profile_on_options(_frame(closes), "short", "FLASH_REVERSAL", vol_beta=0.0)
    assert res["n"] == 1
    assert res["expectancy_pct"] > 0


def test_put_loses_when_underlying_rises():
    closes = [100.0 + min(i, 15) * 0.3 for i in range(30)]
    res = score_profile_on_options(_frame(closes), "short", "FLASH_REVERSAL", vol_beta=0.0)
    assert res["n"] == 1
    assert res["expectancy_pct"] < 0


def test_iv_direction_aware():
    assert _iv(0.4, 0.0, 2.5) == 0.4              # no move -> entry IV
    assert _iv(0.4, -0.02, 2.5) > 0.4             # spot down -> put IV expands
    assert _iv(0.4, 0.02, 2.5) < 0.4              # spot up -> IV bleeds
    assert _iv(0.4, -0.05, 0.0) == 0.4            # vol_beta 0 -> flat
    assert _iv(0.4, -10.0, 2.5) == 0.4 * 3.0      # clamped at 3x


def test_leverage_helps_puts_in_selloff():
    # A put bought into a selloff should do at least as well with IV expansion on.
    closes = [100.0 - min(i, 15) * 0.3 for i in range(30)]
    flat = score_profile_on_options(_frame(closes), "short", "FLASH_REVERSAL", vol_beta=0.0)
    lev = score_profile_on_options(_frame(closes), "short", "FLASH_REVERSAL", vol_beta=2.5)
    assert lev["expectancy_pct"] >= flat["expectancy_pct"]


def test_zero_trade_frame_returns_n0_without_error():
    # _frame signals "short"; scoring "long" matches nothing -> empty pnls path.
    res = score_profile_on_options(_frame([100.0] * 20), "long", "FLASH_REVERSAL", vol_beta=0.0)
    assert res["n"] == 0
    assert res["expectancy_pct"] == 0.0


def test_realized_vol_fallback():
    assert annualized_realized_vol(_frame([100.0] * 30)) == 0.30


def test_band_returns_scenarios():
    closes = [100.0 - min(i, 15) * 0.3 for i in range(30)]
    band = score_profile_band(_frame(closes), "short", "FLASH_REVERSAL")
    assert [b["scenario"] for b in band] == ["flat", "leverage", "cheap_iv", "rich_iv"]


def _flat_two_session_frame():
    """Two trading days of 1-min bars, an entry signal on the FIRST bar of each.

    Day 1 is FLAT (price never moves), so the day-1 trade hits neither target nor
    stop and is short enough (8 bars < FLASH_REVERSAL's 15-bar no_progress) that
    no time stop fires either -- it holds until the inner loop falls through the
    *date boundary*. That fall-through (rather than a policy-driven exit) is the
    exact path where the old ``i = j + 1`` cursor advance skipped index ``j`` --
    the first bar of day 2, which carries the day-2 entry signal.
    """
    rows = []
    base = dt.datetime(2026, 1, 5, 9, 30)
    for day in range(2):
        for minute in range(8):
            ts = base + dt.timedelta(days=day, minutes=minute)
            rows.append({
                "timestamp": ts,
                "close": 100.0,
                "high": 100.02,
                "low": 99.98,
                "signal": minute == 0,            # signal on each day's first bar
                "signal_direction": "short" if minute == 0 else None,
            })
    return pl.DataFrame(rows)


def test_does_not_skip_first_signal_of_next_session():
    # Regression: when the inner loop fell through the session/date boundary (no
    # policy exit), the outer scan advanced ``i = j + 1`` and skipped bar ``j`` --
    # the first bar of the next day -- dropping its entry signal. Both first-bar
    # signals must be counted -> n == 2 (old code returned 1).
    res = score_profile_on_options(_flat_two_session_frame(), "short", "FLASH_REVERSAL", vol_beta=0.0)
    assert res["n"] == 2
