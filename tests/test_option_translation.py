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


def _two_day_frame(day1, day2, *, day1_signal_bars, day2_signal_bars):
    """Two trading days of 1-min bars with explicit per-bar closes/signals."""
    rows = []
    base1 = dt.datetime(2025, 1, 2, 9, 30)
    for i, c in enumerate(day1):
        rows.append((base1 + dt.timedelta(minutes=i), c, i in day1_signal_bars))
    base2 = dt.datetime(2025, 1, 3, 9, 30)
    for i, c in enumerate(day2):
        rows.append((base2 + dt.timedelta(minutes=i), c, i in day2_signal_bars))
    ts = [r[0] for r in rows]
    closes = [r[1] for r in rows]
    sig = [r[2] for r in rows]
    return pl.DataFrame({
        "timestamp": ts,
        "close": closes,
        "high": [c + 0.05 for c in closes],
        "low": [c - 0.05 for c in closes],
        "signal": sig,
        "signal_direction": ["short" if s else None for s in sig],
    })


def test_hold_to_eod_prices_on_entry_day_not_next_day_gap():
    # A put bought on day1 (flat at 100 all session, no exit fires -> hold to EOD)
    # must be valued on day1's last bar, NOT day2's gapped-down open. The scorer is
    # single-session; pricing the EOD exit on the overnight gap fabricated a ~+998%
    # win. Regression: expectancy stays a tiny theta loss near zero.
    frame = _two_day_frame(
        [100.0] * 30, [80.0] * 30, day1_signal_bars={0}, day2_signal_bars=set()
    )
    res = score_profile_on_options(frame, "short", "RANGE_EXPANSION", vol_beta=0.0)
    assert res["n"] == 1
    assert -5.0 < res["expectancy_pct"] < 1.0  # pre-fix this was ~+998


def test_next_day_first_bar_signal_is_not_skipped():
    # Day1 signal holds to EOD; day2's FIRST bar also signals. The hold-to-EOD path
    # used to advance the entry cursor past the boundary bar (i = j + 1), dropping
    # the next-day signal. Both signals must now enter -> n == 2.
    frame = _two_day_frame(
        [100.0] * 30, [100.0] * 30, day1_signal_bars={0}, day2_signal_bars={0}
    )
    res = score_profile_on_options(frame, "short", "EXHAUSTION_REVERSAL", vol_beta=0.0)
    assert res["n"] == 2
