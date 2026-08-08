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


def _two_day_frame(day2_close: float):
    """One day-1 entry that never triggers an exit (flat ~100), then a single
    day-2 bar at ``day2_close``. The scorer must EOD-flatten on day 1 and never
    read the day-2 bar — so the result must not depend on ``day2_close``.
    """
    rows = []
    base1 = dt.datetime(2025, 6, 2, 14, 0)  # 10:00 ET (June, ET = UTC-4)
    rows.append((base1, 100.0, 100.0, 100.0, True, "long"))
    for m in range(1, 6):
        rows.append((base1 + dt.timedelta(minutes=m), 100.0, 100.05, 99.95, False, None))
    base2 = dt.datetime(2025, 6, 3, 14, 0)
    rows.append((base2, day2_close, day2_close, day2_close, False, None))
    return pl.DataFrame({
        "timestamp": [r[0] for r in rows],
        "close": [r[1] for r in rows],
        "high": [r[2] for r in rows],
        "low": [r[3] for r in rows],
        "signal": [r[4] for r in rows],
        "signal_direction": [r[5] for r in rows],
    })


def test_eod_flatten_does_not_read_next_day_bar():
    # Regression: the no-exit fallback used to price the exit on the FIRST bar of
    # the next day (min(j, n-1)), a cross-day lookahead. A wild day-2 move must
    # not change the day-1 single-session expectancy.
    drop = score_profile_on_options(_two_day_frame(50.0), "long", "RANGE_EXPANSION", vol_beta=0.0)
    pop = score_profile_on_options(_two_day_frame(200.0), "long", "RANGE_EXPANSION", vol_beta=0.0)
    assert drop["n"] == 1 and pop["n"] == 1
    # Identical results regardless of the next day's price.
    assert drop["expectancy_pct"] == pop["expectancy_pct"]
    # Underlying never moved on day 1 -> only mild theta decay, nowhere near the
    # -100% / huge-gain the day-2 bar would have produced under the old bug.
    assert -1.0 < drop["expectancy_pct"] < 0.0
