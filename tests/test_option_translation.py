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


def _multiday_frame(days):
    """Frame spanning multiple trading days. ``days`` is a list of (n_bars,
    signal_index) -> a session of n_bars flat bars with a long signal at the
    given bar index (or None for no signal)."""
    rows = []
    base = dt.datetime(2025, 1, 2, 9, 30)
    for d, (n_bars, sig_idx) in enumerate(days):
        day_start = base + dt.timedelta(days=d)
        for m in range(n_bars):
            t = day_start + dt.timedelta(minutes=m)
            rows.append(
                (t, 100.0, 100.05, 99.95, m == sig_idx, "long" if m == sig_idx else None)
            )
    return pl.DataFrame(
        rows,
        schema=["timestamp", "close", "high", "low", "signal", "signal_direction"],
        orient="row",
    )


def test_next_session_first_bar_signal_not_skipped():
    # Regression: a trade that holds to EOD ends the inner loop on the first bar
    # of the NEXT day (day rollover). The outer scan must resume AT that bar, not
    # past it -- otherwise a signal on the first bar of the next session is
    # silently dropped. Both days signal on their first bar -> expect 2 trades.
    frame = _multiday_frame([(5, 0), (5, 0)])
    res = score_profile_on_options(frame, "long", "FLASH_REVERSAL", vol_beta=0.0)
    assert res["n"] == 2


def test_realized_vol_fallback():
    assert annualized_realized_vol(_frame([100.0] * 30)) == 0.30


def test_band_returns_scenarios():
    closes = [100.0 - min(i, 15) * 0.3 for i in range(30)]
    band = score_profile_band(_frame(closes), "short", "FLASH_REVERSAL")
    assert [b["scenario"] for b in band] == ["flat", "leverage", "cheap_iv", "rich_iv"]
