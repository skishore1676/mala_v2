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
    res = score_profile_on_options(_frame(closes), "short", "FLASH_REVERSAL", iv_model="flat")
    assert res["n"] == 1
    assert res["expectancy_pct"] > 0


def test_put_loses_when_underlying_rises():
    closes = [100.0 + min(i, 15) * 0.3 for i in range(30)]
    res = score_profile_on_options(_frame(closes), "short", "FLASH_REVERSAL", iv_model="flat")
    assert res["n"] == 1
    assert res["expectancy_pct"] < 0


def test_iv_models():
    assert _iv("flat", 0.4, 0.3, 0.5, 0.5) == 0.4
    assert _iv("crush", 0.4, 0.3, 0.5, 1.0) < 0.4
    assert _iv("mean_revert", 0.4, 0.2, 1.0, 0.0) == 0.2  # full revert at elapsed=1


def test_realized_vol_fallback():
    assert annualized_realized_vol(_frame([100.0] * 30)) == 0.30


def test_band_returns_three_models():
    closes = [100.0 - min(i, 15) * 0.3 for i in range(30)]
    band = score_profile_band(_frame(closes), "short", "FLASH_REVERSAL")
    assert [b["iv_model"] for b in band] == ["flat", "mean_revert", "crush"]
