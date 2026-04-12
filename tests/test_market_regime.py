"""Unit tests for src.research.market_regime.

Pure-function tests — no Polygon calls, no local file I/O.
"""

from __future__ import annotations

from datetime import date

import pytest

from src.research.market_regime import (
    SPY_TREND_FLAT_THRESHOLD_PCT_PER_DAY,
    VIX_HIGH_FLOOR,
    VIX_LOW_CEILING,
    _is_third_friday,
    classify_session_type,
    classify_spy_trend,
    classify_vix_band,
)


# ── VIX band ─────────────────────────────────────────────────────────────────


class TestClassifyVixBand:
    def test_below_low_ceiling_is_low(self) -> None:
        assert classify_vix_band(14.99) == "low"
        assert classify_vix_band(10.0) == "low"

    def test_low_ceiling_boundary_is_mid(self) -> None:
        assert classify_vix_band(VIX_LOW_CEILING) == "mid"

    def test_mid_band(self) -> None:
        assert classify_vix_band(18.0) == "mid"
        assert classify_vix_band(22.0) == "mid"

    def test_high_floor_boundary_is_mid(self) -> None:
        assert classify_vix_band(VIX_HIGH_FLOOR) == "mid"

    def test_above_high_floor_is_high(self) -> None:
        assert classify_vix_band(22.01) == "high"
        assert classify_vix_band(35.0) == "high"

    def test_none_defaults_to_mid(self) -> None:
        assert classify_vix_band(None) == "mid"


# ── SPY trend ─────────────────────────────────────────────────────────────────


class TestClassifySpyTrend:
    def test_flat_when_no_change(self) -> None:
        label, slope = classify_spy_trend(500.0, 500.0)
        assert label == "flat"
        assert slope == 0.0

    def test_flat_when_under_threshold(self) -> None:
        sma_today = 500.0 * (1 + 0.002)
        label, slope = classify_spy_trend(sma_today, 500.0)
        assert label == "flat"
        assert slope == pytest.approx(0.04, abs=1e-6)

    def test_up_when_above_threshold(self) -> None:
        sma_today = 500.0 * 1.01
        label, slope = classify_spy_trend(sma_today, 500.0)
        assert label == "up"
        assert slope == pytest.approx(0.2, abs=1e-6)

    def test_down_when_below_negative_threshold(self) -> None:
        sma_today = 500.0 * 0.99
        label, slope = classify_spy_trend(sma_today, 500.0)
        assert label == "down"
        assert slope == pytest.approx(-0.2, abs=1e-6)

    def test_boundary_exact_flat_threshold_stays_flat(self) -> None:
        total_pct = SPY_TREND_FLAT_THRESHOLD_PCT_PER_DAY * 5.0
        sma_today = 500.0 * (1 + total_pct / 100.0)
        label, _ = classify_spy_trend(sma_today, 500.0)
        assert label == "flat"

    def test_zero_lag_returns_flat(self) -> None:
        label, slope = classify_spy_trend(500.0, 0.0)
        assert label == "flat"
        assert slope == 0.0


# ── Third-Friday / opex detection ────────────────────────────────────────────


class TestIsThirdFriday:
    @pytest.mark.parametrize(
        "d",
        [
            date(2024, 1, 19),
            date(2024, 2, 16),
            date(2024, 3, 15),
            date(2024, 6, 21),
            date(2024, 12, 20),
            date(2025, 1, 17),
            date(2026, 4, 17),
        ],
    )
    def test_known_third_fridays(self, d: date) -> None:
        assert _is_third_friday(d) is True

    @pytest.mark.parametrize(
        "d",
        [
            date(2024, 1, 12),
            date(2024, 1, 26),
            date(2024, 3, 18),
            date(2024, 3, 22),
            date(2026, 4, 10),
            date(2026, 4, 24),
        ],
    )
    def test_non_third_fridays(self, d: date) -> None:
        assert _is_third_friday(d) is False


class TestClassifySessionType:
    def test_third_friday_is_opex(self) -> None:
        assert classify_session_type(date(2024, 3, 15)) == "opex"
        assert classify_session_type(date(2026, 4, 17)) == "opex"

    def test_regular_weekday_is_normal(self) -> None:
        assert classify_session_type(date(2026, 4, 13)) == "normal"
        assert classify_session_type(date(2026, 4, 14)) == "normal"

    def test_non_opex_friday_is_normal(self) -> None:
        assert classify_session_type(date(2026, 4, 10)) == "normal"


# ── MarketRegime.regime_key ───────────────────────────────────────────────────


def test_regime_key_format() -> None:
    from src.research.market_regime import MarketRegime
    r = MarketRegime(
        trading_date=date(2026, 4, 14),
        vix_band="high",
        spy_trend_20d="down",
        session_type="normal",
        vix_close=28.5,
        spy_close=510.0,
        spy_sma20=518.0,
        spy_trend_slope_pct=-0.12,
    )
    assert r.regime_key == "vix=high__spy=down__session=normal"
