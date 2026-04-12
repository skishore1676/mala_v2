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


# ── VIX fallback chain ──────────────────────────────────────────────────────

import logging
import re
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

from src.research.market_regime import (
    _fetch_vix_closes,
    _load_vix_cache,
    _save_vix_cache,
    _vix_from_cboe_csv,
    _vix_from_fred_csv,
)


class TestVixFallbackChain:
    """Test the CBOE → FRED → cache fallback chain."""

    def test_cboe_success_skips_fred(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """When CBOE succeeds, FRED is never called."""
        monkeypatch.setattr(
            "src.research.market_regime.VIX_CACHE_PATH",
            tmp_path / "_vix_daily.parquet",
        )
        fake_cboe = {date(2025, 1, 2): 14.5, date(2025, 1, 3): 15.1}
        fred_called = False

        def mock_fred() -> dict:
            nonlocal fred_called
            fred_called = True
            return {date(2025, 1, 2): 99.0}

        monkeypatch.setattr("src.research.market_regime._vix_from_cboe_csv", lambda: fake_cboe)
        monkeypatch.setattr("src.research.market_regime._vix_from_fred_csv", mock_fred)

        result = _fetch_vix_closes(date(2025, 1, 1), date(2025, 1, 5))
        assert date(2025, 1, 2) in result
        assert result[date(2025, 1, 2)] == 14.5
        assert not fred_called

    def test_cboe_fails_falls_to_fred(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """When CBOE fails, FRED is tried."""
        monkeypatch.setattr(
            "src.research.market_regime.VIX_CACHE_PATH",
            tmp_path / "_vix_daily.parquet",
        )
        fake_fred = {date(2025, 1, 2): 16.0}

        monkeypatch.setattr("src.research.market_regime._vix_from_cboe_csv", lambda: {})
        monkeypatch.setattr("src.research.market_regime._vix_from_fred_csv", lambda: fake_fred)

        result = _fetch_vix_closes(date(2025, 1, 1), date(2025, 1, 5))
        assert result[date(2025, 1, 2)] == 16.0

    def test_all_sources_fail_returns_empty(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """When everything fails, returns {} (caller defaults to mid)."""
        monkeypatch.setattr(
            "src.research.market_regime.VIX_CACHE_PATH",
            tmp_path / "_vix_daily.parquet",
        )
        monkeypatch.setattr("src.research.market_regime._vix_from_cboe_csv", lambda: {})
        monkeypatch.setattr("src.research.market_regime._vix_from_fred_csv", lambda: {})

        result = _fetch_vix_closes(date(2025, 1, 1), date(2025, 1, 5))
        assert result == {}

    def test_cache_is_used_when_fresh(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Fresh cache data is returned without hitting any URL."""
        cache_path = tmp_path / "_vix_daily.parquet"
        monkeypatch.setattr("src.research.market_regime.VIX_CACHE_PATH", cache_path)

        # Seed the cache
        seed = {date(2025, 6, d): 18.0 + d for d in range(1, 20)}
        _save_vix_cache(seed)

        cboe_called = False
        def mock_cboe() -> dict:
            nonlocal cboe_called
            cboe_called = True
            return {}

        monkeypatch.setattr("src.research.market_regime._vix_from_cboe_csv", mock_cboe)

        result = _fetch_vix_closes(date(2025, 6, 1), date(2025, 6, 15))
        assert len(result) > 0
        assert not cboe_called, "Fresh cache should not trigger a network fetch"

    def test_stale_cache_used_as_last_resort(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Stale cache is returned when all live sources fail."""
        cache_path = tmp_path / "_vix_daily.parquet"
        monkeypatch.setattr("src.research.market_regime.VIX_CACHE_PATH", cache_path)

        # Seed stale cache (old data, query for much later dates)
        stale = {date(2024, 1, d): 20.0 for d in range(2, 20)}
        _save_vix_cache(stale)

        monkeypatch.setattr("src.research.market_regime._vix_from_cboe_csv", lambda: {})
        monkeypatch.setattr("src.research.market_regime._vix_from_fred_csv", lambda: {})

        result = _fetch_vix_closes(date(2024, 1, 2), date(2024, 1, 15))
        assert len(result) > 0, "Stale cache should still be returned as last resort"


class TestVixCacheRoundTrip:
    """Test Parquet cache save/load."""

    def test_save_and_load(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "src.research.market_regime.VIX_CACHE_PATH",
            tmp_path / "_vix_daily.parquet",
        )
        data = {date(2025, 3, d): 17.0 + d * 0.5 for d in range(1, 10)}
        _save_vix_cache(data)
        loaded = _load_vix_cache()
        assert loaded == data

    def test_merge_preserves_existing(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "src.research.market_regime.VIX_CACHE_PATH",
            tmp_path / "_vix_daily.parquet",
        )
        batch1 = {date(2025, 1, 2): 15.0, date(2025, 1, 3): 16.0}
        batch2 = {date(2025, 1, 3): 16.5, date(2025, 1, 6): 17.0}  # updates Jan 3
        _save_vix_cache(batch1)
        _save_vix_cache(batch2)
        loaded = _load_vix_cache()
        assert loaded[date(2025, 1, 2)] == 15.0  # preserved
        assert loaded[date(2025, 1, 3)] == 16.5  # updated
        assert loaded[date(2025, 1, 6)] == 17.0  # added

    def test_empty_cache_returns_empty(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "src.research.market_regime.VIX_CACHE_PATH",
            tmp_path / "_vix_daily.parquet",
        )
        assert _load_vix_cache() == {}


class TestNoUrlOrKeyLeaks:
    """Ensure warning/log messages never contain URLs or API keys."""

    _URL_PATTERN = re.compile(r"https?://", re.IGNORECASE)
    _KEY_PATTERN = re.compile(r"api[_-]?key", re.IGNORECASE)

    def test_cboe_failure_log_no_url(self, caplog: pytest.LogCaptureFixture) -> None:
        with patch("requests.get", side_effect=ConnectionError("refused")):
            with caplog.at_level(logging.DEBUG, logger="src.research.market_regime"):
                result = _vix_from_cboe_csv()
        assert result == {}
        for record in caplog.records:
            assert not self._URL_PATTERN.search(record.message), (
                f"URL leaked in log: {record.message!r}"
            )

    def test_fred_failure_log_no_url(self, caplog: pytest.LogCaptureFixture) -> None:
        with patch("requests.get", side_effect=ConnectionError("refused")):
            with caplog.at_level(logging.DEBUG, logger="src.research.market_regime"):
                result = _vix_from_fred_csv()
        assert result == {}
        for record in caplog.records:
            assert not self._URL_PATTERN.search(record.message), (
                f"URL leaked in log: {record.message!r}"
            )

    def test_full_chain_failure_log_no_url_or_key(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture,
    ) -> None:
        monkeypatch.setattr(
            "src.research.market_regime.VIX_CACHE_PATH",
            tmp_path / "_vix_daily.parquet",
        )
        monkeypatch.setattr("src.research.market_regime._vix_from_cboe_csv", lambda: {})
        monkeypatch.setattr("src.research.market_regime._vix_from_fred_csv", lambda: {})

        with caplog.at_level(logging.DEBUG, logger="src.research.market_regime"):
            result = _fetch_vix_closes(date(2025, 1, 1), date(2025, 1, 31))
        assert result == {}
        for record in caplog.records:
            assert not self._URL_PATTERN.search(record.message), (
                f"URL leaked in log: {record.message!r}"
            )
            assert not self._KEY_PATTERN.search(record.message), (
                f"API key reference leaked in log: {record.message!r}"
            )
