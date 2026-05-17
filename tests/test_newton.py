"""Tests for the Newton Physics Engine."""

from datetime import datetime, timedelta, timezone

import numpy as np
import polars as pl
import pytest

from src.newton.engine import PhysicsEngine
from src.newton.resampler import TimeframeResampler
from src.newton.transforms import (
    JerkTransform,
    MarketImpulseTransform,
    RelativeVolumeRthTransform,
    RelativeVolumeTransform,
)
from src.strategy.base import required_feature_union
from src.strategy.market_impulse import MarketImpulseStrategy


@pytest.fixture
def sample_ohlcv() -> pl.DataFrame:
    """Create a small synthetic OHLCV DataFrame for testing."""
    np.random.seed(42)
    n = 300  # enough rows for VPOC lookback (240)
    close = np.cumsum(np.random.randn(n) * 0.1) + 100
    high = close + np.abs(np.random.randn(n) * 0.05)
    low = close - np.abs(np.random.randn(n) * 0.05)
    volume = np.random.randint(1000, 50000, size=n)

    return pl.DataFrame({
        "close": close,
        "high": high,
        "low": low,
        "open": close - np.random.randn(n) * 0.02,
        "volume": volume,
    })


class TestPhysicsEngine:
    def test_enrich_adds_all_columns(self, sample_ohlcv: pl.DataFrame) -> None:
        engine = PhysicsEngine(vpoc_lookback=50, ema_periods=[4, 8, 12])
        result = engine.enrich(sample_ohlcv)

        expected_cols = {
            "velocity_1m", "accel_1m", "jerk_1m",
            "ema_4", "ema_8", "ema_12",
            "volume_ma_20", "internal_strength",
            "directional_mass", "directional_mass_ma_20", "vpoc_4h",
        }
        assert expected_cols.issubset(set(result.columns))

    def test_velocity_is_first_diff(self, sample_ohlcv: pl.DataFrame) -> None:
        engine = PhysicsEngine(vpoc_lookback=50)
        result = engine.enrich(sample_ohlcv)

        close = result["close"].to_numpy()
        velocity = result["velocity_1m"].to_numpy()
        # velocity[1] should equal close[1] - close[0]
        np.testing.assert_almost_equal(velocity[1], close[1] - close[0], decimal=10)

    def test_acceleration_is_second_diff(self, sample_ohlcv: pl.DataFrame) -> None:
        engine = PhysicsEngine(vpoc_lookback=50)
        result = engine.enrich(sample_ohlcv)

        vel = result["velocity_1m"].to_numpy()
        accel = result["accel_1m"].to_numpy()
        # accel[2] should equal vel[2] - vel[1]
        np.testing.assert_almost_equal(accel[2], vel[2] - vel[1], decimal=10)

    def test_jerk_is_third_diff(self, sample_ohlcv: pl.DataFrame) -> None:
        engine = PhysicsEngine(vpoc_lookback=50)
        result = engine.enrich(sample_ohlcv)

        accel = result["accel_1m"].to_numpy()
        jerk = result["jerk_1m"].to_numpy()
        np.testing.assert_almost_equal(jerk[3], accel[3] - accel[2], decimal=10)

    def test_ema_columns_count(self, sample_ohlcv: pl.DataFrame) -> None:
        periods = [5, 10, 20, 50]
        engine = PhysicsEngine(vpoc_lookback=50, ema_periods=periods)
        result = engine.enrich(sample_ohlcv)

        for p in periods:
            assert f"ema_{p}" in result.columns

    def test_vpoc_populated_after_lookback(self, sample_ohlcv: pl.DataFrame) -> None:
        lookback = 50
        engine = PhysicsEngine(vpoc_lookback=lookback)
        result = engine.enrich(sample_ohlcv)

        vpoc = result["vpoc_4h"].to_numpy()
        # Before lookback window, VPOC should be NaN
        assert np.isnan(vpoc[lookback - 1])
        # After lookback window, VPOC should be filled
        assert not np.isnan(vpoc[lookback])

    def test_directional_mass_formula_and_zero_range_handling(self) -> None:
        df = pl.DataFrame({
            "open": [10.0, 10.0, 10.0],
            "high": [11.0, 10.0, 12.0],
            "low": [9.0, 10.0, 10.0],
            "close": [10.5, 10.0, 11.0],
            "volume": [1000.0, 2000.0, 1500.0],
        })
        engine = PhysicsEngine(vpoc_lookback=2, volume_ma_period=2)
        result = engine.enrich(df)

        strength = result["internal_strength"].to_numpy()
        dmass = result["directional_mass"].to_numpy()
        dmass_ma = result["directional_mass_ma_2"].to_numpy()

        np.testing.assert_almost_equal(strength[0], 0.5, decimal=10)
        np.testing.assert_almost_equal(dmass[0], 500.0, decimal=10)
        np.testing.assert_almost_equal(strength[1], 0.0, decimal=10)  # high == low
        np.testing.assert_almost_equal(dmass[1], 0.0, decimal=10)
        np.testing.assert_almost_equal(dmass_ma[1], 250.0, decimal=10)
        np.testing.assert_almost_equal(dmass_ma[2], 0.0, decimal=10)

    def test_raises_on_missing_columns(self) -> None:
        df = pl.DataFrame({"close": [1.0, 2.0]})
        engine = PhysicsEngine()
        with pytest.raises(ValueError, match="missing required columns"):
            engine.enrich(df)

    def test_pipeline_resolves_dependencies_for_subset_transforms(
        self,
        sample_ohlcv: pl.DataFrame,
    ) -> None:
        engine = PhysicsEngine(transforms=[JerkTransform()])
        result = engine.enrich(sample_ohlcv)

        assert {"velocity_1m", "accel_1m", "jerk_1m"}.issubset(result.columns)
        assert "vpoc_4h" not in result.columns
        assert "ema_20" not in result.columns

    def test_enrich_for_features_builds_minimal_pipeline(
        self,
        sample_ohlcv: pl.DataFrame,
    ) -> None:
        engine = PhysicsEngine(vpoc_lookback=50, ema_periods=[4, 8, 12])
        result = engine.enrich_for_features(sample_ohlcv, {"jerk_1m", "vpoc_4h"})

        assert {"velocity_1m", "accel_1m", "jerk_1m", "vpoc_4h"}.issubset(result.columns)
        assert "ema_4" not in result.columns
        assert "directional_mass" not in result.columns

    def test_parameterized_velocity_uses_multi_bar_difference(
        self,
        sample_ohlcv: pl.DataFrame,
    ) -> None:
        engine = PhysicsEngine(vpoc_lookback=50)
        result = engine.enrich_for_features(sample_ohlcv, {"velocity_3"})

        close = result["close"].to_numpy()
        velocity = result["velocity_3"].to_numpy()
        np.testing.assert_almost_equal(velocity[3], close[3] - close[0], decimal=10)
        assert "velocity_1m" not in result.columns

    def test_parameterized_jerk_features_resolve_matching_dependencies(
        self,
        sample_ohlcv: pl.DataFrame,
    ) -> None:
        engine = PhysicsEngine(vpoc_lookback=50)
        result = engine.enrich_for_features(sample_ohlcv, {"jerk_3"})

        assert {"velocity_3", "accel_3", "jerk_3"}.issubset(result.columns)
        assert "jerk_1m" not in result.columns

    def test_parameterized_kinematic_transform_specs_are_supported(
        self,
        sample_ohlcv: pl.DataFrame,
    ) -> None:
        engine = PhysicsEngine(transforms=["jerk:3"])
        result = engine.enrich(sample_ohlcv)

        assert {"velocity_3", "accel_3", "jerk_3"}.issubset(result.columns)


def test_resampler_joins_without_lookahead() -> None:
    timestamps = [datetime(2025, 1, 1, 9, 30) + timedelta(minutes=i) for i in range(6)]
    base = pl.DataFrame(
        {
            "timestamp": timestamps,
            "open": [1, 2, 3, 4, 5, 6],
            "high": [1, 2, 3, 4, 5, 6],
            "low": [1, 2, 3, 4, 5, 6],
            "close": [1, 2, 3, 4, 5, 6],
            "volume": [10, 10, 10, 10, 10, 10],
        }
    )
    resampler = TimeframeResampler()
    five_min = resampler.resample_ohlcv(base, every="5m").with_columns(
        pl.Series("regime_5m", ["first", "second"])
    )

    joined = resampler.join_timeframe_features(
        base,
        five_min,
        every="5m",
        feature_columns=["regime_5m"],
    )
    assert joined["regime_5m"].to_list() == ["first", "first", "first", "first", "first", "second"]


def test_market_impulse_transform_supports_custom_timeframe() -> None:
    timestamps = [datetime(2025, 1, 2, 14, 30) + timedelta(minutes=i) for i in range(40)]
    df = pl.DataFrame(
        {
            "timestamp": timestamps,
            "open": np.linspace(100, 102, 40),
            "high": np.linspace(100.1, 102.1, 40),
            "low": np.linspace(99.9, 101.9, 40),
            "close": np.linspace(100, 102, 40),
            "volume": np.full(40, 1000.0),
        }
    )
    engine = PhysicsEngine(transforms=[MarketImpulseTransform(timeframe="15m")])
    result = engine.enrich(df)

    assert "impulse_regime_15m" in result.columns
    assert "impulse_stage_15m" in result.columns
    assert "market_pulse_stage_15m" in result.columns
    assert "vwma_stage_15m" in result.columns
    assert "vma_10_15m" in result.columns


def test_market_impulse_transform_rejects_invalid_vwma_order() -> None:
    with pytest.raises(ValueError, match="strictly increasing"):
        MarketImpulseTransform(vwma_periods=(34, 21, 8))


def test_market_impulse_transform_rejects_invalid_vwma_count() -> None:
    with pytest.raises(ValueError, match="exactly three"):
        MarketImpulseTransform(vwma_periods=(8, 21))


def test_enrich_for_features_accepts_market_impulse_transform_name() -> None:
    timestamps = [datetime(2025, 1, 2, 14, 30) + timedelta(minutes=i) for i in range(40)]
    df = pl.DataFrame(
        {
            "timestamp": timestamps,
            "open": np.linspace(100, 102, 40),
            "high": np.linspace(100.1, 102.1, 40),
            "low": np.linspace(99.9, 101.9, 40),
            "close": np.linspace(100, 102, 40),
            "volume": np.full(40, 1000.0),
        }
    )
    engine = PhysicsEngine()
    result = engine.enrich_for_features(df, {"market_impulse"})

    assert "impulse_regime_5m" in result.columns
    assert "market_pulse_stage_5m" in result.columns
    assert "vwma_stage_5m" in result.columns
    assert "vma_10_5m" in result.columns


def test_enrich_for_features_accepts_parameterized_market_impulse_transform_name() -> None:
    timestamps = [datetime(2025, 1, 2, 14, 30) + timedelta(minutes=i) for i in range(40)]
    df = pl.DataFrame(
        {
            "timestamp": timestamps,
            "open": np.linspace(100, 102, 40),
            "high": np.linspace(100.1, 102.1, 40),
            "low": np.linspace(99.9, 101.9, 40),
            "close": np.linspace(100, 102, 40),
            "volume": np.full(40, 1000.0),
        }
    )
    engine = PhysicsEngine()
    result = engine.enrich_for_features(df, {"market_impulse:15m"})

    assert "impulse_regime_15m" in result.columns
    assert "market_pulse_stage_15m" in result.columns
    assert "vwma_stage_15m" in result.columns
    assert "vma_10_15m" in result.columns
    assert "impulse_regime_5m" not in result.columns
    assert "market_pulse_stage_5m" not in result.columns
    assert "vwma_stage_5m" not in result.columns


def test_enrich_for_features_resolves_market_pulse_stage_column() -> None:
    timestamps = [datetime(2025, 1, 2, 14, 30) + timedelta(minutes=i) for i in range(60)]
    df = pl.DataFrame(
        {
            "timestamp": timestamps,
            "open": np.linspace(100, 104, 60),
            "high": np.linspace(100.1, 104.1, 60),
            "low": np.linspace(99.9, 103.9, 60),
            "close": np.linspace(100, 104, 60),
            "volume": np.full(60, 1000.0),
        }
    )
    result = PhysicsEngine().enrich_for_features(df, {"market_pulse_stage_5m"})

    assert "market_pulse_stage_5m" in result.columns
    assert set(result["market_pulse_stage_5m"].drop_nulls().unique().to_list()).issubset(
        {"bullish", "accumulation", "distribution", "bearish"}
    )


def test_opening_vwap_resets_by_et_session() -> None:
    df = pl.DataFrame(
        {
            "timestamp": [
                datetime(2025, 1, 2, 14, 30, tzinfo=timezone.utc),
                datetime(2025, 1, 2, 14, 31, tzinfo=timezone.utc),
                datetime(2025, 1, 3, 14, 30, tzinfo=timezone.utc),
                datetime(2025, 1, 3, 14, 31, tzinfo=timezone.utc),
            ],
            "open": [10.0, 12.0, 20.0, 22.0],
            "high": [10.5, 12.5, 20.5, 22.5],
            "low": [9.5, 11.5, 19.5, 21.5],
            "close": [10.0, 12.0, 20.0, 22.0],
            "volume": [100.0, 100.0, 100.0, 300.0],
        }
    )

    result = PhysicsEngine().enrich_for_features(df, {"opening_vwap"})

    assert result["opening_vwap"].to_list() == [10.0, 11.0, 20.0, 21.5]


def test_opening_vwap_rth_ignores_premarket_bars() -> None:
    df = pl.DataFrame(
        {
            "timestamp": [
                datetime(2025, 1, 2, 14, 0, tzinfo=timezone.utc),
                datetime(2025, 1, 2, 14, 30, tzinfo=timezone.utc),
                datetime(2025, 1, 2, 14, 31, tzinfo=timezone.utc),
                datetime(2025, 1, 3, 14, 30, tzinfo=timezone.utc),
            ],
            "open": [50.0, 100.0, 102.0, 200.0],
            "high": [50.5, 100.5, 102.5, 200.5],
            "low": [49.5, 99.5, 101.5, 199.5],
            "close": [50.0, 100.0, 102.0, 200.0],
            "volume": [1000.0, 100.0, 300.0, 100.0],
        }
    )

    result = PhysicsEngine().enrich_for_features(df, {"opening_vwap_rth"})

    values = result["opening_vwap_rth"].to_list()
    assert values[0] is None
    np.testing.assert_almost_equal(values[1], 100.0)
    np.testing.assert_almost_equal(values[2], 101.5)
    np.testing.assert_almost_equal(values[3], 200.0)


def test_prior_close_atr_transform_adds_gap_state_and_distance() -> None:
    rows = []
    for day, open_price, close_price in [
        (2, 10.0, 11.0),
        (3, 13.0, 14.0),
    ]:
        for minute in range(2):
            rows.append(
                {
                    "timestamp": datetime(2025, 1, day, 14, 30 + minute, tzinfo=timezone.utc),
                    "open": open_price if minute == 0 else close_price,
                    "high": close_price + 1.0,
                    "low": open_price - 1.0,
                    "close": close_price,
                    "volume": 1000.0,
                }
            )
    df = pl.DataFrame(rows)

    result = PhysicsEngine().enrich_for_features(
        df,
        {"prior_close", "daily_atr_14", "atr_distance_from_prior_close", "gap_state"},
    )
    second_day = result.filter(pl.col("timestamp").dt.date() == datetime(2025, 1, 3).date())

    assert second_day["prior_close"].to_list() == [11.0, 11.0]
    assert second_day["gap_state"].to_list() == ["gap_up_small", "gap_up_small"]
    np.testing.assert_almost_equal(second_day["atr_distance_from_prior_close"].to_list()[0], 1.0)


def test_prior_rth_close_atr_uses_regular_session_only() -> None:
    rows = [
        {
            "timestamp": datetime(2025, 1, 2, 14, 0, tzinfo=timezone.utc),
            "open": 99.0,
            "high": 100.0,
            "low": 98.0,
            "close": 99.0,
            "volume": 1000.0,
        },
        {
            "timestamp": datetime(2025, 1, 2, 14, 30, tzinfo=timezone.utc),
            "open": 10.0,
            "high": 12.0,
            "low": 9.0,
            "close": 11.0,
            "volume": 1000.0,
        },
        {
            "timestamp": datetime(2025, 1, 3, 14, 0, tzinfo=timezone.utc),
            "open": 90.0,
            "high": 91.0,
            "low": 89.0,
            "close": 90.0,
            "volume": 1000.0,
        },
        {
            "timestamp": datetime(2025, 1, 3, 14, 30, tzinfo=timezone.utc),
            "open": 13.0,
            "high": 14.5,
            "low": 12.5,
            "close": 14.0,
            "volume": 1000.0,
        },
    ]
    df = pl.DataFrame(rows)

    result = PhysicsEngine().enrich_for_features(
        df,
        {
            "prior_rth_close",
            "daily_rth_atr_14",
            "atr_distance_from_prior_rth_close",
            "gap_rth_atr",
            "gap_state_rth_open",
        },
    )

    premarket_day2 = result.row(2, named=True)
    rth_day2 = result.row(3, named=True)
    assert premarket_day2["gap_state_rth_open"] is None
    np.testing.assert_almost_equal(rth_day2["prior_rth_close"], 11.0)
    np.testing.assert_almost_equal(rth_day2["daily_rth_atr_14"], 3.0)
    np.testing.assert_almost_equal(rth_day2["gap_rth_atr"], 2.0 / 3.0)
    assert rth_day2["gap_state_rth_open"] == "gap_up_small"
    np.testing.assert_almost_equal(rth_day2["atr_distance_from_prior_rth_close"], 1.0)


def test_enrich_for_features_resolves_market_impulse_from_feature_columns() -> None:
    timestamps = [datetime(2025, 1, 2, 14, 30) + timedelta(minutes=i) for i in range(40)]
    df = pl.DataFrame(
        {
            "timestamp": timestamps,
            "open": np.linspace(100, 102, 40),
            "high": np.linspace(100.1, 102.1, 40),
            "low": np.linspace(99.9, 101.9, 40),
            "close": np.linspace(100, 102, 40),
            "volume": np.full(40, 1000.0),
        }
    )
    engine = PhysicsEngine()
    result = engine.enrich_for_features(df, {"impulse_regime_15m", "vma_10"})

    assert "impulse_regime_15m" in result.columns
    assert "vma_10" in result.columns


def test_enrich_for_features_can_build_multiple_market_impulse_timeframes() -> None:
    timestamps = [datetime(2025, 1, 2, 14, 30) + timedelta(minutes=i) for i in range(90)]
    df = pl.DataFrame(
        {
            "timestamp": timestamps,
            "open": np.linspace(100, 104, 90),
            "high": np.linspace(100.1, 104.1, 90),
            "low": np.linspace(99.9, 103.9, 90),
            "close": np.linspace(100, 104, 90),
            "volume": np.full(90, 1000.0),
        }
    )
    engine = PhysicsEngine()
    result = engine.enrich_for_features(df, {"market_impulse:5m", "market_impulse:15m"})

    assert "impulse_regime_5m" in result.columns
    assert "market_pulse_stage_5m" in result.columns
    assert "impulse_regime_15m" in result.columns
    assert "market_pulse_stage_15m" in result.columns


def test_market_impulse_strategy_declares_pipeline_resolvable_features() -> None:
    timestamps = [datetime(2025, 1, 2, 14, 30) + timedelta(minutes=i) for i in range(40)]
    df = pl.DataFrame(
        {
            "timestamp": timestamps,
            "open": np.linspace(100, 102, 40),
            "high": np.linspace(100.1, 102.1, 40),
            "low": np.linspace(99.9, 101.9, 40),
            "close": np.linspace(100, 102, 40),
            "volume": np.full(40, 1000.0),
        }
    )
    strategy = MarketImpulseStrategy()
    engine = PhysicsEngine()
    result = engine.enrich_for_features(df, required_feature_union([strategy]))

    assert strategy.required_features.issubset(result.columns)
    assert "vma_10_5m" in result.columns
    assert "close_location" in result.columns
    assert "vma_excursion_pct" in result.columns


def test_market_impulse_strategy_can_request_alternate_regime_timeframe() -> None:
    timestamps = [datetime(2025, 1, 2, 14, 30) + timedelta(minutes=i) for i in range(40)]
    df = pl.DataFrame(
        {
            "timestamp": timestamps,
            "open": np.linspace(100, 102, 40),
            "high": np.linspace(100.1, 102.1, 40),
            "low": np.linspace(99.9, 101.9, 40),
            "close": np.linspace(100, 102, 40),
            "volume": np.full(40, 1000.0),
        }
    )
    strategy = MarketImpulseStrategy(regime_timeframe="15m")
    engine = PhysicsEngine()
    result = engine.enrich_for_features(df, required_feature_union([strategy]))

    assert "impulse_regime_15m" in result.columns
    assert strategy.required_features.issubset(result.columns)


def test_market_impulse_strategy_can_request_alternate_vwma_stack() -> None:
    timestamps = [datetime(2025, 1, 2, 14, 30) + timedelta(minutes=i) for i in range(40)]
    df = pl.DataFrame(
        {
            "timestamp": timestamps,
            "open": np.linspace(100, 102, 40),
            "high": np.linspace(100.1, 102.1, 40),
            "low": np.linspace(99.9, 101.9, 40),
            "close": np.linspace(100, 102, 40),
            "volume": np.full(40, 1000.0),
        }
    )
    strategy = MarketImpulseStrategy(vwma_periods=(5, 13, 21))
    engine = PhysicsEngine()
    result = engine.enrich_for_features(df, required_feature_union([strategy]))

    assert "vwma_5" in result.columns
    assert "vwma_13" in result.columns
    assert "vwma_21" in result.columns
    assert "vwma_34" not in result.columns
    assert strategy.required_features.issubset(result.columns)


def test_relative_volume_transform_uses_simple_volume_ma_ratio() -> None:
    df = pl.DataFrame(
        {
            "open": [1.0, 1.0, 1.0],
            "high": [1.0, 1.0, 1.0],
            "low": [1.0, 1.0, 1.0],
            "close": [1.0, 1.0, 1.0],
            "volume": [100.0, 100.0, 200.0],
        }
    )
    engine = PhysicsEngine(transforms=[RelativeVolumeTransform(period=2)])
    result = engine.enrich(df)

    assert "relative_volume_2" in result.columns
    np.testing.assert_almost_equal(result["relative_volume_2"].to_list()[2], 200.0 / 150.0)


def test_relative_volume_rth_transform_ignores_premarket_bars() -> None:
    df = pl.DataFrame(
        {
            "timestamp": [
                datetime(2025, 1, 2, 14, 0, tzinfo=timezone.utc),
                datetime(2025, 1, 2, 14, 30, tzinfo=timezone.utc),
                datetime(2025, 1, 2, 14, 31, tzinfo=timezone.utc),
                datetime(2025, 1, 2, 14, 32, tzinfo=timezone.utc),
            ],
            "open": [1.0] * 4,
            "high": [1.0] * 4,
            "low": [1.0] * 4,
            "close": [1.0] * 4,
            "volume": [5000.0, 100.0, 200.0, 300.0],
        }
    )
    engine = PhysicsEngine(transforms=[RelativeVolumeRthTransform(period=2)])

    result = engine.enrich(df)

    values = result["relative_volume_rth_2"].to_list()
    assert values[0] is None
    assert values[1] is None
    np.testing.assert_almost_equal(values[2], 200.0 / 150.0)
    np.testing.assert_almost_equal(values[3], 300.0 / 250.0)


def test_aggregated_relative_volume_transform_uses_volume_sum_ratio() -> None:
    df = pl.DataFrame(
        {
            "open": [1.0] * 6,
            "high": [1.0] * 6,
            "low": [1.0] * 6,
            "close": [1.0] * 6,
            "volume": [100.0, 100.0, 200.0, 300.0, 500.0, 800.0],
        }
    )
    engine = PhysicsEngine()
    result = engine.enrich_for_features(df, {"relative_volume_sum_3_over_ma_2"})

    assert "relative_volume_sum_3_over_ma_2" in result.columns
    # 3-bar sums at indices 4 and 5 are 1000 and 1600; their 2-period mean at
    # index 5 is 1300.
    np.testing.assert_almost_equal(
        result["relative_volume_sum_3_over_ma_2"].to_list()[5],
        1600.0 / 1300.0,
    )


def test_descendant_market_impulse_strategy_declares_resolvable_features() -> None:
    timestamps = [datetime(2025, 1, 2, 14, 30) + timedelta(minutes=i) for i in range(40)]
    df = pl.DataFrame(
        {
            "timestamp": timestamps,
            "open": np.linspace(100, 102, 40),
            "high": np.linspace(100.1, 102.1, 40),
            "low": np.linspace(99.9, 101.9, 40),
            "close": np.linspace(100, 102, 40),
            "volume": np.linspace(1000.0, 2000.0, 40),
        }
    )
    strategy = MarketImpulseStrategy(
        entry_mode="same_bar_shallow_reclaim",
        max_vma_excursion_pct=0.001,
        use_volume_filter=True,
        min_relative_volume=1.1,
    )
    engine = PhysicsEngine()
    result = engine.enrich_for_features(df, required_feature_union([strategy]))

    assert "vma_excursion_pct" in result.columns
    assert "relative_volume_20" in result.columns
    assert strategy.required_features.issubset(result.columns)
