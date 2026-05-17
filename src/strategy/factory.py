"""Strategy factory helpers used by evaluation and orchestration layers."""

from __future__ import annotations

import re
from typing import Any, Callable

from src.config import settings
from src.strategy.base import BaseStrategy
from src.strategy.compression_breakout import CompressionBreakoutStrategy
from src.strategy.elastic_band_reversion import ElasticBandReversionStrategy
from src.strategy.intraday_mean_reversion import IntradayMeanReversionStrategy
from src.strategy.jerk_pivot_momentum import JerkPivotMomentumStrategy
from src.strategy.market_impulse import MarketImpulseStrategy
from src.strategy.opening_drive_classifier import OpeningDriveClassifierStrategy


StrategyBuilder = Callable[[dict[str, Any] | None], BaseStrategy]


def _build_elastic(params: dict[str, Any] | None = None) -> BaseStrategy:
    return ElasticBandReversionStrategy(**(params or {}))


def _build_compression(params: dict[str, Any] | None = None) -> BaseStrategy:
    defaults = {
        "compression_window": 20,
        "breakout_lookback": 20,
        "compression_factor": 0.85,
        "volume_ma_period": settings.volume_ma_period,
        "volume_multiplier": 1.15,
        "use_time_filter": True,
    }
    return CompressionBreakoutStrategy(**(defaults | (params or {})))


def _build_opening_drive(params: dict[str, Any] | None = None) -> BaseStrategy:
    defaults = {
        "opening_window_minutes": 25,
        "entry_start_offset_minutes": 30,
        "entry_end_offset_minutes": 120,
        "min_drive_return_pct": 0.0015,
        "volume_multiplier": 1.2,
    }
    return OpeningDriveClassifierStrategy(**(defaults | (params or {})))


def _build_jerk_pivot_tight(params: dict[str, Any] | None = None) -> BaseStrategy:
    cleaned = dict(params or {})
    # `reward_risk_ratio` is tracked in repo memory for evaluation, but it is not
    # a constructor argument for the strategy itself.
    cleaned.pop("reward_risk_ratio", None)
    defaults = {
        "vpoc_proximity_pct": 0.002,
        "jerk_lookback": 10,
        "volume_multiplier": 1.3,
        "use_volume_filter": True,
        "strategy_label": "Jerk-Pivot Momentum (tight)",
    }
    return JerkPivotMomentumStrategy(**(defaults | cleaned))


def _build_intraday_mean_reversion(params: dict[str, Any] | None = None) -> BaseStrategy:
    return IntradayMeanReversionStrategy(**(params or {}))


def _build_market_impulse(params: dict[str, Any] | None = None) -> BaseStrategy:
    defaults = {
        "entry_buffer_minutes": 3,
        "entry_window_minutes": 60,
        "regime_timeframe": "5m",
    }
    return MarketImpulseStrategy(**(defaults | (params or {})))


def _build_market_impulse_descendants(params: dict[str, Any] | None = None) -> BaseStrategy:
    defaults = {
        "entry_buffer_minutes": 3,
        "entry_window_minutes": 60,
        "regime_timeframe": "5m",
        "entry_mode": "same_bar_shallow_reclaim",
        "max_vma_excursion_pct": 0.001,
    }
    return MarketImpulseStrategy(**(defaults | (params or {})))


def _build_market_impulse_mode(
    entry_mode: str,
    defaults: dict[str, Any] | None = None,
) -> StrategyBuilder:
    def builder(params: dict[str, Any] | None = None) -> BaseStrategy:
        base = {
            "entry_buffer_minutes": 3,
            "entry_window_minutes": 60,
            "regime_timeframe": "5m",
            "entry_mode": entry_mode,
        }
        return MarketImpulseStrategy(**(base | (defaults or {}) | (params or {})))

    return builder


_NAMED_BUILDERS: dict[str, StrategyBuilder] = {
    "Elastic Band Reversion": _build_elastic,
    "Compression Expansion Breakout": _build_compression,
    "Opening Drive Classifier": _build_opening_drive,
    "Jerk-Pivot Momentum (tight)": _build_jerk_pivot_tight,
    "Intraday Mean Reversion at Extremes": _build_intraday_mean_reversion,
    "Market Impulse (Cross & Reclaim)": _build_market_impulse,
    "Market Impulse Descendants": _build_market_impulse_descendants,
    "MI Shallow Spring": _build_market_impulse_mode(
        "same_bar_shallow_reclaim",
        {"max_vma_excursion_pct": 0.001},
    ),
    "MI Second Touch": _build_market_impulse_mode(
        "delayed_reclaim",
        {"max_vma_excursion_pct": 0.002, "reclaim_window_bars": 3},
    ),
    "MI High Close Reclaim": _build_market_impulse_mode(
        "close_location_reclaim",
        {"min_close_location": 0.7},
    ),
    "MI Push Through": _build_market_impulse_mode(
        "continuation_confirmation",
        {"confirmation_window_bars": 2},
    ),
}


def available_strategy_names() -> tuple[str, ...]:
    return tuple(sorted(_NAMED_BUILDERS))


def build_strategy(strategy_name: str, params: dict[str, Any] | None = None) -> BaseStrategy:
    builder = _NAMED_BUILDERS.get(strategy_name)
    if builder is None:
        raise ValueError(f"Unsupported strategy name: {strategy_name}")
    return builder(params)


def build_strategy_by_name(strategy_name: str) -> BaseStrategy:
    """Backward-compatible parser for scripts and stored strategy names."""
    # Parse parametric Elastic Band: "Elastic Band z=1.0/w=240+dm"
    if strategy_name.startswith("Elastic Band z="):
        match = re.search(r"z=([\d\.]+)/w=(\d+)(\+dm)?", strategy_name)
        if match:
            return ElasticBandReversionStrategy(
                z_score_threshold=float(match.group(1)),
                z_score_window=int(match.group(2)),
                use_directional_mass=bool(match.group(3)),
            )

    return build_strategy(strategy_name)
