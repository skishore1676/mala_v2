"""
Market Impulse Strategy

Multi-timeframe entry strategy based on the TOS Market Pulse indicator.

Regime detection (5-min chart):
  - Bullish: VWMA(8) > VWMA(21) > VWMA(34)
  - Bearish: VWMA(8) < VWMA(21) < VWMA(34)

Entry trigger (1-min chart):
  - Long:  Bullish 5-min regime + price cross-and-reclaims VMA from above
           (bar low dips to/below VMA, close recovers above VMA)
  - Short: Bearish 5-min regime + price cross-and-reclaims VMA from below
           (bar high reaches to/above VMA, close falls back below VMA)

Time filter:
  - Only enter between market_open + buffer and market_open + max_window
  - Default: 9:33 – 10:30 ET
"""

from __future__ import annotations

import re
from datetime import time
from typing import Any, Optional

import polars as pl
from loguru import logger

from src.newton.market_impulse import (
    market_impulse_vwma_feature_spec,
    validate_vwma_periods,
)
from src.research.models import (
    ConstraintSpec,
    DomainSpec,
    GatingCondition,
    MonotonicOrdering,
    ObjectiveSpec,
    ParameterSpec,
    StrategySearchSpec,
)
from src.strategy.base import BaseStrategy
from src.time_utils import et_time_expr


BASELINE_ENTRY_MODE = "cross_reclaim"
DESCENDANT_ENTRY_MODES = (
    "same_bar_shallow_reclaim",
    "delayed_reclaim",
    "close_location_reclaim",
    "continuation_confirmation",
)
ENTRY_MODE_LABELS = {
    BASELINE_ENTRY_MODE: "Market Impulse (Cross & Reclaim)",
    "same_bar_shallow_reclaim": "MI Shallow Spring",
    "delayed_reclaim": "MI Second Touch",
    "close_location_reclaim": "MI High Close Reclaim",
    "continuation_confirmation": "MI Push Through",
}
CONFIRMATION_TYPES = ("break_reclaim_high_low", "close_beyond_reclaim", "vma_margin")


class MarketImpulseStrategy(BaseStrategy):
    """Multi-timeframe Market Impulse strategy with cross-and-reclaim entry."""

    def __init__(
        self,
        entry_buffer_minutes: int = 3,
        entry_window_minutes: int = 60,
        market_open_hour: int = 9,
        market_open_minute: int = 30,
        vma_col: str | None = None,
        regime_timeframe: str = "5m",
        regime_col: str | None = None,
        vma_length: int | None = None,
        vwma_periods: tuple[int, ...] = (8, 21, 34),
        entry_mode: str = BASELINE_ENTRY_MODE,
        max_vma_excursion_pct: float | None = None,
        max_vma_excursion_atr: float | None = None,
        min_reclaim_margin_pct: float = 0.0,
        min_close_location: float | None = None,
        reclaim_window_bars: int = 3,
        min_bars_after_pierce: int = 1,
        require_regime_persistent: bool = True,
        confirmation_window_bars: int = 2,
        confirmation_type: str = "break_reclaim_high_low",
        confirmation_margin_pct: float = 0.0,
        use_volume_filter: bool = False,
        relative_volume_period: int = 20,
        min_relative_volume: float | None = None,
        max_panic_relative_volume: float | None = None,
        use_gap_context: bool = False,
        use_sector_confirmation: bool = False,
    ) -> None:
        self.entry_buffer_minutes = entry_buffer_minutes
        self.entry_window_minutes = entry_window_minutes
        self.market_open = time(market_open_hour, market_open_minute)
        self.vma_length = _resolve_vma_length(vma_length=vma_length, vma_col=vma_col)
        self.vwma_periods = _normalize_vwma_periods(vwma_periods)
        self.vma_col = vma_col or f"vma_{self.vma_length}"
        self.regime_timeframe = _resolve_regime_timeframe(
            regime_timeframe=regime_timeframe,
            regime_col=regime_col,
        )
        self.regime_col = regime_col or f"impulse_regime_{self.regime_timeframe}"
        self.entry_mode = _normalize_entry_mode(entry_mode)
        self.max_vma_excursion_pct = max_vma_excursion_pct
        self.max_vma_excursion_atr = max_vma_excursion_atr
        self.min_reclaim_margin_pct = float(min_reclaim_margin_pct)
        self.min_close_location = min_close_location
        self.reclaim_window_bars = int(reclaim_window_bars)
        self.min_bars_after_pierce = int(min_bars_after_pierce)
        self.require_regime_persistent = bool(require_regime_persistent)
        self.confirmation_window_bars = int(confirmation_window_bars)
        self.confirmation_type = _normalize_confirmation_type(confirmation_type)
        self.confirmation_margin_pct = float(confirmation_margin_pct)
        self.use_volume_filter = bool(use_volume_filter)
        self.relative_volume_period = int(relative_volume_period)
        self.min_relative_volume = min_relative_volume
        self.max_panic_relative_volume = max_panic_relative_volume
        self.use_gap_context = bool(use_gap_context)
        self.use_sector_confirmation = bool(use_sector_confirmation)
        if self.use_gap_context:
            raise ValueError(
                "Market Impulse gap context is deferred until prior-session close/open "
                "features are canonical in Newton."
            )
        if self.use_sector_confirmation:
            raise ValueError(
                "Market Impulse sector confirmation is deferred until the engine supports "
                "clean multi-symbol feature joins."
            )

        # Compute the valid entry window bounds
        open_minutes = market_open_hour * 60 + market_open_minute
        self._entry_start = self._minutes_to_time(open_minutes + entry_buffer_minutes)
        self._entry_end = self._minutes_to_time(open_minutes + entry_window_minutes)

    @property
    def name(self) -> str:
        return ENTRY_MODE_LABELS[self.entry_mode]

    @property
    def required_features(self) -> set[str]:
        features = {
            "timestamp",
            "close",
            "high",
            "low",
            self.vma_col,
            self.regime_col,
        }
        if self.entry_mode in DESCENDANT_ENTRY_MODES:
            features.add("vma_excursion_pct")
        if self.entry_mode in {"same_bar_shallow_reclaim", "close_location_reclaim"}:
            if self.min_close_location is not None:
                features.add("close_location")
        if self.entry_mode == "close_location_reclaim":
            features.add("close_location")
        if self.use_volume_filter:
            features.add(f"relative_volume_{self.relative_volume_period}")
        return features

    @property
    def feature_requests(self) -> set[str]:
        requests = {market_impulse_vwma_feature_spec(self.vwma_periods)}
        if self.use_volume_filter:
            requests.add(f"relative_volume:{self.relative_volume_period}")
        return requests

    @property
    def parameter_space(self) -> dict[str, list[Any]]:
        return {
            "entry_buffer_minutes": [3, 5],
            "entry_window_minutes": [45, 60, 90],
            "regime_timeframe": ["5m", "15m", "30m", "1h"],
            "vwma_periods": [(5, 13, 21), (8, 21, 34), (10, 20, 40)],
            "entry_mode": list(DESCENDANT_ENTRY_MODES),
        }

    @property
    def evaluation_mode(self) -> str:
        return "directional"

    @property
    def search_spec(self) -> StrategySearchSpec:
        if self.entry_mode == BASELINE_ENTRY_MODE:
            return StrategySearchSpec(
                parameters=[
                    ParameterSpec(
                        name="entry_buffer_minutes",
                        type="discrete",
                        domain=DomainSpec(values=[3, 5]),
                        default=self.entry_buffer_minutes,
                        prior_center=3,
                    ),
                    ParameterSpec(
                        name="entry_window_minutes",
                        type="discrete",
                        domain=DomainSpec(values=[45, 60, 90]),
                        default=self.entry_window_minutes,
                        prior_center=60,
                    ),
                    ParameterSpec(
                        name="regime_timeframe",
                        type="categorical",
                        domain=DomainSpec(values=["5m", "15m", "30m", "1h"]),
                        default=self.regime_timeframe,
                        prior_center="5m",
                    ),
                    ParameterSpec(
                        name="vwma_periods",
                        type="categorical",
                        domain=DomainSpec(
                            values=[(5, 13, 21), (8, 21, 34), (10, 20, 40)]
                        ),
                        default=tuple(self.vwma_periods),
                        prior_center=(8, 21, 34),
                    ),
                ],
                constraints=ConstraintSpec(
                    monotonic_ordering=[
                        MonotonicOrdering(
                            parameters=["entry_buffer_minutes", "entry_window_minutes"],
                            direction="strictly_ascending",
                        )
                    ]
                ),
                objective=ObjectiveSpec(
                    primary_metric="avg_test_exp_r",
                    minimum_signals=20,
                    tie_breakers=["pct_positive_oos_windows", "oos_signals"],
                ),
            )
        return StrategySearchSpec(
            parameters=[
                ParameterSpec(
                    name="entry_buffer_minutes",
                    type="discrete",
                    domain=DomainSpec(values=[3, 5]),
                    default=self.entry_buffer_minutes,
                    prior_center=3,
                ),
                ParameterSpec(
                    name="entry_window_minutes",
                    type="discrete",
                    domain=DomainSpec(values=[45, 60, 90]),
                    default=self.entry_window_minutes,
                    prior_center=60,
                ),
                ParameterSpec(
                    name="regime_timeframe",
                    type="categorical",
                    domain=DomainSpec(values=["5m", "15m", "30m", "1h"]),
                    default=self.regime_timeframe,
                    prior_center="5m",
                ),
                ParameterSpec(
                    name="vwma_periods",
                    type="categorical",
                    domain=DomainSpec(values=[(5, 13, 21), (8, 21, 34), (10, 20, 40)]),
                    default=tuple(self.vwma_periods),
                    prior_center=(8, 21, 34),
                ),
                ParameterSpec(
                    name="entry_mode",
                    type="categorical",
                    domain=DomainSpec(
                        values=[self.entry_mode]
                        if self.entry_mode == BASELINE_ENTRY_MODE
                        else list(DESCENDANT_ENTRY_MODES)
                    ),
                    default=self.entry_mode,
                    prior_center=self.entry_mode,
                ),
                ParameterSpec(
                    name="max_vma_excursion_pct",
                    type="discrete",
                    domain=DomainSpec(values=[0.0005, 0.001, 0.002]),
                    default=(
                        self.max_vma_excursion_pct
                        if self.max_vma_excursion_pct is not None
                        else 0.001
                    ),
                    prior_center=0.001,
                ),
                ParameterSpec(
                    name="min_reclaim_margin_pct",
                    type="discrete",
                    domain=DomainSpec(values=[0.0, 0.0002, 0.0005]),
                    default=self.min_reclaim_margin_pct,
                    prior_center=0.0,
                ),
                ParameterSpec(
                    name="min_close_location",
                    type="discrete",
                    domain=DomainSpec(values=[0.6, 0.7, 0.8]),
                    default=self.min_close_location if self.min_close_location is not None else 0.7,
                    prior_center=0.7,
                ),
                ParameterSpec(
                    name="reclaim_window_bars",
                    type="discrete",
                    domain=DomainSpec(values=[2, 3, 5]),
                    default=self.reclaim_window_bars,
                    prior_center=3,
                ),
                ParameterSpec(
                    name="min_bars_after_pierce",
                    type="discrete",
                    domain=DomainSpec(values=[1, 2]),
                    default=self.min_bars_after_pierce,
                    prior_center=1,
                ),
                ParameterSpec(
                    name="require_regime_persistent",
                    type="categorical",
                    domain=DomainSpec(values=[True, False]),
                    default=self.require_regime_persistent,
                    prior_center=True,
                ),
                ParameterSpec(
                    name="confirmation_window_bars",
                    type="discrete",
                    domain=DomainSpec(values=[1, 2, 3]),
                    default=self.confirmation_window_bars,
                    prior_center=2,
                ),
                ParameterSpec(
                    name="confirmation_type",
                    type="categorical",
                    domain=DomainSpec(values=list(CONFIRMATION_TYPES)),
                    default=self.confirmation_type,
                    prior_center="break_reclaim_high_low",
                ),
                ParameterSpec(
                    name="confirmation_margin_pct",
                    type="discrete",
                    domain=DomainSpec(values=[0.0, 0.0003, 0.0005]),
                    default=self.confirmation_margin_pct,
                    prior_center=0.0,
                ),
                ParameterSpec(
                    name="use_volume_filter",
                    type="categorical",
                    domain=DomainSpec(values=[False, True]),
                    default=self.use_volume_filter,
                    prior_center=False,
                ),
                ParameterSpec(
                    name="relative_volume_period",
                    type="discrete",
                    domain=DomainSpec(values=[20, 30]),
                    default=self.relative_volume_period,
                    prior_center=20,
                ),
                ParameterSpec(
                    name="min_relative_volume",
                    type="discrete",
                    domain=DomainSpec(values=[1.0, 1.2, 1.5]),
                    default=self.min_relative_volume if self.min_relative_volume is not None else 1.2,
                    prior_center=1.2,
                ),
                ParameterSpec(
                    name="max_panic_relative_volume",
                    type="discrete",
                    domain=DomainSpec(values=[3.0, 5.0]),
                    default=(
                        self.max_panic_relative_volume
                        if self.max_panic_relative_volume is not None
                        else 5.0
                    ),
                    prior_center=5.0,
                ),
            ],
            constraints=ConstraintSpec(
                gating_conditions=[
                    GatingCondition(
                        parameter="min_close_location",
                        requires={"entry_mode": "close_location_reclaim"},
                    ),
                    GatingCondition(
                        parameter="reclaim_window_bars",
                        requires={"entry_mode": "delayed_reclaim"},
                    ),
                    GatingCondition(
                        parameter="min_bars_after_pierce",
                        requires={"entry_mode": "delayed_reclaim"},
                    ),
                    GatingCondition(
                        parameter="require_regime_persistent",
                        requires={"entry_mode": "delayed_reclaim"},
                    ),
                    GatingCondition(
                        parameter="confirmation_window_bars",
                        requires={"entry_mode": "continuation_confirmation"},
                    ),
                    GatingCondition(
                        parameter="confirmation_type",
                        requires={"entry_mode": "continuation_confirmation"},
                    ),
                    GatingCondition(
                        parameter="confirmation_margin_pct",
                        requires={"entry_mode": "continuation_confirmation"},
                    ),
                    GatingCondition(
                        parameter="relative_volume_period",
                        requires={"use_volume_filter": True},
                    ),
                    GatingCondition(
                        parameter="min_relative_volume",
                        requires={"use_volume_filter": True},
                    ),
                    GatingCondition(
                        parameter="max_panic_relative_volume",
                        requires={"use_volume_filter": True},
                    ),
                ],
                monotonic_ordering=[
                    MonotonicOrdering(
                        parameters=["entry_buffer_minutes", "entry_window_minutes"],
                        direction="strictly_ascending",
                    )
                ]
            ),
            objective=ObjectiveSpec(
                primary_metric="avg_test_exp_r",
                minimum_signals=20,
                tie_breakers=["pct_positive_oos_windows", "oos_signals"],
            ),
        )

    def strategy_config(self) -> dict[str, Any]:
        return {
            "entry_buffer_minutes": self.entry_buffer_minutes,
            "entry_window_minutes": self.entry_window_minutes,
            "market_open_hour": self.market_open.hour,
            "market_open_minute": self.market_open.minute,
            "vma_length": self.vma_length,
            "regime_timeframe": self.regime_timeframe,
            "vwma_periods": list(self.vwma_periods),
            "entry_mode": self.entry_mode,
            "max_vma_excursion_pct": self.max_vma_excursion_pct,
            "max_vma_excursion_atr": self.max_vma_excursion_atr,
            "min_reclaim_margin_pct": self.min_reclaim_margin_pct,
            "min_close_location": self.min_close_location,
            "reclaim_window_bars": self.reclaim_window_bars,
            "min_bars_after_pierce": self.min_bars_after_pierce,
            "require_regime_persistent": self.require_regime_persistent,
            "confirmation_window_bars": self.confirmation_window_bars,
            "confirmation_type": self.confirmation_type,
            "confirmation_margin_pct": self.confirmation_margin_pct,
            "use_volume_filter": self.use_volume_filter,
            "relative_volume_period": self.relative_volume_period,
            "min_relative_volume": self.min_relative_volume,
            "max_panic_relative_volume": self.max_panic_relative_volume,
            "use_gap_context": self.use_gap_context,
            "use_sector_confirmation": self.use_sector_confirmation,
        }

    def generate_signals(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Apply the Market Impulse entry logic.

        Required columns:
          - timestamp (datetime)
          - close, high, low
          - vma_10  (1-min VMA from Market Impulse indicator)
          - impulse_regime_5m (5-min regime: "bullish" / "bearish" / "neutral")
        """
        required = self.required_features
        missing = required - set(df.columns)
        if missing:
            raise ValueError(
                f"Strategy '{self.name}' requires columns: {missing}"
            )

        # ── Time Filter ─────────────────────────────────────────────────
        # Extract ET time-of-day from stored UTC timestamp
        df = df.with_columns(
            et_time_expr("timestamp").alias("_bar_time")
        )

        time_filter = (
            (pl.col("_bar_time") >= self._entry_start)
            & (pl.col("_bar_time") <= self._entry_end)
        )

        # ── Cross-and-Reclaim Logic ─────────────────────────────────────
        # Long: bar low dips to/below VMA AND close recovers above VMA
        long_cross_reclaim = (
            (pl.col("low") <= pl.col(self.vma_col))
            & (pl.col("close") > pl.col(self.vma_col))
        )

        # Short: bar high reaches to/above VMA AND close falls below VMA
        short_cross_reclaim = (
            (pl.col("high") >= pl.col(self.vma_col))
            & (pl.col("close") < pl.col(self.vma_col))
        )

        # ── Regime Filter ───────────────────────────────────────────────
        bullish_regime = pl.col(self.regime_col) == "bullish"
        bearish_regime = pl.col(self.regime_col) == "bearish"
        volume_filter = self._volume_filter_expr()

        # ── Combine Signals ─────────────────────────────────────────────
        if self.entry_mode == BASELINE_ENTRY_MODE:
            long_signal = time_filter & bullish_regime & long_cross_reclaim & volume_filter
            short_signal = time_filter & bearish_regime & short_cross_reclaim & volume_filter
            df = df.with_columns([
                (long_signal | short_signal).alias("signal"),
                pl.when(long_signal)
                .then(pl.lit("long"))
                .when(short_signal)
                .then(pl.lit("short"))
                .otherwise(pl.lit(None))
                .alias("signal_direction"),
            ])
        elif self.entry_mode in {
            "same_bar_shallow_reclaim",
            "close_location_reclaim",
        }:
            long_signal = (
                time_filter
                & bullish_regime
                & long_cross_reclaim
                & self._long_reclaim_margin_expr()
                & self._excursion_filter_expr("long")
                & self._long_close_location_expr()
                & volume_filter
            )
            short_signal = (
                time_filter
                & bearish_regime
                & short_cross_reclaim
                & self._short_reclaim_margin_expr()
                & self._excursion_filter_expr("short")
                & self._short_close_location_expr()
                & volume_filter
            )
            df = df.with_columns([
                (long_signal | short_signal).alias("signal"),
                pl.when(long_signal)
                .then(pl.lit("long"))
                .when(short_signal)
                .then(pl.lit("short"))
                .otherwise(pl.lit(None))
                .alias("signal_direction"),
            ])
        elif self.entry_mode == "delayed_reclaim":
            df = self._generate_delayed_reclaim_signals(
                df,
                time_filter=time_filter,
                bullish_regime=bullish_regime,
                bearish_regime=bearish_regime,
                volume_filter=volume_filter,
            )
        elif self.entry_mode == "continuation_confirmation":
            df = self._generate_continuation_confirmation_signals(
                df,
                time_filter=time_filter,
                bullish_regime=bullish_regime,
                bearish_regime=bearish_regime,
                volume_filter=volume_filter,
            )
        else:
            raise ValueError(f"Unsupported Market Impulse entry_mode: {self.entry_mode}")

        # Clean up temp column
        df = df.drop("_bar_time")

        # Log summary
        total_signals = df.filter(pl.col("signal")).height
        long_count = df.filter(pl.col("signal_direction") == "long").height
        short_count = df.filter(pl.col("signal_direction") == "short").height
        logger.info(
            "Strategy '{}' generated {} signals ({} long, {} short) "
            "out of {} bars (window: {} – {})",
            self.name,
            total_signals,
            long_count,
            short_count,
            len(df),
            self._entry_start,
            self._entry_end,
        )
        return df

    def _generate_delayed_reclaim_signals(
        self,
        df: pl.DataFrame,
        *,
        time_filter: pl.Expr,
        bullish_regime: pl.Expr,
        bearish_regime: pl.Expr,
        volume_filter: pl.Expr,
    ) -> pl.DataFrame:
        context = df.with_columns(
            [
                time_filter.alias("_time_ok"),
                bullish_regime.alias("_bullish_regime"),
                bearish_regime.alias("_bearish_regime"),
                volume_filter.alias("_volume_ok"),
                self._excursion_filter_expr("long").alias("_long_excursion_ok"),
                self._excursion_filter_expr("short").alias("_short_excursion_ok"),
                self._long_reclaim_margin_expr().alias("_long_reclaim_ok"),
                self._short_reclaim_margin_expr().alias("_short_reclaim_ok"),
            ]
        )
        rows = context.select(
            [
                "high",
                "low",
                "close",
                self.vma_col,
                self.regime_col,
                "_time_ok",
                "_bullish_regime",
                "_bearish_regime",
                "_volume_ok",
                "_long_excursion_ok",
                "_short_excursion_ok",
                "_long_reclaim_ok",
                "_short_reclaim_ok",
            ]
        ).to_dicts()
        signals, directions = self._delayed_reclaim_from_rows(rows)
        return context.with_columns(
            [
                pl.Series("signal", signals),
                pl.Series("signal_direction", directions),
            ]
        ).drop(
            [
                "_time_ok",
                "_bullish_regime",
                "_bearish_regime",
                "_volume_ok",
                "_long_excursion_ok",
                "_short_excursion_ok",
                "_long_reclaim_ok",
                "_short_reclaim_ok",
            ]
        )

    def _generate_continuation_confirmation_signals(
        self,
        df: pl.DataFrame,
        *,
        time_filter: pl.Expr,
        bullish_regime: pl.Expr,
        bearish_regime: pl.Expr,
        volume_filter: pl.Expr,
    ) -> pl.DataFrame:
        context = df.with_columns(
            [
                time_filter.alias("_time_ok"),
                bullish_regime.alias("_bullish_regime"),
                bearish_regime.alias("_bearish_regime"),
                volume_filter.alias("_volume_ok"),
                self._excursion_filter_expr("long").alias("_long_excursion_ok"),
                self._excursion_filter_expr("short").alias("_short_excursion_ok"),
                self._long_reclaim_margin_expr().alias("_long_reclaim_ok"),
                self._short_reclaim_margin_expr().alias("_short_reclaim_ok"),
            ]
        )
        rows = context.select(
            [
                "high",
                "low",
                "close",
                self.vma_col,
                self.regime_col,
                "_time_ok",
                "_bullish_regime",
                "_bearish_regime",
                "_volume_ok",
                "_long_excursion_ok",
                "_short_excursion_ok",
                "_long_reclaim_ok",
                "_short_reclaim_ok",
            ]
        ).to_dicts()
        signals, directions = self._continuation_confirmation_from_rows(rows)
        return context.with_columns(
            [
                pl.Series("signal", signals),
                pl.Series("signal_direction", directions),
            ]
        ).drop(
            [
                "_time_ok",
                "_bullish_regime",
                "_bearish_regime",
                "_volume_ok",
                "_long_excursion_ok",
                "_short_excursion_ok",
                "_long_reclaim_ok",
                "_short_reclaim_ok",
            ]
        )

    def _delayed_reclaim_from_rows(
        self,
        rows: list[dict[str, Any]],
    ) -> tuple[list[bool], list[str | None]]:
        signals = [False] * len(rows)
        directions: list[str | None] = [None] * len(rows)
        pending: dict[str, dict[str, Any] | None] = {"long": None, "short": None}

        for index, row in enumerate(rows):
            vma = row[self.vma_col]
            if vma is None:
                continue

            for side in ("long", "short"):
                state = pending[side]
                if state is None:
                    continue
                age = index - int(state["index"])
                regime_ok = (
                    row["_bullish_regime"] if side == "long" else row["_bearish_regime"]
                )
                if age > self.reclaim_window_bars or (
                    self.require_regime_persistent and not regime_ok
                ):
                    pending[side] = None
                    continue
                if age < self.min_bars_after_pierce:
                    continue
                reclaim_ok = (
                    row["_long_reclaim_ok"] if side == "long" else row["_short_reclaim_ok"]
                )
                if row["_time_ok"] and row["_volume_ok"] and regime_ok and reclaim_ok:
                    signals[index] = True
                    directions[index] = side
                    pending[side] = None

            if (
                pending["long"] is None
                and row["_bullish_regime"]
                and row["_long_excursion_ok"]
                and row["low"] <= vma
                and row["close"] <= vma
            ):
                pending["long"] = {"index": index}
            if (
                pending["short"] is None
                and row["_bearish_regime"]
                and row["_short_excursion_ok"]
                and row["high"] >= vma
                and row["close"] >= vma
            ):
                pending["short"] = {"index": index}

        return signals, directions

    def _continuation_confirmation_from_rows(
        self,
        rows: list[dict[str, Any]],
    ) -> tuple[list[bool], list[str | None]]:
        signals = [False] * len(rows)
        directions: list[str | None] = [None] * len(rows)
        pending: dict[str, dict[str, Any] | None] = {"long": None, "short": None}

        for index, row in enumerate(rows):
            vma = row[self.vma_col]
            if vma is None:
                continue

            for side in ("long", "short"):
                state = pending[side]
                if state is None:
                    continue
                age = index - int(state["index"])
                regime_ok = (
                    row["_bullish_regime"] if side == "long" else row["_bearish_regime"]
                )
                if age > self.confirmation_window_bars or not regime_ok:
                    pending[side] = None
                    continue
                if age <= 0:
                    continue
                if (
                    row["_time_ok"]
                    and row["_volume_ok"]
                    and self._row_confirms_continuation(row, state, side)
                ):
                    signals[index] = True
                    directions[index] = side
                    pending[side] = None

            long_reclaim = (
                row["_bullish_regime"]
                and row["_long_excursion_ok"]
                and row["_long_reclaim_ok"]
                and row["low"] <= vma
            )
            short_reclaim = (
                row["_bearish_regime"]
                and row["_short_excursion_ok"]
                and row["_short_reclaim_ok"]
                and row["high"] >= vma
            )
            if long_reclaim:
                pending["long"] = {"index": index, "high": row["high"], "low": row["low"]}
            if short_reclaim:
                pending["short"] = {"index": index, "high": row["high"], "low": row["low"]}

        return signals, directions

    def _row_confirms_continuation(
        self,
        row: dict[str, Any],
        state: dict[str, Any],
        side: str,
    ) -> bool:
        vma = row[self.vma_col]
        if self.confirmation_type == "break_reclaim_high_low":
            return row["high"] > state["high"] if side == "long" else row["low"] < state["low"]
        if self.confirmation_type == "close_beyond_reclaim":
            return row["close"] > state["high"] if side == "long" else row["close"] < state["low"]
        margin = self.confirmation_margin_pct
        if side == "long":
            return row["close"] >= vma * (1.0 + margin)
        return row["close"] <= vma * (1.0 - margin)

    def _excursion_filter_expr(self, side: str) -> pl.Expr:
        expr = pl.lit(True)
        if self.max_vma_excursion_pct is not None:
            if side == "long":
                excursion = pl.max_horizontal(pl.col(self.vma_col) - pl.col("low"), pl.lit(0.0))
            else:
                excursion = pl.max_horizontal(pl.col("high") - pl.col(self.vma_col), pl.lit(0.0))
            expr = expr & (
                pl.when(pl.col(self.vma_col) > 0)
                .then(excursion / pl.col(self.vma_col))
                .otherwise(None)
                <= float(self.max_vma_excursion_pct)
            )
        # ATR-normalized excursion is intentionally dormant until the data model has
        # a canonical ATR column for this path.
        return expr

    def _long_reclaim_margin_expr(self) -> pl.Expr:
        return pl.col("close") > pl.col(self.vma_col) * (1.0 + self.min_reclaim_margin_pct)

    def _short_reclaim_margin_expr(self) -> pl.Expr:
        return pl.col("close") < pl.col(self.vma_col) * (1.0 - self.min_reclaim_margin_pct)

    def _long_close_location_expr(self) -> pl.Expr:
        if self.min_close_location is None:
            return pl.lit(True)
        return pl.col("close_location") >= float(self.min_close_location)

    def _short_close_location_expr(self) -> pl.Expr:
        if self.min_close_location is None:
            return pl.lit(True)
        return pl.col("close_location") <= (1.0 - float(self.min_close_location))

    def _volume_filter_expr(self) -> pl.Expr:
        if not self.use_volume_filter:
            return pl.lit(True)
        relvol = pl.col(f"relative_volume_{self.relative_volume_period}")
        expr = pl.lit(True)
        if self.min_relative_volume is not None:
            expr = expr & (relvol >= float(self.min_relative_volume))
        if self.max_panic_relative_volume is not None:
            expr = expr & (relvol <= float(self.max_panic_relative_volume))
        return expr.fill_null(False)

    @staticmethod
    def _minutes_to_time(total_minutes: int) -> time:
        """Convert total minutes since midnight to a time object."""
        return time(total_minutes // 60, total_minutes % 60)

    def __repr__(self) -> str:
        return (
            f"<MarketImpulseStrategy window={self._entry_start}-{self._entry_end} "
            f"vma={self.vma_col} regime={self.regime_col}>"
        )


def _resolve_vma_length(*, vma_length: int | None, vma_col: str | None) -> int:
    if vma_length is not None:
        return int(vma_length)
    if vma_col:
        match = re.fullmatch(r"vma_(\d+)", vma_col)
        if match:
            return int(match.group(1))
    return 10


def _resolve_regime_timeframe(*, regime_timeframe: str, regime_col: str | None) -> str:
    if regime_col and regime_timeframe == "5m":
        match = re.fullmatch(r"impulse_regime_(.+)", regime_col)
        if match:
            return match.group(1)
    return regime_timeframe


def _normalize_vwma_periods(vwma_periods: Any) -> tuple[int, int, int]:
    if isinstance(vwma_periods, str):
        parts = [part.strip() for part in vwma_periods.split(",") if part.strip()]
        return validate_vwma_periods(tuple(int(part) for part in parts))
    return validate_vwma_periods(tuple(vwma_periods))


def _normalize_entry_mode(entry_mode: str) -> str:
    normalized = str(entry_mode).strip()
    aliases = {
        "MI Shallow Spring": "same_bar_shallow_reclaim",
        "MI Second Touch": "delayed_reclaim",
        "MI High Close Reclaim": "close_location_reclaim",
        "MI Push Through": "continuation_confirmation",
        "Market Impulse (Cross & Reclaim)": BASELINE_ENTRY_MODE,
    }
    normalized = aliases.get(normalized, normalized)
    legal = {BASELINE_ENTRY_MODE, *DESCENDANT_ENTRY_MODES}
    if normalized not in legal:
        raise ValueError(f"Unsupported Market Impulse entry_mode: {entry_mode!r}")
    return normalized


def _normalize_confirmation_type(confirmation_type: str) -> str:
    normalized = str(confirmation_type).strip()
    if normalized not in CONFIRMATION_TYPES:
        raise ValueError(
            f"Unsupported confirmation_type {confirmation_type!r}; "
            f"expected one of {CONFIRMATION_TYPES!r}"
        )
    return normalized
