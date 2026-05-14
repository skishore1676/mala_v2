"""Intraday mean-reversion-at-extremes playbook strategy.

This strategy is intentionally a playbook event constructor, not a live
execution adapter. It turns Suman's early-session overextension/reversal-range
setup into deterministic signal events that the research layer can slice into a
conditional surface.
"""

from __future__ import annotations

from datetime import time
from typing import Any

import polars as pl
from loguru import logger

from src.newton.transforms import jerk_column_name, validate_periods_back, velocity_column_name
from src.research.models import DomainSpec, ObjectiveSpec, ParameterSpec, StrategySearchSpec
from src.strategy.base import BaseStrategy, coerce_time
from src.time_utils import et_time_expr


STRATEGY_NAME = "Intraday Mean Reversion at Extremes"
PLAYBOOK_ID = "mean-reversion-at-extremes-intraday"

STRETCH_SOURCES = ("opening_vwap", "prior_close_atr", "vpoc_4h")
VELOCITY_FILTERS = ("no_filter", "aligned", "climactic", "non_climactic")
STAGE_FILTERS = ("no_filter", "bullish", "bearish", "neutral")
GAP_STATE_FILTERS = (
    "no_filter",
    "gap_up_large",
    "gap_up_small",
    "flat",
    "gap_down_small",
    "gap_down_large",
)
STOP_FAMILIES = (
    "reversal_extreme",
    "reversal_midpoint",
    "immediate_entry_bar_failure",
)
EXIT_FAMILIES = (
    "fixed_1r",
    "fixed_1_5r",
    "fixed_2r",
    "vwap_return",
    "partial_retrace_50",
    "time_stop",
)


class IntradayMeanReversionStrategy(BaseStrategy):
    """Early-session extreme fade with reversal-range confirmation."""

    def __init__(
        self,
        *,
        entry_window_start: time | str = "09:30",
        entry_window_end: time | str = "10:15",
        stretch_source: str = "opening_vwap",
        stretch_threshold: float = 2.0,
        z_score_window: int = 30,
        reversal_range_minutes: int = 5,
        confirming_bars: int = 1,
        velocity_periods_back: int = 5,
        velocity_filter: str = "no_filter",
        velocity_atr_threshold: float = 0.10,
        stage_filter: str = "no_filter",
        gap_state_filter: str = "no_filter",
        use_jerk_confirmation: bool = True,
        jerk_periods_back: int | None = None,
        relative_volume_period: int = 20,
        relative_volume_threshold: float | None = None,
        stop_family: str = "reversal_extreme",
        exit_family: str = "fixed_1r",
        allow_long: bool = True,
        allow_short: bool = True,
    ) -> None:
        self.entry_window_start = coerce_time(entry_window_start)
        self.entry_window_end = coerce_time(entry_window_end)
        if self.entry_window_start >= self.entry_window_end:
            raise ValueError("entry_window_start must be before entry_window_end")

        self.stretch_source = _require_one("stretch_source", stretch_source, STRETCH_SOURCES)
        self.stretch_threshold = float(stretch_threshold)
        if self.stretch_threshold <= 0:
            raise ValueError("stretch_threshold must be positive")
        self.z_score_window = max(5, int(z_score_window))
        self.reversal_range_minutes = max(2, int(reversal_range_minutes))
        self.confirming_bars = max(1, int(confirming_bars))
        self.velocity_periods_back = validate_periods_back(velocity_periods_back)
        self.velocity_filter = _require_one("velocity_filter", velocity_filter, VELOCITY_FILTERS)
        self.velocity_atr_threshold = abs(float(velocity_atr_threshold))
        self.stage_filter = _require_one("stage_filter", stage_filter, STAGE_FILTERS)
        self.gap_state_filter = _require_one(
            "gap_state_filter",
            gap_state_filter,
            GAP_STATE_FILTERS,
        )
        self.use_jerk_confirmation = bool(use_jerk_confirmation)
        self.jerk_periods_back = validate_periods_back(
            jerk_periods_back if jerk_periods_back is not None else velocity_periods_back
        )
        self.relative_volume_period = max(2, int(relative_volume_period))
        self.relative_volume_threshold = (
            None if relative_volume_threshold is None else float(relative_volume_threshold)
        )
        self.stop_family = _require_one("stop_family", stop_family, STOP_FAMILIES)
        self.exit_family = _require_one("exit_family", exit_family, EXIT_FAMILIES)
        self.allow_long = bool(allow_long)
        self.allow_short = bool(allow_short)

    @property
    def name(self) -> str:
        return STRATEGY_NAME

    @property
    def required_features(self) -> set[str]:
        features = {
            "timestamp",
            "open",
            "high",
            "low",
            "close",
            "opening_vwap",
            "prior_close",
            "daily_atr_14",
            "gap_state",
            "impulse_regime_5m",
            velocity_column_name(self.velocity_periods_back),
        }
        if self.stretch_source == "prior_close_atr":
            features.add("atr_distance_from_prior_close")
        elif self.stretch_source == "vpoc_4h":
            features.add("vpoc_4h")
        else:
            features.add("opening_vwap")
        if self.use_jerk_confirmation:
            features.add(jerk_column_name(self.jerk_periods_back))
        if self.relative_volume_threshold is not None:
            features.add(f"relative_volume_{self.relative_volume_period}")
        return features

    @property
    def feature_requests(self) -> set[str]:
        requests = {
            "opening_vwap",
            "prior_close_atr",
            "market_impulse:5m",
            _kinematic_spec("velocity", self.velocity_periods_back),
        }
        if self.stretch_source == "vpoc_4h":
            requests.add("vpoc")
        if self.use_jerk_confirmation:
            requests.add(_kinematic_spec("jerk", self.jerk_periods_back))
        if self.relative_volume_threshold is not None:
            requests.add(f"relative_volume:{self.relative_volume_period}")
        return requests

    @property
    def evaluation_mode(self) -> str:
        return "directional"

    @property
    def parameter_space(self) -> dict[str, list[Any]]:
        return self.search_spec.parameter_space()

    @property
    def search_spec(self) -> StrategySearchSpec:
        return StrategySearchSpec(
            parameters=[
                ParameterSpec(
                    name="entry_window_end",
                    type="categorical",
                    domain=DomainSpec(values=["09:45", "10:00", "10:15", "11:00"]),
                    default=self.entry_window_end.isoformat(timespec="minutes"),
                    prior_center="10:15",
                ),
                ParameterSpec(
                    name="stretch_source",
                    type="categorical",
                    domain=DomainSpec(values=list(STRETCH_SOURCES)),
                    default=self.stretch_source,
                    prior_center="opening_vwap",
                ),
                ParameterSpec(
                    name="stretch_threshold",
                    type="discrete",
                    domain=DomainSpec(values=[0.75, 1.0, 1.25, 1.5, 2.0, 2.5, 3.0]),
                    default=self.stretch_threshold,
                    prior_center=2.0,
                ),
                ParameterSpec(
                    name="reversal_range_minutes",
                    type="discrete",
                    domain=DomainSpec(values=[5, 15]),
                    default=self.reversal_range_minutes,
                    prior_center=5,
                ),
                ParameterSpec(
                    name="confirming_bars",
                    type="discrete",
                    domain=DomainSpec(values=[1, 2]),
                    default=self.confirming_bars,
                    prior_center=1,
                ),
                ParameterSpec(
                    name="velocity_periods_back",
                    type="discrete",
                    domain=DomainSpec(values=[1, 5, 15]),
                    default=self.velocity_periods_back,
                    prior_center=5,
                ),
                ParameterSpec(
                    name="velocity_filter",
                    type="categorical",
                    domain=DomainSpec(values=list(VELOCITY_FILTERS)),
                    default=self.velocity_filter,
                    prior_center="no_filter",
                ),
                ParameterSpec(
                    name="stage_filter",
                    type="categorical",
                    domain=DomainSpec(values=list(STAGE_FILTERS)),
                    default=self.stage_filter,
                    prior_center="no_filter",
                ),
                ParameterSpec(
                    name="gap_state_filter",
                    type="categorical",
                    domain=DomainSpec(values=list(GAP_STATE_FILTERS)),
                    default=self.gap_state_filter,
                    prior_center="no_filter",
                ),
                ParameterSpec(
                    name="use_jerk_confirmation",
                    type="categorical",
                    domain=DomainSpec(values=[True, False]),
                    default=self.use_jerk_confirmation,
                    prior_center=True,
                ),
                ParameterSpec(
                    name="relative_volume_threshold",
                    type="categorical",
                    domain=DomainSpec(values=[None, 1.0, 1.25, 1.5]),
                    default=self.relative_volume_threshold,
                    prior_center=None,
                ),
                ParameterSpec(
                    name="stop_family",
                    type="categorical",
                    domain=DomainSpec(values=list(STOP_FAMILIES)),
                    default=self.stop_family,
                    prior_center="reversal_extreme",
                ),
                ParameterSpec(
                    name="exit_family",
                    type="categorical",
                    domain=DomainSpec(values=list(EXIT_FAMILIES)),
                    default=self.exit_family,
                    prior_center="fixed_1r",
                ),
            ],
            objective=ObjectiveSpec(
                primary_metric="holdout_expectancy_r",
                minimum_signals=20,
                tie_breakers=["holdout_win_rate", "sample_count"],
            ),
        )

    def strategy_config(self) -> dict[str, Any]:
        return {
            "entry_window_start": self.entry_window_start.isoformat(timespec="minutes"),
            "entry_window_end": self.entry_window_end.isoformat(timespec="minutes"),
            "stretch_source": self.stretch_source,
            "stretch_threshold": self.stretch_threshold,
            "z_score_window": self.z_score_window,
            "reversal_range_minutes": self.reversal_range_minutes,
            "confirming_bars": self.confirming_bars,
            "velocity_periods_back": self.velocity_periods_back,
            "velocity_filter": self.velocity_filter,
            "velocity_atr_threshold": self.velocity_atr_threshold,
            "stage_filter": self.stage_filter,
            "gap_state_filter": self.gap_state_filter,
            "use_jerk_confirmation": self.use_jerk_confirmation,
            "jerk_periods_back": self.jerk_periods_back,
            "relative_volume_period": self.relative_volume_period,
            "relative_volume_threshold": self.relative_volume_threshold,
            "stop_family": self.stop_family,
            "exit_family": self.exit_family,
            "allow_long": self.allow_long,
            "allow_short": self.allow_short,
        }

    def generate_signals(self, df: pl.DataFrame) -> pl.DataFrame:
        missing = self.required_features - set(df.columns)
        if missing:
            raise ValueError(f"Strategy '{self.name}' requires columns: {sorted(missing)}")

        velocity_col = velocity_column_name(self.velocity_periods_back)
        jerk_col = jerk_column_name(self.jerk_periods_back)
        relvol_col = f"relative_volume_{self.relative_volume_period}"
        range_window = self.reversal_range_minutes

        context = (
            df.with_columns(et_time_expr("timestamp").alias("_bar_time"))
            .with_columns(self._stretch_raw_expr().alias("_stretch_raw"))
            .with_columns(self._stretch_value_expr().alias("_stretch_value"))
            .with_columns(
                [
                    pl.col("high")
                    .rolling_max(window_size=range_window, min_samples=1)
                    .shift(1)
                    .alias("_reversal_high"),
                    pl.col("low")
                    .rolling_min(window_size=range_window, min_samples=1)
                    .shift(1)
                    .alias("_reversal_low"),
                    pl.col("_stretch_value")
                    .rolling_max(window_size=range_window, min_samples=1)
                    .shift(1)
                    .alias("_prev_max_stretch"),
                    pl.col("_stretch_value")
                    .rolling_min(window_size=range_window, min_samples=1)
                    .shift(1)
                    .alias("_prev_min_stretch"),
                    pl.col(velocity_col)
                    .rolling_max(window_size=range_window, min_samples=1)
                    .shift(1)
                    .alias("_prev_max_velocity"),
                    pl.col(velocity_col)
                    .rolling_min(window_size=range_window, min_samples=1)
                    .shift(1)
                    .alias("_prev_min_velocity"),
                ]
            )
            .with_columns(
                ((pl.col("_reversal_high") + pl.col("_reversal_low")) / 2.0).alias(
                    "playbook_reversal_midpoint"
                )
            )
            .with_columns(
                [
                    pl.col("_reversal_high").alias("playbook_reversal_high"),
                    pl.col("_reversal_low").alias("playbook_reversal_low"),
                    pl.lit(self.stretch_source).alias("playbook_stretch_source"),
                    pl.col("_stretch_value").alias("playbook_stretch_value"),
                    pl.col("_prev_max_stretch").alias("playbook_prior_max_stretch"),
                    pl.col("_prev_min_stretch").alias("playbook_prior_min_stretch"),
                    self._reference_price_expr().alias("playbook_reference_price"),
                ]
            )
        )

        time_filter = (
            (pl.col("_bar_time") >= self.entry_window_start)
            & (pl.col("_bar_time") <= self.entry_window_end)
        )
        stage_filter = self._stage_filter_expr()
        gap_filter = self._gap_filter_expr()
        volume_filter = (
            pl.lit(True)
            if self.relative_volume_threshold is None
            else (pl.col(relvol_col) >= float(self.relative_volume_threshold)).fill_null(False)
        )

        long_breakout = pl.col("close") > pl.col("_reversal_high")
        short_breakout = pl.col("close") < pl.col("_reversal_low")
        if self.confirming_bars >= 2:
            long_breakout = long_breakout & (pl.col("close").shift(1) > pl.col("_reversal_high").shift(1))
            short_breakout = short_breakout & (pl.col("close").shift(1) < pl.col("_reversal_low").shift(1))

        long_stretch = pl.col("_prev_min_stretch") <= -self.stretch_threshold
        short_stretch = pl.col("_prev_max_stretch") >= self.stretch_threshold
        long_velocity = self._velocity_filter_expr("long")
        short_velocity = self._velocity_filter_expr("short")
        if self.use_jerk_confirmation:
            long_jerk = pl.col(jerk_col) > 0
            short_jerk = pl.col(jerk_col) < 0
        else:
            long_jerk = pl.lit(True)
            short_jerk = pl.lit(True)

        long_signal = (
            pl.lit(self.allow_long)
            & time_filter
            & stage_filter
            & gap_filter
            & volume_filter
            & long_stretch
            & long_breakout
            & long_velocity
            & long_jerk
        )
        short_signal = (
            pl.lit(self.allow_short)
            & time_filter
            & stage_filter
            & gap_filter
            & volume_filter
            & short_stretch
            & short_breakout
            & short_velocity
            & short_jerk
        )

        result = (
            context.with_columns(
                [
                    (long_signal | short_signal).fill_null(False).alias("signal"),
                    pl.when(long_signal)
                    .then(pl.lit("long"))
                    .when(short_signal)
                    .then(pl.lit("short"))
                    .otherwise(pl.lit(None))
                    .alias("signal_direction"),
                    pl.lit(self.entry_window_end.isoformat(timespec="minutes")).alias(
                        "playbook_entry_cutoff_et"
                    ),
                    pl.lit(self.stage_filter).alias("playbook_stage_filter"),
                    pl.lit(self.gap_state_filter).alias("playbook_gap_state_filter"),
                    pl.lit(self.stop_family).alias("playbook_stop_family"),
                    pl.lit(self.exit_family).alias("playbook_exit_family"),
                    pl.lit(self._volume_filter_label()).alias(
                        "playbook_volume_confirmation_filter"
                    ),
                ]
            )
            .drop(["_bar_time", "_stretch_raw"])
        )

        total = result.filter(pl.col("signal")).height
        longs = result.filter(pl.col("signal_direction") == "long").height
        shorts = result.filter(pl.col("signal_direction") == "short").height
        logger.info(
            "Strategy '{}' generated {} signals ({} long, {} short) out of {} bars",
            self.name,
            total,
            longs,
            shorts,
            len(df),
        )
        return result

    def _stretch_raw_expr(self) -> pl.Expr:
        if self.stretch_source == "prior_close_atr":
            return pl.col("atr_distance_from_prior_close")
        reference = self._reference_price_expr()
        return (
            pl.when(reference > 0)
            .then((pl.col("close") - reference) / reference)
            .otherwise(None)
        )

    def _stretch_value_expr(self) -> pl.Expr:
        if self.stretch_source == "prior_close_atr":
            return pl.col("_stretch_raw")
        min_samples = min(5, self.z_score_window)
        mean = pl.col("_stretch_raw").rolling_mean(
            window_size=self.z_score_window,
            min_samples=min_samples,
        )
        std = pl.col("_stretch_raw").rolling_std(
            window_size=self.z_score_window,
            min_samples=min_samples,
        )
        return pl.when(std > 0).then((pl.col("_stretch_raw") - mean) / std).otherwise(None)

    def _reference_price_expr(self) -> pl.Expr:
        if self.stretch_source == "vpoc_4h":
            return pl.col("vpoc_4h")
        if self.stretch_source == "prior_close_atr":
            return pl.col("prior_close")
        return pl.col("opening_vwap")

    def _stage_filter_expr(self) -> pl.Expr:
        if self.stage_filter == "no_filter":
            return pl.lit(True)
        return (pl.col("impulse_regime_5m") == self.stage_filter).fill_null(False)

    def _gap_filter_expr(self) -> pl.Expr:
        if self.gap_state_filter == "no_filter":
            return pl.lit(True)
        return (pl.col("gap_state") == self.gap_state_filter).fill_null(False)

    def _velocity_filter_expr(self, direction: str) -> pl.Expr:
        if self.velocity_filter == "no_filter":
            return pl.lit(True)
        if direction == "long":
            velocity = pl.col("_prev_min_velocity")
            aligned = velocity < 0
        else:
            velocity = pl.col("_prev_max_velocity")
            aligned = velocity > 0
        if self.velocity_filter == "aligned":
            return aligned.fill_null(False)

        normalized = (velocity.abs() / pl.col("daily_atr_14")).fill_null(0.0)
        if self.velocity_filter == "climactic":
            return (aligned & (normalized >= self.velocity_atr_threshold)).fill_null(False)
        return (aligned & (normalized < self.velocity_atr_threshold)).fill_null(False)

    def _volume_filter_label(self) -> str:
        if self.relative_volume_threshold is None:
            return "no_filter"
        return f"rvol_gt_{self.relative_volume_threshold:g}"


def _kinematic_spec(kind: str, periods_back: int) -> str:
    return kind if periods_back == 1 else f"{kind}:{periods_back}"


def _require_one(name: str, value: str, legal: tuple[str, ...]) -> str:
    normalized = str(value).strip()
    if normalized not in legal:
        raise ValueError(f"Unsupported {name} {value!r}; expected one of {legal!r}")
    return normalized
