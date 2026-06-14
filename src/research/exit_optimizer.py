"""Underlying-anchored thesis exit optimization for validated research candidates.

Called after M5 execution mapping for candidates that pass. Evaluates a
policy grid tuned for same-session options use, including fast scalp exits,
fixed reward-risk, time stops, and trailing exits. It selects the best policy
with a short-options-aware objective and writes per-candidate exit
optimization artifacts to the run directory.

Output fields formerly fed into legacy Strategy_Catalog playbook_summary_json:
    thesis_exit_policy     e.g. "fixed_rr_underlying"
    thesis_exit_params     e.g. {"stop_loss_underlying_pct": 0.0035, ...}
    catastrophe_exit_params e.g. {"hard_flat_time_et": "15:55", ...}
    has_optimized_thesis_exit: true
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import UTC, date, datetime, time as dt_time, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import polars as pl
from pydantic import BaseModel, Field

from src.oracle.trade_simulator import (
    AtrTrailingExitPolicy,
    FixedPercentRewardRiskExitPolicy,
    HoldToEodExitPolicy,
    MovingAverageCrossoverExitPolicy,
    MovingAverageTrailingExitPolicy,
    TimeStopExitPolicy,
    TradeSimulator,
    VmaTrailingExitPolicy,
)
from src.research.exit_profiles import EXIT_PROFILES
from src.research.strategy_keys import to_strategy_key
from src.strategy.base import BaseStrategy

# The underlying optimizer is the WRONG judge for option-exit profiles (they're
# scored on the option path via src/research/option_translation.py). Exclude them
# from underlying candidate selection by default so production M-runs are
# unchanged; enable for research with MALA_EXIT_PROFILE_CANDIDATES=1.
INCLUDE_PROFILE_CANDIDATES = os.environ.get("MALA_EXIT_PROFILE_CANDIDATES", "0") == "1"


# ── Default catastrophe exit (used when no strategy-specific one is set) ─────

DEFAULT_CATASTROPHE_EXIT: dict[str, Any] = {
    "hard_flat_time_et": "15:55",
    "stop_loss_pct": 0.35,
}

MARKET_SESSION_MINUTES = 390.0
LATE_DAY_THETA_START_ET = dt_time(15, 0)
EASTERN_TZ = ZoneInfo("America/New_York")


# ── Models ────────────────────────────────────────────────────────────────────

class ExitPolicyEvaluation(BaseModel):
    policy_name: str
    thesis_exit_anchor: str = "underlying"
    thesis_exit_policy: str
    thesis_exit_params: dict[str, Any] = Field(default_factory=dict)
    metrics: dict[str, Any] = Field(default_factory=dict)


class ExitOptimizationResult(BaseModel):
    generated_at: str
    strategy_key: str
    symbol: str
    direction: str
    selection_metric: str = "option_adjusted_expectancy_pct"
    selection_slice: dict[str, str]
    selected_policy_name: str
    thesis_exit_anchor: str = "underlying"
    thesis_exit_policy: str
    thesis_exit_params: dict[str, Any] = Field(default_factory=dict)
    catastrophe_exit_anchor: str = "option_premium"
    catastrophe_exit_params: dict[str, Any] = Field(default_factory=dict)
    selected_metrics: dict[str, Any] = Field(default_factory=dict)
    candidate_policies: list[ExitPolicyEvaluation] = Field(default_factory=list)


# ── Policy grids per strategy ─────────────────────────────────────────────────
# (stop_loss_pct, reward_multiple) pairs — ordered from conservative to aggressive

_FIXED_RR_GRID: dict[str, list[tuple[float, float]]] = {
    "market_impulse":              [(0.0035, 1.5), (0.005, 2.0), (0.0075, 2.0)],
    "jerk_pivot_momentum":         [(0.0025, 1.25), (0.0035, 1.5), (0.005, 1.75)],
    "elastic_band_reversion":      [(0.0035, 1.0), (0.005, 1.5), (0.0075, 2.0)],
    "opening_drive_classifier":    [(0.0035, 1.25), (0.005, 1.5), (0.0075, 2.0)],
    "compression_expansion_breakout": [(0.003, 1.5), (0.005, 2.0), (0.0075, 2.5)],
}

_DEFAULT_FIXED_RR_GRID: list[tuple[float, float]] = [
    (0.0035, 1.5), (0.005, 2.0), (0.0075, 2.0),
]

_RUNNER_FIXED_RR_GRID: dict[str, list[tuple[float, float]]] = {
    "market_impulse": [(0.0075, 3.0), (0.01, 3.0), (0.01, 4.0)],
    "jerk_pivot_momentum": [(0.005, 2.5), (0.0075, 3.0), (0.01, 3.0)],
    "compression_expansion_breakout": [(0.0075, 3.0), (0.01, 3.0), (0.01, 4.0)],
    "opening_drive_classifier": [(0.0075, 2.5), (0.01, 3.0)],
}

_ATR_GRID: dict[str, list[tuple[str, float]]] = {
    "default": [("atr_14_exit", 1.5), ("atr_14_exit", 2.0), ("atr_30_exit", 2.0)],
    "market_impulse": [("atr_14_exit", 1.5), ("atr_14_exit", 2.0), ("atr_30_exit", 2.5)],
    "compression_expansion_breakout": [("atr_14_exit", 2.0), ("atr_30_exit", 2.5), ("atr_30_exit", 3.0)],
    "jerk_pivot_momentum": [("atr_14_exit", 1.5), ("atr_14_exit", 2.0), ("atr_30_exit", 2.5)],
}

_MA_TRAILING_COLS = ["ema_8_exit", "ema_12_exit", "ema_20_exit", "ema_50_exit"]
_MA_CROSSOVER_PAIRS = [("ema_8_exit", "ema_20_exit"), ("ema_12_exit", "ema_50_exit")]
_TIME_STOP_GRID = [
    dt_time(10, 15),
    dt_time(10, 30),
    dt_time(11, 0),
    dt_time(11, 30),
    dt_time(13, 0),
    dt_time(14, 30),
    dt_time(15, 55),
]

_SCALP_FIXED_RR_GRID: dict[str, list[tuple[float, float]]] = {
    "default": [(0.0015, 1.0), (0.0025, 1.0), (0.0025, 1.5), (0.0035, 1.0)],
    "market_impulse": [(0.0015, 1.0), (0.0025, 1.0), (0.0025, 1.5), (0.0035, 1.0)],
    "opening_drive_classifier": [(0.0025, 1.0), (0.0035, 1.0), (0.0035, 1.5)],
    "jerk_pivot_momentum": [(0.0025, 1.0), (0.0035, 1.0), (0.0035, 1.5)],
    "elastic_band_reversion": [(0.0015, 1.0), (0.0025, 1.0), (0.0035, 1.0)],
}


@dataclass(frozen=True, slots=True)
class _PolicyCandidate:
    name: str
    thesis_exit_policy: str
    thesis_exit_params: dict[str, Any]
    simulator: TradeSimulator


# ── Core optimizer ────────────────────────────────────────────────────────────

def optimize_underlying_exit(
    *,
    strategy_key: str,
    symbol: str,
    direction: str,
    strategy: BaseStrategy,
    enriched_frame: pl.DataFrame,
    holdout_start: date,
    holdout_end: date,
    catastrophe_exit_params: dict[str, Any] | None = None,
    entry_delay_bars: int = 0,
    min_hold_bars: int = 0,
    cooldown_bars_after_signal: int = 0,
) -> ExitOptimizationResult | None:
    """Evaluate exit policies on the holdout window. Returns the best."""
    if enriched_frame.is_empty():
        return None
    canonical_strategy_key = to_strategy_key(strategy_key)
    if direction.lower() not in {"long", "short"}:
        return None

    signal_frame = _with_exit_policy_features(strategy.generate_signals(enriched_frame.clone()))
    filtered = _holdout_signal_frame(signal_frame, direction, holdout_start, holdout_end)
    if filtered.is_empty():
        return None

    candidates = _policy_candidates(
        strategy_key=canonical_strategy_key,
        strategy=strategy,
        entry_delay_bars=entry_delay_bars,
        min_hold_bars=min_hold_bars,
        cooldown_bars_after_signal=cooldown_bars_after_signal,
    )
    evaluations: list[ExitPolicyEvaluation] = []
    best: ExitPolicyEvaluation | None = None

    for candidate in candidates:
        try:
            result = candidate.simulator.simulate(filtered)
        except ValueError:
            continue
        metrics = {
            "trade_count":    int(result.total_trades),
            "win_rate":       float(result.win_rate),
            "expectancy":     float(result.expectancy),
            "profit_factor":  float(result.profit_factor),
            "total_pnl":      float(result.total_pnl),
            "avg_winner":     float(result.avg_winner),
            "avg_loser":      float(result.avg_loser),
            "avg_bars_held":  _avg_bars_held(result),
            "median_bars_held": _median_bars_held(result),
            "target_hit_rate": _exit_reason_rate(result, "take_profit"),
            "stop_loss_rate": _exit_reason_rate(result, "stop_loss"),
            "pnl_per_bar":    _pnl_per_bar(result),
        }
        metrics.update(_option_adjusted_metrics(result))
        if metrics["trade_count"] <= 0:
            continue
        evaluation = ExitPolicyEvaluation(
            policy_name=candidate.name,
            thesis_exit_policy=candidate.thesis_exit_policy,
            thesis_exit_params=candidate.thesis_exit_params,
            metrics=metrics,
        )
        evaluations.append(evaluation)
        if best is None or _sort_key(evaluation) > _sort_key(best):
            best = evaluation

    if best is None:
        return None

    cat_params = dict(catastrophe_exit_params or DEFAULT_CATASTROPHE_EXIT)

    return ExitOptimizationResult(
        generated_at=datetime.now(UTC).isoformat(),
        strategy_key=canonical_strategy_key,
        symbol=symbol.upper(),
        direction=direction,
        selection_slice={
            "holdout_start": holdout_start.isoformat(),
            "holdout_end":   holdout_end.isoformat(),
            "entry_delay_bars": str(max(0, int(entry_delay_bars))),
            "min_hold_bars": str(max(0, int(min_hold_bars))),
            "cooldown_bars_after_signal": str(max(0, int(cooldown_bars_after_signal))),
        },
        selected_policy_name=best.policy_name,
        thesis_exit_policy=best.thesis_exit_policy,
        thesis_exit_params=best.thesis_exit_params,
        catastrophe_exit_params=cat_params,
        selected_metrics=best.metrics,
        candidate_policies=evaluations,
    )


def write_exit_optimization_result(
    result: ExitOptimizationResult, *, path: str | Path
) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        json.dumps(result.model_dump(mode="json"), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return target


# ── Private helpers ───────────────────────────────────────────────────────────

def _holdout_signal_frame(
    signal_frame: pl.DataFrame,
    direction: str,
    holdout_start: date,
    holdout_end: date,
) -> pl.DataFrame:
    filtered = signal_frame.filter(
        pl.col("timestamp").dt.date().is_between(holdout_start, holdout_end, closed="both")
    )
    if filtered.is_empty():
        return filtered
    if "signal_direction" in filtered.columns:
        d = direction.lower()
        filtered = filtered.with_columns(
            (
                pl.col("signal")
                & (pl.col("signal_direction").str.to_lowercase() == d)
            ).fill_null(False).alias("signal")
        )
    return filtered


def _policy_candidates(
    *,
    strategy_key: str,
    strategy: BaseStrategy,
    entry_delay_bars: int = 0,
    min_hold_bars: int = 0,
    cooldown_bars_after_signal: int = 0,
    include_profiles: bool = INCLUDE_PROFILE_CANDIDATES,
) -> list[_PolicyCandidate]:
    candidates: list[_PolicyCandidate] = []

    candidates.append(
        _PolicyCandidate(
            name="hold_to_eod_underlying",
            thesis_exit_policy="hold_to_eod_underlying",
            thesis_exit_params={},
            simulator=TradeSimulator(
                entry_delay_bars=entry_delay_bars,
                min_hold_bars=min_hold_bars,
                cooldown_bars_after_signal=cooldown_bars_after_signal,
                exit_policy=HoldToEodExitPolicy(),
            ),
        )
    )

    for exit_time in _TIME_STOP_GRID:
        time_label = exit_time.strftime("%H%M")
        candidates.append(
            _PolicyCandidate(
                name=f"time_stop_underlying:{time_label}",
                thesis_exit_policy="time_stop_underlying",
                thesis_exit_params={"exit_time_et": exit_time.strftime("%H:%M")},
                simulator=TradeSimulator(
                    entry_delay_bars=entry_delay_bars,
                    min_hold_bars=min_hold_bars,
                    cooldown_bars_after_signal=cooldown_bars_after_signal,
                    exit_policy=TimeStopExitPolicy(
                        exit_time=exit_time,
                        policy_name="time_stop_underlying",
                    ),
                ),
            )
        )

    for atr_col, atr_multiple in _ATR_GRID.get(strategy_key, _ATR_GRID["default"]):
        candidates.append(
            _PolicyCandidate(
                name=f"atr_trailing_underlying:{atr_col}x{atr_multiple:.2f}",
                thesis_exit_policy="atr_trailing_underlying",
                thesis_exit_params={"atr_col": atr_col, "atr_multiple": atr_multiple},
                simulator=TradeSimulator(
                    entry_delay_bars=entry_delay_bars,
                    min_hold_bars=min_hold_bars,
                    cooldown_bars_after_signal=cooldown_bars_after_signal,
                    exit_policy=AtrTrailingExitPolicy(
                        atr_col=atr_col,
                        atr_multiple=atr_multiple,
                        policy_name="atr_trailing_underlying",
                    ),
                ),
            )
        )

    for ma_col in _MA_TRAILING_COLS:
        candidates.append(
            _PolicyCandidate(
                name=f"ma_trailing_underlying:{ma_col}",
                thesis_exit_policy="ma_trailing_underlying",
                thesis_exit_params={"ma_col": ma_col},
                simulator=TradeSimulator(
                    entry_delay_bars=entry_delay_bars,
                    min_hold_bars=min_hold_bars,
                    cooldown_bars_after_signal=cooldown_bars_after_signal,
                    exit_policy=MovingAverageTrailingExitPolicy(
                        ma_col=ma_col,
                        policy_name="ma_trailing_underlying",
                    ),
                ),
            )
        )

    for fast_col, slow_col in _MA_CROSSOVER_PAIRS:
        candidates.append(
            _PolicyCandidate(
                name=f"ma_crossover_underlying:{fast_col}>{slow_col}",
                thesis_exit_policy="ma_crossover_underlying",
                thesis_exit_params={"fast_ma_col": fast_col, "slow_ma_col": slow_col},
                simulator=TradeSimulator(
                    entry_delay_bars=entry_delay_bars,
                    min_hold_bars=min_hold_bars,
                    cooldown_bars_after_signal=cooldown_bars_after_signal,
                    exit_policy=MovingAverageCrossoverExitPolicy(
                        fast_ma_col=fast_col,
                        slow_ma_col=slow_col,
                        policy_name="ma_crossover_underlying",
                    ),
                ),
            )
        )

    # VMA trailing (market_impulse only, where vma_col exists)
    if strategy_key == "market_impulse":
        vma_col = getattr(strategy, "vma_col", "vma_10")
        candidates.append(
            _PolicyCandidate(
                name=f"trailing_vma_underlying:{vma_col}",
                thesis_exit_policy="trailing_vma_underlying",
                thesis_exit_params={"vma_col": vma_col},
                simulator=TradeSimulator(
                    entry_delay_bars=entry_delay_bars,
                    min_hold_bars=min_hold_bars,
                    cooldown_bars_after_signal=cooldown_bars_after_signal,
                    exit_policy=VmaTrailingExitPolicy(
                        vma_col=vma_col, policy_name="trailing_vma_underlying"
                    )
                ),
            )
        )

    # Fast fixed reward-risk grid for short-term options. These intentionally
    # test smaller underlying moves because near-DTE options often need quick
    # management before theta/spread noise overwhelms the thesis.
    for stop_loss_pct, reward_multiple in _SCALP_FIXED_RR_GRID.get(
        strategy_key, _SCALP_FIXED_RR_GRID["default"]
    ):
        name = f"fixed_rr_underlying_scalp:{stop_loss_pct:.4f}x{reward_multiple:.2f}"
        candidates.append(
            _PolicyCandidate(
                name=name,
                thesis_exit_policy="fixed_rr_underlying",
                thesis_exit_params={
                    "stop_loss_underlying_pct": stop_loss_pct,
                    "take_profit_underlying_r_multiple": reward_multiple,
                },
                simulator=TradeSimulator(
                    entry_delay_bars=entry_delay_bars,
                    min_hold_bars=min_hold_bars,
                    cooldown_bars_after_signal=cooldown_bars_after_signal,
                    exit_policy=FixedPercentRewardRiskExitPolicy(
                        stop_loss_pct=stop_loss_pct,
                        reward_multiple=reward_multiple,
                    )
                ),
            )
        )

    # Fixed reward-risk grid
    grid = [
        *_FIXED_RR_GRID.get(strategy_key, _DEFAULT_FIXED_RR_GRID),
        *_RUNNER_FIXED_RR_GRID.get(strategy_key, []),
    ]
    for stop_loss_pct, reward_multiple in grid:
        name = f"fixed_rr_underlying:{stop_loss_pct:.4f}x{reward_multiple:.2f}"
        candidates.append(
            _PolicyCandidate(
                name=name,
                thesis_exit_policy="fixed_rr_underlying",
                thesis_exit_params={
                    "stop_loss_underlying_pct": stop_loss_pct,
                    "take_profit_underlying_r_multiple": reward_multiple,
                },
                simulator=TradeSimulator(
                    entry_delay_bars=entry_delay_bars,
                    min_hold_bars=min_hold_bars,
                    cooldown_bars_after_signal=cooldown_bars_after_signal,
                    exit_policy=FixedPercentRewardRiskExitPolicy(
                        stop_loss_pct=stop_loss_pct,
                        reward_multiple=reward_multiple,
                    )
                ),
            )
        )
    # Operator exit profiles (Wave 1) — OFF by default (INCLUDE_PROFILE_CANDIDATES):
    # the underlying path is the wrong judge for option-exit profiles, which are
    # scored on the option path via src/research/option_translation.py.
    if include_profiles:
        for profile in EXIT_PROFILES.values():
            policy_label = f"profile:{profile.name.lower()}"
            candidates.append(
                _PolicyCandidate(
                    name=policy_label,
                    thesis_exit_policy=policy_label,
                    thesis_exit_params=profile.thesis_exit_params(),
                    simulator=TradeSimulator(
                        entry_delay_bars=entry_delay_bars,
                        min_hold_bars=min_hold_bars,
                        cooldown_bars_after_signal=cooldown_bars_after_signal,
                        exit_policy=profile.build_policy(),
                    ),
                )
            )

    return candidates


def _with_exit_policy_features(frame: pl.DataFrame) -> pl.DataFrame:
    if frame.is_empty():
        return frame
    required = {"high", "low", "close"}
    if not required.issubset(frame.columns):
        return frame
    prev_close = pl.col("close").shift(1)
    true_range = pl.max_horizontal(
        pl.col("high") - pl.col("low"),
        (pl.col("high") - prev_close).abs(),
        (pl.col("low") - prev_close).abs(),
    )
    return (
        frame
        .with_columns(true_range.alias("_exit_true_range"))
        .with_columns([
            pl.col("_exit_true_range").rolling_mean(window_size=14).alias("atr_14_exit"),
            pl.col("_exit_true_range").rolling_mean(window_size=30).alias("atr_30_exit"),
            pl.col("close").ewm_mean(span=8, adjust=False).alias("ema_8_exit"),
            pl.col("close").ewm_mean(span=12, adjust=False).alias("ema_12_exit"),
            pl.col("close").ewm_mean(span=20, adjust=False).alias("ema_20_exit"),
            pl.col("close").ewm_mean(span=50, adjust=False).alias("ema_50_exit"),
        ])
        .drop("_exit_true_range")
    )


def _sort_key(e: ExitPolicyEvaluation) -> tuple[float, float, float, float, float, float]:
    m = e.metrics
    median_minutes = _metric(m, "median_minutes_held", default=float("inf"))
    return (
        _metric(m, "option_adjusted_expectancy_pct"),
        _metric(m, "pnl_pct_per_minute"),
        _metric(m, "target_hit_within_30_minutes"),
        -median_minutes,
        _metric(m, "profit_factor"),
        _metric(m, "win_rate"),
    )


def _metric(metrics: dict[str, Any], key: str, *, default: float = float("-inf")) -> float:
    value = metrics.get(key)
    if value is None:
        return default
    return float(value)


def _avg_bars_held(result: Any) -> float:
    if not result.trades:
        return 0.0
    return round(sum(float(trade.bars_held) for trade in result.trades) / len(result.trades), 4)


def _median_bars_held(result: Any) -> float:
    if not result.trades:
        return 0.0
    values = sorted(float(trade.bars_held) for trade in result.trades)
    mid = len(values) // 2
    if len(values) % 2:
        return round(values[mid], 4)
    return round((values[mid - 1] + values[mid]) / 2.0, 4)


def _exit_reason_rate(result: Any, prefix: str) -> float:
    if not result.trades:
        return 0.0
    count = sum(1 for trade in result.trades if str(trade.exit_reason).startswith(prefix))
    return round(count / len(result.trades), 6)


def _pnl_per_bar(result: Any) -> float:
    bars = sum(max(1, int(trade.bars_held)) for trade in result.trades)
    if not result.trades or bars <= 0:
        return 0.0
    return round(float(result.total_pnl) / bars, 6)


def _option_adjusted_metrics(result: Any) -> dict[str, Any]:
    trades = list(result.trades or [])
    if not trades:
        return {
            "expectancy_pct": 0.0,
            "avg_win_pct": 0.0,
            "avg_loss_pct_abs": 0.0,
            "pnl_pct_per_minute": 0.0,
            "pnl_pct_per_bar": 0.0,
            "median_minutes_held": 0.0,
            "avg_minutes_held": 0.0,
            "target_hit_within_15_minutes": 0.0,
            "target_hit_within_30_minutes": 0.0,
            "stop_loss_within_15_minutes": 0.0,
            "theta_penalty_pct": 0.0,
            "option_adjusted_expectancy_pct": 0.0,
            "recommended_dte_min": 0,
            "recommended_dte_max": 3,
            "option_exit_quality": "no_trades",
            "option_trade_ready": False,
        }

    minutes = [_trade_minutes_held(trade) for trade in trades]
    median_minutes = _median(minutes)
    avg_minutes = sum(minutes) / len(minutes)
    pnl_pcts = [_trade_pnl_pct(trade) for trade in trades]
    win_pcts = [value for value in pnl_pcts if value > 0]
    loss_pcts = [abs(value) for value in pnl_pcts if value < 0]
    total_pnl_pct = sum(pnl_pcts)
    total_minutes = sum(max(value, 1e-9) for value in minutes)
    total_bars = sum(max(1, int(trade.bars_held)) for trade in trades)
    dte_min, dte_max, base_penalty = _dte_bucket_for_minutes(median_minutes)
    theta_penalty = _theta_penalty_pct(trades, dte_min=dte_min, base_penalty_pct=base_penalty)
    expectancy_pct = total_pnl_pct / len(trades)
    option_adjusted = expectancy_pct - theta_penalty
    trade_ready = bool(option_adjusted > 0 and dte_max <= 14)

    metrics: dict[str, Any] = {
        "expectancy_pct": round(expectancy_pct, 6),
        "avg_win_pct": round(sum(win_pcts) / len(win_pcts), 6) if win_pcts else 0.0,
        "avg_loss_pct_abs": round(sum(loss_pcts) / len(loss_pcts), 6) if loss_pcts else 0.0,
        "pnl_pct_per_minute": round(total_pnl_pct / total_minutes, 8),
        "pnl_pct_per_bar": round(total_pnl_pct / total_bars, 8) if total_bars > 0 else 0.0,
        "median_minutes_held": round(median_minutes, 4),
        "avg_minutes_held": round(avg_minutes, 4),
        "target_hit_within_15_minutes": _exit_reason_within_minutes_rate(result, "take_profit", 15),
        "target_hit_within_30_minutes": _exit_reason_within_minutes_rate(result, "take_profit", 30),
        "stop_loss_within_15_minutes": _exit_reason_within_minutes_rate(result, "stop_loss", 15),
        "theta_penalty_pct": round(theta_penalty, 6),
        "option_adjusted_expectancy_pct": round(option_adjusted, 6),
        "recommended_dte_min": dte_min,
        "recommended_dte_max": dte_max,
        "option_exit_quality": _option_exit_quality(
            option_adjusted_expectancy_pct=option_adjusted,
            recommended_dte_max=dte_max,
            median_minutes_held=median_minutes,
        ),
        "option_trade_ready": trade_ready,
    }
    return metrics


def _trade_pnl_pct(trade: Any) -> float:
    entry_price = float(getattr(trade, "entry_price", 0.0) or 0.0)
    if entry_price <= 0:
        return 0.0
    return float(getattr(trade, "pnl", 0.0) or 0.0) / entry_price * 100.0


def _trade_minutes_held(trade: Any) -> float:
    entry = getattr(trade, "entry_time", None)
    exit_ = getattr(trade, "exit_time", None)
    delta = _datetime_delta(exit_, entry)
    if delta is not None:
        return max(0.0, delta.total_seconds() / 60.0)
    return float(max(1, int(getattr(trade, "bars_held", 1) or 1)))


def _datetime_delta(exit_: Any, entry: Any) -> timedelta | None:
    if not isinstance(entry, datetime) or not isinstance(exit_, datetime):
        return None
    try:
        return exit_ - entry
    except TypeError:
        return _as_utc_datetime(exit_) - _as_utc_datetime(entry)


def _as_utc_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _median(values: list[float]) -> float:
    if not values:
        return 0.0
    sorted_values = sorted(values)
    mid = len(sorted_values) // 2
    if len(sorted_values) % 2:
        return float(sorted_values[mid])
    return (float(sorted_values[mid - 1]) + float(sorted_values[mid])) / 2.0


def _exit_reason_within_minutes_rate(result: Any, prefix: str, minutes: float) -> float:
    trades = list(result.trades or [])
    if not trades:
        return 0.0
    count = sum(
        1
        for trade in trades
        if str(trade.exit_reason).startswith(prefix)
        and _trade_minutes_held(trade) <= minutes
    )
    return round(count / len(trades), 6)


def _dte_bucket_for_minutes(median_minutes_held: float) -> tuple[int, int, float]:
    if median_minutes_held <= 30:
        return 0, 3, 1.2
    if median_minutes_held <= 120:
        return 3, 7, 0.6
    if median_minutes_held <= MARKET_SESSION_MINUTES:
        return 7, 14, 0.3
    return 14, 21, 0.15


def _theta_penalty_pct(trades: list[Any], *, dte_min: int, base_penalty_pct: float) -> float:
    if not trades:
        return 0.0
    weighted_minutes = [
        _theta_weighted_minutes_held(trade, apply_late_day=dte_min == 0)
        for trade in trades
    ]
    return base_penalty_pct * _median(weighted_minutes) / MARKET_SESSION_MINUTES


def _theta_weighted_minutes_held(trade: Any, *, apply_late_day: bool) -> float:
    minutes = _trade_minutes_held(trade)
    if not apply_late_day:
        return minutes
    return minutes + _late_day_minutes_held(trade)


def _late_day_minutes_held(trade: Any) -> float:
    entry = getattr(trade, "entry_time", None)
    exit_ = getattr(trade, "exit_time", None)
    if not isinstance(entry, datetime) or not isinstance(exit_, datetime):
        return 0.0
    start = _to_et_datetime(entry)
    end = _to_et_datetime(exit_)
    if end <= start:
        return 0.0

    total = 0.0
    current_date = start.date()
    while current_date <= end.date():
        late_start = datetime.combine(current_date, LATE_DAY_THETA_START_ET, tzinfo=EASTERN_TZ)
        day_start = max(start, late_start)
        day_end = min(end, datetime.combine(current_date, dt_time(23, 59, 59), tzinfo=EASTERN_TZ))
        if day_end > day_start:
            total += (day_end - day_start).total_seconds() / 60.0
        current_date = current_date + timedelta(days=1)
    return total


def _to_et_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC).astimezone(EASTERN_TZ)
    return value.astimezone(EASTERN_TZ)


def _option_exit_quality(
    *,
    option_adjusted_expectancy_pct: float,
    recommended_dte_max: int,
    median_minutes_held: float,
) -> str:
    if option_adjusted_expectancy_pct <= 0:
        return "negative_after_theta"
    if recommended_dte_max > 14:
        return "too_slow_for_short_dated_options"
    if median_minutes_held <= 30:
        return "fast_scalp"
    if median_minutes_held <= 120:
        return "fast_intraday"
    return "slow_intraday"
