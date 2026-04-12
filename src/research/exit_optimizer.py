"""Underlying-anchored thesis exit optimization for validated research candidates.

Called after M5 execution mapping for candidates that pass. Evaluates a
small policy grid (fixed reward-risk and VMA trailing where applicable),
selects the best by expectancy, and writes m5_exit_optimization.json to
the run directory.

Output fields fed into Strategy_Catalog's playbook_summary_json:
    thesis_exit_policy     e.g. "fixed_rr_underlying"
    thesis_exit_params     e.g. {"stop_loss_underlying_pct": 0.0035, ...}
    catastrophe_exit_params e.g. {"hard_flat_time_et": "15:55", ...}
    has_optimized_thesis_exit: true
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import polars as pl
from pydantic import BaseModel, Field

from src.oracle.trade_simulator import (
    FixedPercentRewardRiskExitPolicy,
    TradeSimulator,
    VmaTrailingExitPolicy,
)
from src.strategy.base import BaseStrategy


# ── Default catastrophe exit (used when no strategy-specific one is set) ─────

DEFAULT_CATASTROPHE_EXIT: dict[str, Any] = {
    "hard_flat_time_et": "15:55",
    "stop_loss_pct": 0.35,
}


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
    selection_metric: str = "expectancy"
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
    "opening_drive_v2":            [(0.0035, 1.25), (0.005, 1.5), (0.0075, 2.0)],
    "kinematic_ladder":            [(0.003, 1.25), (0.0035, 1.5), (0.005, 2.0)],
    "compression_expansion_breakout": [(0.003, 1.5), (0.005, 2.0), (0.0075, 2.5)],
    "regime_router":               [(0.0035, 1.5), (0.005, 2.0), (0.0075, 2.5)],
}

_DEFAULT_FIXED_RR_GRID: list[tuple[float, float]] = [
    (0.0035, 1.5), (0.005, 2.0), (0.0075, 2.0),
]


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
) -> ExitOptimizationResult | None:
    """Evaluate exit policies on the holdout window. Returns the best."""
    if enriched_frame.is_empty():
        return None

    signal_frame = strategy.generate_signals(enriched_frame.clone())
    filtered = _holdout_signal_frame(signal_frame, direction, holdout_start, holdout_end)
    if filtered.is_empty():
        return None

    candidates = _policy_candidates(strategy_key=strategy_key, strategy=strategy)
    evaluations: list[ExitPolicyEvaluation] = []
    best: ExitPolicyEvaluation | None = None

    for candidate in candidates:
        result = candidate.simulator.simulate(filtered)
        metrics = {
            "trade_count":    int(result.total_trades),
            "win_rate":       float(result.win_rate),
            "expectancy":     float(result.expectancy),
            "profit_factor":  float(result.profit_factor),
            "total_pnl":      float(result.total_pnl),
            "avg_winner":     float(result.avg_winner),
            "avg_loser":      float(result.avg_loser),
        }
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

    cat_params = catastrophe_exit_params or DEFAULT_CATASTROPHE_EXIT

    return ExitOptimizationResult(
        generated_at=datetime.now(UTC).isoformat(),
        strategy_key=strategy_key,
        symbol=symbol.upper(),
        direction=direction,
        selection_slice={
            "holdout_start": holdout_start.isoformat(),
            "holdout_end":   holdout_end.isoformat(),
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
    *, strategy_key: str, strategy: BaseStrategy
) -> list[_PolicyCandidate]:
    candidates: list[_PolicyCandidate] = []

    # VMA trailing (market_impulse only, where vma_col exists)
    if strategy_key == "market_impulse":
        vma_col = getattr(strategy, "vma_col", "vma_10")
        candidates.append(
            _PolicyCandidate(
                name=f"trailing_vma_underlying:{vma_col}",
                thesis_exit_policy="trailing_vma_underlying",
                thesis_exit_params={"vma_col": vma_col},
                simulator=TradeSimulator(
                    exit_policy=VmaTrailingExitPolicy(
                        vma_col=vma_col, policy_name="trailing_vma_underlying"
                    )
                ),
            )
        )

    # Fixed reward-risk grid
    grid = _FIXED_RR_GRID.get(strategy_key, _DEFAULT_FIXED_RR_GRID)
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
                    exit_policy=FixedPercentRewardRiskExitPolicy(
                        stop_loss_pct=stop_loss_pct,
                        reward_multiple=reward_multiple,
                    )
                ),
            )
        )
    return candidates


def _sort_key(e: ExitPolicyEvaluation) -> tuple[float, float, float, float]:
    m = e.metrics
    return (
        float(m.get("expectancy") or float("-inf")),
        float(m.get("profit_factor") or float("-inf")),
        float(m.get("win_rate") or float("-inf")),
        float(m.get("trade_count") or 0.0),
    )
