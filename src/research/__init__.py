"""Research stage exports."""

from src.research.stages import (
    Window,
    aggregate_walk_forward,
    build_gate_report,
    build_windows,
    cost_r_from_bps,
    choose_ratio,
    evaluate_df,
    promoted_candidates_from_gate_report,
    promoted_candidates_from_holdout,
    run_execution_mapping_for_candidates,
    run_holdout_validation_for_candidates,
    run_walk_forward_for_strategies,
    summarize_holdout,
)

__all__ = [
    "Window",
    "aggregate_walk_forward",
    "build_gate_report",
    "build_windows",
    "cost_r_from_bps",
    "choose_ratio",
    "evaluate_df",
    "promoted_candidates_from_gate_report",
    "promoted_candidates_from_holdout",
    "run_execution_mapping_for_candidates",
    "run_holdout_validation_for_candidates",
    "run_walk_forward_for_strategies",
    "summarize_holdout",
]
