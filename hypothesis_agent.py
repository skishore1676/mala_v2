#!/usr/bin/env python3
"""
hypothesis_agent.py - Autonomous M1→M5 Research Runner
=======================================================
Reads a hypothesis .md file, determines current stage, runs the next
gate(s) via the research infrastructure, updates the hypothesis file,
and writes all results as local CSVs.

Usage:
    python hypothesis_agent.py \
        --hypothesis research/hypotheses/iwm-opening-range-regime-continuation.md \
        [--max-stage M5] \
        [--dry-run]

State machine (driven by the hypothesis .md `state` field):
    pending  → run M1 discovery sweep
    retune   → run M1 with tighter parameter grid
    running  → resume from last completed gate
    completed / kill → no-op, just report
"""

from __future__ import annotations

import argparse
import re
import sys
from datetime import date, datetime, timezone
from itertools import product
from pathlib import Path
from typing import Any

import polars as pl

REPO_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(REPO_ROOT))

from src.chronos.storage import LocalStorage
from src.config import PROJECT_ROOT, settings
from src.newton.engine import PhysicsEngine
from src.oracle.metrics import MetricsCalculator
from src.oracle.monte_carlo import ExecutionStressConfig
from src.research.models import ResearchStage
from src.research.stages import (
    aggregate_walk_forward,
    build_gate_report,
    build_windows,
    promoted_candidates_from_gate_report,
    promoted_candidates_from_holdout,
    run_execution_mapping_for_candidates,
    run_holdout_validation_for_candidates,
    run_walk_forward_for_strategies,
    summarize_holdout,
)
from src.research.stages.candidates import build_candidate_strategy
from src.strategy.base import required_feature_union
from src.strategy.opening_drive_classifier import OpeningDriveClassifierStrategy


# ──────────────────────────────────────────────────────────────────────────────
# Defaults & constants
# ──────────────────────────────────────────────────────────────────────────────

STRATEGY_NAME = "Opening Drive Classifier"

# Calibration / holdout splits
DEFAULT_START         = date(2024, 1, 2)
DEFAULT_CAL_END       = date(2025, 11, 30)
DEFAULT_HOLDOUT_START = date(2025, 12, 1)
DEFAULT_HOLDOUT_END   = date(2026, 2, 28)
DEFAULT_END           = DEFAULT_HOLDOUT_END

# Walk-forward settings
TRAIN_MONTHS  = 6
TEST_MONTHS   = 3
MIN_SIGNALS   = 15
RATIOS        = [1.0, 1.25, 1.5, 2.0]
M1_COST_BPS   = 8.0
COST_GRID_BPS = [5.0, 8.0, 12.0]

# M1 gate thresholds
GATE_MIN_OOS_WINDOWS  = 3
GATE_MIN_OOS_SIGNALS  = 50
GATE_MIN_PCT_POSITIVE = 0.60
GATE_MIN_EXP_R        = 0.0

# M4 thresholds
MIN_CALIBRATION_SIGNALS = 40
MIN_HOLDOUT_SIGNALS     = 15

# M5
BASE_COST_R     = 0.08
BOOTSTRAP_ITERS = 4000


# ──────────────────────────────────────────────────────────────────────────────
# Parameter spaces
# Edit these before running if your hypothesis explores a different region.
# ──────────────────────────────────────────────────────────────────────────────

DISCOVERY_PARAMETER_SPACE: dict[str, list[Any]] = {
    "opening_window_minutes":     [5, 10, 15, 20, 30],
    "entry_start_offset_minutes": [15, 20, 25, 30, 45],
    "entry_end_offset_minutes":   [60, 90, 120, 180],
    "min_drive_return_pct":       [0.001, 0.0015, 0.002, 0.003],
    "breakout_buffer_pct":        [0.0, 0.0005, 0.001],
    "kinematic_periods_back":     [1, 3, 5],
    "use_volume_filter":          [True, False],
    "volume_multiplier":          [1.2, 1.4, 1.6],
    "use_directional_mass":       [True, False],
    "use_jerk_confirmation":      [True, False],
    "use_regime_filter":          [True, False],
    "regime_timeframe":           ["5m", "1m"],
}

RETUNE_PARAMETER_SPACE: dict[str, list[Any]] = {
    "opening_window_minutes":     [15, 20],
    "entry_start_offset_minutes": [20, 25, 30],
    "entry_end_offset_minutes":   [90, 120],
    "min_drive_return_pct":       [0.0015, 0.002],
    "breakout_buffer_pct":        [0.0, 0.0005],
    "kinematic_periods_back":     [1, 3],
    "use_volume_filter":          [True],
    "volume_multiplier":          [1.2, 1.4],
    "use_directional_mass":       [True],
    "use_jerk_confirmation":      [True, False],
    "use_regime_filter":          [True],
    "regime_timeframe":           ["5m"],
}


# ──────────────────────────────────────────────────────────────────────────────
# Logging helpers
# ──────────────────────────────────────────────────────────────────────────────

def log(msg: str) -> None:
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def fmt_float(value: object, digits: int = 4) -> str:
    if value is None:
        return "null"
    try:
        return f"{float(value):+.{digits}f}"
    except (TypeError, ValueError):
        return str(value)


# ──────────────────────────────────────────────────────────────────────────────
# Parameter grid builder (replaces ResearchToolbox dependency)
# ──────────────────────────────────────────────────────────────────────────────

def _bounded_param_grid(
    space: dict[str, list[Any]],
    max_configs: int = 32,
) -> list[dict[str, Any]]:
    """Cartesian product of `space`, sampled evenly to at most `max_configs`."""
    keys = sorted(space)
    values = [space[k] for k in keys]
    all_configs = [dict(zip(keys, combo, strict=True)) for combo in product(*values)]

    seen: set[str] = set()
    unique: list[dict[str, Any]] = []
    for c in all_configs:
        key = str(sorted(c.items()))
        if key not in seen:
            seen.add(key)
            unique.append(c)

    if len(unique) <= max_configs:
        return unique

    indices = sorted({
        round(i * (len(unique) - 1) / (max_configs - 1))
        for i in range(max_configs)
    })
    return [unique[i] for i in indices]


# ──────────────────────────────────────────────────────────────────────────────
# Hypothesis file parser
# ──────────────────────────────────────────────────────────────────────────────

def parse_hypothesis_state(hypothesis_path: Path) -> dict[str, str]:
    """Extract key fields from the hypothesis markdown file."""
    text = hypothesis_path.read_text()
    fields: dict[str, str] = {}

    m = re.search(r"- state:\s*`(\w+)`", text)
    fields["state"] = m.group(1) if m else "pending"

    m = re.search(r"- symbol_scope:\s*`([^`]+)`", text)
    fields["symbol_scope"] = m.group(1) if m else "IWM"

    m = re.search(r"- next_action:\s*`([^`]+)`", text)
    fields["next_action"] = m.group(1) if m else ""

    m = re.search(r"- preferred_strategy_family:\s*`([^`]+)`", text)
    fields["strategy_family"] = m.group(1) if m else STRATEGY_NAME

    log(f"HYPOTHESIS  state={fields['state']}  symbol={fields['symbol_scope']}")
    return fields


# ──────────────────────────────────────────────────────────────────────────────
# Strategy builder
# ──────────────────────────────────────────────────────────────────────────────

def build_opening_drive(config: dict[str, Any]) -> OpeningDriveClassifierStrategy:
    return OpeningDriveClassifierStrategy(
        opening_window_minutes=int(config.get("opening_window_minutes", 15)),
        entry_start_offset_minutes=int(config.get("entry_start_offset_minutes", 25)),
        entry_end_offset_minutes=int(config.get("entry_end_offset_minutes", 120)),
        min_drive_return_pct=float(config.get("min_drive_return_pct", 0.002)),
        breakout_buffer_pct=float(config.get("breakout_buffer_pct", 0.0)),
        kinematic_periods_back=int(config.get("kinematic_periods_back", 1)),
        use_volume_filter=bool(config.get("use_volume_filter", True)),
        volume_multiplier=float(config.get("volume_multiplier", 1.2)),
        use_directional_mass=bool(config.get("use_directional_mass", True)),
        use_jerk_confirmation=bool(config.get("use_jerk_confirmation", True)),
        use_regime_filter=bool(config.get("use_regime_filter", True)),
        regime_timeframe=str(config.get("regime_timeframe", "5m")),
    )


def _is_valid_config(config: dict[str, Any]) -> bool:
    """Monotonic ordering constraint: opening_window < entry_start < entry_end."""
    ow = int(config.get("opening_window_minutes", 0))
    es = int(config.get("entry_start_offset_minutes", 0))
    ee = int(config.get("entry_end_offset_minutes", 0))
    return ow < es < ee


def build_retune_configs(max_configs: int = 32) -> list[dict[str, Any]]:
    valid = [c for c in _bounded_param_grid(RETUNE_PARAMETER_SPACE, max_configs=256)
             if _is_valid_config(c)]
    if len(valid) <= max_configs:
        return valid
    indices = sorted({
        round(i * (len(valid) - 1) / (max_configs - 1))
        for i in range(max_configs)
    })
    sampled = [valid[i] for i in indices]
    log(f"RETUNE_GRID  valid={len(valid)}  sampled={len(sampled)}")
    return sampled


# ──────────────────────────────────────────────────────────────────────────────
# Data loader
# ──────────────────────────────────────────────────────────────────────────────

def load_frames(
    *,
    tickers: list[str],
    start: date,
    end: date,
    strategies: list[OpeningDriveClassifierStrategy],
) -> dict[str, pl.DataFrame]:
    storage = LocalStorage()
    physics = PhysicsEngine()
    needed = required_feature_union(strategies)
    frames: dict[str, pl.DataFrame] = {}
    for ticker in tickers:
        raw = storage.load_bars(ticker, start, end)
        if raw.is_empty():
            log(f"SKIP_NO_DATA {ticker}")
            continue
        frames[ticker] = physics.enrich_for_features(raw, needed)
        log(f"LOADED  {ticker}  rows={frames[ticker].height}")
    return frames


# ──────────────────────────────────────────────────────────────────────────────
# M1 — Discovery / Retune sweep
# ──────────────────────────────────────────────────────────────────────────────

def run_m1(
    *,
    frames: dict[str, pl.DataFrame],
    windows: list,
    ratios: list[float],
    metrics: MetricsCalculator,
    configs: list[dict[str, Any]],
    top_per_ticker: int = 2,
    train_months: int = TRAIN_MONTHS,
    test_months: int = TEST_MONTHS,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Return (detail_df, aggregate_df, top_df)."""
    detail_rows: list[dict[str, Any]] = []
    aggregate_rows: list[dict[str, Any]] = []

    for idx, config in enumerate(configs, start=1):
        strategy = build_opening_drive(config)
        log(f"M1  {idx}/{len(configs)}  ow={config['opening_window_minutes']}  "
            f"es={config['entry_start_offset_minutes']}  drive={config['min_drive_return_pct']:.4f}  "
            f"regime={config['use_regime_filter']}")
        for ticker, frame in frames.items():
            rows = run_walk_forward_for_strategies(
                ticker=ticker,
                df=frame,
                strategies=[strategy],
                windows=windows,
                ratios=ratios,
                metrics=metrics,
                min_signals=MIN_SIGNALS,
                cost_bps=M1_COST_BPS,
            )
            if not rows:
                continue
            for row in rows:
                detail_rows.append({**row, **config})
            agg = aggregate_walk_forward(rows)
            for row in agg.iter_rows(named=True):
                aggregate_rows.append({**row, **config})

    detail_df = pl.DataFrame(detail_rows) if detail_rows else pl.DataFrame()
    aggregate_df = pl.DataFrame(aggregate_rows) if aggregate_rows else pl.DataFrame()

    if aggregate_df.is_empty():
        return detail_df, aggregate_df, pl.DataFrame()

    ranked_df = (
        aggregate_df
        .filter(pl.col("direction").is_in(["long", "short", "combined"]))
        .filter(pl.col("avg_test_exp_r").is_not_null())
        .filter(pl.col("pct_positive_oos_windows").is_not_null())
        .filter(pl.col("avg_test_exp_r") > 0)
        .with_columns([
            (
                pl.col("avg_test_exp_r") * 1000
                + pl.col("pct_positive_oos_windows") * 100
                + pl.col("oos_signals") / 1000
            ).alias("m1_score")
        ])
        .sort(
            ["ticker", "oos_windows", "m1_score", "avg_test_exp_r", "oos_signals"],
            descending=[False, True, True, True, True],
        )
    )

    top_rows: list[dict[str, Any]] = []
    for ticker in frames:
        subset = ranked_df.filter(pl.col("ticker") == ticker)
        if subset.is_empty():
            continue
        top_rows.extend(subset.head(top_per_ticker).iter_rows(named=True))

    top_df = pl.DataFrame(top_rows) if top_rows else pl.DataFrame()
    return detail_df, aggregate_df, top_df


# ──────────────────────────────────────────────────────────────────────────────
# M1 gate check
# ──────────────────────────────────────────────────────────────────────────────

def evaluate_m1_gate(top_df: pl.DataFrame) -> tuple[bool, str]:
    if top_df.is_empty():
        return False, "no positive configs found"

    best = top_df.sort("m1_score", descending=True).row(0, named=True)
    pct = float(best.get("pct_positive_oos_windows", 0) or 0)
    exp_r = float(best.get("avg_test_exp_r", 0) or 0)
    signals = int(best.get("oos_signals", 0) or 0)
    windows = int(best.get("oos_windows", 0) or 0)

    reasons: list[str] = []
    if pct < GATE_MIN_PCT_POSITIVE:
        reasons.append(f"pct_positive_oos_windows={pct:.1%} < {GATE_MIN_PCT_POSITIVE:.1%}")
    if signals < GATE_MIN_OOS_SIGNALS:
        reasons.append(f"oos_signals={signals} < {GATE_MIN_OOS_SIGNALS}")
    if windows < GATE_MIN_OOS_WINDOWS:
        reasons.append(f"oos_windows={windows} < {GATE_MIN_OOS_WINDOWS}")
    if exp_r <= GATE_MIN_EXP_R:
        reasons.append(f"avg_test_exp_r={exp_r:+.4f} <= 0")

    if reasons:
        return False, "; ".join(reasons)
    return True, f"pct_positive={pct:.1%}  exp_r={exp_r:+.4f}  signals={signals}  windows={windows}"


# ──────────────────────────────────────────────────────────────────────────────
# M2 — Convergence grid
# ──────────────────────────────────────────────────────────────────────────────

def run_m2(
    *,
    frames: dict[str, pl.DataFrame],
    windows: list,
    ratios: list[float],
    metrics: MetricsCalculator,
    top_candidates: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Return (combined_df, gate_report_df, promoted_df)."""
    config_keys = [
        "opening_window_minutes", "entry_start_offset_minutes", "entry_end_offset_minutes",
        "min_drive_return_pct", "breakout_buffer_pct", "kinematic_periods_back",
        "use_volume_filter", "volume_multiplier", "use_directional_mass",
        "use_jerk_confirmation", "use_regime_filter", "regime_timeframe",
    ]

    convergence_frames: list[pl.DataFrame] = []
    for cost_bps in COST_GRID_BPS:
        rows: list[dict[str, Any]] = []
        log(f"M2  cost_bps={cost_bps}")
        for candidate in top_candidates.iter_rows(named=True):
            config = {k: candidate[k] for k in config_keys if k in candidate}
            strategy = build_opening_drive(config)
            ticker = str(candidate["ticker"])
            if ticker not in frames:
                continue
            wf_rows = run_walk_forward_for_strategies(
                ticker=ticker,
                df=frames[ticker],
                strategies=[strategy],
                windows=windows,
                ratios=ratios,
                metrics=metrics,
                min_signals=MIN_SIGNALS,
                cost_bps=cost_bps,
            )
            if not wf_rows:
                continue
            agg = aggregate_walk_forward(wf_rows)
            direction = str(candidate.get("direction", "combined"))
            agg = agg.filter(pl.col("direction") == direction)
            if agg.is_empty():
                continue
            for row in agg.iter_rows(named=True):
                rows.append({**row, "cost_bps": cost_bps, **config})
        if rows:
            convergence_frames.append(pl.DataFrame(rows))

    if not convergence_frames:
        return pl.DataFrame(), pl.DataFrame(), pl.DataFrame()

    combined_df = pl.concat(convergence_frames, how="vertical")
    gate_report = build_gate_report(
        combined=combined_df,
        cost_count=len(COST_GRID_BPS),
        gate_min_oos_windows=GATE_MIN_OOS_WINDOWS,
        gate_min_oos_signals=GATE_MIN_OOS_SIGNALS,
        gate_min_pct_positive=GATE_MIN_PCT_POSITIVE,
        gate_min_exp_r=GATE_MIN_EXP_R,
    )
    promoted_df = promoted_candidates_from_gate_report(gate_report)
    log(f"M2  gate_report_rows={gate_report.height}  promoted={promoted_df.height}")
    return combined_df, gate_report, promoted_df


# ──────────────────────────────────────────────────────────────────────────────
# M3 — Walk-forward OOS validation
# ──────────────────────────────────────────────────────────────────────────────

def run_m3(
    *,
    frames: dict[str, pl.DataFrame],
    windows: list,
    ratios: list[float],
    metrics: MetricsCalculator,
    promoted_candidates: pl.DataFrame,
) -> pl.DataFrame:
    rows: list[dict[str, Any]] = []
    for candidate in promoted_candidates.iter_rows(named=True):
        try:
            strategy = build_candidate_strategy(candidate)
        except Exception:
            config = {
                k: candidate[k]
                for k in [
                    "opening_window_minutes", "entry_start_offset_minutes", "entry_end_offset_minutes",
                    "min_drive_return_pct", "breakout_buffer_pct", "kinematic_periods_back",
                    "use_volume_filter", "volume_multiplier", "use_directional_mass",
                    "use_jerk_confirmation", "use_regime_filter", "regime_timeframe",
                ]
                if k in candidate
            }
            strategy = build_opening_drive(config)

        ticker = str(candidate["ticker"])
        if ticker not in frames:
            continue
        wf_rows = run_walk_forward_for_strategies(
            ticker=ticker,
            df=frames[ticker],
            strategies=[strategy],
            windows=windows,
            ratios=ratios,
            metrics=metrics,
            min_signals=MIN_SIGNALS,
            cost_bps=M1_COST_BPS,
        )
        if wf_rows:
            rows.extend(wf_rows)

    m3_df = pl.DataFrame(rows) if rows else pl.DataFrame()
    log(f"M3  detail_rows={m3_df.height}")
    return m3_df


# ──────────────────────────────────────────────────────────────────────────────
# M4 — Holdout validation
# ──────────────────────────────────────────────────────────────────────────────

def run_m4(
    *,
    frames: dict[str, pl.DataFrame],
    metrics: MetricsCalculator,
    promoted_candidates: pl.DataFrame,
    holdout_start: date,
    holdout_end: date,
    calibration_end: date,
    start: date,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    detail_rows = run_holdout_validation_for_candidates(
        promoted=promoted_candidates,
        ticker_frames=frames,
        metrics=metrics,
        start_date=start,
        calibration_end=calibration_end,
        holdout_start=holdout_start,
        holdout_end=holdout_end,
        ratios=RATIOS,
        costs=COST_GRID_BPS,
        min_calibration_signals=MIN_CALIBRATION_SIGNALS,
        min_holdout_signals=MIN_HOLDOUT_SIGNALS,
    )
    detail_df = pl.DataFrame(detail_rows) if detail_rows else pl.DataFrame()
    summary_df = summarize_holdout(detail_df, cost_count=len(COST_GRID_BPS)) if detail_rows else pl.DataFrame()
    promoted_m4 = promoted_candidates_from_holdout(summary_df) if not summary_df.is_empty() else pl.DataFrame()
    log(f"M4  detail_rows={detail_df.height}  promoted={promoted_m4.height}")
    return detail_df, promoted_m4


# ──────────────────────────────────────────────────────────────────────────────
# M5 — Execution mapping
# ──────────────────────────────────────────────────────────────────────────────

def run_m5(
    *,
    frames: dict[str, pl.DataFrame],
    metrics: MetricsCalculator,
    m4_promoted: pl.DataFrame,
    m4_detail: pl.DataFrame,
    holdout_start: date,
    holdout_end: date,
) -> pl.DataFrame:
    stress_cfg = ExecutionStressConfig(bootstrap_iters=BOOTSTRAP_ITERS)
    rows = run_execution_mapping_for_candidates(
        promoted=m4_promoted,
        holdout_detail=m4_detail,
        ticker_frames=frames,
        metrics=metrics,
        holdout_start=holdout_start,
        holdout_end=holdout_end,
        base_cost_r=BASE_COST_R,
        stress_cfg=stress_cfg,
    )
    m5_df = pl.DataFrame(rows) if rows else pl.DataFrame()
    log(f"M5  mapped_rows={m5_df.height}")
    return m5_df


# ──────────────────────────────────────────────────────────────────────────────
# Hypothesis file updater
# ──────────────────────────────────────────────────────────────────────────────

def build_agent_report_section(
    *,
    run_ts: str,
    hypothesis_id: str,
    stages_run: list[str],
    final_disposition: str,
    m1_best: dict[str, Any] | None,
    m2_promoted: int,
    m4_promoted: int,
    m5_promoted: int,
    artifact_dir: str,
    notes: str,
) -> str:
    m1_summary = "no positive configs" if m1_best is None else (
        f"best: ow={m1_best.get('opening_window_minutes')} "
        f"es={m1_best.get('entry_start_offset_minutes')} "
        f"ee={m1_best.get('entry_end_offset_minutes')} "
        f"drive={m1_best.get('min_drive_return_pct')} "
        f"exp_r={fmt_float(m1_best.get('avg_test_exp_r'))} "
        f"pct_pos={float(m1_best.get('pct_positive_oos_windows') or 0):.1%}"
    )
    stages_str = " → ".join(stages_run) if stages_run else "none"
    return f"""## Agent Report
### Run Timestamp
`{run_ts}`

### Stages Executed
`{stages_str}`

### Stage Outcomes
- M1 (discovery/retune): `{m1_summary}`
- M2 promoted: `{m2_promoted}`
- M4 holdout promoted: `{m4_promoted}`
- M5 execution promoted: `{m5_promoted}`

### Notes
{notes}

### Disposition
- decision: `{final_disposition}`

### Artifact Directory
`{artifact_dir}`
"""


def update_hypothesis_file(
    hypothesis_path: Path,
    *,
    new_state: str,
    next_action: str,
    agent_report: str,
) -> None:
    text = hypothesis_path.read_text()

    text = re.sub(r"(- state:\s*)`[^`]+`", f"\\1`{new_state}`", text)
    now_str = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+0000")
    text = re.sub(r"(- last_run_at:\s*)`[^`]+`", f"\\1`{now_str}`", text)
    text = re.sub(r"(- next_action:\s*)`[^`]+`", f"\\1`{next_action}`", text)

    if "## Agent Report" in text:
        text = re.sub(r"## Agent Report.*", agent_report, text, flags=re.DOTALL)
    else:
        text = text.rstrip() + "\n\n" + agent_report

    hypothesis_path.write_text(text)
    log(f"HYPOTHESIS  updated  state={new_state}  path={hypothesis_path}")


# ──────────────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--hypothesis",
        default="research/hypotheses/iwm-opening-range-regime-continuation.md",
        help="Path to hypothesis .md file (relative to repo root or absolute)",
    )
    parser.add_argument("--tickers", nargs="+", default=["IWM"])
    parser.add_argument("--start",           type=date.fromisoformat, default=DEFAULT_START)
    parser.add_argument("--calibration-end", type=date.fromisoformat, default=DEFAULT_CAL_END)
    parser.add_argument("--holdout-start",   type=date.fromisoformat, default=DEFAULT_HOLDOUT_START)
    parser.add_argument("--holdout-end",     type=date.fromisoformat, default=DEFAULT_HOLDOUT_END)
    parser.add_argument("--end",             type=date.fromisoformat, default=DEFAULT_END)
    parser.add_argument("--max-stage",       choices=["M1", "M2", "M3", "M4", "M5"], default="M5")
    parser.add_argument("--train-months",    type=int, default=TRAIN_MONTHS)
    parser.add_argument("--test-months",     type=int, default=TEST_MONTHS)
    parser.add_argument("--top-per-ticker",  type=int, default=2)
    parser.add_argument(
        "--out-dir",
        default="data/results/hypothesis_runs",
        help="Directory for local artifact CSV files",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Parse hypothesis and print plan without running any experiments",
    )
    return parser.parse_args()


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────

def main() -> None:
    args = parse_args()

    hyp_path = Path(args.hypothesis)
    if not hyp_path.is_absolute():
        hyp_path = REPO_ROOT / hyp_path
    if not hyp_path.exists():
        log(f"ERROR  hypothesis file not found: {hyp_path}")
        sys.exit(1)

    hypothesis_id = hyp_path.stem
    run_ts = datetime.now().strftime("%Y-%m-%dT%H%M%S")

    h = parse_hypothesis_state(hyp_path)
    state = h["state"]
    tickers = [t.strip() for t in h["symbol_scope"].split(",") if t.strip()] or args.tickers

    if state in ("completed", "kill"):
        log(f"SKIP  hypothesis is {state} — nothing to run")
        return

    if args.dry_run:
        configs = build_retune_configs() if state == "retune" else \
                  [c for c in _bounded_param_grid(DISCOVERY_PARAMETER_SPACE) if _is_valid_config(c)]
        log(f"DRY_RUN  state={state}  configs={len(configs)}  tickers={tickers}")
        log(f"DRY_RUN  would run stages up to {args.max_stage}")
        return

    out_dir = REPO_ROOT / args.out_dir / hypothesis_id / run_ts
    out_dir.mkdir(parents=True, exist_ok=True)
    log(f"ARTIFACTS  {out_dir}")

    # Build configs
    if state == "retune":
        configs = build_retune_configs()
        log(f"MODE  retune  configs={len(configs)}")
    else:
        all_cfg = _bounded_param_grid(DISCOVERY_PARAMETER_SPACE, max_configs=32)
        configs = [c for c in all_cfg if _is_valid_config(c)]
        log(f"MODE  discovery  configs={len(configs)}")

    # Load data
    all_strategies = [build_opening_drive(c) for c in configs]
    frames_cal = load_frames(tickers=tickers, start=args.start, end=args.calibration_end,
                             strategies=all_strategies)
    frames_full = load_frames(tickers=tickers, start=args.start, end=args.end,
                              strategies=all_strategies)

    if not frames_cal:
        log("ERROR  no data loaded for any ticker")
        sys.exit(1)

    metrics = MetricsCalculator()
    windows = build_windows(args.start, args.calibration_end, args.train_months, args.test_months)
    log(f"WINDOWS  {len(windows)} walk-forward windows  (train={args.train_months}m / test={args.test_months}m)")

    stage_frames: dict[str, pl.DataFrame] = {}
    summary_rows: list[dict[str, Any]] = []
    stages_run: list[str] = []
    final_disposition = "retune"
    notes: list[str] = []
    m1_best: dict[str, Any] | None = None
    m2_promoted_count = 0
    m4_promoted_count = 0
    m5_promoted_count = 0

    def add_summary(stage: str, metric: str, value: Any, note: str = "") -> None:
        summary_rows.append({"stage": stage, "metric": metric, "value": str(value), "note": note})

    def finish(disposition: str) -> None:
        next_action_map = {
            "retune":        "run another bounded M1 retune with tighter thresholds",
            "kill":          "hypothesis killed — insufficient edge across all gates",
            "promote_to_m2": "M1 passed — run M2 convergence grid",
            "promote_to_m3": "M2 passed — run M3 walk-forward OOS",
            "promote_to_m4": "M3 passed — run M4 holdout validation",
            "promote_to_m5": "M4 holdout passed — run M5 execution mapping",
            "promote":       "M5 passed — ready for human review → trade_lab",
        }
        next_action = next_action_map.get(disposition, disposition)
        new_state = (
            "completed" if disposition == "promote"
            else "kill" if disposition == "kill"
            else "running" if disposition.startswith("promote_to")
            else "retune"
        )
        report = build_agent_report_section(
            run_ts=run_ts, hypothesis_id=hypothesis_id,
            stages_run=stages_run, final_disposition=disposition,
            m1_best=m1_best, m2_promoted=m2_promoted_count,
            m4_promoted=m4_promoted_count, m5_promoted=m5_promoted_count,
            artifact_dir=str(out_dir), notes="\n".join(f"- {n}" for n in notes),
        )
        try:
            update_hypothesis_file(hyp_path, new_state=new_state,
                                   next_action=next_action, agent_report=report)
        except Exception as exc:
            log(f"WARN  hypothesis file update failed: {exc}")
        log("─" * 60)
        log(f"DONE  disposition={disposition}  stages={' → '.join(stages_run)}")

    # ── M1 ───────────────────────────────────────────────────────────────────
    log("─" * 60)
    log("STAGE  M1  discovery/retune sweep")
    detail_m1, aggregate_m1, top_m1 = run_m1(
        frames=frames_cal, windows=windows, ratios=RATIOS,
        metrics=metrics, configs=configs, top_per_ticker=args.top_per_ticker,
        train_months=args.train_months, test_months=args.test_months,
    )
    stages_run.append("M1")

    if not detail_m1.is_empty():
        detail_m1.write_csv(out_dir / "M1_detail.csv")
    if not aggregate_m1.is_empty():
        aggregate_m1.write_csv(out_dir / "M1_aggregate.csv")
    stage_frames[f"M1_top_{run_ts[:8]}"] = top_m1

    m1_passes, m1_reason = evaluate_m1_gate(top_m1)
    add_summary("M1", "gate_result", "PASS" if m1_passes else "FAIL", m1_reason)
    add_summary("M1", "configs_tested", len(configs))
    add_summary("M1", "positive_configs", top_m1.height if not top_m1.is_empty() else 0)

    if not top_m1.is_empty():
        m1_best = top_m1.sort("m1_score", descending=True).row(0, named=True) \
                  if "m1_score" in top_m1.columns else top_m1.row(0, named=True)

    log(f"M1  gate={'PASS' if m1_passes else 'FAIL'}  reason={m1_reason}")

    if not m1_passes:
        notes.append(f"M1 gate failed: {m1_reason}.")
        any_positive = (
            not aggregate_m1.is_empty()
            and "avg_test_exp_r" in aggregate_m1.columns
            and aggregate_m1.filter(pl.col("avg_test_exp_r") > 0).height > 0
        )
        final_disposition = "retune" if any_positive else "kill"
        if final_disposition == "kill":
            notes.append("No positive exp_r found in any config. Recommend kill.")
        else:
            notes.append("Some positive signal remains. Recommend another retune with narrower grid.")
        finish(final_disposition)
        return

    notes.append(f"M1 gate passed: {m1_reason}.")
    if args.max_stage == "M1":
        finish("promote_to_m2")
        return

    # ── M2 ───────────────────────────────────────────────────────────────────
    log("─" * 60)
    log("STAGE  M2  convergence grid")
    combined_m2, gate_report_m2, promoted_m2 = run_m2(
        frames=frames_cal, windows=windows, ratios=RATIOS,
        metrics=metrics, top_candidates=top_m1,
    )
    stages_run.append("M2")
    m2_promoted_count = promoted_m2.height

    if not combined_m2.is_empty():
        combined_m2.write_csv(out_dir / "M2_convergence.csv")
    if not gate_report_m2.is_empty():
        gate_report_m2.write_csv(out_dir / "M2_gate_report.csv")
    if not promoted_m2.is_empty():
        promoted_m2.write_csv(out_dir / "M2_promoted.csv")
    stage_frames[f"M2_gate_{run_ts[:8]}"] = gate_report_m2

    add_summary("M2", "candidates_tested", top_m1.height)
    add_summary("M2", "promoted", m2_promoted_count)

    if promoted_m2.is_empty():
        notes.append("M2 gate: no candidates passed convergence across all friction levels.")
        finish("retune")
        return

    notes.append(f"M2 passed: {m2_promoted_count} candidates promoted.")
    if args.max_stage == "M2":
        finish("promote_to_m3")
        return

    # ── M3 ───────────────────────────────────────────────────────────────────
    log("─" * 60)
    log("STAGE  M3  walk-forward OOS")
    m3_df = run_m3(
        frames=frames_cal, windows=windows, ratios=RATIOS,
        metrics=metrics, promoted_candidates=promoted_m2,
    )
    stages_run.append("M3")

    if not m3_df.is_empty():
        m3_df.write_csv(out_dir / "M3_walk_forward.csv")
        stage_frames[f"M3_wf_{run_ts[:8]}"] = m3_df

    add_summary("M3", "detail_rows", m3_df.height)
    if args.max_stage == "M3":
        finish("promote_to_m4")
        return

    # ── M4 ───────────────────────────────────────────────────────────────────
    log("─" * 60)
    log("STAGE  M4  holdout validation")
    m4_detail, m4_promoted = run_m4(
        frames=frames_full, metrics=metrics, promoted_candidates=promoted_m2,
        holdout_start=args.holdout_start, holdout_end=args.holdout_end,
        calibration_end=args.calibration_end, start=args.start,
    )
    stages_run.append("M4")
    m4_promoted_count = m4_promoted.height

    if not m4_detail.is_empty():
        m4_detail.write_csv(out_dir / "M4_holdout.csv")
    if not m4_promoted.is_empty():
        m4_promoted.write_csv(out_dir / "M4_promoted.csv")
        stage_frames[f"M4_holdout_{run_ts[:8]}"] = m4_detail

    add_summary("M4", "holdout_detail_rows", m4_detail.height)
    add_summary("M4", "promoted", m4_promoted_count)

    if m4_promoted.is_empty():
        notes.append("M4 holdout: no candidates promoted. Edge does not survive untouched holdout.")
        finish("kill")
        return

    notes.append(f"M4 passed: {m4_promoted_count} candidates promoted.")
    if args.max_stage == "M4":
        finish("promote_to_m5")
        return

    # ── M5 ───────────────────────────────────────────────────────────────────
    log("─" * 60)
    log("STAGE  M5  execution mapping")
    m5_df = run_m5(
        frames=frames_full, metrics=metrics, m4_promoted=m4_promoted, m4_detail=m4_detail,
        holdout_start=args.holdout_start, holdout_end=args.holdout_end,
    )
    stages_run.append("M5")
    m5_promoted_count = m5_df.height

    if not m5_df.is_empty():
        m5_df.write_csv(out_dir / "M5_execution.csv")
        stage_frames[f"M5_exec_{run_ts[:8]}"] = m5_df

    add_summary("M5", "mapped_rows", m5_promoted_count)

    if m5_df.is_empty():
        notes.append("M5 execution: no candidates passed execution stress.")
        finish("kill")
    else:
        notes.append(f"M5 passed: {m5_promoted_count} execution mappings produced. Ready for human review.")
        finish("promote")


if __name__ == "__main__":
    main()
