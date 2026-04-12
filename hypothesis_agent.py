#!/usr/bin/env python3
"""
hypothesis_agent.py — Generic M1→M5 Research Runner
=====================================================
Reads a hypothesis .md file and runs the configured strategy through
M1-M5 gates. All results are written as local CSVs.

Usage:
    python hypothesis_agent.py --hypothesis research/hypotheses/my-idea.md
    python hypothesis_agent.py --hypothesis research/hypotheses/my-idea.md --max-stage M2
    python hypothesis_agent.py --hypothesis research/hypotheses/my-idea.md --dry-run

Hypothesis file schema (fields read by this script):
    - id:           `slug-kebab`
    - state:        `pending | running | retune | completed | kill`
    - decision:     `` (set by agent after each run)
    - symbol_scope: `IWM` or `SPY, QQQ`
    - strategy:     `Opening Drive Classifier`  (must match factory registry)
    - max_stage:    `M5`

State machine:
    pending   → full discovery sweep from M1
    retune    → tight retune sweep from M1
    running   → resume from stage indicated by `decision` field
    completed / kill → no-op
"""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from datetime import date, datetime, timezone
from itertools import product
from pathlib import Path
from typing import Any, Callable

import polars as pl

REPO_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(REPO_ROOT))

from src.chronos.storage import LocalStorage
from src.config import settings
from src.newton.engine import PhysicsEngine
from src.oracle.metrics import MetricsCalculator
from src.oracle.monte_carlo import ExecutionStressConfig
from src.research.catalog import upsert_strategy_catalog
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
from src.strategy.factory import build_strategy


# ── Gate defaults ─────────────────────────────────────────────────────────────
DEFAULT_START         = date(2024, 1, 2)
DEFAULT_CAL_END       = date(2025, 11, 30)
DEFAULT_HOLDOUT_START = date(2025, 12, 1)
DEFAULT_HOLDOUT_END   = date(2026, 2, 28)
DEFAULT_END           = DEFAULT_HOLDOUT_END

TRAIN_MONTHS  = 6
TEST_MONTHS   = 3
MIN_SIGNALS   = 15
RATIOS        = [1.0, 1.25, 1.5, 2.0]
M1_COST_BPS   = 8.0
COST_GRID_BPS = [5.0, 8.0, 12.0]

GATE_MIN_OOS_WINDOWS  = 3
GATE_MIN_OOS_SIGNALS  = 50
GATE_MIN_PCT_POSITIVE = 0.60
GATE_MIN_EXP_R        = 0.0

MIN_CALIBRATION_SIGNALS = 40
MIN_HOLDOUT_SIGNALS     = 15
BASE_COST_R             = 0.08
BOOTSTRAP_ITERS         = 4000

# Maps a `decision` value → the next stage to start from when resuming
DECISION_TO_STAGE: dict[str, str] = {
    "promote_to_m2": "M2",
    "promote_to_m3": "M3",
    "promote_to_m4": "M4",
    "promote_to_m5": "M5",
}


# ── Per-strategy parameter spaces ─────────────────────────────────────────────
# Each entry has "discovery" (broad sweep) and "retune" (tight follow-up).
# Add a new strategy here to make it sweepable.

PARAMETER_SPACES: dict[str, dict[str, dict[str, list[Any]]]] = {
    "Opening Drive Classifier": {
        "discovery": {
            "opening_window_minutes":     [5, 10, 15, 20, 30],
            "entry_start_offset_minutes": [15, 20, 25, 30, 45],
            "entry_end_offset_minutes":   [60, 90, 120, 180],
            "min_drive_return_pct":       [0.001, 0.0015, 0.002, 0.003],
            "breakout_buffer_pct":        [0.0, 0.0005],
            "kinematic_periods_back":     [1, 3],
            "use_volume_filter":          [True, False],
            "volume_multiplier":          [1.2, 1.4],
            "use_directional_mass":       [True, False],
            "use_jerk_confirmation":      [True, False],
            "use_regime_filter":          [True, False],
            "regime_timeframe":           ["5m"],
        },
        "retune": {
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
        },
    },
    "Market Impulse (Cross & Reclaim)": {
        "discovery": {
            "entry_buffer_minutes": [3, 5, 10],
            "entry_window_minutes": [30, 60, 90],
            "regime_timeframe":     ["5m", "1m"],
        },
        "retune": {
            "entry_buffer_minutes": [3, 5],
            "entry_window_minutes": [45, 60],
            "regime_timeframe":     ["5m"],
        },
    },
    "Elastic Band Reversion": {
        "discovery": {
            "z_score_threshold":    [1.5, 2.0, 2.5],
            "z_score_window":       [120, 240, 360],
            "use_directional_mass": [True, False],
            "use_jerk_confirmation":[True, False],
            "kinematic_periods_back":[1, 3],
        },
        "retune": {
            "z_score_threshold":    [1.75, 2.0, 2.25],
            "z_score_window":       [200, 240, 280],
            "use_directional_mass": [True],
            "use_jerk_confirmation":[True, False],
            "kinematic_periods_back":[1],
        },
    },
    "Jerk-Pivot Momentum (tight)": {
        "discovery": {
            "vpoc_proximity_pct": [0.001, 0.002, 0.005],
            "jerk_lookback":      [5, 10, 20],
            "volume_multiplier":  [1.0, 1.2, 1.5],
            "use_volume_filter":  [True, False],
        },
        "retune": {
            "vpoc_proximity_pct": [0.002, 0.003],
            "jerk_lookback":      [10, 15],
            "volume_multiplier":  [1.2, 1.3],
            "use_volume_filter":  [True],
        },
    },
}

# Per-strategy config validators (return False to discard a config from the grid)
STRATEGY_VALIDATORS: dict[str, Callable[[dict[str, Any]], bool]] = {
    "Opening Drive Classifier": lambda c: (
        int(c.get("opening_window_minutes", 0))
        < int(c.get("entry_start_offset_minutes", 0))
        < int(c.get("entry_end_offset_minutes", 0))
    ),
}


# ── Hypothesis file ───────────────────────────────────────────────────────────

@dataclass
class HypothesisState:
    path: Path
    id: str
    state: str       # pending | running | retune | completed | kill
    decision: str    # promote_to_m2 | ... | promote | kill | ''
    tickers: list[str]
    strategy: str
    max_stage: str


def parse_hypothesis(path: Path) -> HypothesisState:
    text = path.read_text()

    def _field(name: str, default: str = "") -> str:
        m = re.search(rf"- {name}:\s*`([^`]*)`", text)
        return m.group(1).strip() if m else default

    hyp_id   = _field("id", path.stem)
    state    = _field("state", "pending")
    decision = _field("decision", "")
    scope    = _field("symbol_scope", "SPY")
    strategy = _field("strategy", "Opening Drive Classifier")
    max_st   = _field("max_stage", "M5")

    tickers = [t.strip() for t in scope.split(",") if t.strip()]
    return HypothesisState(
        path=path, id=hyp_id, state=state, decision=decision,
        tickers=tickers, strategy=strategy, max_stage=max_st,
    )


def update_hypothesis(
    path: Path,
    *,
    new_state: str,
    new_decision: str,
    report: str,
) -> None:
    text = path.read_text()
    now = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+0000")

    def _replace(field: str, value: str) -> str:
        return re.sub(rf"(- {field}:\s*)`[^`]*`", rf"\1`{value}`", text)

    text = _replace("state", new_state)
    text = _replace("decision", new_decision)
    text = re.sub(r"(- last_run:\s*)`[^`]*`", rf"\1`{now}`", text)

    if "## Agent Report" in text:
        text = re.sub(r"## Agent Report.*", report, text, flags=re.DOTALL)
    else:
        text = text.rstrip() + "\n\n" + report

    path.write_text(text)
    log(f"UPDATED  {path.name}  state={new_state}  decision={new_decision}")


# ── Parameter grid ────────────────────────────────────────────────────────────

def _bounded_grid(space: dict[str, list[Any]], max_configs: int = 32) -> list[dict[str, Any]]:
    keys = sorted(space)
    all_cfg = [dict(zip(keys, combo, strict=True)) for combo in product(*[space[k] for k in keys])]
    seen: set[str] = set()
    unique = [c for c in all_cfg if (k := str(sorted(c.items()))) not in seen and not seen.add(k)]  # type: ignore[func-returns-value]
    if len(unique) <= max_configs:
        return unique
    indices = sorted({round(i * (len(unique) - 1) / (max_configs - 1)) for i in range(max_configs)})
    return [unique[i] for i in indices]


def build_configs(strategy: str, mode: str, max_configs: int = 32) -> list[dict[str, Any]]:
    """Return a bounded list of parameter dicts for the strategy."""
    spaces = PARAMETER_SPACES.get(strategy)
    if not spaces:
        log(f"PARAM_SPACE  no space defined for '{strategy}', using factory default")
        return [{}]  # single run with factory defaults

    space = spaces.get(mode, spaces["discovery"])
    validator = STRATEGY_VALIDATORS.get(strategy, lambda _: True)
    all_cfg = _bounded_grid(space, max_configs=max_configs * 4)
    valid = [c for c in all_cfg if validator(c)]

    if len(valid) <= max_configs:
        return valid
    indices = sorted({round(i * (len(valid) - 1) / (max_configs - 1)) for i in range(max_configs)})
    sampled = [valid[i] for i in indices]
    log(f"PARAM_SPACE  strategy='{strategy}'  mode={mode}  valid={len(valid)}  sampled={len(sampled)}")
    return sampled


# ── Data loading ──────────────────────────────────────────────────────────────

def load_frames(
    tickers: list[str],
    start: date,
    end: date,
    strategies: list,
) -> dict[str, pl.DataFrame]:
    storage = LocalStorage()
    physics = PhysicsEngine()
    needed = required_feature_union(strategies)
    frames: dict[str, pl.DataFrame] = {}
    for ticker in tickers:
        raw = storage.load_bars(ticker, start, end)
        if raw.is_empty():
            log(f"SKIP_NO_DATA  {ticker}")
            continue
        frames[ticker] = physics.enrich_for_features(raw, needed)
        log(f"LOADED  {ticker}  rows={frames[ticker].height}")
    return frames


# ── Stage helpers ─────────────────────────────────────────────────────────────

def run_m1(
    *,
    strategy_name: str,
    frames: dict[str, pl.DataFrame],
    windows: list,
    configs: list[dict[str, Any]],
    metrics: MetricsCalculator,
    top_per_ticker: int = 2,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    detail_rows: list[dict] = []
    aggregate_rows: list[dict] = []

    for idx, config in enumerate(configs, 1):
        s = build_strategy(strategy_name, config)
        log(f"M1  {idx}/{len(configs)}  {config}")
        for ticker, frame in frames.items():
            rows = run_walk_forward_for_strategies(
                ticker=ticker, df=frame, strategies=[s], windows=windows,
                ratios=RATIOS, metrics=metrics, min_signals=MIN_SIGNALS, cost_bps=M1_COST_BPS,
            )
            if not rows:
                continue
            for r in rows:
                detail_rows.append({**r, **config})
            for r in aggregate_walk_forward(rows).iter_rows(named=True):
                aggregate_rows.append({**r, **config})

    detail_df    = pl.DataFrame(detail_rows) if detail_rows else pl.DataFrame()
    aggregate_df = pl.DataFrame(aggregate_rows) if aggregate_rows else pl.DataFrame()

    if aggregate_df.is_empty():
        return detail_df, aggregate_df, pl.DataFrame()

    ranked = (
        aggregate_df
        .filter(pl.col("direction").is_in(["long", "short", "combined"]))
        .filter(pl.col("avg_test_exp_r").is_not_null() & (pl.col("avg_test_exp_r") > 0))
        .filter(pl.col("pct_positive_oos_windows").is_not_null())
        .with_columns([
            (pl.col("avg_test_exp_r") * 1000 + pl.col("pct_positive_oos_windows") * 100
             + pl.col("oos_signals") / 1000).alias("m1_score")
        ])
        .sort(["ticker", "oos_windows", "m1_score"], descending=[False, True, True])
    )
    top_rows = [
        r for ticker in frames
        for r in ranked.filter(pl.col("ticker") == ticker).head(top_per_ticker).iter_rows(named=True)
    ]
    return detail_df, aggregate_df, pl.DataFrame(top_rows) if top_rows else pl.DataFrame()


def evaluate_m1_gate(top_df: pl.DataFrame) -> tuple[bool, str]:
    if top_df.is_empty():
        return False, "no positive configs found"
    best = top_df.sort("m1_score", descending=True).row(0, named=True) if "m1_score" in top_df.columns else top_df.row(0, named=True)
    pct     = float(best.get("pct_positive_oos_windows", 0) or 0)
    exp_r   = float(best.get("avg_test_exp_r", 0) or 0)
    signals = int(best.get("oos_signals", 0) or 0)
    windows = int(best.get("oos_windows", 0) or 0)
    reasons = []
    if pct < GATE_MIN_PCT_POSITIVE:  reasons.append(f"pct_pos={pct:.0%}<{GATE_MIN_PCT_POSITIVE:.0%}")
    if signals < GATE_MIN_OOS_SIGNALS: reasons.append(f"signals={signals}<{GATE_MIN_OOS_SIGNALS}")
    if windows < GATE_MIN_OOS_WINDOWS: reasons.append(f"windows={windows}<{GATE_MIN_OOS_WINDOWS}")
    if exp_r <= 0:                     reasons.append(f"exp_r={exp_r:+.4f}<=0")
    if reasons:
        return False, "; ".join(reasons)
    return True, f"pct_pos={pct:.0%}  exp_r={exp_r:+.4f}  signals={signals}  windows={windows}"


def run_m2(
    *,
    strategy_name: str,
    frames: dict[str, pl.DataFrame],
    windows: list,
    metrics: MetricsCalculator,
    top_m1: pl.DataFrame,
    param_keys: list[str],
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    frames_list: list[pl.DataFrame] = []
    for cost in COST_GRID_BPS:
        rows: list[dict] = []
        log(f"M2  cost_bps={cost}")
        for candidate in top_m1.iter_rows(named=True):
            config = {k: candidate[k] for k in param_keys if k in candidate}
            s = build_strategy(strategy_name, config)
            ticker = str(candidate["ticker"])
            if ticker not in frames:
                continue
            wf = run_walk_forward_for_strategies(
                ticker=ticker, df=frames[ticker], strategies=[s], windows=windows,
                ratios=RATIOS, metrics=metrics, min_signals=MIN_SIGNALS, cost_bps=cost,
            )
            if not wf:
                continue
            agg = aggregate_walk_forward(wf).filter(
                pl.col("direction") == str(candidate.get("direction", "combined"))
            )
            for r in agg.iter_rows(named=True):
                rows.append({**r, "cost_bps": cost, **config})
        if rows:
            frames_list.append(pl.DataFrame(rows))

    if not frames_list:
        return pl.DataFrame(), pl.DataFrame(), pl.DataFrame()

    combined = pl.concat(frames_list, how="vertical")
    gate = build_gate_report(
        combined=combined, cost_count=len(COST_GRID_BPS),
        gate_min_oos_windows=GATE_MIN_OOS_WINDOWS, gate_min_oos_signals=GATE_MIN_OOS_SIGNALS,
        gate_min_pct_positive=GATE_MIN_PCT_POSITIVE, gate_min_exp_r=GATE_MIN_EXP_R,
    )
    promoted = promoted_candidates_from_gate_report(gate)
    log(f"M2  promoted={promoted.height}")
    return combined, gate, promoted


def run_m3(
    *,
    strategy_name: str,
    frames: dict[str, pl.DataFrame],
    windows: list,
    metrics: MetricsCalculator,
    promoted_m2: pl.DataFrame,
    param_keys: list[str],
) -> pl.DataFrame:
    rows: list[dict] = []
    for candidate in promoted_m2.iter_rows(named=True):
        try:
            s = build_candidate_strategy(candidate)
        except Exception:
            config = {k: candidate[k] for k in param_keys if k in candidate}
            s = build_strategy(strategy_name, config)
        ticker = str(candidate["ticker"])
        if ticker not in frames:
            continue
        wf = run_walk_forward_for_strategies(
            ticker=ticker, df=frames[ticker], strategies=[s], windows=windows,
            ratios=RATIOS, metrics=metrics, min_signals=MIN_SIGNALS, cost_bps=M1_COST_BPS,
        )
        rows.extend(wf)
    return pl.DataFrame(rows) if rows else pl.DataFrame()


def run_m4(
    *,
    frames: dict[str, pl.DataFrame],
    metrics: MetricsCalculator,
    promoted_m2: pl.DataFrame,
    start: date,
    calibration_end: date,
    holdout_start: date,
    holdout_end: date,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    rows = run_holdout_validation_for_candidates(
        promoted=promoted_m2, ticker_frames=frames, metrics=metrics,
        start_date=start, calibration_end=calibration_end,
        holdout_start=holdout_start, holdout_end=holdout_end,
        ratios=RATIOS, costs=COST_GRID_BPS,
        min_calibration_signals=MIN_CALIBRATION_SIGNALS,
        min_holdout_signals=MIN_HOLDOUT_SIGNALS,
    )
    detail = pl.DataFrame(rows) if rows else pl.DataFrame()
    summary = summarize_holdout(detail, cost_count=len(COST_GRID_BPS)) if rows else pl.DataFrame()
    promoted = promoted_candidates_from_holdout(summary) if not summary.is_empty() else pl.DataFrame()
    log(f"M4  detail={detail.height}  promoted={promoted.height}")
    return detail, promoted


def run_m5(
    *,
    frames: dict[str, pl.DataFrame],
    metrics: MetricsCalculator,
    m4_promoted: pl.DataFrame,
    m4_detail: pl.DataFrame,
    holdout_start: date,
    holdout_end: date,
) -> pl.DataFrame:
    rows = run_execution_mapping_for_candidates(
        promoted=m4_promoted, holdout_detail=m4_detail, ticker_frames=frames,
        metrics=metrics, holdout_start=holdout_start, holdout_end=holdout_end,
        base_cost_r=BASE_COST_R,
        stress_cfg=ExecutionStressConfig(bootstrap_iters=BOOTSTRAP_ITERS),
    )
    df = pl.DataFrame(rows) if rows else pl.DataFrame()
    log(f"M5  mapped={df.height}")
    return df


# ── Artifact resumption ───────────────────────────────────────────────────────

def _latest_run_dir(out_dir: Path, hypothesis_id: str) -> Path | None:
    base = out_dir / hypothesis_id
    if not base.exists():
        return None
    dirs = sorted((d for d in base.iterdir() if d.is_dir()), key=lambda d: d.name, reverse=True)
    return dirs[0] if dirs else None


def _load_csv(run_dir: Path | None, filename: str) -> pl.DataFrame:
    if run_dir is None:
        return pl.DataFrame()
    p = run_dir / filename
    return pl.read_csv(p) if p.exists() else pl.DataFrame()


# ── Logging ───────────────────────────────────────────────────────────────────

def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


# ── Agent report ──────────────────────────────────────────────────────────────

def build_report(
    *,
    run_ts: str,
    hypothesis_id: str,
    strategy: str,
    stages_run: list[str],
    decision: str,
    notes: list[str],
    artifact_dir: str,
) -> str:
    return f"""## Agent Report
### Run
`{run_ts}` — strategy: `{strategy}`

### Stages Executed
`{" → ".join(stages_run) if stages_run else "none"}`

### Notes
{chr(10).join(f"- {n}" for n in notes)}

### Decision
`{decision}`

### Artifacts
`{artifact_dir}`
"""


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hypothesis", required=True,
                        help="Path to hypothesis .md (relative to repo root or absolute)")
    parser.add_argument("--max-stage", choices=["M1", "M2", "M3", "M4", "M5"], default=None,
                        help="Override max_stage from hypothesis file")
    parser.add_argument("--tickers", nargs="+", default=None,
                        help="Override symbol_scope from hypothesis file")
    parser.add_argument("--start",           type=date.fromisoformat, default=DEFAULT_START)
    parser.add_argument("--calibration-end", type=date.fromisoformat, default=DEFAULT_CAL_END)
    parser.add_argument("--holdout-start",   type=date.fromisoformat, default=DEFAULT_HOLDOUT_START)
    parser.add_argument("--holdout-end",     type=date.fromisoformat, default=DEFAULT_HOLDOUT_END)
    parser.add_argument("--end",             type=date.fromisoformat, default=DEFAULT_END)
    parser.add_argument("--top-per-ticker",  type=int, default=2)
    parser.add_argument("--out-dir",         default="data/results/hypothesis_runs")
    parser.add_argument("--dry-run",         action="store_true")
    parser.add_argument("--google-credentials", default=None,
                        help="Path to Google service-account JSON (enables Strategy_Catalog write on promote)")
    parser.add_argument("--catalog-sheet-id", default=None,
                        help="Override spreadsheet ID for Strategy_Catalog")
    return parser.parse_args()


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    args = parse_args()

    hyp_path = Path(args.hypothesis)
    if not hyp_path.is_absolute():
        hyp_path = REPO_ROOT / hyp_path
    if not hyp_path.exists():
        log(f"ERROR  not found: {hyp_path}")
        sys.exit(1)

    h = parse_hypothesis(hyp_path)
    tickers   = args.tickers or h.tickers
    max_stage = args.max_stage or h.max_stage
    strategy  = h.strategy

    log(f"HYPOTHESIS  id={h.id}  state={h.state}  strategy={strategy}")
    log(f"TICKERS  {tickers}  max_stage={max_stage}")

    if h.state in ("completed", "kill"):
        log(f"SKIP  state is '{h.state}'")
        return

    mode = "retune" if h.state == "retune" else "discovery"
    configs = build_configs(strategy, mode)
    param_keys = sorted(configs[0].keys()) if configs and configs[0] else []

    # Determine which stage to start from (resumption)
    start_stage = "M1"
    if h.state == "running" and h.decision in DECISION_TO_STAGE:
        start_stage = DECISION_TO_STAGE[h.decision]

    if args.dry_run:
        log(f"DRY_RUN  mode={mode}  configs={len(configs)}  start_from={start_stage}")
        return

    # Load previous artifacts for resumption
    prev_dir = _latest_run_dir(REPO_ROOT / args.out_dir, h.id) if start_stage != "M1" else None
    top_m1      = _load_csv(prev_dir, "M1_top.csv")
    promoted_m2 = _load_csv(prev_dir, "M2_promoted.csv")

    if start_stage in ("M3", "M4", "M5") and promoted_m2.is_empty():
        log("WARN  M2_promoted.csv not found in previous run, restarting from M1")
        start_stage = "M1"
    elif start_stage == "M2" and top_m1.is_empty():
        log("WARN  M1_top.csv not found in previous run, restarting from M1")
        start_stage = "M1"

    out_dir = REPO_ROOT / args.out_dir / h.id / datetime.now().strftime("%Y-%m-%dT%H%M%S")
    out_dir.mkdir(parents=True, exist_ok=True)
    run_ts = out_dir.name
    log(f"ARTIFACTS  {out_dir}")

    # Build strategies for data loading (needed for Newton feature planning)
    all_strategies = [build_strategy(strategy, c) for c in (configs if configs[0] else [{}])]

    frames_cal  = load_frames(tickers, args.start, args.calibration_end, all_strategies)
    frames_full = load_frames(tickers, args.start, args.end, all_strategies)

    if not frames_cal:
        log("ERROR  no data for any ticker")
        sys.exit(1)

    metrics = MetricsCalculator()
    windows = build_windows(args.start, args.calibration_end, TRAIN_MONTHS, TEST_MONTHS)
    log(f"WINDOWS  {len(windows)}  (train={TRAIN_MONTHS}m / test={TEST_MONTHS}m)")

    stages_run: list[str] = []
    notes:      list[str] = []
    decision = ""
    m5_df: pl.DataFrame = pl.DataFrame()  # populated when M5 runs; read by finish()

    STAGES = ["M1", "M2", "M3", "M4", "M5"]
    active_stages = STAGES[STAGES.index(start_stage):STAGES.index(max_stage) + 1]

    def finish(d: str) -> None:
        nonlocal decision
        decision = d
        decision_to_state = {
            "promote":      "completed",
            "kill":         "kill",
            "retune":       "retune",
        }
        new_state = decision_to_state.get(d, "running")
        report = build_report(
            run_ts=run_ts, hypothesis_id=h.id, strategy=strategy,
            stages_run=stages_run, decision=d, notes=notes, artifact_dir=str(out_dir),
        )
        update_hypothesis(hyp_path, new_state=new_state, new_decision=d, report=report)

        if d == "promote":
            creds = args.google_credentials or settings.google_api_credentials_path
            sheet_id = args.catalog_sheet_id or settings.strategy_catalog_sheet_id
            if creds and sheet_id and not m5_df.is_empty():
                # Pick best M5 row: prefer debit_spread_default, rank by mc_prob_positive_exp
                ranked = m5_df
                if "execution_profile" in m5_df.columns:
                    primary = m5_df.filter(pl.col("execution_profile") == "debit_spread_default")
                    ranked = primary if not primary.is_empty() else m5_df
                if "mc_prob_positive_exp" in ranked.columns:
                    ranked = ranked.sort("mc_prob_positive_exp", descending=True)
                m5_best = ranked.row(0, named=True)
                try:
                    upsert_strategy_catalog(
                        catalog_key=h.id,
                        symbol=str(m5_best.get("ticker", ", ".join(tickers))),
                        strategy=strategy,
                        m5_best=m5_best,
                        spreadsheet_id=sheet_id,
                        credentials_path=creds,
                        sheet_name=settings.strategy_catalog_sheet_name,
                    )
                    log(f"CATALOG  upserted  catalog_key={h.id}")
                except Exception as exc:
                    log(f"CATALOG_WARN  Strategy_Catalog write failed: {exc}")
            else:
                log("CATALOG_SKIP  no google credentials configured — skipping Strategy_Catalog write")

    # ── M1 ────────────────────────────────────────────────────────────────────
    if "M1" in active_stages:
        log("─" * 56)
        log("STAGE M1  discovery/retune sweep")
        detail_m1, agg_m1, top_m1 = run_m1(
            strategy_name=strategy, frames=frames_cal, windows=windows,
            configs=configs, metrics=metrics, top_per_ticker=args.top_per_ticker,
        )
        stages_run.append("M1")
        if not detail_m1.is_empty(): detail_m1.write_csv(out_dir / "M1_detail.csv")
        if not agg_m1.is_empty():    agg_m1.write_csv(out_dir / "M1_aggregate.csv")
        if not top_m1.is_empty():    top_m1.write_csv(out_dir / "M1_top.csv")

        passes, reason = evaluate_m1_gate(top_m1)
        notes.append(f"M1 {'PASS' if passes else 'FAIL'}: {reason}")
        log(f"M1  {'PASS' if passes else 'FAIL'}  {reason}")

        if not passes:
            any_pos = (not agg_m1.is_empty() and "avg_test_exp_r" in agg_m1.columns
                       and agg_m1.filter(pl.col("avg_test_exp_r") > 0).height > 0)
            finish("retune" if any_pos else "kill")
            return

    if "M2" not in active_stages:
        finish("promote_to_m2")
        return

    # ── M2 ────────────────────────────────────────────────────────────────────
    log("─" * 56)
    log("STAGE M2  convergence grid")
    combined_m2, gate_m2, promoted_m2 = run_m2(
        strategy_name=strategy, frames=frames_cal, windows=windows,
        metrics=metrics, top_m1=top_m1, param_keys=param_keys,
    )
    stages_run.append("M2")
    if not combined_m2.is_empty():  combined_m2.write_csv(out_dir / "M2_convergence.csv")
    if not gate_m2.is_empty():      gate_m2.write_csv(out_dir / "M2_gate_report.csv")
    if not promoted_m2.is_empty():  promoted_m2.write_csv(out_dir / "M2_promoted.csv")

    notes.append(f"M2: {promoted_m2.height} candidates promoted")
    if promoted_m2.is_empty():
        finish("retune")
        return

    if "M3" not in active_stages:
        finish("promote_to_m3")
        return

    # ── M3 ────────────────────────────────────────────────────────────────────
    log("─" * 56)
    log("STAGE M3  walk-forward OOS")
    m3_df = run_m3(
        strategy_name=strategy, frames=frames_cal, windows=windows,
        metrics=metrics, promoted_m2=promoted_m2, param_keys=param_keys,
    )
    stages_run.append("M3")
    if not m3_df.is_empty(): m3_df.write_csv(out_dir / "M3_walk_forward.csv")
    notes.append(f"M3: {m3_df.height} detail rows")

    if "M4" not in active_stages:
        finish("promote_to_m4")
        return

    # ── M4 ────────────────────────────────────────────────────────────────────
    log("─" * 56)
    log("STAGE M4  holdout validation")
    m4_detail, m4_promoted = run_m4(
        frames=frames_full, metrics=metrics, promoted_m2=promoted_m2,
        start=args.start, calibration_end=args.calibration_end,
        holdout_start=args.holdout_start, holdout_end=args.holdout_end,
    )
    stages_run.append("M4")
    if not m4_detail.is_empty():   m4_detail.write_csv(out_dir / "M4_holdout.csv")
    if not m4_promoted.is_empty(): m4_promoted.write_csv(out_dir / "M4_promoted.csv")

    notes.append(f"M4: {m4_promoted.height} promoted")
    if m4_promoted.is_empty():
        finish("kill")
        return

    if "M5" not in active_stages:
        finish("promote_to_m5")
        return

    # ── M5 ────────────────────────────────────────────────────────────────────
    log("─" * 56)
    log("STAGE M5  execution mapping")
    m5_df = run_m5(
        frames=frames_full, metrics=metrics, m4_promoted=m4_promoted, m4_detail=m4_detail,
        holdout_start=args.holdout_start, holdout_end=args.holdout_end,
    )
    stages_run.append("M5")
    if not m5_df.is_empty(): m5_df.write_csv(out_dir / "M5_execution.csv")
    notes.append(f"M5: {m5_df.height} execution mappings")

    finish("promote" if not m5_df.is_empty() else "kill")


if __name__ == "__main__":
    main()
