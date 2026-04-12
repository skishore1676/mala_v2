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
    - search_mode:  `discovery | fixed`  (optional; fixed replays one configured strategy)
    - direction_scope: `long`  (optional comma-separated long/short/combined filter)
    - max_configs:  `32`  (optional search cap; raise for full family replay)

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
from pathlib import Path
from typing import Any

import polars as pl

REPO_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(REPO_ROOT))

from src.chronos.storage import LocalStorage
from src.config import settings
from src.newton.engine import PhysicsEngine
from src.oracle.metrics import MetricsCalculator
from src.oracle.monte_carlo import ExecutionStressConfig
from src.research.catalog import upsert_strategy_catalog
from src.research.exit_optimizer import (
    DEFAULT_CATASTROPHE_EXIT,
    ExitOptimizationResult,
    optimize_underlying_exit,
    write_exit_optimization_result,
)
from src.research.market_regime import MarketRegime, classify_range as _classify_regime_range
from src.research.search_space import build_search_configs, search_param_keys
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
from src.research.strategy_keys import to_strategy_key
from src.strategy.base import required_feature_union
from src.strategy.factory import build_strategy, build_strategy_by_name


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
    search_mode: str
    directions: list[str]
    max_configs: int


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
    search_mode = _field("search_mode", "")
    direction_scope = _field("direction_scope", "")
    max_configs_raw = _field("max_configs", "32")
    try:
        max_configs = max(1, int(max_configs_raw))
    except ValueError:
        max_configs = 32

    tickers = [t.strip() for t in scope.split(",") if t.strip()]
    directions = [d.strip() for d in direction_scope.split(",") if d.strip()]
    return HypothesisState(
        path=path, id=hyp_id, state=state, decision=decision,
        tickers=tickers, strategy=strategy, max_stage=max_st,
        search_mode=search_mode,
        directions=directions,
        max_configs=max_configs,
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
    directions: list[str] | None = None,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    detail_rows: list[dict] = []
    aggregate_rows: list[dict] = []

    for idx, config in enumerate(configs, 1):
        s = _build_configured_strategy(strategy_name, config)
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

    direction_filter = directions or ["long", "short", "combined"]
    ranked = (
        aggregate_df
        .filter(pl.col("direction").is_in(direction_filter))
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
            s = _build_configured_strategy(strategy_name, config)
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
            s = _build_configured_strategy(strategy_name, config)
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


def _latest_run_dir_with_file(out_dir: Path, hypothesis_id: str, filename: str) -> Path | None:
    base = out_dir / hypothesis_id
    if not base.exists():
        return None
    dirs = sorted((d for d in base.iterdir() if d.is_dir()), key=lambda d: d.name, reverse=True)
    for run_dir in dirs:
        if (run_dir / filename).exists():
            return run_dir
    return None


def _load_csv(run_dir: Path | None, filename: str) -> pl.DataFrame:
    if run_dir is None:
        return pl.DataFrame()
    p = run_dir / filename
    return pl.read_csv(p) if p.exists() else pl.DataFrame()


def _strategy_family_name(strategy_name: str) -> str:
    if strategy_name.startswith("Elastic Band z="):
        return "Elastic Band Reversion"
    if strategy_name.startswith("Kinematic Ladder rw="):
        return "Kinematic Ladder"
    return strategy_name


def _build_configured_strategy(strategy_name: str, config: dict[str, Any]):
    return build_strategy(_strategy_family_name(strategy_name), config)


def _best_m5_row(m5_df: pl.DataFrame) -> dict[str, Any] | None:
    """Pick the row that will represent the promoted candidate."""
    if m5_df.is_empty():
        return None
    ranked = m5_df
    if "execution_profile" in m5_df.columns:
        primary = m5_df.filter(pl.col("execution_profile") == "debit_spread_default")
        ranked = primary if not primary.is_empty() else m5_df
    if "mc_prob_positive_exp" in ranked.columns:
        ranked = ranked.sort("mc_prob_positive_exp", descending=True)
    return ranked.row(0, named=True)


def _matching_promoted_candidate(
    promoted: pl.DataFrame,
    m5_best: dict[str, Any],
    param_keys: list[str],
) -> dict[str, Any] | None:
    if promoted.is_empty():
        return None
    keys = ["ticker", "strategy", "direction", *param_keys]
    for candidate in promoted.iter_rows(named=True):
        if all(
            _values_match(candidate.get(key), m5_best.get(key))
            for key in keys
            if key in candidate and key in m5_best
        ):
            return candidate
    return promoted.row(0, named=True)


def _values_match(left: Any, right: Any) -> bool:
    if left == right:
        return True
    if left is None or right is None:
        return False
    try:
        return float(left) == float(right)
    except (TypeError, ValueError):
        return str(left) == str(right)


# ── Logging ───────────────────────────────────────────────────────────────────

def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


# ── Regime tagging ────────────────────────────────────────────────────────────

def _tag_regime_cols(
    df: pl.DataFrame,
    regime_map: dict[date, MarketRegime],
    date_col: str,
) -> pl.DataFrame:
    """Left-join market regime columns onto df using date_col as the key.

    Adds: market_regime_key, vix_band, spy_trend_20d, session_type.
    date_col must contain ISO date strings or polars Date values.
    """
    if df.is_empty() or not regime_map or date_col not in df.columns:
        return df

    regime_rows = [
        {
            "_rdate": r.trading_date.isoformat(),
            "market_regime_key": r.regime_key,
            "vix_band":          r.vix_band,
            "spy_trend_20d":     r.spy_trend_20d,
            "session_type":      r.session_type,
        }
        for r in regime_map.values()
    ]
    regime_df = pl.DataFrame(regime_rows)

    # Coerce date_col to string for the join
    col_type = df.schema.get(date_col)
    if col_type == pl.Date:
        df = df.with_columns(pl.col(date_col).cast(pl.Utf8).alias("_join_date"))
        join_col = "_join_date"
    else:
        join_col = date_col

    result = df.join(
        regime_df.rename({"_rdate": join_col}),
        on=join_col,
        how="left",
    )
    if join_col == "_join_date":
        result = result.drop("_join_date")
    return result


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


def _format_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.4f}"
    return str(value)


def _markdown_table(rows: list[dict[str, Any]], columns: list[str]) -> str:
    if not rows:
        return "_No rows._"
    header = "| " + " | ".join(columns) + " |"
    divider = "| " + " | ".join("---" for _ in columns) + " |"
    body = [
        "| " + " | ".join(_format_value(row.get(column)) for column in columns) + " |"
        for row in rows
    ]
    return "\n".join([header, divider, *body])


def _read_artifact(out_dir: Path, filename: str) -> pl.DataFrame:
    path = out_dir / filename
    return pl.read_csv(path) if path.exists() else pl.DataFrame()


def _candidate_columns(df: pl.DataFrame) -> list[str]:
    return [
        column for column in [
            "ticker",
            "direction",
            "strategy",
            "z_score_threshold",
            "z_score_window",
            "use_directional_mass",
            "use_jerk_confirmation",
            "kinematic_periods_back",
        ]
        if column in df.columns
    ]


def write_run_summary(
    *,
    out_dir: Path,
    hypothesis: HypothesisState,
    stages_run: list[str],
    decision: str,
    notes: list[str],
) -> Path:
    sections: list[str] = [
        f"# Run Summary: {hypothesis.id}",
        "",
        f"- strategy: `{hypothesis.strategy}`",
        f"- symbols: `{', '.join(hypothesis.tickers)}`",
        f"- directions: `{', '.join(hypothesis.directions) if hypothesis.directions else 'long, short, combined'}`",
        f"- stages: `{' -> '.join(stages_run) if stages_run else 'none'}`",
        f"- decision: `{decision}`",
        "",
        "## Notes",
        *(f"- {note}" for note in notes),
    ]

    m1_top = _read_artifact(out_dir, "M1_top.csv")
    if not m1_top.is_empty():
        columns = [
            column for column in [
                "ticker",
                "direction",
                "strategy",
                "avg_test_exp_r",
                "pct_positive_oos_windows",
                "oos_signals",
            ]
            if column in m1_top.columns
        ]
        rows = m1_top.select(columns).sort(
            [column for column in ["avg_test_exp_r", "oos_signals"] if column in columns],
            descending=True,
        ).head(12).to_dicts()
        sections.extend(["", "## M1 Top Candidates", _markdown_table(rows, columns)])

    m2_promoted = _read_artifact(out_dir, "M2_promoted.csv")
    if not m2_promoted.is_empty():
        columns = _candidate_columns(m2_promoted)
        sections.extend([
            "",
            f"## M2 Promoted ({m2_promoted.height})",
            _markdown_table(m2_promoted.select(columns).head(20).to_dicts(), columns),
        ])

    m4_holdout = _read_artifact(out_dir, "M4_holdout.csv")
    if not m4_holdout.is_empty():
        group_cols = _candidate_columns(m4_holdout)
        summary = (
            m4_holdout.group_by(group_cols)
            .agg([
                pl.len().alias("cost_points"),
                pl.col("holdout_signals").min().alias("min_signals"),
                pl.col("holdout_exp_r").min().alias("min_exp_r"),
                pl.col("holdout_exp_r").mean().alias("mean_exp_r"),
                pl.col("passes_cost_gate").sum().alias("passed_cost_points"),
                pl.col("passes_cost_gate").all().alias("passed_all_costs"),
            ])
            .sort(["passed_all_costs", "mean_exp_r"], descending=[True, True])
        )
        columns = [
            *group_cols,
            "cost_points",
            "min_signals",
            "min_exp_r",
            "mean_exp_r",
            "passed_cost_points",
            "passed_all_costs",
        ]
        sections.extend([
            "",
            "## M4 Holdout Read",
            _markdown_table(summary.select(columns).head(15).to_dicts(), columns),
        ])

    m5 = _read_artifact(out_dir, "M5_execution.csv")
    if not m5.is_empty():
        columns = [
            column for column in [
                "ticker",
                "direction",
                "strategy",
                "execution_profile",
                "selected_ratio",
                "base_exp_r",
                "mc_exp_r_p50",
                "mc_prob_positive_exp",
                "mc_max_dd_p50",
            ]
            if column in m5.columns
        ]
        rows = m5.select(columns).sort(
            [column for column in ["mc_prob_positive_exp", "base_exp_r"] if column in columns],
            descending=True,
        ).head(12).to_dicts()
        sections.extend(["", "## M5 Execution Read", _markdown_table(rows, columns)])

    path = out_dir / "RUN_SUMMARY.md"
    path.write_text("\n".join(sections).rstrip() + "\n")
    return path


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
    parser.add_argument("--max-configs",     type=int, default=None,
                        help="Override hypothesis max_configs search cap")
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
    if h.directions:
        log(f"DIRECTIONS  {h.directions}")

    if h.state in ("completed", "kill"):
        log(f"SKIP  state is '{h.state}'")
        return

    mode = h.search_mode or ("retune" if h.state == "retune" else "discovery")
    max_configs = args.max_configs or h.max_configs
    configs = build_search_configs(strategy, mode=mode, max_configs=max_configs)
    param_keys = search_param_keys(strategy)

    # Determine which stage to start from (resumption)
    start_stage = "M1"
    if h.state == "running" and h.decision in DECISION_TO_STAGE:
        start_stage = DECISION_TO_STAGE[h.decision]

    if args.dry_run:
        log(f"DRY_RUN  mode={mode}  configs={len(configs)}  max_configs={max_configs}  start_from={start_stage}")
        return

    # Load previous artifacts for resumption. Pick the newest run containing
    # each required artifact so a partial later run does not hide valid state.
    previous_root = REPO_ROOT / args.out_dir
    top_m1 = _load_csv(
        _latest_run_dir_with_file(previous_root, h.id, "M1_top.csv") if start_stage != "M1" else None,
        "M1_top.csv",
    )
    promoted_m2 = _load_csv(
        _latest_run_dir_with_file(previous_root, h.id, "M2_promoted.csv") if start_stage != "M1" else None,
        "M2_promoted.csv",
    )
    m4_detail = _load_csv(
        _latest_run_dir_with_file(previous_root, h.id, "M4_holdout.csv") if start_stage == "M5" else None,
        "M4_holdout.csv",
    )
    m4_promoted = _load_csv(
        _latest_run_dir_with_file(previous_root, h.id, "M4_promoted.csv") if start_stage == "M5" else None,
        "M4_promoted.csv",
    )

    if start_stage in ("M3", "M4", "M5") and promoted_m2.is_empty():
        log("WARN  M2_promoted.csv not found in previous run, restarting from M1")
        start_stage = "M1"
    elif start_stage == "M5" and m4_promoted.is_empty():
        log("WARN  M4_promoted.csv not found in previous run, restarting from M4")
        start_stage = "M4"
    elif start_stage == "M2" and top_m1.is_empty():
        log("WARN  M1_top.csv not found in previous run, restarting from M1")
        start_stage = "M1"

    out_dir = REPO_ROOT / args.out_dir / h.id / datetime.now().strftime("%Y-%m-%dT%H%M%S")
    out_dir.mkdir(parents=True, exist_ok=True)
    run_ts = out_dir.name
    log(f"ARTIFACTS  {out_dir}")

    # Build strategies for data loading (needed for Newton feature planning)
    if mode == "fixed":
        all_strategies = [build_strategy_by_name(strategy)]
    else:
        all_strategies = [_build_configured_strategy(strategy, c) for c in (configs if configs[0] else [{}])]

    frames_cal  = load_frames(tickers, args.start, args.calibration_end, all_strategies)
    frames_full = load_frames(tickers, args.start, args.end, all_strategies)

    if not frames_cal:
        log("ERROR  no data for any ticker")
        sys.exit(1)

    metrics = MetricsCalculator()
    windows = build_windows(args.start, args.calibration_end, TRAIN_MONTHS, TEST_MONTHS)
    log(f"WINDOWS  {len(windows)}  (train={TRAIN_MONTHS}m / test={TEST_MONTHS}m)")

    # Regime map — computed once, used to tag all detail artifacts (observational)
    regime_map: dict[date, MarketRegime] = {}
    try:
        regime_map = _classify_regime_range(args.start, args.end)
        log(f"REGIME  classified {len(regime_map)} days")
    except Exception as exc:
        log(f"REGIME_WARN  {exc}")

    stages_run: list[str] = []
    notes:      list[str] = []
    decision = ""
    m5_df: pl.DataFrame = pl.DataFrame()              # populated by M5 stage
    m5_exit_opt: ExitOptimizationResult | None = None  # populated by exit optimizer

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
        summary_path = write_run_summary(
            out_dir=out_dir,
            hypothesis=h,
            stages_run=stages_run,
            decision=d,
            notes=notes,
        )
        log(f"SUMMARY  {summary_path}")
        report = build_report(
            run_ts=run_ts, hypothesis_id=h.id, strategy=strategy,
            stages_run=stages_run, decision=d, notes=notes, artifact_dir=str(out_dir),
        )
        update_hypothesis(hyp_path, new_state=new_state, new_decision=d, report=report)

        if d == "promote":
            creds = args.google_credentials or settings.google_api_credentials_path
            sheet_id = args.catalog_sheet_id or settings.strategy_catalog_sheet_id
            if creds and sheet_id and not m5_df.is_empty():
                m5_best = _best_m5_row(m5_df)
                if m5_best is None:
                    log("CATALOG_SKIP  no M5 row available for Strategy_Catalog write")
                    return
                try:
                    upsert_strategy_catalog(
                        catalog_key=h.id,
                        symbol=str(m5_best.get("ticker", ", ".join(tickers))),
                        strategy=strategy,
                        m5_best=m5_best,
                        spreadsheet_id=sheet_id,
                        credentials_path=creds,
                        sheet_name=settings.strategy_catalog_sheet_name,
                        exit_opt=m5_exit_opt.model_dump(mode="json") if m5_exit_opt else None,
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
            directions=h.directions,
        )
        stages_run.append("M1")
        if not detail_m1.is_empty():
            _tag_regime_cols(detail_m1, regime_map, "test_start").write_csv(out_dir / "M1_detail.csv")
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
    if "M2" in active_stages:
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
    if "M3" in active_stages:
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
    if "M4" in active_stages:
        log("─" * 56)
        log("STAGE M4  holdout validation")
        m4_detail, m4_promoted = run_m4(
            frames=frames_full, metrics=metrics, promoted_m2=promoted_m2,
            start=args.start, calibration_end=args.calibration_end,
            holdout_start=args.holdout_start, holdout_end=args.holdout_end,
        )
        stages_run.append("M4")
        if not m4_detail.is_empty():
            _tag_regime_cols(m4_detail, regime_map, "trade_date").write_csv(out_dir / "M4_holdout.csv")
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

    # ── Exit optimization (M5-plus) ───────────────────────────────────────────
    if not m5_df.is_empty() and not m4_promoted.is_empty():
        m5_best = _best_m5_row(m5_df)
        best_cand = _matching_promoted_candidate(m4_promoted, m5_best or {}, param_keys)
        if m5_best and best_cand:
            _ticker    = str(m5_best.get("ticker", best_cand.get("ticker", tickers[0] if tickers else "SPY")))
            _direction = str(m5_best.get("direction", best_cand.get("direction", "long")))
            _config    = {k: best_cand[k] for k in param_keys if k in best_cand}
            _skey      = to_strategy_key(str(m5_best.get("strategy", strategy)))
        else:
            _ticker = ""
            _direction = ""
            _config = {}
            _skey = to_strategy_key(strategy)
        if _ticker in frames_full and _direction in {"long", "short"}:
            try:
                m5_exit_opt = optimize_underlying_exit(
                    strategy_key=_skey,
                    symbol=_ticker,
                    direction=_direction,
                    strategy=_build_configured_strategy(strategy, _config),
                    enriched_frame=frames_full[_ticker],
                    holdout_start=args.holdout_start,
                    holdout_end=args.holdout_end,
                    catastrophe_exit_params=DEFAULT_CATASTROPHE_EXIT,
                )
                if m5_exit_opt:
                    write_exit_optimization_result(
                        m5_exit_opt, path=out_dir / "m5_exit_optimization.json"
                    )
                    notes.append(f"exit_opt: {m5_exit_opt.selected_policy_name}")
                    log(
                        f"EXIT_OPT  {m5_exit_opt.selected_policy_name}"
                        f"  expectancy={m5_exit_opt.selected_metrics.get('expectancy', 0):+.4f}"
                    )
            except Exception as exc:
                log(f"EXIT_OPT_WARN  {exc}")

    finish("promote" if not m5_df.is_empty() else "kill")


if __name__ == "__main__":
    main()
