#!/usr/bin/env python3
"""Compare best legacy exit vs best operator-profile exit for promoted candidates.

For each run dir (containing CATALOG_SELECTED.csv) this rebuilds the strategy,
loads + enriches bars from the local cache, runs the profile-augmented exit
optimizer over the holdout window, and reports whether the operator exit
profiles change or improve the selected exit (best-legacy vs best-profile,
apples-to-apples within one rerun).

Read-only: writes nothing and never touches the Google Sheet. Wave 1 analysis
aid for docs/EXIT_PROFILE_PLAYBOOKS.md.

Usage:
    python scripts/analyze_profile_exits.py                 # default candidate set
    python scripts/analyze_profile_exits.py --run-dir <dir> [--run-dir <dir> ...]
    python scripts/analyze_profile_exits.py --start 2025-01-01
"""
from __future__ import annotations

import argparse
import inspect
import sys
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

import polars as pl
from loguru import logger

from src.chronos.storage import LocalStorage
from src.newton.engine import PhysicsEngine
from src.research.exit_optimizer import optimize_underlying_exit
from src.research.exit_profiles import assigned_profile
from src.research.strategy_keys import to_strategy_key
from src.strategy.base import required_feature_union
from src.strategy.factory import build_strategy

logger.remove()
logger.add(sys.stderr, level="WARNING")

DEFAULT_START = date(2025, 1, 1)
HOLDOUT_START = date(2025, 12, 1)
HOLDOUT_END = date(2026, 2, 28)

_EVIDENCE = REPO_ROOT / "research/results/m7_mala_evidence_full/20260526T013740Z/pilot_runs"
_RUN_TS = "2026-05-22Tmala-evidence-m7"
DEFAULT_CANDIDATES = [
    _EVIDENCE / "market-impulse-all-basket-discovery__pltr_short" / _RUN_TS,
    _EVIDENCE / "jerk-pivot-current-basket-discovery__tsla_short" / _RUN_TS,
    _EVIDENCE / "elastic-band-current-basket-discovery__iwm_short" / _RUN_TS,
    _EVIDENCE / "compression-breakout-current-basket-discovery__amd_short" / _RUN_TS,
]


def _strategy_family_name(name: str) -> str:
    if name.startswith("Elastic Band z="):
        return "Elastic Band Reversion"
    if name.startswith("Kinematic Ladder rw="):
        return "Kinematic Ladder"
    return name


def _config_for(row: dict, strategy_name: str) -> dict:
    base = build_strategy(strategy_name, {})
    accepted = set(inspect.signature(type(base).__init__).parameters) - {"self"}
    return {k: v for k, v in row.items() if k in accepted and v not in (None, "")}


def _opt(evaluation) -> float:
    return float(evaluation.metrics.get("option_adjusted_expectancy_pct", float("-inf")))


def analyze(run_dir: Path, start: date) -> dict:
    cat_path = run_dir / "CATALOG_SELECTED.csv"
    if not cat_path.exists():
        return {"run_dir": run_dir.name, "error": "no CATALOG_SELECTED.csv"}
    row = pl.read_csv(cat_path, infer_schema_length=10000).row(0, named=True)
    raw_name = str(row["strategy"])
    strategy_name = _strategy_family_name(raw_name)
    symbol, direction = str(row["ticker"]), str(row["direction"])
    config = _config_for(row, strategy_name)

    strat = build_strategy(strategy_name, config)
    raw = LocalStorage().load_bars(symbol, start, HOLDOUT_END)
    if raw.is_empty():
        return {"symbol": symbol, "strategy": raw_name, "error": "no cached data"}
    frame = PhysicsEngine().enrich_for_features(raw, required_feature_union([strat]))
    result = optimize_underlying_exit(
        strategy_key=raw_name, symbol=symbol, direction=direction, strategy=strat,
        enriched_frame=frame, holdout_start=HOLDOUT_START, holdout_end=HOLDOUT_END,
    )
    if result is None:
        return {"symbol": symbol, "strategy": raw_name, "error": "no signals in holdout"}

    ranked = sorted(result.candidate_policies, key=_opt, reverse=True)
    legacy = [e for e in ranked if not e.policy_name.startswith("profile:")]
    profiles = [e for e in ranked if e.policy_name.startswith("profile:")]
    best_legacy = legacy[0] if legacy else None
    best_profile = profiles[0] if profiles else None
    assigned = assigned_profile(to_strategy_key(raw_name))
    assigned_eval = next(
        (e for e in profiles if e.policy_name == f"profile:{(assigned or '').lower()}"), None
    )
    assigned_rank = (ranked.index(assigned_eval) + 1) if assigned_eval else None

    return {
        "symbol": symbol, "direction": direction, "strategy": raw_name,
        "selected": result.selected_policy_name,
        "selected_is_profile": result.selected_policy_name.startswith("profile:"),
        "best_legacy": best_legacy.policy_name if best_legacy else None,
        "best_legacy_opt": _opt(best_legacy) if best_legacy else None,
        "best_profile": best_profile.policy_name if best_profile else None,
        "best_profile_opt": _opt(best_profile) if best_profile else None,
        "assigned_profile": assigned,
        "assigned_opt": _opt(assigned_eval) if assigned_eval else None,
        "assigned_rank": assigned_rank,
        "n_candidates": len(ranked),
        "trade_count": (best_profile or best_legacy).metrics.get("trade_count") if ranked else 0,
    }


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--run-dir", action="append", default=None, type=Path)
    p.add_argument("--start", type=date.fromisoformat, default=DEFAULT_START)
    args = p.parse_args()
    run_dirs = args.run_dir or DEFAULT_CANDIDATES

    print(f"Holdout {HOLDOUT_START}..{HOLDOUT_END}  |  warmup from {args.start}\n")
    rows = []
    for run_dir in run_dirs:
        print(f"... {run_dir.name}", flush=True)
        rows.append(analyze(Path(run_dir), args.start))

    print(f"\n{'strategy':>34s} {'sym':>5s} {'dir':>5s} {'best_legacy':>30s} {'leg_opt':>8s} "
          f"{'best_profile':>26s} {'prof_opt':>8s} {'assigned(rank)':>22s} {'SELECTED':>26s}")
    for r in rows:
        if r.get("error"):
            print(f"{r.get('strategy','?')[:34]:>34s} {r.get('symbol','?'):>5s}  --  ERROR: {r['error']}")
            continue
        assigned = f"{r['assigned_profile']}(#{r['assigned_rank']})" if r['assigned_profile'] else "-"
        print(f"{r['strategy'][:34]:>34s} {r['symbol']:>5s} {r['direction']:>5s} "
              f"{str(r['best_legacy'])[:30]:>30s} {r['best_legacy_opt']:>+8.4f} "
              f"{str(r['best_profile'])[:26]:>26s} {r['best_profile_opt']:>+8.4f} "
              f"{assigned[:22]:>22s} {str(r['selected'])[:26]:>26s}")


if __name__ == "__main__":
    main()
