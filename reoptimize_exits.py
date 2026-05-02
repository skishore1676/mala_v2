#!/usr/bin/env python3
"""
reoptimize_exits.py — Exit-Only Optimizer Against Existing M5 Artifacts
========================================================================
Re-runs the exit optimizer on M5 catalog candidates from an existing run
without touching M1-M5 discovery, hypothesis state files, or the Google
Strategy_Catalog (unless --catalog-write is explicitly set).

Typical use case: you added new exit policy families to exit_optimizer.py
and want to see whether they improve the selected exits for already-validated
candidates, without forcing a full M1→M5 rediscovery.

Usage:
    # Re-optimize the latest run for a hypothesis (dry run first):
    python reoptimize_exits.py --hypothesis research/hypotheses/market-impulse-all-basket-discovery.md --dry-run

    # Re-optimize and overwrite exit artifacts in-place:
    python reoptimize_exits.py --hypothesis research/hypotheses/market-impulse-all-basket-discovery.md

    # Target a specific run directory:
    python reoptimize_exits.py --hypothesis research/hypotheses/jerk-pivot-current-basket-discovery.md \\
        --run-dir data/results/hypothesis_runs/jerk-pivot-current-basket-discovery/2026-04-15T225844

    # Re-optimize AND write results back to Google Strategy_Catalog (explicit opt-in):
    python reoptimize_exits.py --hypothesis research/hypotheses/market-impulse-all-basket-discovery.md \\
        --catalog-write

What it does:
    1. Parses the hypothesis file to get strategy, tickers, guardrail settings.
    2. Finds the target run directory (--run-dir or the latest run with M5_execution.csv).
    3. Loads M5_execution.csv and M4_promoted.csv from that directory.
    4. Re-downloads / re-enriches data from the local Parquet cache (no new API calls
       if the cache is warm; if data is absent the Polygon pull is triggered).
    5. For each catalog candidate (mc_prob >= min threshold), calls
       optimize_underlying_exit() with the full current policy grid.
    6. Writes updated per-candidate JSON artifacts and m5_exit_optimizations.json
       to the SAME run directory, overwriting old exit artifacts.
    7. Optionally upserts to Strategy_Catalog only when --catalog-write is set.

What it does NOT do:
    - Does not re-run M1, M2, M3, M4, or M5.
    - Does not modify the hypothesis .md state file.
    - Does not write to the Google Sheet unless --catalog-write is passed.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import polars as pl
import yaml

REPO_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(REPO_ROOT))

_CONFIG_PATH = REPO_ROOT / "config" / "hypothesis_defaults.yaml"


def _load_config() -> dict[str, Any]:
    if _CONFIG_PATH.exists():
        return yaml.safe_load(_CONFIG_PATH.read_text()) or {}
    return {}


_CFG = _load_config()

from src.chronos.storage import LocalStorage
from src.config import settings
from src.newton.engine import PhysicsEngine
from src.research.catalog import upsert_strategy_catalog
from src.research.exit_optimizer import (
    DEFAULT_CATASTROPHE_EXIT,
    ExitOptimizationResult,
    optimize_underlying_exit,
    write_exit_optimization_result,
)
from src.research.search_space import search_param_keys
from src.research.strategy_keys import to_strategy_key
from src.strategy.base import required_feature_union
from src.strategy.factory import build_strategy


# ── Config defaults ───────────────────────────────────────────────────────────

_dates = _CFG.get("dates", {})
DEFAULT_START         = date.fromisoformat(_dates.get("start", "2024-01-02"))
DEFAULT_HOLDOUT_START = date.fromisoformat(_dates.get("holdout_start", "2025-12-01"))
DEFAULT_HOLDOUT_END   = date.fromisoformat(_dates.get("holdout_end", "2026-02-28"))
DEFAULT_END           = DEFAULT_HOLDOUT_END

_exec = _CFG.get("execution", {})
ENTRY_DELAY_BARS           = _exec.get("entry_delay_bars", 0)
MIN_HOLD_BARS              = _exec.get("min_hold_bars", 0)
COOLDOWN_BARS_AFTER_SIGNAL = _exec.get("cooldown_bars_after_signal", 0)

_cat = _CFG.get("catalog", {})
MIN_MC_PROB_FOR_CATALOG = _cat.get("min_mc_prob_for_catalog", 0.70)
EXECUTION_PROFILE_PRIORITY = [
    str(p) for p in _cat.get(
        "execution_profile_priority",
        ["single_option", "debit_spread_tight", "debit_spread_default", "stock_like"],
    )
    if str(p).strip()
] or ["single_option", "debit_spread_tight", "debit_spread_default", "stock_like"]


# ── Logging ───────────────────────────────────────────────────────────────────

def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


# ── Hypothesis parsing (minimal — only what we need) ─────────────────────────

def _field(text: str, name: str, default: str = "") -> str:
    m = re.search(rf"- {name}:\s*`([^`]*)`", text)
    return m.group(1).strip() if m else default


def _parse_hypothesis(path: Path) -> dict[str, Any]:
    text = path.read_text()
    scope = _field(text, "symbol_scope", "SPY")
    tickers = [t.strip() for t in scope.split(",") if t.strip()]
    return {
        "id":       _field(text, "id", path.stem),
        "strategy": _field(text, "strategy", "Opening Drive Classifier"),
        "tickers":  tickers,
        "state":    _field(text, "state", "pending"),
    }


# ── Run directory resolution ──────────────────────────────────────────────────

def _find_run_dir(out_root: Path, hypothesis_id: str) -> Path | None:
    """Return the latest run directory that contains M5_execution.csv."""
    base = out_root / hypothesis_id
    if not base.exists():
        return None
    dirs = sorted(
        (d for d in base.iterdir() if d.is_dir() and (d / "M5_execution.csv").exists()),
        key=lambda d: d.name,
        reverse=True,
    )
    return dirs[0] if dirs else None


# ── Candidate helpers ─────────────────────────────────────────────────────────

def _format_value(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, float):
        return f"{v:.4f}"
    return str(v)


def _candidate_key(
    row: dict[str, Any],
    param_keys: list[str],
) -> tuple[tuple[str, str], ...]:
    keys = ["ticker", "strategy", "direction", *param_keys]
    return tuple(
        (k, _format_value(row.get(k)))
        for k in keys
        if k in row
    )


def _candidate_slug(row: dict[str, Any], param_keys: list[str]) -> str:
    payload = json.dumps(dict(_candidate_key(row, param_keys)), sort_keys=True)
    return hashlib.sha1(payload.encode()).hexdigest()[:10]


def _feature_cache_key(strategy_name: str, config: dict[str, Any]) -> str:
    strategy = _build_strategy(strategy_name, config)
    return "|".join(sorted(required_feature_union([strategy])))


def _summary_merge_key(item: dict[str, Any]) -> str:
    key = item.get("candidate_key", {})
    if isinstance(key, dict) and key:
        return json.dumps(key, sort_keys=True)
    return str(item.get("artifact", ""))


def _best_m5_row(m5_df: pl.DataFrame) -> dict[str, Any] | None:
    if m5_df.is_empty():
        return None
    for profile in EXECUTION_PROFILE_PRIORITY:
        sub = m5_df.filter(pl.col("execution_profile") == profile)
        if not sub.is_empty():
            return sub.sort("mc_prob_positive_exp", descending=True).row(0, named=True)
    return m5_df.sort("mc_prob_positive_exp", descending=True).row(0, named=True)


def _catalog_candidates(
    m5_df: pl.DataFrame,
    min_mc_prob: float,
) -> list[dict[str, Any]]:
    if m5_df.is_empty():
        return []
    results = []
    for td in (
        m5_df.select(["ticker", "direction"]).unique().sort(["ticker", "direction"]).iter_rows(named=True)
    ):
        row = _best_m5_row(
            m5_df.filter(
                (pl.col("ticker") == td["ticker"]) & (pl.col("direction") == td["direction"])
            )
        )
        if row and float(row.get("mc_prob_positive_exp", 0) or 0) >= min_mc_prob:
            results.append(row)
    return results


def _matching_promoted(
    promoted: pl.DataFrame,
    m5_best: dict[str, Any],
    param_keys: list[str],
) -> dict[str, Any] | None:
    if promoted.is_empty():
        return None
    for c in promoted.iter_rows(named=True):
        if all(
            _format_value(c.get(k)) == _format_value(m5_best.get(k))
            for k in ["ticker", "strategy", "direction", *param_keys]
            if k in c and k in m5_best
        ):
            return c
    return promoted.row(0, named=True)


# ── Strategy builder ──────────────────────────────────────────────────────────

def _strategy_family_name(name: str) -> str:
    if name.startswith("Elastic Band z="):
        return "Elastic Band Reversion"
    if name.startswith("Kinematic Ladder rw="):
        return "Kinematic Ladder"
    return name


def _build_strategy(strategy_name: str, config: dict[str, Any]):
    return build_strategy(_strategy_family_name(strategy_name), config)


# ── Data loading ──────────────────────────────────────────────────────────────

def _load_enriched_frames(
    tickers: list[str],
    strategy_name: str,
    config: dict[str, Any],
    start: date,
    end: date,
) -> dict[str, pl.DataFrame]:
    storage = LocalStorage()
    physics = PhysicsEngine()
    s = _build_strategy(strategy_name, config)
    needed = required_feature_union([s])
    frames: dict[str, pl.DataFrame] = {}
    for ticker in tickers:
        raw = storage.load_bars(ticker, start, end)
        if raw.is_empty():
            log(f"SKIP_NO_DATA  {ticker}")
            continue
        frames[ticker] = physics.enrich_for_features(raw, needed)
        log(f"LOADED  {ticker}  rows={frames[ticker].height}")
    return frames


# ── Main ──────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--hypothesis", required=True,
                   help="Path to hypothesis .md file (relative to repo root or absolute)")
    p.add_argument("--run-dir", default=None,
                   help="Specific run directory to target. Default: latest run with M5_execution.csv")
    p.add_argument("--out-dir", default="data/results/hypothesis_runs",
                   help="Root for hypothesis run directories")
    p.add_argument("--start",          type=date.fromisoformat, default=DEFAULT_START)
    p.add_argument("--holdout-start",  type=date.fromisoformat, default=DEFAULT_HOLDOUT_START)
    p.add_argument("--holdout-end",    type=date.fromisoformat, default=DEFAULT_HOLDOUT_END)
    p.add_argument("--end",            type=date.fromisoformat, default=DEFAULT_END)
    p.add_argument("--min-mc-prob",    type=float, default=None,
                   help=f"Minimum mc_prob to include a candidate (default: {MIN_MC_PROB_FOR_CATALOG})")
    p.add_argument("--catalog-write",  action="store_true",
                   help="Write updated exit policies to Google Strategy_Catalog. "
                        "Requires GOOGLE_API_CREDENTIALS_PATH and STRATEGY_CATALOG_SHEET_ID. "
                        "Default: off (never writes to Sheet).")
    p.add_argument("--google-credentials", default=None,
                   help="Path to Google service-account JSON")
    p.add_argument("--catalog-sheet-id", default=None,
                   help="Override spreadsheet ID for Strategy_Catalog")
    p.add_argument("--dry-run", action="store_true",
                   help="Print what would be done without running the optimizer or writing files")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    min_mc_prob = args.min_mc_prob if args.min_mc_prob is not None else MIN_MC_PROB_FOR_CATALOG

    # ── Resolve hypothesis ────────────────────────────────────────────────────
    hyp_path = Path(args.hypothesis)
    if not hyp_path.is_absolute():
        hyp_path = REPO_ROOT / hyp_path
    if not hyp_path.exists():
        log(f"ERROR  hypothesis not found: {hyp_path}")
        sys.exit(1)

    h = _parse_hypothesis(hyp_path)
    strategy = h["strategy"]
    tickers  = h["tickers"]
    log(f"HYPOTHESIS  id={h['id']}  strategy={strategy}  state={h['state']}")
    log(f"TICKERS  {tickers}")

    # ── Resolve run directory ─────────────────────────────────────────────────
    out_root = REPO_ROOT / args.out_dir
    if args.run_dir:
        run_dir = Path(args.run_dir)
        if not run_dir.is_absolute():
            run_dir = REPO_ROOT / run_dir
    else:
        run_dir = _find_run_dir(out_root, h["id"])

    if run_dir is None or not run_dir.exists():
        log(f"ERROR  no run directory with M5_execution.csv found for {h['id']}")
        log(f"       Run hypothesis_agent.py --hypothesis {hyp_path} first.")
        sys.exit(1)

    log(f"RUN_DIR  {run_dir}")

    # ── Load M5 artifacts ─────────────────────────────────────────────────────
    m5_csv = run_dir / "M5_execution.csv"
    m4_csv = run_dir / "M4_promoted.csv"
    if not m5_csv.exists():
        log(f"ERROR  M5_execution.csv not found in {run_dir}")
        sys.exit(1)

    m5_df = pl.read_csv(m5_csv)
    m4_promoted = pl.read_csv(m4_csv) if m4_csv.exists() else pl.DataFrame()
    log(f"M5  {m5_df.height} rows  M4_promoted  {m4_promoted.height} rows")

    param_keys = search_param_keys(strategy)
    candidates = _catalog_candidates(m5_df, min_mc_prob)

    if not candidates:
        log(f"NO_CANDIDATES  no rows with mc_prob >= {min_mc_prob:.0%}")
        sys.exit(0)

    log(f"CANDIDATES  {len(candidates)} to re-optimize")
    for c in candidates:
        log(f"  {c['ticker']} {c['direction']}  mc_prob={float(c.get('mc_prob_positive_exp',0)):.1%}")

    if args.dry_run:
        log("DRY_RUN  no files written, no catalog upsert")
        return

    # ── Data cache ────────────────────────────────────────────────────────────
    # Candidates inside one run may require different Newton feature sets
    # (e.g. Market Impulse 15m vs 1h regime columns). Load per exact strategy
    # config so exit backfills do not reuse an incompatible enriched frame.
    frame_cache: dict[tuple[str, str], pl.DataFrame] = {}

    def _frame_for_candidate(ticker: str, config: dict[str, Any]) -> pl.DataFrame | None:
        cache_key = (ticker, _feature_cache_key(strategy, config))
        if cache_key not in frame_cache:
            frames = _load_enriched_frames([ticker], strategy, config, args.start, args.end)
            if ticker not in frames:
                return None
            frame_cache[cache_key] = frames[ticker]
        return frame_cache[cache_key]

    # ── Re-run exit optimizer ─────────────────────────────────────────────────
    exit_opts: dict[tuple[tuple[str, str], ...], ExitOptimizationResult] = {}
    rebuilt = 0

    for m5_best in candidates:
        ticker    = str(m5_best.get("ticker", ""))
        direction = str(m5_best.get("direction", ""))

        if direction not in {"long", "short"}:
            log(f"SKIP  {ticker} {direction}  — combined direction skipped (not evaluable by exit optimizer)")
            continue

        best_cand = _matching_promoted(m4_promoted, m5_best, param_keys)
        if not best_cand:
            log(f"SKIP  {ticker} {direction}  — no matching M4 promoted config")
            continue

        config = {k: best_cand[k] for k in param_keys if k in best_cand}
        strategy_key = to_strategy_key(str(m5_best.get("strategy", strategy)))

        try:
            s = _build_strategy(strategy, config)
        except Exception as exc:
            log(f"SKIP  {ticker} {direction}  — strategy build error: {exc}")
            continue
        enriched = _frame_for_candidate(ticker, config)
        if enriched is None:
            log(f"SKIP  {ticker} {direction}  — no data frame")
            continue

        log(f"OPTIMIZING  {ticker} {direction}  strategy_key={strategy_key}")
        try:
            result = optimize_underlying_exit(
                strategy_key=strategy_key,
                symbol=ticker,
                direction=direction,
                strategy=s,
                enriched_frame=enriched,
                holdout_start=args.holdout_start,
                holdout_end=args.holdout_end,
                catastrophe_exit_params=DEFAULT_CATASTROPHE_EXIT,
                entry_delay_bars=ENTRY_DELAY_BARS,
                min_hold_bars=MIN_HOLD_BARS,
                cooldown_bars_after_signal=COOLDOWN_BARS_AFTER_SIGNAL,
            )
        except Exception as exc:
            log(f"WARN  {ticker} {direction}  — optimizer error: {exc}")
            continue

        if not result:
            log(f"WARN  {ticker} {direction}  — optimizer returned no result (empty holdout slice?)")
            continue

        ckey = _candidate_key(m5_best, param_keys)
        exit_opts[ckey] = result
        rebuilt += 1

        # Write per-candidate artifact (overwrites existing file for this candidate)
        slug = _candidate_slug(m5_best, param_keys)
        artifact_path = run_dir / f"m5_exit_optimization_{ticker.lower()}_{direction}_{slug}.json"
        write_exit_optimization_result(result, path=artifact_path)
        log(
            f"WRITTEN  {artifact_path.name}"
            f"  policy={result.selected_policy_name}"
            f"  exp={result.selected_metrics.get('expectancy', 0):+.4f}"
            f"  trades={result.selected_metrics.get('trade_count', 0)}"
        )

    if not exit_opts:
        log("WARN  no exit optimization results produced — artifacts unchanged")
        return

    # ── Update m5_exit_optimizations.json ────────────────────────────────────
    summary_path = run_dir / "m5_exit_optimizations.json"
    existing_summary: list[dict[str, Any]] = []
    if summary_path.exists():
        try:
            payload = json.loads(summary_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            payload = []
        if isinstance(payload, list):
            existing_summary = [item for item in payload if isinstance(item, dict)]
    rebuilt_summary = [
        {
            "candidate_key": dict(key),
            "artifact": (
                f"m5_exit_optimization_{opt.symbol.lower()}_{opt.direction}_"
                f"{hashlib.sha1(json.dumps(dict(key), sort_keys=True).encode()).hexdigest()[:10]}.json"
            ),
            "selected_policy_name": opt.selected_policy_name,
            "selected_metrics": opt.selected_metrics,
        }
        for key, opt in exit_opts.items()
    ]
    by_key = {_summary_merge_key(item): item for item in existing_summary}
    for item in rebuilt_summary:
        by_key[_summary_merge_key(item)] = item
    summary = list(by_key.values())
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    log(f"SUMMARY  {summary_path}  ({rebuilt} rebuilt, {len(summary)} total candidates)")

    # ── Optional catalog upsert ───────────────────────────────────────────────
    if not args.catalog_write:
        log("CATALOG_SKIP  --catalog-write not set — Google Sheet unchanged")
        log("              Re-run with --catalog-write to push updated exits to Strategy_Catalog.")
        return

    creds    = args.google_credentials or settings.google_api_credentials_path
    sheet_id = args.catalog_sheet_id or settings.strategy_catalog_sheet_id
    if not creds or not sheet_id:
        log("CATALOG_SKIP  --catalog-write set but no credentials/sheet-id found — check .env")
        return

    written = 0
    for ckey, result in exit_opts.items():
        ckey_dict = dict(ckey)
        ticker    = ckey_dict.get("ticker", "")
        direction = ckey_dict.get("direction", "")
        catalog_key = f"{h['id']}__{ticker.lower()}_{direction}"

        # Find the corresponding M5 row to pass to the upsert
        m5_row = _best_m5_row(
            m5_df.filter(
                (pl.col("ticker") == ticker) & (pl.col("direction") == direction)
            )
        )
        if not m5_row:
            log(f"CATALOG_SKIP  {catalog_key}  — no M5 row found")
            continue

        try:
            upsert_strategy_catalog(
                catalog_key=catalog_key,
                symbol=ticker,
                strategy=strategy,
                m5_best=m5_row,
                spreadsheet_id=sheet_id,
                credentials_path=creds,
                sheet_name=settings.strategy_catalog_sheet_name,
                exit_opt=result.model_dump(mode="json"),
            )
            written += 1
            log(f"CATALOG  upserted  {catalog_key}  policy={result.selected_policy_name}")
        except Exception as exc:
            log(f"CATALOG_WARN  {catalog_key}: {exc}")

    log(f"CATALOG  {written} rows upserted to Strategy_Catalog")


if __name__ == "__main__":
    main()
