#!/usr/bin/env python3
"""Option-path expectancy of each promoted candidate's assigned exit profile (S4).

For each run dir: rebuild the strategy, load+enrich bars from the local cache,
run the direction-aware option-path IV band for the strategy's *assigned* exit
profile, and report premium-% expectancy, win rate, payoff ratio, and an
account-level contribution (expectancy x the profile's max_capital_pct).

Read-only: no Sheet, no live API calls. Wave 1 / S4 analysis aid.

    python scripts/analyze_profile_options.py            # curated cross-family set
    python scripts/analyze_profile_options.py --run-dir <dir> ...
"""
from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path
from statistics import median

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "scripts"))

import polars as pl
from loguru import logger

from analyze_profile_exits import _config_for, _strategy_family_name
from src.chronos.storage import LocalStorage
from src.newton.engine import PhysicsEngine
from src.research.exit_optimizer import _holdout_signal_frame, _with_exit_policy_features
from src.research.exit_profiles import assigned_profile
from src.research.option_translation import score_profile_band
from src.research.strategy_keys import to_strategy_key
from src.strategy.base import required_feature_union
from src.strategy.factory import build_strategy

logger.remove()
logger.add(sys.stderr, level="ERROR")

START, HS, HE = date(2025, 1, 1), date(2025, 12, 1), date(2026, 2, 28)
# Per-profile capital allowance (premium basis), from the operator profiles.
MAX_CAPITAL = {"FLASH_REVERSAL": 5.0, "EXHAUSTION_REVERSAL": 4.0,
               "TREND_CONTINUATION": 6.0, "RANGE_EXPANSION": 5.0}

_EV = REPO_ROOT / "research/results/m7_mala_evidence_full/20260526T013740Z/pilot_runs"
_TS = "2026-05-22Tmala-evidence-m7"
DEFAULT_CANDIDATES = [
    _EV / "elastic-band-current-basket-discovery__iwm_short" / _TS,
    _EV / "elastic-band-current-basket-discovery__meta_short" / _TS,
    _EV / "market-impulse-all-basket-discovery__amd_short" / _TS,
    _EV / "jerk-pivot-current-basket-discovery__tsla_short" / _TS,
    _EV / "opening-drive-current-basket-discovery__nvda_short" / _TS,
    _EV / "compression-breakout-current-basket-discovery__amd_short" / _TS,
]


def analyze(run_dir: Path) -> dict:
    cat = run_dir / "CATALOG_SELECTED.csv"
    if not cat.exists():
        return {"run_dir": run_dir.name, "error": "no CATALOG_SELECTED.csv"}
    row = pl.read_csv(cat, infer_schema_length=10000).row(0, named=True)
    name = _strategy_family_name(str(row["strategy"]))
    symbol, direction = str(row["ticker"]), str(row["direction"])
    profile = assigned_profile(to_strategy_key(name))
    if profile is None:
        return {"symbol": symbol, "strategy": name, "error": "no assigned profile"}
    strat = build_strategy(name, _config_for(row, name))
    raw = LocalStorage().load_bars(symbol, START, HE)
    if raw.is_empty():
        return {"symbol": symbol, "strategy": name, "error": "no cached data"}
    frame = PhysicsEngine().enrich_for_features(raw, required_feature_union([strat]))
    filt = _holdout_signal_frame(_with_exit_policy_features(strat.generate_signals(frame.clone())),
                                 direction, HS, HE)
    band = {b["scenario"]: b for b in score_profile_band(filt, direction, profile)}
    lev = band["leverage"]
    if lev["n"] == 0:
        return {"symbol": symbol, "strategy": name, "error": "no signals in holdout"}
    payoff = (lev["avg_win_pct"] / abs(lev["avg_loss_pct"])) if lev["avg_loss_pct"] else float("inf")
    return {
        "strategy": name, "symbol": symbol, "direction": direction, "profile": profile,
        "exp_pct": lev["expectancy_pct"], "win": lev["win_rate"], "payoff": payoff,
        "acct_pct_per_trade": lev["expectancy_pct"] / 100.0 * MAX_CAPITAL[profile],
        "band_lo": min(band[s]["expectancy_pct"] for s in band),
        "band_hi": max(band[s]["expectancy_pct"] for s in band),
        "n": lev["n"],
    }


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--run-dir", action="append", default=None, type=Path)
    args = p.parse_args()
    rows = []
    for rd in (args.run_dir or DEFAULT_CANDIDATES):
        print(f"... {Path(rd).name}", flush=True)
        rows.append(analyze(Path(rd)))

    print(f"\n{'strategy':>32s} {'sym':>5s} {'profile':>20s} {'exp%/t':>7s} {'win':>5s} "
          f"{'payoff':>7s} {'acct%/t':>8s} {'band[lo..hi]':>16s} {'n':>4s}")
    ok = []
    for r in rows:
        if r.get("error"):
            print(f"{r.get('strategy','?')[:32]:>32s} {r.get('symbol','?'):>5s}  ERROR: {r['error']}")
            continue
        ok.append(r)
        print(f"{r['strategy'][:32]:>32s} {r['symbol']:>5s} {r['profile']:>20s} "
              f"{r['exp_pct']:>+7.1f} {r['win']:>5.2f} {r['payoff']:>7.2f} "
              f"{r['acct_pct_per_trade']:>+8.3f} {r['band_lo']:>+7.1f}..{r['band_hi']:>-5.1f} {r['n']:>4d}")
    if ok:
        pos = sum(1 for r in ok if r["exp_pct"] > 0)
        print(f"\n  {pos}/{len(ok)} positive (leverage). "
              f"median exp%={median(r['exp_pct'] for r in ok):+.1f}  "
              f"median acct%/trade={median(r['acct_pct_per_trade'] for r in ok):+.3f}  "
              f"median win={median(r['win'] for r in ok):.2f}")


if __name__ == "__main__":
    main()
