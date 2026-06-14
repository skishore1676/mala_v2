#!/usr/bin/env python3
"""Calibrate the S4 modeled IV against kamandal's real captured IV.

kamandal (`kamandal_v2.db:iv_snapshots`, written by its `capture-iv` job) is the
IV store; this reads it and computes the empirical
``iv_premium_factor = real_IV / realized_vol`` (realized vol from the local bar
cache), flagging symbols whose real factor diverges from the modeled default
(1.2x in option_translation). Read-only — mala_v2 no longer collects IV itself.

    python scripts/calibrate_iv.py --symbols IWM,TSLA,NVDA,AMD,GE
    python scripts/calibrate_iv.py --db /path/to/kamandal_v2.db
"""
from __future__ import annotations

import argparse
import sys
from datetime import date, timedelta
from pathlib import Path
from statistics import median

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from loguru import logger

from src.chronos.storage import LocalStorage
from src.research.kamandal_iv import DEFAULT_KAMANDAL_DB, latest_atm_iv
from src.research.option_translation import annualized_realized_vol

logger.remove()
logger.add(sys.stderr, level="ERROR")

MODELED_FACTOR = 1.2
SURPRISE_BAND = (0.9, 1.6)  # real factor outside this (vs modeled) -> flag


def compute_factor(real_iv: float | None, realized_vol: float | None) -> float | None:
    if not real_iv or not realized_vol or realized_vol <= 0:
        return None
    return real_iv / realized_vol


def realized_vol_for(symbol: str, asof: date, lookback_days: int = 60) -> float | None:
    try:
        bars = LocalStorage().load_bars(symbol, asof - timedelta(days=lookback_days), asof)
    except Exception:
        return None
    return annualized_realized_vol(bars) if not bars.is_empty() else None


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--symbols", default="IWM,SPY,TSLA,NVDA,AMD,GE")
    p.add_argument("--db", default=str(DEFAULT_KAMANDAL_DB))
    args = p.parse_args()
    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]

    if not Path(args.db).exists():
        print(f"kamandal db not found: {args.db}\nRun kamandal `capture-iv` first.")
        return

    print(f"Calibrate vs kamandal real IV  (modeled factor = {MODELED_FACTOR})  db={args.db}\n")
    print(f"  {'sym':>5s} {'snap_date':>11s} {'metric':>18s} {'real_IV':>8s} {'realized':>9s} {'factor':>7s} {'note':>14s}")
    factors = []
    for sym in symbols:
        res = latest_atm_iv(sym, db_path=args.db)
        if res is None:
            print(f"  {sym:>5s} {'—':>11s} {'—':>18s} {'—':>8s} {'—':>9s} {'—':>7s} {'no captured IV':>14s}")
            continue
        iv, snap, metric = res
        rv = realized_vol_for(sym, snap)
        f = compute_factor(iv, rv)
        note = ""
        if f is None:
            note = "no cache bars"
        elif not (SURPRISE_BAND[0] <= f / MODELED_FACTOR <= SURPRISE_BAND[1]):
            note = "** surprise"
        if f is not None:
            factors.append(f)
        print(f"  {sym:>5s} {snap.isoformat():>11s} {metric:>18s} {iv:>8.3f} "
              f"{(rv if rv else float('nan')):>9.3f} {(f if f else float('nan')):>7.2f} {note:>14s}")

    if factors:
        m = median(factors)
        print(f"\n  real iv_premium_factor: median={m:.2f}  range={min(factors):.2f}-{max(factors):.2f}  (modeled {MODELED_FACTOR})")
        verdict = ("modeled is close" if abs(m - MODELED_FACTOR) < 0.15
                   else "modeled too RICH — lower it" if m < MODELED_FACTOR else "modeled too CHEAP — raise it")
        print(f"  -> {verdict}. Pass use_real_iv=True / symbol=... to score on real per-symbol IV.")
    else:
        print("\n  No factors — kamandal has no IV for these symbols yet (run `capture-iv` daily).")


if __name__ == "__main__":
    main()
