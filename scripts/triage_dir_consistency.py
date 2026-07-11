#!/usr/bin/env python3
"""Direction-consistency re-scorer for the Tier-B shortlist (docs/COMPLETE_TRIAGE_PROGRAM.md).

FIX (adversarial finding 2026-07-10): the Tier-A "robust" flag counted a symbol robust if ANY
config/direction passed a prior era — direction-blind. A candidate can be recommended SHORT while
only its LONG side survived the prior regimes (e.g. AMD short: e1/e2 promoted long, only e3 short).
That is recent-regime overfitting mislabeled as robust.

This rescorer, per Tier-B directional candidate, checks whether the SAME direction was funnel-promoted
in a prior era (e1_bear2022 / e2_bull2324), by reading that era's stored M4_holdout.csv. A candidate
is `dir_robust` only if its direction cleared holdout (all cost points) in >=1 prior era. The corrected
shortlist requires yardstick AND dir_robust AND FDR.

READ-ONLY. No re-run needed — reads existing wave + tierb artifacts.

Usage:
    ./.venv/bin/python scripts/triage_dir_consistency.py --tierb <wave>__tierb.csv
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

import polars as pl  # noqa: E402

from scripts.triage_tierb import WIN_LO, WIN_HI, MIN_MC_PROB, FDR_Q, bh_fdr  # noqa: E402

WAVES = REPO / "data" / "results" / "triage_waves"
PRIOR_ERAS = ("e1_bear2022", "e2_bull2324")


def promoted_dirs(fam: str, sym: str, era: str) -> set[str]:
    """Directions the funnel promoted past M4 (all cost points pass) in this wave cell."""
    hid = f"wave-{fam}-{sym}-{era}".lower()
    base = WAVES / f"{fam}__{sym}__{era}" / hid
    run_dirs = sorted(base.glob("2*"), reverse=True) if base.exists() else []
    if not run_dirs:
        return set()
    m4 = run_dirs[0] / "M4_holdout.csv"
    if not m4.exists():
        return set()
    df = pl.read_csv(m4)
    if "direction" not in df.columns or "passes_cost_gate" not in df.columns:
        return set()
    cfg = [c for c in ["direction", "entry_buffer_minutes", "entry_window_minutes",
                       "regime_timeframe", "vwma_periods"] if c in df.columns]
    agg = df.group_by(cfg).agg(pl.col("passes_cost_gate").min().alias("all_pass"))
    return set(agg.filter(pl.col("all_pass"))["direction"].to_list())


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--tierb", required=True)
    args = ap.parse_args()
    tb = pl.read_csv(args.tierb, infer_schema_length=2000)
    ok = tb.filter(pl.col("status") == "ok").filter(pl.col("option_adj_exp_pct").is_not_null())

    rows = []
    for r in ok.iter_rows(named=True):
        d = r["direction"]
        prior = [e for e in PRIOR_ERAS if d in promoted_dirs(r["family"], r["symbol"], e)]
        r["dir_robust_eras"] = len(prior)
        r["dir_robust"] = len(prior) >= 1
        rows.append(r)

    # yardstick (same as Tier-B) + direction-consistency + FDR over full scored set
    def yard(r):
        wr = r.get("holdout_win_rate")
        return (r["option_adj_exp_pct"] > 0 and wr is not None and WIN_LO <= wr <= WIN_HI
                and (r.get("payoff") or 0) > 1 and (r.get("mc_prob") or 0) >= MIN_MC_PROB
                and r.get("recommendation_tier") in ("shadow", "promote"))
    pvals = [1.0 - float(r.get("mc_prob") or 0.0) for r in rows]
    keep = bh_fdr(pvals, FDR_Q)
    for r, k in zip(rows, keep):
        r["fdr_keep"] = k

    yardstick = [r for r in rows if yard(r)]
    dir_ok = [r for r in yardstick if r["dir_robust"]]
    shortlist = sorted([r for r in dir_ok if r["fdr_keep"]],
                       key=lambda r: r["option_adj_exp_pct"], reverse=True)
    # also a broader "shadow-watch" band: yardstick + dir_robust but not FDR-kept
    watch = sorted([r for r in dir_ok if not r["fdr_keep"]],
                   key=lambda r: r["option_adj_exp_pct"], reverse=True)
    dropped_dir = sorted([r for r in yardstick if not r["dir_robust"]],
                         key=lambda r: r["option_adj_exp_pct"], reverse=True)

    out = Path(args.tierb).with_name(Path(args.tierb).stem.replace("__tierb", "") +
                                     "__shortlist_dirfixed.md")
    L = [f"# Corrected shadow shortlist (direction-consistent) — {out.stem}", "",
         f"{len(yardstick)} yardstick candidates; {len(dir_ok)} also direction-robust "
         f"(deploy-direction promoted in ≥1 prior era); {len(shortlist)} survive FDR q≤{FDR_Q}. "
         f"**{len(dropped_dir)} yardstick candidates DROPPED for direction-inconsistency** "
         "(recommended direction never survived a prior regime = recent-only edge).", "",
         "## Tier 1 — direction-robust + FDR (primary shadow set)",
         "| family | symbol | dir | opt_adj_exp | win | payoff | mc_prob | prior-era dir-support | trades |",
         "| --- | --- | --- | --- | --- | --- | --- | :-: | --- |"]
    for r in shortlist:
        L.append(f"| {r['family']} | {r['symbol']} | {r['direction']} | {r['option_adj_exp_pct']:+.3f} "
                 f"| {r['holdout_win_rate']:.2f} | {(r['payoff'] or 0):.2f} | {r['mc_prob']:.3f} "
                 f"| {r['dir_robust_eras']}/2 | {r['holdout_trades']} |")
    L += ["", "## Tier 2 — direction-robust, below FDR (shadow-watch)",
          "| family | symbol | dir | opt_adj_exp | win | payoff | mc_prob | prior-era dir-support | trades |",
          "| --- | --- | --- | --- | --- | --- | --- | :-: | --- |"]
    for r in watch:
        L.append(f"| {r['family']} | {r['symbol']} | {r['direction']} | {r['option_adj_exp_pct']:+.3f} "
                 f"| {r['holdout_win_rate']:.2f} | {(r['payoff'] or 0):.2f} | {r['mc_prob']:.3f} "
                 f"| {r['dir_robust_eras']}/2 | {r['holdout_trades']} |")
    L += ["", "## Dropped — yardstick pass but direction-INCONSISTENT (recent-only, not robust)",
          "| family | symbol | dir | opt_adj_exp | win | note |", "| --- | --- | --- | --- | --- | --- |"]
    for r in dropped_dir:
        L.append(f"| {r['family']} | {r['symbol']} | {r['direction']} | {r['option_adj_exp_pct']:+.3f} "
                 f"| {r['holdout_win_rate']:.2f} | deploy-dir never survived a prior regime |")
    out.write_text("\n".join(L) + "\n")

    print(f"[dirfix] yardstick={len(yardstick)} dir_robust={len(dir_ok)} "
          f"FDR-kept={len(shortlist)} dropped_for_direction={len(dropped_dir)}")
    print("[dirfix] TIER-1 (primary shadow):")
    for r in shortlist:
        print(f"   {r['family']}/{r['symbol']} {r['direction']}  opt_adj_exp={r['option_adj_exp_pct']:+.3f} "
              f"win={r['holdout_win_rate']:.2f} dir_support={r['dir_robust_eras']}/2 trades={r['holdout_trades']}")
    print("[dirfix] DROPPED for direction-inconsistency:")
    for r in dropped_dir:
        print(f"   {r['family']}/{r['symbol']} {r['direction']}  (was opt_adj_exp={r['option_adj_exp_pct']:+.3f})")
    print(f"[dirfix] shortlist → {out}")


if __name__ == "__main__":
    main()
