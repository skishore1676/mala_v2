#!/usr/bin/env python3
"""Complete Triage Program — statistical acceptance layer (Tier A).

Consumes a wave manifest (`triage_wave.py` output) and applies the multi-regime
gate that the base funnel lacks (docs/COMPLETE_TRIAGE_PROGRAM.md §4):

  Tier A (this script, from the wave manifest):
    - multi-regime survivor  = holdout PASS in >= MIN_ERAS of the 3 regime eras
    - single-regime          = passed in exactly 1 era  (fork: likely regime artifact)
    - dead                   = passed in 0 eras         (fork: no edge)
  Every (family, symbol) gets a recorded fork verdict — the terrain map is the
  deliverable, not just the survivors.

  Tier B (downstream, survivors only): re-run M5 + option-path + yardstick floors
    (payoff>1, win 0.45-0.60, capital-adjusted) + FDR haircut before any candidate
    is named. Tier A is the cheap filter that decides who earns Tier B compute.

Usage:
    ./.venv/bin/python scripts/triage_acceptance.py --manifest <path>
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

import polars as pl  # noqa: E402

DEPLOY_ERA = "e3_recent"       # recent calib->holdout = the deployment-realistic gate
ROBUST_ERAS = ("e1_bear2022", "e2_bull2324")  # prior-regime robustness


def verdict(deployable: bool, robust_count: int) -> tuple[str, str]:
    # A shadow candidate must work in the CURRENT regime (deployable) AND have shown edge
    # in >=1 prior regime (robust). Deploy-only = regime-fragile; robust-only = stale.
    if deployable and robust_count >= 1:
        return "SURVIVOR", "deployable + robust (Tier B candidate)"
    if deployable and robust_count == 0:
        return "DEPLOY_ONLY", "fork (c): works now, no prior-regime edge — regime-fragile"
    if not deployable and robust_count >= 1:
        return "ROBUST_ONLY", "fork (c): edge in prior regimes, dead in current — stale"
    return "DEAD", "fork (a): no durable edge"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--manifest", required=True)
    args = ap.parse_args()

    mpath = Path(args.manifest)
    df = pl.read_csv(mpath, infer_schema_length=1000)

    # Coerce numeric cols that may have blanks (str) → float.
    for col in ("best_holdout_exp_r", "best_holdout_signals"):
        if col in df.columns:
            df = df.with_columns(pl.col(col).cast(pl.Float64, strict=False).alias(col))
    # Pivot funnel-authoritative pass (any_pass) to per-era columns.
    df = df.with_columns(pl.col("any_pass").cast(pl.Boolean).alias("era_pass"))
    piv = (df.pivot(values="era_pass", index=["family", "symbol"], on="era",
                    aggregate_function="first")
           .fill_null(False))
    expr = (df.group_by(["family", "symbol"])
            .agg(pl.col("best_holdout_exp_r").max().alias("best_exp_r"),
                 pl.col("best_holdout_signals").max().alias("max_signals")))
    g = piv.join(expr, on=["family", "symbol"])

    era_cols = [c for c in g.columns if c.startswith("e")]
    rows = g.to_dicts()
    out_md = mpath.with_name(mpath.stem.replace("__manifest", "") + "__terrain.md")
    survivors, deploy_only, robust_only = [], [], []
    lines = [f"# Triage terrain map — {mpath.stem}", "",
             "Gate: **deployable** = pass `e3_recent` (recent calib→holdout, the deployment gate); "
             "**robust** = pass ≥1 of {e1_bear2022, e2_bull2324}. SURVIVOR = deployable AND robust. "
             "Pass = funnel `promote_to_m5` (authoritative).", "",
             "| family | symbol | e1_bear | e2_bull | e3_recent | best_exp_r | verdict | fork |",
             "| --- | --- | :-: | :-: | :-: | --- | --- | --- |"]

    def _p(r, era):
        return bool(r.get(era, False))
    for r in rows:
        deployable = _p(r, DEPLOY_ERA)
        robust_count = sum(1 for e in ROBUST_ERAS if _p(r, e))
        v, fork = verdict(deployable, robust_count)
        {"SURVIVOR": survivors, "DEPLOY_ONLY": deploy_only,
         "ROBUST_ONLY": robust_only}.get(v, []).append(r)
        exp = r["best_exp_r"]
        exp_s = f"{exp:+.4f}" if exp is not None else "—"
        def mk(e):
            return "✅" if _p(r, e) else "·"
        lines.append(f"| {r['family']} | {r['symbol']} | {mk('e1_bear2022')} | {mk('e2_bull2324')} "
                     f"| {mk('e3_recent')} | {exp_s} | {v} | {fork} |")

    survivors.sort(key=lambda r: (r.get("best_exp_r") or -9))
    survivors.reverse()
    lines += ["", f"**SURVIVORS (deployable + robust) → Tier B queue: {len(survivors)}**",
              f"- deploy-only (regime-fragile): {len(deploy_only)}  |  robust-only (stale): {len(robust_only)}",
              "", "_Tier B next: recent-window M1→M5 + option-path (classify_explore_propose) + "
              "yardstick (payoff>1, win 0.45–0.60) + FDR across survivors + Opus adversarial re-run._"]
    out_md.write_text("\n".join(lines) + "\n")

    # Emit a survivors CSV that the Tier-B runner consumes.
    surv_csv = mpath.with_name(mpath.stem.replace("__manifest", "") + "__survivors.csv")
    import csv as _csv
    with surv_csv.open("w", newline="") as fh:
        w = _csv.writer(fh)
        w.writerow(["family", "symbol", "e1_bear2022", "e2_bull2324", "e3_recent", "best_exp_r"])
        for r in survivors:
            w.writerow([r["family"], r["symbol"], _p(r, "e1_bear2022"), _p(r, "e2_bull2324"),
                        _p(r, "e3_recent"), r.get("best_exp_r")])

    print(f"[accept] {len(rows)} (family,symbol) pairs → {len(survivors)} SURVIVORS "
          f"(deployable+robust); {len(deploy_only)} deploy-only; {len(robust_only)} robust-only. "
          f"terrain → {out_md}")
    for r in survivors:
        flags = "".join("1" if _p(r, e) else "0" for e in ["e1_bear2022", "e2_bull2324", "e3_recent"])
        print(f"[accept]   SURVIVOR  {r['family']}/{r['symbol']}  eras(e1e2e3)={flags}  "
              f"best_exp_r={(r['best_exp_r'] or 0):+.4f}")


if __name__ == "__main__":
    main()
