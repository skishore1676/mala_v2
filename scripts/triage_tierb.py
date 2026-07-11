#!/usr/bin/env python3
"""Complete Triage Program — Tier B: tradeability gate (docs/COMPLETE_TRIAGE_PROGRAM.md §4, Gate 3).

Takes Tier-A survivors (deployable+robust) and runs each recent-window M1→M5 with the option-path
exit optimization attached. Collects CATALOG_SELECTED rows (option-adjusted expectancy, win-rate,
mc_prob, recommendation_tier) and applies the operator-DNA yardstick + a Benjamini-Hochberg FDR
haircut across the survivor set. Output = ranked shadow shortlist.

Yardstick (all must hold for a shadow candidate):
  - option_adjusted_expectancy_pct > 0          (positive on the OPTION path, not just underlying)
  - 0.45 <= holdout_win_rate <= 0.62            (asymmetric/convex, not hit-rate-carried)
  - payoff (avg_winner/avg_loser) > 1
  - mc_prob_positive_exp >= 0.70                (funnel catalog floor)
  - recommendation_tier in {shadow, promote}
  - BH-FDR q<=0.10 across the survivor set on the mc_prob-derived p-values

READ-ONLY to live: --no-catalog-write, no sheet, no active_strategy.

Usage:
    ./.venv/bin/python scripts/triage_tierb.py --survivors <wave>__survivors.csv --workers 4
"""
from __future__ import annotations

import argparse
import csv
import glob
import json
import os
import subprocess
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

import polars as pl  # noqa: E402

from scripts.triage_wave import FAMILIES  # reuse strategy-name map  # noqa: E402

RECENT = dict(start="2024-01-02", calib_end="2025-11-30",
              holdout_start="2025-12-01", holdout_end="2026-02-28")
OUT_ROOT = REPO / "data" / "results" / "triage_tierb"
WAVE_DIR = REPO / "research" / "hypotheses" / "_tierb"

# yardstick thresholds
WIN_LO, WIN_HI = 0.45, 0.62
MIN_MC_PROB = 0.70
FDR_Q = 0.10

HYP = """# Hypothesis: TIERB {fam} {sym}
## Config
- id: `{hid}`
- state: `pending`
- decision: ``
- symbol_scope: `{sym}`
- strategy: `{strategy}`
- max_stage: `M5`
- max_configs: `48`
- last_run: ``
## Thesis
Tier-B tradeability run (recent window, option-path). Family {fam} on {sym}.
## Agent Report
Pending.
"""


def run_tierb(fam: str, sym: str, polars_threads: int) -> list[dict]:
    strategy = FAMILIES[fam]
    hid = f"tierb-{fam}-{sym}".lower()
    WAVE_DIR.mkdir(parents=True, exist_ok=True)
    hyp = WAVE_DIR / f"{hid}.md"
    hyp.write_text(HYP.format(fam=fam, sym=sym, hid=hid, strategy=strategy))
    out_dir = OUT_ROOT / f"{fam}__{sym}"
    out_dir.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ, POLARS_MAX_THREADS=str(polars_threads))
    cmd = [str(REPO / ".venv" / "bin" / "python"), str(REPO / "hypothesis_agent.py"),
           "--hypothesis", str(hyp), "--tickers", sym, "--max-stage", "M5",
           "--start", RECENT["start"], "--calibration-end", RECENT["calib_end"],
           "--holdout-start", RECENT["holdout_start"], "--holdout-end", RECENT["holdout_end"],
           "--force-rerun", "--no-catalog-write", "--out-dir", str(out_dir)]
    try:
        subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=1200)
    except subprocess.TimeoutExpired:
        return [{"family": fam, "symbol": sym, "status": "timeout"}]
    run_dirs = sorted((out_dir / hid).glob("2*"), reverse=True) if (out_dir / hid).exists() else []
    if not run_dirs:
        return [{"family": fam, "symbol": sym, "status": "no_run"}]
    cat = run_dirs[0] / "CATALOG_SELECTED.csv"
    if not cat.exists():
        return [{"family": fam, "symbol": sym, "status": "killed_pre_m5"}]
    df = pl.read_csv(cat)
    out = []
    for r in df.iter_rows(named=True):
        # attach payoff from the per-config exit optimization json
        payoff = None
        opt_pct = None
        js = glob.glob(str(run_dirs[0] / f"m5_exit_optimization_{sym.lower()}_*.json"))
        if js:
            try:
                m = json.load(open(js[0])).get("selected_metrics", {})
                aw, al = m.get("avg_winner"), m.get("avg_loser")
                payoff = abs(aw / al) if aw and al else None
                opt_pct = m.get("option_adjusted_expectancy_pct")
            except Exception:  # noqa: BLE001
                pass
        out.append({
            "family": fam, "symbol": sym, "status": "ok",
            "direction": r.get("direction"),
            "recommendation_tier": r.get("recommendation_tier"),
            "option_adj_exp_pct": opt_pct,
            "holdout_win_rate": r.get("holdout_win_rate"),
            "payoff": payoff,
            "mc_prob": r.get("mc_prob_positive_exp"),
            "mc_exp_r_p50": r.get("mc_exp_r_p50"),
            "holdout_trades": r.get("holdout_trades"),
            "exit_trade_count": r.get("exit_trade_count"),
            "exit_policy": r.get("selected_exit_policy"),
            "run_dir": str(run_dirs[0]),
        })
    return out


def bh_fdr(pvals: list[float], q: float) -> list[bool]:
    """Benjamini-Hochberg: return keep-mask aligned to input order."""
    idx = sorted(range(len(pvals)), key=lambda i: pvals[i])
    m = len(pvals)
    keep = [False] * m
    kmax = -1
    for rank, i in enumerate(idx, start=1):
        if pvals[i] <= (rank / m) * q:
            kmax = rank
    for rank, i in enumerate(idx, start=1):
        if rank <= kmax:
            keep[i] = True
    return keep


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--survivors", required=True, help="path to <wave>__survivors.csv")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--polars-threads", type=int, default=2)
    args = ap.parse_args()

    with open(args.survivors) as fh:
        surv = [(row["family"], row["symbol"]) for row in csv.DictReader(fh)]
    print(f"[tierb] {len(surv)} survivors → recent-window M5 + option-path, {args.workers} workers")
    OUT_ROOT.mkdir(parents=True, exist_ok=True)

    rows: list[dict] = []
    with ProcessPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(run_tierb, f, s, args.polars_threads): (f, s) for f, s in surv}
        done = 0
        for fut in as_completed(futs):
            f, s = futs[fut]
            try:
                res = fut.result()
            except Exception as exc:  # noqa: BLE001
                res = [{"family": f, "symbol": s, "status": f"exc:{exc}"}]
            rows.extend(res)
            done += 1
            oks = [r for r in res if r.get("status") == "ok"]
            print(f"[tierb] {done}/{len(surv)}  {f}/{s}  "
                  f"{res[0].get('status')}  rows={len(oks)}")

    base = Path(args.survivors).name.replace("__survivors.csv", "")
    man = OUT_ROOT / f"{base}__tierb.csv"
    fields = ["family", "symbol", "direction", "recommendation_tier", "option_adj_exp_pct",
              "holdout_win_rate", "payoff", "mc_prob", "mc_exp_r_p50", "holdout_trades",
              "exit_trade_count", "exit_policy", "status", "run_dir"]
    with man.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)

    # Yardstick + FDR over the ok rows.
    cand = [r for r in rows if r.get("status") == "ok" and r.get("option_adj_exp_pct") is not None]
    def yard(r):
        wr = r.get("holdout_win_rate")
        return (r["option_adj_exp_pct"] > 0 and wr is not None and WIN_LO <= wr <= WIN_HI
                and (r.get("payoff") or 0) > 1 and (r.get("mc_prob") or 0) >= MIN_MC_PROB
                and r.get("recommendation_tier") in ("shadow", "promote"))
    # FDR over the FULL scored set (the real multiple-testing universe), then intersect yardstick.
    if cand:
        pvals = [1.0 - float(r.get("mc_prob") or 0.0) for r in cand]
        keep = bh_fdr(pvals, FDR_Q)
        for r, k in zip(cand, keep):
            r["fdr_keep"] = k
    passed_yard = [r for r in cand if yard(r)]
    shortlist = sorted([r for r in passed_yard if r.get("fdr_keep")],
                       key=lambda r: r["option_adj_exp_pct"], reverse=True)

    md = man.with_suffix("").as_posix() + "__shortlist.md"
    lines = [f"# Tier-B shadow shortlist — {base}", "",
             f"{len(cand)} directional candidates scored; {len(passed_yard)} pass yardstick "
             f"(option-adj exp>0, win {WIN_LO}-{WIN_HI}, payoff>1, mc_prob≥{MIN_MC_PROB}, "
             f"tier∈shadow/promote); {len(shortlist)} survive BH-FDR q≤{FDR_Q}.", "",
             "| rank | family | symbol | dir | opt_adj_exp | win | payoff | mc_prob | tier | exit | trades |",
             "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |"]
    for i, r in enumerate(shortlist, 1):
        lines.append(f"| {i} | {r['family']} | {r['symbol']} | {r['direction']} "
                     f"| {r['option_adj_exp_pct']:+.3f} | {r['holdout_win_rate']:.2f} "
                     f"| {(r['payoff'] or 0):.2f} | {r['mc_prob']:.3f} | {r['recommendation_tier']} "
                     f"| {r['exit_policy']} | {r['holdout_trades']} |")
    lines += ["", "_Next: Opus adversarial re-run on the top shortlist before the operator shadow gate. "
              "No sheet/active_strategy write happens without operator authorization._"]
    Path(md).write_text("\n".join(lines) + "\n")
    print(f"[tierb] {len(cand)} scored → {len(passed_yard)} yardstick → {len(shortlist)} FDR-kept. "
          f"manifest {man}  shortlist {md}")
    for i, r in enumerate(shortlist, 1):
        print(f"[tierb]   #{i} {r['family']}/{r['symbol']} {r['direction']}  "
              f"opt_adj_exp={r['option_adj_exp_pct']:+.3f} win={r['holdout_win_rate']:.2f} "
              f"payoff={(r['payoff'] or 0):.2f} tier={r['recommendation_tier']}")


if __name__ == "__main__":
    main()
