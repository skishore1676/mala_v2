#!/usr/bin/env python3
"""Complete Triage Program — wave harness (docs/COMPLETE_TRIAGE_PROGRAM.md).

Fans out {family × symbol × regime-era} research runs across a bounded process
pool, one isolated `hypothesis_agent.py` invocation per cell, and collects the
M4 holdout read into a flat manifest CSV. The manifest is the input to the
statistical acceptance layer (`triage_acceptance.py`).

Design invariants (from the 5-viewpoint review):
- Runs on THIS Mac only; never on oldmac (production). Compute is local + reversible.
- READ-ONLY to live: `--no-catalog-write`, never `--publish-sheets`, no sheet/active_strategy touch.
- One ephemeral hypothesis .md per (family, symbol, era) → race-free, resumable.
- POLARS_MAX_THREADS pinned per worker to avoid thread oversubscription.
- Multi-regime baked in: 3 wide eras, each ≥18mo calibration so M1 gets ≥3 OOS windows.

Usage:
    ./.venv/bin/python scripts/triage_wave.py --families trend --symbols GOOGL NFLX JPM
    ./.venv/bin/python scripts/triage_wave.py --families trend --universe all --workers 5
"""
from __future__ import annotations

import argparse
import csv
import os
import subprocess
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

import polars as pl  # noqa: E402

# Strategy factory names, grouped by exit-profile family (PROFILE_BY_STRATEGY).
FAMILIES = {
    "market_impulse": "Market Impulse (Cross & Reclaim)",
    "opening_drive": "Opening Drive Classifier",
    "jerk_pivot": "Jerk-Pivot Momentum (tight)",
    "elastic_band": "Elastic Band Reversion",
    "compression": "Compression Expansion Breakout",  # research-only (no bhiksha adapter)
}
FAMILY_GROUPS = {
    "trend": ["market_impulse", "opening_drive", "jerk_pivot"],
    "reversion": ["elastic_band"],
    "range": ["compression"],
    "all": ["market_impulse", "opening_drive", "jerk_pivot", "elastic_band", "compression"],
}

# Three wide regime eras. Each calibration span ≥18mo → M1 yields ≥3 walk-forward windows.
# Diversity, not clinical isolation, is the goal (see COMPLETE_TRIAGE_PROGRAM §4).
ERAS = {
    "e1_bear2022": dict(start="2021-06-01", calib_end="2022-12-31",
                        holdout_start="2023-01-01", holdout_end="2023-06-30"),
    "e2_bull2324": dict(start="2022-07-01", calib_end="2024-03-31",
                        holdout_start="2024-04-01", holdout_end="2024-12-31"),
    "e3_recent":   dict(start="2024-01-02", calib_end="2025-11-30",
                        holdout_start="2025-12-01", holdout_end="2026-02-28"),
}

WAVE_DIR = REPO / "research" / "hypotheses" / "_wave"
OUT_ROOT = REPO / "data" / "results" / "triage_waves"

HYP_TEMPLATE = """# Hypothesis: WAVE {fam} {sym} {era}

## Config
- id: `{hid}`
- state: `pending`
- decision: ``
- symbol_scope: `{sym}`
- strategy: `{strategy}`
- max_stage: `M4`
- max_configs: `{max_configs}`
- last_run: ``

## Thesis
Automated triage-wave cell (docs/COMPLETE_TRIAGE_PROGRAM.md). Family {fam} on {sym},
regime era {era}. Survivor detection only — not a candidate verdict.

## Agent Report
Pending.
"""


def _era_symbol_dir(fam: str, sym: str, era: str) -> Path:
    return OUT_ROOT / f"{fam}__{sym}__{era}"


def run_cell(fam: str, sym: str, era: str, max_configs: int, polars_threads: int) -> dict:
    """Run one (family, symbol, era) M1→M4 cell. Returns a manifest dict."""
    strategy = FAMILIES[fam]
    ewin = ERAS[era]
    hid = f"wave-{fam}-{sym}-{era}".lower()
    WAVE_DIR.mkdir(parents=True, exist_ok=True)
    hyp_path = WAVE_DIR / f"{hid}.md"
    hyp_path.write_text(HYP_TEMPLATE.format(
        fam=fam, sym=sym, era=era, hid=hid, strategy=strategy, max_configs=max_configs))
    out_dir = _era_symbol_dir(fam, sym, era)
    out_dir.mkdir(parents=True, exist_ok=True)

    env = dict(os.environ, POLARS_MAX_THREADS=str(polars_threads))
    cmd = [
        str(REPO / ".venv" / "bin" / "python"), str(REPO / "hypothesis_agent.py"),
        "--hypothesis", str(hyp_path), "--tickers", sym, "--max-stage", "M4",
        "--start", ewin["start"], "--calibration-end", ewin["calib_end"],
        "--holdout-start", ewin["holdout_start"], "--holdout-end", ewin["holdout_end"],
        "--force-rerun", "--no-catalog-write", "--out-dir", str(out_dir),
    ]
    row = {"family": fam, "symbol": sym, "era": era, "status": "error",
           "m1": "", "n_holdout_configs": 0, "best_holdout_exp_r": None,
           "best_holdout_signals": 0, "funnel_decision": "", "m4_promoted": 0,
           "any_pass": False, "run_dir": ""}
    try:
        proc = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=900)
    except subprocess.TimeoutExpired:
        row["status"] = "timeout"
        return row
    if proc.returncode != 0:
        row["status"] = "crash"
        row["m1"] = (proc.stderr or proc.stdout or "")[-300:]
        return row

    # Locate newest run dir for this hid and read M4_holdout if present.
    # hypothesis_agent writes to <out-dir>/<hid>/<timestamp>/ (no "hypothesis_runs" segment).
    run_dirs = sorted((out_dir / hid).glob("2*"), reverse=True) \
        if (out_dir / hid).exists() else []
    row["run_dir"] = str(run_dirs[0]) if run_dirs else ""
    m1_line = [ln for ln in proc.stdout.splitlines() if "] M1  " in ln and ("PASS" in ln or "FAIL" in ln or "KILL" in ln)]
    row["m1"] = m1_line[-1].split("] M1  ")[-1].strip() if m1_line else ""
    row["status"] = "ok"
    if run_dirs:
        _read_run_dir(run_dirs[0], row)
    return row


def _read_run_dir(run_dir: Path, row: dict) -> None:
    """Populate a manifest row from a completed run dir. AUTHORITATIVE pass signal is the
    funnel's own decision (promote_to_m5 / M4 promoted>0), NOT a reconstructed cost-gate read."""
    row["run_dir"] = str(run_dir)
    summ = run_dir / "RUN_SUMMARY.md"
    if summ.exists():
        text = summ.read_text()
        import re
        dm = re.search(r"decision:\s*`?([a-zA-Z_]+)`?", text)
        row["funnel_decision"] = dm.group(1) if dm else ""
        pm = re.search(r"M4:\s*(\d+)\s*promoted", text)
        row["m4_promoted"] = int(pm.group(1)) if pm else 0
        m1m = re.search(r"M1 PASS:\s*(.+)", text) or re.search(r"M1 (FAIL|KILL)[^\n]*", text)
        if m1m and not row.get("m1"):
            row["m1"] = m1m.group(0).split("M1 ")[-1].strip()
    # authoritative: funnel promoted this config past M4 holdout
    row["any_pass"] = row["m4_promoted"] > 0 or row["funnel_decision"] in ("promote_to_m5", "promote")
    # best holdout expectancy for ranking (informational; not the gate)
    m4 = run_dir / "M4_holdout.csv"
    if m4.exists():
        df = pl.read_csv(m4)
        cfg_cols = [c for c in ["direction", "entry_buffer_minutes", "entry_window_minutes",
                                "regime_timeframe", "vwma_periods"] if c in df.columns]
        if df.height and "holdout_exp_r" in df.columns and cfg_cols:
            agg = df.group_by(cfg_cols).agg(
                pl.col("holdout_exp_r").min().alias("min_exp_r"),
                pl.col("holdout_signals").min().alias("min_signals"))
            row["n_holdout_configs"] = agg.height
            mx = agg["min_exp_r"].max()
            row["best_holdout_exp_r"] = float(mx) if mx is not None else None
            best = agg.sort("min_exp_r", descending=True, nulls_last=True).head(1)
            if best.height and best["min_signals"][0] is not None:
                row["best_holdout_signals"] = int(best["min_signals"][0])


MANIFEST_FIELDS = ["family", "symbol", "era", "status", "m1", "n_holdout_configs",
                   "best_holdout_exp_r", "best_holdout_signals", "funnel_decision",
                   "m4_promoted", "any_pass", "run_dir"]


def _rescan(wave_name: str, families: list[str] | None = None) -> None:
    """Rebuild a wave manifest from run dirs on disk, using the AUTHORITATIVE funnel decision.
    families: restrict to these family keys (avoids mixing waves that share OUT_ROOT)."""
    fam_filter = set(families) if families else None
    rows = []
    for cell_dir in sorted(OUT_ROOT.glob("*__*__*")):
        if not cell_dir.is_dir():
            continue
        try:
            fam, sym, era = cell_dir.name.split("__")
        except ValueError:
            continue
        if fam_filter and fam not in fam_filter:
            continue
        hid = f"wave-{fam}-{sym}-{era}".lower()
        run_dirs = sorted((cell_dir / hid).glob("2*"), reverse=True) if (cell_dir / hid).exists() else []
        row = {"family": fam, "symbol": sym, "era": era, "status": "ok" if run_dirs else "no_run",
               "m1": "", "n_holdout_configs": 0, "best_holdout_exp_r": None,
               "best_holdout_signals": 0, "funnel_decision": "", "m4_promoted": 0,
               "any_pass": False, "run_dir": ""}
        if run_dirs:
            _read_run_dir(run_dirs[0], row)
        rows.append(row)
    manifest_path = OUT_ROOT / f"{wave_name}__manifest.csv"
    with manifest_path.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=MANIFEST_FIELDS)
        w.writeheader()
        for r in sorted(rows, key=lambda x: (x["family"], x["symbol"], x["era"])):
            w.writerow(r)
    n_pass = sum(1 for r in rows if r.get("any_pass"))
    print(f"[rescan] {len(rows)} cells, {n_pass} funnel-promoted. manifest: {manifest_path}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--families", default="trend", choices=list(FAMILY_GROUPS))
    ap.add_argument("--symbols", nargs="*", help="explicit symbols; overrides --universe")
    ap.add_argument("--universe", default=None, help="'all' = every full-history local symbol")
    ap.add_argument("--eras", nargs="*", default=list(ERAS), choices=list(ERAS))
    ap.add_argument("--max-configs", type=int, default=48)
    ap.add_argument("--workers", type=int, default=5)
    ap.add_argument("--polars-threads", type=int, default=2)
    ap.add_argument("--wave-name", default=None)
    ap.add_argument("--rescan", default=None,
                    help="rebuild <wave>__manifest.csv from existing run dirs (no recompute)")
    ap.add_argument("--rescan-families", default="trend",
                    help="family group to restrict rescan to (default trend)")
    args = ap.parse_args()

    if args.rescan:
        _rescan(args.rescan, FAMILY_GROUPS.get(args.rescan_families, [args.rescan_families]))
        return

    fams = FAMILY_GROUPS[args.families]
    if args.symbols:
        symbols = args.symbols
    elif args.universe == "all":
        data_dir = REPO / "data"
        symbols = sorted(
            d.name for d in data_dir.iterdir()
            if d.is_dir() and d.name.isupper()
            and any(f.name.startswith("2021") for f in d.glob("2021-*.parquet"))
        )
    else:
        ap.error("provide --symbols or --universe all")

    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
    wave_name = args.wave_name or f"wave_{args.families}_{stamp}"
    OUT_ROOT.mkdir(parents=True, exist_ok=True)
    manifest_path = OUT_ROOT / f"{wave_name}__manifest.csv"

    cells = [(f, s, e) for f in fams for s in symbols for e in args.eras]
    print(f"[wave] {wave_name}: {len(fams)} families × {len(symbols)} symbols × "
          f"{len(args.eras)} eras = {len(cells)} cells, {args.workers} workers")
    print(f"[wave] manifest → {manifest_path}")

    rows: list[dict] = []
    done = 0
    with ProcessPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(run_cell, f, s, e, args.max_configs, args.polars_threads): (f, s, e)
                for (f, s, e) in cells}
        for fut in as_completed(futs):
            f, s, e = futs[fut]
            try:
                row = fut.result()
            except Exception as exc:  # noqa: BLE001
                row = {"family": f, "symbol": s, "era": e, "status": f"exc:{exc}",
                       "m1": "", "n_holdout_configs": 0, "best_holdout_exp_r": None,
                       "best_holdout_signals": 0, "any_pass": False, "run_dir": ""}
            rows.append(row)
            done += 1
            flag = "PASS" if row.get("any_pass") else ("· " + (row.get("m1", "")[:22]))
            print(f"[wave] {done}/{len(cells)}  {f}/{s}/{e}  {row['status']}  {flag}")

    fields = ["family", "symbol", "era", "status", "m1", "n_holdout_configs",
              "best_holdout_exp_r", "best_holdout_signals", "funnel_decision",
              "m4_promoted", "any_pass", "run_dir"]
    with manifest_path.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        for r in sorted(rows, key=lambda x: (x["family"], x["symbol"], x["era"])):
            w.writerow(r)
    n_pass = sum(1 for r in rows if r.get("any_pass"))
    print(f"[wave] done. {n_pass}/{len(cells)} cells passed holdout. manifest: {manifest_path}")


if __name__ == "__main__":
    main()
