"""P3 selection layer: rescore the detector signal sets under operator-style
selectivity, with cost sensitivity.

The unselected P3 run showed broad gross-positive edge (+0.5..+3.4%/trade,
44/48 regime cells) that dies under a 4% premium cost haircut — the mechanical
firehose takes ~12 fires/day where the operator takes 1-2. This pass applies
the selection rules HIS OWN materials specify, then rescores:

  S1 regime-aligned direction (handwritten rules: bull tape → fade flash
     SALES (long); bear tape → fade POPS (short); tape = sign of prior-5-day
     run) — applies to FLASH and EXHAUSTION detectors; TREND stays with-trend.
  S2 best-fire-of-the-day: strongest signal per (symbol, det, dir, day)
     (strength: FLASH = flush size; EXH = stretch percentile; TREND = trend
     persistence).
  S3 = S1 ∩ S2.
  E-85: for EXHAUSTION also require stretch ≥ p85 (the operator's own number).

Reports net expectancy at 4% and 2% round-trip haircuts (conservative vs
realistic for penny-wide IWM/SPY near-ATM options).

Usage: .venv/bin/python scripts/p3_selection_rerun.py   (reuses p3_signal_events.csv)
"""

from __future__ import annotations

import csv
import math
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import polars as pl

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from src.research.playbook_tagging import SymbolBars, extract_features  # noqa: E402
from src.research.option_translation import score_profile_band  # noqa: E402
from scripts.p3_option_path_backtest import (  # noqa: E402
    CANDIDATES, EVENTS_CSV, OUT_DIR, DATA_DIR, REGIMES, SYMBOLS, load_frames,
)

STRENGTH_KEY = {"F-A": "fade_flush_atr", "F-C": "fade_flush_atr",
                "E-C": "stretch_pctile", "T-C": "trend_side_frac_60"}


def annotate_events() -> list[dict]:
    events = list(csv.DictReader(open(EVENTS_CSV)))
    bars = {s: SymbolBars(s, DATA_DIR) for s in SYMBOLS}
    out = []
    for e in events:
        d = int(e["dir"])
        dt_ = datetime.fromisoformat(e["dt"])
        f = extract_features(bars[e["symbol"]], dt_, d)
        if not f:
            continue
        tape = f.get("ret_5d_atr", float("nan"))
        out.append({
            **e, "dir": d, "day": dt_.date().isoformat(),
            "strength": f.get(STRENGTH_KEY[e["det"]], float("nan")),
            "stretch": f.get("stretch_pctile", float("nan")),
            "tape_5d": tape,
        })
    print(f"annotated {len(out)} events", file=sys.stderr)
    return out


def regime_aligned(e: dict) -> bool:
    """Operator's tape rule for the reversal fades. TREND passes through."""
    if e["det"] == "T-C":
        return True
    t = e["tape_5d"]
    if not (isinstance(t, float) and math.isfinite(t)) or abs(t) < 0.5:
        return True  # no strong tape → no directional veto
    # bullish tape: only fade sales (long); bearish tape: only fade pops (short)
    return e["dir"] == (1 if t > 0 else -1)


def select(events: list[dict], mode: str) -> list[dict]:
    ev = list(events)
    if mode in ("S1", "S3", "S5"):
        ev = [e for e in ev if regime_aligned(e)]
    if mode.startswith("E85") or mode in ("S3", "S5"):
        ev = [e for e in ev if e["det"] != "E-C"
              or (isinstance(e["stretch"], float) and e["stretch"] >= 85)]
    if mode in ("S2", "S3"):
        # NOTE: strongest-fire-of-day has LOOKAHEAD (you can't know at 10:00
        # that a stronger fire comes at 14:00) — reported for diagnosis only;
        # live-translatable selection is S4/S5 (first fire of day).
        best: dict[tuple, dict] = {}
        for e in ev:
            k = (e["symbol"], e["det"], e["dir"], e["day"])
            cur = best.get(k)
            s = e["strength"] if isinstance(e["strength"], float) and \
                math.isfinite(e["strength"]) else -1e9
            cs = cur["strength"] if cur and isinstance(cur["strength"], float) and \
                math.isfinite(cur["strength"]) else -1e9
            if cur is None or s > cs:
                best[k] = e
        ev = list(best.values())
    if mode in ("S4", "S5"):
        # first fire of day per (sym, det, dir) — NO lookahead, live-translatable
        first: dict[tuple, dict] = {}
        for e in sorted(ev, key=lambda x: x["dt"]):
            k = (e["symbol"], e["det"], e["dir"], e["day"])
            if k not in first:
                first[k] = e
        ev = list(first.values())
    if mode == "S6":
        # absolute strength threshold — knowable AT fire time (no lookahead).
        # Threshold = p75 of the detector's strength on 2021-22 events ONLY
        # (fit regime); 2023-26 stays out-of-sample. Cap: first 2 qualifying
        # fires per (sym, det, dir, day).
        import statistics
        thr: dict[str, float] = {}
        for det in {e["det"] for e in ev}:
            fit = [e["strength"] for e in ev
                   if e["det"] == det and e["day"] < "2023-01-01"
                   and isinstance(e["strength"], float) and math.isfinite(e["strength"])]
            if fit:
                thr[det] = statistics.quantiles(fit, n=4)[2]  # p75
        picked: list[dict] = []
        per_day: dict[tuple, int] = defaultdict(int)
        for e in sorted(ev, key=lambda x: x["dt"]):
            t = thr.get(e["det"])
            s = e["strength"]
            if t is None or not (isinstance(s, float) and math.isfinite(s)) or s < t:
                continue
            k = (e["symbol"], e["det"], e["dir"], e["day"])
            if per_day[k] >= 2:
                continue
            per_day[k] += 1
            picked.append(e)
        ev = picked
    return ev


def main() -> None:
    events = annotate_events()
    frames = load_frames()
    rows_out = []
    for mode in ("S6",):
        sel = select(events, mode)
        idx: dict[tuple, set] = defaultdict(set)
        for e in sel:
            idx[(e["symbol"], e["det"], e["dir"])].add(
                datetime.fromisoformat(e["dt"]))
        for (sym, det, d), fired in sorted(idx.items()):
            direction = "long" if d > 0 else "short"
            profile = CANDIDATES[det][1]
            frame = frames[sym].with_columns(
                pl.col("timestamp").is_in(sorted(fired)).alias("signal"),
                pl.lit(direction).alias("signal_direction"),
            )
            for reg_name, d0, d1 in REGIMES:
                sub = frame.filter(
                    (pl.col("timestamp").dt.date() >= d0)
                    & (pl.col("timestamp").dt.date() <= d1)
                )
                if not sub["signal"].any():
                    continue
                for row in score_profile_band(sub, direction, profile):
                    row.update(mode=mode, symbol=sym, det=det,
                               direction=direction, regime=reg_name,
                               profile=profile)
                    rows_out.append(row)
            print(f"scored {mode} {sym} {det} {direction}", file=sys.stderr)

    with open(OUT_DIR / "p3_selection_results.csv", "w", newline="") as fh:
        cols = ["mode", "symbol", "det", "direction", "profile", "regime",
                "scenario", "n", "expectancy_pct", "win_rate", "avg_win_pct",
                "avg_loss_pct", "iv_premium_factor", "vol_beta"]
        w = csv.DictWriter(fh, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows_out)

    # verdict table at 2% and 4% haircuts
    lines = ["# P3 selection-layer results",
             "",
             "P3 bar: net>0 across FULL IV band in ≥2 regimes; n≥100 full-window; payoff>1.",
             "",
             "| mode | symbol | det | dir | n | gross exp% | net@2% | net@4% | payoff | band-pos regimes @2% | verdict@2% |",
             "|---|---|---|---|---|---|---|---|---|---|---|"]
    groups = defaultdict(list)
    for r in rows_out:
        groups[(r["mode"], r["symbol"], r["det"], r["direction"])].append(r)
    for key, rows in sorted(groups.items()):
        mode, sym, det, direction = key
        lev = [r for r in rows if r["scenario"] == "leverage"]
        n_full = sum(r["n"] for r in lev)
        if not n_full:
            continue
        exp_w = sum(r["expectancy_pct"] * r["n"] for r in lev) / n_full
        aw = sum(r["avg_win_pct"] * r["n"] for r in lev) / n_full
        al = sum(r["avg_loss_pct"] * r["n"] for r in lev) / n_full
        payoff = abs(aw / al) if al else float("nan")
        reg_pass2 = 0
        for reg_name, _, _ in REGIMES:
            rr = [r for r in rows if r["regime"] == reg_name]
            if rr and all(r["expectancy_pct"] - 2.0 > 0 for r in rr) and \
                    sum(r["n"] for r in rr if r["scenario"] == "leverage") >= 10:
                reg_pass2 += 1
        ok = reg_pass2 >= 2 and n_full >= 100 and payoff > 1 and exp_w - 2.0 > 0
        lines.append(
            f"| {mode} | {sym} | {det} | {direction} | {n_full} | {exp_w:+.1f}% | "
            f"{exp_w - 2:+.1f}% | {exp_w - 4:+.1f}% | {payoff:.2f} | {reg_pass2}/3 | "
            f"{'PASS' if ok else 'fail'} |"
        )
    (OUT_DIR / "p3_selection_report.md").write_text("\n".join(lines))
    print("\n".join(lines))


if __name__ == "__main__":
    main()
