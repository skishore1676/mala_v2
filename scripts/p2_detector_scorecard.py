"""P2: detector scorecard — "does a mechanical detector fire where he fired?"

For each playbook, a small family of interpretable detector variants (boolean
predicates over the P1 v4 feature set) is scored on:

  recall     — fraction of the FROZEN gold entries (per playbook, HIGH tier =
               machine-HIGH or operator-labeled) whose at-entry features fire
               the detector;
  base rate  — firing probability on a 5-minute grid over ALL sessions in the
               corpus window (both directions), per symbol;
  lift       — recall / base rate (the gate bar is ≥5x);
  fires/day  — median debounced firing events per day per symbol (bar ≤~5).

Gate P2 bar (docs/PLAYBOOK_DISCOVERY_PROGRAM.md §P2): ≥1 detector per playbook
with recall ≥50%, lift ≥5x, bounded fire rate. A playbook with no passing
detector is recorded as "eye not yet captured — fork (b)", never as no-edge.

Usage:  .venv/bin/python scripts/p2_detector_scorecard.py [--symbols IWM,SPY]
"""

from __future__ import annotations

import argparse
import csv
import math
import sys
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from src.research.playbook_tagging import SymbolBars, extract_features  # noqa: E402

ET = ZoneInfo("America/New_York")
FROZEN = REPO / "data/personal_imports/tagged/round_trips_tagged_FROZEN.csv"
OUT_DIR = REPO / "data/personal_imports/tagged"
DATA_DIR = REPO / "data"

GRID_STEP_MIN = 5          # sweep resolution
DEBOUNCE_MIN = 10          # consecutive fires within this gap = one event


def _fin(v) -> float:
    try:
        x = float(v)
        return x if math.isfinite(x) else float("nan")
    except (TypeError, ValueError):
        return float("nan")


# ── Detector definitions (feature dict + thesis_dir -> bool) ────────────────

def flash_A(f, d):  # the frozen FLASH-HIGH rule
    return (
        _fin(f.get("fade_flush_atr")) >= 0.25
        and _fin(f.get("fade_ext_age_min")) <= 30
        and _fin(f.get("fade_flush_dur_min")) <= 35
    )


def flash_B(f, d):  # includes the weak tier
    return (
        _fin(f.get("fade_flush_atr")) >= 0.12
        and _fin(f.get("fade_ext_age_min")) <= 30
        and _fin(f.get("fade_flush_dur_min")) <= 35
    )


def flash_C(f, d):  # faster + fresher
    return (
        _fin(f.get("fade_flush_atr")) >= 0.15
        and _fin(f.get("fade_ext_age_min")) <= 15
        and _fin(f.get("fade_flush_dur_min")) <= 25
    )


def _exh_hits(f, d):
    hits = 0
    for key, bar in (("ret_120_atr", 0.40), ("day_move_atr", 0.60),
                     ("ret_3d_atr", 0.80), ("ret_5d_atr", 1.00)):
        v = _fin(f.get(key))
        if math.isfinite(v) and abs(v) >= bar and (1 if v >= 0 else -1) == -d:
            hits += 1
    return hits


def exh_A(f, d):  # any scale
    return _exh_hits(f, d) >= 1


def exh_B(f, d):  # the frozen EXH-HIGH rule
    if _exh_hits(f, d) >= 2:
        return True
    return _exh_hits(f, d) >= 1 and (
        _fin(f.get("stretch_pctile")) >= 70 or bool(f.get("failed_retest"))
    )


def exh_C(f, d):  # single scale + mild stretch confirm
    return _exh_hits(f, d) >= 1 and _fin(f.get("stretch_pctile")) >= 50


def exh_D(f, d):  # run context + the operator's TRIGGER: stalled failed retest
    gap = _fin(f.get("fade_retest_gap_atr"))
    stall = _fin(f.get("fade_stall_min"))
    return (
        _exh_hits(f, d) >= 1
        and math.isfinite(gap) and 0.0 < gap <= 0.20
        and 10 <= stall <= 150
    )


def exh_E(f, d):  # trigger with wider stall window, 2-scale or stretch confirm
    gap = _fin(f.get("fade_retest_gap_atr"))
    stall = _fin(f.get("fade_stall_min"))
    return (
        (_exh_hits(f, d) >= 2 or (_exh_hits(f, d) >= 1
                                  and _fin(f.get("stretch_pctile")) >= 60))
        and math.isfinite(gap) and 0.0 < gap <= 0.30
        and stall >= 10
    )


def exh_F(f, d):  # run + EVENT trigger: a failed retest happened recently
    return (
        _exh_hits(f, d) >= 1
        and _fin(f.get("fade_retest_event_age")) <= 45
    )


def exh_G(f, d):  # event trigger with confirmation (2 scales or stretch)
    return (
        (_exh_hits(f, d) >= 2 or (_exh_hits(f, d) >= 1
                                  and _fin(f.get("stretch_pctile")) >= 60))
        and _fin(f.get("fade_retest_event_age")) <= 60
    )


def _with_trend(f, d):
    return (
        int(_fin(f.get("trend_dir")) or 0) == d
        and _fin(f.get("trend_side_frac_60")) >= 0.75
    )


def trend_A(f, d):  # 10-VMA touch only
    return _with_trend(f, d) and bool(f.get("touched_vma10"))


def trend_B(f, d):  # the frozen TREND-HIGH rule
    pull = _fin(f.get("pullback_depth_frac"))
    return _with_trend(f, d) and (
        bool(f.get("touched_vma10")) or (math.isfinite(pull) and 0.1 <= pull <= 0.6)
    )


def trend_C(f, d):  # ACTIVE pullback: recent counter-move stalling at the VMA
    r15 = _fin(f.get("ret_15_atr"))
    return (
        trend_B(f, d)
        and math.isfinite(r15)
        and (1 if r15 >= 0 else -1) == -d   # last 15m moved against the trend
        and abs(r15) >= 0.08                 # a real pull, not drift
    )


def range_A(f, d):  # compressed base at the break-side edge
    return (
        _fin(f.get("range_width_90m_pctile")) <= 25
        and _fin(f.get("minutes_since_open")) >= 90
        and _fin(f.get("edge_pos")) >= 0.4
    )


def range_B(f, d):  # gap continuation
    g = _fin(f.get("gap_pct"))
    return (
        math.isfinite(g) and abs(g) >= 0.015
        and (1 if g >= 0 else -1) == d
        and _fin(f.get("minutes_since_open")) <= 120
    )


DETECTORS = {
    "FLASH_REVERSAL": {"F-A(0.25ATR/30m)": flash_A, "F-B(0.12ATR/30m)": flash_B,
                       "F-C(0.15ATR/15m)": flash_C},
    "EXHAUSTION_REVERSAL": {"E-A(any-scale)": exh_A, "E-B(HIGH-rule)": exh_B,
                            "E-C(scale+p50)": exh_C,
                            "E-D(run+trigger)": exh_D, "E-E(2scale+trigger)": exh_E,
                            "E-F(run+event)": exh_F, "E-G(event+confirm)": exh_G},
    "TREND_CONTINUATION": {"T-A(vma-touch)": trend_A, "T-B(HIGH-rule)": trend_B,
                           "T-C(active-pull)": trend_C},
    "RANGE_EXPANSION": {"R-A(compress-edge)": range_A, "R-B(gap-go)": range_B},
}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", default="IWM,SPY")
    args = ap.parse_args()
    symbols = args.symbols.split(",")

    # Gold entries (at-entry features already stored in the frozen corpus).
    rows = list(csv.DictReader(open(FROZEN)))
    gold = defaultdict(list)  # tag -> [(features_row, thesis_dir)]
    date_range = [None, None]
    for r in rows:
        if r["underlying"] not in symbols or not r.get("stretch_pctile"):
            continue
        d = datetime.fromisoformat(r["entry_dt_et"]).date()
        date_range[0] = min(date_range[0] or d, d)
        date_range[1] = max(date_range[1] or d, d)
        high = r["confidence"] == "HIGH" or "comment" in r.get("tag_source", "")
        gold[r["final_tag"]].append((r, int(r["thesis_dir"]), high))

    # Sweep: base rate + debounced fires/day on a 5-min grid, both directions.
    stats = {  # det name -> counters
        name: {"grid_fires": 0, "events_by_day": defaultdict(int)}
        for fam in DETECTORS.values() for name in fam
    }
    grid_points = 0
    for sym in symbols:
        sb = SymbolBars(sym, DATA_DIR)
        days = [d for d in sb.day_list if date_range[0] <= d <= date_range[1]]
        for day in days:
            last_fire: dict[tuple, int] = {}
            for minute in range(9 * 60 + 35, 15 * 60 + 56, GRID_STEP_MIN):
                dt = datetime(day.year, day.month, day.day, minute // 60,
                              minute % 60, tzinfo=ET)
                for d in (1, -1):
                    f = extract_features(sb, dt, d)
                    grid_points += 1
                    if not f:
                        continue
                    for fam in DETECTORS.values():
                        for name, fn in fam.items():
                            if fn(f, d):
                                stats[name]["grid_fires"] += 1
                                key = (name, d)
                                if minute - last_fire.get(key, -10**6) > DEBOUNCE_MIN:
                                    stats[name]["events_by_day"][(sym, day)] += 1
                                last_fire[key] = minute
        print(f"swept {sym}: {len(days)} sessions", file=sys.stderr)

    # Scorecard.
    def median(xs):
        xs = sorted(xs)
        return xs[len(xs) // 2] if xs else 0

    lines = [
        "# P2 detector scorecard — fires where he fired (IWM/SPY, frozen gold)",
        "",
        f"- corpus window {date_range[0]} → {date_range[1]}, grid {GRID_STEP_MIN}m, "
        f"{grid_points:,} bar-direction evaluations",
        "- gate bar per playbook: recall ≥50% (HIGH tier), lift ≥5x, fires/day ≤~5",
        "",
        "| playbook | detector | recall HIGH | recall all | base rate | lift | med fires/day/sym | verdict |",
        "|---|---|---|---|---|---|---|---|",
    ]
    csv_rows = []
    for tag, fam in DETECTORS.items():
        eps = gold.get(tag, [])
        hi = [(r, d) for r, d, h in eps if h]
        for name, fn in fam.items():
            def _rec(pairs):
                if not pairs:
                    return float("nan")
                fires = sum(1 for r, d in pairs if fn(r, d))
                return fires / len(pairs)
            rec_h, rec_a = _rec(hi), _rec([(r, d) for r, d, _ in eps])
            base = stats[name]["grid_fires"] / grid_points if grid_points else 0
            lift = (rec_h / base) if base > 0 and math.isfinite(rec_h) else float("nan")
            fpd = median(list(stats[name]["events_by_day"].values()))
            ok = (
                math.isfinite(rec_h) and rec_h >= 0.5
                and math.isfinite(lift) and lift >= 5
                and fpd <= 5
            )
            verdict = "PASS" if ok else "fail"
            lines.append(
                f"| {tag[:14]} | {name} | "
                + (f"{rec_h:.0%} (n={len(hi)})" if hi else "n=0")
                + f" | {rec_a:.0%} (n={len(eps)}) | {base:.3%} | "
                + (f"{lift:.1f}x" if math.isfinite(lift) else "—")
                + f" | {fpd} | {verdict} |"
            )
            csv_rows.append([tag, name, rec_h, rec_a, len(hi), len(eps), base, lift, fpd, verdict])
    lines += [
        "",
        "_Recall from stored at-entry features; base rate from the full-session sweep "
        "(both directions). A playbook with no PASS is fork (b): eye not yet captured._",
    ]
    (OUT_DIR / "p2_detector_scorecard.md").write_text("\n".join(lines))
    with open(OUT_DIR / "p2_detector_scorecard.csv", "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["playbook", "detector", "recall_high", "recall_all", "n_high",
                    "n_all", "base_rate", "lift", "med_fires_day", "verdict"])
        w.writerows(csv_rows)
    print("\n".join(lines))


if __name__ == "__main__":
    main()
