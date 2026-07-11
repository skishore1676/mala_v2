"""P3: option-path backtest of the P2 detectors with native profile exits.

For each passing P2 detector (FLASH F-A/F-C, TREND T-C) plus the flagged
sub-bar EXHAUSTION screen (E-C), generate the full-history signal set on
IWM/SPY (2021-05 → 2026-04, 5-min grid, 10-min debounce, both directions) and
score it with the S4 option-path scorer (`score_profile_on_options`) using the
NATIVE profile exit, across the IV band and three regime windows, with a flat
premium cost haircut.

Gate P3 bars (docs/PLAYBOOK_DISCOVERY_PROGRAM.md §P3), judged per
detector×direction:
  - net expectancy > 0 across the ENTIRE IV band (incl. adverse scenario) in
    ≥2 of 3 regime windows;
  - ≥100 signals over the full window;
  - payoff ratio > 1 (asymmetric, convexity-carried);
  - capital-adjusted net expectancy > 0.

Usage:
  .venv/bin/python scripts/p3_option_path_backtest.py            # sweep+score
  .venv/bin/python scripts/p3_option_path_backtest.py --no-sweep # reuse events
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

import polars as pl

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from src.research.playbook_tagging import SymbolBars, extract_features  # noqa: E402
from src.research.option_translation import score_profile_band  # noqa: E402
from scripts.p2_detector_scorecard import (  # noqa: E402
    flash_A, flash_C, trend_C, exh_C,
)

ET = ZoneInfo("America/New_York")
OUT_DIR = REPO / "data/personal_imports/tagged"
DATA_DIR = REPO / "data"
EVENTS_CSV = OUT_DIR / "p3_signal_events.csv"

SYMBOLS = ["IWM", "SPY"]
GRID_STEP_MIN = 5
DEBOUNCE_MIN = 10
COST_HAIRCUT_PCT = 4.0  # premium % per round trip (spread + slippage, short-DTE)

# detector -> (fn, native profile, sub-bar flag, max capital % from the
# playbook dials table)
CANDIDATES = {
    "F-A": (flash_A, "FLASH_REVERSAL", False, 5.0),
    "F-C": (flash_C, "FLASH_REVERSAL", False, 5.0),
    "T-C": (trend_C, "TREND_CONTINUATION", False, 6.0),
    "E-C": (exh_C, "EXHAUSTION_REVERSAL", True, 4.0),
}

REGIMES = [
    ("2021-22", date(2021, 5, 1), date(2022, 12, 31)),
    ("2023-24", date(2023, 1, 1), date(2024, 12, 31)),
    ("2025-26", date(2025, 1, 1), date(2026, 4, 30)),
]


def sweep_events() -> list[dict]:
    """Full-history detector sweep -> [{symbol, det, dir, dt_iso}]."""
    events: list[dict] = []
    for sym in SYMBOLS:
        sb = SymbolBars(sym, DATA_DIR)
        for day in sb.day_list:
            last_fire: dict[tuple, int] = {}
            for minute in range(9 * 60 + 35, 15 * 60 + 56, GRID_STEP_MIN):
                dt_ = datetime(day.year, day.month, day.day, minute // 60,
                               minute % 60, tzinfo=ET)
                for d in (1, -1):
                    f = extract_features(sb, dt_, d)
                    if not f:
                        continue
                    for det, (fn, _, _, _) in CANDIDATES.items():
                        if fn(f, d):
                            key = (det, d)
                            if minute - last_fire.get(key, -10**6) > DEBOUNCE_MIN:
                                events.append({
                                    "symbol": sym, "det": det, "dir": d,
                                    "dt": dt_.isoformat(),
                                })
                            last_fire[key] = minute
        print(f"swept {sym}: {sum(1 for e in events if e['symbol'] == sym)} events",
              file=sys.stderr)
    with open(EVENTS_CSV, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=["symbol", "det", "dir", "dt"])
        w.writeheader()
        w.writerows(events)
    return events


def load_frames() -> dict[str, pl.DataFrame]:
    """Per-symbol 1-min session frame (timestamp/close/high/low) from the
    bar cache, ET timestamps."""
    frames = {}
    for sym in SYMBOLS:
        sb = SymbolBars(sym, DATA_DIR)
        ts, close, high, low = [], [], [], []
        for day in sb.day_list:
            b = sb.days[day]
            for i in range(len(b["close"])):
                m = int(b["min_of_day"][i])
                ts.append(datetime(day.year, day.month, day.day, m // 60,
                                   m % 60, tzinfo=ET))
                close.append(float(b["close"][i]))
                high.append(float(b["high"][i]))
                low.append(float(b["low"][i]))
        frames[sym] = pl.DataFrame(
            {"timestamp": ts, "close": close, "high": high, "low": low}
        ).with_columns(pl.col("timestamp").dt.convert_time_zone("America/New_York"))
        print(f"frame {sym}: {len(ts):,} bars", file=sys.stderr)
    return frames


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--no-sweep", action="store_true",
                    help="reuse existing p3_signal_events.csv")
    args = ap.parse_args()

    if args.no_sweep and EVENTS_CSV.exists():
        events = list(csv.DictReader(open(EVENTS_CSV)))
        for e in events:
            e["dir"] = int(e["dir"])
    else:
        events = sweep_events()

    frames = load_frames()
    # index events: (symbol, det, dir) -> set of timestamps
    ev_idx: dict[tuple, set] = defaultdict(set)
    for e in events:
        ev_idx[(e["symbol"], e["det"], int(e["dir"]))].add(
            datetime.fromisoformat(e["dt"]))

    results = []
    for sym in SYMBOLS:
        base = frames[sym]
        for det, (fn, profile, sub_bar, maxcap) in CANDIDATES.items():
            for d in (1, -1):
                fired = ev_idx.get((sym, det, d), set())
                if not fired:
                    continue
                direction = "long" if d > 0 else "short"
                frame = base.with_columns(
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
                    band = score_profile_band(sub, direction, profile)
                    for row in band:
                        row.update(symbol=sym, det=det, direction=direction,
                                   regime=reg_name, profile=profile,
                                   sub_bar=sub_bar,
                                   expectancy_net=row["expectancy_pct"] - COST_HAIRCUT_PCT,
                                   cap_adj_net=(row["expectancy_pct"] - COST_HAIRCUT_PCT)
                                   * maxcap / 100.0)
                        results.append(row)
                    print(f"scored {sym} {det} {direction} {reg_name}",
                          file=sys.stderr)

    with open(OUT_DIR / "p3_backtest_results.csv", "w", newline="") as fh:
        cols = ["symbol", "det", "direction", "profile", "regime", "scenario",
                "n", "expectancy_pct", "expectancy_net", "cap_adj_net",
                "win_rate", "avg_win_pct", "avg_loss_pct", "sub_bar",
                "iv_premium_factor", "vol_beta"]
        w = csv.DictWriter(fh, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        w.writerows(results)

    # ── verdicts per (symbol, det, direction) ──
    lines = [
        "# P3 option-path backtest — detectors × native profile exits (IWM/SPY, 2021-2026)",
        "",
        f"- cost haircut {COST_HAIRCUT_PCT:.0f}% of premium per round trip; "
        "IV band = flat / leverage / cheap / rich; regimes 2021-22 / 2023-24 / 2025-26",
        "- P3 bar: net>0 across FULL IV band in ≥2 regimes; n≥100; payoff>1; cap-adj net>0",
        "",
        "| symbol | det | dir | profile | n(full) | net exp% (lev, full) | win% | payoff | regimes band-positive | verdict |",
        "|---|---|---|---|---|---|---|---|---|---|",
    ]
    groups = defaultdict(list)
    for r in results:
        groups[(r["symbol"], r["det"], r["direction"])].append(r)
    for (sym, det, direction), rows in sorted(groups.items()):
        prof = rows[0]["profile"]
        n_full = sum(r["n"] for r in rows if r["scenario"] == "leverage")
        # regime passes: all 4 scenarios net-positive within the regime
        reg_pass = 0
        for reg_name, _, _ in REGIMES:
            rr = [r for r in rows if r["regime"] == reg_name]
            if rr and all(r["expectancy_net"] > 0 for r in rr) and \
                    sum(r["n"] for r in rr if r["scenario"] == "leverage") >= 10:
                reg_pass += 1
        lev = [r for r in rows if r["scenario"] == "leverage"]
        tot_n = sum(r["n"] for r in lev) or 1
        exp_w = sum(r["expectancy_net"] * r["n"] for r in lev) / tot_n
        win_w = sum(r["win_rate"] * r["n"] for r in lev) / tot_n
        aw = sum(r["avg_win_pct"] * r["n"] for r in lev) / tot_n
        al = sum(r["avg_loss_pct"] * r["n"] for r in lev) / tot_n
        payoff = abs(aw / al) if al else float("nan")
        cap_adj = exp_w * rows[0]["cap_adj_net"] / rows[0]["expectancy_net"] \
            if rows[0]["expectancy_net"] else 0.0
        ok = (reg_pass >= 2 and n_full >= 100 and payoff > 1 and exp_w > 0)
        verdict = "PASS" + (" (sub-bar screen)" if rows[0]["sub_bar"] else "") \
            if ok else "fail"
        lines.append(
            f"| {sym} | {det} | {direction} | {prof[:10]} | {n_full} | "
            f"{exp_w:+.1f}% | {win_w:.0%} | {payoff:.2f} | {reg_pass}/3 | {verdict} |"
        )
    lines += ["", "_Full per-regime × per-scenario grid in p3_backtest_results.csv._"]
    (OUT_DIR / "p3_backtest_report.md").write_text("\n".join(lines))
    print("\n".join(lines))


if __name__ == "__main__":
    main()
