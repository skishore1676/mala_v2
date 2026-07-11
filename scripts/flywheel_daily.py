"""Flywheel Phase A1+A2: daily detector feed + fire ledger.

For a trading day, sweeps the FLASH/EXHAUSTION/TREND detectors on IWM/SPY,
appends each fire (with features, strength, tape context) to the fire ledger,
computes each fire's native-profile option-path outcome to EOD (real IV via
kamandal when available, else modeled), and formats a Telegram consultation
card (signals only — no order suggestions, no live-money surface).

The ledger is the going-forward evidence stream; matched later against the
operator's fills (flywheel_fill_match.py) to grow the Phase B selection set.

Usage:
  .venv/bin/python scripts/flywheel_daily.py --date 2026-06-15         # dry-run: print card
  .venv/bin/python scripts/flywheel_daily.py --date 2026-06-15 --send  # push via lathi-bus
  (no --date → latest day present in BOTH IWM and SPY caches)

Deploy note: the live daily job runs on oldmac (fresh bars + fresh kamandal
IV). This script is machine-agnostic; oldmac scheduling is a launchd step.
"""

from __future__ import annotations

import argparse
import csv
import math
import subprocess
import sys
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import polars as pl

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from src.research.playbook_tagging import SymbolBars, extract_features  # noqa: E402
from src.research.option_translation import score_profile_on_options  # noqa: E402
from scripts.p2_detector_scorecard import flash_A, flash_C, trend_C, exh_C  # noqa: E402

ET = ZoneInfo("America/New_York")
DATA_DIR = REPO / "data"
LEDGER = REPO / "data/personal_imports/tagged/flywheel_fire_ledger.csv"
SYMBOLS = ("IWM", "SPY")
GRID_STEP_MIN = 5
DEBOUNCE_MIN = 10

# detector -> (fn, profile, strength_feature, human label)
DETS = {
    "F-A": (flash_A, "FLASH_REVERSAL", "fade_flush_atr", "flash"),
    "F-C": (flash_C, "FLASH_REVERSAL", "fade_flush_atr", "flash(fast)"),
    "T-C": (trend_C, "TREND_CONTINUATION", "trend_side_frac_60", "trend-pullback"),
    "E-C": (exh_C, "EXHAUSTION_REVERSAL", "stretch_pctile", "exhaustion"),
}
LEDGER_COLS = ["date", "symbol", "det", "profile", "dir", "time_et", "strength",
               "tape_5d_atr", "stretch_pctile", "outcome_pct", "iv_source"]


def latest_common_day() -> date:
    days = None
    for s in SYMBOLS:
        d = {p.stem for p in (DATA_DIR / s).glob("*.parquet")}
        days = d if days is None else (days & d)
    return max(date.fromisoformat(x) for x in days)


def sweep_day(sb: SymbolBars, day: date) -> list[dict]:
    """Debounced fires for one day, both directions, with features."""
    if day not in sb.days:
        return []
    fires, last = [], {}
    for minute in range(9 * 60 + 35, 15 * 60 + 56, GRID_STEP_MIN):
        dt = datetime(day.year, day.month, day.day, minute // 60, minute % 60, tzinfo=ET)
        for d in (1, -1):
            f = extract_features(sb, dt, d)
            if not f:
                continue
            for det, (fn, prof, skey, _) in DETS.items():
                if fn(f, d):
                    key = (det, d)
                    if minute - last.get(key, -10**6) > DEBOUNCE_MIN:
                        fires.append({"det": det, "profile": prof, "dir": d,
                                      "dt": dt, "strength": f.get(skey, float("nan")),
                                      "tape": f.get("ret_5d_atr", float("nan")),
                                      "stretch": f.get("stretch_pctile", float("nan"))})
                    last[key] = minute
    return fires


def fire_outcome(frame_hist: pl.DataFrame, fire: dict, symbol: str,
                 use_real_iv: bool = True) -> tuple[float, str]:
    """Native-profile option-path %-outcome for a single fire. frame_hist =
    trailing sessions incl. the fire day (vol anchor). ``use_real_iv=False``
    (modeled, constant) is far faster for bulk RELATIVE comparisons where the
    IV level cancels — real IV reloads the bar cache per call."""
    direction = "long" if fire["dir"] > 0 else "short"
    marked = frame_hist.with_columns(
        (pl.col("timestamp") == fire["dt"]).alias("signal"),
        pl.lit(direction).alias("signal_direction"),
    )
    if not marked["signal"].any():
        return float("nan"), "none"
    res = score_profile_on_options(marked, direction, fire["profile"],
                                   symbol=symbol, use_real_iv=use_real_iv)
    src = "real" if res.get("iv_premium_factor", 1.2) != 1.2 else "modeled"
    return (res["expectancy_pct"] if res["n"] else float("nan")), src


def card_view(fires: list[dict]) -> list[dict]:
    """The trader-credible subset shown on the card (ledger keeps ALL fires).
    Applies the operator's own thresholds: exhaustion needs stretch ≥ p85 (his
    E-spec number); flash needs a real flush (≥0.20 ATR); drop ambiguous
    same-symbol/same-minute opposite-direction pairs (whipsaw bars)."""
    kept = []
    for fr in fires:
        s = fr["strength"]
        if fr["profile"] == "EXHAUSTION_REVERSAL" and not (
                isinstance(s, float) and s >= 85):
            continue
        if fr["profile"] == "FLASH_REVERSAL" and not (
                isinstance(s, float) and s >= 0.20):
            continue
        kept.append(fr)
    conflict = set()
    for a in kept:
        for b in kept:
            if a is not b and a["sym"] == b["sym"] and a["det"] == b["det"] \
                    and a["dt"] == b["dt"] and a["dir"] == -b["dir"]:
                conflict.add(id(a))
    return [fr for fr in kept if id(fr) not in conflict]


def format_card(day: date, fires: list[dict], n_total: int) -> str:
    shown = card_view(fires)
    if not shown:
        return (f"📊 Flywheel {day:%a %b %d} — no high-conviction IWM/SPY fires "
                f"(swept {n_total}, none cleared your thresholds).")
    lines = [f"📊 *Flywheel consult — {day:%a %b %d}* (signals only, not advice)"]
    order = {"FLASH_REVERSAL": 0, "EXHAUSTION_REVERSAL": 1, "TREND_CONTINUATION": 2}
    for fr in sorted(shown, key=lambda x: (x["sym"], order.get(x["profile"], 9), x["dt"])):
        arrow = "🟢long" if fr["dir"] > 0 else "🔴short"
        s = fr["strength"]
        strg = f"{s:.2f}" if isinstance(s, float) and math.isfinite(s) else "?"
        prof = fr["profile"].split("_")[0].lower()
        lines.append(f"• {fr['sym']} {fr['dt']:%H:%M} {arrow} *{prof}* "
                     f"({DETS[fr['det']][3]}, str {strg})")
    lines.append(f"_{len(shown)} of {n_total} swept cleared your thresholds · "
                 f"your call which to take · all logged_")
    return "\n".join(lines)


def append_ledger(rows: list[dict]) -> None:
    exists = LEDGER.exists()
    with open(LEDGER, "a", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=LEDGER_COLS, extrasaction="ignore")
        if not exists:
            w.writeheader()
        w.writerows(rows)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date")
    ap.add_argument("--send", action="store_true")
    ap.add_argument("--no-outcome", action="store_true",
                    help="skip per-fire option sim (fast card preview)")
    args = ap.parse_args()
    day = date.fromisoformat(args.date) if args.date else latest_common_day()

    all_fires, ledger_rows = [], []
    for sym in SYMBOLS:
        sb = SymbolBars(sym, DATA_DIR)
        fires = sweep_day(sb, day)
        # trailing frame for the vol anchor (~40 sessions ending on `day`)
        hist_days = [d for d in sb.day_list if d <= day][-40:]
        ts, cl, hi, lo = [], [], [], []
        for d in hist_days:
            b = sb.days[d]
            for i in range(len(b["close"])):
                m = int(b["min_of_day"][i])
                ts.append(datetime(d.year, d.month, d.day, m // 60, m % 60, tzinfo=ET))
                cl.append(float(b["close"][i])); hi.append(float(b["high"][i]))
                lo.append(float(b["low"][i]))
        frame = pl.DataFrame({"timestamp": ts, "close": cl, "high": hi, "low": lo}) \
            .with_columns(pl.col("timestamp").dt.convert_time_zone("America/New_York"))
        for fr in fires:
            fr["sym"] = sym
            outcome, src = (float("nan"), "skip") if args.no_outcome \
                else fire_outcome(frame, fr, sym)
            ledger_rows.append({
                "date": day.isoformat(), "symbol": sym, "det": fr["det"],
                "profile": fr["profile"], "dir": fr["dir"],
                "time_et": f"{fr['dt']:%H:%M}", "strength": fr["strength"],
                "tape_5d_atr": fr["tape"], "stretch_pctile": fr["stretch"],
                "outcome_pct": outcome, "iv_source": src,
            })
            all_fires.append(fr)

    card = format_card(day, all_fires, len(all_fires))
    if not args.no_outcome:
        append_ledger(ledger_rows)
        print(f"[ledger] appended {len(ledger_rows)} fires for {day}", file=sys.stderr)
    print(card)

    if args.send:
        subprocess.run([
            "python3", "-m", "lathi_bus.cli", "telegram-notify",
            "--profile", "coding-agent-northstar",
            "--title", f"Flywheel consult {day:%b %d}",
            "--body", card, "--live",
        ], cwd="/Users/suman/code/lathi-bus", check=False)
        print("[sent] via lathi-bus", file=sys.stderr)


if __name__ == "__main__":
    main()
