"""Flywheel Phase A3: match the operator's real fills to detector fires.

Zero operator effort: his manual IWM/SPY entries (the frozen episode corpus)
are cross-referenced against the full-history detector fire set
(p3_signal_events.csv). A his-entry within MATCH_WINDOW_MIN of a same-symbol,
same-direction fire = TAKE (label which detector). Every other fire in his
trading window = SKIP. The result is the selection-function training set for
Phase B, plus the coverage check that guards against overstated P2 recall.

Outputs (local-only):
  data/personal_imports/tagged/flywheel_take_skip.csv   (fire-level, labeled)
  data/personal_imports/tagged/flywheel_coverage.md     (readback)

Usage:  .venv/bin/python scripts/flywheel_fill_match.py
"""

from __future__ import annotations

import csv
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

FROZEN = REPO / "data/personal_imports/tagged/round_trips_tagged_FROZEN.csv"
FIRES = REPO / "data/personal_imports/tagged/p3_signal_events.csv"
OUT_DIR = REPO / "data/personal_imports/tagged"

SYMBOLS = ("IWM", "SPY")
MATCH_WINDOW_MIN = 10          # a fill within ±10m of a fire = that fire taken
DET_PROFILE = {"F-A": "FLASH_REVERSAL", "F-C": "FLASH_REVERSAL",
               "T-C": "TREND_CONTINUATION", "E-C": "EXHAUSTION_REVERSAL"}


def main() -> None:
    # His IWM/SPY episodes with entry context.
    entries = []
    for r in csv.DictReader(open(FROZEN)):
        if r["underlying"] not in SYMBOLS or not r.get("stretch_pctile"):
            continue
        entries.append({
            "episode_id": r["episode_id"], "symbol": r["underlying"],
            "dt": datetime.fromisoformat(r["entry_dt_et"]),
            "dir": int(r["thesis_dir"]), "tag": r["final_tag"],
        })
    ent_window = (min(e["dt"] for e in entries), max(e["dt"] for e in entries))

    # Fires, restricted to his trading window (so SKIP base rate is comparable).
    fires = []
    for r in csv.DictReader(open(FIRES)):
        if r["symbol"] not in SYMBOLS:
            continue
        dt = datetime.fromisoformat(r["dt"])
        if not (ent_window[0].date() <= dt.date() <= ent_window[1].date()):
            continue
        fires.append({"symbol": r["symbol"], "det": r["det"],
                      "dir": int(r["dir"]), "dt": dt})

    # Index fires by (symbol, dir, day) for fast windowed lookup.
    by_key = defaultdict(list)
    for i, fr in enumerate(fires):
        by_key[(fr["symbol"], fr["dir"], fr["dt"].date())].append(i)

    # TAKE: for each his-entry, find fires within the window.
    taken_fire_idx = set()
    entry_matched = 0
    entry_det = Counter()
    win = timedelta(minutes=MATCH_WINDOW_MIN)
    for e in entries:
        cands = by_key.get((e["symbol"], e["dir"], e["dt"].date()), [])
        hits = [i for i in cands if abs(fires[i]["dt"] - e["dt"]) <= win]
        if hits:
            entry_matched += 1
            taken_fire_idx.update(hits)
            for i in hits:
                entry_det[fires[i]["det"]] += 1

    # Label every fire.
    rows = []
    for i, fr in enumerate(fires):
        rows.append({
            "symbol": fr["symbol"], "det": fr["det"], "dir": fr["dir"],
            "dt": fr["dt"].isoformat(), "profile": DET_PROFILE[fr["det"]],
            "label": "TAKE" if i in taken_fire_idx else "SKIP",
        })
    with open(OUT_DIR / "flywheel_take_skip.csv", "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=["symbol", "det", "dir", "dt",
                                           "profile", "label"])
        w.writeheader()
        w.writerows(rows)

    # Readback.
    n_ent = len(entries)
    cov = entry_matched / n_ent if n_ent else 0.0
    take_n = len(taken_fire_idx)
    fire_n = len(fires)
    take_rate = take_n / fire_n if fire_n else 0.0
    det_take = Counter(fires[i]["det"] for i in taken_fire_idx)
    det_all = Counter(fr["det"] for fr in fires)
    md = [
        "# Flywheel A3 — fill↔fire coverage & take/skip set",
        "",
        f"- his IWM/SPY episodes (with context): **{n_ent}**",
        f"- entries matching ≥1 same-dir fire within ±{MATCH_WINDOW_MIN}m: "
        f"**{entry_matched} ({cov:.0%})**  ← coverage check (bar ≥90%)",
        f"- fires in his window: **{fire_n:,}**; taken **{take_n:,}** "
        f"({take_rate:.1%}); skipped {fire_n - take_n:,}",
        f"- selectivity: he takes ~1 of every {round(1/take_rate) if take_rate else 0} fires "
        "→ the selection layer Phase B must learn",
        "",
        "## Take rate by detector",
        "",
        "| detector | profile | fires | taken | take rate |",
        "|---|---|---|---|---|",
    ]
    for det in ("F-A", "F-C", "T-C", "E-C"):
        a, t = det_all.get(det, 0), det_take.get(det, 0)
        md.append(f"| {det} | {DET_PROFILE[det]} | {a:,} | {t} | "
                  f"{(t/a if a else 0):.1%} |")
    md += [
        "",
        "## His entries by playbook tag matched to a fire",
        "",
        "| his tag | entries | matched a fire | via detector(s) |",
        "|---|---|---|---|",
    ]
    tag_tot = Counter(e["tag"] for e in entries)
    tag_hit = Counter()
    tag_det = defaultdict(Counter)
    for e in entries:
        cands = by_key.get((e["symbol"], e["dir"], e["dt"].date()), [])
        hits = [i for i in cands if abs(fires[i]["dt"] - e["dt"]) <= win]
        if hits:
            tag_hit[e["tag"]] += 1
            for i in hits:
                tag_det[e["tag"]][fires[i]["det"]] += 1
    for tag, tot in tag_tot.most_common():
        dets = ", ".join(f"{d}×{n}" for d, n in tag_det[tag].most_common(3))
        md.append(f"| {tag} | {tot} | {tag_hit.get(tag, 0)} "
                  f"({tag_hit.get(tag, 0)/tot:.0%}) | {dets or '—'} |")
    md += ["", "_Take/skip rows: flywheel_take_skip.csv (Phase B training set)._"]
    (OUT_DIR / "flywheel_coverage.md").write_text("\n".join(md))
    print("\n".join(md))


if __name__ == "__main__":
    main()
