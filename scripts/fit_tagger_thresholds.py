"""Fit tag_episode thresholds to the adjudication gold set (P1).

Grid-scans DEFAULT_THRESHOLDS candidates against the operator's explicit
labels (primary score) and the silent-agree cards (secondary). Prints the
best combinations; the chosen values get frozen into
src/research/playbook_tagging.DEFAULT_THRESHOLDS with provenance.

Usage:  .venv/bin/python scripts/fit_tagger_thresholds.py
"""

from __future__ import annotations

import csv
import itertools
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from src.research.playbook_tagging import tag_episode  # noqa: E402
from scripts.tag_personal_trades import FEATURE_COLS  # noqa: E402

TAGGED = REPO / "data/personal_imports/tagged/round_trips_tagged.csv"
GOLD = REPO / "data/personal_imports/tagged/gold_round1.csv"

GRID = {
    "leg_atr_high": [0.12, 0.15, 0.20, 0.25, 0.30],
    "leg_age_max": [15, 20, 30],
    "run_120_atr": [0.30, 0.40, 0.50],
    "run_day_atr": [0.40, 0.60, 0.80],
    "run_3d_atr": [0.80, 1.00, 1.50],
    "run_5d_atr": [1.00, 1.50, 2.00],
    "stretch_hi": [70, 80],
}


def _num(v: str):
    if v in ("True", "False"):
        return v == "True"
    try:
        return float(v)
    except (TypeError, ValueError):
        return float("nan")


def main() -> None:
    gold = {}
    for r in csv.DictReader(open(GOLD)):
        gold[r["episode_id"]] = (r["gold_label"], r["source"])
    feats: dict[str, tuple[dict, int]] = {}
    for r in csv.DictReader(open(TAGGED)):
        if r["episode_id"] in gold:
            f = {c: _num(r.get(c, "")) for c in FEATURE_COLS}
            # ints the tagger compares by identity; bools stay bools
            for c in ("leg_dir", "run_dir_30", "run_dir_60", "run_dir_120",
                      "move_dir", "trend_dir"):
                v = f.get(c)
                f[c] = int(v) if isinstance(v, float) and v == v else 0
            for c in ("failed_retest", "touched_vma10"):
                f[c] = bool(f.get(c))
            feats[r["episode_id"]] = (f, int(r["thesis_dir"]))

    def score(th):
        expl = expl_ok = sil = sil_ok = 0
        for eid, (label, src) in gold.items():
            if eid not in feats:
                continue
            f, d = feats[eid]
            tag, _, _ = tag_episode(f, d, th=th)
            ok = tag == label or (label == "OTHER" and tag == "UNCLASSIFIED")
            if src == "operator_comment":
                expl += 1
                expl_ok += ok
            else:
                sil += 1
                sil_ok += ok
        return expl_ok, expl, sil_ok, sil

    keys = list(GRID)
    results = []
    for combo in itertools.product(*(GRID[k] for k in keys)):
        th = dict(zip(keys, combo))
        eo, e, so, s = score(th)
        results.append((eo, so, th))
    results.sort(key=lambda x: (-x[0], -x[1]))
    _, _, _, s_tot = score(dict(zip(keys, (GRID[k][0] for k in keys))))
    e_tot = sum(1 for _, (l, src) in gold.items() if src == "operator_comment")
    print(f"gold: {e_tot} explicit, {s_tot} silent-agree scoreable\ntop 8 combos:")
    for eo, so, th in results[:8]:
        print(f"  explicit {eo}/{e_tot}  silent {so}/{s_tot}  {th}")


if __name__ == "__main__":
    main()
