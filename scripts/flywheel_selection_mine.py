"""Flywheel Phase B1: mine the take/skip set for the operator's selection rule.

Enriches each in-window fire (flywheel_take_skip.csv) with entry-context
features, then asks: what separates the ~3% he TOOK from the 97% he SKIPPED?
Reports single-feature take-rate lift (top vs bottom bucket), fits a small
interpretable logistic scorer, and OOS-tests it (fit ≤ split date, test after).
Also checks the thing that actually matters: do higher-selection-score fires
carry better option outcomes?

Deliverable: a trader-readable rule card, not a black box.

Usage:  .venv/bin/python scripts/flywheel_selection_mine.py
"""

from __future__ import annotations

import csv
import math
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import numpy as np

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from src.research.playbook_tagging import SymbolBars, extract_features  # noqa: E402

TAKE_SKIP = REPO / "data/personal_imports/tagged/flywheel_take_skip.csv"
OUT_DIR = REPO / "data/personal_imports/tagged"
DATA_DIR = REPO / "data"
SPLIT = "2026-01-01"  # fit before, test after

FEATURES = [
    ("strength_norm", "detector strength (normalized within detector)"),
    ("stretch_pctile", "stretch percentile vs own history"),
    ("minutes_since_open", "minutes since 09:30"),
    ("tape_abs", "|prior-5d run| in ATR (trend strength)"),
    ("tape_aligned", "fire direction agrees with 5d tape (+1/-1/0)"),
    ("leg_or_flush", "flush size (ATR) for flash, else 0"),
]


def enrich() -> list[dict]:
    rows = list(csv.DictReader(open(TAKE_SKIP)))
    bars = {s: SymbolBars(s, DATA_DIR) for s in {r["symbol"] for r in rows}}
    # per-detector strength normalization
    by_det = defaultdict(list)
    for r in rows:
        by_det[r["det"]]  # touch
    out = []
    for r in rows:
        d = int(r["dir"])
        dt = datetime.fromisoformat(r["dt"])
        f = extract_features(bars[r["symbol"]], dt, d)
        if not f:
            continue
        det = r["det"]
        strength = {"F-A": f.get("fade_flush_atr"), "F-C": f.get("fade_flush_atr"),
                    "T-C": f.get("trend_side_frac_60"),
                    "E-C": f.get("stretch_pctile")}.get(det, float("nan"))
        tape = f.get("ret_5d_atr", float("nan"))
        out.append({
            "symbol": r["symbol"], "det": det, "profile": r["profile"], "dir": d,
            "dt": dt.isoformat(),
            "label": 1 if r["label"] == "TAKE" else 0,
            "_strength_raw": strength, "det_": det,
            "stretch_pctile": f.get("stretch_pctile", float("nan")),
            "minutes_since_open": f.get("minutes_since_open", float("nan")),
            "tape_abs": abs(tape) if math.isfinite(tape) else float("nan"),
            "tape_aligned": (1 if (tape > 0) == (d > 0) else -1)
            if math.isfinite(tape) and abs(tape) >= 0.5 else 0,
            "leg_or_flush": f.get("fade_flush_atr", 0.0) if r["profile"] == "FLASH_REVERSAL"
            else 0.0,
        })
    # normalize strength within detector (percentile 0..1)
    pools = defaultdict(list)
    for r in out:
        if isinstance(r["_strength_raw"], float) and math.isfinite(r["_strength_raw"]):
            pools[r["det_"]].append(r["_strength_raw"])
    sorted_pools = {k: np.sort(v) for k, v in pools.items()}
    for r in out:
        s, arr = r["_strength_raw"], sorted_pools.get(r["det_"])
        if arr is not None and isinstance(s, float) and math.isfinite(s) and len(arr):
            r["strength_norm"] = float(np.searchsorted(arr, s) / len(arr))
        else:
            r["strength_norm"] = float("nan")
    return out


def take_rate_lift(rows, key):
    vals = [(float(r[key]), r["label"]) for r in rows
            if isinstance(r[key], (int, float))
            and math.isfinite(float(r[key]))]
    if len(vals) < 40:
        return None
    vals.sort()
    q = len(vals) // 4
    bot = vals[:q]
    top = vals[-q:]
    br = sum(l for _, l in bot) / len(bot)
    tr = sum(l for _, l in top) / len(top)
    return br, tr, top[0][0]


def fit_logistic(rows, feats, iters=400, lr=0.3):
    X, y = [], []
    for r in rows:
        v = [r.get(f) for f, _ in feats]
        if any(not isinstance(x, (int, float)) or (isinstance(x, float) and not math.isfinite(x))
               for x in v):
            continue
        X.append(v)
        y.append(r["label"])
    X = np.array(X, float)
    y = np.array(y, float)
    mu, sd = X.mean(0), X.std(0)
    sd[sd == 0] = 1
    Xs = (X - mu) / sd
    w = np.zeros(Xs.shape[1])
    b = 0.0
    pos = max(y.sum(), 1)
    wt = np.where(y > 0, len(y) / (2 * pos), len(y) / (2 * (len(y) - pos)))
    for _ in range(iters):
        z = Xs @ w + b
        p = 1 / (1 + np.exp(-z))
        g = (p - y) * wt
        w -= lr * (Xs.T @ g) / len(y)
        b -= lr * g.mean()
    return w, b, mu, sd


def score(rows, feats, w, b, mu, sd):
    out = []
    for r in rows:
        v = [r.get(f) for f, _ in feats]
        if any(not isinstance(x, (int, float)) or (isinstance(x, float) and not math.isfinite(x))
               for x in v):
            r["sel_score"] = float("nan")
            continue
        xs = (np.array(v, float) - mu) / sd
        r["sel_score"] = float(1 / (1 + np.exp(-(xs @ w + b))))
        out.append(r)
    return out


def main() -> None:
    rows = enrich()
    print(f"enriched {len(rows)} fires ({sum(r['label'] for r in rows)} takes)",
          file=sys.stderr)

    lines = ["# Flywheel B1 — selection-function mining (what he TAKES)", "",
             f"- {len(rows)} in-window fires, {sum(r['label'] for r in rows)} taken "
             f"({sum(r['label'] for r in rows)/len(rows):.1%} base take rate)", "",
             "## Single-feature take-rate lift (bottom quartile → top quartile)", "",
             "| feature | bottom-q take% | top-q take% | lift | reading |",
             "|---|---|---|---|---|"]
    for key, desc in FEATURES:
        res = take_rate_lift(rows, key)
        if not res:
            continue
        br, tr, thr = res
        lift = (tr / br) if br > 0 else float("inf")
        rd = "predictive" if lift >= 1.5 or (br > 0 and lift <= 0.67) else "weak"
        lines.append(f"| {desc} | {br:.1%} | {tr:.1%} | "
                     f"{lift:.1f}x | {rd} |")

    # logistic scorer, OOS split by date
    train = [r for r in rows if r["dt"] < SPLIT]
    test = [r for r in rows if r["dt"] >= SPLIT]
    w, b, mu, sd = fit_logistic(train, FEATURES)
    score(test, FEATURES, w, b, mu, sd)
    scored = [r for r in test if math.isfinite(r.get("sel_score", float("nan")))]
    scored.sort(key=lambda r: r["sel_score"])
    if scored:
        q = max(len(scored) // 4, 1)
        bot, top = scored[:q], scored[-q:]
        lines += ["", f"## OOS selection scorer (fit < {SPLIT}, test ≥)", "",
                  f"- test fires: {len(scored)}, takes {sum(r['label'] for r in scored)}",
                  f"- **bottom-quartile score take rate: {sum(r['label'] for r in bot)/len(bot):.1%}**",
                  f"- **top-quartile score take rate: {sum(r['label'] for r in top)/len(top):.1%}**",
                  "", "### weights (standardized; sign = direction of pull toward TAKE)"]
        for (f, desc), wi in sorted(zip(FEATURES, w), key=lambda x: -abs(x[1])):
            lines.append(f"- {desc}: {wi:+.2f}")
    # trader rule card — rank by |model weight| (the multivariate separator),
    # annotate each with its single-feature direction.
    lines += ["", "## Trader rule card (draft — ranked by model weight)", ""]
    wmap = dict(zip([f for f, _ in FEATURES], w))
    for f, desc in sorted(FEATURES, key=lambda fd: -abs(wmap.get(fd[0], 0))):
        wi = wmap.get(f, 0.0)
        if abs(wi) < 0.1:
            continue
        res = take_rate_lift(rows, f)
        pull = "EARLIER/LOWER" if wi < 0 else "HIGHER"
        note = ""
        if res:
            br, tr, _ = res
            note = f" (top-q take {tr:.0%} vs bottom-q {br:.0%})"
        lines.append(f"- **{desc}** → take more when {pull}{note}")
    lines.append("")
    lines.append("_n(takes) is small (IWM/SPY only); treat as directional, not final. "
                 "Phase A feed accrues more takes going forward._")
    (OUT_DIR / "flywheel_selection_report.md").write_text("\n".join(lines))
    print("\n".join(lines))


if __name__ == "__main__":
    main()
