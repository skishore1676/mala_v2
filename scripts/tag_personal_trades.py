"""P1 driver: tag the operator's timestamped personal round-trips by playbook.

Reads the superset import batch, clusters fills into episodes (spec X2),
extracts entry-context features from the local 1-min bar cache, tags each
episode against the P0 spec, and writes (all local-only, gitignored):

  data/personal_imports/tagged/round_trips_tagged.csv
  data/personal_imports/tagged/fingerprint_report.md
  data/personal_imports/tagged/adjudication_round1.md   (Lathi-bus packet)

Usage:  .venv/bin/python scripts/tag_personal_trades.py [--sample-per-tag 10]
"""

from __future__ import annotations

import argparse
import base64
import csv
import math
import random
import sys
from collections import Counter, defaultdict
from datetime import timedelta
from pathlib import Path

import numpy as np

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from src.research.playbook_tagging import (  # noqa: E402
    PLAYBOOKS,
    Episode,
    SymbolBars,
    cluster_episodes,
    extract_features,
    load_timestamped_trades,
    tag_episode,
)

CORPUS = REPO / "data/personal_imports/processed/20260429_232737/round_trips.csv"
OUT_DIR = REPO / "data/personal_imports/tagged"
DATA_DIR = REPO / "data"

FEATURE_COLS = [
    "ret_15", "ret_60", "vwap_dist_atr", "stretch_pctile", "fresh_extreme_min",
    "run_len", "flash_mag", "vol_ratio_15", "trend_side_frac_60",
    "vma10_dist_pct", "pullback_depth_frac", "range_width_90m_pctile",
    "gap_pct", "minutes_since_open",
]


def svg_chart(bars: SymbolBars, ep: Episode, pre_min: int = 120, post_min: int = 60) -> str:
    """Dependency-free inline SVG: close + session VWAP for pre/post window,
    entry marker. No PnL shown (adjudication stays outcome-blind-ish)."""
    day = ep.entry_dt.date()
    b = bars.days.get(day)
    if b is None:
        return ""
    entry_min = ep.entry_dt.hour * 60 + ep.entry_dt.minute
    mins, close, vwap = b["min_of_day"], b["close"], b["vwap"]
    lo_i = int(np.searchsorted(mins, entry_min - pre_min))
    hi_i = int(np.searchsorted(mins, entry_min + post_min))
    e_i = int(np.searchsorted(mins, entry_min))
    seg_c, seg_v, seg_m = close[lo_i:hi_i], vwap[lo_i:hi_i], mins[lo_i:hi_i]
    if len(seg_c) < 10:
        return ""
    w, h, pad = 340, 150, 6
    y_lo = float(min(seg_c.min(), seg_v.min()))
    y_hi = float(max(seg_c.max(), seg_v.max()))
    y_rng = (y_hi - y_lo) or 1.0
    x_lo, x_rng = float(seg_m[0]), float(seg_m[-1] - seg_m[0]) or 1.0

    def pt(m: float, p: float) -> str:
        x = pad + (m - x_lo) / x_rng * (w - 2 * pad)
        y = h - pad - (p - y_lo) / y_rng * (h - 2 * pad)
        return f"{x:.1f},{y:.1f}"

    path_c = " ".join(pt(m, p) for m, p in zip(seg_m, seg_c))
    path_v = " ".join(pt(m, p) for m, p in zip(seg_m, seg_v))
    ex = pad + (entry_min - x_lo) / x_rng * (w - 2 * pad)
    arrow = "▲" if ep.thesis_dir > 0 else "▼"
    e_px = float(close[min(e_i, len(close) - 1)])
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{w}" height="{h}" '
        f'viewBox="0 0 {w} {h}"><rect width="{w}" height="{h}" fill="#fff"/>'
        f'<polyline points="{path_v}" fill="none" stroke="#e0a030" stroke-width="1" '
        f'stroke-dasharray="3,2"/>'
        f'<polyline points="{path_c}" fill="none" stroke="#1a56b0" stroke-width="1.3"/>'
        f'<line x1="{ex:.1f}" y1="{pad}" x2="{ex:.1f}" y2="{h - pad}" stroke="#c03030" '
        f'stroke-width="1"/>'
        f'<text x="{ex + 3:.1f}" y="16" font-size="11" fill="#c03030">{arrow} '
        f'{ep.entry_dt:%H:%M}</text>'
        f'<text x="{pad}" y="{h - 10}" font-size="9" fill="#666">{ep.underlying} '
        f'{day} · blue=px orange=VWAP · {pre_min}m pre / {post_min}m post · '
        f'entry {e_px:.2f}</text></svg>'
    )


def _median(vals: list[float]):
    xs = sorted(vals)
    return xs[len(xs) // 2] if xs else None


def _tag_row(tag: str, eps: list[Episode]) -> str:
    """One fingerprint-table row. Every stat cell is guarded independently:
    a tag with valid episodes but missing holds / DTE / PnL blanks only that
    cell (it must never crash on an empty list nor collapse the whole row)."""
    n_high = sum(1 for e in eps if e.confidence == "HIGH")
    n_med = sum(1 for e in eps if e.confidence == "MEDIUM")
    med_hold = _median([e.holding_minutes for e in eps if e.holding_minutes is not None])
    med_dte = _median([e.dte for e in eps if e.dte is not None])
    pnls = [e.pnl for e in eps if e.pnl is not None]
    hold_cell = f"{med_hold:.0f}" if med_hold is not None else ""
    dte_cell = f"{med_dte}" if med_dte is not None else ""
    if pnls:
        win_cell = f"{sum(1 for p in pnls if p > 0) / len(pnls):.0%}"
        sum_cell = f"{sum(pnls):+,.0f}"
    else:
        win_cell = sum_cell = ""
    return (
        f"| {tag} | {len(eps)} | {n_high} | {n_med} | "
        f"{hold_cell} | {dte_cell} | {win_cell} | {sum_cell} |"
    )


def kmeans_sidecar(episodes: list[Episode], k: int = 4, iters: int = 60) -> str:
    """Tiny numpy k-means on standardized features; contingency vs tags."""
    tagged = [e for e in episodes if e.features]
    mat = np.array(
        [[e.features.get(c, float("nan")) or float("nan") for c in FEATURE_COLS] for e in tagged],
        dtype=float,
    )
    col_med = np.nanmedian(mat, axis=0)
    inds = np.where(np.isnan(mat))
    mat[inds] = np.take(col_med, inds[1])
    mu, sd = mat.mean(0), mat.std(0)
    sd[sd == 0] = 1.0
    X = (mat - mu) / sd
    rng = np.random.default_rng(7)
    cent = X[rng.choice(len(X), size=k, replace=False)]
    for _ in range(iters):
        d = ((X[:, None, :] - cent[None, :, :]) ** 2).sum(-1)
        lab = d.argmin(1)
        for j in range(k):
            if (lab == j).any():
                cent[j] = X[lab == j].mean(0)
    lines = ["| cluster | n | " + " | ".join(p[:8] for p in PLAYBOOKS) + " | UNCLASS |",
             "|---|---|" + "---|" * (len(PLAYBOOKS) + 1)]
    for j in range(k):
        members = [tagged[i] for i in range(len(tagged)) if lab[i] == j]
        cnt = Counter(e.tag for e in members)
        lines.append(
            f"| {j} | {len(members)} | "
            + " | ".join(str(cnt.get(p, 0)) for p in PLAYBOOKS)
            + f" | {cnt.get('UNCLASSIFIED', 0)} |"
        )
    return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sample-per-tag", type=int, default=10)
    args = ap.parse_args()
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    trades = load_timestamped_trades(CORPUS)
    episodes = cluster_episodes(trades)
    print(f"trades(long,timestamped)={len(trades)} episodes={len(episodes)}")

    bars_by_sym: dict[str, SymbolBars] = {}
    skipped: Counter = Counter()
    for ep in episodes:
        if ep.underlying not in bars_by_sym:
            try:
                bars_by_sym[ep.underlying] = SymbolBars(ep.underlying, DATA_DIR)
            except FileNotFoundError:
                bars_by_sym[ep.underlying] = None  # type: ignore[assignment]
        sb = bars_by_sym[ep.underlying]
        if sb is None:
            skipped[ep.underlying] += 1
            ep.reason = "no bar cache for symbol"
            continue
        ep.features = extract_features(sb, ep.entry_dt, ep.thesis_dir)
        ep.tag, ep.confidence, ep.reason = tag_episode(ep.features, ep.thesis_dir)

    # ── tagged CSV ──
    csv_path = OUT_DIR / "round_trips_tagged.csv"
    with open(csv_path, "w", newline="") as fh:
        wr = csv.writer(fh)
        wr.writerow(
            ["episode_id", "underlying", "thesis_dir", "entry_dt_et", "n_fills",
             "dte", "holding_minutes", "pnl", "tag", "confidence", "reason"]
            + FEATURE_COLS
        )
        for e in episodes:
            wr.writerow(
                [e.episode_id, e.underlying, e.thesis_dir, e.entry_dt.isoformat(),
                 e.n_fills, e.dte, e.holding_minutes, e.pnl, e.tag, e.confidence,
                 e.reason]
                + [e.features.get(c, "") for c in FEATURE_COLS]
            )
    print(f"wrote {csv_path}")

    # ── fingerprint report ──
    ok = [e for e in episodes if e.features]
    by_tag: dict[str, list[Episode]] = defaultdict(list)
    for e in ok:
        by_tag[e.tag].append(e)
    rep = ["# Playbook fingerprint report (P1, machine tags — pre-adjudication)",
           "",
           f"- episodes: {len(episodes)} (from {len(trades)} long timestamped fills)",
           f"- with bar context: {len(ok)}; skipped (no bars): {dict(skipped)}",
           f"- UNCLASSIFIED share: {len(by_tag.get('UNCLASSIFIED', []))/max(1,len(ok)):.0%}"
           " (P1 target ≤25% at freeze)",
           "", "## Tag distribution", "",
           "| tag | n | HIGH | MED | med hold (m) | med DTE | win% | sum PnL |",
           "|---|---|---|---|---|---|---|---|"]
    for tag in list(PLAYBOOKS) + ["UNCLASSIFIED"]:
        eps = by_tag.get(tag, [])
        if not eps:
            rep.append(f"| {tag} | 0 | | | | | | |")
            continue
        rep.append(_tag_row(tag, eps))
    rep += ["", "## Symbol league by tag", ""]
    for tag in PLAYBOOKS:
        eps = by_tag.get(tag, [])
        if eps:
            top = Counter(e.underlying for e in eps).most_common(5)
            rep.append(f"- **{tag}**: " + ", ".join(f"{s} ({n})" for s, n in top))
    rep += ["", "## Time-of-day (episodes per hour CT, by tag)", ""]
    hour_rows = {tag: Counter((e.entry_dt.hour) for e in by_tag.get(tag, []))
                 for tag in PLAYBOOKS}
    hours = list(range(9, 17))
    rep.append("| tag | " + " | ".join(f"{h}:xx" for h in hours) + " |")
    rep.append("|---|" + "---|" * len(hours))
    for tag, cnt in hour_rows.items():
        rep.append(f"| {tag[:10]} | " + " | ".join(str(cnt.get(h, 0)) for h in hours) + " |")
    rep += ["", "## Clustering sidecar (k=4 on standardized features vs tags)", "",
            kmeans_sidecar(ok), "",
            "_PnL appears in this report only; it is never a tagging input._"]
    (OUT_DIR / "fingerprint_report.md").write_text("\n".join(rep))
    print(f"wrote {OUT_DIR / 'fingerprint_report.md'}")

    # ── adjudication round-1 packet ──
    rng = random.Random(11)
    sample: list[Episode] = []
    for tag in list(PLAYBOOKS) + ["UNCLASSIFIED"]:
        pool = by_tag.get(tag, [])
        highs = [e for e in pool if e.confidence == "HIGH"]
        others = [e for e in pool if e.confidence != "HIGH"]
        take = rng.sample(highs, min(len(highs), args.sample_per_tag // 2)) + \
            rng.sample(others, min(len(others), args.sample_per_tag - args.sample_per_tag // 2))
        sample += take
    sample.sort(key=lambda e: e.entry_dt)
    pk = ["# Adjudication round 1 — machine playbook tags vs your eye",
          "",
          "For each episode: the chart shows 120m before / 60m after your entry "
          "(red line = your entry, blue = price, orange = session VWAP). The tag "
          "line gives the machine's call and its reason. **Reply in pointy "
          "brackets on any card you disagree with** — e.g. `<EXHAUSTION not "
          "FLASH — that move was 40m old>` or `<OTHER — earnings lotto>`. "
          "A card with no comment counts as AGREE.", ""]
    for e in sample:
        sb = bars_by_sym.get(e.underlying)
        svg = svg_chart(sb, e) if sb else ""
        uri = ("data:image/svg+xml;base64," +
               base64.b64encode(svg.encode()).decode()) if svg else ""
        pk += [f"### {e.episode_id} — {e.underlying} "
               f"{'CALL' if e.thesis_dir > 0 else 'PUT'} · {e.entry_dt:%a %Y-%m-%d %H:%M ET}",
               f"**Machine: {e.tag} ({e.confidence})** — {e.reason}",
               f"fills: {e.n_fills} · DTE {e.dte} · held {e.holding_minutes:.0f}m"
               if e.holding_minutes is not None else f"fills: {e.n_fills} · DTE {e.dte}",
               (f"![chart]({uri})" if uri else "_(no chart — bars missing)_"),
               "", ""]
    (OUT_DIR / "adjudication_round1.md").write_text("\n".join(pk))
    print(f"wrote {OUT_DIR / 'adjudication_round1.md'} ({len(sample)} episodes)")


if __name__ == "__main__":
    main()
