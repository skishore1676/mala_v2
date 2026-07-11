"""Flywheel Phase B1 economic validation: does selecting like the operator pay?

Predicting his clicks (take-rate lift) is not the goal — the goal is that
his-style selection improves P&L. This computes the native-profile option-path
outcome for every OOS fire (test window), scores each with the B1 selection
model (fit on the pre-split window), and compares mean NET expectancy of the
top-score quartile vs the bottom — the Phase B bar (≥2%/trade gross separation
OOS).

Usage:  .venv/bin/python scripts/flywheel_selection_economic.py
"""

from __future__ import annotations

import csv
import math
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import polars as pl

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from scripts.flywheel_selection_mine import enrich, fit_logistic, score, FEATURES, SPLIT  # noqa: E402
from scripts.flywheel_daily import fire_outcome  # noqa: E402
from src.research.playbook_tagging import SymbolBars  # noqa: E402

DATA_DIR = REPO / "data"
OUT_DIR = REPO / "data/personal_imports/tagged"
COST = 3.0  # midpoint of the 2-4% haircut band


def frames() -> dict:
    out = {}
    for s in ("IWM", "SPY"):
        sb = SymbolBars(s, DATA_DIR)
        ts, cl, hi, lo = [], [], [], []
        for d in sb.day_list:
            b = sb.days[d]
            for i in range(len(b["close"])):
                m = int(b["min_of_day"][i])
                ts.append(datetime(d.year, d.month, d.day, m // 60, m % 60,
                                   tzinfo=ZoneInfo("America/New_York")))
                cl.append(float(b["close"][i])); hi.append(float(b["high"][i]))
                lo.append(float(b["low"][i]))
        out[s] = pl.DataFrame({"timestamp": ts, "close": cl, "high": hi, "low": lo}) \
            .with_columns(pl.col("timestamp").dt.convert_time_zone("America/New_York"))
    return out


def main() -> None:
    rows = enrich()
    train = [r for r in rows if r["dt"] < SPLIT]
    test = [r for r in rows if r["dt"] >= SPLIT]
    w, b, mu, sd = fit_logistic(train, FEATURES)
    score(test, FEATURES, w, b, mu, sd)
    scored = [r for r in test if math.isfinite(r.get("sel_score", float("nan")))]
    print(f"scoring {len(scored)} OOS fires for option outcome…", file=sys.stderr)

    et = ZoneInfo("America/New_York")
    fr = frames()
    for i, r in enumerate(scored):
        sym = r["symbol"]
        dt = datetime.fromisoformat(r["dt"]).astimezone(et)  # ET-zoned like frames
        fire = {"dir": r["dir"], "dt": dt, "profile": r["profile"]}
        out, _ = fire_outcome(fr[sym], fire, sym, use_real_iv=False)  # modeled: fast, relative
        r["outcome"] = out
        if (i + 1) % 500 == 0:
            print(f"  {i+1}/{len(scored)}", file=sys.stderr)

    valid = [r for r in scored if math.isfinite(r.get("outcome", float("nan")))]
    valid.sort(key=lambda r: r["sel_score"])
    q = max(len(valid) // 4, 1)
    bot, top = valid[:q], valid[-q:]

    def net(rs):
        return sum(r["outcome"] for r in rs) / len(rs) - COST if rs else float("nan")
    lines = [
        "# Flywheel B1 — economic validation (does his-style selection pay?)",
        "",
        f"- OOS fires with outcome: {len(valid)} (test ≥ {SPLIT}); cost haircut {COST:.0f}%",
        f"- all-fires mean NET expectancy: **{net(valid):+.1f}%/trade**",
        f"- **bottom-quartile selection score: {net(bot):+.1f}%/trade net** "
        f"(mean take rate {sum(r['label'] for r in bot)/len(bot):.1%})",
        f"- **top-quartile selection score: {net(top):+.1f}%/trade net** "
        f"(mean take rate {sum(r['label'] for r in top)/len(top):.1%})",
        f"- separation: **{net(top) - net(bot):+.1f}%/trade** (Phase B bar: ≥2%)",
        "",
        "## By playbook, top-vs-bottom selection quartile (net%/trade)",
        "",
        "| profile | n | bottom-q net | top-q net | separation |",
        "|---|---|---|---|---|",
    ]
    byp = defaultdict(list)
    for r in valid:
        byp[r["profile"]].append(r)
    for prof, rs in sorted(byp.items()):
        rs.sort(key=lambda r: r["sel_score"])
        qq = max(len(rs) // 4, 1)
        lines.append(f"| {prof} | {len(rs)} | {net(rs[:qq]):+.1f}% | "
                     f"{net(rs[-qq:]):+.1f}% | {net(rs[-qq:]) - net(rs[:qq]):+.1f}% |")
    lines += ["", f"_The economic question: does taking his top-quartile fires beat "
              f"his bottom-quartile? Positive separation = selection has P&L value, "
              f"not just click-prediction. n small; directional._"]
    (OUT_DIR / "flywheel_economic_report.md").write_text("\n".join(lines))
    print("\n".join(lines))


if __name__ == "__main__":
    main()
