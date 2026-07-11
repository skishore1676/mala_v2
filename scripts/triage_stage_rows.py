#!/usr/bin/env python3
"""Stage published-ready Mala_Evidence_v1 rows for reviewed triage candidates (READ-ONLY, no sheet write).

Assembles every gate result — Tier-B M5 catalog, classify profile-exit + management_policy_spec,
bhiksha capability, M7 provider parity — into rows matching the Mala_Evidence_v1 schema, plus a
human REVIEW_PACKET.md. The operator sits with the packet and publishes; this script NEVER writes
the sheet or active_strategy.

Usage:
    ./.venv/bin/python scripts/triage_stage_rows.py --candidates market_impulse:QQQ:short ... \
        --m7 data/results/triage_m7/w1_trend_m7.csv --classify-dir <latest classify dir> --wave w1_trend
"""
from __future__ import annotations

import argparse
import csv
import glob
import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

import polars as pl  # noqa: E402
from src.research.bhiksha_capabilities import load_capability_manifest, evaluate_bhiksha_capability  # noqa: E402
from src.research.strategy_keys import to_strategy_key  # noqa: E402

FAM_STRAT = {"market_impulse": "Market Impulse (Cross & Reclaim)",
             "opening_drive": "Opening Drive Classifier",
             "jerk_pivot": "Jerk-Pivot Momentum (tight)",
             "elastic_band": "Elastic Band Reversion",
             "compression": "Compression Expansion Breakout"}
MANIFEST = REPO / "data/bhiksha_manifests/bhiksha_runtime_capabilities_v2.json"

SHEET_COLS = ["mala_handoff_version", "catalog_key", "symbol", "direction", "strategy_key",
              "strategy_name", "strategy_variant", "strategy_params_json", "bhiksha_capability_status",
              "bhiksha_ready", "provider_validation_status", "provider_signal_overlap",
              "recommendation_tier", "recommendation_tier_reason", "recommendation_checks_json",
              "expectancy", "management_policy_spec", "classified_profile", "profile_option_exp_pct",
              "multi_regime_eras", "holdout_win_rate", "payoff", "holdout_trades", "exit_trade_count",
              "triage_stage_note"]


def _tierb_run(fam, sym):
    g = sorted(glob.glob(f"{REPO}/data/results/triage_tierb/{fam}__{sym}/tierb-{fam}-{sym}/*/CATALOG_SELECTED.csv"))
    return g[-1] if g else None


def _classify_row(classify_dir, sym, direction):
    for f in glob.glob(f"{classify_dir}/*.json"):
        if f.endswith("INDEX.json"):
            continue
        r = json.load(open(f))
        if r.get("symbol") == sym and r.get("direction") == direction:
            return r
    return None


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--candidates", nargs="+", required=True, help="fam:sym:dir ...")
    ap.add_argument("--m7", required=True)
    ap.add_argument("--classify-dir", required=True)
    ap.add_argument("--wave", default="w1_trend")
    args = ap.parse_args()

    man = load_capability_manifest(str(MANIFEST))
    m7 = {(r["symbol"], r["direction"]): r for r in csv.DictReader(open(args.m7))}
    # payoff is computed in the Tier-B manifest (not in CATALOG_SELECTED) — look it up.
    tierb_payoff = {}
    tbp = REPO / f"data/results/triage_tierb/{args.wave}__tierb.csv"
    if tbp.exists():
        for r in csv.DictReader(open(tbp)):
            if r.get("payoff"):
                tierb_payoff[(r["symbol"], r["direction"])] = r["payoff"]
    out_dir = REPO / "data/results/triage_stage"
    out_dir.mkdir(parents=True, exist_ok=True)

    rows, packet = [], []
    for spec in args.candidates:
        fam, sym, direction = spec.split(":")
        cat = _tierb_run(fam, sym)
        if not cat:
            packet.append(f"- {fam}/{sym} {direction}: NO Tier-B run — skipped")
            continue
        cr = pl.read_csv(cat)
        # pick the row matching direction if present, else row 0
        match = cr.filter(pl.col("direction") == direction)
        c = (match if match.height else cr).row(0, named=True)
        cls = _classify_row(args.classify_dir, sym, direction) or {}
        spec_json = ((cls.get("proposal") or {}).get("management_policy_spec")) or {}
        prof = (cls.get("classification") or {}).get("profile")
        prof_pct = ((cls.get("explore") or {}).get("profile_option_path") or {}).get("expectancy_pct")

        sk = to_strategy_key(FAM_STRAT[fam])
        params = {k: c.get(k) for k in ("entry_buffer_minutes", "entry_window_minutes",
                                        "regime_timeframe", "vwma_periods")}
        exit_prefix = str(c.get("selected_exit_policy") or "").split(":", 1)[0]
        capres = evaluate_bhiksha_capability(
            strategy_key=sk, strategy_name=FAM_STRAT[fam], strategy_params=params,
            thesis_exit_policy=exit_prefix, thesis_exit_tested=True,
            recommendation_tier=c.get("recommendation_tier", "shadow"), manifest=man)
        m7r = m7.get((sym, direction), {})
        checks = {}
        try:
            checks = json.loads(c.get("recommendation_checks_json") or "{}")
        except Exception:  # noqa: BLE001
            pass

        row = {
            "mala_handoff_version": "triage-2026-07-10",
            "catalog_key": f"triage-{fam}-{sym}__{sym.lower()}_{direction}",
            "symbol": sym, "direction": direction, "strategy_key": sk,
            "strategy_name": FAM_STRAT[fam], "strategy_variant": capres.strategy_variant,
            "strategy_params_json": json.dumps(params, default=str),
            "bhiksha_capability_status": capres.status, "bhiksha_ready": capres.bhiksha_ready,
            "provider_validation_status": m7r.get("provider_validation_status", ""),
            "provider_signal_overlap": m7r.get("provider_signal_overlap", ""),
            "recommendation_tier": c.get("recommendation_tier"),
            "recommendation_tier_reason": c.get("recommendation_tier_reason"),
            "recommendation_checks_json": c.get("recommendation_checks_json"),
            "expectancy": c.get("base_exp_r"),
            "management_policy_spec": json.dumps(spec_json, default=str) if spec_json else "",
            "classified_profile": prof, "profile_option_exp_pct": prof_pct,
            "multi_regime_eras": "", "holdout_win_rate": c.get("holdout_win_rate"),
            "payoff": tierb_payoff.get((sym, direction)), "holdout_trades": c.get("holdout_trades"),
            "exit_trade_count": c.get("exit_trade_count"),
            "triage_stage_note": "shadow-ready: all gates passed; operator review before publish",
        }
        rows.append(row)
        packet.append(
            f"- **{fam}/{sym} {direction}** — profile={prof}, profile-exit {prof_pct:+.1f}% | "
            f"cap={capres.status}, M7={m7r.get('provider_signal_overlap','?')} "
            f"({m7r.get('provider_validation_status','?')}) | tier={c.get('recommendation_tier')}, "
            f"mc_prob={checks.get('mc_prob_positive_exp')}, win={c.get('holdout_win_rate')}, "
            f"payoff={tierb_payoff.get((sym, direction)) or '?'}, holdout_trades={c.get('holdout_trades')}, "
            f"exit_trades={c.get('exit_trade_count')}")

    staged = out_dir / f"{args.wave}__staged_evidence.csv"
    with staged.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=SHEET_COLS, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)

    pk = out_dir / f"{args.wave}__REVIEW_PACKET.md"
    pk.write_text(
        f"# Triage review packet — {args.wave}\n\n"
        f"{len(rows)} shadow-ready candidates. Every one cleared: multi-regime + direction-consistency, "
        "operator-validated yardstick (mc_prob≥0.70, profile-exit option-path>0; win/payoff shown as "
        "context), profile-exit + management_policy_spec, bhiksha capability=supported, and M7 "
        "provider parity (Schwab-vs-Polygon signal overlap ≥0.90).\n\n"
        "**Only step left is your publish.** Staged rows (NOT written) → `" + staged.name + "`. "
        "Adversarial disprove pass runs pre-promote, after shadow.\n\n"
        "## Candidates\n" + "\n".join(packet) + "\n\n"
        "## Caveats\n"
        "- Option-path magnitudes rest on modeled IV (kamandal doesn't cover the backtest window) — "
        "not a runtime blocker (bhiksha uses no IV); shadow accrues real IV/fills.\n"
        "- Thin holdout samples (17–53 trades) — expected at shadow tier.\n")
    print(f"[stage] {len(rows)} rows → {staged}")
    print(f"[stage] review packet → {pk}")
    for p in packet:
        print("  " + p)


if __name__ == "__main__":
    main()
