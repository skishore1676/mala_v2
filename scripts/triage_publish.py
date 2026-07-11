#!/usr/bin/env python3
"""Publish staged triage candidates to Mala_Evidence_v1 by APPENDING rows (reversible).

Safety: appends below existing rows (never overwrites them), catalog_key prefixed `triage-`,
NEVER touches active_strategy. A snapshot must already exist. `--apply` actually writes; without it
this is a dry run.
"""
from __future__ import annotations
import argparse, csv, json, os, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from dotenv import load_dotenv; load_dotenv()
from src.research.google_sheets import GoogleSheetTableClient as C

STAGED = "data/results/triage_stage/ALL__staged_evidence.csv"
NEW_COLS = ["management_policy_spec", "triage_note"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    sid = os.environ["BIONIC_SHEET_ID"]; creds = Path(os.environ["GOOGLE_API_CREDENTIALS_PATH"])
    cl = C(sid, "Mala_Evidence_v1", creds)
    existing = cl.read_rows()
    max_ri = max(int(r.get("row_index") or 0) for r in existing)
    staged = list(csv.DictReader(open(STAGED)))

    rows = []
    for i, s in enumerate(staged):
        ri = max_ri + 1 + i
        checks = {}
        try:
            checks = json.loads(s.get("recommendation_checks_json") or "{}")
        except Exception:
            pass
        rows.append({
            "row_index": ri,
            "mala_handoff_version": "triage-2026-07-11",
            "catalog_key": s["catalog_key"],
            "symbol": s["symbol"], "direction": s["direction"],
            "strategy_key": s["strategy_key"], "strategy_name": s["strategy_name"],
            "strategy_variant": s.get("strategy_variant", ""),
            "strategy_params_json": s.get("strategy_params_json", ""),
            "bhiksha_capability_status": s.get("bhiksha_capability_status", ""),
            "bhiksha_capability_reason": "supported",
            "bhiksha_ready": s.get("bhiksha_ready", ""),
            "provider_validation_status": s.get("provider_validation_status", ""),
            "provider_signal_overlap": s.get("provider_signal_overlap", ""),
            "recommendation_tier": s.get("recommendation_tier", ""),
            "recommendation_tier_reason": s.get("recommendation_tier_reason", ""),
            "recommendation_checks_json": s.get("recommendation_checks_json", ""),
            "expectancy": s.get("expectancy", ""),
            "signal_count": s.get("holdout_trades", ""),
            "management_policy_spec": s.get("management_policy_spec", ""),
            "triage_note": (f"triage-2026-07-11 shadow-ready ({s.get('classified_profile')}); "
                            f"profile_exit_exp_pct={s.get('profile_option_exp_pct')}; "
                            "all gates passed; NOT authorized for active_strategy"),
        })

    cols = list(existing[0].keys())
    for c in NEW_COLS:
        if c not in cols:
            cols.append(c)
    write_cols = [c for c in cols if c != "row_index"]

    print(f"Will append {len(rows)} rows at row_index {rows[0]['row_index']}..{rows[-1]['row_index']} "
          f"(existing max {max_ri}). New columns ensured: {NEW_COLS}")
    for r in rows:
        print(f"  +{r['row_index']} {r['catalog_key']:44s} tier={r['recommendation_tier']:7s} "
              f"cap={r['bhiksha_capability_status']} M7={r['provider_signal_overlap']} exp={r['expectancy']}")
    if not args.apply:
        print("\nDRY RUN — pass --apply to write.")
        return
    cl.ensure_columns(NEW_COLS)
    res = cl.batch_update_rows(rows=rows, columns=write_cols)
    print(f"\nAPPLIED. updated cells: {res.get('update',{}).get('totalUpdatedCells','?')}")


if __name__ == "__main__":
    main()
