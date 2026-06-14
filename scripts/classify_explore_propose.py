#!/usr/bin/env python3
"""Phase 3 entry point: classify -> explore -> propose, per promoted row.

For each promoted strategy/evidence row (the local m7 evidence pilots the
analysis scripts already use), this:

  1. CLASSIFY  - deterministically classify the row's strategy into one of the
     four operator playbooks (Flash/Exhaustion Reversal, Trend Continuation,
     Range Expansion) via ``src.research.playbook_classifier``. Low-confidence
     rows are flagged for operator review, never force-assigned.

  2. EXPLORE   - score the S4 option-path band for the classified profile
     (``score_profile_band``, ``use_real_iv=True`` where kamandal has IV), and
     score the best LEGACY exit (``optimize_underlying_exit``'s best non-profile
     candidate) on the same holdout, so profile vs legacy are comparable.

  3. PROPOSE   - pick the best exit (option-path expectancy, operator profile
     favored on a tie since it is live-validated and tighter) and serialize it
     as a kernel ``ManagementPolicySpec`` JSON -- the ``management_policy_spec``
     contract the bhiksha active_plan compiler reads.

Emits one per-row artifact + an index under
``research/results/exit_profile_classify_propose/<stamp>/``.

READ-ONLY: no Google Sheet push, no external service, no live. Local artifacts
only. ``profile_exit_drives_live`` is intentionally NOT set (live stays the
operator's manual flip).

    python scripts/classify_explore_propose.py                 # default candidate set
    python scripts/classify_explore_propose.py --run-dir <dir> [--run-dir <dir> ...]
    python scripts/classify_explore_propose.py --data-root /path/to/data
"""
from __future__ import annotations

import argparse
import inspect
import json
import sys
from datetime import UTC, date, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "scripts"))

import polars as pl
from loguru import logger

from src.chronos.storage import LocalStorage
from src.newton.engine import PhysicsEngine
from src.research.exit_optimizer import optimize_underlying_exit
from src.research.exit_proposal import propose_management_policy_spec, select_exit
from src.research.option_translation import score_profile_band
from src.research.playbook_classifier import classify_strategy
from src.research.strategy_keys import to_strategy_key
from src.strategy.base import required_feature_union
from src.strategy.factory import build_strategy

# Reuse the existing exploration engine's helpers (family-name normalization +
# config projection) so this stays apples-to-apples with analyze_profile_*.py.
from analyze_profile_exits import _config_for, _strategy_family_name  # noqa: E402

logger.remove()
logger.add(sys.stderr, level="ERROR")

# Holdout window (matches analyze_profile_options.py / analyze_profile_exits.py).
START, HOLDOUT_START, HOLDOUT_END = date(2025, 1, 1), date(2025, 12, 1), date(2026, 2, 28)

_EVIDENCE_REL = Path("research/results/m7_mala_evidence_full/20260526T013740Z/pilot_runs")
_RUN_TS = "2026-05-22Tmala-evidence-m7"
_DEFAULT_PILOTS = [
    "elastic-band-current-basket-discovery__iwm_short",
    "elastic-band-current-basket-discovery__meta_short",
    "market-impulse-all-basket-discovery__amd_short",
    "jerk-pivot-current-basket-discovery__tsla_short",
    "opening-drive-current-basket-discovery__nvda_short",
    "compression-breakout-current-basket-discovery__amd_short",
]

OUTPUT_REL = Path("research/results/exit_profile_classify_propose")


def _main_checkout_root() -> Path:
    """The real (non-worktree) repo root.

    When running from a git worktree (``.../mala_v2/.claude/worktrees/<id>``)
    the tracked bar cache and evidence pilots live beside the REAL checkout, not
    the worktree. Walk up to the first ancestor that has a populated ``data``
    dir, falling back to the worktree root.
    """
    for ancestor in [REPO_ROOT, *REPO_ROOT.parents]:
        data_dir = ancestor / "data"
        if data_dir.is_dir() and any(data_dir.iterdir()):
            return ancestor
    return REPO_ROOT


def resolve_data_root(explicit: Path | None) -> Path:
    if explicit:
        return explicit
    worktree_data = REPO_ROOT / "data"
    if worktree_data.is_dir() and any(worktree_data.iterdir()):
        return worktree_data
    return _main_checkout_root() / "data"


def resolve_evidence_root(explicit: Path | None) -> Path:
    if explicit:
        return explicit
    worktree_ev = REPO_ROOT / _EVIDENCE_REL
    if worktree_ev.is_dir():
        return worktree_ev
    return _main_checkout_root() / _EVIDENCE_REL


def _safe_num(value) -> float | None:
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    return f if f == f and abs(f) != float("inf") else None


def _legacy_option_expectancy(
    *, raw_name: str, symbol: str, direction: str, strat, frame: pl.DataFrame
) -> dict:
    """Best LEGACY (non-profile) exit's option-adjusted expectancy on the holdout."""
    result = optimize_underlying_exit(
        strategy_key=raw_name, symbol=symbol, direction=direction, strategy=strat,
        enriched_frame=frame, holdout_start=HOLDOUT_START, holdout_end=HOLDOUT_END,
    )
    if result is None:
        return {"legacy_policy": None, "legacy_option_expectancy_pct": None, "note": "no signals in holdout"}
    legacy = [
        e for e in result.candidate_policies if not e.policy_name.startswith("profile:")
    ]
    if not legacy:
        return {"legacy_policy": None, "legacy_option_expectancy_pct": None, "note": "no legacy candidates"}
    best = max(legacy, key=lambda e: _safe_num(e.metrics.get("option_adjusted_expectancy_pct")) or float("-inf"))
    return {
        "legacy_policy": best.policy_name,
        "legacy_option_expectancy_pct": _safe_num(best.metrics.get("option_adjusted_expectancy_pct")),
        "legacy_underlying_expectancy_r": _safe_num(best.metrics.get("expectancy")),
        "legacy_trade_count": best.metrics.get("trade_count"),
        "note": "",
    }


def process_row(run_dir: Path, *, storage: LocalStorage) -> dict:
    """Classify -> explore -> propose for one promoted candidate run dir."""
    cat = run_dir / "CATALOG_SELECTED.csv"
    if not cat.exists():
        return {"run_dir": run_dir.name, "error": "no CATALOG_SELECTED.csv"}
    row = pl.read_csv(cat, infer_schema_length=10000).row(0, named=True)
    raw_name = str(row["strategy"])
    family_name = _strategy_family_name(raw_name)
    symbol, direction = str(row["ticker"]), str(row["direction"])
    catalog_key = str(row.get("catalog_key") or f"{run_dir.parent.name}__{symbol.lower()}_{direction}")
    strategy_key = to_strategy_key(family_name)

    # ── 1. CLASSIFY ──────────────────────────────────────────────────────────
    classification = classify_strategy(family_name, strategy_key=strategy_key)
    out: dict = {
        "catalog_key": catalog_key,
        "run_dir": str(run_dir),
        "strategy_name": raw_name,
        "strategy_family": family_name,
        "strategy_key": strategy_key,
        "symbol": symbol,
        "direction": direction,
        "classification": classification.to_dict(),
    }

    # A low-confidence / unclassified row is flagged for operator review and is
    # NOT force-assigned a profile -> no explore/propose.
    if classification.profile is None or classification.needs_operator_review:
        out["status"] = "flagged_for_operator_review"
        out["note"] = (
            "low-confidence or unclassified -> no exit proposed; operator must "
            "assign a playbook before this row gets a management_policy_spec."
        )
        return out

    profile = classification.profile

    # Rebuild the strategy + enrich bars for the explore step.
    try:
        strat = build_strategy(family_name, _config_for(row, family_name))
    except Exception as exc:  # pragma: no cover - defensive
        out["status"] = "explore_error"
        out["error"] = f"build_strategy failed: {exc}"
        return out
    raw = storage.load_bars(symbol, START, HOLDOUT_END)
    if raw.is_empty():
        out["status"] = "explore_error"
        out["error"] = f"no cached bars for {symbol} (data root)"
        return out
    frame = PhysicsEngine().enrich_for_features(raw, required_feature_union([strat]))
    filt = frame.clone().pipe(strat.generate_signals)

    # ── 2. EXPLORE ─────────────────────────────────────────────────────────────
    from src.research.exit_optimizer import _holdout_signal_frame, _with_exit_policy_features

    holdout = _holdout_signal_frame(
        _with_exit_policy_features(filt), direction, HOLDOUT_START, HOLDOUT_END
    )
    band = {
        b["scenario"]: b
        for b in score_profile_band(holdout, direction, profile, symbol=symbol, use_real_iv=True)
    }
    leverage = band.get("leverage", {})
    profile_exp = _safe_num(leverage.get("expectancy_pct"))
    legacy = _legacy_option_expectancy(
        raw_name=raw_name, symbol=symbol, direction=direction, strat=strat, frame=frame
    )

    explore = {
        "holdout_start": HOLDOUT_START.isoformat(),
        "holdout_end": HOLDOUT_END.isoformat(),
        "profile": profile,
        "use_real_iv": True,
        "profile_option_path": {
            "expectancy_pct": profile_exp,
            "win_rate": _safe_num(leverage.get("win_rate")),
            "avg_win_pct": _safe_num(leverage.get("avg_win_pct")),
            "avg_loss_pct": _safe_num(leverage.get("avg_loss_pct")),
            "n": leverage.get("n"),
            "band": {
                s: {
                    "scenario": s,
                    "expectancy_pct": _safe_num(band[s].get("expectancy_pct")),
                    "iv_premium_factor": _safe_num(band[s].get("iv_premium_factor")),
                    "n": band[s].get("n"),
                }
                for s in band
            },
            "band_lo": min((_safe_num(band[s].get("expectancy_pct")) or 0.0 for s in band), default=None),
            "band_hi": max((_safe_num(band[s].get("expectancy_pct")) or 0.0 for s in band), default=None),
        },
        "legacy_exit": legacy,
    }
    out["explore"] = explore

    if profile_exp is None and leverage.get("n", 0) == 0:
        out["status"] = "explore_no_signals"
        out["note"] = "no matching signals in holdout for the option-path band"
        # Still propose the profile spec (entry-condition issue, not an exit issue).

    # ── 3. PROPOSE ─────────────────────────────────────────────────────────────
    selection = select_exit(
        profile_name=profile,
        profile_expectancy_pct=profile_exp,
        legacy_policy=legacy.get("legacy_policy"),
        legacy_expectancy_pct=legacy.get("legacy_option_expectancy_pct"),
    )
    spec_json = propose_management_policy_spec(
        selection,
        source_config_id=str(row.get("config_id") or "") or None,
        parameters={
            "catalog_key": catalog_key,
            "symbol": symbol,
            "direction": direction,
            "classified_profile": profile,
            "classification_confidence": classification.confidence,
            "classification_source": classification.source,
        },
    )
    out["proposal"] = {
        "selection": selection.to_dict(),
        "management_policy_spec": spec_json,
    }
    out.setdefault("status", "proposed")
    return out


def _artifact_name(rec: dict) -> str:
    key = rec.get("catalog_key") or Path(rec.get("run_dir", "row")).name
    safe = "".join(c if c.isalnum() or c in "-_." else "_" for c in str(key))
    return f"{safe}.json"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", action="append", default=None, type=Path)
    parser.add_argument("--data-root", type=Path, default=None, help="bar cache root (default: auto)")
    parser.add_argument("--evidence-root", type=Path, default=None, help="m7 pilot_runs root (default: auto)")
    parser.add_argument("--out-dir", type=Path, default=None, help="artifact dir (default: stamped under research/results)")
    args = parser.parse_args()

    data_root = resolve_data_root(args.data_root)
    evidence_root = resolve_evidence_root(args.evidence_root)
    storage = LocalStorage(base_dir=data_root)

    if args.run_dir:
        run_dirs = [Path(r) for r in args.run_dir]
    else:
        run_dirs = [evidence_root / pilot / _RUN_TS for pilot in _DEFAULT_PILOTS]

    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    # Artifacts land under THIS checkout's research/results (gitignored). Keeping
    # them in the worktree keeps the task self-contained; --out-dir overrides.
    out_dir = args.out_dir or (REPO_ROOT / OUTPUT_REL / stamp)
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"data_root      = {data_root}")
    print(f"evidence_root  = {evidence_root}")
    print(f"out_dir        = {out_dir}\n")

    records: list[dict] = []
    for run_dir in run_dirs:
        print(f"... {Path(run_dir).name}", flush=True)
        rec = process_row(Path(run_dir), storage=storage)
        records.append(rec)
        (out_dir / _artifact_name(rec)).write_text(json.dumps(rec, indent=2, default=str), encoding="utf-8")

    index = {
        "generated_at": datetime.now(UTC).isoformat(),
        "holdout": {"start": HOLDOUT_START.isoformat(), "end": HOLDOUT_END.isoformat()},
        "data_root": str(data_root),
        "evidence_root": str(evidence_root),
        "rows": [
            {
                "catalog_key": r.get("catalog_key"),
                "strategy_key": r.get("strategy_key"),
                "symbol": r.get("symbol"),
                "direction": r.get("direction"),
                "status": r.get("status", "error" if r.get("error") else "?"),
                "classified_profile": (r.get("classification") or {}).get("profile"),
                "confidence": (r.get("classification") or {}).get("confidence"),
                "chosen_exit": (r.get("proposal") or {}).get("selection", {}).get("chosen"),
                "profile_option_exp_pct": (r.get("explore") or {}).get("profile_option_path", {}).get("expectancy_pct"),
                "legacy_option_exp_pct": (r.get("explore") or {}).get("legacy_exit", {}).get("legacy_option_expectancy_pct"),
                "artifact": _artifact_name(r),
                "error": r.get("error"),
            }
            for r in records
        ],
    }
    index_path = out_dir / "INDEX.json"
    index_path.write_text(json.dumps(index, indent=2, default=str), encoding="utf-8")

    # Console summary.
    print(f"\n{'catalog_key':>52s} {'profile':>20s} {'conf':>7s} {'chosen':>8s} "
          f"{'prof%':>7s} {'leg%':>7s} {'status':>22s}")
    for r in index["rows"]:
        print(f"{str(r['catalog_key'])[:52]:>52s} {str(r['classified_profile']):>20s} "
              f"{str(r['confidence']):>7s} {str(r['chosen_exit']):>8s} "
              f"{_fmt(r['profile_option_exp_pct']):>7s} {_fmt(r['legacy_option_exp_pct']):>7s} "
              f"{str(r['status'])[:22]:>22s}")
    print(f"\nINDEX: {index_path}")


def _fmt(v) -> str:
    return f"{v:+.2f}" if isinstance(v, (int, float)) else "-"


if __name__ == "__main__":
    main()
