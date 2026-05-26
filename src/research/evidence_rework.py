"""Build the Mala_Evidence_v1 rework manifest.

This command does not publish sheets or authorize runtime execution. It turns
the current regenerated Mala handoff plus optional live sheet reads into an
explicit queue for rerun, exit re-optimization, provider/parity refresh, and
active-strategy cleanup.
"""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from datetime import UTC, datetime
import json
from pathlib import Path
from typing import Any

from src.config import settings
from src.research.google_sheets import GoogleSheetTableClient


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_HANDOFF_CSV = REPO_ROOT / "research" / "results" / "mala_handoff_rework_preview_20260517" / "MALA_HANDOFF_CANDIDATES.csv"
DEFAULT_OUT_DIR = REPO_ROOT / "research" / "results" / "mala_evidence_rework"


@dataclass(frozen=True, slots=True)
class ManifestInputs:
    local_rows: list[dict[str, Any]]
    live_rows: list[dict[str, Any]]
    active_rows: list[dict[str, Any]]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--handoff-csv", default=str(DEFAULT_HANDOFF_CSV))
    parser.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR))
    parser.add_argument("--sheet-id", default="", help="Spreadsheet id/URL for live sheet inventory.")
    parser.add_argument("--google-credentials", default="", help="Service-account credentials for live sheet inventory.")
    parser.add_argument("--offline", action="store_true", help="Use only the local handoff CSV; do not read Google Sheets.")
    parser.add_argument("--evidence-sheet-name", default="Mala_Evidence_v1")
    parser.add_argument("--active-strategy-sheet-name", default="active_strategy")
    args = parser.parse_args(argv)

    inputs = _load_inputs(args)
    rows = build_manifest(inputs)
    out_dir = Path(args.out_dir) / datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / "mala_evidence_rework_manifest.csv"
    md_path = out_dir / "MALA_EVIDENCE_REWORK.md"
    _write_csv(csv_path, rows)
    md_path.write_text(render_manifest_markdown(rows), encoding="utf-8")
    print(f"REWORK_ROWS={len(rows)}")
    print(f"CSV={csv_path}")
    print(f"MARKDOWN={md_path}")
    return 0


def _load_inputs(args: argparse.Namespace) -> ManifestInputs:
    local_rows = _read_csv(Path(args.handoff_csv))
    sheet_id = "" if args.offline else args.sheet_id or str(settings.strategy_catalog_sheet_id or "")
    credentials = "" if args.offline else args.google_credentials or str(settings.google_api_credentials_path or "")
    live_rows: list[dict[str, Any]] = []
    active_rows: list[dict[str, Any]] = []
    if sheet_id and credentials:
        evidence_client = GoogleSheetTableClient(
            spreadsheet_id=sheet_id,
            sheet_name=args.evidence_sheet_name,
            credentials_path=Path(credentials),
        )
        active_client = GoogleSheetTableClient(
            spreadsheet_id=sheet_id,
            sheet_name=args.active_strategy_sheet_name,
            credentials_path=Path(credentials),
        )
        evidence_client.require_sheet_exists()
        active_client.require_sheet_exists()
        live_rows = evidence_client.read_rows(range_suffix="A1:AI1000")
        active_rows = active_client.read_rows(range_suffix="A1:Z1000")
    return ManifestInputs(local_rows=local_rows, live_rows=live_rows, active_rows=active_rows)


def build_manifest(inputs: ManifestInputs) -> list[dict[str, Any]]:
    local_by_key = {_clean(row.get("catalog_key")): row for row in inputs.local_rows if _clean(row.get("catalog_key"))}
    live_by_key = {_clean(row.get("catalog_key")): row for row in inputs.live_rows if _clean(row.get("catalog_key"))}
    active_by_key = {_clean(row.get("strategy_id")): row for row in inputs.active_rows if _clean(row.get("strategy_id"))}
    keys = sorted(set(local_by_key) | set(live_by_key) | set(active_by_key))
    return [
        _manifest_row(
            key=key,
            local=local_by_key.get(key, {}),
            live=live_by_key.get(key, {}),
            active=active_by_key.get(key, {}),
        )
        for key in keys
    ]


def _manifest_row(
    *,
    key: str,
    local: dict[str, Any],
    live: dict[str, Any],
    active: dict[str, Any],
) -> dict[str, Any]:
    row = local or live or {}
    reasons: list[str] = ["rerun_current_transforms"]
    enabled = _truthy(active.get("enabled"))
    if active and not local:
        reasons.append("active_strategy_missing_from_regenerated_handoff")
    if live and not local:
        reasons.append("live_evidence_missing_from_regenerated_handoff")
    if local and not live:
        reasons.append("regenerated_candidate_missing_from_live_sheet")
    if _clean(row.get("recommendation_tier")) == "watch_only":
        reasons.append("watch_only_retest_before_activation")
    if _clean(row.get("bhiksha_capability_status")) != "supported":
        reasons.append("runtime_adapter_or_exclude")
    provider_risk = _clean(row.get("provider_feature_risk")).lower()
    provider_status = _clean(row.get("provider_validation_status")).lower()
    if provider_risk in {"red", "yellow"} or provider_status in {"provider_blocked", "provider_watch", "provider_unknown", ""}:
        reasons.append("provider_parity_refresh")
    exit_flags = _exit_flags(row)
    reasons.extend(exit_flags)
    whipsaw_flags = _whipsaw_flags(row)
    reasons.extend(whipsaw_flags)
    priority = _priority(enabled=enabled, local=local, live=live, reasons=reasons)
    return {
        "priority": priority,
        "catalog_key": key,
        "active_enabled": "TRUE" if enabled else "FALSE",
        "live_sheet_present": "TRUE" if live else "FALSE",
        "regenerated_present": "TRUE" if local else "FALSE",
        "hypothesis_id": _clean(row.get("hypothesis_id")),
        "symbol": _clean(row.get("symbol")),
        "direction": _clean(row.get("direction")),
        "strategy_key": _clean(row.get("strategy_key")),
        "live_tier": _clean(live.get("recommendation_tier")),
        "regenerated_tier": _clean(local.get("recommendation_tier")),
        "bhiksha_capability_status": _clean(row.get("bhiksha_capability_status")),
        "provider_validation_status": _clean(row.get("provider_validation_status")),
        "provider_feature_risk": _clean(row.get("provider_feature_risk")),
        "thesis_exit_policy": _clean(row.get("thesis_exit_policy")),
        "exit_trade_count": _clean(row.get("exit_trade_count")),
        "signal_count": _clean(row.get("signal_count")),
        "execution_robustness": _clean(row.get("execution_robustness")),
        "rework_reasons": "; ".join(dict.fromkeys(reasons)),
        "exit_rework_flags": "; ".join(exit_flags),
        "whipsaw_flags": "; ".join(whipsaw_flags),
        "run_dir": _clean(row.get("run_dir")),
    }


def _priority(*, enabled: bool, local: dict[str, Any], live: dict[str, Any], reasons: list[str]) -> str:
    if enabled:
        return "P0_active_rework"
    if "runtime_adapter_or_exclude" in reasons or "provider_parity_refresh" in reasons:
        return "P1_contract_rework"
    if local and not live:
        return "P2_publish_review"
    if live and not local:
        return "P2_stale_live_cleanup"
    return "P3_watch"


def _exit_flags(row: dict[str, Any]) -> list[str]:
    policy = _clean(row.get("thesis_exit_policy"))
    params = _jsonish(row.get("thesis_exit_params_json"))
    flags: list[str] = ["short_option_exit_reopt"]
    if policy == "hold_to_eod_underlying":
        flags.append("hold_to_eod_not_short_option_native")
    if policy == "time_stop_underlying" and str(params.get("exit_time_et", "")) >= "14:30":
        flags.append("late_time_stop_for_short_option")
    try:
        if int(float(_clean(row.get("exit_trade_count")) or 0)) < 40:
            flags.append("thin_exit_sample")
    except ValueError:
        flags.append("missing_exit_sample")
    return flags


def _whipsaw_flags(row: dict[str, Any]) -> list[str]:
    params = _jsonish(row.get("strategy_params_json"))
    flags: list[str] = ["whipsaw_config_review"]
    strategy = _clean(row.get("strategy_key"))
    if strategy == "market_impulse":
        if int(params.get("entry_buffer_minutes") or 0) <= 3:
            flags.append("early_entry_buffer_whipsaw_risk")
        if int(params.get("entry_window_minutes") or 0) >= 90:
            flags.append("wide_entry_window_repeat_fire_risk")
    if strategy == "opening_drive_classifier":
        if float(params.get("breakout_buffer_pct") or 0) == 0:
            flags.append("zero_breakout_buffer_whipsaw_risk")
    if str(params.get("use_volume_filter", "")).lower() in {"true", "1", "yes"}:
        flags.append("volume_filter_provider_whipsaw_risk")
    return flags


def render_manifest_markdown(rows: list[dict[str, Any]]) -> str:
    counts: dict[str, int] = {}
    for row in rows:
        counts[row["priority"]] = counts.get(row["priority"], 0) + 1
    lines = [
        "# Mala Evidence Rework Manifest",
        "",
        f"- generated_at: `{datetime.now(UTC).isoformat()}`",
        f"- rows: `{len(rows)}`",
        f"- priority_counts: `{json.dumps(counts, sort_keys=True)}`",
        "",
        "Every row is treated as needing current-transform rerun plus short-option exit re-optimization before it is trusted again.",
        "",
        "| priority | active | key | symbol | strategy | live -> regenerated | reasons |",
        "|---|---:|---|---|---|---|---|",
    ]
    for row in rows:
        lines.append(
            "| "
            + " | ".join(
                [
                    row["priority"],
                    row["active_enabled"],
                    row["catalog_key"],
                    f"{row['symbol']} {row['direction']}",
                    row["strategy_key"],
                    f"{row['live_tier']} -> {row['regenerated_tier']}",
                    row["rework_reasons"],
                ]
            )
            + " |"
        )
    lines.append("")
    lines.extend(
        [
            "## Work Order",
            "",
            "1. Rerun P0 active rows under current transforms.",
            "2. Re-optimize all exits with the short-option grid.",
            "3. Refresh M7 provider translation and same-bars Mala/Bhiksha parity.",
            "4. Publish regenerated `Mala_Evidence_v1` only from artifacts.",
            "5. Rebuild `active_strategy` from the fresh shadow packet; disable anything not re-earned.",
            "",
        ]
    )
    return "\n".join(lines)


def _read_csv(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    headers = list(rows[0]) if rows else ["catalog_key"]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _truthy(value: Any) -> bool:
    return _clean(value).lower() in {"true", "1", "yes", "y", "enabled", "shadow", "live"}


def _jsonish(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    text = _clean(value)
    if not text:
        return {}
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


if __name__ == "__main__":
    raise SystemExit(main())
