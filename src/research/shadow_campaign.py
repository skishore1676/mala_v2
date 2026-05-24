"""Shadow-campaign planning and daily review helpers.

This module is deliberately small and deterministic.  It does not authorize
live trading; it turns Mala evidence plus Bhiksha feedback bundles into
reviewable operator artifacts.
"""

from __future__ import annotations

import csv
import json
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.research.google_sheets import GoogleSheetTableClient
from src.research.time_utils import sheet_timestamp


DEFAULT_EVIDENCE_SHEET_NAME = "Mala_Evidence_v1"
DEFAULT_ACTIVE_STRATEGY_SHEET_NAME = "active_strategy"
DEFAULT_OPERATOR_DEFAULTS_SHEET_NAME = "Operator_Defaults_v1"

ACTIVE_STRATEGY_HEADERS = [
    "enabled",
    "authorization_mode",
    "strategy_id",
    "entry_window_start_et",
    "max_trade_premium_usd",
    "execution_overrides",
    "notes",
]

OPERATOR_DEFAULT_PATCH = {
    "max_trade_premium_usd": "2000",
    "min_open_interest": "100",
    "max_bid_ask_spread_pct": "0.08",
    "dte_min": "7",
    "dte_max": "21",
    "delta_min": "0.15",
    "delta_max": "0.35",
}


@dataclass(slots=True, frozen=True)
class ShadowActivationConfig:
    min_execution_robustness: float = 0.75
    experiment_min_execution_robustness: float = 0.65
    min_signal_count: int = 20
    min_expectancy: float = 0.0
    max_trade_premium_usd: float = 2000.0
    dte_min: int = 7
    dte_max: int = 21
    delta_min: float = 0.15
    delta_max: float = 0.35
    max_bid_ask_spread_pct: float = 0.08
    min_open_interest: int = 100


@dataclass(slots=True, frozen=True)
class ShadowActivationArtifacts:
    out_dir: Path
    packet_md: Path
    packet_csv: Path
    active_strategy_csv: Path
    defaults_patch_csv: Path
    recommended_rows: list[dict[str, Any]]
    active_strategy_rows: list[dict[str, Any]]
    defaults_patch_rows: list[dict[str, Any]]


@dataclass(slots=True, frozen=True)
class ShadowDailyReportArtifacts:
    report_md: Path
    scorecard_csv: Path
    bundle_count: int
    observation_count: int
    issue_count: int


def _require_exact_sheet_name(actual: str, expected: str, *, surface: str) -> None:
    if actual != expected:
        raise RuntimeError(f"Refusing {surface} access to {actual!r}; canonical tab is {expected!r}.")


def read_sheet_rows(
    *,
    spreadsheet_id: str,
    credentials_path: str | Path,
    evidence_sheet_name: str = DEFAULT_EVIDENCE_SHEET_NAME,
    active_strategy_sheet_name: str = DEFAULT_ACTIVE_STRATEGY_SHEET_NAME,
    operator_defaults_sheet_name: str = DEFAULT_OPERATOR_DEFAULTS_SHEET_NAME,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    _require_exact_sheet_name(evidence_sheet_name, DEFAULT_EVIDENCE_SHEET_NAME, surface="Mala evidence")
    _require_exact_sheet_name(active_strategy_sheet_name, DEFAULT_ACTIVE_STRATEGY_SHEET_NAME, surface="active_strategy")
    _require_exact_sheet_name(operator_defaults_sheet_name, DEFAULT_OPERATOR_DEFAULTS_SHEET_NAME, surface="operator defaults")
    evidence_client = GoogleSheetTableClient(
        spreadsheet_id=spreadsheet_id,
        sheet_name=evidence_sheet_name,
        credentials_path=Path(credentials_path),
    )
    active_client = GoogleSheetTableClient(
        spreadsheet_id=spreadsheet_id,
        sheet_name=active_strategy_sheet_name,
        credentials_path=Path(credentials_path),
    )
    defaults_client = GoogleSheetTableClient(
        spreadsheet_id=spreadsheet_id,
        sheet_name=operator_defaults_sheet_name,
        credentials_path=Path(credentials_path),
    )
    for client in (evidence_client, active_client, defaults_client):
        client.require_sheet_exists()
    evidence = evidence_client.read_rows(range_suffix="A1:ZZ5000")
    active = active_client.read_rows(range_suffix="A1:ZZ5000")
    defaults = defaults_client.read_rows(range_suffix="A1:ZZ5000")
    return evidence, active, defaults


def build_shadow_activation_packet(
    *,
    evidence_rows: list[dict[str, Any]],
    active_strategy_rows: list[dict[str, Any]],
    out_dir: str | Path,
    config: ShadowActivationConfig | None = None,
    include_experiments: bool = False,
) -> ShadowActivationArtifacts:
    cfg = config or ShadowActivationConfig()
    target_dir = Path(out_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    active_ids = {
        _clean(row.get("strategy_id")): row
        for row in active_strategy_rows
        if _clean(row.get("strategy_id"))
    }

    rows: list[dict[str, Any]] = []
    for evidence in evidence_rows:
        decision, reasons = classify_shadow_activation(
            evidence,
            config=cfg,
            include_experiments=include_experiments,
        )
        catalog_key = _clean(evidence.get("catalog_key"))
        active_row = active_ids.get(catalog_key)
        rows.append(
            {
                "decision": decision,
                "reason_codes": ",".join(reasons),
                "already_active": "TRUE" if active_row and _truthy(active_row.get("enabled")) else "FALSE",
                "active_authorization_mode": _clean(active_row.get("authorization_mode")) if active_row else "",
                "catalog_key": catalog_key,
                "symbol": _clean(evidence.get("symbol")),
                "direction": _clean(evidence.get("direction")),
                "strategy_key": _clean(evidence.get("strategy_key")),
                "strategy_name": _clean(evidence.get("strategy_name")),
                "recommendation_tier": _clean(evidence.get("recommendation_tier")),
                "bhiksha_ready": _clean(evidence.get("bhiksha_ready")),
                "capability_status": _clean(evidence.get("bhiksha_capability_status")),
                "expectancy": _clean(evidence.get("expectancy")),
                "confidence": _clean(evidence.get("confidence")),
                "signal_count": _clean(evidence.get("signal_count")),
                "execution_robustness": _clean(evidence.get("execution_robustness")),
                "thesis_exit_policy": _clean(evidence.get("thesis_exit_policy")),
                "exit_reliability": _clean(evidence.get("exit_reliability")),
                "exit_trade_count": _clean(evidence.get("exit_trade_count")),
                "signal_window_et": _clean(evidence.get("signal_window_et")),
                "option_trade_ready": _clean(evidence.get("option_trade_ready")),
                "option_adjusted_expectancy_pct": _clean(evidence.get("option_adjusted_expectancy_pct")),
                "option_exit_quality": _clean(evidence.get("option_exit_quality")),
                "median_minutes_held": _clean(evidence.get("median_minutes_held")),
                "pnl_pct_per_minute": _clean(evidence.get("pnl_pct_per_minute")),
                "target_hit_within_30_minutes": _clean(evidence.get("target_hit_within_30_minutes")),
                "option_dte_min": _option_int(evidence.get("recommended_dte_min"), cfg.dte_min),
                "option_dte_max": _option_int(evidence.get("recommended_dte_max"), cfg.dte_max),
                "option_delta_min": cfg.delta_min,
                "option_delta_max": cfg.delta_max,
                "max_bid_ask_spread_pct": cfg.max_bid_ask_spread_pct,
                "min_open_interest": cfg.min_open_interest,
                "max_trade_premium_usd": cfg.max_trade_premium_usd,
            }
        )

    rows = sorted(rows, key=_activation_sort_key)
    recommended = [row for row in rows if row["decision"] == "shadow"]
    active_rows = [active_strategy_row_from_recommendation(row) for row in recommended]
    defaults_patch = operator_defaults_patch_rows(cfg)

    packet_csv = target_dir / "shadow_activation_packet.csv"
    active_csv = target_dir / "active_strategy_shadow_rows.csv"
    defaults_csv = target_dir / "operator_defaults_patch.csv"
    packet_md = target_dir / "SHADOW_ACTIVATION_PACKET.md"
    _write_csv(packet_csv, rows)
    _write_csv(active_csv, active_rows)
    _write_csv(defaults_csv, defaults_patch)
    packet_md.write_text(_activation_markdown(rows, active_rows, defaults_patch, cfg), encoding="utf-8")
    return ShadowActivationArtifacts(
        out_dir=target_dir,
        packet_md=packet_md,
        packet_csv=packet_csv,
        active_strategy_csv=active_csv,
        defaults_patch_csv=defaults_csv,
        recommended_rows=recommended,
        active_strategy_rows=active_rows,
        defaults_patch_rows=defaults_patch,
    )


def classify_shadow_activation(
    row: dict[str, Any],
    *,
    config: ShadowActivationConfig,
    include_experiments: bool = False,
) -> tuple[str, list[str]]:
    reasons: list[str] = []
    if not _truthy(row.get("bhiksha_ready")):
        reasons.append("bhiksha_not_ready")
    if _clean(row.get("bhiksha_capability_status")).lower() != "supported":
        reasons.append("runtime_unsupported")
    tier = _clean(row.get("recommendation_tier")).lower()
    if tier not in {"shadow", "promote"}:
        reasons.append(f"tier_{tier or 'missing'}")
    if _float(row.get("expectancy")) <= config.min_expectancy:
        reasons.append("non_positive_expectancy")
    if "option_trade_ready" in row and _clean(row.get("option_trade_ready")):
        if not _truthy(row.get("option_trade_ready")):
            reasons.append("option_trade_not_ready")
    if "option_adjusted_expectancy_pct" in row and _clean(row.get("option_adjusted_expectancy_pct")):
        if _float(row.get("option_adjusted_expectancy_pct")) <= 0:
            reasons.append("non_positive_option_adjusted_expectancy")
    if "recommended_dte_max" in row and _clean(row.get("recommended_dte_max")):
        if _int(row.get("recommended_dte_max")) > 14:
            reasons.append("option_dte_outside_short_packet")
    robustness = _float(row.get("execution_robustness"))
    if robustness < config.min_execution_robustness:
        if include_experiments and robustness >= config.experiment_min_execution_robustness:
            reasons.append("experiment_low_robustness")
        else:
            reasons.append("execution_robustness_below_floor")
    if _int(row.get("signal_count")) < config.min_signal_count:
        reasons.append("thin_signal_count")

    hard_blocks = {
        "bhiksha_not_ready",
        "runtime_unsupported",
        "non_positive_expectancy",
        "option_trade_not_ready",
        "non_positive_option_adjusted_expectancy",
        "option_dte_outside_short_packet",
    }
    if any(reason in hard_blocks for reason in reasons):
        return "blocked", reasons
    if any(reason.startswith("tier_") for reason in reasons):
        return "observe_only", reasons
    soft_blocks = {"execution_robustness_below_floor", "thin_signal_count"}
    if any(reason in soft_blocks for reason in reasons):
        return "observe_only", reasons
    return "shadow", reasons or ["eligible"]


def active_strategy_row_from_recommendation(row: dict[str, Any]) -> dict[str, Any]:
    execution_overrides = {
        "dte_min": _int(row.get("option_dte_min")),
        "dte_max": _int(row.get("option_dte_max")),
        "target_abs_delta_min": _float(row.get("option_delta_min")),
        "target_abs_delta_max": _float(row.get("option_delta_max")),
        "max_bid_ask_spread_pct": _float(row.get("max_bid_ask_spread_pct")),
        "min_open_interest": _int(row.get("min_open_interest")),
    }
    notes = (
        "Shadow campaign candidate; "
        f"tier={row['recommendation_tier']} mc={row['execution_robustness']} "
        f"exp={row['expectancy']} signals={row['signal_count']} "
        f"exit={row['thesis_exit_policy']} option_exp={row.get('option_adjusted_expectancy_pct', '')} "
        f"dte={row.get('option_dte_min', '')}-{row.get('option_dte_max', '')} "
        f"median_min={row.get('median_minutes_held', '')} exit_trades={row['exit_trade_count']}."
    )
    return {
        "enabled": "TRUE",
        "authorization_mode": "shadow",
        "strategy_id": row["catalog_key"],
        "entry_window_start_et": _entry_start(row.get("signal_window_et")),
        "max_trade_premium_usd": str(_format_number(row["max_trade_premium_usd"])),
        "execution_overrides": json.dumps(execution_overrides, sort_keys=True, separators=(",", ":")),
        "notes": notes,
    }


def operator_defaults_patch_rows(config: ShadowActivationConfig) -> list[dict[str, Any]]:
    values = dict(OPERATOR_DEFAULT_PATCH)
    values.update(
        {
            "max_trade_premium_usd": str(_format_number(config.max_trade_premium_usd)),
            "min_open_interest": str(config.min_open_interest),
            "max_bid_ask_spread_pct": str(config.max_bid_ask_spread_pct),
            "dte_min": str(config.dte_min),
            "dte_max": str(config.dte_max),
            "delta_min": str(config.delta_min),
            "delta_max": str(config.delta_max),
        }
    )
    return [
        {
            "section": "default",
            "key": key,
            "recommended_value": value,
            "reason": "shadow_campaign_single_leg_options_constraints",
        }
        for key, value in values.items()
    ]


def merge_active_strategy_rows(
    *,
    existing_rows: list[dict[str, Any]],
    recommended_rows: list[dict[str, Any]],
    disable_non_recommended: bool = False,
) -> list[dict[str, Any]]:
    recommended_ids = {_clean(row.get("strategy_id")) for row in recommended_rows if _clean(row.get("strategy_id"))}
    by_id: dict[str, dict[str, Any]] = {}
    ordered_ids: list[str] = []
    for row in existing_rows:
        strategy_id = _clean(row.get("strategy_id"))
        if not strategy_id:
            continue
        if strategy_id not in by_id:
            ordered_ids.append(strategy_id)
        clean = {key: _clean(row.get(key)) for key in ACTIVE_STRATEGY_HEADERS}
        if disable_non_recommended and strategy_id not in recommended_ids and _truthy(clean.get("enabled")):
            clean["enabled"] = "FALSE"
            clean["notes"] = "Disabled by shadow campaign activation: not in current shadow packet."
        by_id[strategy_id] = clean
    for row in recommended_rows:
        strategy_id = _clean(row.get("strategy_id"))
        if not strategy_id:
            continue
        if strategy_id not in by_id:
            ordered_ids.append(strategy_id)
        by_id[strategy_id] = {key: _clean(row.get(key)) for key in ACTIVE_STRATEGY_HEADERS}
    return [by_id[strategy_id] for strategy_id in ordered_ids]


def apply_active_strategy_rows(
    *,
    spreadsheet_id: str,
    credentials_path: str | Path,
    active_strategy_sheet_name: str,
    existing_rows: list[dict[str, Any]],
    recommended_rows: list[dict[str, Any]],
    disable_non_recommended: bool = False,
) -> list[dict[str, Any]]:
    merged = merge_active_strategy_rows(
        existing_rows=existing_rows,
        recommended_rows=recommended_rows,
        disable_non_recommended=disable_non_recommended,
    )
    _require_exact_sheet_name(active_strategy_sheet_name, DEFAULT_ACTIVE_STRATEGY_SHEET_NAME, surface="active_strategy")
    client = GoogleSheetTableClient(
        spreadsheet_id=spreadsheet_id,
        sheet_name=active_strategy_sheet_name,
        credentials_path=Path(credentials_path),
    )
    client.require_sheet_exists()
    client.overwrite_table(headers=ACTIVE_STRATEGY_HEADERS, rows=merged)
    return merged


def apply_operator_defaults_patch(
    *,
    spreadsheet_id: str,
    credentials_path: str | Path,
    operator_defaults_sheet_name: str,
    defaults_rows: list[dict[str, Any]],
    patch_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    by_key = {_clean(row.get("key")): dict(row) for row in defaults_rows if _clean(row.get("key"))}
    updates: list[dict[str, Any]] = []
    for patch in patch_rows:
        key = _clean(patch.get("key"))
        existing = by_key.get(key)
        if not existing:
            continue
        if _clean(existing.get("value")) == _clean(patch.get("recommended_value")):
            continue
        existing["value"] = _clean(patch.get("recommended_value"))
        existing["source"] = "shadow_campaign_activation"
        updates.append(existing)
    if updates:
        _require_exact_sheet_name(operator_defaults_sheet_name, DEFAULT_OPERATOR_DEFAULTS_SHEET_NAME, surface="operator defaults")
        client = GoogleSheetTableClient(
            spreadsheet_id=spreadsheet_id,
            sheet_name=operator_defaults_sheet_name,
            credentials_path=Path(credentials_path),
        )
        client.require_sheet_exists()
        client.batch_update_rows(rows=updates, columns=["value", "source"])
    return updates


def build_shadow_daily_report(
    *,
    feedback_root: str | Path,
    evidence_rows: list[dict[str, Any]] | None,
    out_dir: str | Path,
    active_plan_id: str | None = None,
) -> ShadowDailyReportArtifacts:
    feedback_dirs = _feedback_dirs(Path(feedback_root), active_plan_id=active_plan_id)
    evidence_by_key = _evidence_lookup(evidence_rows or [])
    score_rows: list[dict[str, Any]] = []
    issue_count = 0
    for bundle_dir in feedback_dirs:
        plan = _read_json(bundle_dir / "active_plan.json")
        session = _read_json(bundle_dir / "session_summary.json")
        observation_index = _read_json(bundle_dir / "observation_index.json")
        observations = observation_index.get("reports") if isinstance(observation_index, dict) else []
        if not isinstance(observations, list):
            observations = []
        active_plan_name = _clean(plan.get("active_plan_id") if isinstance(plan, dict) else "") or bundle_dir.name
        for packet in observations:
            if not isinstance(packet, dict):
                continue
            deployment_id = _clean(packet.get("deployment_id"))
            evidence_key = _packet_catalog_key(packet)
            evidence = evidence_by_key.get(evidence_key) or evidence_by_key.get(deployment_id, {})
            session_blocked = _dict_at(session, "blocked_entry_reasons_by_deployment", deployment_id)
            blocked = _blocked_reasons(packet.get("blocked_entry_reasons") or session_blocked)
            runtime_issues = _runtime_issues(
                packet=packet,
                session_issues=_dict_at(session, "runtime_issue_counts_by_deployment", deployment_id),
            )
            if blocked or runtime_issues or packet.get("ambiguous_cancel_count"):
                issue_count += 1
            signal_true_count = _session_count(session, "signal_true_counts", deployment_id, packet.get("signal_true_count"))
            exit_true_count = _session_count(session, "exit_true_counts", deployment_id, packet.get("exit_true_count"))
            trade_plan_count = _session_count(
                session,
                "trade_plan_counts",
                deployment_id,
                _trade_plan_count(packet.get("trade_plan_count"), session_blocked),
            )
            score_rows.append(
                {
                    "active_plan_id": active_plan_name,
                    "deployment_id": deployment_id,
                    "symbol": _clean(packet.get("symbol")),
                    "strategy_key": _clean(packet.get("strategy_key")),
                    "authorization_mode": _clean(packet.get("authorization_mode")),
                    "shadow_only": str(packet.get("shadow_only")),
                    "mala_tier": _clean(evidence.get("recommendation_tier")),
                    "mala_expectancy": _clean(evidence.get("expectancy")),
                    "mala_mc_prob": _clean(evidence.get("execution_robustness")),
                    "signal_decisions_total": packet.get("signal_decisions_total", 0),
                    "signal_true_count": signal_true_count,
                    "trade_plan_count": trade_plan_count,
                    "exit_true_count": exit_true_count,
                    "pending_exit_count": packet.get("pending_exit_count", 0),
                    "blocked_entry_reasons": _json_compact(blocked),
                    "runtime_issue_counts": _json_compact(runtime_issues),
                    "signal_reason_counts": _json_compact(packet.get("signal_reason_counts") or {}),
                    "exit_reason_counts": _json_compact(packet.get("exit_reason_counts") or {}),
                    "latest_lifecycle_state": _clean(packet.get("latest_lifecycle_state")),
                    "replay_status": _clean((packet.get("replay") or {}).get("status") if isinstance(packet.get("replay"), dict) else ""),
                    "replay_trade_count": (packet.get("replay") or {}).get("trade_count", "") if isinstance(packet.get("replay"), dict) else "",
                }
            )
        if not observations:
            issue_count += 1
            score_rows.append(
                {
                    "active_plan_id": active_plan_name,
                    "deployment_id": "",
                    "symbol": "",
                    "strategy_key": "",
                    "authorization_mode": "",
                    "shadow_only": "",
                    "mala_tier": "",
                    "mala_expectancy": "",
                    "mala_mc_prob": "",
                    "signal_decisions_total": 0,
                    "signal_true_count": 0,
                    "trade_plan_count": 0,
                    "exit_true_count": 0,
                    "pending_exit_count": 0,
                    "blocked_entry_reasons": "",
                    "runtime_issue_counts": "missing_observation_index",
                    "signal_reason_counts": "",
                    "exit_reason_counts": "",
                    "latest_lifecycle_state": "",
                    "replay_status": "",
                    "replay_trade_count": "",
                }
            )
        if isinstance(session, dict) and session.get("runtime_issue_counts"):
            issue_count += 1

    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    scorecard_csv = out / f"shadow_daily_scorecard_{stamp}.csv"
    report_md = out / f"shadow_daily_report_{stamp}.md"
    _write_csv(scorecard_csv, score_rows)
    report_md.write_text(_daily_markdown(score_rows, feedback_dirs, scorecard_csv), encoding="utf-8")
    return ShadowDailyReportArtifacts(
        report_md=report_md,
        scorecard_csv=scorecard_csv,
        bundle_count=len(feedback_dirs),
        observation_count=len(score_rows),
        issue_count=issue_count,
    )


def _activation_markdown(
    rows: list[dict[str, Any]],
    active_rows: list[dict[str, Any]],
    defaults_patch: list[dict[str, Any]],
    config: ShadowActivationConfig,
) -> str:
    counts = Counter(row["decision"] for row in rows)
    strategy_counts = Counter(row["strategy_key"] for row in rows if row["decision"] == "shadow")
    lines = [
        "# Shadow Activation Packet",
        "",
        f"- generated_at: `{datetime.now(UTC).replace(microsecond=0).isoformat()}`",
        f"- candidate_rows: `{len(rows)}`",
        f"- shadow: `{counts.get('shadow', 0)}`",
        f"- observe_only: `{counts.get('observe_only', 0)}`",
        f"- blocked: `{counts.get('blocked', 0)}`",
        "",
        "## Option Constraints",
        "",
        f"- DTE: `{config.dte_min}-{config.dte_max}`",
        f"- absolute delta: `{config.delta_min}-{config.delta_max}`",
        f"- max bid/ask spread: `{config.max_bid_ask_spread_pct}`",
        f"- min open interest: `{config.min_open_interest}`",
        f"- max premium per trade: `${_format_number(config.max_trade_premium_usd)}`",
        "",
        "## Shadow Rows",
        "",
    ]
    if not active_rows:
        lines.append("- No rows met the shadow gate.")
    else:
        lines.append("| Strategy ID | Symbol | Direction | Strategy | MC | Exp R | Signals | Exit | Already Active |")
        lines.append("|---|---|---|---|---:|---:|---:|---|---|")
        by_key = {row["catalog_key"]: row for row in rows}
        for active in active_rows:
            row = by_key[active["strategy_id"]]
            lines.append(
                "| {strategy_id} | {symbol} | {direction} | {strategy_key} | {mc} | {exp} | {signals} | {exit_policy} | {already} |".format(
                    strategy_id=active["strategy_id"],
                    symbol=row["symbol"],
                    direction=row["direction"],
                    strategy_key=row["strategy_key"],
                    mc=row["execution_robustness"],
                    exp=row["expectancy"],
                    signals=row["signal_count"],
                    exit_policy=row["thesis_exit_policy"],
                    already=row["already_active"],
                )
            )
    lines.extend(["", "## Strategy Mix", ""])
    if not strategy_counts:
        lines.append("- Empty.")
    for strategy, count in sorted(strategy_counts.items()):
        lines.append(f"- `{strategy}`: `{count}`")
    lines.extend(["", "## Operator Defaults Patch", ""])
    for row in defaults_patch:
        lines.append(f"- `{row['key']}` -> `{row['recommended_value']}`")
    lines.extend(["", "## Non-Shadow Rows", ""])
    for row in rows:
        if row["decision"] == "shadow":
            continue
        lines.append(f"- `{row['decision']}` `{row['catalog_key']}`: {row['reason_codes']}")
    return "\n".join(lines) + "\n"


def _daily_markdown(
    score_rows: list[dict[str, Any]],
    feedback_dirs: list[Path],
    scorecard_csv: Path,
) -> str:
    signal_count = sum(_int(row.get("signal_true_count")) for row in score_rows)
    trade_plan_count = sum(_int(row.get("trade_plan_count")) for row in score_rows)
    blocked_rows = [row for row in score_rows if _clean(row.get("blocked_entry_reasons")) not in {"", "{}"}]
    runtime_issue_rows = [row for row in score_rows if _clean(row.get("runtime_issue_counts")) not in {"", "{}"}]
    lines = [
        "# Shadow Campaign Daily Report",
        "",
        f"- generated_at: `{datetime.now(UTC).replace(microsecond=0).isoformat()}`",
        f"- feedback_bundles: `{len(feedback_dirs)}`",
        f"- observations: `{len(score_rows)}`",
        f"- signal_true_count: `{signal_count}`",
        f"- trade_plan_count: `{trade_plan_count}`",
        f"- blocked_rows: `{len(blocked_rows)}`",
        f"- runtime_issue_rows: `{len(runtime_issue_rows)}`",
        f"- scorecard_csv: `{scorecard_csv}`",
        "",
        "## Plain-English Read",
        "",
        "- This report checks whether the shadow session produced reviewable runtime evidence: active deployments, signals, trade plans, blocks, replay status, and runtime issues.",
        "- Use the Signal Expected Value report for strategy-quality questions like realized option PnL, signal concordance, and whether Bhiksha's fired signals agree with Mala.",
        "",
        "## Metric Glossary",
        "",
        "- `feedback_bundles`: Bhiksha feedback folders reviewed, usually one active-plan session/day.",
        "- `observations`: active deployments found in the feedback bundle; one observation roughly equals one active_strategy row.",
        "- `signal_true_count`: total strategy fires observed by Bhiksha across those deployments.",
        "- `trade_plan_count`: signals that reached option trade planning; these can still be blocked by budget, lifecycle state, spread, open interest, or other execution gates.",
        "- `blocked_rows`: deployments with non-approval block reasons, such as insufficient budget for a single contract or an already protected open position.",
        "- `runtime_issue_rows`: deployments with runtime or replay issues. A replay error means the review/replay lane failed; it does not automatically mean the live shadow engine failed.",
        "",
        "## Bundles",
    ]
    if not feedback_dirs:
        lines.append("- No feedback bundles found.")
    for path in feedback_dirs:
        lines.append(f"- `{path}`")
    lines.extend(["", "## Deployment Scorecard", ""])
    if not score_rows:
        lines.append("- No observations.")
    else:
        lines.append("| Deployment | Symbol | Strategy | Signals | Plans | Blocks | Runtime Issues | Replay |")
        lines.append("|---|---|---|---:|---:|---|---|---|")
        for row in score_rows:
            lines.append(
                "| {deployment} | {symbol} | {strategy} | {signals} | {plans} | {blocks} | {issues} | {replay} |".format(
                    deployment=row["deployment_id"],
                    symbol=row["symbol"],
                    strategy=row["strategy_key"],
                    signals=row["signal_true_count"],
                    plans=row["trade_plan_count"],
                    blocks=row["blocked_entry_reasons"] or "",
                    issues=row["runtime_issue_counts"] or "",
                    replay=row["replay_status"],
                )
            )
    lines.extend(["", "## Triage Questions", ""])
    lines.append("- Did any enabled deployment fail to produce an observation packet?")
    lines.append("- Did Bhiksha fire where the Mala setup expected it to fire?")
    lines.append("- Did option selection pass DTE, delta, spread, OI, and premium constraints?")
    lines.append("- Are blocked entries legitimate market-quality skips or runtime bugs?")
    lines.append("- Did any exit, lifecycle, or cancellation issue require a code fix before tomorrow?")
    return "\n".join(lines) + "\n"


def _feedback_dirs(root: Path, *, active_plan_id: str | None) -> list[Path]:
    if active_plan_id:
        candidate = root / active_plan_id
        return [candidate] if candidate.exists() else []
    if not root.exists():
        return []
    dirs = [path for path in root.iterdir() if path.is_dir()]
    return sorted(dirs, key=lambda path: path.stat().st_mtime, reverse=True)[:3]


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return value if isinstance(value, dict) else {}


def _evidence_lookup(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    by_key: dict[str, dict[str, Any]] = {}
    for row in rows:
        for field in ("catalog_key", "sheet_row_id", "strategy_id", "deployment_id"):
            key = _clean(row.get(field))
            if key:
                by_key[key] = row
    return by_key


def _packet_catalog_key(packet: dict[str, Any]) -> str:
    startup = packet.get("startup_deployment")
    if isinstance(startup, dict):
        source = startup.get("source")
        if isinstance(source, dict):
            metadata = source.get("metadata")
            if isinstance(metadata, dict):
                return _clean(metadata.get("catalog_key"))
    return _clean(packet.get("catalog_key"))


def _dict_at(session: dict[str, Any], key: str, deployment_id: str) -> dict[str, Any]:
    value = session.get(key)
    if not isinstance(value, dict):
        return {}
    item = value.get(deployment_id)
    return item if isinstance(item, dict) else {}


def _session_count(session: dict[str, Any], key: str, deployment_id: str, fallback: Any) -> int:
    value = session.get(key)
    if isinstance(value, dict) and deployment_id in value:
        return _int(value.get(deployment_id))
    return _int(fallback)


def _blocked_reasons(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    return {str(key): count for key, count in value.items() if key != "approved" and _int(count) > 0}


def _trade_plan_count(packet_value: Any, session_blocked: dict[str, Any]) -> int:
    count = _int(packet_value)
    if count:
        return count
    if not session_blocked:
        return 0
    return sum(_int(value) for key, value in session_blocked.items() if not str(key).startswith("lifecycle_state:"))


def _runtime_issues(*, packet: dict[str, Any], session_issues: dict[str, Any]) -> dict[str, Any]:
    issues: dict[str, Any] = {}
    packet_issues = packet.get("runtime_issue_counts")
    if isinstance(packet_issues, dict):
        issues.update(packet_issues)
    issues.update(session_issues)
    replay = packet.get("replay")
    if isinstance(replay, dict) and _clean(replay.get("status")) == "error":
        issues.setdefault("replay_error", 1)
    return {str(key): value for key, value in issues.items() if _int(value) > 0}


def _activation_sort_key(row: dict[str, Any]) -> tuple[int, str, float, float]:
    decision_rank = {"shadow": 0, "observe_only": 1, "blocked": 2}.get(str(row.get("decision")), 9)
    return (
        decision_rank,
        str(row.get("strategy_key", "")),
        -_float(row.get("execution_robustness")),
        -_float(row.get("expectancy")),
    )


def _entry_start(signal_window_et: Any) -> str:
    text = _clean(signal_window_et)
    if "-" in text:
        return text.split("-", 1)[0]
    return "09:30"


def _json_compact(value: Any) -> str:
    if not value:
        return ""
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    headers = list(rows[0].keys()) if rows else []
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        if headers:
            writer.writeheader()
            writer.writerows(rows)


def _clean(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _truthy(value: Any) -> bool:
    return _clean(value).lower() in {"true", "1", "yes", "y", "shadow", "live"}


def _float(value: Any) -> float:
    try:
        return float(_clean(value) or 0.0)
    except ValueError:
        return 0.0


def _int(value: Any) -> int:
    try:
        return int(float(_clean(value) or 0))
    except ValueError:
        return 0


def _option_int(value: Any, default: int) -> int:
    text = _clean(value)
    if not text:
        return default
    try:
        return int(float(text))
    except ValueError:
        return default


def _format_number(value: Any) -> str:
    number = _float(value)
    if number.is_integer():
        return str(int(number))
    return str(number)


__all__ = [
    "ACTIVE_STRATEGY_HEADERS",
    "DEFAULT_ACTIVE_STRATEGY_SHEET_NAME",
    "DEFAULT_EVIDENCE_SHEET_NAME",
    "DEFAULT_OPERATOR_DEFAULTS_SHEET_NAME",
    "ShadowActivationArtifacts",
    "ShadowActivationConfig",
    "ShadowDailyReportArtifacts",
    "apply_active_strategy_rows",
    "apply_operator_defaults_patch",
    "build_shadow_activation_packet",
    "build_shadow_daily_report",
    "classify_shadow_activation",
    "read_sheet_rows",
]
