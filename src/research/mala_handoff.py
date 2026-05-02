"""Generate Mala-owned handoff evidence packets from M5 artifacts.

This is intentionally narrower than the legacy Strategy_Catalog publisher.
Mala publishes what it actually tested: strategy identity, strategy params,
derived signal window, M5 metrics, and optimized thesis exits. Runtime
execution details owned by Bhiksha or the operator are intentionally omitted
from the evidence table rather than guessed from generic option prose.
"""

from __future__ import annotations

import argparse
import csv
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, time, timedelta
import json
from pathlib import Path
import re
from typing import Any

import yaml

from src.config import settings
from src.research.bhiksha_capabilities import (
    BhikshaCapabilityResult,
    derive_strategy_variant,
    evaluate_bhiksha_capability,
    load_capability_manifest,
)
from src.research.google_sheets import GoogleSheetTableClient
from src.research.recommendation_tier import RecommendationThresholds, classify_recommendation_tier
from src.research.strategy_keys import to_strategy_key


MALA_HANDOFF_VERSION = 1
GENERATOR_VERSION = 1

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_RUNS_ROOT = REPO_ROOT / "data" / "results" / "hypothesis_runs"

_M5_NON_PARAM_COLS = {
    "ticker",
    "strategy",
    "direction",
    "execution_profile",
    "stress_profile",
    "selected_ratio",
    "evaluation_window",
    "holdout_trades",
    "holdout_win_rate",
    "base_exp_r",
    "trades",
    "passes_all_cost_gates",
    "passes_holdout",
    "mc_prob_positive_exp",
    "mc_exp_r_mean",
    "mc_exp_r_p05",
    "mc_exp_r_p50",
    "mc_exp_r_p95",
    "mc_total_r_p05",
    "mc_total_r_p50",
    "mc_total_r_p95",
    "mc_max_dd_p50",
    "mc_max_drawdown_p50",
    "structure",
    "dte",
    "delta_plan",
    "entry_window_et",
    "profit_take",
    "risk_rule",
}

_SELECTED_NON_PARAM_COLS = {
    "catalog_key",
    "ticker",
    "direction",
    "strategy",
    "execution_profile",
    "recommendation_tier",
    "recommendation_tier_reason",
    "recommendation_checks_json",
    "exit_reliability",
    "exit_trade_count",
    "selected_exit_policy",
    "mc_prob_positive_exp",
    "mc_exp_r_p50",
    "base_exp_r",
    "holdout_trades",
    "holdout_win_rate",
}

DEFAULT_EVIDENCE_SHEET_NAME = "Mala_Evidence_v1"

_REVIEW_METRIC_ORDER = [
    "expectancy",
    "profit_factor",
    "trade_count",
    "win_rate",
    "avg_winner",
    "avg_loser",
    "total_pnl",
]


@dataclass(frozen=True)
class MalaStrategyEvidence:
    strategy_key: str
    strategy_name: str
    strategy_variant: str
    symbol: str
    direction: str
    params: dict[str, Any]
    signal_window_start_et: str | None
    signal_window_end_et: str | None
    signal_window_derivation: str


@dataclass(frozen=True)
class MalaM5Evidence:
    m5_passed: bool
    recommendation_tier: str
    recommendation_tier_reason: str
    recommendation_checks: dict[str, Any]
    m5_execution_profile: str
    m5_stress_profile: str
    expectancy: float | None
    confidence: float | None
    signal_count: int | None
    execution_robustness: float | None
    mc_exp_r_p50: float | None
    holdout_win_rate: float | None
    holdout_trades: int | None
    note: str


@dataclass(frozen=True)
class MalaThesisExitEvidence:
    tested: bool
    policy: str | None
    policy_name: str | None
    params: dict[str, Any]
    metrics: dict[str, Any]
    reliability: str
    trade_count: int | None
    catastrophe_exit_params: dict[str, Any]
    note: str


@dataclass(frozen=True)
class HandoffProvenance:
    hypothesis_id: str
    catalog_key: str
    run_dir: str
    source_files: list[str]
    generated_at: str
    generator_version: int = GENERATOR_VERSION


@dataclass(frozen=True)
class MalaBhikshaCapabilityEvidence:
    strategy_variant: str
    status: str
    reason: str
    manifest_version: int | None
    bhiksha_ready: bool


@dataclass(frozen=True)
class MalaHandoffPacket:
    mala_handoff_version: int
    catalog_key: str
    strategy: MalaStrategyEvidence
    evidence: MalaM5Evidence
    thesis_exit: MalaThesisExitEvidence
    bhiksha_capability: MalaBhikshaCapabilityEvidence
    provenance: HandoffProvenance
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def build_handoff_packets(
    *,
    runs_root: str | Path = DEFAULT_RUNS_ROOT,
    latest_only: bool = True,
    include_watch_only: bool = True,
    bhiksha_capabilities_path: str | Path | None = None,
) -> list[MalaHandoffPacket]:
    selected = discover_selected_rows(runs_root=runs_root, latest_only=latest_only)
    capability_manifest = load_capability_manifest(bhiksha_capabilities_path)
    packets: list[MalaHandoffPacket] = []
    for item in selected:
        packet = build_handoff_packet(item.run_dir, item.selected, capability_manifest=capability_manifest)
        if not include_watch_only and packet.evidence.recommendation_tier == "watch_only":
            continue
        packets.append(packet)
    return sorted(packets, key=lambda packet: (packet.catalog_key, packet.provenance.run_dir))


@dataclass(frozen=True)
class SelectedArtifactRow:
    run_dir: Path
    selected: dict[str, str]


def discover_selected_rows(*, runs_root: str | Path, latest_only: bool = True) -> list[SelectedArtifactRow]:
    root = Path(runs_root)
    rows: list[SelectedArtifactRow] = []
    for path in sorted(root.glob("*/*/CATALOG_SELECTED.csv")):
        run_dir = path.parent
        for selected in _read_csv_dicts(path):
            if selected.get("catalog_key"):
                rows.append(SelectedArtifactRow(run_dir=run_dir, selected=selected))
    if not latest_only:
        return rows

    latest: dict[str, SelectedArtifactRow] = {}
    for row in rows:
        key = str(row.selected.get("catalog_key") or "")
        previous = latest.get(key)
        if previous is None or _run_sort_token(row.run_dir) >= _run_sort_token(previous.run_dir):
            latest[key] = row
    return list(latest.values())


def build_handoff_packet(
    run_dir: Path,
    selected: dict[str, str],
    *,
    capability_manifest: dict[str, Any] | None = None,
) -> MalaHandoffPacket:
    m5_row = matching_m5_row(run_dir, selected)
    exit_opt = exit_opt_for_selected(run_dir, selected)
    params = strategy_params_from_rows(m5_row=m5_row, selected=selected)
    strategy_name = str(m5_row.get("strategy") or selected.get("strategy") or "")
    strategy_key = to_strategy_key(strategy_name)
    symbol = str(m5_row.get("ticker") or selected.get("ticker") or "").upper()
    direction = str(m5_row.get("direction") or selected.get("direction") or "").lower()
    strategy_variant = derive_strategy_variant(
        strategy_key=strategy_key,
        strategy_name=strategy_name,
        strategy_params=params,
        manifest=capability_manifest,
    )
    signal_start, signal_end, derivation = derive_signal_window(strategy_key, params)
    catalog_key = str(selected.get("catalog_key") or f"{run_dir.parent.name}__{symbol.lower()}_{direction}")
    warnings = packet_warnings(
        selected=selected,
        m5_row=m5_row,
        exit_opt=exit_opt,
        signal_start=signal_start,
        signal_end=signal_end,
    )
    thesis_exit = thesis_exit_evidence(selected=selected, exit_opt=exit_opt)
    recommendation = classify_recommendation_tier(
        mc_prob_positive_exp=selected.get("mc_prob_positive_exp") or m5_row.get("mc_prob_positive_exp"),
        holdout_trades=selected.get("holdout_trades") or m5_row.get("holdout_trades"),
        base_exp_r=selected.get("base_exp_r") or m5_row.get("base_exp_r"),
        thesis_exit_tested=thesis_exit.tested,
        exit_trade_count=thesis_exit.trade_count,
        thresholds=_recommendation_thresholds(),
    )
    capability = evaluate_bhiksha_capability(
        strategy_key=strategy_key,
        strategy_name=strategy_name,
        strategy_params=params,
        thesis_exit_policy=thesis_exit.policy,
        thesis_exit_tested=thesis_exit.tested,
        recommendation_tier=recommendation.tier,
        manifest=capability_manifest,
    )
    warnings.extend(capability_warnings(capability))
    source_files = [
        _display_path(run_dir / "CATALOG_SELECTED.csv"),
        _display_path(run_dir / "M5_execution.csv"),
    ]
    if (run_dir / "m5_exit_optimizations.json").exists():
        source_files.append(_display_path(run_dir / "m5_exit_optimizations.json"))
    if exit_opt and exit_opt.get("_artifact_path"):
        source_files.append(_display_path(Path(exit_opt["_artifact_path"])))

    return MalaHandoffPacket(
        mala_handoff_version=MALA_HANDOFF_VERSION,
        catalog_key=catalog_key,
        strategy=MalaStrategyEvidence(
            strategy_key=strategy_key,
            strategy_name=strategy_name,
            strategy_variant=strategy_variant,
            symbol=symbol,
            direction=direction,
            params=params,
            signal_window_start_et=signal_start,
            signal_window_end_et=signal_end,
            signal_window_derivation=derivation,
        ),
        evidence=MalaM5Evidence(
            m5_passed=True,
            recommendation_tier=recommendation.tier,
            recommendation_tier_reason=recommendation.reason,
            recommendation_checks=recommendation.checks,
            m5_execution_profile=str(m5_row.get("execution_profile") or selected.get("execution_profile") or ""),
            m5_stress_profile=str(m5_row.get("stress_profile") or ""),
            expectancy=_float_or_none(selected.get("base_exp_r") or m5_row.get("base_exp_r")),
            confidence=_float_or_none(selected.get("holdout_win_rate") or m5_row.get("holdout_win_rate")),
            signal_count=_int_or_none(selected.get("holdout_trades") or m5_row.get("holdout_trades")),
            execution_robustness=_float_or_none(selected.get("mc_prob_positive_exp") or m5_row.get("mc_prob_positive_exp")),
            mc_exp_r_p50=_float_or_none(selected.get("mc_exp_r_p50") or m5_row.get("mc_exp_r_p50")),
            holdout_win_rate=_float_or_none(m5_row.get("holdout_win_rate") or selected.get("holdout_win_rate")),
            holdout_trades=_int_or_none(m5_row.get("holdout_trades") or selected.get("holdout_trades")),
            note="M5 evidence is research/backtest evidence, not live option execution authorization.",
        ),
        thesis_exit=thesis_exit,
        bhiksha_capability=MalaBhikshaCapabilityEvidence(
            strategy_variant=capability.strategy_variant,
            status=capability.status,
            reason=capability.reason,
            manifest_version=capability.manifest_version,
            bhiksha_ready=capability.bhiksha_ready,
        ),
        provenance=HandoffProvenance(
            hypothesis_id=run_dir.parent.name,
            catalog_key=catalog_key,
            run_dir=_display_path(run_dir),
            source_files=source_files,
            generated_at=datetime.now(UTC).isoformat(),
        ),
        warnings=warnings,
    )


def strategy_params_from_rows(*, m5_row: dict[str, str], selected: dict[str, str]) -> dict[str, Any]:
    params: dict[str, Any] = {}
    for source, excluded in ((m5_row, _M5_NON_PARAM_COLS), (selected, _SELECTED_NON_PARAM_COLS)):
        for key, value in source.items():
            if key in excluded or value in ("", None):
                continue
            params[key] = _parse_scalar(value)
    return params


def derive_signal_window(strategy_key: str, params: dict[str, Any]) -> tuple[str | None, str | None, str]:
    if strategy_key == "market_impulse":
        open_hour = _int_or_default(params.get("market_open_hour"), 9)
        open_minute = _int_or_default(params.get("market_open_minute"), 30)
        buffer_minutes = _int_or_default(params.get("entry_buffer_minutes"), 3)
        window_minutes = _int_or_default(params.get("entry_window_minutes"), 60)
        base = time(open_hour, open_minute)
        return (
            _add_minutes(base, buffer_minutes).strftime("%H:%M"),
            _add_minutes(base, window_minutes).strftime("%H:%M"),
            "market_open + entry_buffer_minutes / entry_window_minutes",
        )
    if strategy_key == "opening_drive_classifier":
        market_open = _parse_time(params.get("market_open")) or time(9, 30)
        start_offset = _int_or_default(params.get("entry_start_offset_minutes"), 25)
        end_offset = _int_or_default(params.get("entry_end_offset_minutes"), 120)
        return (
            _add_minutes(market_open, start_offset).strftime("%H:%M"),
            _add_minutes(market_open, end_offset).strftime("%H:%M"),
            "market_open + entry_start_offset_minutes / entry_end_offset_minutes",
        )
    if strategy_key == "jerk_pivot_momentum":
        use_time_filter = _bool_or_default(params.get("use_time_filter"), True)
        if use_time_filter:
            return (
                _normalize_time(params.get("session_start") or "09:35"),
                _normalize_time(params.get("session_end") or "15:30"),
                "session_start / session_end",
            )
        return None, None, "use_time_filter=false"
    return None, None, "no strategy-level signal window in Mala strategy params"


def thesis_exit_evidence(selected: dict[str, str], exit_opt: dict[str, Any] | None) -> MalaThesisExitEvidence:
    if not exit_opt:
        selected_policy = str(selected.get("selected_exit_policy") or "")
        policy = selected_policy.split(":", 1)[0] if selected_policy else None
        return MalaThesisExitEvidence(
            tested=False,
            policy=policy,
            policy_name=selected_policy or None,
            params={},
            metrics={},
            reliability=str(selected.get("exit_reliability") or "none"),
            trade_count=_int_or_none(selected.get("exit_trade_count")),
            catastrophe_exit_params={},
            note="No exit optimization artifact matched this selected row.",
        )
    return MalaThesisExitEvidence(
        tested=True,
        policy=str(exit_opt.get("thesis_exit_policy") or ""),
        policy_name=str(exit_opt.get("selected_policy_name") or ""),
        params=dict(exit_opt.get("thesis_exit_params") or {}),
        metrics=dict(exit_opt.get("selected_metrics") or {}),
        reliability=str(selected.get("exit_reliability") or ""),
        trade_count=_int_or_none(selected.get("exit_trade_count") or (exit_opt.get("selected_metrics") or {}).get("trade_count")),
        catastrophe_exit_params=dict(exit_opt.get("catastrophe_exit_params") or {}),
        note="Underlying thesis exit was selected by Mala exit optimization; option premium handling remains operator/Bhiksha owned.",
    )


def packet_warnings(
    *,
    selected: dict[str, str],
    m5_row: dict[str, str],
    exit_opt: dict[str, Any] | None,
    signal_start: str | None,
    signal_end: str | None,
) -> list[str]:
    warnings: list[str] = []
    if not signal_start and not signal_end:
        warnings.append("strategy_signal_window_not_declared")
    if selected.get("selected_exit_policy") and not exit_opt:
        warnings.append("selected_exit_policy_without_matching_exit_artifact")
    if selected.get("recommendation_tier") == "watch_only":
        warnings.append("watch_only_candidate_not_catalog_ready")
    return warnings


def capability_warnings(capability: BhikshaCapabilityResult) -> list[str]:
    if capability.status == "supported":
        return []
    if capability.status == "unknown_manifest":
        return [capability.reason]
    return [f"bhiksha_unsupported_variant:{capability.strategy_variant}:{capability.reason}"]


def matching_m5_row(run_dir: Path, selected: dict[str, str]) -> dict[str, str]:
    rows = _read_csv_dicts(run_dir / "M5_execution.csv")
    for row in rows:
        if selected_matches_m5(selected, row):
            return row
    catalog_key = selected.get("catalog_key", "<unknown>")
    raise RuntimeError(f"No M5_execution.csv row matched {catalog_key} in {run_dir}")


def selected_matches_m5(selected: dict[str, str], row: dict[str, str]) -> bool:
    ignored = {
        "catalog_key",
        "recommendation_tier",
        "exit_reliability",
        "exit_trade_count",
        "selected_exit_policy",
        "mc_prob_positive_exp",
        "mc_exp_r_p50",
        "base_exp_r",
        "holdout_trades",
        "holdout_win_rate",
    }
    for key, value in selected.items():
        if key in ignored or value in ("", None):
            continue
        if key not in row:
            continue
        if str(row.get(key, "")).strip() != str(value).strip():
            return False
    return True


def exit_opt_for_selected(run_dir: Path, selected: dict[str, str]) -> dict[str, Any] | None:
    summary_path = run_dir / "m5_exit_optimizations.json"
    if not summary_path.exists():
        return None
    try:
        items = json.loads(summary_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    if not isinstance(items, list):
        return None
    for item in items:
        if not isinstance(item, dict) or not exit_opt_matches_selected(item, selected):
            continue
        artifact = run_dir / str(item.get("artifact") or "")
        if artifact.exists():
            try:
                payload = json.loads(artifact.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                return None
            if isinstance(payload, dict):
                payload["_artifact_path"] = str(artifact)
                return payload
        selected_policy = str(item.get("selected_policy_name") or selected.get("selected_exit_policy") or "")
        if not selected_policy:
            return None
        return {
            "selected_policy_name": selected_policy,
            "thesis_exit_policy": selected_policy.split(":", 1)[0],
            "selected_metrics": item.get("selected_metrics", {}),
        }
    return None


def exit_opt_matches_selected(item: dict[str, Any], selected: dict[str, str]) -> bool:
    key = item.get("candidate_key", {})
    if not isinstance(key, dict):
        return False
    match_keys = [
        "ticker",
        "direction",
        "strategy",
        "entry_buffer_minutes",
        "entry_window_minutes",
        "regime_timeframe",
        "vwma_periods",
    ]
    for field in match_keys:
        selected_value = selected.get(field, "")
        if selected_value in ("", None):
            continue
        if not _values_match(key.get(field, ""), selected_value):
            return False
    return True


def write_handoff_outputs(packets: list[MalaHandoffPacket], out_dir: str | Path) -> dict[str, Path]:
    output = Path(out_dir)
    output.mkdir(parents=True, exist_ok=True)
    csv_path = output / "MALA_HANDOFF_CANDIDATES.csv"
    md_path = output / "MALA_HANDOFF_CANDIDATES.md"
    jsonl_path = output / "mala_handoff_payloads.jsonl"
    warnings_path = output / "MALA_HANDOFF_WARNINGS.md"

    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=handoff_csv_fieldnames())
        writer.writeheader()
        for packet in packets:
            writer.writerow(packet_to_csv_row(packet))

    with jsonl_path.open("w", encoding="utf-8") as handle:
        for packet in packets:
            handle.write(json.dumps(packet.to_dict(), sort_keys=True, default=str) + "\n")

    md_path.write_text(render_handoff_markdown(packets), encoding="utf-8")
    warnings_path.write_text(render_warnings_markdown(packets), encoding="utf-8")
    return {
        "csv": csv_path,
        "markdown": md_path,
        "jsonl": jsonl_path,
        "warnings": warnings_path,
    }


def publish_review_tabs(
    packets: list[MalaHandoffPacket],
    *,
    spreadsheet_id: str,
    credentials_path: str | Path,
    evidence_sheet_name: str = DEFAULT_EVIDENCE_SHEET_NAME,
) -> dict[str, int]:
    evidence_client = GoogleSheetTableClient(
        spreadsheet_id=spreadsheet_id,
        sheet_name=evidence_sheet_name,
        credentials_path=Path(credentials_path),
    )
    evidence_rows = [packet_to_csv_row(packet) for packet in packets]
    evidence_client.overwrite_table(headers=handoff_csv_fieldnames(), rows=evidence_rows)
    return {
        "evidence_rows": len(evidence_rows),
    }


def handoff_csv_fieldnames() -> list[str]:
    return [
        "mala_handoff_version",
        "catalog_key",
        "hypothesis_id",
        "symbol",
        "direction",
        "strategy_key",
        "strategy_name",
        "strategy_variant",
        "strategy_params_json",
        "bhiksha_capability_status",
        "bhiksha_capability_reason",
        "bhiksha_ready",
        "signal_window_et",
        "signal_window_derivation",
        "recommendation_tier",
        "recommendation_tier_reason",
        "recommendation_checks_json",
        "expectancy",
        "confidence",
        "signal_count",
        "execution_robustness",
        "m5_execution_profile",
        "m5_stress_profile",
        "thesis_exit_tested",
        "thesis_exit_policy",
        "thesis_exit_params_json",
        "thesis_exit_metrics_json",
        "exit_reliability",
        "exit_trade_count",
        "run_dir",
        "warnings",
    ]


def packet_to_csv_row(packet: MalaHandoffPacket) -> dict[str, Any]:
    return {
        "mala_handoff_version": packet.mala_handoff_version,
        "catalog_key": packet.catalog_key,
        "hypothesis_id": packet.provenance.hypothesis_id,
        "symbol": packet.strategy.symbol,
        "direction": packet.strategy.direction,
        "strategy_key": packet.strategy.strategy_key,
        "strategy_name": packet.strategy.strategy_name,
        "strategy_variant": packet.bhiksha_capability.strategy_variant,
        "strategy_params_json": json.dumps(packet.strategy.params, sort_keys=True),
        "bhiksha_capability_status": packet.bhiksha_capability.status,
        "bhiksha_capability_reason": packet.bhiksha_capability.reason,
        "bhiksha_ready": str(packet.bhiksha_capability.bhiksha_ready).lower(),
        "signal_window_et": _format_window(packet.strategy.signal_window_start_et, packet.strategy.signal_window_end_et),
        "signal_window_derivation": packet.strategy.signal_window_derivation,
        "recommendation_tier": packet.evidence.recommendation_tier,
        "recommendation_tier_reason": packet.evidence.recommendation_tier_reason,
        "recommendation_checks_json": json.dumps(packet.evidence.recommendation_checks, sort_keys=True),
        "expectancy": _format_optional(packet.evidence.expectancy),
        "confidence": _format_optional(packet.evidence.confidence),
        "signal_count": _format_optional(packet.evidence.signal_count),
        "execution_robustness": _format_optional(packet.evidence.execution_robustness),
        "m5_execution_profile": packet.evidence.m5_execution_profile,
        "m5_stress_profile": packet.evidence.m5_stress_profile,
        "thesis_exit_tested": packet.thesis_exit.tested,
        "thesis_exit_policy": packet.thesis_exit.policy or "",
        "thesis_exit_params_json": json.dumps(packet.thesis_exit.params, sort_keys=True),
        "thesis_exit_metrics_json": json.dumps(review_thesis_exit_metrics(packet.thesis_exit.metrics)),
        "exit_reliability": packet.thesis_exit.reliability,
        "exit_trade_count": _format_optional(packet.thesis_exit.trade_count),
        "run_dir": packet.provenance.run_dir,
        "warnings": " | ".join(packet.warnings),
    }


def review_thesis_exit_metrics(metrics: dict[str, Any]) -> dict[str, Any]:
    """Return human-review metrics in stable order, rounded for Sheet readability."""
    formatted: dict[str, Any] = {}
    for key in _REVIEW_METRIC_ORDER:
        if key in metrics:
            formatted[key] = _round_review_value(metrics[key])
    for key, value in metrics.items():
        if key not in formatted:
            formatted[key] = _round_review_value(value)
    return formatted


def render_handoff_markdown(packets: list[MalaHandoffPacket]) -> str:
    counts: dict[str, int] = {}
    for packet in packets:
        counts[packet.evidence.recommendation_tier] = counts.get(packet.evidence.recommendation_tier, 0) + 1
    lines = [
        "# Mala Handoff Candidates",
        "",
        f"- generated_at: `{datetime.now(UTC).isoformat()}`",
        f"- packets: `{len(packets)}`",
        f"- recommendation_counts: `{json.dumps(counts, sort_keys=True)}`",
        "",
        "This file is Mala-owned evidence only. Runtime option vehicle, execution window, premium budget, option stops/targets, live/shadow mode, and conflict policy are operator/Bhiksha-owned and intentionally excluded.",
        "",
        "| catalog_key | symbol | strategy | variant | Bhiksha | signal_window_et | tier | reason | expectancy | thesis_exit | warnings |",
        "|---|---|---|---|---|---|---|---|---:|---|---|",
    ]
    for packet in packets:
        lines.append(
            "| "
            + " | ".join(
                [
                    _md(packet.catalog_key),
                    _md(f"{packet.strategy.symbol} {packet.strategy.direction}"),
                    _md(packet.strategy.strategy_key),
                    _md(packet.bhiksha_capability.strategy_variant),
                    _md(packet.bhiksha_capability.status),
                    _md(_format_window(packet.strategy.signal_window_start_et, packet.strategy.signal_window_end_et)),
                    _md(packet.evidence.recommendation_tier),
                    _md(packet.evidence.recommendation_tier_reason),
                    _md(_format_optional(packet.evidence.expectancy)),
                    _md(packet.thesis_exit.policy or ""),
                    _md("<br>".join(packet.warnings)),
                ]
            )
            + " |"
        )
    lines.append("")
    return "\n".join(lines)


def render_warnings_markdown(packets: list[MalaHandoffPacket]) -> str:
    lines = [
        "# Mala Handoff Warnings",
        "",
        f"- generated_at: `{datetime.now(UTC).isoformat()}`",
        "",
    ]
    for packet in packets:
        if not packet.warnings:
            continue
        lines.append(f"## {packet.catalog_key}")
        lines.append("")
        lines.append(f"- symbol: `{packet.strategy.symbol}`")
        lines.append(f"- strategy: `{packet.strategy.strategy_key}`")
        for warning in packet.warnings:
            lines.append(f"- `{warning}`")
        lines.append("")
    return "\n".join(lines)


def default_output_dir() -> Path:
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    return REPO_ROOT / "data" / "results" / "mala_handoff" / stamp


def _read_csv_dicts(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _display_path(path: Path) -> str:
    resolved = path.resolve()
    try:
        return str(resolved.relative_to(REPO_ROOT))
    except ValueError:
        return str(resolved)


def _run_sort_token(run_dir: Path) -> tuple[str, str]:
    return (run_dir.parent.name, run_dir.name)


def _parse_scalar(value: Any) -> Any:
    if value is None:
        return None
    text = str(value).strip()
    if text == "":
        return ""
    lowered = text.lower()
    if lowered in {"true", "false"}:
        return lowered == "true"
    if "," in text:
        parts = [part.strip() for part in text.split(",") if part.strip()]
        if parts and all(_looks_int(part) for part in parts):
            return [int(part) for part in parts]
        return text
    if _looks_int(text):
        return int(text)
    if _looks_float(text):
        return float(text)
    return text


def _values_match(left: Any, right: Any) -> bool:
    left_text = str(left or "").strip()
    right_text = str(right or "").strip()
    if left_text == right_text:
        return True
    return _parse_match_value(left_text) == _parse_match_value(right_text)


def _parse_match_value(value: str) -> Any:
    text = value.strip()
    if text.startswith("[") and text.endswith("]"):
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            parsed = None
        if isinstance(parsed, list):
            return parsed
    return _parse_scalar(text)


def _looks_int(text: str) -> bool:
    return bool(re.fullmatch(r"-?\d+", text))


def _looks_float(text: str) -> bool:
    return bool(re.fullmatch(r"-?\d+\.\d+", text))


def _float_or_none(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _int_or_none(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _int_or_default(value: Any, default: int) -> int:
    parsed = _int_or_none(value)
    return default if parsed is None else parsed


def _bool_or_default(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    text = str(value).strip().lower()
    if text in {"true", "yes", "y", "1"}:
        return True
    if text in {"false", "no", "n", "0"}:
        return False
    return default


def _parse_time(value: Any) -> time | None:
    if value is None:
        return None
    text = str(value).strip().strip("'\"")
    match = re.fullmatch(r"(?P<h>\d{1,2}):(?P<m>\d{2})(?::\d{2})?", text)
    if not match:
        return None
    hour = int(match.group("h"))
    minute = int(match.group("m"))
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        return None
    return time(hour, minute)


def _normalize_time(value: Any) -> str | None:
    parsed = _parse_time(value)
    return parsed.strftime("%H:%M") if parsed else None


def _add_minutes(base: time, minutes: int) -> time:
    shifted = datetime(2000, 1, 1, base.hour, base.minute) + timedelta(minutes=minutes)
    return shifted.time()


def _format_window(start: str | None, end: str | None) -> str:
    if start and end:
        return f"{start}-{end}"
    if start:
        return f"{start}-open"
    if end:
        return f"open-{end}"
    return ""


def _format_optional(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:g}"
    return str(value)


def _round_review_value(value: Any) -> Any:
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return round(value, 2)
    return value


def _md(value: Any) -> str:
    return str(value or "").replace("|", "\\|")


def _recommendation_thresholds() -> RecommendationThresholds:
    config_path = REPO_ROOT / "config" / "hypothesis_defaults.yaml"
    try:
        config = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    except FileNotFoundError:
        config = {}
    catalog = config.get("catalog", {}) if isinstance(config, dict) else {}
    m4 = config.get("m4", {}) if isinstance(config, dict) else {}
    return RecommendationThresholds(
        min_mc_prob_for_catalog=float(catalog.get("min_mc_prob_for_catalog", 0.70)),
        min_mc_prob_for_promote=float(catalog.get("min_mc_prob_for_promote", 0.95)),
        min_holdout_trades_for_promote=int(catalog.get("min_holdout_trades_for_promote", 80)),
        min_holdout_trades_for_shadow=int(m4.get("min_holdout_signals", 15)),
        min_exit_trades_for_promote=int(catalog.get("min_exit_trades_for_bhiksha_ready", 40)),
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate Mala-owned handoff evidence packets from M5 artifacts.")
    parser.add_argument("--runs-root", default=str(DEFAULT_RUNS_ROOT), help="Root containing data/results/hypothesis_runs artifacts.")
    parser.add_argument("--out-dir", default="", help="Output directory for handoff preview artifacts.")
    parser.add_argument("--all-runs", action="store_true", help="Include older duplicate catalog_key rows instead of latest only.")
    parser.add_argument("--promote-shadow-only", action="store_true", help="Exclude watch_only candidates.")
    parser.add_argument("--publish-sheets", action="store_true", help="Overwrite the Mala evidence review tab in Google Sheets.")
    parser.add_argument("--sheet-id", default="", help="Google spreadsheet ID or URL. Defaults to STRATEGY_CATALOG_SHEET_ID.")
    parser.add_argument("--google-credentials", default="", help="Google service-account JSON path. Defaults to GOOGLE_API_CREDENTIALS_PATH.")
    parser.add_argument("--evidence-sheet-name", default=DEFAULT_EVIDENCE_SHEET_NAME)
    parser.add_argument(
        "--bhiksha-capabilities",
        default="",
        help="Path to Bhiksha-owned capability manifest. Defaults to BHIKSHA_CAPABILITIES_PATH or sibling/oldmac checkout.",
    )
    args = parser.parse_args(argv)

    packets = build_handoff_packets(
        runs_root=args.runs_root,
        latest_only=not args.all_runs,
        include_watch_only=not args.promote_shadow_only,
        bhiksha_capabilities_path=args.bhiksha_capabilities or None,
    )
    out_dir = Path(args.out_dir) if args.out_dir else default_output_dir()
    paths = write_handoff_outputs(packets, out_dir)
    counts: dict[str, int] = {}
    for packet in packets:
        tier = packet.evidence.recommendation_tier or "unknown"
        counts[tier] = counts.get(tier, 0) + 1
    print(f"HANDOFF_ROWS={len(packets)}")
    print(f"RECOMMENDATION_COUNTS={json.dumps(counts, sort_keys=True)}")
    for name, path in paths.items():
        print(f"{name.upper()}={path}")
    if args.publish_sheets:
        sheet_id = args.sheet_id or settings.strategy_catalog_sheet_id
        credentials = args.google_credentials or settings.google_api_credentials_path
        if not sheet_id:
            raise SystemExit("--sheet-id or STRATEGY_CATALOG_SHEET_ID is required for --publish-sheets")
        if not credentials:
            raise SystemExit("--google-credentials or GOOGLE_API_CREDENTIALS_PATH is required for --publish-sheets")
        publish_result = publish_review_tabs(
            packets,
            spreadsheet_id=sheet_id,
            credentials_path=credentials,
            evidence_sheet_name=args.evidence_sheet_name,
        )
        print(f"EVIDENCE_SHEET={args.evidence_sheet_name}")
        print(f"SHEET_PUBLISH={json.dumps(publish_result, sort_keys=True)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
