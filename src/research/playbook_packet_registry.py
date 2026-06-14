"""Write shared-kernel packets for Mala playbook surfaces."""

from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.research.exit_profiles import assigned_profile
from src.research.option_translation import OPTION_PROFILES
from src.research.shared_kernel import ensure_kernel_on_path
from src.research.strategy_keys import to_strategy_key
from src.strategy.intraday_mean_reversion import PLAYBOOK_ID, STRATEGY_NAME

ensure_kernel_on_path()

from mala_bhiksha_kernel import (  # noqa: E402
    ExecutionPacket,
    FeatureContract,
    FeatureSpec,
    ManagementPolicy,
    ManagementPolicySpec,
    OperatorApproval,
    PacketLineage,
    PacketRef,
    PacketStatus,
    PlaybookPacket,
    RuntimeMode,
    SourceArtifact,
    read_packet_file,
    write_packet,
    write_registry_index,
)


DEFAULT_PACKET_ID = "playbook.mean_reversion_at_extremes.iwm_qqq"
DEFAULT_PACKET_VERSION = 1
DEFAULT_EXECUTION_PACKET_ID = "execution.mean_reversion_at_extremes.iwm_qqq"
DEFAULT_LIVE_EXECUTION_PACKET_VERSION = 2
DEFAULT_CAPABILITY_MANIFEST_ID = "bhiksha.packet_capabilities.v1"
DEFAULT_EXPLORATION_UNIVERSE = "iwm_qqq"
DEFAULT_EXPLORATION_LABEL = "IWM/QQQ Exploration"


def write_mean_reversion_playbook_packet(
    run_dir: Path,
    *,
    packet_root: Path,
    packet_id: str = DEFAULT_PACKET_ID,
    version: int = DEFAULT_PACKET_VERSION,
    status: PacketStatus = PacketStatus.REVIEW,
) -> Path:
    config_path = run_dir / "config.json"
    surface_path = run_dir / "conditional_surface_by_symbol.csv"
    if not config_path.exists():
        raise FileNotFoundError(f"config.json not found under {run_dir}")
    if not surface_path.exists():
        raise FileNotFoundError(f"conditional_surface_by_symbol.csv not found under {run_dir}")

    run_config = json.loads(config_path.read_text(encoding="utf-8"))
    playbook_id = str(run_config.get("playbook_id", ""))
    if playbook_id != PLAYBOOK_ID:
        raise ValueError(f"unsupported playbook {playbook_id!r}; expected {PLAYBOOK_ID!r}")

    symbols = [str(symbol).upper() for symbol in run_config.get("symbols", [])]
    packet = PlaybookPacket(
        packet_id=packet_id,
        version=version,
        status=status,
        title=f"Mean Reversion At Extremes Intraday - {DEFAULT_EXPLORATION_LABEL}",
        symbol_scope=symbols,
        intended_horizon="intraday-short-horizon",
        feature_contract=_feature_contract(run_config),
        lineage=_lineage(run_dir),
        playbook_id=PLAYBOOK_ID,
        entry_model={
            "strategy": STRATEGY_NAME,
            "config_generation": run_config.get("config_generation"),
            "calibration_holdout_split": run_config.get("calibration_holdout_split"),
            "match_grade_thresholds": run_config.get("match_grade_thresholds", {}),
        },
        management_policies=_management_policies(surface_path),
        consultation_state="ready_for_parity",
        metadata={
            "source_run_dir": str(run_dir),
            "surface_rows": _row_count(surface_path),
            "config_count": run_config.get("config_count"),
            "feature_families_tested": run_config.get("feature_families_tested", {}),
            "date_range": {
                "start": run_config.get("start"),
                "end": run_config.get("end"),
            },
            "generated_from": "src.research.playbook_packet_registry",
            "playbook_scope": PLAYBOOK_ID,
            "exploration_universe": DEFAULT_EXPLORATION_UNIVERSE,
        },
    )
    packet_path = write_packet(packet_root, packet)
    write_registry_index(packet_root)
    return packet_path


def write_mean_reversion_shadow_execution_packet(
    *,
    playbook_packet_path: Path,
    parity_report_path: Path,
    packet_root: Path,
    packet_id: str = DEFAULT_EXECUTION_PACKET_ID,
    version: int = DEFAULT_PACKET_VERSION,
    status: PacketStatus = PacketStatus.REVIEW,
    operator: str | None = None,
    operator_notes: str | None = None,
    capability_manifest_id: str = DEFAULT_CAPABILITY_MANIFEST_ID,
) -> Path:
    """Draft or approve the first shadow execution packet for the playbook."""
    playbook_packet = read_packet_file(playbook_packet_path)
    if not isinstance(playbook_packet, PlaybookPacket):
        raise ValueError(
            f"expected a playbook packet at {playbook_packet_path}, "
            f"found {playbook_packet.kind.value}"
        )
    if playbook_packet.playbook_id != PLAYBOOK_ID:
        raise ValueError(
            f"unsupported playbook {playbook_packet.playbook_id!r}; expected {PLAYBOOK_ID!r}"
        )
    parity_report = _load_passed_parity_report(
        parity_report_path,
        expected_packet_ref=playbook_packet.ref,
    )
    if status == PacketStatus.APPROVED and not operator:
        raise ValueError("--operator is required when writing an approved execution packet")

    management_policy_ids = [policy.policy_id for policy in playbook_packet.management_policies]
    management_policy_specs = {
        policy.policy_id: _management_policy_spec_from_policy(
            policy, strategy_name=STRATEGY_NAME
        ).model_dump(mode="json")
        for policy in playbook_packet.management_policies
    }
    packet = ExecutionPacket(
        packet_id=packet_id,
        version=version,
        status=status,
        title=f"Mean Reversion At Extremes Shadow Execution - {DEFAULT_EXPLORATION_LABEL}",
        symbol_scope=playbook_packet.symbol_scope,
        intended_horizon=playbook_packet.intended_horizon,
        feature_contract=playbook_packet.feature_contract,
        lineage=PacketLineage(
            source_system="mala_v2",
            parent_packet=playbook_packet.ref,
            source_artifacts=[
                SourceArtifact(
                    label="source_playbook_packet",
                    uri=str(playbook_packet_path),
                ),
                SourceArtifact(
                    label="parity_report",
                    uri=str(parity_report_path),
                ),
                *playbook_packet.lineage.source_artifacts,
            ],
        ),
        operator_approval=OperatorApproval(
            status="approved" if status == PacketStatus.APPROVED else "pending",
            actor=operator,
            approved_at=datetime.now(UTC) if status == PacketStatus.APPROVED else None,
            notes=operator_notes,
        ),
        metadata={
            "generated_from": "src.research.playbook_packet_registry",
            "playbook_scope": PLAYBOOK_ID,
            "exploration_universe": DEFAULT_EXPLORATION_UNIVERSE,
            "source_playbook_packet_id": playbook_packet.packet_id,
            "source_playbook_packet_version": playbook_packet.version,
            "parity_compared_event_count": parity_report.get("compared_event_count"),
            "consultation_required_before_live": True,
        },
        source_packet=playbook_packet.ref,
        runtime_mode=RuntimeMode.SHADOW,
        capability_manifest_id=capability_manifest_id,
        parity_report_id=str(parity_report["report_id"]),
        runtime_controls={
            "allowed_management_policy_ids": management_policy_ids,
            "management_policy_specs": management_policy_specs,
            "management_policy_specs_required": True,
            "operator_must_select_management_policy": True,
            "shadow_only": True,
            "live_automated_allowed": False,
            "option_selection_preview_only": True,
            "consultation_batch_gate": "review_closed_batch_before_live",
        },
    )
    packet_path = write_packet(packet_root, packet)
    write_registry_index(packet_root)
    return packet_path


def write_mean_reversion_live_execution_packet(
    *,
    playbook_packet_path: Path,
    parity_report_path: Path,
    packet_root: Path,
    packet_id: str = DEFAULT_EXECUTION_PACKET_ID,
    version: int = DEFAULT_LIVE_EXECUTION_PACKET_VERSION,
    status: PacketStatus = PacketStatus.REVIEW,
    operator: str | None = None,
    operator_notes: str | None = None,
    capability_manifest_id: str = DEFAULT_CAPABILITY_MANIFEST_ID,
    max_live_quantity: int = 1,
    max_trade_premium_usd: float = 300.0,
) -> Path:
    """Draft or approve the live approval-gated execution packet for Monday pilot."""
    playbook_packet = read_packet_file(playbook_packet_path)
    if not isinstance(playbook_packet, PlaybookPacket):
        raise ValueError(
            f"expected a playbook packet at {playbook_packet_path}, "
            f"found {playbook_packet.kind.value}"
        )
    if playbook_packet.playbook_id != PLAYBOOK_ID:
        raise ValueError(
            f"unsupported playbook {playbook_packet.playbook_id!r}; expected {PLAYBOOK_ID!r}"
        )
    parity_report = _load_passed_parity_report(
        parity_report_path,
        expected_packet_ref=playbook_packet.ref,
    )
    if status == PacketStatus.APPROVED and not operator:
        raise ValueError("--operator is required when writing an approved execution packet")

    management_policy_ids = [policy.policy_id for policy in playbook_packet.management_policies]
    management_policy_specs = {
        policy.policy_id: _management_policy_spec_from_policy(
            policy, strategy_name=STRATEGY_NAME
        ).model_dump(mode="json")
        for policy in playbook_packet.management_policies
    }
    packet = ExecutionPacket(
        packet_id=packet_id,
        version=version,
        status=status,
        title=(
            "Mean Reversion At Extremes Live Approval-Gated Execution - "
            f"{DEFAULT_EXPLORATION_LABEL}"
        ),
        symbol_scope=playbook_packet.symbol_scope,
        intended_horizon=playbook_packet.intended_horizon,
        feature_contract=playbook_packet.feature_contract,
        lineage=PacketLineage(
            source_system="mala_v2",
            parent_packet=playbook_packet.ref,
            source_artifacts=[
                SourceArtifact(label="source_playbook_packet", uri=str(playbook_packet_path)),
                SourceArtifact(label="parity_report", uri=str(parity_report_path)),
                *playbook_packet.lineage.source_artifacts,
            ],
        ),
        operator_approval=OperatorApproval(
            status="approved" if status == PacketStatus.APPROVED else "pending",
            actor=operator,
            approved_at=datetime.now(UTC) if status == PacketStatus.APPROVED else None,
            notes=operator_notes,
        ),
        metadata={
            "generated_from": "src.research.playbook_packet_registry",
            "playbook_scope": PLAYBOOK_ID,
            "exploration_universe": DEFAULT_EXPLORATION_UNIVERSE,
            "source_playbook_packet_id": playbook_packet.packet_id,
            "source_playbook_packet_version": playbook_packet.version,
            "parity_compared_event_count": parity_report.get("compared_event_count"),
            "live_pilot": True,
            "live_pilot_date": "2026-05-18",
            "live_pilot_boundary": "operator_approval_gated_small_account",
        },
        source_packet=playbook_packet.ref,
        runtime_mode=RuntimeMode.LIVE_APPROVAL_GATED,
        capability_manifest_id=capability_manifest_id,
        parity_report_id=str(parity_report["report_id"]),
        runtime_controls={
            "allowed_management_policy_ids": management_policy_ids,
            "management_policy_specs": management_policy_specs,
            "management_policy_specs_required": True,
            "operator_must_select_management_policy": True,
            "shadow_only": False,
            "live_automated_allowed": False,
            "live_ticket_required": True,
            "option_selection_preview_only": True,
            "max_live_quantity": int(max_live_quantity),
            "max_trade_premium_usd": float(max_trade_premium_usd),
            "requires_underlying_stop_price": True,
            "live_management_required": True,
        },
    )
    packet_path = write_packet(packet_root, packet)
    write_registry_index(packet_root)
    return packet_path


def _load_passed_parity_report(
    parity_report_path: Path,
    *,
    expected_packet_ref: PacketRef,
) -> dict[str, Any]:
    report = json.loads(parity_report_path.read_text(encoding="utf-8"))
    if report.get("status") != "passed":
        raise ValueError(f"parity report {parity_report_path} is not passed")
    packet_ref = report.get("packet_ref") or {}
    expected = expected_packet_ref.model_dump(mode="json")
    if packet_ref != expected:
        raise ValueError(
            f"parity report packet_ref {packet_ref!r} does not match {expected!r}"
        )
    return report


def _feature_contract(run_config: dict[str, Any]) -> FeatureContract:
    features = [
        FeatureSpec(
            name=name,
            provider_sensitive=True,
        )
        for name in [
            "opening_vwap_rth",
            "prior_rth_close_atr",
            "vpoc_4h",
            "market_pulse_stage",
            "gap_state_rth_open",
            "velocity",
            "jerk",
            "relative_volume_rth",
        ]
    ]
    return FeatureContract(
        contract_id="mean_reversion_at_extremes_intraday_v1",
        bar_interval="1m",
        session="rth",
        provider="polygon",
        warmup_bars=60,
        features=features,
    )


def _lineage(run_dir: Path) -> PacketLineage:
    artifacts = [
        SourceArtifact(label="run_config", uri=str(run_dir / "config.json")),
        SourceArtifact(
            label="conditional_surface",
            uri=str(run_dir / "conditional_surface_by_symbol.csv"),
        ),
        SourceArtifact(label="sample_events", uri=str(run_dir / "sample_events.csv")),
        SourceArtifact(label="receipt", uri=str(run_dir / "RECEIPT.md")),
    ]
    review = run_dir / "surface_review" / "SURFACE_REVIEW.md"
    if review.exists():
        artifacts.append(SourceArtifact(label="surface_review", uri=str(review)))
    return PacketLineage(source_system="mala_v2", source_artifacts=artifacts)


def _management_policies(surface_path: Path) -> list[ManagementPolicy]:
    rows = _surface_rows(surface_path)
    ranked = sorted(
        rows,
        key=lambda row: (
            _grade_rank(str(row.get("match_grade", ""))),
            _rank_float(row.get("holdout_expectancy_r")),
            _rank_float(row.get("calibration_expectancy_r")),
            _rank_float(row.get("holdout_win_rate")),
        ),
        reverse=True,
    )
    policies: list[ManagementPolicy] = []
    seen: set[tuple[str, str]] = set()
    for row in ranked:
        key = (str(row.get("stop_family", "")), str(row.get("exit_family", "")))
        if key in seen:
            continue
        seen.add(key)
        policies.append(
            _management_policy_from_surface_row(row, rank=len(policies) + 1)
        )
        if len(policies) == 2:
            break
    return policies


def _management_policy_from_surface_row(row: dict[str, str], *, rank: int) -> ManagementPolicy:
    stop_family = str(row.get("stop_family", ""))
    exit_family = str(row.get("exit_family", ""))
    spec = _management_policy_spec(
        policy_id=f"{stop_family}__{exit_family}",
        stop_family=stop_family,
        exit_family=exit_family,
        source_config_id=row.get("config_id"),
        parameters={
            "match_grade": row.get("match_grade"),
            "sample_count": _safe_int(row.get("sample_count")),
            "calibration_expectancy_r": _optional_float(row.get("calibration_expectancy_r")),
            "holdout_expectancy_r": _optional_float(row.get("holdout_expectancy_r")),
            "holdout_win_rate": _optional_float(row.get("holdout_win_rate")),
        },
    )
    return ManagementPolicy(
        policy_id=spec.policy_id,
        name=f"{stop_family} / {exit_family}",
        rank=rank,
        parameters={
            "source_config_id": row.get("config_id"),
            "stop_family": stop_family,
            "stop_anchor": spec.stop_anchor,
            "exit_family": exit_family,
            "target_model": spec.target_model,
            "target_r": spec.target_r,
            "option_stop_fallback_pct": spec.option_stop_fallback_pct,
            "hard_flat_time_et": spec.hard_flat_time_et,
            "target_order_mode": spec.target_order_mode,
            "match_grade": row.get("match_grade"),
            "sample_count": _safe_int(row.get("sample_count")),
            "holdout_expectancy_r": _optional_float(row.get("holdout_expectancy_r")),
            "holdout_win_rate": _optional_float(row.get("holdout_win_rate")),
        },
    )


# Exit-profile bars are 1-minute bars; the kernel spec carries wall-clock
# seconds, so time bounds convert ×60.
_BARS_TO_SECONDS = 60

# Map the profile's giveback tier name onto the kernel's high_water_giveback
# policy enum (OFF|STRICT|MODERATE|LOOSE). They share the same vocabulary.
_GIVEBACK_TO_POLICY = {
    "OFF": "OFF",
    "STRICT": "STRICT",
    "MODERATE": "MODERATE",
    "LOOSE": "LOOSE",
}


def _exit_profile_spec_fields(strategy_name: str | None) -> dict[str, Any]:
    """Resolve a strategy's assigned exit profile into v2 ManagementPolicySpec
    fields.

    Returns the additive kwargs (initial_stop_pct, premium_disaster_stop_pct,
    target_1_r/target_2_r, target_1_quantity, no_progress_seconds,
    max_hold_seconds, high_water_giveback_policy, breakeven_after_t1, eod_flat)
    for a strategy that maps (via to_strategy_key -> PROFILE_BY_STRATEGY ->
    OPTION_PROFILES) to a profile. Returns an EMPTY dict when there is no
    strategy name or no assigned profile, so the spec is emitted exactly as
    before (pre-v2 defaults, back-compatible).
    """
    if not strategy_name:
        return {}
    profile_name = assigned_profile(to_strategy_key(strategy_name))
    if not profile_name:
        return {}
    profile = OPTION_PROFILES.get(profile_name)
    if profile is None:
        return {}

    no_progress_bars = profile.get("no_progress_bars")
    max_hold_bars = profile.get("max_hold_bars")
    return {
        # premium % managed stop -> initial protective stop
        "initial_stop_pct": profile["stop_loss_pct"],
        # operator's hard premium backstop -> disaster stop
        "premium_disaster_stop_pct": profile["disaster_stop_pct"],
        # staged R-multiple targets + partial bank at target 1
        "target_1_r": profile["target_1_r"],
        "target_2_r": profile["target_2_r"],
        "target_1_quantity": profile["target_1_quantity"],
        # time bounds: 1-minute bars -> wall-clock seconds (None stays None)
        "no_progress_seconds": (
            None if no_progress_bars is None else int(no_progress_bars) * _BARS_TO_SECONDS
        ),
        "max_hold_seconds": (
            None if max_hold_bars is None else int(max_hold_bars) * _BARS_TO_SECONDS
        ),
        # giveback tier -> high-water giveback policy enum
        "high_water_giveback_policy": _GIVEBACK_TO_POLICY[profile["giveback"]],
        # ratchet to breakeven after target 1 is banked
        "breakeven_after_t1": True,
        # flatten by the session close per the profile
        "eod_flat": profile["eod_flat"],
    }


def _management_policy_spec_from_policy(
    policy: ManagementPolicy,
    *,
    strategy_name: str | None = None,
) -> ManagementPolicySpec:
    stop_family, exit_family = _policy_families(policy)
    parameters = dict(policy.parameters)
    return _management_policy_spec(
        policy_id=policy.policy_id,
        stop_family=stop_family,
        exit_family=exit_family,
        source_config_id=_optional_str(parameters.get("source_config_id")),
        parameters=parameters,
        strategy_name=strategy_name,
    )


def _management_policy_spec(
    *,
    policy_id: str,
    stop_family: str,
    exit_family: str,
    source_config_id: str | None,
    parameters: dict[str, Any],
    strategy_name: str | None = None,
) -> ManagementPolicySpec:
    # Additive exit-profile fields (kernel contract v2). Empty when the strategy
    # has no assigned profile -> spec is identical to the pre-v2 emission.
    profile_fields = _exit_profile_spec_fields(strategy_name)
    return ManagementPolicySpec(
        policy_id=policy_id,
        stop_family=stop_family,
        stop_anchor=_stop_anchor(stop_family),
        exit_family=exit_family,
        target_model=_target_model(exit_family),
        target_r=_target_r(exit_family),
        hard_flat_time_et="15:55",
        option_stop_fallback_pct=0.45,
        target_order_mode="virtual_or_broker",
        source_config_id=source_config_id,
        parameters=parameters,
        **profile_fields,
    )


def _policy_families(policy: ManagementPolicy) -> tuple[str, str]:
    raw_stop = policy.parameters.get("stop_family")
    raw_exit = policy.parameters.get("exit_family")
    if raw_stop and raw_exit:
        return str(raw_stop), str(raw_exit)
    if "__" in policy.policy_id:
        stop_family, exit_family = policy.policy_id.split("__", 1)
        return stop_family, exit_family
    return policy.policy_id, "unknown"


def _stop_anchor(stop_family: str) -> str:
    anchors = {
        "reversal_extreme": "underlying_reversal_extreme",
        "reversal_midpoint": "underlying_reversal_midpoint",
        "immediate_entry_bar_failure": "underlying_entry_bar_failure",
    }
    return anchors.get(stop_family, f"underlying_{stop_family}")


def _target_model(exit_family: str) -> str:
    if exit_family.startswith("fixed_"):
        return "fixed_r"
    return exit_family


def _target_r(exit_family: str) -> float:
    fixed_r = {
        "fixed_1r": 1.0,
        "fixed_1_5r": 1.5,
        "fixed_2r": 2.0,
    }
    return fixed_r.get(exit_family, 0.0)


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text or None


def _surface_rows(surface_path: Path) -> list[dict[str, str]]:
    with surface_path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _row_count(surface_path: Path) -> int:
    return len(_surface_rows(surface_path))


def _rank_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float("-inf")


def _optional_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any) -> int | None:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _grade_rank(grade: str) -> int:
    return {
        "favorable": 3,
        "near_favorable": 2,
        "partial": 1,
    }.get(grade, 0)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    raw_argv = list(sys.argv[1:] if argv is None else argv)
    if raw_argv and raw_argv[0] not in {"playbook", "shadow-execution", "live-execution", "-h", "--help"}:
        raw_argv.insert(0, "playbook")
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command")

    playbook = subparsers.add_parser("playbook", help="Write the playbook packet")
    playbook.add_argument("--run-dir", type=Path, required=True)
    playbook.add_argument("--packet-root", type=Path, default=Path.cwd())
    playbook.add_argument("--packet-id", default=DEFAULT_PACKET_ID)
    playbook.add_argument("--version", type=int, default=DEFAULT_PACKET_VERSION)
    playbook.add_argument(
        "--status",
        choices=[status.value for status in PacketStatus],
        default=PacketStatus.REVIEW.value,
    )

    execution = subparsers.add_parser(
        "shadow-execution",
        help="Write the shadow execution packet from a passed playbook parity report",
    )
    execution.add_argument("--playbook-packet", type=Path, required=True)
    execution.add_argument("--parity-report", type=Path, required=True)
    execution.add_argument("--packet-root", type=Path, default=Path.cwd())
    execution.add_argument("--packet-id", default=DEFAULT_EXECUTION_PACKET_ID)
    execution.add_argument("--version", type=int, default=DEFAULT_PACKET_VERSION)
    execution.add_argument(
        "--status",
        choices=[PacketStatus.REVIEW.value, PacketStatus.APPROVED.value],
        default=PacketStatus.REVIEW.value,
    )
    execution.add_argument("--operator", default="")
    execution.add_argument("--operator-notes", default="")
    execution.add_argument("--capability-manifest-id", default=DEFAULT_CAPABILITY_MANIFEST_ID)

    live_execution = subparsers.add_parser(
        "live-execution",
        help="Write the live approval-gated execution packet from a passed playbook parity report",
    )
    live_execution.add_argument("--playbook-packet", type=Path, required=True)
    live_execution.add_argument("--parity-report", type=Path, required=True)
    live_execution.add_argument("--packet-root", type=Path, default=Path.cwd())
    live_execution.add_argument("--packet-id", default=DEFAULT_EXECUTION_PACKET_ID)
    live_execution.add_argument("--version", type=int, default=DEFAULT_LIVE_EXECUTION_PACKET_VERSION)
    live_execution.add_argument(
        "--status",
        choices=[PacketStatus.REVIEW.value, PacketStatus.APPROVED.value],
        default=PacketStatus.REVIEW.value,
    )
    live_execution.add_argument("--operator", default="")
    live_execution.add_argument("--operator-notes", default="")
    live_execution.add_argument("--capability-manifest-id", default=DEFAULT_CAPABILITY_MANIFEST_ID)
    live_execution.add_argument("--max-live-quantity", type=int, default=1)
    live_execution.add_argument("--max-trade-premium-usd", type=float, default=300.0)
    return parser.parse_args(raw_argv)


def main() -> None:
    args = _parse_args()
    if args.command in {None, "playbook"}:
        packet_path = write_mean_reversion_playbook_packet(
            args.run_dir,
            packet_root=args.packet_root,
            packet_id=args.packet_id,
            version=args.version,
            status=PacketStatus(args.status),
        )
    elif args.command == "shadow-execution":
        packet_path = write_mean_reversion_shadow_execution_packet(
            playbook_packet_path=args.playbook_packet,
            parity_report_path=args.parity_report,
            packet_root=args.packet_root,
            packet_id=args.packet_id,
            version=args.version,
            status=PacketStatus(args.status),
            operator=args.operator or None,
            operator_notes=args.operator_notes or None,
            capability_manifest_id=args.capability_manifest_id,
        )
    elif args.command == "live-execution":
        packet_path = write_mean_reversion_live_execution_packet(
            playbook_packet_path=args.playbook_packet,
            parity_report_path=args.parity_report,
            packet_root=args.packet_root,
            packet_id=args.packet_id,
            version=args.version,
            status=PacketStatus(args.status),
            operator=args.operator or None,
            operator_notes=args.operator_notes or None,
            capability_manifest_id=args.capability_manifest_id,
            max_live_quantity=args.max_live_quantity,
            max_trade_premium_usd=args.max_trade_premium_usd,
        )
    else:
        raise AssertionError(f"Unhandled command {args.command!r}")
    print(packet_path)


if __name__ == "__main__":
    main()
