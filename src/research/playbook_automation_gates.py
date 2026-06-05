"""Evaluate the playbook lane from surface map to automation readiness."""

from __future__ import annotations

import argparse
import csv
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


GATE_ORDER = (
    "p1_surface_gate",
    "p2_packet_freeze_gate",
    "p3_parity_gate",
    "p4_shadow_authorization_gate",
    "p5_shadow_feedback_gate",
    "p6_live_approval_gate",
    "p7_automation_gate",
)


@dataclass(frozen=True, slots=True)
class GateResult:
    gate: str
    status: str
    reason: str
    evidence: dict[str, Any]
    next_action: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "gate": self.gate,
            "status": self.status,
            "reason": self.reason,
            "evidence": self.evidence,
            "next_action": self.next_action,
        }


def evaluate_playbook_automation_gates(
    *,
    run_dir: Path,
    playbook_packet: Path | None = None,
    locked_validation: Path | None = None,
    parity_report: Path | None = None,
    shadow_execution_packet: Path | None = None,
    shadow_outcomes: Path | None = None,
    live_execution_packet: Path | None = None,
    execution_packet: Path | None = None,
    min_shadow_closed_trades: int = 10,
) -> dict[str, Any]:
    """Return the current promotion-gate state for a playbook automation lane."""
    run_dir = run_dir.expanduser()
    shadow_execution_packet = shadow_execution_packet or execution_packet
    live_execution_packet = live_execution_packet or execution_packet
    candidate_rows = _read_candidate_regions(run_dir)
    surface_gate = _surface_gate(candidate_rows)
    packet_gate = _packet_freeze_gate(
        playbook_packet=playbook_packet,
        locked_validation=locked_validation,
        surface_passed=surface_gate.status == "pass",
    )
    parity_gate = _parity_gate(parity_report, packet_gate.status == "pass")
    shadow_authorization_gate = _shadow_authorization_gate(
        shadow_execution_packet=shadow_execution_packet,
        parity_passed=parity_gate.status == "pass",
    )
    shadow_feedback_gate = _shadow_feedback_gate(
        shadow_outcomes=shadow_outcomes,
        shadow_authorized=shadow_authorization_gate.status == "pass",
        min_shadow_closed_trades=min_shadow_closed_trades,
    )
    live_gate = _live_approval_gate(
        live_execution_packet=live_execution_packet,
        shadow_feedback_passed=shadow_feedback_gate.status == "pass",
    )
    automation_gate = _automation_gate(
        live_gate=live_gate,
        locked_validation=locked_validation,
        shadow_outcomes=shadow_outcomes,
    )
    gates = [
        surface_gate,
        packet_gate,
        parity_gate,
        shadow_authorization_gate,
        shadow_feedback_gate,
        live_gate,
        automation_gate,
    ]
    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "run_dir": str(run_dir),
        "overall_status": _overall_status(gates),
        "next_gate": _next_gate(gates),
        "gates": [gate.as_dict() for gate in gates],
    }


def write_playbook_automation_gate_artifacts(
    report: dict[str, Any],
    *,
    out_dir: Path,
) -> tuple[Path, Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "PLAYBOOK_AUTOMATION_GATES.json"
    md_path = out_dir / "PLAYBOOK_AUTOMATION_GATES.md"
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    _write_markdown(report, md_path)
    return json_path, md_path


def _read_candidate_regions(run_dir: Path) -> list[dict[str, str]]:
    path = run_dir / "surface_review" / "candidate_regions.csv"
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _surface_gate(rows: list[dict[str, str]]) -> GateResult:
    favorable = [row for row in rows if row.get("match_grade") == "favorable"]
    near = [row for row in rows if row.get("match_grade") == "near_favorable"]
    evidence = {
        "candidate_count": len(rows),
        "favorable_count": len(favorable),
        "near_favorable_count": len(near),
    }
    if favorable:
        return GateResult(
            gate="p1_surface_gate",
            status="pass",
            reason="At least one candidate region passed strict surface gates.",
            evidence=evidence,
            next_action="Freeze one candidate into a packet for parity and shadow authorization.",
        )
    if near:
        return GateResult(
            gate="p1_surface_gate",
            status="review",
            reason="Near-favorable regions exist, but no strict favorable region passed.",
            evidence=evidence,
            next_action="Use chart review or bounded retune before locking a packet.",
        )
    return GateResult(
        gate="p1_surface_gate",
        status="block",
        reason="No favorable surface region is available for automation.",
        evidence=evidence,
        next_action="Do not create an execution packet; refine or kill the playbook surface.",
    )


def _packet_freeze_gate(
    *,
    playbook_packet: Path | None,
    locked_validation: Path | None,
    surface_passed: bool,
) -> GateResult:
    evidence = {
        "playbook_packet": str(playbook_packet) if playbook_packet else "",
        "playbook_packet_exists": bool(playbook_packet and playbook_packet.exists()),
        "locked_validation": str(locked_validation) if locked_validation else "",
        "locked_validation_exists": bool(locked_validation and locked_validation.exists()),
    }
    if not surface_passed:
        return GateResult(
            gate="p2_packet_freeze_gate",
            status="block",
            reason="Surface gate has not passed.",
            evidence=evidence,
            next_action="Resolve the surface gate first.",
        )
    if not evidence["playbook_packet_exists"]:
        return GateResult(
            gate="p2_packet_freeze_gate",
            status="block",
            reason="No locked playbook packet was provided.",
            evidence=evidence,
            next_action="Write a reviewed playbook packet before parity or shadow work.",
        )
    if not evidence["locked_validation_exists"]:
        return GateResult(
            gate="p2_packet_freeze_gate",
            status="pass",
            reason="Playbook packet exists. Locked stress evidence is missing, but it does not block shadow.",
            evidence=evidence,
            next_action="Run Mala/Bhiksha signal parity and prepare the shadow-only execution packet.",
        )
    payload = _read_json(locked_validation)
    status = str(payload.get("status", "")).lower()
    evidence["locked_validation_status"] = status
    if status == "passed":
        return GateResult(
            gate="p2_packet_freeze_gate",
            status="pass",
            reason="Playbook packet exists and optional locked stress evidence passed.",
            evidence=evidence,
            next_action="Run or attach Mala/Bhiksha signal parity.",
        )
    return GateResult(
        gate="p2_packet_freeze_gate",
        status="review",
        reason="Playbook packet exists, but optional locked stress evidence did not pass.",
        evidence=evidence,
        next_action="Shadow may continue only if the packet is explicitly marked as a scout; do not use this artifact for live promotion.",
    )


def _parity_gate(parity_report: Path | None, packet_frozen: bool) -> GateResult:
    evidence = {
        "parity_report": str(parity_report) if parity_report else "",
        "parity_report_exists": bool(parity_report and parity_report.exists()),
    }
    if not packet_frozen:
        return GateResult(
            gate="p3_parity_gate",
            status="block",
            reason="Packet freeze gate has not passed.",
            evidence=evidence,
            next_action="Resolve packet freeze before parity work.",
        )
    if not evidence["parity_report_exists"]:
        return GateResult(
            gate="p3_parity_gate",
            status="block",
            reason="No parity report was provided.",
            evidence=evidence,
            next_action="Run Mala/Bhiksha signal parity for the locked packet.",
        )
    payload = _read_json(parity_report)
    status = str(payload.get("status", "")).lower()
    missing = _as_int(payload.get("missing_event_count", payload.get("missing_count", 0)))
    extra = _as_int(payload.get("extra_event_count", payload.get("extra_count", 0)))
    evidence.update(
        {
            "parity_status": status,
            "compared_event_count": _as_int(payload.get("compared_event_count", 0)),
            "missing_event_count": missing,
            "extra_event_count": extra,
        }
    )
    if status == "passed" and missing == 0 and extra == 0:
        return GateResult(
            gate="p3_parity_gate",
            status="pass",
            reason="Mala/Bhiksha signal parity passed with no missing or extra events.",
            evidence=evidence,
            next_action="Approve or attach a shadow-only execution packet for this exact packet version.",
        )
    return GateResult(
        gate="p3_parity_gate",
        status="block",
        reason="Parity is not clean enough for execution promotion.",
        evidence=evidence,
        next_action="Fix adapter/feature/session drift before shadowing this packet.",
    )


def _shadow_authorization_gate(
    *,
    shadow_execution_packet: Path | None,
    parity_passed: bool,
) -> GateResult:
    evidence = {
        "shadow_execution_packet": str(shadow_execution_packet) if shadow_execution_packet else "",
        "shadow_execution_packet_exists": bool(
            shadow_execution_packet and shadow_execution_packet.exists()
        ),
    }
    if not parity_passed:
        return GateResult(
            gate="p4_shadow_authorization_gate",
            status="block",
            reason="Parity has not passed.",
            evidence=evidence,
            next_action="Do not shadow until parity is clean.",
        )
    if not evidence["shadow_execution_packet_exists"]:
        return GateResult(
            gate="p4_shadow_authorization_gate",
            status="review",
            reason="Parity passed, but no approved shadow-only execution packet is attached.",
            evidence=evidence,
            next_action="Write or attach the Bhiksha shadow-only execution packet.",
        )
    packet = _read_json(shadow_execution_packet)
    status = str(packet.get("status", "")).lower()
    runtime_mode = str(packet.get("runtime_mode", "")).lower()
    controls = packet.get("runtime_controls", {}) if isinstance(packet.get("runtime_controls"), dict) else {}
    shadow_only = bool(controls.get("shadow_only"))
    live_automated_allowed = bool(controls.get("live_automated_allowed"))
    evidence.update(
        {
            "packet_status": status,
            "runtime_mode": runtime_mode,
            "shadow_only": shadow_only,
            "live_automated_allowed": live_automated_allowed,
        }
    )
    if status == "approved" and runtime_mode == "shadow" and shadow_only and not live_automated_allowed:
        return GateResult(
            gate="p4_shadow_authorization_gate",
            status="pass",
            reason="Approved Bhiksha shadow-only packet is attached with live automation disabled.",
            evidence=evidence,
            next_action="Run Bhiksha shadow and collect executable option, lifecycle, and skip evidence.",
        )
    return GateResult(
        gate="p4_shadow_authorization_gate",
        status="block",
        reason="Execution packet is not a clean approved shadow-only packet.",
        evidence=evidence,
        next_action="Use a packet with runtime_mode=shadow, shadow_only=true, approved status, and live automation disabled.",
    )


def _shadow_feedback_gate(
    *,
    shadow_outcomes: Path | None,
    shadow_authorized: bool,
    min_shadow_closed_trades: int,
) -> GateResult:
    evidence = {
        "shadow_outcomes": str(shadow_outcomes) if shadow_outcomes else "",
        "shadow_outcomes_exists": bool(shadow_outcomes and shadow_outcomes.exists()),
        "min_shadow_closed_trades": min_shadow_closed_trades,
    }
    if not shadow_authorized:
        return GateResult(
            gate="p5_shadow_feedback_gate",
            status="block",
            reason="Shadow authorization has not passed.",
            evidence=evidence,
            next_action="Resolve shadow authorization before judging feedback.",
        )
    if not evidence["shadow_outcomes_exists"]:
        return GateResult(
            gate="p5_shadow_feedback_gate",
            status="review",
            reason="Shadow is authorized, but no closed shadow outcome evidence is attached yet.",
            evidence=evidence,
            next_action="Run shadow and collect option fill, PnL, lifecycle, and skip evidence.",
        )
    rows = _read_csv(shadow_outcomes)
    closed = [row for row in rows if str(row.get("status", "")).lower() in {"closed", "filled_closed"}]
    option_rs = [_as_float(row.get("option_r")) for row in closed]
    option_rs = [value for value in option_rs if value is not None]
    defects = [
        row
        for row in rows
        if str(row.get("runtime_defect", "")).strip().lower() not in {"", "false", "none", "0"}
    ]
    avg_option_r = sum(option_rs) / len(option_rs) if option_rs else 0.0
    evidence.update(
        {
            "row_count": len(rows),
            "closed_trade_count": len(closed),
            "avg_option_r": round(avg_option_r, 6),
            "runtime_defect_count": len(defects),
        }
    )
    if len(closed) < min_shadow_closed_trades:
        return GateResult(
            gate="p5_shadow_feedback_gate",
            status="review",
            reason="Shadow evidence exists, but sample is too small for live review.",
            evidence=evidence,
            next_action="Continue shadow or kill for no-signal if the row remains idle for the review window.",
        )
    if defects:
        return GateResult(
            gate="p5_shadow_feedback_gate",
            status="block",
            reason="Runtime defects remain in shadow evidence.",
            evidence=evidence,
            next_action="Fix lifecycle/provider/broker defects before judging the playbook.",
        )
    if avg_option_r <= 0:
        return GateResult(
            gate="p5_shadow_feedback_gate",
            status="block",
            reason="Closed shadow trades do not show positive executable option R.",
            evidence=evidence,
            next_action="Retune or kill; do not promote the packet.",
        )
    return GateResult(
        gate="p5_shadow_feedback_gate",
        status="pass",
        reason="Shadow evidence is positive and runtime-clean.",
        evidence=evidence,
        next_action="Create or review a live approval-gated execution packet.",
    )


def _live_approval_gate(
    *,
    live_execution_packet: Path | None,
    shadow_feedback_passed: bool,
) -> GateResult:
    evidence = {
        "live_execution_packet": str(live_execution_packet) if live_execution_packet else "",
        "live_execution_packet_exists": bool(live_execution_packet and live_execution_packet.exists()),
    }
    if not shadow_feedback_passed:
        return GateResult(
            gate="p6_live_approval_gate",
            status="block",
            reason="Shadow feedback gate has not passed.",
            evidence=evidence,
            next_action="Do not create live approval until shadow evidence is clean.",
        )
    if not evidence["live_execution_packet_exists"]:
        return GateResult(
            gate="p6_live_approval_gate",
            status="review",
            reason="Shadow passed, but no live approval-gated execution packet is attached.",
            evidence=evidence,
            next_action="Draft a live approval-gated execution packet with explicit controls.",
        )
    packet = _read_json(live_execution_packet)
    status = str(packet.get("status", "")).lower()
    runtime_mode = str(packet.get("runtime_mode", "")).lower()
    controls = packet.get("runtime_controls", {}) if isinstance(packet.get("runtime_controls"), dict) else {}
    live_ticket_required = bool(controls.get("live_ticket_required"))
    live_automated_allowed = bool(controls.get("live_automated_allowed"))
    evidence.update(
        {
            "packet_status": status,
            "runtime_mode": runtime_mode,
            "live_ticket_required": live_ticket_required,
            "live_automated_allowed": live_automated_allowed,
        }
    )
    if (
        status == "approved"
        and runtime_mode == "live_approval_gated"
        and live_ticket_required
        and not live_automated_allowed
    ):
        return GateResult(
            gate="p6_live_approval_gate",
            status="pass",
            reason="Live approval-gated packet is approved with live automation still disabled.",
            evidence=evidence,
            next_action="Run approval-gated pilot and collect managed lifecycle evidence.",
        )
    return GateResult(
        gate="p6_live_approval_gate",
        status="block",
        reason="Execution packet is not a clean live approval-gated packet.",
        evidence=evidence,
        next_action="Require approval, live tickets, and live automation disabled for the pilot.",
    )


def _automation_gate(
    *,
    live_gate: GateResult,
    locked_validation: Path | None,
    shadow_outcomes: Path | None,
) -> GateResult:
    return GateResult(
        gate="p7_automation_gate",
        status="block",
        reason=(
            "Full automation is intentionally blocked until approval-gated live pilot feedback "
            "is ingested and a separate autonomous-control packet is approved."
        ),
        evidence={
            "p6_live_approval_gate_status": live_gate.status,
            "locked_stress_artifact": str(locked_validation) if locked_validation else "",
            "locked_stress_artifact_exists": bool(locked_validation and locked_validation.exists()),
            "shadow_outcomes": str(shadow_outcomes) if shadow_outcomes else "",
            "shadow_outcomes_exists": bool(shadow_outcomes and shadow_outcomes.exists()),
            "requires_feedback_ingestion": True,
            "requires_autonomous_control_packet": True,
        },
        next_action="Ingest live/shadow feedback into Mala before any autonomous-live decision.",
    )


def _overall_status(gates: list[GateResult]) -> str:
    first_open = next((gate for gate in gates if gate.status != "pass"), None)
    if first_open is None:
        return "automation_ready"
    if first_open.gate == "p5_shadow_feedback_gate" and first_open.status == "review":
        return "shadow_ready"
    if first_open.gate == "p7_automation_gate":
        return "automation_blocked"
    if first_open.status == "block":
        return "blocked"
    return "in_review"


def _next_gate(gates: list[GateResult]) -> str:
    for gate in gates:
        if gate.status != "pass":
            return gate.gate
    return gates[-1].gate


def _read_json(path: Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def _read_csv(path: Path | None) -> list[dict[str, str]]:
    if path is None or not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _as_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_int(value: Any) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def _write_markdown(report: dict[str, Any], path: Path) -> None:
    lines = [
        "# Playbook Automation Gates",
        "",
        f"- generated_at: `{report.get('generated_at', '')}`",
        f"- run_dir: `{report.get('run_dir', '')}`",
        f"- overall_status: `{report.get('overall_status', '')}`",
        f"- next_gate: `{report.get('next_gate', '')}`",
        "",
        "## Gates",
        "",
    ]
    for gate in report.get("gates", []):
        lines.extend(
            [
                f"### {gate.get('gate')}",
                "",
                f"- status: `{gate.get('status')}`",
                f"- reason: {gate.get('reason')}",
                f"- next_action: {gate.get('next_action')}",
                f"- evidence: `{json.dumps(gate.get('evidence', {}), sort_keys=True)}`",
                "",
            ]
        )
    path.write_text("\n".join(lines), encoding="utf-8")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--playbook-packet", type=Path, default=None)
    parser.add_argument("--locked-validation", type=Path, default=None)
    parser.add_argument("--parity-report", type=Path, default=None)
    parser.add_argument("--shadow-execution-packet", type=Path, default=None)
    parser.add_argument("--shadow-outcomes", type=Path, default=None)
    parser.add_argument(
        "--execution-packet",
        type=Path,
        default=None,
        help="Backward-compatible alias for --shadow-execution-packet.",
    )
    parser.add_argument("--live-execution-packet", type=Path, default=None)
    parser.add_argument("--min-shadow-closed-trades", type=int, default=10)
    parser.add_argument("--out-dir", type=Path, default=None)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    report = evaluate_playbook_automation_gates(
        run_dir=args.run_dir,
        playbook_packet=args.playbook_packet,
        locked_validation=args.locked_validation,
        parity_report=args.parity_report,
        shadow_execution_packet=args.shadow_execution_packet,
        shadow_outcomes=args.shadow_outcomes,
        live_execution_packet=args.live_execution_packet,
        execution_packet=args.execution_packet,
        min_shadow_closed_trades=args.min_shadow_closed_trades,
    )
    out_dir = args.out_dir or args.run_dir / "automation_gates"
    json_path, md_path = write_playbook_automation_gate_artifacts(report, out_dir=out_dir)
    print(f"OVERALL_STATUS={report['overall_status']}")
    print(f"NEXT_GATE={report['next_gate']}")
    print(f"JSON={json_path}")
    print(f"MARKDOWN={md_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
