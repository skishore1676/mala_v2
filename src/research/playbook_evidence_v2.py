"""Write Mala_Playbook_Evidence_v2 rows for playbook packet adoption."""

from __future__ import annotations

import argparse
import csv
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


EVIDENCE_VERSION = "2"
CSV_NAME = "Mala_Playbook_Evidence_v2.csv"
JSON_NAME = "Mala_Playbook_Evidence_v2.json"

FIELDNAMES = [
    "mala_playbook_evidence_version",
    "playbook_id",
    "exploration_universe",
    "playbook_packet_id",
    "playbook_packet_version",
    "execution_packet_id",
    "shadow_execution_packet_version",
    "live_execution_packet_version",
    "symbol_scope",
    "direction_scope",
    "primary_candidate_config_id",
    "surface_match_grade",
    "surface_candidate_count",
    "surface_favorable_count",
    "sample_count",
    "holdout_count",
    "calibration_expectancy_r",
    "holdout_expectancy_r",
    "management_policy_ids",
    "parity_report",
    "parity_status",
    "p_gate_status_json",
    "bhiksha_shadow_status",
    "shadow_closed_count",
    "shadow_avg_option_r",
    "shadow_runtime_defect_count",
    "shadow_feedback_artifact",
    "promotion_verdict",
    "promotion_reason",
    "next_action",
    "updated_at",
]


def write_playbook_evidence_v2(
    *,
    run_dir: Path,
    gate_report: Path | None = None,
    playbook_packet: Path | None = None,
    shadow_execution_packet: Path | None = None,
    live_execution_packet: Path | None = None,
    out_dir: Path | None = None,
) -> tuple[Path, Path]:
    run_dir = run_dir.expanduser()
    gate_report = gate_report or run_dir / "automation_gates" / "PLAYBOOK_AUTOMATION_GATES.json"
    out_dir = out_dir or run_dir / "playbook_evidence_v2"
    out_dir.mkdir(parents=True, exist_ok=True)

    gate_payload = _read_json(gate_report)
    playbook_payload = _read_json(playbook_packet)
    shadow_payload = _read_json(shadow_execution_packet)
    live_payload = _read_json(live_execution_packet)
    candidate = _primary_candidate(run_dir)
    gates = gate_payload.get("gates", []) if isinstance(gate_payload.get("gates"), list) else []
    gate_statuses = {
        str(gate.get("gate", "")): str(gate.get("status", ""))
        for gate in gates
        if isinstance(gate, dict)
    }
    surface_gate = next(
        (gate for gate in gates if isinstance(gate, dict) and gate.get("gate") == "p1_surface_gate"),
        {},
    )
    shadow_gate = next(
        (gate for gate in gates if isinstance(gate, dict) and gate.get("gate") == "p5_shadow_feedback_gate"),
        {},
    )
    parity_gate = next(
        (gate for gate in gates if isinstance(gate, dict) and gate.get("gate") == "p3_parity_gate"),
        {},
    )
    surface_evidence = surface_gate.get("evidence", {}) if isinstance(surface_gate, dict) else {}
    shadow_evidence = shadow_gate.get("evidence", {}) if isinstance(shadow_gate, dict) else {}
    parity_evidence = parity_gate.get("evidence", {}) if isinstance(parity_gate, dict) else {}

    row = {
        "mala_playbook_evidence_version": EVIDENCE_VERSION,
        "playbook_id": _first(playbook_payload.get("playbook_id"), playbook_payload.get("metadata", {}).get("playbook_scope")),
        "exploration_universe": playbook_payload.get("metadata", {}).get("exploration_universe", ""),
        "playbook_packet_id": playbook_payload.get("packet_id", ""),
        "playbook_packet_version": playbook_payload.get("version", ""),
        "execution_packet_id": _first(shadow_payload.get("packet_id"), live_payload.get("packet_id")),
        "shadow_execution_packet_version": shadow_payload.get("version", ""),
        "live_execution_packet_version": live_payload.get("version", ""),
        "symbol_scope": _json_list(playbook_payload.get("symbol_scope", [])),
        "direction_scope": candidate.get("direction", ""),
        "primary_candidate_config_id": candidate.get("config_id", ""),
        "surface_match_grade": candidate.get("match_grade", ""),
        "surface_candidate_count": surface_evidence.get("candidate_count", ""),
        "surface_favorable_count": surface_evidence.get("favorable_count", ""),
        "sample_count": candidate.get("sample_count", ""),
        "holdout_count": candidate.get("holdout_count", ""),
        "calibration_expectancy_r": candidate.get("calibration_expectancy_r", ""),
        "holdout_expectancy_r": candidate.get("holdout_expectancy_r", ""),
        "management_policy_ids": _json_list(_management_policy_ids(playbook_payload)),
        "parity_report": parity_evidence.get("parity_report", ""),
        "parity_status": parity_evidence.get("parity_status", ""),
        "p_gate_status_json": json.dumps(gate_statuses, sort_keys=True),
        "bhiksha_shadow_status": _shadow_status(gate_payload, shadow_gate),
        "shadow_closed_count": shadow_evidence.get("closed_trade_count", ""),
        "shadow_avg_option_r": shadow_evidence.get("avg_option_r", ""),
        "shadow_runtime_defect_count": shadow_evidence.get("runtime_defect_count", ""),
        "shadow_feedback_artifact": shadow_evidence.get("shadow_outcomes", ""),
        "promotion_verdict": _promotion_verdict(gate_payload),
        "promotion_reason": _promotion_reason(gate_payload, gates),
        "next_action": _next_action(gate_payload, gates),
        "updated_at": datetime.now(UTC).isoformat(),
    }
    rows = [row]

    csv_path = out_dir / CSV_NAME
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)

    json_path = out_dir / JSON_NAME
    json_path.write_text(json.dumps({"rows": rows}, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return csv_path, json_path


def _primary_candidate(run_dir: Path) -> dict[str, str]:
    path = run_dir / "surface_review" / "candidate_regions.csv"
    if not path.exists():
        return {}
    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    for grade in ("favorable", "near_favorable", "partial"):
        for row in rows:
            if row.get("match_grade") == grade:
                return row
    return rows[0] if rows else {}


def _management_policy_ids(packet: dict[str, Any]) -> list[str]:
    policies = packet.get("management_policies", [])
    if not isinstance(policies, list):
        return []
    return [
        str(policy.get("policy_id"))
        for policy in policies
        if isinstance(policy, dict) and policy.get("policy_id")
    ]


def _shadow_status(gate_report: dict[str, Any], shadow_gate: dict[str, Any]) -> str:
    overall = str(gate_report.get("overall_status", ""))
    status = str(shadow_gate.get("status", ""))
    if overall == "shadow_ready" and status == "review":
        return "not_started"
    if status == "pass":
        return "pass"
    if status == "block":
        return "block"
    if status == "review":
        return "sample_small"
    return ""


def _promotion_verdict(gate_report: dict[str, Any]) -> str:
    overall = str(gate_report.get("overall_status", ""))
    next_gate = str(gate_report.get("next_gate", ""))
    if overall == "shadow_ready":
        return "shadow"
    if overall == "automation_blocked":
        return "autonomy_blocked"
    if next_gate == "p6_live_approval_gate":
        return "promote_review"
    if next_gate == "p5_shadow_feedback_gate":
        return "shadow"
    if overall == "blocked":
        return "retune_or_kill"
    return "review"


def _promotion_reason(gate_report: dict[str, Any], gates: list[Any]) -> str:
    next_gate = str(gate_report.get("next_gate", ""))
    for gate in gates:
        if isinstance(gate, dict) and gate.get("gate") == next_gate:
            return str(gate.get("reason", ""))
    return ""


def _next_action(gate_report: dict[str, Any], gates: list[Any]) -> str:
    next_gate = str(gate_report.get("next_gate", ""))
    for gate in gates:
        if isinstance(gate, dict) and gate.get("gate") == next_gate:
            return str(gate.get("next_action", ""))
    return ""


def _read_json(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def _json_list(values: Any) -> str:
    if not isinstance(values, list):
        values = []
    return json.dumps(values, separators=(",", ":"))


def _first(*values: Any) -> Any:
    for value in values:
        if value not in (None, ""):
            return value
    return ""


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--gate-report", type=Path, default=None)
    parser.add_argument("--playbook-packet", type=Path, default=None)
    parser.add_argument("--shadow-execution-packet", type=Path, default=None)
    parser.add_argument("--live-execution-packet", type=Path, default=None)
    parser.add_argument("--out-dir", type=Path, default=None)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    csv_path, json_path = write_playbook_evidence_v2(
        run_dir=args.run_dir,
        gate_report=args.gate_report,
        playbook_packet=args.playbook_packet,
        shadow_execution_packet=args.shadow_execution_packet,
        live_execution_packet=args.live_execution_packet,
        out_dir=args.out_dir,
    )
    print(f"CSV={csv_path}")
    print(f"JSON={json_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
