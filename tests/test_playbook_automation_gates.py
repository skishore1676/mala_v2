from __future__ import annotations

import csv
import json
from pathlib import Path

from src.research.playbook_automation_gates import (
    evaluate_playbook_automation_gates,
    write_playbook_automation_gate_artifacts,
)


def _write_candidate_regions(run_dir: Path, *, grade: str = "favorable") -> None:
    review_dir = run_dir / "surface_review"
    review_dir.mkdir(parents=True)
    with (review_dir / "candidate_regions.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "config_id",
                "symbol",
                "direction",
                "match_grade",
                "sample_count",
                "holdout_count",
                "criteria_failed_count",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "config_id": "locked-candidate",
                "symbol": "IWM",
                "direction": "long",
                "match_grade": grade,
                "sample_count": "80",
                "holdout_count": "30",
                "criteria_failed_count": "0" if grade == "favorable" else "1",
            }
        )


def _write_json(path: Path, payload: dict) -> Path:
    path.write_text(json.dumps(payload) + "\n", encoding="utf-8")
    return path


def _write_shadow_outcomes(path: Path, option_rs: list[float], *, defect: bool = False) -> Path:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["status", "option_r", "runtime_defect"])
        writer.writeheader()
        for value in option_rs:
            writer.writerow(
                {
                    "status": "closed",
                    "option_r": value,
                    "runtime_defect": "lifecycle_error" if defect else "",
                }
            )
    return path


def _write_execution_packet(
    path: Path,
    *,
    runtime_mode: str = "shadow",
    shadow_only: bool = True,
    live_ticket_required: bool = False,
) -> Path:
    return _write_json(
        path,
        {
            "status": "approved",
            "runtime_mode": runtime_mode,
            "runtime_controls": {
                "shadow_only": shadow_only,
                "live_automated_allowed": False,
                "live_ticket_required": live_ticket_required,
            },
        },
    )


def test_playbook_automation_blocks_without_surface_candidates(tmp_path: Path) -> None:
    report = evaluate_playbook_automation_gates(run_dir=tmp_path)

    assert report["overall_status"] == "blocked"
    assert report["next_gate"] == "p1_surface_gate"
    assert report["gates"][0]["status"] == "block"


def test_playbook_automation_opens_shadow_without_locked_validation(tmp_path: Path) -> None:
    _write_candidate_regions(tmp_path)
    playbook_packet = _write_json(tmp_path / "playbook.json", {"status": "review"})
    parity = _write_json(
        tmp_path / "parity.json",
        {
            "status": "passed",
            "compared_event_count": 21127,
            "missing_event_count": 0,
            "extra_event_count": 0,
        },
    )
    shadow_packet = _write_execution_packet(tmp_path / "shadow_execution.json")

    report = evaluate_playbook_automation_gates(
        run_dir=tmp_path,
        playbook_packet=playbook_packet,
        parity_report=parity,
        shadow_execution_packet=shadow_packet,
    )

    statuses = {gate["gate"]: gate["status"] for gate in report["gates"]}
    assert statuses["p1_surface_gate"] == "pass"
    assert statuses["p2_packet_freeze_gate"] == "pass"
    assert statuses["p3_parity_gate"] == "pass"
    assert statuses["p4_shadow_authorization_gate"] == "pass"
    assert statuses["p5_shadow_feedback_gate"] == "review"
    assert report["overall_status"] == "shadow_ready"
    assert report["next_gate"] == "p5_shadow_feedback_gate"


def test_playbook_automation_blocks_live_on_negative_shadow_option_r(tmp_path: Path) -> None:
    _write_candidate_regions(tmp_path)
    playbook_packet = _write_json(tmp_path / "playbook.json", {"status": "review"})
    parity = _write_json(
        tmp_path / "parity.json",
        {"status": "passed", "missing_event_count": 0, "extra_event_count": 0},
    )
    shadow_packet = _write_execution_packet(tmp_path / "shadow_execution.json")
    shadow = _write_shadow_outcomes(tmp_path / "shadow.csv", [-0.1] * 12)

    report = evaluate_playbook_automation_gates(
        run_dir=tmp_path,
        playbook_packet=playbook_packet,
        parity_report=parity,
        shadow_execution_packet=shadow_packet,
        shadow_outcomes=shadow,
    )

    statuses = {gate["gate"]: gate["status"] for gate in report["gates"]}
    assert statuses["p5_shadow_feedback_gate"] == "block"
    assert statuses["p6_live_approval_gate"] == "block"
    assert report["next_gate"] == "p5_shadow_feedback_gate"


def test_playbook_automation_allows_live_approval_but_blocks_full_auto(tmp_path: Path) -> None:
    _write_candidate_regions(tmp_path)
    playbook_packet = _write_json(tmp_path / "playbook.json", {"status": "review"})
    parity = _write_json(
        tmp_path / "parity.json",
        {"status": "passed", "missing_event_count": 0, "extra_event_count": 0},
    )
    shadow_packet = _write_execution_packet(tmp_path / "shadow_execution.json")
    shadow = _write_shadow_outcomes(tmp_path / "shadow.csv", [0.2] * 12)
    live_execution = _write_execution_packet(
        tmp_path / "live_execution.json",
        runtime_mode="live_approval_gated",
        shadow_only=False,
        live_ticket_required=True,
    )

    report = evaluate_playbook_automation_gates(
        run_dir=tmp_path,
        playbook_packet=playbook_packet,
        parity_report=parity,
        shadow_execution_packet=shadow_packet,
        shadow_outcomes=shadow,
        live_execution_packet=live_execution,
    )
    json_path, md_path = write_playbook_automation_gate_artifacts(
        report,
        out_dir=tmp_path / "automation_gates",
    )

    statuses = {gate["gate"]: gate["status"] for gate in report["gates"]}
    assert statuses["p5_shadow_feedback_gate"] == "pass"
    assert statuses["p6_live_approval_gate"] == "pass"
    assert statuses["p7_automation_gate"] == "block"
    assert report["overall_status"] == "automation_blocked"
    assert report["next_gate"] == "p7_automation_gate"
    assert json_path.exists()
    assert "Playbook Automation Gates" in md_path.read_text(encoding="utf-8")
