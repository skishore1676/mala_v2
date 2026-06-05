from __future__ import annotations

import csv
import json
from pathlib import Path

from src.research.playbook_evidence_v2 import write_playbook_evidence_v2


def _write_json(path: Path, payload: dict) -> Path:
    path.write_text(json.dumps(payload) + "\n", encoding="utf-8")
    return path


def test_playbook_evidence_v2_writes_shadow_ready_passport(tmp_path: Path) -> None:
    review_dir = tmp_path / "surface_review"
    review_dir.mkdir()
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
                "calibration_expectancy_r",
                "holdout_expectancy_r",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "config_id": "f09fcdd6b5",
                "symbol": "IWM",
                "direction": "short",
                "match_grade": "favorable",
                "sample_count": "114",
                "holdout_count": "22",
                "calibration_expectancy_r": "0.1997",
                "holdout_expectancy_r": "0.1818",
            }
        )
    gate_report = _write_json(
        tmp_path / "gates.json",
        {
            "overall_status": "shadow_ready",
            "next_gate": "p5_shadow_feedback_gate",
            "gates": [
                {
                    "gate": "p1_surface_gate",
                    "status": "pass",
                    "evidence": {"candidate_count": 16, "favorable_count": 1},
                },
                {
                    "gate": "p3_parity_gate",
                    "status": "pass",
                    "evidence": {
                        "parity_report": "PARITY_REPORT.json",
                        "parity_status": "passed",
                    },
                },
                {
                    "gate": "p5_shadow_feedback_gate",
                    "status": "review",
                    "reason": "Shadow is authorized.",
                    "next_action": "Run shadow.",
                    "evidence": {"shadow_outcomes": ""},
                },
            ],
        },
    )
    playbook_packet = _write_json(
        tmp_path / "playbook.json",
        {
            "packet_id": "playbook.mean_reversion_at_extremes.iwm_qqq",
            "version": 1,
            "playbook_id": "mean-reversion-at-extremes-intraday",
            "symbol_scope": ["IWM", "QQQ"],
            "metadata": {"exploration_universe": "iwm_qqq"},
            "management_policies": [{"policy_id": "reversal_extreme__fixed_1r"}],
        },
    )
    shadow_packet = _write_json(
        tmp_path / "shadow.json",
        {"packet_id": "execution.mean_reversion_at_extremes.iwm_qqq", "version": 1},
    )
    live_packet = _write_json(
        tmp_path / "live.json",
        {"packet_id": "execution.mean_reversion_at_extremes.iwm_qqq", "version": 2},
    )

    csv_path, json_path = write_playbook_evidence_v2(
        run_dir=tmp_path,
        gate_report=gate_report,
        playbook_packet=playbook_packet,
        shadow_execution_packet=shadow_packet,
        live_execution_packet=live_packet,
    )

    rows = list(csv.DictReader(csv_path.open(newline="", encoding="utf-8")))
    assert json_path.exists()
    assert rows[0]["promotion_verdict"] == "shadow"
    assert rows[0]["bhiksha_shadow_status"] == "not_started"
    assert rows[0]["primary_candidate_config_id"] == "f09fcdd6b5"
    assert rows[0]["management_policy_ids"] == '["reversal_extreme__fixed_1r"]'
