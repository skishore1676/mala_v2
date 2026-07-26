from __future__ import annotations

from copy import deepcopy

from src.research.strategy_discovery_intake import (
    RECEIPT_SCHEMA,
    validate_strategy_discovery_intake,
)


EVIDENCE_ID = "sha256:" + ("a" * 64)


def _intake() -> dict:
    return {
        "schema": "mala.hypothesis_intake.v2",
        "hypothesis_id": "mala-hypothesis:fixture",
        "source_lead_id": "lead:fixture",
        "thesis": "A source-backed explicit rule can be tested historically.",
        "counter_thesis": "The apparent effect may disappear after costs and holdout.",
        "universe": "SPX",
        "direction": "long",
        "entry_rule": "Enter after the explicit source condition.",
        "exit_rule": "Exit at the source-defined invalidation.",
        "holding_period": "one session",
        "regime": "source-defined regime",
        "data_requirements": {
            "required_fields": ["timestamp", "close", "volume"],
            "known_missing_fields": [],
            "feasibility": "known",
        },
        "m1_admission": {
            "required_tests": ["chronological split", "cost stress"],
            "admit_criteria": "Every variable maps to a point-in-time field.",
        },
        "novelty_link": {
            "catalog_id": "unmatched:lead:fixture",
            "relationship": "new",
        },
        "compute_class": "mala_heavy",
        "requires_mac_air": True,
        "status": "proposed",
        "evidence_ids": [EVIDENCE_ID],
        "source_provenance": f"tradelab:origin:{EVIDENCE_ID}",
        "model_receipt_id": "agent-run:fixture",
        "created_at": "2026-07-26T18:00:00Z",
    }


def test_accepts_valid_proposal_without_effects() -> None:
    receipt = validate_strategy_discovery_intake(
        _intake(),
        evidence_status_by_id={EVIDENCE_ID: "available"},
        compute_platform="mac_air",
    )
    assert receipt["schema"] == RECEIPT_SCHEMA
    assert receipt["accepted"] is True
    assert receipt["result"] == "accepted"
    assert not any(receipt["effects"].values())


def test_rejects_stale_evidence() -> None:
    receipt = validate_strategy_discovery_intake(
        _intake(),
        evidence_status_by_id={EVIDENCE_ID: "stale"},
        compute_platform="mac_air",
    )
    assert receipt["accepted"] is False
    assert receipt["result"] == "needs_evidence"
    assert any(issue.startswith("evidence_not_available:") for issue in receipt["issues"])


def test_rejects_oldmac_heavy_compute() -> None:
    receipt = validate_strategy_discovery_intake(
        _intake(),
        evidence_status_by_id={EVIDENCE_ID: "available"},
        compute_platform="oldmac",
    )
    assert receipt["accepted"] is False
    assert "heavy_compute_platform_must_be_mac_air" in receipt["issues"]


def test_rejects_unknown_and_model_authority_fields() -> None:
    intake = deepcopy(_intake())
    intake["profitability_score"] = 0.9
    intake["option_legs"] = [{"kind": "call"}]
    receipt = validate_strategy_discovery_intake(
        intake,
        evidence_status_by_id={EVIDENCE_ID: "available"},
        compute_platform="mac_air",
    )
    assert receipt["accepted"] is False
    assert "$.profitability_score:forbidden_model_authority" in receipt["issues"]
    assert "$.option_legs:forbidden_model_authority" in receipt["issues"]


def test_needs_evidence_status_is_preserved_without_starting_research() -> None:
    intake = deepcopy(_intake())
    intake["status"] = "needs_evidence"
    intake["data_requirements"]["feasibility"] = "missing"
    receipt = validate_strategy_discovery_intake(
        intake,
        evidence_status_by_id={EVIDENCE_ID: "available"},
        compute_platform="mac_air",
    )
    assert receipt["accepted"] is False
    assert receipt["result"] == "needs_evidence"
    assert not any(receipt["effects"].values())


def test_rejects_malformed_nested_contract_fields() -> None:
    intake = deepcopy(_intake())
    intake["data_requirements"]["required_fields"] = []
    intake["m1_admission"]["required_tests"] = []
    intake["novelty_link"]["relationship"] = "probably_new"
    intake["created_at"] = "yesterday"
    receipt = validate_strategy_discovery_intake(
        intake,
        evidence_status_by_id={EVIDENCE_ID: "available"},
        compute_platform="mac_air",
    )
    assert receipt["accepted"] is False
    assert "invalid_required_fields" in receipt["issues"]
    assert "invalid_required_tests" in receipt["issues"]
    assert "invalid_novelty_relationship" in receipt["issues"]
    assert "invalid_datetime:created_at" in receipt["issues"]
