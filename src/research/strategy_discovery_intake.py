"""Application-owned validation for broker-inert strategy-discovery proposals."""
from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime
from typing import Any, Mapping


INTAKE_SCHEMA = "mala.hypothesis_intake.v2"
RECEIPT_SCHEMA = "mala.strategy_discovery_admission_receipt.v1"
_EVIDENCE_ID = re.compile(r"^sha256:[a-f0-9]{64}$")
_REQUIRED = {
    "schema",
    "hypothesis_id",
    "source_lead_id",
    "thesis",
    "counter_thesis",
    "universe",
    "direction",
    "entry_rule",
    "exit_rule",
    "holding_period",
    "regime",
    "data_requirements",
    "m1_admission",
    "novelty_link",
    "compute_class",
    "requires_mac_air",
    "status",
    "evidence_ids",
    "source_provenance",
    "model_receipt_id",
    "created_at",
}
_FORBIDDEN_FIELDS = {
    "option_legs",
    "mala_gate_result",
    "m1_gate",
    "m1_gate_result",
    "profitability",
    "profitability_score",
    "expected_profit",
    "requested_effect",
    "requested_effects",
}


def _hash(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


def _find_forbidden(value: Any, path: str = "$") -> list[str]:
    issues: list[str] = []
    if isinstance(value, dict):
        for key, child in value.items():
            if key in _FORBIDDEN_FIELDS:
                issues.append(f"{path}.{key}:forbidden_model_authority")
            issues.extend(_find_forbidden(child, f"{path}.{key}"))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            issues.extend(_find_forbidden(child, f"{path}[{index}]"))
    return issues


def _valid_string(value: Any, minimum: int = 1, maximum: int | None = None) -> bool:
    return (
        isinstance(value, str)
        and len(value) >= minimum
        and (maximum is None or len(value) <= maximum)
    )


def _valid_string_list(
    value: Any,
    *,
    minimum_items: int = 0,
    minimum_length: int = 1,
) -> bool:
    return (
        isinstance(value, list)
        and len(value) >= minimum_items
        and all(_valid_string(item, minimum_length) for item in value)
    )


def _valid_datetime(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    return True


def validate_strategy_discovery_intake(
    payload: Mapping[str, Any],
    *,
    evidence_status_by_id: Mapping[str, str],
    compute_platform: str,
) -> dict[str, Any]:
    """Return a pure validation receipt; never create or execute a hypothesis."""
    issues = _find_forbidden(payload)
    keys = set(payload)
    for key in sorted(_REQUIRED - keys):
        issues.append(f"missing_required:{key}")
    for key in sorted(keys - _REQUIRED):
        issues.append(f"unknown_field:{key}")
    if payload.get("schema") != INTAKE_SCHEMA:
        issues.append("invalid_schema")
    if payload.get("direction") not in {"long", "short", "both"}:
        issues.append("invalid_direction")
    if payload.get("compute_class") != "mala_heavy":
        issues.append("invalid_compute_class")
    if payload.get("requires_mac_air") is not True:
        issues.append("requires_mac_air_must_be_true")
    if compute_platform != "mac_air":
        issues.append("heavy_compute_platform_must_be_mac_air")
    if payload.get("status") not in {"proposed", "needs_evidence", "rejected"}:
        issues.append("invalid_status")

    string_limits = {
        "hypothesis_id": (8, None),
        "source_lead_id": (6, None),
        "thesis": (20, 1200),
        "counter_thesis": (20, 1200),
        "universe": (2, 200),
        "entry_rule": (5, 800),
        "exit_rule": (5, 800),
        "holding_period": (3, 240),
        "regime": (3, 300),
        "source_provenance": (12, 2000),
        "model_receipt_id": (4, None),
    }
    for key, (minimum, maximum) in string_limits.items():
        if not _valid_string(payload.get(key), minimum, maximum):
            issues.append(f"invalid_string:{key}")
    if not _valid_datetime(payload.get("created_at")):
        issues.append("invalid_datetime:created_at")

    evidence_ids = payload.get("evidence_ids")
    if not isinstance(evidence_ids, list) or not evidence_ids:
        issues.append("evidence_ids_required")
        evidence_ids = []
    elif len(evidence_ids) != len(set(evidence_ids)):
        issues.append("duplicate_evidence_ids")
    for evidence_id in evidence_ids:
        if not isinstance(evidence_id, str) or not _EVIDENCE_ID.fullmatch(evidence_id):
            issues.append(f"invalid_evidence_id:{evidence_id}")
            continue
        status = evidence_status_by_id.get(evidence_id)
        if status != "available":
            issues.append(f"evidence_not_available:{evidence_id}:{status or 'missing'}")

    requirements = payload.get("data_requirements")
    if not isinstance(requirements, dict) or set(requirements) != {
        "required_fields",
        "known_missing_fields",
        "feasibility",
    }:
        issues.append("invalid_data_requirements")
    else:
        if not _valid_string_list(
            requirements.get("required_fields"),
            minimum_items=1,
            minimum_length=3,
        ):
            issues.append("invalid_required_fields")
        if not _valid_string_list(
            requirements.get("known_missing_fields"),
            minimum_length=3,
        ):
            issues.append("invalid_known_missing_fields")
        if requirements.get("feasibility") not in {"known", "plausible", "missing"}:
            issues.append("invalid_data_feasibility")

    admission = payload.get("m1_admission")
    if not isinstance(admission, dict) or set(admission) != {
        "required_tests",
        "admit_criteria",
    }:
        issues.append("invalid_m1_admission")
    else:
        if not _valid_string_list(
            admission.get("required_tests"),
            minimum_items=1,
            minimum_length=3,
        ):
            issues.append("invalid_required_tests")
        if not _valid_string(admission.get("admit_criteria"), 6, 500):
            issues.append("invalid_admit_criteria")

    novelty = payload.get("novelty_link")
    if not isinstance(novelty, dict) or set(novelty) != {
        "catalog_id",
        "relationship",
    }:
        issues.append("invalid_novelty_link")
    else:
        if not _valid_string(novelty.get("catalog_id"), 3):
            issues.append("invalid_novelty_catalog_id")
        if novelty.get("relationship") not in {
            "new",
            "known",
            "supersedes",
            "duplicate",
        }:
            issues.append("invalid_novelty_relationship")

    accepted = not issues and payload.get("status") == "proposed"
    result = "accepted" if accepted else (
        "needs_evidence"
        if payload.get("status") == "needs_evidence"
        or (
            isinstance(requirements, dict)
            and requirements.get("feasibility") == "missing"
        )
        or any(issue.startswith("evidence_not_available:") for issue in issues)
        else "rejected"
    )
    return {
        "schema": RECEIPT_SCHEMA,
        "source_lead_id": payload.get("source_lead_id"),
        "hypothesis_id": payload.get("hypothesis_id"),
        "intake_sha256": _hash(payload),
        "evidence_ids": list(evidence_ids),
        "compute_platform": compute_platform,
        "result": result,
        "accepted": accepted,
        "issues": sorted(set(issues)),
        "effects": {
            "hypothesis_written": False,
            "research_started": False,
            "evidence_published": False,
            "sheet_mutated": False,
            "runtime_mutated": False,
        },
    }
