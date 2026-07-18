"""Source-grounded V3 review overlay for class-hidden rectangle batches.

V3 deliberately reuses the immutable V2 sampling and chart artifacts while
replacing the overloaded trade-worthiness response with source-fidelity fields.
The overlay never reads lifecycle, trade, outcome, or P&L artifacts.
"""

from __future__ import annotations

from collections import Counter, defaultdict
import csv
from dataclasses import dataclass
from datetime import datetime
import hashlib
import json
from pathlib import Path
from typing import Any, Sequence

from .review import FORBIDDEN_REVIEW_KEYS, verify_semantic_calibration_batch_v2


SOURCE_FIDELITY_RESPONSE_FIELDS = (
    "batch_id",
    "card_id",
    "config_hash",
    "card_hash",
    "rubric_hash",
    "reviewer_id",
    "review_pass",
    "mala_rectangle_state",
    "lfd_assessment",
    "lfd_date",
    "spec_reason_codes",
    "source_ambiguity_codes",
    "note",
    "reviewed_at",
    "outcome_hidden_attestation",
    "no_future_consulted_attestation",
)
MALA_RECTANGLE_STATE = frozenset(
    {
        "no_mala_rectangle",
        "mala_rectangle_no_close_breakout",
        "mala_rectangle_long_close_breakout",
        "mala_rectangle_short_close_breakout",
        "indeterminate",
    }
)
LFD_ASSESSMENT = frozenset({"identified", "not_applicable", "indeterminate"})
SPEC_REASON_CODES = frozenset(
    {
        "mala_not_horizontal_balance",
        "mala_insufficient_touch_structure",
        "mala_trend_not_balance",
        "mala_boundary_misplaced",
        "mala_range_height_out_of_bounds",
        "mala_close_containment_failure",
        "mala_touch_recency_failure",
        "mala_breakout_not_close_confirmed",
        "mala_breakout_direction_wrong",
        "techcharts_lfd_misidentified",
        "pattern_morphed_or_competing_label",
        "insufficient_visible_context",
        "chart_data_suspect",
        "other",
    }
)
SOURCE_AMBIGUITY_CODES = frozenset(
    {
        "source_rectangle_geometry_undefined",
        "source_breakout_completion_nonuniversal",
        "source_lfd_exact_rule_secondary_only",
        "source_lfd_boundary_edge_case_undefined",
        "source_negation_level_undefined",
        "source_objective_formula_undefined",
        "source_meaningful_retest_undefined",
        "source_type_event_ordering_undefined",
        "source_reentry_rule_nonuniversal",
    }
)


@dataclass(frozen=True, slots=True)
class SourceFidelityReviewResultV3:
    batch_id: str
    review_dir: Path
    contract_path: Path
    receipt_path: Path
    guide_path: Path
    response_path: Path
    canonical_hash: str


@dataclass(frozen=True, slots=True)
class SourceFidelityIngestionResultV3:
    batch_id: str
    reviewed_count: int
    complete_review_pass_count: int
    decision_log_path: Path
    scorecard_path: Path


def initialize_source_fidelity_review_v3(
    *, batch_dir: Path, rubric_path: Path
) -> SourceFidelityReviewResultV3:
    """Create a hash-bound V3 response overlay beside one verified V2 batch."""

    batch_dir = batch_dir.expanduser().resolve()
    base_receipt = verify_semantic_calibration_batch_v2(batch_dir)
    manifest = json.loads((batch_dir / "batch_manifest.json").read_text(encoding="utf-8"))
    rubric_path = rubric_path.expanduser().resolve()
    if not rubric_path.is_file():
        raise FileNotFoundError(f"Source-fidelity rubric not found: {rubric_path}")
    rubric_hash = _sha256_path(rubric_path)
    review_dir = batch_dir / "source_fidelity_v3"
    if review_dir.exists() and any(review_dir.iterdir()):
        raise ValueError("Source-fidelity V3 review directory must be new or empty.")
    review_dir.mkdir(parents=True, exist_ok=True)

    cards_dir = review_dir / "cards"
    cards_dir.mkdir(parents=True, exist_ok=True)
    cards: list[dict[str, Any]] = []
    for base_card in manifest["cards"]:
        relative_card_path = Path("cards") / f"{base_card['card_id']}.md"
        card_path = review_dir / relative_card_path
        card_text = _render_source_fidelity_card(
            batch_id=str(manifest["batch_id"]),
            config_hash=str(manifest["config_hash"]),
            rubric_hash=rubric_hash,
            base_card=base_card,
        )
        _assert_no_v2_review_language(card_text)
        card_path.write_text(card_text, encoding="utf-8")
        cards.append(
            {
                "card_id": base_card["card_id"],
                "card_hash": _sha256_path(card_path),
                "source_card_hash": base_card["card_hash"],
                "chart_hash": base_card["chart_hash"],
                "symbol": base_card["symbol"],
                "evaluation_date": base_card["evaluation_date"],
                "displayed_bar_count": base_card["displayed_bar_count"],
                "card_path": relative_card_path.as_posix(),
                "chart_path": f"../{base_card['chart_path']}",
            }
        )
    contract = {
        "schema_version": "ClassicalPatternSourceFidelityReviewV3",
        "batch_id": manifest["batch_id"],
        "base_batch_schema": manifest["schema_version"],
        "base_canonical_hash": base_receipt["canonical_hash"],
        "config_hash": manifest["config_hash"],
        "rubric_name": rubric_path.name,
        "rubric_hash": rubric_hash,
        "response_fields": list(SOURCE_FIDELITY_RESPONSE_FIELDS),
        "labels": {
            "mala_rectangle_state": sorted(MALA_RECTANGLE_STATE),
            "lfd_assessment": sorted(LFD_ASSESSMENT),
        },
        "spec_reason_codes": sorted(SPEC_REASON_CODES),
        "source_ambiguity_codes": sorted(SOURCE_AMBIGUITY_CODES),
        "review_policy": {
            "minimum_distinct_reviewers": 2,
            "minimum_complete_review_passes": 2,
            "minimum_state_agreement_fraction": 7 / 9,
            "minimum_lfd_agreement_fraction": 7 / 9,
            "maximum_indeterminate_fraction": 1 / 3,
            "minimum_per_hidden_class_match_fraction": 2 / 3,
        },
        "cards": cards,
        "economic_selection_allowed": False,
        "outcomes_hidden": True,
    }
    _assert_outcome_hidden(contract)
    contract_path = review_dir / "source_fidelity_contract.json"
    contract_path.write_text(json.dumps(contract, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    rubric_copy_path = review_dir / "FROZEN_RUBRIC.md"
    rubric_copy_path.write_bytes(rubric_path.read_bytes())
    guide_path = review_dir / "REVIEW_GUIDE.md"
    guide_path.write_text(_render_guide(contract), encoding="utf-8")
    template_path = review_dir / "source_fidelity_responses.template.csv"
    response_path = review_dir / "source_fidelity_responses.csv"
    _write_response_template(contract, template_path)
    _write_response_template(contract, response_path)

    artifacts = {
        path.name: _sha256_path(path)
        for path in (contract_path, rubric_copy_path, guide_path, template_path)
    }
    canonical_payload = {
        "batch_id": contract["batch_id"],
        "base_canonical_hash": contract["base_canonical_hash"],
        "rubric_hash": rubric_hash,
        "artifacts": artifacts,
    }
    canonical_hash = _hash_json(canonical_payload)
    receipt = {
        "schema_version": "ClassicalPatternSourceFidelityReceiptV3",
        **canonical_payload,
        "canonical_hash": canonical_hash,
        "status": "complete",
        "executable": False,
        "outcomes_hidden": True,
        "economic_selection_allowed": False,
        "card_count": len(cards),
        "forbidden_review_keys": sorted(FORBIDDEN_REVIEW_KEYS),
    }
    receipt_path = review_dir / "source_fidelity_receipt.json"
    receipt_path.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    verify_source_fidelity_review_v3(batch_dir)
    return SourceFidelityReviewResultV3(
        batch_id=str(contract["batch_id"]),
        review_dir=review_dir,
        contract_path=contract_path,
        receipt_path=receipt_path,
        guide_path=guide_path,
        response_path=response_path,
        canonical_hash=canonical_hash,
    )


def verify_source_fidelity_review_v3(batch_dir: Path) -> dict[str, Any]:
    batch_dir = batch_dir.expanduser().resolve()
    base_receipt = verify_semantic_calibration_batch_v2(batch_dir)
    base_manifest = json.loads((batch_dir / "batch_manifest.json").read_text(encoding="utf-8"))
    review_dir = batch_dir / "source_fidelity_v3"
    contract = json.loads((review_dir / "source_fidelity_contract.json").read_text(encoding="utf-8"))
    receipt = json.loads((review_dir / "source_fidelity_receipt.json").read_text(encoding="utf-8"))
    expected_contract_keys = {
        "schema_version", "batch_id", "base_batch_schema", "base_canonical_hash", "config_hash",
        "rubric_name", "rubric_hash", "response_fields", "labels", "spec_reason_codes", "source_ambiguity_codes", "review_policy",
        "cards", "economic_selection_allowed", "outcomes_hidden",
    }
    if set(contract) != expected_contract_keys:
        raise ValueError("Source-fidelity V3 contract fields mismatch.")
    if (
        contract["schema_version"] != "ClassicalPatternSourceFidelityReviewV3"
        or contract["base_batch_schema"] != "ClassicalPatternSemanticCalibrationBatchV2"
        or contract["economic_selection_allowed"] is not False
        or contract["outcomes_hidden"] is not True
    ):
        raise ValueError("Source-fidelity V3 safety contract mismatch.")
    if (
        contract["batch_id"] != base_manifest["batch_id"]
        or contract["config_hash"] != base_manifest["config_hash"]
        or contract["base_canonical_hash"] != base_receipt["canonical_hash"]
    ):
        raise ValueError("Source-fidelity V3 base batch identity mismatch.")
    if contract["response_fields"] != list(SOURCE_FIDELITY_RESPONSE_FIELDS):
        raise ValueError("Source-fidelity V3 response fields mismatch.")
    if contract["labels"] != {
        "mala_rectangle_state": sorted(MALA_RECTANGLE_STATE),
        "lfd_assessment": sorted(LFD_ASSESSMENT),
    } or contract["spec_reason_codes"] != sorted(SPEC_REASON_CODES) or contract["source_ambiguity_codes"] != sorted(SOURCE_AMBIGUITY_CODES):
        raise ValueError("Source-fidelity V3 label contract mismatch.")
    if contract["review_policy"] != {
        "minimum_distinct_reviewers": 2,
        "minimum_complete_review_passes": 2,
        "minimum_state_agreement_fraction": 7 / 9,
        "minimum_lfd_agreement_fraction": 7 / 9,
        "maximum_indeterminate_fraction": 1 / 3,
        "minimum_per_hidden_class_match_fraction": 2 / 3,
    }:
        raise ValueError("Source-fidelity V3 review policy mismatch.")
    base_cards = {
        card["card_id"]: card for card in base_manifest["cards"]
    }
    if len(contract["cards"]) != len(base_cards):
        raise ValueError("Source-fidelity V3 card count mismatch.")
    seen: set[str] = set()
    for card in contract["cards"]:
        if set(card) != {
            "card_id", "card_hash", "source_card_hash", "chart_hash", "symbol",
            "evaluation_date", "displayed_bar_count", "card_path", "chart_path",
        }:
            raise ValueError("Source-fidelity V3 card fields mismatch.")
        base = base_cards.get(card["card_id"])
        if base is None or (
            card["source_card_hash"] != base["card_hash"]
            or card["chart_hash"] != base["chart_hash"]
            or card["symbol"] != base["symbol"]
            or card["evaluation_date"] != base["evaluation_date"]
            or card["displayed_bar_count"] != base["displayed_bar_count"]
            or card["card_path"] != f"cards/{card['card_id']}.md"
            or card["chart_path"] != f"../{base['chart_path']}"
        ):
            raise ValueError("Source-fidelity V3 card identity mismatch.")
        card_path = review_dir / card["card_path"]
        if _sha256_path(card_path) != card["card_hash"]:
            raise ValueError("Source-fidelity V3 card hash mismatch.")
        _assert_no_v2_review_language(card_path.read_text(encoding="utf-8"))
        if _sha256_path(review_dir / card["chart_path"]) != card["chart_hash"]:
            raise ValueError("Source-fidelity V3 chart hash mismatch.")
        if card["card_id"] in seen:
            raise ValueError("Source-fidelity V3 duplicate card id.")
        seen.add(card["card_id"])
    expected_receipt_keys = {
        "schema_version", "batch_id", "base_canonical_hash", "rubric_hash", "artifacts",
        "canonical_hash", "status", "executable", "outcomes_hidden",
        "economic_selection_allowed", "card_count", "forbidden_review_keys",
    }
    if set(receipt) != expected_receipt_keys:
        raise ValueError("Source-fidelity V3 receipt fields mismatch.")
    if (
        receipt["schema_version"] != "ClassicalPatternSourceFidelityReceiptV3"
        or receipt["status"] != "complete"
        or receipt["executable"] is not False
        or receipt["outcomes_hidden"] is not True
        or receipt["economic_selection_allowed"] is not False
        or receipt["card_count"] != len(base_cards)
        or receipt["forbidden_review_keys"] != sorted(FORBIDDEN_REVIEW_KEYS)
    ):
        raise ValueError("Source-fidelity V3 receipt safety contract mismatch.")
    for field in ("batch_id", "base_canonical_hash", "rubric_hash"):
        if receipt[field] != contract[field]:
            raise ValueError(f"Source-fidelity V3 receipt identity mismatch: {field}")
    expected_artifacts = {
        "source_fidelity_contract.json", "FROZEN_RUBRIC.md", "REVIEW_GUIDE.md",
        "source_fidelity_responses.template.csv"
    }
    if set(receipt["artifacts"]) != expected_artifacts:
        raise ValueError("Source-fidelity V3 artifact inventory mismatch.")
    for name, expected_hash in receipt["artifacts"].items():
        if _sha256_path(review_dir / name) != expected_hash:
            raise ValueError(f"Source-fidelity V3 artifact hash mismatch: {name}")
    if _sha256_path(review_dir / "FROZEN_RUBRIC.md") != contract["rubric_hash"]:
        raise ValueError("Source-fidelity V3 frozen rubric hash mismatch.")
    payload = {
        "batch_id": receipt["batch_id"],
        "base_canonical_hash": receipt["base_canonical_hash"],
        "rubric_hash": receipt["rubric_hash"],
        "artifacts": receipt["artifacts"],
    }
    if receipt["canonical_hash"] != _hash_json(payload):
        raise ValueError("Source-fidelity V3 canonical hash mismatch.")
    _assert_outcome_hidden(contract)
    return receipt


def validate_source_fidelity_response_v3(row: dict[str, str], contract: dict[str, Any]) -> None:
    if set(row) != set(SOURCE_FIDELITY_RESPONSE_FIELDS):
        raise ValueError("Source-fidelity V3 response fields mismatch.")
    cards = {card["card_id"]: card for card in contract["cards"]}
    card = cards.get(row["card_id"])
    if card is None:
        raise ValueError("Unknown source-fidelity card id.")
    expected = {
        "batch_id": contract["batch_id"],
        "config_hash": contract["config_hash"],
        "card_hash": card["card_hash"],
        "rubric_hash": contract["rubric_hash"],
    }
    for field, value in expected.items():
        if row[field] != str(value):
            raise ValueError(f"Stale source-fidelity response: {field}")
    if row["mala_rectangle_state"] not in MALA_RECTANGLE_STATE:
        raise ValueError("Invalid mala_rectangle_state.")
    if row["lfd_assessment"] not in LFD_ASSESSMENT:
        raise ValueError("Invalid lfd_assessment.")
    spec_codes = _parse_codes(row["spec_reason_codes"])
    if spec_codes - SPEC_REASON_CODES:
        raise ValueError("Invalid spec_reason_codes.")
    codes = _parse_codes(row["source_ambiguity_codes"])
    if codes - SOURCE_AMBIGUITY_CODES:
        raise ValueError("Invalid source_ambiguity_codes.")
    lfd_date = row["lfd_date"].strip()
    if row["lfd_assessment"] == "identified":
        try:
            datetime.fromisoformat(lfd_date)
        except ValueError as exc:
            raise ValueError("identified LFD requires an ISO date.") from exc
    elif lfd_date:
        raise ValueError("lfd_date must be blank unless LFD is identified.")
    if not row["reviewer_id"].strip():
        raise ValueError("reviewer_id is required.")
    try:
        if int(row["review_pass"]) <= 0:
            raise ValueError
    except ValueError as exc:
        raise ValueError("review_pass must be a positive integer.") from exc
    try:
        reviewed_at = datetime.fromisoformat(row["reviewed_at"])
    except ValueError as exc:
        raise ValueError("reviewed_at must be an ISO-8601 timestamp.") from exc
    if reviewed_at.tzinfo is None:
        raise ValueError("reviewed_at must include a timezone.")
    if row["outcome_hidden_attestation"].lower() != "true":
        raise ValueError("Review must attest that outcomes remained hidden.")
    if row["no_future_consulted_attestation"].lower() != "true":
        raise ValueError("Review must attest that no future bars were consulted.")
    breakout_states = {"mala_rectangle_long_close_breakout", "mala_rectangle_short_close_breakout"}
    if row["mala_rectangle_state"] in breakout_states:
        if row["lfd_assessment"] == "not_applicable":
            raise ValueError("Mala close breakout requires an LFD assessment.")
    elif row["lfd_assessment"] == "identified":
        raise ValueError("LFD may be identified only for a Mala close breakout.")


def ingest_source_fidelity_responses_v3(
    *, batch_dir: Path, responses_csv: Path | None = None
) -> SourceFidelityIngestionResultV3:
    batch_dir = batch_dir.expanduser().resolve()
    verify_source_fidelity_review_v3(batch_dir)
    review_dir = batch_dir / "source_fidelity_v3"
    contract = json.loads((review_dir / "source_fidelity_contract.json").read_text(encoding="utf-8"))
    path = responses_csv or review_dir / "source_fidelity_responses.csv"
    with path.open(newline="", encoding="utf-8") as handle:
        rows = [
            dict(row) for row in csv.DictReader(handle)
            if row.get("mala_rectangle_state", "").strip()
        ]
    decisions_dir = review_dir / "decisions"
    decisions_dir.mkdir(parents=True, exist_ok=True)
    log_path = decisions_dir / "source_fidelity_decisions.jsonl"
    existing: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    if log_path.exists():
        for line in log_path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            record = json.loads(line)
            expected_fields = set(SOURCE_FIDELITY_RESPONSE_FIELDS) | {"schema_version", "response_id"}
            if set(record) != expected_fields or record["schema_version"] != "RectangleSourceFidelityResponseV3":
                raise ValueError("Existing source-fidelity decision fields mismatch.")
            response = {field: str(record[field]) for field in SOURCE_FIDELITY_RESPONSE_FIELDS}
            validate_source_fidelity_response_v3(response, contract)
            if record["response_id"] != _hash_json(response):
                raise ValueError("Existing source-fidelity response id mismatch.")
            key = (response["batch_id"], response["card_id"], response["reviewer_id"], response["review_pass"])
            if key in existing:
                raise ValueError("Duplicate existing source-fidelity review identity.")
            existing[key] = record
    appended: list[dict[str, Any]] = []
    for row in rows:
        validate_source_fidelity_response_v3(row, contract)
        response_id = _hash_json(row)
        key = (row["batch_id"], row["card_id"], row["reviewer_id"], row["review_pass"])
        prior = existing.get(key)
        if prior:
            if prior["response_id"] != response_id:
                raise ValueError("Conflicting source-fidelity response identity.")
            continue
        record = {"schema_version": "RectangleSourceFidelityResponseV3", "response_id": response_id, **row}
        existing[key] = record
        appended.append(record)
    if appended:
        with log_path.open("a", encoding="utf-8") as handle:
            for record in appended:
                handle.write(json.dumps(record, sort_keys=True) + "\n")
    elif not log_path.exists():
        log_path.touch()

    card_ids = {card["card_id"] for card in contract["cards"]}
    pass_cards: dict[tuple[str, str], set[str]] = defaultdict(set)
    by_card: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in existing.values():
        pass_cards[(record["reviewer_id"], record["review_pass"])].add(record["card_id"])
        by_card[record["card_id"]].append(record)
    complete_passes = sorted(
        {f"{reviewer}:{review_pass}" for (reviewer, review_pass), cards in pass_cards.items() if cards == card_ids}
    )
    agreement_fields = ("mala_rectangle_state", "lfd_assessment", "lfd_date")
    agreement_counts = {
        field: sum(
            1 for records in by_card.values()
            if len(records) >= 2 and len({record[field] for record in records}) == 1
        )
        for field in agreement_fields
    }
    disagreement_cards = sorted(
        card_id for card_id, records in by_card.items()
        if len(records) >= 2 and any(len({record[field] for record in records}) > 1 for field in agreement_fields)
    )
    scorecard = {
        "schema_version": "ClassicalPatternSourceFidelityScorecardV3",
        "batch_id": contract["batch_id"],
        "rubric_hash": contract["rubric_hash"],
        "reviewed_count": len(existing),
        "card_count": len(card_ids),
        "complete_review_passes": complete_passes,
        "agreement_counts": agreement_counts,
        "disagreement_card_ids": disagreement_cards,
        "mala_rectangle_state_counts": dict(sorted(Counter(
            record["mala_rectangle_state"] for record in existing.values()
        ).items())),
        "economic_fields_present": False,
        "economic_selection_allowed": False,
        "decision_log_hash": _sha256_path(log_path),
    }
    scorecard_path = decisions_dir / "source_fidelity_scorecard.json"
    scorecard_path.write_text(json.dumps(scorecard, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return SourceFidelityIngestionResultV3(
        batch_id=str(contract["batch_id"]),
        reviewed_count=len(existing),
        complete_review_pass_count=len(complete_passes),
        decision_log_path=log_path,
        scorecard_path=scorecard_path,
    )


def freeze_mala_rectangle_semantic_spec_v1(
    *, batch_dir: Path, detector_git_commit: str
) -> Path:
    """Evaluate aggregate V3 gates and write a non-filtering semantic freeze receipt."""

    batch_dir = batch_dir.expanduser().resolve()
    base_receipt = verify_semantic_calibration_batch_v2(batch_dir)
    verify_source_fidelity_review_v3(batch_dir)
    review_dir = batch_dir / "source_fidelity_v3"
    contract = json.loads((review_dir / "source_fidelity_contract.json").read_text(encoding="utf-8"))
    decisions_dir = review_dir / "decisions"
    log_path = decisions_dir / "source_fidelity_decisions.jsonl"
    scorecard_path = decisions_dir / "source_fidelity_scorecard.json"
    if not log_path.is_file() or not scorecard_path.is_file():
        raise ValueError("Source-fidelity responses must be ingested before freeze.")
    scorecard = json.loads(scorecard_path.read_text(encoding="utf-8"))
    records = [
        json.loads(line)
        for line in log_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    base_manifest = json.loads((batch_dir / "batch_manifest.json").read_text(encoding="utf-8"))
    private_by_card = {item["card_id"]: item for item in base_manifest["private_cases"]}
    class_totals: Counter[str] = Counter()
    class_matches: Counter[str] = Counter()
    for record in records:
        private = private_by_card[record["card_id"]]
        hidden_class = str(private["hidden_class"])
        class_totals[hidden_class] += 1
        expected = _expected_mala_state(private)
        if record["mala_rectangle_state"] == expected:
            class_matches[hidden_class] += 1
    per_class = {
        hidden_class: {
            "matched": class_matches[hidden_class],
            "review_rows": class_totals[hidden_class],
            "match_fraction": (
                class_matches[hidden_class] / class_totals[hidden_class]
                if class_totals[hidden_class]
                else 0.0
            ),
        }
        for hidden_class in sorted(
            {private["hidden_class"] for private in base_manifest["private_cases"]}
        )
    }
    policy = contract["review_policy"]
    complete_reviewers = {
        item.split(":", 1)[0] for item in scorecard["complete_review_passes"]
    }
    card_count = int(scorecard["card_count"])
    state_agreement = scorecard["agreement_counts"]["mala_rectangle_state"] / card_count
    lfd_agreement = scorecard["agreement_counts"]["lfd_assessment"] / card_count
    indeterminate_fraction = (
        sum(record["mala_rectangle_state"] == "indeterminate" for record in records) / len(records)
        if records
        else 1.0
    )
    gate_checks = {
        "distinct_reviewers": len(complete_reviewers)
        >= policy["minimum_distinct_reviewers"],
        "complete_review_passes": len(scorecard["complete_review_passes"])
        >= policy["minimum_complete_review_passes"],
        "state_agreement": state_agreement >= policy["minimum_state_agreement_fraction"],
        "lfd_agreement": lfd_agreement >= policy["minimum_lfd_agreement_fraction"],
        "indeterminate_fraction": indeterminate_fraction <= policy["maximum_indeterminate_fraction"],
        "per_hidden_class_match": bool(per_class) and all(
            row["match_fraction"] >= policy["minimum_per_hidden_class_match_fraction"]
            for row in per_class.values()
        ),
    }
    status = "frozen" if all(gate_checks.values()) else "revise"
    payload = {
        "schema_version": "MalaRectangleSemanticSpecFreezeV1",
        "freeze_id": f"mala-rectangle-semantic-v1-{contract['batch_id']}",
        "status": status,
        "playbook_id": base_manifest["playbook_id"],
        "config_hash": contract["config_hash"],
        "rubric_hash": contract["rubric_hash"],
        "detector_git_commit": detector_git_commit,
        "batch_id": contract["batch_id"],
        "batch_canonical_hash": base_receipt["canonical_hash"],
        "decision_log_hash": _sha256_path(log_path),
        "scorecard_hash": _sha256_path(scorecard_path),
        "review_policy": policy,
        "reviewer_agreement": {
            "mala_rectangle_state": state_agreement,
            "lfd_assessment": lfd_agreement,
        },
        "indeterminate_fraction": indeterminate_fraction,
        "per_hidden_class_match": per_class,
        "gate_checks": gate_checks,
        "economic_filtering_allowed": False,
        "trade_worthiness_fields_present": False,
        "frozen_at": datetime.now().astimezone().isoformat(),
    }
    payload["canonical_hash"] = _hash_json(payload)
    freeze_path = decisions_dir / "mala_rectangle_semantic_freeze_v1.json"
    freeze_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return freeze_path


def _write_response_template(contract: dict[str, Any], path: Path) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=SOURCE_FIDELITY_RESPONSE_FIELDS)
        writer.writeheader()
        for card in contract["cards"]:
            writer.writerow(
                {
                    "batch_id": contract["batch_id"],
                    "card_id": card["card_id"],
                    "config_hash": contract["config_hash"],
                    "card_hash": card["card_hash"],
                    "rubric_hash": contract["rubric_hash"],
                }
            )


def _render_source_fidelity_card(
    *,
    batch_id: str,
    config_hash: str,
    rubric_hash: str,
    base_card: dict[str, Any],
) -> str:
    card_id = str(base_card["card_id"])
    return "\n".join(
        [
            "---",
            f"batch_id: {batch_id}",
            f"card_id: {card_id}",
            f"config_hash: {config_hash}",
            f"rubric_hash: {rubric_hash}",
            f"symbol: {base_card['symbol']}",
            f"evaluation_date: {base_card['evaluation_date']}",
            "---",
            "",
            "# Daily OHLC Source-Fidelity Card",
            "",
            f"![As-of OHLC chart](../../charts/{card_id}.svg)",
            "",
            f"- Symbol: `{base_card['symbol']}`",
            f"- Evaluation cutoff: `{base_card['evaluation_date']}`",
            f"- Displayed candidate window: `{base_card['displayed_bar_count']}` sessions",
            "- Judge only the displayed window; inclusion is not a machine verdict.",
            "- The pale final band marks the evaluation cutoff; it is not necessarily a breakout.",
            "",
            "## Source-Fidelity Review",
            "",
            "Record answers in `../source_fidelity_responses.csv` using the frozen rubric:",
            "",
            "1. Choose one `mala_rectangle_state`.",
            "2. Choose one `lfd_assessment`; add `lfd_date` only when identified.",
            "3. Add only applicable spec and source-ambiguity codes.",
            "4. Add a concise chart-grounded note if useful.",
            "",
            "Do not judge personal attractiveness or likely profitability. Only bars through",
            "the evaluation cutoff are shown; subsequent bars and economic results are absent.",
            "",
        ]
    )


def _assert_no_v2_review_language(text: str) -> None:
    lowered = text.casefold()
    forbidden_phrases = (
        "as_of_trade_worthiness",
        "trade worthiness",
        "strict rectangle validity",
        "trade`, `watch`, `no_trade",
    )
    if any(phrase in lowered for phrase in forbidden_phrases):
        raise ValueError("Source-fidelity V3 card contains historical V2 review language.")


def _render_guide(contract: dict[str, Any]) -> str:
    lines = [
        "# Classical Rectangle Source-Fidelity Review v3",
        "",
        f"- Batch: `{contract['batch_id']}`",
        f"- Rubric: `{contract['rubric_name']}`",
        f"- Rubric hash: `{contract['rubric_hash']}`",
        "- Outcomes/P&L: hidden",
        "- Economic-selection authority: none",
        "",
        "Read the frozen rubric, then inspect every card and chart. Judge source fidelity,",
        "not whether you would trade the setup and not whether it later made money.",
        "",
        "## Cards",
        "",
    ]
    for index, card in enumerate(contract["cards"], start=1):
        lines.append(
            f"{index}. [{card['symbol']} · {card['evaluation_date']}]({card['card_path']}) "
            f"— `{card['card_id']}`"
        )
    lines.extend(
        [
            "",
            "## Response",
            "",
            "Complete `source_fidelity_responses.csv` using only the rubric enums.",
            "Every response is keyed by reviewer id and review pass; independent passes do not overwrite.",
            "",
        ]
    )
    return "\n".join(lines)


def _parse_codes(value: str) -> set[str]:
    return {code.strip() for chunk in value.split(";") for code in chunk.split(",") if code.strip()}


def _expected_mala_state(private: dict[str, Any]) -> str:
    hidden_class = private["hidden_class"]
    if hidden_class == "qualified_no_trigger":
        return "mala_rectangle_no_close_breakout"
    if hidden_class == "rejected_geometry":
        return "no_mala_rectangle"
    direction = private.get("causal_attempt_direction")
    if hidden_class == "confirmed_signal" and direction in {"long", "short"}:
        return f"mala_rectangle_{direction}_close_breakout"
    raise ValueError("Unsupported hidden class for semantic freeze.")


def _hash_json(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _sha256_path(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _assert_outcome_hidden(value: Any, path: str = "root") -> None:
    if isinstance(value, dict):
        forbidden = set(value) & FORBIDDEN_REVIEW_KEYS
        if forbidden:
            raise ValueError(f"Forbidden economic/future keys at {path}: {sorted(forbidden)}")
        for key, item in value.items():
            _assert_outcome_hidden(item, f"{path}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _assert_outcome_hidden(item, f"{path}[{index}]")
