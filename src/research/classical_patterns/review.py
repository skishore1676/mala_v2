"""Outcome-hidden semantic review packets for classical rectangle signals."""

from __future__ import annotations

from collections import Counter, defaultdict
import csv
from dataclasses import asdict, dataclass, fields
from datetime import date, datetime
from html import escape
import hashlib
import json
from math import ceil
from pathlib import Path
from typing import Any, Sequence
from xml.etree import ElementTree

import polars as pl

from src.trading_calendar import trading_dates

from .contracts import RectangleResearchConfig, RectangleSignal
from .daily_bars import hash_daily_bars
from .readiness import DataReadinessReport, validate_readiness_report
from .rectangle import enumerate_rectangles


FORBIDDEN_REVIEW_KEYS = frozenset(
    {
        "outcome",
        "outcome_type",
        "terminal_reason",
        "exit_reason",
        "entry_price",
        "exit_price",
        "gross_pnl",
        "net_pnl",
        "net_return",
        "net_r",
        "mfe",
        "mae",
        "objective_hit",
        "future_bars",
    }
)
REVIEW_DECISIONS = frozenset({"accept", "revise", "reject", "ambiguous"})
FIDELITY_DECISIONS = frozenset({"yes", "no", "revise", "ambiguous"})
REVIEW_RESPONSE_FIELDS = frozenset(
    {
        "batch_id",
        "card_id",
        "signal_id",
        "config_hash",
        "card_hash",
        "decision",
        "rectangle_fidelity",
        "boundary_fidelity",
        "lfd_fidelity",
        "breakout_fidelity",
        "reason_codes",
        "upper_boundary_revision",
        "lower_boundary_revision",
        "lfd_date_revision",
        "note",
        "reviewer",
        "reviewed_at",
        "outcome_hidden_attestation",
        "no_future_consulted_attestation",
    }
)
REVIEW_REASON_CODES = frozenset(
    {
        "not_rectangle",
        "insufficient_touch_structure",
        "trend_not_balance",
        "boundary_misplaced",
        "breakout_not_confirmed",
        "direction_wrong",
        "lfd_misidentified",
        "pattern_morphed",
        "chart_data_suspect",
        "insufficient_context",
        "other",
    }
)


@dataclass(frozen=True, slots=True)
class ReviewCardRecord:
    schema_version: str
    batch_id: str
    card_id: str
    signal_id: str
    candidate_id: str
    config_hash: str
    symbol: str
    direction: str
    visible_as_of: str
    breakout_date: str
    pattern_start_date: str
    pattern_end_date: str
    lookback_sessions: int
    quality_band: str
    diagnostic_band: str
    levels: dict[str, float | str]
    geometry: dict[str, float | int]
    breakout_bar_diagnostic_codes: tuple[str, ...]
    source_slice_hash: str
    chart_path: str
    chart_hash: str
    card_path: str
    card_hash: str


@dataclass(frozen=True, slots=True)
class SemanticBatchResult:
    batch_id: str
    output_dir: Path
    eligible_signal_count: int
    selected_signal_count: int
    manifest_path: Path
    receipt_path: Path
    review_index_path: Path
    response_template_path: Path
    canonical_hash: str


@dataclass(frozen=True, slots=True)
class SemanticIngestionResult:
    batch_id: str
    status: str
    reviewed_count: int
    total_count: int
    decision_log_path: Path
    scorecard_path: Path


def build_semantic_review_batch(
    daily_by_symbol: dict[str, pl.DataFrame],
    *,
    config: RectangleResearchConfig,
    readiness: DataReadinessReport,
    output_dir: Path,
    batch_id: str,
    batch_size: int = 12,
    sampling_seed: str = "classical-rectangle-semantic-v1",
    eligibility_start: date | None = None,
    eligibility_end: date | None = None,
) -> SemanticBatchResult:
    """Generate a deterministic review projection without lifecycle/trade data."""

    if readiness.semantic_review_status != "ready":
        raise ValueError("Readiness report does not authorize a semantic review pilot.")
    validate_readiness_report(readiness)
    if readiness.config_hash != config.source_hash:
        raise ValueError("Readiness/config hash mismatch.")
    if batch_size <= 0:
        raise ValueError("batch_size must be positive.")
    if not batch_id.strip():
        raise ValueError("batch_id is required.")

    output_dir = output_dir.expanduser().resolve()
    if output_dir.exists() and any(output_dir.iterdir()):
        raise ValueError(
            "Semantic batch output_dir must be absent or empty; use a new batch root."
        )
    _validate_readiness_alignment(
        readiness,
        daily_by_symbol,
        eligibility_start=eligibility_start,
        eligibility_end=eligibility_end,
    )
    charts_dir = output_dir / "charts"
    cards_dir = output_dir / "cards"
    charts_dir.mkdir(parents=True, exist_ok=True)
    cards_dir.mkdir(parents=True, exist_ok=True)

    signals: list[RectangleSignal] = []
    signal_frames: dict[str, pl.DataFrame] = {}
    ineligible = Counter[str]()
    enumerated_by_symbol: dict[str, int] = {}
    source_hashes: dict[str, str] = {}
    for symbol in sorted(daily_by_symbol):
        daily = daily_by_symbol[symbol].sort("session_date")
        source_hashes[symbol] = hash_daily_bars(daily)
        result = enumerate_rectangles(daily, config)
        enumerated_by_symbol[symbol] = len(result.signals)
        for signal in result.signals:
            if eligibility_start and signal.candidate.breakout_date < eligibility_start:
                ineligible["before_eligibility_window"] += 1
                continue
            if eligibility_end and signal.candidate.breakout_date > eligibility_end:
                ineligible["after_eligibility_window"] += 1
                continue
            if signal.candidate.split != "calibration":
                ineligible["non_calibration_split"] += 1
                continue
            reason = _semantic_eligibility_reason(signal, daily)
            if reason:
                ineligible[reason] += 1
                continue
            signals.append(signal)
            signal_frames[signal.signal_id] = daily

    selected = _select_review_signals(
        signals,
        batch_size=batch_size,
        sampling_seed=sampling_seed,
    )
    if not selected:
        raise ValueError("No eligible rectangle signals were available for review.")

    records: list[ReviewCardRecord] = []
    for signal in selected:
        daily = signal_frames[signal.signal_id]
        candidate = signal.candidate
        start_index = max(0, candidate.breakout_index - max(80, candidate.lookback_sessions))
        as_of_slice = daily.slice(start_index, candidate.breakout_index - start_index + 1)
        if as_of_slice.get_column("session_date").max() != candidate.breakout_date:
            raise RuntimeError(f"As-of slice failed for {signal.signal_id}")

        card_id = hashlib.sha256(
            f"{batch_id}|{config.source_hash}|{signal.signal_id}".encode("utf-8")
        ).hexdigest()[:16]
        source_slice_hash = hash_daily_bars(as_of_slice)
        chart_path = charts_dir / f"{card_id}.svg"
        chart_path.write_text(
            _render_signal_svg(
                signal,
                as_of_slice,
                definition_hash=config.source_hash,
                source_slice_hash=source_slice_hash,
            ),
            encoding="utf-8",
        )
        chart_hash = _sha256_path(chart_path)
        card_path = cards_dir / f"{card_id}.md"
        card_payload = _card_payload(
            signal,
            batch_id=batch_id,
            card_id=card_id,
            config_hash=config.source_hash,
            source_slice_hash=source_slice_hash,
            chart_path=f"charts/{card_id}.svg",
            chart_hash=chart_hash,
        )
        _assert_outcome_hidden(card_payload)
        card_path.write_text(_render_card_markdown(card_payload), encoding="utf-8")
        card_hash = _sha256_path(card_path)
        records.append(
            ReviewCardRecord(
                **card_payload,
                card_path=f"cards/{card_id}.md",
                card_hash=card_hash,
            )
        )

    records.sort(key=lambda row: row.card_id)
    manifest_payload = {
        "schema_version": "ClassicalPatternSemanticBatchV1",
        "batch_id": batch_id,
        "playbook_id": config.playbook_id,
        "config_hash": config.source_hash,
        "readiness_report_hash": readiness.report_hash,
        "sampling": {
            "version": "causal_stratified_round_robin_v1",
            "seed": sampling_seed,
            "requested_size": batch_size,
            "eligibility_start": eligibility_start.isoformat() if eligibility_start else None,
            "eligibility_end": eligibility_end.isoformat() if eligibility_end else None,
            "eligible_count": len(signals),
            "selected_count": len(records),
            "unselected_count": len(signals) - len(records),
            "inputs": [
                "symbol",
                "direction",
                "lookback_sessions",
                "quality_band",
                "as_of_diagnostic_codes",
                "signal_id",
            ],
        },
        "source_daily_hashes": source_hashes,
        "enumerated_signals_by_symbol": enumerated_by_symbol,
        "ineligible_reasons": dict(sorted(ineligible.items())),
        "cards": [asdict(record) for record in records],
    }
    _assert_outcome_hidden(manifest_payload)
    manifest_path = output_dir / "batch_manifest.json"
    manifest_path.write_text(
        json.dumps(manifest_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    response_template_path = output_dir / "review_responses.template.csv"
    _write_response_template(records, response_template_path)
    response_path = output_dir / "review_responses.csv"
    _write_response_template(records, response_path)
    index_path = output_dir / "REVIEW_INDEX.md"
    index_path.write_text(_render_review_index(batch_id, records), encoding="utf-8")

    artifacts = {
        path.relative_to(output_dir).as_posix(): _sha256_path(path)
        for path in sorted([manifest_path, response_template_path, index_path, *charts_dir.glob("*.svg"), *cards_dir.glob("*.md")])
    }
    canonical_payload = {
        "batch_id": batch_id,
        "config_hash": config.source_hash,
        "readiness_report_hash": readiness.report_hash,
        "artifacts": artifacts,
    }
    canonical_hash = hashlib.sha256(
        json.dumps(canonical_payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    receipt = {
        "schema_version": "ClassicalPatternSemanticBatchReceiptV1",
        **canonical_payload,
        "canonical_hash": canonical_hash,
        "status": "complete",
        "readiness": "semantic_pilot",
        "executable": False,
        "outcomes_hidden": True,
        "eligible_signal_count": len(signals),
        "selected_signal_count": len(records),
        "unselected_signal_count": len(signals) - len(records),
        "ineligible_reasons": dict(sorted(ineligible.items())),
        "forbidden_review_keys": sorted(FORBIDDEN_REVIEW_KEYS),
    }
    receipt_path = output_dir / "batch_receipt.json"
    receipt_path.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    verify_semantic_batch(output_dir)
    return SemanticBatchResult(
        batch_id=batch_id,
        output_dir=output_dir,
        eligible_signal_count=len(signals),
        selected_signal_count=len(records),
        manifest_path=manifest_path,
        receipt_path=receipt_path,
        review_index_path=index_path,
        response_template_path=response_path,
        canonical_hash=canonical_hash,
    )


def verify_semantic_batch(output_dir: Path) -> dict[str, Any]:
    output_dir = output_dir.expanduser().resolve()
    receipt = json.loads((output_dir / "batch_receipt.json").read_text(encoding="utf-8"))
    _assert_exact_keys(
        receipt,
        {
            "schema_version",
            "batch_id",
            "config_hash",
            "readiness_report_hash",
            "artifacts",
            "canonical_hash",
            "status",
            "readiness",
            "executable",
            "outcomes_hidden",
            "eligible_signal_count",
            "selected_signal_count",
            "unselected_signal_count",
            "ineligible_reasons",
            "forbidden_review_keys",
        },
        "receipt",
    )
    if (
        receipt.get("schema_version") != "ClassicalPatternSemanticBatchReceiptV1"
        or receipt.get("status") != "complete"
        or receipt.get("readiness") != "semantic_pilot"
    ):
        raise ValueError("Semantic batch receipt has an invalid status contract.")
    if receipt.get("executable") is not False or receipt.get("outcomes_hidden") is not True:
        raise ValueError("Semantic batch receipt has an unsafe readiness boundary.")
    for relative_path, expected_hash in receipt.get("artifacts", {}).items():
        path = (output_dir / relative_path).resolve()
        if output_dir not in path.parents:
            raise ValueError(f"Artifact escapes batch root: {relative_path}")
        if not path.is_file():
            raise ValueError(f"Missing semantic batch artifact: {relative_path}")
        if _sha256_path(path) != expected_hash:
            raise ValueError(f"Semantic batch artifact hash mismatch: {relative_path}")
    manifest = json.loads((output_dir / "batch_manifest.json").read_text(encoding="utf-8"))
    _assert_exact_keys(
        manifest,
        {
            "schema_version",
            "batch_id",
            "playbook_id",
            "config_hash",
            "readiness_report_hash",
            "sampling",
            "source_daily_hashes",
            "enumerated_signals_by_symbol",
            "ineligible_reasons",
            "cards",
        },
        "manifest",
    )
    if manifest["schema_version"] != "ClassicalPatternSemanticBatchV1":
        raise ValueError("Unsupported semantic batch manifest version.")
    _assert_exact_keys(
        manifest["sampling"],
        {
            "version",
            "seed",
            "requested_size",
            "eligibility_start",
            "eligibility_end",
            "eligible_count",
            "selected_count",
            "unselected_count",
            "inputs",
        },
        "sampling",
    )
    if manifest["sampling"]["version"] != "causal_stratified_round_robin_v1":
        raise ValueError("Unsupported semantic batch sampling version.")
    for field in ("batch_id", "config_hash", "readiness_report_hash"):
        if receipt[field] != manifest[field]:
            raise ValueError(f"Receipt/manifest identity mismatch: {field}")
    if receipt["ineligible_reasons"] != manifest["ineligible_reasons"]:
        raise ValueError("Receipt/manifest ineligible reasons mismatch.")
    if receipt["forbidden_review_keys"] != sorted(FORBIDDEN_REVIEW_KEYS):
        raise ValueError("Receipt forbidden-key contract mismatch.")
    expected_card_keys = {field.name for field in fields(ReviewCardRecord)}
    expected_artifacts = {
        "batch_manifest.json",
        "review_responses.template.csv",
        "REVIEW_INDEX.md",
    }
    seen_card_ids: set[str] = set()
    seen_signal_ids: set[str] = set()
    for card in manifest["cards"]:
        _assert_exact_keys(card, expected_card_keys, "card")
        if card["schema_version"] != "ClassicalPatternSemanticReviewCardV1":
            raise ValueError("Unsupported semantic review card version.")
        if card["card_id"] in seen_card_ids or card["signal_id"] in seen_signal_ids:
            raise ValueError("Semantic batch contains duplicate card or signal identifiers.")
        seen_card_ids.add(card["card_id"])
        seen_signal_ids.add(card["signal_id"])
        chart_relative = _safe_batch_relative_path(card["chart_path"], "chart_path")
        card_relative = _safe_batch_relative_path(card["card_path"], "card_path")
        expected_artifacts.update({chart_relative, card_relative})
        chart_path = output_dir / chart_relative
        card_path = output_dir / card_relative
        if _sha256_path(chart_path) != card["chart_hash"]:
            raise ValueError(f"Card chart_hash mismatch: {card['card_id']}")
        if _sha256_path(card_path) != card["card_hash"]:
            raise ValueError(f"Card card_hash mismatch: {card['card_id']}")
        _verify_svg_metadata(chart_path, card)
    if set(receipt.get("artifacts", {})) != expected_artifacts:
        raise ValueError("Receipt artifact inventory does not exactly match the manifest.")
    selected_count = len(manifest["cards"])
    if manifest["sampling"]["selected_count"] != selected_count:
        raise ValueError("Manifest selected_count does not match its cards.")
    if receipt.get("selected_signal_count") != selected_count:
        raise ValueError("Receipt selected_signal_count does not match the manifest.")
    for count_name in ("eligible_signal_count", "unselected_signal_count"):
        sampling_name = count_name.removesuffix("_signal_count") + "_count"
        if receipt[count_name] != manifest["sampling"][sampling_name]:
            raise ValueError(f"Receipt {count_name} does not match the manifest.")
    canonical_payload = {
        "batch_id": manifest["batch_id"],
        "config_hash": manifest["config_hash"],
        "readiness_report_hash": manifest["readiness_report_hash"],
        "artifacts": receipt["artifacts"],
    }
    expected_canonical_hash = hashlib.sha256(
        json.dumps(canonical_payload, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
    ).hexdigest()
    if receipt.get("canonical_hash") != expected_canonical_hash:
        raise ValueError("Semantic batch canonical hash mismatch.")
    _assert_outcome_hidden(manifest)
    return receipt


def validate_review_response(row: dict[str, str], manifest: dict[str, Any]) -> None:
    if set(row) != REVIEW_RESPONSE_FIELDS:
        raise ValueError("Review response fields do not match V1 contract.")
    cards = {card["card_id"]: card for card in manifest.get("cards", [])}
    card = cards.get(row["card_id"])
    if card is None:
        raise ValueError("Unknown review card.")
    for field in ("batch_id", "signal_id", "config_hash", "card_hash"):
        expected = manifest[field] if field in manifest else card[field]
        if row[field] != str(expected):
            raise ValueError(f"Stale or mismatched review response: {field}")
    if row["decision"] not in REVIEW_DECISIONS:
        raise ValueError("Invalid review decision.")
    for field in ("rectangle_fidelity", "boundary_fidelity", "lfd_fidelity", "breakout_fidelity"):
        if row[field] not in FIDELITY_DECISIONS:
            raise ValueError(f"Invalid fidelity decision: {field}")
    reason_codes = {
        code.strip()
        for chunk in row["reason_codes"].split(";")
        for code in chunk.split(",")
        if code.strip()
    }
    unknown_reasons = reason_codes - REVIEW_REASON_CODES
    if unknown_reasons:
        raise ValueError(f"Invalid review reason codes: {sorted(unknown_reasons)}")
    revisions = {
        field: row[field].strip()
        for field in (
            "upper_boundary_revision",
            "lower_boundary_revision",
            "lfd_date_revision",
        )
    }
    for field in ("upper_boundary_revision", "lower_boundary_revision"):
        if revisions[field]:
            try:
                float(revisions[field])
            except ValueError as exc:
                raise ValueError(f"Invalid numeric revision: {field}") from exc
    if revisions["lfd_date_revision"]:
        try:
            date.fromisoformat(revisions["lfd_date_revision"])
        except ValueError as exc:
            raise ValueError("Invalid lfd_date_revision") from exc
    if row["decision"] == "accept":
        if reason_codes:
            raise ValueError("Accepted reviews cannot contain reason codes.")
        if any(revisions.values()):
            raise ValueError("Accepted reviews cannot contain revisions.")
        if any(row[field] != "yes" for field in (
            "rectangle_fidelity",
            "boundary_fidelity",
            "lfd_fidelity",
            "breakout_fidelity",
        )):
            raise ValueError("Accepted reviews require yes for every fidelity field.")
    if row["decision"] == "revise" and not (reason_codes or any(revisions.values())):
        raise ValueError("Revised reviews require a reason code or correction.")
    if row["decision"] in {"reject", "ambiguous"} and not reason_codes:
        raise ValueError(f"{row['decision'].title()} reviews require a reason code.")
    if not row["reviewer"].strip():
        raise ValueError("Reviewer is required.")
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


def ingest_review_responses(
    *,
    batch_dir: Path,
    responses_csv: Path | None = None,
) -> SemanticIngestionResult:
    """Append validated decisions and write a semantic-only progress scorecard."""

    batch_dir = batch_dir.expanduser().resolve()
    verify_semantic_batch(batch_dir)
    manifest = json.loads((batch_dir / "batch_manifest.json").read_text(encoding="utf-8"))
    responses_path = responses_csv or batch_dir / "review_responses.csv"
    with responses_path.open(newline="", encoding="utf-8") as handle:
        response_rows = [dict(row) for row in csv.DictReader(handle) if row.get("decision", "").strip()]

    decisions_dir = batch_dir / "semantic_decisions"
    decisions_dir.mkdir(parents=True, exist_ok=True)
    log_path = decisions_dir / "review_decisions.jsonl"
    existing: dict[str, dict[str, Any]] = {}
    if log_path.exists():
        for line in log_path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            record = json.loads(line)
            expected_record_fields = set(REVIEW_RESPONSE_FIELDS) | {
                "schema_version",
                "response_id",
            }
            if set(record) != expected_record_fields:
                raise ValueError("Existing semantic decision fields do not match V1 contract.")
            if record["schema_version"] != "RectangleSemanticReviewResponseV1":
                raise ValueError("Existing semantic decision has an invalid schema version.")
            prior_row = {field: record[field] for field in REVIEW_RESPONSE_FIELDS}
            validate_review_response(prior_row, manifest)
            expected_response_id = hashlib.sha256(
                json.dumps(prior_row, sort_keys=True, separators=(",", ":")).encode(
                    "utf-8"
                )
            ).hexdigest()
            if record["response_id"] != expected_response_id:
                raise ValueError("Existing semantic decision response_id mismatch.")
            if record["card_id"] in existing:
                raise ValueError("Duplicate existing semantic decision card.")
            existing[record["card_id"]] = record

    appended: list[dict[str, Any]] = []
    seen_in_input: set[str] = set()
    for row in response_rows:
        validate_review_response(row, manifest)
        card_id = row["card_id"]
        if card_id in seen_in_input:
            raise ValueError(f"Duplicate response row for card: {card_id}")
        seen_in_input.add(card_id)
        response_id = hashlib.sha256(
            json.dumps(row, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        record = {
            "schema_version": "RectangleSemanticReviewResponseV1",
            "response_id": response_id,
            **row,
        }
        prior = existing.get(card_id)
        if prior:
            if prior["response_id"] != response_id:
                raise ValueError(
                    f"Conflicting response already exists for card {card_id}; start a new review round."
                )
            continue
        appended.append(record)
        existing[card_id] = record

    if appended:
        with log_path.open("a", encoding="utf-8") as handle:
            for record in appended:
                handle.write(json.dumps(record, sort_keys=True) + "\n")
    elif not log_path.exists():
        log_path.touch()

    total = len(manifest["cards"])
    reviewed = len(existing)
    decision_counts = Counter(record["decision"] for record in existing.values())
    scorecard = {
        "schema_version": "ClassicalPatternSemanticScorecardV1",
        "batch_id": manifest["batch_id"],
        "config_hash": manifest["config_hash"],
        "status": "complete" if reviewed == total else "in_progress",
        "reviewed_count": reviewed,
        "unreviewed_count": total - reviewed,
        "total_count": total,
        "decision_counts": dict(sorted(decision_counts.items())),
        "economic_fields_present": False,
        "decision_log_hash": _sha256_path(log_path) if log_path.exists() else None,
    }
    scorecard_path = decisions_dir / "semantic_scorecard.json"
    scorecard_path.write_text(
        json.dumps(scorecard, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return SemanticIngestionResult(
        batch_id=manifest["batch_id"],
        status=scorecard["status"],
        reviewed_count=reviewed,
        total_count=total,
        decision_log_path=log_path,
        scorecard_path=scorecard_path,
    )


def _semantic_eligibility_reason(signal: RectangleSignal, daily: pl.DataFrame) -> str | None:
    candidate = signal.candidate
    available = set(
        daily.slice(
            candidate.breakout_index - candidate.lookback_sessions,
            candidate.lookback_sessions + 1,
        )
        .get_column("session_date")
        .to_list()
    )
    expected = set(trading_dates(candidate.pattern_start_date, candidate.breakout_date))
    if available != expected:
        return "calendar_gap_in_pattern_window"
    return None


def _validate_readiness_alignment(
    readiness: DataReadinessReport,
    daily_by_symbol: dict[str, pl.DataFrame],
    *,
    eligibility_start: date | None,
    eligibility_end: date | None,
) -> None:
    audited = {row.symbol: row for row in readiness.symbols}
    supplied_symbols = {symbol.upper() for symbol in daily_by_symbol}
    if supplied_symbols != set(audited):
        raise ValueError("Readiness symbols do not exactly match semantic batch inputs.")
    for symbol, daily in daily_by_symbol.items():
        row = audited[symbol.upper()]
        if not row.semantic_pilot_ready or not row.complete_daily_hash:
            raise ValueError(f"Readiness does not authorize symbol: {symbol}")
        if hash_daily_bars(daily.sort("session_date")) != row.complete_daily_hash:
            raise ValueError(f"Readiness daily hash mismatch: {symbol}")
    if (
        eligibility_start
        and readiness.requested_start
        and eligibility_start < date.fromisoformat(readiness.requested_start)
    ):
        raise ValueError("Eligibility starts before the audited readiness window.")
    if (
        eligibility_end
        and readiness.requested_end
        and eligibility_end > date.fromisoformat(readiness.requested_end)
    ):
        raise ValueError("Eligibility ends after the audited readiness window.")


def _safe_batch_relative_path(value: str, field_name: str) -> str:
    path = Path(value)
    if path.is_absolute() or ".." in path.parts or path.as_posix() != value:
        raise ValueError(f"Unsafe semantic batch {field_name}: {value}")
    return value


def _verify_svg_metadata(chart_path: Path, card: dict[str, Any]) -> None:
    try:
        root = ElementTree.fromstring(chart_path.read_text(encoding="utf-8"))
        metadata_node = root.find("{http://www.w3.org/2000/svg}metadata")
        if metadata_node is None or metadata_node.text is None:
            raise ValueError("missing SVG metadata")
        metadata = json.loads(metadata_node.text)
    except (ElementTree.ParseError, json.JSONDecodeError, ValueError) as exc:
        raise ValueError(f"Invalid chart metadata: {card['card_id']}") from exc
    expected = {
        "renderer_version": "classical_rectangle_svg_v1",
        "signal_id": card["signal_id"],
        "definition_hash": card["config_hash"],
        "source_slice_hash": card["source_slice_hash"],
        "last_bar_date": card["breakout_date"],
    }
    for key, value in expected.items():
        if metadata.get(key) != value:
            raise ValueError(f"Chart metadata mismatch for {card['card_id']}: {key}")
    bar_count = metadata.get("bar_count")
    if not isinstance(bar_count, int) or not 1 <= bar_count <= 81:
        raise ValueError(f"Invalid chart bar_count: {card['card_id']}")
    if metadata.get("first_bar_date", "") > metadata["last_bar_date"]:
        raise ValueError(f"Invalid chart date range: {card['card_id']}")


def _select_review_signals(
    signals: Sequence[RectangleSignal],
    *,
    batch_size: int,
    sampling_seed: str,
) -> list[RectangleSignal]:
    buckets: dict[tuple[str, int, str, str], list[RectangleSignal]] = defaultdict(list)
    for signal in signals:
        candidate = signal.candidate
        buckets[
            (
                candidate.direction.value,
                candidate.lookback_sessions,
                _quality_band(signal),
                "diagnostic" if _review_diagnostic_codes(signal) else "clean",
            )
        ].append(signal)
    for cohort in buckets.values():
        cohort.sort(key=lambda item: _sampling_key(item, sampling_seed))

    selected: list[RectangleSignal] = []
    selected_ids: set[str] = set()
    symbol_counts: Counter[str] = Counter()
    unique_symbols = {signal.candidate.symbol for signal in signals}
    symbol_cap = max(1, ceil(batch_size / max(1, len(unique_symbols))))
    while len(selected) < batch_size:
        progressed = False
        for key in sorted(buckets):
            cohort = buckets[key]
            while cohort and cohort[0].signal_id in selected_ids:
                cohort.pop(0)
            eligible_index = next(
                (
                    index
                    for index, item in enumerate(cohort)
                    if symbol_counts[item.candidate.symbol] < symbol_cap
                ),
                None,
            )
            if eligible_index is None:
                continue
            signal = cohort.pop(eligible_index)
            selected.append(signal)
            selected_ids.add(signal.signal_id)
            symbol_counts[signal.candidate.symbol] += 1
            progressed = True
            if len(selected) >= batch_size:
                break
        if not progressed:
            break

    if len(selected) < batch_size:
        remainder = sorted(
            (signal for signal in signals if signal.signal_id not in selected_ids),
            key=lambda item: _sampling_key(item, sampling_seed),
        )
        selected.extend(remainder[: batch_size - len(selected)])
    return selected


def _quality_band(signal: RectangleSignal) -> str:
    candidate = signal.candidate
    if (
        candidate.minimum_touch_count >= 3
        or candidate.touch_alternations >= 4
        or candidate.center_close_containment >= 0.9
    ):
        return "strong_geometry"
    return "threshold_geometry"


def _sampling_key(signal: RectangleSignal, seed: str) -> tuple[str, str]:
    digest = hashlib.sha256(f"{seed}|{signal.signal_id}".encode("utf-8")).hexdigest()
    return digest, signal.signal_id


def _card_payload(
    signal: RectangleSignal,
    *,
    batch_id: str,
    card_id: str,
    config_hash: str,
    source_slice_hash: str,
    chart_path: str,
    chart_hash: str,
) -> dict[str, Any]:
    candidate = signal.candidate
    return {
        "schema_version": "ClassicalPatternSemanticReviewCardV1",
        "batch_id": batch_id,
        "card_id": card_id,
        "signal_id": signal.signal_id,
        "candidate_id": candidate.candidate_id,
        "config_hash": config_hash,
        "symbol": candidate.symbol,
        "direction": candidate.direction.value,
        "visible_as_of": candidate.breakout_time.isoformat(),
        "breakout_date": candidate.breakout_date.isoformat(),
        "pattern_start_date": candidate.pattern_start_date.isoformat(),
        "pattern_end_date": candidate.pattern_end_date.isoformat(),
        "lookback_sessions": candidate.lookback_sessions,
        "quality_band": _quality_band(signal),
        "diagnostic_band": "diagnostic" if _review_diagnostic_codes(signal) else "clean",
        "levels": {
            "upper_boundary": candidate.upper_boundary,
            "lower_boundary": candidate.lower_boundary,
            "upper_edge": candidate.upper_edge,
            "lower_edge": candidate.lower_edge,
            "breakout_boundary": candidate.breakout_boundary,
            "last_full_day": candidate.lfd_date.isoformat(),
            "last_full_day_high": candidate.lfd_high,
            "last_full_day_low": candidate.lfd_low,
            "raw_lfd_stop": candidate.base_stop,
        },
        "geometry": {
            "height_atr": candidate.height_atr,
            "minimum_touch_count": candidate.minimum_touch_count,
            "touch_alternations": candidate.touch_alternations,
            "center_close_containment": candidate.center_close_containment,
            "boundary_dispersion": candidate.boundary_dispersion,
            "close_drift_fraction": candidate.close_drift_fraction,
            "latest_touch_age_sessions": candidate.latest_touch_age_sessions,
        },
        "breakout_bar_diagnostic_codes": _review_diagnostic_codes(signal),
        "source_slice_hash": source_slice_hash,
        "chart_path": chart_path,
        "chart_hash": chart_hash,
    }


def _render_card_markdown(payload: dict[str, Any]) -> str:
    levels = payload["levels"]
    geometry = payload["geometry"]
    return "\n".join(
        [
            "---",
            f"batch_id: {payload['batch_id']}",
            f"card_id: {payload['card_id']}",
            f"signal_id: {payload['signal_id']}",
            f"config_hash: {payload['config_hash']}",
            f"symbol: {payload['symbol']}",
            f"direction: {payload['direction']}",
            f"visible_as_of: {payload['visible_as_of']}",
            "---",
            "",
            f"# {payload['symbol']} {payload['direction'].upper()} — {payload['breakout_date']}",
            "",
            f"![As-of chart](../{payload['chart_path']})",
            "",
            "## Machine Read",
            "",
            f"- Base: `{payload['pattern_start_date']}` through `{payload['pattern_end_date']}` ({payload['lookback_sessions']} sessions)",
            f"- Central boundaries: `{levels['lower_boundary']:.4f}` to `{levels['upper_boundary']:.4f}`",
            f"- Last Full Day: `{levels['last_full_day']}`; raw stop `{levels['raw_lfd_stop']:.4f}`",
            f"- Geometry: `{geometry['minimum_touch_count']}` minimum-side touches, `{geometry['touch_alternations']}` alternations, `{geometry['height_atr']:.2f}` ATR high",
            "",
            "## Blind Review",
            "",
            "Record answers in `../review_responses.csv`.",
            "",
            "1. Is this a genuine horizontal rectangle at the stated cutoff?",
            "2. Are the upper and lower boundaries faithful to the visual structure?",
            "3. Is the marked Last Full Day the correct risk reference?",
            "4. Is the close-confirmed breakout semantically valid?",
            "5. Overall decision: accept, revise, reject, or ambiguous. Why?",
            "",
            "Subsequent bars and economic results are intentionally absent.",
            "",
        ]
    )


def _render_review_index(batch_id: str, records: Sequence[ReviewCardRecord]) -> str:
    lines = [
        f"# Classical Rectangle Semantic Review — {batch_id}",
        "",
        "Review each card from the as-of chart only. Results after the displayed cutoff are hidden.",
        "Write decisions into `review_responses.csv`; do not edit the cards or manifest.",
        "",
    ]
    for index, record in enumerate(records, start=1):
        lines.append(
            f"{index}. [{record.symbol} {record.direction} {record.breakout_date}]({record.card_path})"
        )
    lines.extend(["", "This packet is semantic-pilot evidence only and is not executable.", ""])
    return "\n".join(lines)


def _write_response_template(records: Sequence[ReviewCardRecord], path: Path) -> None:
    fields = [
        "batch_id",
        "card_id",
        "signal_id",
        "config_hash",
        "card_hash",
        "decision",
        "rectangle_fidelity",
        "boundary_fidelity",
        "lfd_fidelity",
        "breakout_fidelity",
        "reason_codes",
        "upper_boundary_revision",
        "lower_boundary_revision",
        "lfd_date_revision",
        "note",
        "reviewer",
        "reviewed_at",
        "outcome_hidden_attestation",
        "no_future_consulted_attestation",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for record in records:
            writer.writerow(
                {
                    "batch_id": record.batch_id,
                    "card_id": record.card_id,
                    "signal_id": record.signal_id,
                    "config_hash": record.config_hash,
                    "card_hash": record.card_hash,
                }
            )


def _render_signal_svg(
    signal: RectangleSignal,
    frame: pl.DataFrame,
    *,
    definition_hash: str,
    source_slice_hash: str,
) -> str:
    candidate = signal.candidate
    rows = frame.sort("session_date").to_dicts()
    width, height = 1200, 700
    left, right, top, bottom = 80.0, 145.0, 45.0, 90.0
    plot_width = width - left - right
    plot_height = height - top - bottom
    levels = [
        candidate.upper_edge,
        candidate.lower_edge,
        candidate.base_stop,
    ]
    min_price = min([float(row["low"]) for row in rows] + levels)
    max_price = max([float(row["high"]) for row in rows] + levels)
    padding = max((max_price - min_price) * 0.05, candidate.atr * 0.25)
    min_price -= padding
    max_price += padding

    def x(index: int) -> float:
        return left + (index + 0.5) * plot_width / len(rows)

    def y(price: float) -> float:
        return top + (max_price - price) / (max_price - min_price) * plot_height

    candle_width = max(2.0, min(9.0, plot_width / len(rows) * 0.58))
    elements = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        "<metadata>"
        + escape(
            json.dumps(
                {
                    "renderer_version": "classical_rectangle_svg_v1",
                    "signal_id": signal.signal_id,
                    "definition_hash": definition_hash,
                    "source_slice_hash": source_slice_hash,
                    "first_bar_date": _as_date(rows[0]["session_date"]).isoformat(),
                    "last_bar_date": _as_date(rows[-1]["session_date"]).isoformat(),
                    "bar_count": len(rows),
                },
                sort_keys=True,
                separators=(",", ":"),
            )
        )
        + "</metadata>",
        '<rect width="100%" height="100%" fill="#0f172a"/>',
        f'<text x="{left}" y="28" fill="#e2e8f0" font-family="sans-serif" font-size="18">{escape(candidate.symbol)} {candidate.direction.value.upper()} as of {candidate.breakout_date.isoformat()}</text>',
    ]
    for step in range(6):
        price = min_price + step * (max_price - min_price) / 5
        y_value = y(price)
        elements.append(
            f'<line x1="{left}" x2="{width-right}" y1="{y_value:.2f}" y2="{y_value:.2f}" stroke="#334155" stroke-width="1"/>'
        )
        elements.append(
            f'<text x="8" y="{y_value+4:.2f}" fill="#94a3b8" font-family="monospace" font-size="12">{price:.2f}</text>'
        )
    start_offset = next(
        index for index, row in enumerate(rows) if _as_date(row["session_date"]) >= candidate.pattern_start_date
    )
    end_offset = next(
        index for index, row in enumerate(rows) if _as_date(row["session_date"]) >= candidate.pattern_end_date
    )
    box_x = x(start_offset) - candle_width
    box_width = x(end_offset) - x(start_offset) + 2 * candle_width
    elements.append(
        f'<rect x="{box_x:.2f}" y="{y(candidate.upper_edge):.2f}" width="{box_width:.2f}" height="{y(candidate.lower_edge)-y(candidate.upper_edge):.2f}" fill="#38bdf8" fill-opacity="0.05" stroke="#38bdf8" stroke-width="1.5"/>'
    )
    global_start_index = candidate.breakout_index - (len(rows) - 1)
    lfd_offset = candidate.lfd_index - global_start_index
    if 0 <= lfd_offset < len(rows):
        elements.append(
            f'<rect x="{x(lfd_offset)-candle_width:.2f}" y="{top:.2f}" width="{2*candle_width:.2f}" height="{plot_height:.2f}" fill="#f59e0b" fill-opacity="0.10"/>'
        )
    elements.append(
        f'<rect x="{x(len(rows)-1)-candle_width:.2f}" y="{top:.2f}" width="{2*candle_width:.2f}" height="{plot_height:.2f}" fill="#e2e8f0" fill-opacity="0.08"/>'
    )
    for index, row in enumerate(rows):
        open_price = float(row["open"])
        close_price = float(row["close"])
        high = float(row["high"])
        low = float(row["low"])
        color = "#22c55e" if close_price >= open_price else "#ef4444"
        x_value = x(index)
        elements.append(
            f'<line x1="{x_value:.2f}" x2="{x_value:.2f}" y1="{y(high):.2f}" y2="{y(low):.2f}" stroke="{color}" stroke-width="1"/>'
        )
        body_top = min(y(open_price), y(close_price))
        body_height = max(1.0, abs(y(open_price) - y(close_price)))
        elements.append(
            f'<rect x="{x_value-candle_width/2:.2f}" y="{body_top:.2f}" width="{candle_width:.2f}" height="{body_height:.2f}" fill="{color}"/>'
        )
        if index % max(1, len(rows) // 8) == 0 or index == len(rows) - 1:
            label = _as_date(row["session_date"]).strftime("%m-%d")
            elements.append(
                f'<text x="{x_value:.2f}" y="{height-38}" fill="#94a3b8" font-family="monospace" font-size="10" text-anchor="middle">{label}</text>'
            )

    for touch_index, touch_price, color in [
        *[(index, candidate.upper_boundary, "#67e8f9") for index in candidate.upper_touch_indices],
        *[(index, candidate.lower_boundary, "#67e8f9") for index in candidate.lower_touch_indices],
    ]:
        local_index = touch_index - global_start_index
        if 0 <= local_index < len(rows):
            elements.append(
                f'<circle cx="{x(local_index):.2f}" cy="{y(touch_price):.2f}" r="4" fill="none" stroke="{color}" stroke-width="2"/>'
            )

    for price, label, color, dash in (
        (candidate.upper_boundary, "upper", "#38bdf8", ""),
        (candidate.lower_boundary, "lower", "#38bdf8", ""),
        (candidate.base_stop, "LFD", "#f59e0b", "6,4"),
    ):
        dash_attr = f' stroke-dasharray="{dash}"' if dash else ""
        elements.append(
            f'<line x1="{left}" x2="{width-right}" y1="{y(price):.2f}" y2="{y(price):.2f}" stroke="{color}" stroke-width="1.5"{dash_attr}/>'
        )
        elements.append(
            f'<text x="{width-right+8}" y="{y(price)+4:.2f}" fill="{color}" font-family="sans-serif" font-size="12">{label} {price:.2f}</text>'
        )
    elements.append("</svg>\n")
    return "\n".join(elements)


def _assert_outcome_hidden(value: Any, *, path: str = "root") -> None:
    if isinstance(value, dict):
        forbidden = set(value) & FORBIDDEN_REVIEW_KEYS
        if forbidden:
            raise ValueError(f"Outcome-dependent review fields at {path}: {sorted(forbidden)}")
        for key, item in value.items():
            _assert_outcome_hidden(item, path=f"{path}.{key}")
    elif isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            _assert_outcome_hidden(item, path=f"{path}[{index}]")


def _assert_exact_keys(value: dict[str, Any], expected: set[str], section: str) -> None:
    unknown = set(value) - expected
    missing = expected - set(value)
    if unknown or missing:
        raise ValueError(
            f"Invalid semantic review {section} keys: unknown={sorted(unknown)}, missing={sorted(missing)}"
        )


def _review_diagnostic_codes(signal: RectangleSignal) -> tuple[str, ...]:
    allowed = {"breakout_bar_spans_lfd", "breakout_bar_spans_negation"}
    return tuple(
        code
        for code in signal.candidate.breakout_bar_diagnostic_codes
        if code in allowed
    )


def _sha256_path(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _as_date(value: object) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))
