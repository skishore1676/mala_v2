"""Outcome-hidden semantic review packets for classical rectangle signals."""

from __future__ import annotations

import base64
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

from .contracts import EnumerationRecord, RectangleResearchConfig, RectangleSignal
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
CALIBRATION_RESPONSE_FIELDS = frozenset(
    {
        "batch_id",
        "card_id",
        "config_hash",
        "card_hash",
        "reviewer_id",
        "review_pass",
        "strict_rectangle_validity",
        "as_of_trade_worthiness",
        "note",
        "reviewed_at",
        "outcome_hidden_attestation",
        "no_future_consulted_attestation",
    }
)
STRICT_RECTANGLE_VALIDITY = frozenset({"valid", "invalid", "ambiguous"})
AS_OF_TRADE_WORTHINESS = frozenset({"trade", "watch", "no_trade", "ambiguous"})


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


@dataclass(frozen=True, slots=True)
class CalibrationCardRecordV2:
    """Public card identity.  Its hidden class is kept only in the manifest's private case map."""

    schema_version: str
    batch_id: str
    card_id: str
    source_id: str
    config_hash: str
    symbol: str
    evaluation_date: str
    visible_as_of: str
    lookback_sessions: int
    displayed_bar_count: int
    source_slice_hash: str
    chart_path: str
    chart_hash: str
    card_path: str
    card_hash: str


@dataclass(frozen=True, slots=True)
class SemanticCalibrationBatchResultV2:
    batch_id: str
    output_dir: Path
    selected_count: int
    manifest_path: Path
    receipt_path: Path
    review_index_path: Path
    response_template_path: Path
    canonical_hash: str


@dataclass(frozen=True, slots=True)
class CalibrationIngestionResultV2:
    batch_id: str
    status: str
    reviewed_count: int
    decision_log_path: Path
    scorecard_path: Path


@dataclass(frozen=True, slots=True)
class _CalibrationCase:
    hidden_class: str
    source_id: str
    symbol: str
    evaluation_index: int
    evaluation_date: date
    visible_as_of: datetime
    lookback_sessions: int
    hidden_reason: str | None
    signal: RectangleSignal | None
    record: EnumerationRecord | None

    @property
    def correlation_key(self) -> tuple[str, date, int]:
        return (self.symbol, self.evaluation_date, self.lookback_sessions)


def render_obsidian_review_card(*, batch_dir: Path, output_path: Path) -> Path:
    """Render a self-contained, outcome-hidden adjudication note for Lathi Bus."""

    batch_dir = batch_dir.expanduser().resolve()
    receipt = verify_semantic_batch(batch_dir)
    manifest = json.loads((batch_dir / "batch_manifest.json").read_text(encoding="utf-8"))
    cards = sorted(
        manifest["cards"],
        key=lambda card: (card["breakout_date"], card["symbol"], card["card_id"]),
    )
    lines = [
        "# Classical Rectangle Adjudication — Round 1",
        "",
        "Ten machine-detected daily rectangles are shown below using bars available",
        "only through each close-confirmed breakout. Subsequent prices, outcomes,",
        "trades, and P&L remain hidden.",
        "",
        "## How to review",
        "",
        "- **No pointy comment on a chart means AGREE.**",
        "- If anything is wrong, add one correction in angle brackets on that",
        "  chart's `Pointy correction` line.",
        "- Comment on the rectangle, boundaries, breakout, or Last Full Day only.",
        "- If you leave any corrections, choose **Revise** at the bottom. Otherwise",
        "  choose **Approve**. Park if you want to return later.",
        "- Every uncommented chart remains accepted even when the batch is revised.",
        "",
        "## Chart legend",
        "",
        "- Cyan box and lines: proposed rectangle and central boundaries",
        "- Cyan circles: causal touch anchors",
        "- Orange band/dashed line: proposed Last Full Day and raw risk reference",
        "- Pale final band: close-confirmed breakout bar",
        "",
        f"Batch hash: `{receipt['canonical_hash']}`",
        "",
        "---",
        "",
    ]
    for index, card in enumerate(cards, start=1):
        chart_relative = _safe_batch_relative_path(card["chart_path"], "chart_path")
        chart_bytes = (batch_dir / chart_relative).read_bytes()
        chart_uri = base64.b64encode(chart_bytes).decode("ascii")
        levels = card["levels"]
        lines.extend(
            [
                f"### {index:02d} · {card['symbol']} {card['direction'].upper()} · {card['breakout_date']}",
                f"<!-- classical-pattern-card:{card['card_id']} signal:{card['signal_id']} -->",
                "",
                f"**Machine proposal:** valid {card['direction']} rectangle breakout.",
                f"**Base:** {card['pattern_start_date']} through {card['pattern_end_date']} "
                f"({card['lookback_sessions']} sessions).",
                f"**Central boundaries:** {levels['lower_boundary']:.2f} to "
                f"{levels['upper_boundary']:.2f}.",
                f"**Last Full Day:** {levels['last_full_day']}; raw risk reference "
                f"{levels['raw_lfd_stop']:.2f}.",
                "",
                f"![{card['symbol']} {card['direction']} rectangle](data:image/svg+xml;base64,{chart_uri})",
                "",
                "**Pointy correction (only if you disagree):**",
                "",
                "---",
                "",
            ]
        )
    lines.extend(
        [
            "## What happens next",
            "",
            "Mala will bind collected comments to the hidden card IDs above, retain",
            "accepted charts, and prepare only the corrected or next calibration cases.",
            "Economic outcomes remain hidden until the semantic definition is frozen.",
            "",
        ]
    )
    output_path = output_path.expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines), encoding="utf-8")
    return output_path


def render_calibration_obsidian_gate_v2(
    *, batch_dir: Path, output_path: Path, card_ids: Sequence[str]
) -> Path:
    """Render a small self-contained, outcome-hidden V2 doctrine gate."""

    batch_dir = batch_dir.expanduser().resolve()
    receipt = verify_semantic_calibration_batch_v2(batch_dir)
    manifest = json.loads((batch_dir / "batch_manifest.json").read_text(encoding="utf-8"))
    requested = [card_id.strip() for card_id in card_ids if card_id.strip()]
    if not requested:
        raise ValueError("At least one calibration card_id is required.")
    if len(requested) != len(set(requested)):
        raise ValueError("Calibration doctrine gate contains duplicate card identifiers.")
    if len(requested) > 6:
        raise ValueError("Calibration doctrine gate is capped at six cards.")
    cards_by_id = {card["card_id"]: card for card in manifest["cards"]}
    missing = [card_id for card_id in requested if card_id not in cards_by_id]
    if missing:
        raise ValueError(f"Unknown calibration card_id: {missing[0]}")

    lines = [
        "# Classical Rectangle Doctrine Gate — Round 2",
        "",
        "Two independent blind reviewers disagreed on these exact causal windows.",
        "No future bars, outcomes, trades, or P&L are shown. This is the only",
        "semantic input needed before the autonomous calibration loop continues.",
        "",
        "## How to review",
        "",
        "- Add one pointy comment on every chart's `Your call` line.",
        "- Put two labels in the comment: strict rectangle validity, then as-of action.",
        "- Validity labels: `valid`, `invalid`, or `ambiguous`.",
        "- Action labels: `trade`, `watch`, `no_trade`, or `ambiguous`.",
        "- Example comment content: `valid; watch`.",
        "- Add a short reason only when the labels do not capture your doctrine.",
        "- Choose **Revise** after all charts have a call; Mala will collect the comments.",
        "",
        f"Batch hash: `{receipt['canonical_hash']}`",
        "",
        "---",
        "",
    ]
    for sequence, card_id in enumerate(requested, start=1):
        card = cards_by_id[card_id]
        chart_relative = _safe_batch_relative_path(card["chart_path"], "chart_path")
        encoded = base64.b64encode((batch_dir / chart_relative).read_bytes()).decode("ascii")
        lines.extend(
            [
                f"### {sequence:02d} · {card['symbol']} · {card['evaluation_date']}",
                f"<!-- classical-pattern-calibration-card:{card_id} -->",
                "",
                f"**Candidate window:** {card['displayed_bar_count']} sessions through the evaluation cutoff.",
                "",
                f"![{card['symbol']} causal candidate](data:image/svg+xml;base64,{encoded})",
                "",
                "**Your call (add one pointy comment):**",
                "",
                "---",
                "",
            ]
        )
    lines.extend(
        [
            "## What happens next",
            "",
            "Mala will bind each collected call to the stable hidden card ID, freeze the",
            "human-owned doctrine, and continue the next semantic round autonomously.",
            "Economic testing remains blocked until that semantic definition is stable.",
            "",
        ]
    )
    output_path = output_path.expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines), encoding="utf-8")
    return output_path


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


def build_semantic_calibration_batch_v2(
    daily_by_symbol: dict[str, pl.DataFrame],
    *,
    config: RectangleResearchConfig,
    readiness: DataReadinessReport,
    output_dir: Path,
    batch_id: str,
    confirmed_signal_count: int = 6,
    qualified_no_trigger_count: int = 6,
    rejected_geometry_count: int = 6,
    sampling_seed: str = "classical-rectangle-semantic-calibration-v2",
    eligibility_start: date | None = None,
    eligibility_end: date | None = None,
    exclude_manifests: Sequence[Path] = (),
) -> SemanticCalibrationBatchResultV2:
    """Build a class-hidden, outcome-hidden mixed calibration packet.

    This is deliberately a new projection schema, not a change to v1 detector
    contracts or v1 semantic-review artifacts.
    """

    requested_counts = {
        "confirmed_signal": confirmed_signal_count,
        "qualified_no_trigger": qualified_no_trigger_count,
        "rejected_geometry": rejected_geometry_count,
    }
    if not batch_id.strip():
        raise ValueError("batch_id is required.")
    if any(not isinstance(value, int) or value <= 0 for value in requested_counts.values()):
        raise ValueError("Every v2 calibration class count must be a positive integer.")
    if readiness.semantic_review_status != "ready":
        raise ValueError("Readiness report does not authorize a semantic review pilot.")
    validate_readiness_report(readiness)
    if readiness.config_hash != config.source_hash:
        raise ValueError("Readiness/config hash mismatch.")

    output_dir = output_dir.expanduser().resolve()
    if output_dir.exists() and any(output_dir.iterdir()):
        raise ValueError("Semantic calibration output_dir must be absent or empty; use a new batch root.")
    _validate_readiness_alignment(
        readiness,
        daily_by_symbol,
        eligibility_start=eligibility_start,
        eligibility_end=eligibility_end,
    )
    excluded_source_ids, exclusion_contract = _load_calibration_exclusions_v2(
        exclude_manifests
    )

    pools: dict[str, list[_CalibrationCase]] = {name: [] for name in requested_counts}
    source_hashes: dict[str, str] = {}
    enumerated_counts: dict[str, dict[str, int]] = {}
    frames: dict[str, pl.DataFrame] = {}
    ineligible = Counter[str]()
    for symbol in sorted(daily_by_symbol):
        daily = daily_by_symbol[symbol].sort("session_date")
        frames[symbol] = daily
        source_hashes[symbol] = hash_daily_bars(daily)
        result = enumerate_rectangles(daily, config)
        enumerated_counts[symbol] = {
            "signals": len(result.signals),
            "valid_no_breakout": sum(record.status == "valid_no_breakout" for record in result.records),
            "insufficient_confirmed_boundary_touches": sum(
                record.status == "rejected" and record.reason == "insufficient_confirmed_boundary_touches"
                for record in result.records
            ),
        }
        for signal in result.signals:
            candidate = signal.candidate
            if signal.signal_id in excluded_source_ids:
                ineligible["excluded_prior_manifest_identity"] += 1
                continue
            reason = _calibration_case_eligibility_reason(
                daily, candidate.breakout_index, candidate.lookback_sessions,
                candidate.breakout_date, config, eligibility_start, eligibility_end,
            )
            if reason:
                ineligible[reason] += 1
                continue
            pools["confirmed_signal"].append(
                _CalibrationCase(
                    hidden_class="confirmed_signal",
                    source_id=signal.signal_id,
                    symbol=candidate.symbol,
                    evaluation_index=candidate.breakout_index,
                    evaluation_date=candidate.breakout_date,
                    visible_as_of=candidate.breakout_time,
                    lookback_sessions=candidate.lookback_sessions,
                    hidden_reason="close_confirmed_breakout",
                    signal=signal,
                    record=None,
                )
            )
        for record in result.records:
            if record.status == "valid_no_breakout":
                hidden_class = "qualified_no_trigger"
            elif (
                record.status == "rejected"
                and record.reason == "insufficient_confirmed_boundary_touches"
            ):
                hidden_class = "rejected_geometry"
            else:
                continue
            if record.record_id in excluded_source_ids:
                ineligible["excluded_prior_manifest_identity"] += 1
                continue
            reason = _calibration_case_eligibility_reason(
                daily, record.breakout_index, record.lookback_sessions,
                record.breakout_date, config, eligibility_start, eligibility_end,
            )
            if reason:
                ineligible[reason] += 1
                continue
            visible_at = daily.row(record.breakout_index, named=True)["visible_at"]
            if not isinstance(visible_at, datetime):
                visible_at = datetime.fromisoformat(str(visible_at))
            pools[hidden_class].append(
                _CalibrationCase(
                    hidden_class=hidden_class,
                    source_id=record.record_id,
                    symbol=record.symbol,
                    evaluation_index=record.breakout_index,
                    evaluation_date=record.breakout_date,
                    visible_as_of=visible_at,
                    lookback_sessions=record.lookback_sessions,
                    hidden_reason=record.reason,
                    signal=None,
                    record=record,
                )
            )

    selected = _select_calibration_cases_v2(
        pools, requested_counts=requested_counts, sampling_seed=sampling_seed
    )
    selected_source_ids = {case.source_id for case in selected}
    overlap = selected_source_ids & excluded_source_ids
    if overlap:
        raise RuntimeError("Excluded prior-manifest source identities reached v2 selection.")
    exclusion_contract["excluded_selected_overlap"] = len(overlap)
    charts_dir = output_dir / "charts"
    cards_dir = output_dir / "cards"
    charts_dir.mkdir(parents=True, exist_ok=True)
    cards_dir.mkdir(parents=True, exist_ok=True)
    records: list[CalibrationCardRecordV2] = []
    private_cases: list[dict[str, Any]] = []
    for case in selected:
        daily = frames[case.symbol]
        # Reviewers must judge the same causal window that produced the hidden
        # detector class. Showing a longer generic context lets them select a
        # different structure, which makes the blind label comparison invalid.
        start_index = case.evaluation_index - case.lookback_sessions
        as_of_slice = daily.slice(start_index, case.evaluation_index - start_index + 1)
        if _as_date(as_of_slice.get_column("session_date").max()) != case.evaluation_date:
            raise RuntimeError(f"As-of slice failed for {case.source_id}")
        card_id = hashlib.sha256(
            f"{batch_id}|{config.source_hash}|{case.hidden_class}|{case.source_id}".encode("utf-8")
        ).hexdigest()[:16]
        source_slice_hash = hash_daily_bars(as_of_slice)
        chart_path = charts_dir / f"{card_id}.svg"
        chart_path.write_text(
            _render_neutral_ohlc_svg(
                as_of_slice,
                card_id=card_id,
                definition_hash=config.source_hash,
                source_slice_hash=source_slice_hash,
            ),
            encoding="utf-8",
        )
        chart_hash = _sha256_path(chart_path)
        card_payload = {
            "schema_version": "ClassicalPatternSemanticCalibrationCardV2",
            "batch_id": batch_id,
            "card_id": card_id,
            "source_id": case.source_id,
            "config_hash": config.source_hash,
            "symbol": case.symbol,
            "evaluation_date": case.evaluation_date.isoformat(),
            "visible_as_of": case.visible_as_of.isoformat(),
            "lookback_sessions": case.lookback_sessions,
            "displayed_bar_count": len(as_of_slice),
            "source_slice_hash": source_slice_hash,
            "chart_path": f"charts/{card_id}.svg",
            "chart_hash": chart_hash,
        }
        _assert_outcome_hidden(card_payload)
        card_path = cards_dir / f"{card_id}.md"
        card_path.write_text(_render_calibration_card_v2(card_payload), encoding="utf-8")
        card_hash = _sha256_path(card_path)
        records.append(
            CalibrationCardRecordV2(
                **card_payload,
                card_path=f"cards/{card_id}.md",
                card_hash=card_hash,
            )
        )
        private_cases.append(
            {
                "card_id": card_id,
                "hidden_class": case.hidden_class,
                "hidden_reason": case.hidden_reason,
                "causal_attempt_direction": (
                    case.signal.candidate.direction.value if case.signal is not None else None
                ),
                "causal_diagnostics": _calibration_diagnostics(case, daily, config),
            }
        )

    records.sort(key=lambda record: record.card_id)
    private_cases.sort(key=lambda item: item["card_id"])
    manifest = {
        "schema_version": "ClassicalPatternSemanticCalibrationBatchV2",
        "batch_id": batch_id,
        "playbook_id": config.playbook_id,
        "config_hash": config.source_hash,
        "readiness_report_hash": readiness.report_hash,
        "sampling": {
            "version": "class_hidden_fixed_counts_v2",
            "seed": sampling_seed,
            "requested_counts": requested_counts,
            "selected_counts": dict(sorted(Counter(case.hidden_class for case in selected).items())),
            "dedupe_key": "symbol_evaluation_date_lookback_sessions",
        },
        "cutoff": {
            "eligibility_start": eligibility_start.isoformat() if eligibility_start else None,
            "eligibility_end": eligibility_end.isoformat() if eligibility_end else None,
            "chart_end": "evaluation_close_only",
        },
        "source_daily_hashes": source_hashes,
        "enumerated_counts": enumerated_counts,
        "ineligible_reasons": dict(sorted(ineligible.items())),
        "exclusions": exclusion_contract,
        "cards": [asdict(record) for record in records],
        "private_cases": private_cases,
    }
    _assert_outcome_hidden(manifest)
    manifest_path = output_dir / "batch_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    response_template_path = output_dir / "calibration_responses.template.csv"
    response_path = output_dir / "calibration_responses.csv"
    _write_calibration_response_template_v2(records, response_template_path)
    _write_calibration_response_template_v2(records, response_path)
    index_path = output_dir / "REVIEW_INDEX.md"
    index_path.write_text(_render_calibration_index_v2(batch_id, records), encoding="utf-8")
    artifacts = {
        path.relative_to(output_dir).as_posix(): _sha256_path(path)
        for path in sorted(
            [manifest_path, response_template_path, index_path, *charts_dir.glob("*.svg"), *cards_dir.glob("*.md")]
        )
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
        "schema_version": "ClassicalPatternSemanticCalibrationReceiptV2",
        **canonical_payload,
        "canonical_hash": canonical_hash,
        "status": "complete",
        "readiness": "semantic_calibration",
        "executable": False,
        "outcomes_hidden": True,
        "selected_count": len(records),
        "requested_counts": requested_counts,
        "exclusions": exclusion_contract,
        "forbidden_review_keys": sorted(FORBIDDEN_REVIEW_KEYS),
    }
    receipt_path = output_dir / "batch_receipt.json"
    receipt_path.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    verify_semantic_calibration_batch_v2(output_dir)
    return SemanticCalibrationBatchResultV2(
        batch_id=batch_id,
        output_dir=output_dir,
        selected_count=len(records),
        manifest_path=manifest_path,
        receipt_path=receipt_path,
        review_index_path=index_path,
        response_template_path=response_path,
        canonical_hash=canonical_hash,
    )


def verify_semantic_calibration_batch_v2(output_dir: Path) -> dict[str, Any]:
    """Verify the parallel v2 artifact inventory and its hidden-class boundary."""

    output_dir = output_dir.expanduser().resolve()
    receipt = json.loads((output_dir / "batch_receipt.json").read_text(encoding="utf-8"))
    _assert_exact_keys(
        receipt,
        {
            "schema_version", "batch_id", "config_hash", "readiness_report_hash", "artifacts",
            "canonical_hash", "status", "readiness", "executable", "outcomes_hidden",
            "selected_count", "requested_counts", "exclusions", "forbidden_review_keys",
        },
        "v2 receipt",
    )
    if (
        receipt["schema_version"] != "ClassicalPatternSemanticCalibrationReceiptV2"
        or receipt["status"] != "complete"
        or receipt["readiness"] != "semantic_calibration"
        or receipt["executable"] is not False
        or receipt["outcomes_hidden"] is not True
    ):
        raise ValueError("Semantic calibration receipt has an invalid safety contract.")
    manifest = json.loads((output_dir / "batch_manifest.json").read_text(encoding="utf-8"))
    _assert_exact_keys(
        manifest,
        {
            "schema_version", "batch_id", "playbook_id", "config_hash", "readiness_report_hash",
            "sampling", "cutoff", "source_daily_hashes", "enumerated_counts", "ineligible_reasons",
            "exclusions", "cards", "private_cases",
        },
        "v2 manifest",
    )
    if manifest["schema_version"] != "ClassicalPatternSemanticCalibrationBatchV2":
        raise ValueError("Unsupported semantic calibration manifest version.")
    _assert_exact_keys(
        manifest["sampling"],
        {"version", "seed", "requested_counts", "selected_counts", "dedupe_key"},
        "v2 sampling",
    )
    _assert_exact_keys(manifest["cutoff"], {"eligibility_start", "eligibility_end", "chart_end"}, "v2 cutoff")
    if manifest["sampling"]["version"] != "class_hidden_fixed_counts_v2":
        raise ValueError("Unsupported semantic calibration sampling version.")
    for field in ("batch_id", "config_hash", "readiness_report_hash"):
        if receipt[field] != manifest[field]:
            raise ValueError(f"Receipt/manifest identity mismatch: {field}")
    requested = manifest["sampling"]["requested_counts"]
    if set(requested) != {"confirmed_signal", "qualified_no_trigger", "rejected_geometry"}:
        raise ValueError("Invalid v2 requested class counts.")
    if manifest["sampling"]["selected_counts"] != requested:
        raise ValueError("Semantic calibration class counts do not meet the requested exact counts.")
    if receipt["requested_counts"] != requested or receipt["selected_count"] != sum(requested.values()):
        raise ValueError("Semantic calibration receipt count mismatch.")
    if receipt["exclusions"] != manifest["exclusions"]:
        raise ValueError("Semantic calibration receipt/manifest exclusion mismatch.")
    exclusions = manifest["exclusions"]
    _assert_exact_keys(
        exclusions,
        {
            "version", "manifest_content_hashes", "excluded_identity_count",
            "excluded_source_ids", "excluded_identity_hashes", "excluded_selected_overlap",
        },
        "v2 exclusions",
    )
    if exclusions["version"] != "prior_manifest_source_identity_v1":
        raise ValueError("Unsupported semantic calibration exclusion version.")
    manifest_hashes = exclusions["manifest_content_hashes"]
    excluded_source_ids = exclusions["excluded_source_ids"]
    identity_hashes = exclusions["excluded_identity_hashes"]
    if (
        not isinstance(manifest_hashes, list)
        or manifest_hashes != sorted(set(manifest_hashes))
        or not isinstance(identity_hashes, list)
        or identity_hashes != sorted(set(identity_hashes))
        or not isinstance(excluded_source_ids, list)
        or excluded_source_ids != sorted(set(excluded_source_ids))
        or any(not isinstance(value, str) or not value for value in excluded_source_ids)
        or any(not isinstance(value, str) or len(value) != 64 for value in [*manifest_hashes, *identity_hashes])
        or exclusions["excluded_identity_count"] != len(excluded_source_ids)
        or identity_hashes != sorted(
            hashlib.sha256(value.encode("utf-8")).hexdigest() for value in excluded_source_ids
        )
        or exclusions["excluded_selected_overlap"] != 0
    ):
        raise ValueError("Invalid semantic calibration exclusion contract.")
    expected_card_keys = {field.name for field in fields(CalibrationCardRecordV2)}
    expected_artifacts = {"batch_manifest.json", "calibration_responses.template.csv", "REVIEW_INDEX.md"}
    cards_by_id: dict[str, dict[str, Any]] = {}
    for card in manifest["cards"]:
        _assert_exact_keys(card, expected_card_keys, "v2 card")
        if card["schema_version"] != "ClassicalPatternSemanticCalibrationCardV2":
            raise ValueError("Unsupported semantic calibration card version.")
        if card["card_id"] in cards_by_id:
            raise ValueError("Semantic calibration batch contains duplicate card identifiers.")
        cards_by_id[card["card_id"]] = card
        chart_relative = _safe_batch_relative_path(card["chart_path"], "chart_path")
        card_relative = _safe_batch_relative_path(card["card_path"], "card_path")
        expected_artifacts.update({chart_relative, card_relative})
        if _sha256_path(output_dir / chart_relative) != card["chart_hash"]:
            raise ValueError(f"Card chart_hash mismatch: {card['card_id']}")
        if _sha256_path(output_dir / card_relative) != card["card_hash"]:
            raise ValueError(f"Card card_hash mismatch: {card['card_id']}")
        _verify_neutral_svg_metadata_v2(output_dir / chart_relative, card)
    selected_source_ids = {card["source_id"] for card in manifest["cards"]}
    if selected_source_ids & set(excluded_source_ids):
        raise ValueError("Selected public cards overlap excluded source identities.")
    private_ids: set[str] = set()
    hidden_counts: Counter[str] = Counter()
    for private in manifest["private_cases"]:
        _assert_exact_keys(
            private,
            {"card_id", "hidden_class", "hidden_reason", "causal_attempt_direction", "causal_diagnostics"},
            "v2 private case",
        )
        if private["card_id"] not in cards_by_id or private["card_id"] in private_ids:
            raise ValueError("Invalid v2 private case card identity.")
        if private["hidden_class"] not in requested:
            raise ValueError("Invalid v2 hidden class.")
        if private["hidden_class"] == "rejected_geometry" and private["causal_attempt_direction"] is not None:
            raise ValueError("Rejected geometry may not claim an attempt direction.")
        _assert_exact_keys(
            private["causal_diagnostics"],
            {
                "anchor_span_sessions", "depth_percent", "touch_structure",
                "prior_central_rail_excursion_count", "prior_full_trigger_close_count",
            },
            "v2 causal diagnostics",
        )
        touch_structure = private["causal_diagnostics"]["touch_structure"]
        if touch_structure is not None:
            _assert_exact_keys(
                touch_structure,
                {"upper_touch_count", "lower_touch_count", "alternations"},
                "v2 touch structure",
            )
        private_ids.add(private["card_id"])
        hidden_counts[private["hidden_class"]] += 1
    if private_ids != set(cards_by_id) or dict(sorted(hidden_counts.items())) != requested:
        raise ValueError("V2 private case inventory does not reconcile.")
    if set(receipt["artifacts"]) != expected_artifacts:
        raise ValueError("Receipt artifact inventory does not exactly match the v2 manifest.")
    for relative_path, expected_hash in receipt["artifacts"].items():
        path = (output_dir / relative_path).resolve()
        if output_dir not in path.parents or not path.is_file():
            raise ValueError(f"Missing or unsafe semantic calibration artifact: {relative_path}")
        if _sha256_path(path) != expected_hash:
            raise ValueError(f"Semantic calibration artifact hash mismatch: {relative_path}")
    canonical_payload = {
        "batch_id": manifest["batch_id"],
        "config_hash": manifest["config_hash"],
        "readiness_report_hash": manifest["readiness_report_hash"],
        "artifacts": receipt["artifacts"],
    }
    expected_hash = hashlib.sha256(
        json.dumps(canonical_payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    if receipt["canonical_hash"] != expected_hash:
        raise ValueError("Semantic calibration canonical hash mismatch.")
    if receipt["forbidden_review_keys"] != sorted(FORBIDDEN_REVIEW_KEYS):
        raise ValueError("Semantic calibration forbidden-key contract mismatch.")
    _assert_outcome_hidden(manifest)
    return receipt


def validate_calibration_review_response_v2(row: dict[str, str], manifest: dict[str, Any]) -> None:
    """Validate a v2 response without revealing the hidden calibration class."""

    if set(row) != CALIBRATION_RESPONSE_FIELDS:
        raise ValueError("Calibration review response fields do not match V2 contract.")
    cards = {card["card_id"]: card for card in manifest.get("cards", [])}
    card = cards.get(row["card_id"])
    if card is None:
        raise ValueError("Unknown calibration review card.")
    for field in ("batch_id", "config_hash", "card_hash"):
        expected = manifest[field] if field in manifest else card[field]
        if row[field] != str(expected):
            raise ValueError(f"Stale or mismatched calibration response: {field}")
    if row["strict_rectangle_validity"] not in STRICT_RECTANGLE_VALIDITY:
        raise ValueError("Invalid strict_rectangle_validity.")
    if row["as_of_trade_worthiness"] not in AS_OF_TRADE_WORTHINESS:
        raise ValueError("Invalid as_of_trade_worthiness.")
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


def ingest_calibration_review_responses_v2(
    *, batch_dir: Path, responses_csv: Path | None = None
) -> CalibrationIngestionResultV2:
    """Append v2 responses, scoped by batch/card/reviewer/pass, without economic fields."""

    batch_dir = batch_dir.expanduser().resolve()
    verify_semantic_calibration_batch_v2(batch_dir)
    manifest = json.loads((batch_dir / "batch_manifest.json").read_text(encoding="utf-8"))
    responses_path = responses_csv or batch_dir / "calibration_responses.csv"
    with responses_path.open(newline="", encoding="utf-8") as handle:
        rows = [
            dict(row)
            for row in csv.DictReader(handle)
            if row.get("strict_rectangle_validity", "").strip()
        ]
    decisions_dir = batch_dir / "calibration_decisions"
    decisions_dir.mkdir(parents=True, exist_ok=True)
    log_path = decisions_dir / "review_decisions.jsonl"
    existing: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    if log_path.exists():
        for line in log_path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            record = json.loads(line)
            expected_fields = set(CALIBRATION_RESPONSE_FIELDS) | {"schema_version", "response_id"}
            if set(record) != expected_fields or record["schema_version"] != "RectangleSemanticCalibrationResponseV2":
                raise ValueError("Existing calibration decision fields do not match V2 contract.")
            response = {field: record[field] for field in CALIBRATION_RESPONSE_FIELDS}
            validate_calibration_review_response_v2(response, manifest)
            response_id = hashlib.sha256(
                json.dumps(response, sort_keys=True, separators=(",", ":")).encode("utf-8")
            ).hexdigest()
            if record["response_id"] != response_id:
                raise ValueError("Existing calibration decision response_id mismatch.")
            key = (response["batch_id"], response["card_id"], response["reviewer_id"], response["review_pass"])
            if key in existing:
                raise ValueError("Duplicate existing calibration review identity.")
            existing[key] = record
    appended: list[dict[str, Any]] = []
    for row in rows:
        validate_calibration_review_response_v2(row, manifest)
        response_id = hashlib.sha256(
            json.dumps(row, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        key = (row["batch_id"], row["card_id"], row["reviewer_id"], row["review_pass"])
        prior = existing.get(key)
        if prior:
            if prior["response_id"] != response_id:
                raise ValueError("Conflicting calibration response for batch/card/reviewer/pass.")
            continue
        record = {"schema_version": "RectangleSemanticCalibrationResponseV2", "response_id": response_id, **row}
        existing[key] = record
        appended.append(record)
    if appended:
        with log_path.open("a", encoding="utf-8") as handle:
            for record in appended:
                handle.write(json.dumps(record, sort_keys=True) + "\n")
    elif not log_path.exists():
        log_path.touch()
    strict_counts = Counter(record["strict_rectangle_validity"] for record in existing.values())
    worthiness_counts = Counter(record["as_of_trade_worthiness"] for record in existing.values())
    scorecard = {
        "schema_version": "ClassicalPatternSemanticCalibrationScorecardV2",
        "batch_id": manifest["batch_id"],
        "reviewed_count": len(existing),
        "strict_rectangle_validity_counts": dict(sorted(strict_counts.items())),
        "as_of_trade_worthiness_counts": dict(sorted(worthiness_counts.items())),
        "economic_fields_present": False,
        "decision_log_hash": _sha256_path(log_path),
    }
    scorecard_path = decisions_dir / "semantic_calibration_scorecard.json"
    scorecard_path.write_text(json.dumps(scorecard, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return CalibrationIngestionResultV2(
        batch_id=manifest["batch_id"],
        status="complete",
        reviewed_count=len(existing),
        decision_log_path=log_path,
        scorecard_path=scorecard_path,
    )


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


def _calibration_case_eligibility_reason(
    daily: pl.DataFrame,
    evaluation_index: int,
    lookback_sessions: int,
    evaluation_date: date,
    config: RectangleResearchConfig,
    eligibility_start: date | None,
    eligibility_end: date | None,
) -> str | None:
    if eligibility_start and evaluation_date < eligibility_start:
        return "before_eligibility_window"
    if eligibility_end and evaluation_date > eligibility_end:
        return "after_eligibility_window"
    if config.splits.label(evaluation_date) != "calibration":
        return "non_calibration_split"
    start = evaluation_index - lookback_sessions
    if start < 0:
        return "insufficient_causal_history"
    slice_dates = daily.slice(start, lookback_sessions + 1).get_column("session_date").to_list()
    if len(slice_dates) != lookback_sessions + 1:
        return "insufficient_causal_history"
    available = {_as_date(value) for value in slice_dates}
    expected = set(trading_dates(min(available), evaluation_date))
    if available != expected:
        return "calendar_gap_in_pattern_window"
    return None


def _load_calibration_exclusions_v2(
    manifests: Sequence[Path],
) -> tuple[set[str], dict[str, Any]]:
    """Load source identities from prior semantic manifests without retaining paths."""

    excluded: set[str] = set()
    manifest_hashes: set[str] = set()
    for raw_path in manifests:
        path = Path(raw_path).expanduser().resolve()
        try:
            content = path.read_bytes()
            payload = json.loads(content)
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError(f"Unable to load exclusion manifest: {path.name}") from exc
        if not isinstance(payload, dict):
            raise ValueError("Exclusion manifest must be a JSON object.")
        schema_version = payload.get("schema_version")
        if schema_version == "ClassicalPatternSemanticBatchV1":
            identity_field = "signal_id"
        elif schema_version == "ClassicalPatternSemanticCalibrationBatchV2":
            identity_field = "source_id"
        else:
            raise ValueError("Unsupported exclusion manifest schema.")
        cards = payload.get("cards")
        if not isinstance(cards, list):
            raise ValueError("Exclusion manifest cards must be a list.")
        identities: set[str] = set()
        for card in cards:
            if not isinstance(card, dict):
                raise ValueError("Exclusion manifest card must be an object.")
            identity = card.get(identity_field)
            if not isinstance(identity, str) or not identity.strip():
                raise ValueError("Exclusion manifest contains a malformed source identity.")
            if identity in identities:
                raise ValueError("Exclusion manifest contains duplicate source identities.")
            identities.add(identity)
        excluded.update(identities)
        manifest_hashes.add(hashlib.sha256(content).hexdigest())
    identity_hashes = sorted(
        hashlib.sha256(identity.encode("utf-8")).hexdigest() for identity in excluded
    )
    return excluded, {
        "version": "prior_manifest_source_identity_v1",
        "manifest_content_hashes": sorted(manifest_hashes),
        "excluded_identity_count": len(excluded),
        "excluded_source_ids": sorted(excluded),
        "excluded_identity_hashes": identity_hashes,
        "excluded_selected_overlap": 0,
    }


def _select_calibration_cases_v2(
    pools: dict[str, list[_CalibrationCase]],
    *,
    requested_counts: dict[str, int],
    sampling_seed: str,
) -> list[_CalibrationCase]:
    """Sample fixed class counts while preventing correlated window duplicates."""

    selected: list[_CalibrationCase] = []
    used_keys: set[tuple[str, date, int]] = set()
    used_symbols: set[str] = set()
    for hidden_class in ("confirmed_signal", "qualified_no_trigger", "rejected_geometry"):
        ordered = sorted(
            pools[hidden_class],
            key=lambda case: _calibration_sampling_key(case, sampling_seed),
        )
        class_selected: list[_CalibrationCase] = []
        # Prefer one card per symbol across the whole packet. If the class
        # cannot fill that way, fall back deterministically without weakening
        # the correlation-key dedupe contract.
        for require_fresh_symbol in (True, False):
            for case in ordered:
                if case.correlation_key in used_keys or case in class_selected:
                    continue
                if require_fresh_symbol and case.symbol in used_symbols:
                    continue
                class_selected.append(case)
                used_keys.add(case.correlation_key)
                used_symbols.add(case.symbol)
                if len(class_selected) == requested_counts[hidden_class]:
                    break
            if len(class_selected) == requested_counts[hidden_class]:
                break
        if len(class_selected) != requested_counts[hidden_class]:
            raise ValueError(
                "Insufficient distinct causal cases for "
                f"{hidden_class}: requested={requested_counts[hidden_class]}, available={len(class_selected)}"
            )
        selected.extend(class_selected)
    return selected


def _calibration_sampling_key(case: _CalibrationCase, seed: str) -> tuple[str, str]:
    digest = hashlib.sha256(
        f"{seed}|{case.hidden_class}|{case.source_id}".encode("utf-8")
    ).hexdigest()
    return digest, case.source_id


def _calibration_diagnostics(
    case: _CalibrationCase, daily: pl.DataFrame, config: RectangleResearchConfig
) -> dict[str, Any]:
    fields = {
        "anchor_span_sessions": None,
        "depth_percent": None,
        "touch_structure": None,
        "prior_central_rail_excursion_count": None,
        "prior_full_trigger_close_count": None,
    }
    if case.signal is None:
        return fields
    candidate = case.signal.candidate
    touch_indices = (*candidate.upper_touch_indices, *candidate.lower_touch_indices)
    midpoint = (candidate.upper_boundary + candidate.lower_boundary) / 2.0
    prior = daily.slice(
        candidate.breakout_index - candidate.lookback_sessions,
        candidate.lookback_sessions,
    ).to_dicts()
    closes = [float(row["close"]) for row in prior]
    trigger_buffer = config.definition.breakout_buffer_atr * candidate.atr
    if candidate.direction.value == "long":
        central_rail_excursions = sum(close > candidate.upper_boundary for close in closes)
        full_trigger = candidate.upper_edge + trigger_buffer
        prior_full_triggers = sum(close > full_trigger for close in closes)
    else:
        central_rail_excursions = sum(close < candidate.lower_boundary for close in closes)
        full_trigger = candidate.lower_edge - trigger_buffer
        prior_full_triggers = sum(close < full_trigger for close in closes)
    fields.update(
        {
            "anchor_span_sessions": max(touch_indices) - min(touch_indices) + 1 if touch_indices else None,
            "depth_percent": (candidate.height / midpoint * 100.0) if midpoint else None,
            "touch_structure": {
                "upper_touch_count": len(candidate.upper_touch_indices),
                "lower_touch_count": len(candidate.lower_touch_indices),
                "alternations": candidate.touch_alternations,
            },
            "prior_central_rail_excursion_count": central_rail_excursions,
            "prior_full_trigger_close_count": prior_full_triggers,
        }
    )
    return fields


def _write_calibration_response_template_v2(
    records: Sequence[CalibrationCardRecordV2], path: Path
) -> None:
    fieldnames = [
        "batch_id", "card_id", "config_hash", "card_hash", "reviewer_id", "review_pass",
        "strict_rectangle_validity", "as_of_trade_worthiness", "note", "reviewed_at",
        "outcome_hidden_attestation", "no_future_consulted_attestation",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for record in records:
            writer.writerow(
                {
                    "batch_id": record.batch_id,
                    "card_id": record.card_id,
                    "config_hash": record.config_hash,
                    "card_hash": record.card_hash,
                }
            )


def _render_calibration_card_v2(payload: dict[str, Any]) -> str:
    return "\n".join(
        [
            "---",
            f"batch_id: {payload['batch_id']}",
            f"card_id: {payload['card_id']}",
            f"config_hash: {payload['config_hash']}",
            f"symbol: {payload['symbol']}",
            f"evaluation_date: {payload['evaluation_date']}",
            "---",
            "",
            "# Daily OHLC Calibration Card",
            "",
            f"![As-of OHLC chart](../{payload['chart_path']})",
            "",
            f"- Symbol: `{payload['symbol']}`",
            f"- Evaluation cutoff: `{payload['evaluation_date']}`",
            f"- Candidate window: `{payload['displayed_bar_count']}` sessions ending at the cutoff",
            "- Judge only the displayed candidate window; its inclusion is not a machine verdict.",
            "- The pale final band marks the evaluation cutoff; it is not necessarily a breakout.",
            "",
            "## Blind Review",
            "",
            "Record answers in `../calibration_responses.csv`.",
            "",
            "1. Strict rectangle validity: `valid`, `invalid`, or `ambiguous`.",
            "2. As-of trade worthiness: `trade`, `watch`, `no_trade`, or `ambiguous`.",
            "3. Add a concise chart-grounded note if useful.",
            "",
            "Only bars through the evaluation cutoff are shown. Subsequent bars and economic results are intentionally absent.",
            "",
        ]
    )


def _render_calibration_index_v2(
    batch_id: str, records: Sequence[CalibrationCardRecordV2]
) -> str:
    lines = [
        f"# Classical Rectangle Calibration Review — {batch_id}",
        "",
        "Each chart is raw daily OHLC through its evaluation cutoff. The final pale band is the cutoff, not necessarily a breakout.",
        "Class, detector rationale, and prior verdict are intentionally hidden.",
        "Write responses in `calibration_responses.csv`; do not edit cards or manifests.",
        "",
    ]
    for index, record in enumerate(records, start=1):
        lines.append(f"{index}. [Card {index:02d} · {record.symbol} · {record.evaluation_date}]({record.card_path})")
    lines.extend(["", "This packet is semantic-calibration evidence only and is not executable.", ""])
    return "\n".join(lines)


def _render_neutral_ohlc_svg(
    frame: pl.DataFrame,
    *,
    card_id: str,
    definition_hash: str,
    source_slice_hash: str,
) -> str:
    """Render raw causal OHLC without any class- or detector-revealing overlays."""

    rows = frame.sort("session_date").to_dicts()
    width, height = 1200, 700
    left, right, top, bottom = 80.0, 100.0, 45.0, 90.0
    plot_width, plot_height = width - left - right, height - top - bottom
    min_price = min(float(row["low"]) for row in rows)
    max_price = max(float(row["high"]) for row in rows)
    padding = max((max_price - min_price) * 0.05, 0.01)
    min_price -= padding
    max_price += padding

    def x(index: int) -> float:
        return left + (index + 0.5) * plot_width / len(rows)

    def y(price: float) -> float:
        return top + (max_price - price) / (max_price - min_price) * plot_height

    candle_width = max(2.0, min(9.0, plot_width / len(rows) * 0.58))
    metadata = {
        "renderer_version": "classical_rectangle_neutral_ohlc_v2",
        "card_id": card_id,
        "definition_hash": definition_hash,
        "source_slice_hash": source_slice_hash,
        "first_bar_date": _as_date(rows[0]["session_date"]).isoformat(),
        "last_bar_date": _as_date(rows[-1]["session_date"]).isoformat(),
        "bar_count": len(rows),
    }
    elements = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        "<metadata>" + escape(json.dumps(metadata, sort_keys=True, separators=(",", ":"))) + "</metadata>",
        '<rect width="100%" height="100%" fill="#0f172a"/>',
        f'<text x="{left}" y="28" fill="#e2e8f0" font-family="sans-serif" font-size="18">Daily OHLC through evaluation cutoff · {_as_date(rows[-1]["session_date"]).isoformat()}</text>',
    ]
    for step in range(6):
        price = min_price + step * (max_price - min_price) / 5
        y_value = y(price)
        elements.extend(
            [
                f'<line x1="{left}" x2="{width-right}" y1="{y_value:.2f}" y2="{y_value:.2f}" stroke="#334155" stroke-width="1"/>',
                f'<text x="8" y="{y_value+4:.2f}" fill="#94a3b8" font-family="monospace" font-size="12">{price:.2f}</text>',
            ]
        )
    elements.append(
        f'<rect x="{x(len(rows)-1)-candle_width:.2f}" y="{top:.2f}" width="{2*candle_width:.2f}" height="{plot_height:.2f}" fill="#e2e8f0" fill-opacity="0.10"/>'
    )
    for index, row in enumerate(rows):
        open_price, close_price = float(row["open"]), float(row["close"])
        high, low = float(row["high"]), float(row["low"])
        color = "#22c55e" if close_price >= open_price else "#ef4444"
        x_value = x(index)
        elements.extend(
            [
                f'<line x1="{x_value:.2f}" x2="{x_value:.2f}" y1="{y(high):.2f}" y2="{y(low):.2f}" stroke="{color}" stroke-width="1"/>',
                f'<rect x="{x_value-candle_width/2:.2f}" y="{min(y(open_price), y(close_price)):.2f}" width="{candle_width:.2f}" height="{max(1.0, abs(y(open_price)-y(close_price))):.2f}" fill="{color}"/>',
            ]
        )
        if index % max(1, len(rows) // 8) == 0 or index == len(rows) - 1:
            label = _as_date(row["session_date"]).strftime("%m-%d")
            elements.append(f'<text x="{x_value:.2f}" y="{height-38}" fill="#94a3b8" font-family="monospace" font-size="10" text-anchor="middle">{label}</text>')
    elements.append("</svg>\n")
    return "\n".join(elements)


def _verify_neutral_svg_metadata_v2(chart_path: Path, card: dict[str, Any]) -> None:
    try:
        root = ElementTree.fromstring(chart_path.read_text(encoding="utf-8"))
        node = root.find("{http://www.w3.org/2000/svg}metadata")
        metadata = json.loads(node.text) if node is not None and node.text else None
    except (ElementTree.ParseError, json.JSONDecodeError) as exc:
        raise ValueError(f"Invalid neutral chart metadata: {card['card_id']}") from exc
    expected = {
        "renderer_version": "classical_rectangle_neutral_ohlc_v2",
        "card_id": card["card_id"],
        "definition_hash": card["config_hash"],
        "source_slice_hash": card["source_slice_hash"],
        "last_bar_date": card["evaluation_date"],
        "bar_count": card["displayed_bar_count"],
    }
    if not isinstance(metadata, dict) or any(metadata.get(key) != value for key, value in expected.items()):
        raise ValueError(f"Neutral chart metadata mismatch: {card['card_id']}")
    if (
        not isinstance(metadata.get("bar_count"), int)
        or metadata["bar_count"] != card["lookback_sessions"] + 1
    ):
        raise ValueError(f"Invalid neutral chart bar_count: {card['card_id']}")
