from __future__ import annotations

import ast
import csv
from dataclasses import asdict, replace
from datetime import datetime, time, timezone
import hashlib
import json
from pathlib import Path

import polars as pl
import pytest

from src.research.classical_patterns.contracts import load_rectangle_config
from src.research.classical_patterns.daily_bars import hash_daily_bars
from src.research.classical_patterns.readiness import (
    DataReadinessReport,
    SymbolReadiness,
)
from src.research.classical_patterns.review import (
    build_semantic_review_batch,
    ingest_review_responses,
    render_obsidian_review_card,
    validate_review_response,
    verify_semantic_batch,
)
from src.trading_calendar import trading_dates


CONFIG = Path("config/classical_patterns/rectangle_daily_v1.yaml")


def _rectangle_frame() -> pl.DataFrame:
    dates = trading_dates(
        datetime(2021, 1, 4).date(), datetime(2021, 6, 30).date()
    )[:80]
    rows: list[dict[str, object]] = []
    for index, session_date in enumerate(dates):
        high, low, close = 102.0, 98.0, 100.0
        if index % 8 in {1, 5}:
            high, close = 105.0, 101.0
        elif index % 8 in {3, 7}:
            low, close = 95.0, 99.0
        if index == 60:
            high, low, close = 109.0, 104.0, 108.0
        rows.append(
            {
                "session_date": session_date,
                "visible_at": datetime.combine(
                    session_date, time(21), tzinfo=timezone.utc
                ),
                "symbol": "TEST",
                "open": close,
                "high": high,
                "low": low,
                "close": close,
                "volume": 1_000_000.0,
                "source_bar_count": 390,
                "complete_session": True,
            }
        )
    return pl.DataFrame(rows)


def _readiness(config_hash: str) -> DataReadinessReport:
    daily = _rectangle_frame()
    symbol = SymbolReadiness(
        symbol="TEST",
        cache_file_count=1,
        source_row_count=len(daily),
        source_start="2021-01-04T00:00:00+00:00",
        source_end="2021-06-30T00:00:00+00:00",
        duplicate_timestamp_count=0,
        invalid_source_row_count=0,
        complete_session_count=len(daily),
        incomplete_session_count=0,
        missing_expected_session_count=0,
        unexpected_session_count=0,
        coverage_start="2021-01-04",
        coverage_end="2021-06-30",
        complete_daily_hash=hash_daily_bars(daily),
        semantic_pilot_ready=True,
        readiness_reasons=(),
    )
    payload = {
        "schema_version": "ClassicalPatternDataReadinessV1",
        "config_hash": config_hash,
        "requested_start": "2021-01-04",
        "requested_end": "2021-06-30",
        "adjustment_provenance": "unverified_provider_adjusted",
        "semantic_review_status": "ready",
        "economic_research_status": "blocked_unverified_adjustment_and_point_in_time_universe",
        "symbols": [asdict(symbol)],
    }
    report_hash = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return DataReadinessReport(
        generated_at="2026-07-17T00:00:00+00:00",
        symbols=(symbol,),
        report_hash=report_hash,
        **{key: value for key, value in payload.items() if key != "symbols"},
    )


def test_semantic_batch_is_deterministic_as_of_and_outcome_hidden(tmp_path: Path) -> None:
    config = load_rectangle_config(CONFIG)
    first = build_semantic_review_batch(
        {"TEST": _rectangle_frame()},
        config=config,
        readiness=_readiness(config.source_hash),
        output_dir=tmp_path / "first",
        batch_id="semantic-v1",
        batch_size=1,
    )
    second = build_semantic_review_batch(
        {"TEST": _rectangle_frame()},
        config=config,
        readiness=_readiness(config.source_hash),
        output_dir=tmp_path / "second",
        batch_id="semantic-v1",
        batch_size=1,
    )
    assert first.canonical_hash == second.canonical_hash
    assert first.selected_signal_count == 1
    receipt = verify_semantic_batch(first.output_dir)
    assert receipt["executable"] is False
    assert receipt["outcomes_hidden"] is True

    manifest = json.loads(first.manifest_path.read_text(encoding="utf-8"))
    card = manifest["cards"][0]
    assert set(card["levels"]) == {
        "upper_boundary",
        "lower_boundary",
        "upper_edge",
        "lower_edge",
        "breakout_boundary",
        "last_full_day",
        "last_full_day_high",
        "last_full_day_low",
        "raw_lfd_stop",
    }
    assert "split" not in card
    svg = (first.output_dir / card["chart_path"]).read_text(encoding="utf-8")
    assert card["breakout_date"] in svg
    assert "objective" not in svg
    assert "negation" not in svg
    assert "2021-04-" not in svg  # synthetic future after the March breakout


def test_semantic_batch_hash_verification_fails_closed(tmp_path: Path) -> None:
    config = load_rectangle_config(CONFIG)
    result = build_semantic_review_batch(
        {"TEST": _rectangle_frame()},
        config=config,
        readiness=_readiness(config.source_hash),
        output_dir=tmp_path / "batch",
        batch_id="semantic-v1",
        batch_size=1,
    )
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    chart = result.output_dir / manifest["cards"][0]["chart_path"]
    chart.write_text(chart.read_text(encoding="utf-8") + "<!-- tampered -->\n", encoding="utf-8")
    with pytest.raises(ValueError, match="hash mismatch"):
        verify_semantic_batch(result.output_dir)


def test_semantic_batch_rejects_unprotected_receipt_identity_edit(tmp_path: Path) -> None:
    config = load_rectangle_config(CONFIG)
    result = build_semantic_review_batch(
        {"TEST": _rectangle_frame()},
        config=config,
        readiness=_readiness(config.source_hash),
        output_dir=tmp_path / "batch",
        batch_id="semantic-v1",
        batch_size=1,
    )
    receipt = json.loads(result.receipt_path.read_text(encoding="utf-8"))
    receipt["config_hash"] = "different-current-config"
    result.receipt_path.write_text(
        json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    with pytest.raises(ValueError, match="Receipt/manifest identity mismatch"):
        verify_semantic_batch(result.output_dir)


def test_semantic_batch_refuses_nonempty_output_root(tmp_path: Path) -> None:
    config = load_rectangle_config(CONFIG)
    output_dir = tmp_path / "batch"
    output_dir.mkdir()
    (output_dir / "stale.txt").write_text("stale", encoding="utf-8")
    with pytest.raises(ValueError, match="absent or empty"):
        build_semantic_review_batch(
            {"TEST": _rectangle_frame()},
            config=config,
            readiness=_readiness(config.source_hash),
            output_dir=output_dir,
            batch_id="semantic-v1",
            batch_size=1,
        )


def test_obsidian_projection_is_self_contained_deterministic_and_outcome_hidden(
    tmp_path: Path,
) -> None:
    config = load_rectangle_config(CONFIG)
    result = build_semantic_review_batch(
        {"TEST": _rectangle_frame()},
        config=config,
        readiness=_readiness(config.source_hash),
        output_dir=tmp_path / "batch",
        batch_id="semantic-v1",
        batch_size=1,
    )
    first = render_obsidian_review_card(
        batch_dir=result.output_dir,
        output_path=tmp_path / "first.md",
    )
    second = render_obsidian_review_card(
        batch_dir=result.output_dir,
        output_path=tmp_path / "second.md",
    )
    text = first.read_text(encoding="utf-8")
    assert text == second.read_text(encoding="utf-8")
    assert text.count("data:image/svg+xml;base64,") == 1
    assert "../charts/" not in text
    assert "No pointy comment on a chart means AGREE" in text
    assert "<!-- classical-pattern-card:" in text
    assert result.canonical_hash in text
    assert "2021-04-" not in text
    for forbidden in ("entry_price", "gross_pnl", "net_r", "objective"):
        assert forbidden not in text


def test_semantic_batch_rejects_readiness_daily_hash_mismatch(tmp_path: Path) -> None:
    config = load_rectangle_config(CONFIG)
    readiness = _readiness(config.source_hash)
    mismatched_symbol = replace(
        readiness.symbols[0], complete_daily_hash="different-daily-input"
    )
    payload = {
        "schema_version": readiness.schema_version,
        "config_hash": readiness.config_hash,
        "requested_start": readiness.requested_start,
        "requested_end": readiness.requested_end,
        "adjustment_provenance": readiness.adjustment_provenance,
        "semantic_review_status": readiness.semantic_review_status,
        "economic_research_status": readiness.economic_research_status,
        "symbols": [asdict(mismatched_symbol)],
    }
    valid_but_misaligned = replace(
        readiness,
        symbols=(mismatched_symbol,),
        report_hash=hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest(),
    )
    with pytest.raises(ValueError, match="daily hash mismatch"):
        build_semantic_review_batch(
            {"TEST": _rectangle_frame()},
            config=config,
            readiness=valid_but_misaligned,
            output_dir=tmp_path / "batch",
            batch_id="semantic-v1",
            batch_size=1,
        )


def test_review_module_has_no_economic_or_lifecycle_imports() -> None:
    module_path = Path("src/research/classical_patterns/review.py")
    tree = ast.parse(module_path.read_text(encoding="utf-8"))
    imported_modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_modules.add(node.module)
    assert not any(
        forbidden in module
        for module in imported_modules
        for forbidden in ("lifecycle", "simulator", "economics", "backtest")
    )


def test_review_response_rejects_stale_hash_and_unknown_decision(tmp_path: Path) -> None:
    config = load_rectangle_config(CONFIG)
    result = build_semantic_review_batch(
        {"TEST": _rectangle_frame()},
        config=config,
        readiness=_readiness(config.source_hash),
        output_dir=tmp_path / "batch",
        batch_id="semantic-v1",
        batch_size=1,
    )
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    with result.response_template_path.open(newline="", encoding="utf-8") as handle:
        row = next(csv.DictReader(handle))
    row.update(
        decision="accept",
        rectangle_fidelity="yes",
        boundary_fidelity="yes",
        lfd_fidelity="yes",
        breakout_fidelity="yes",
        reviewer="Suman",
        reviewed_at="2026-07-17T00:00:00-05:00",
        outcome_hidden_attestation="true",
        no_future_consulted_attestation="true",
    )
    validate_review_response(row, manifest)
    stale = dict(row, card_hash="stale")
    with pytest.raises(ValueError, match="card_hash"):
        validate_review_response(stale, manifest)
    invalid = dict(row, decision="maybe")
    with pytest.raises(ValueError, match="Invalid review decision"):
        validate_review_response(invalid, manifest)
    unknown_reason = dict(row, reason_codes="hindsight_was_bad")
    with pytest.raises(ValueError, match="Invalid review reason"):
        validate_review_response(unknown_reason, manifest)
    inconsistent_accept = dict(row, boundary_fidelity="revise")
    with pytest.raises(ValueError, match="Accepted reviews require yes"):
        validate_review_response(inconsistent_accept, manifest)
    empty_revision = dict(
        row,
        decision="revise",
        rectangle_fidelity="revise",
    )
    with pytest.raises(ValueError, match="reason code or correction"):
        validate_review_response(empty_revision, manifest)


def test_receipt_hashes_every_named_artifact(tmp_path: Path) -> None:
    config = load_rectangle_config(CONFIG)
    result = build_semantic_review_batch(
        {"TEST": _rectangle_frame()},
        config=config,
        readiness=_readiness(config.source_hash),
        output_dir=tmp_path / "batch",
        batch_id="semantic-v1",
        batch_size=1,
    )
    receipt = json.loads(result.receipt_path.read_text(encoding="utf-8"))
    for relative, expected in receipt["artifacts"].items():
        assert hashlib.sha256((result.output_dir / relative).read_bytes()).hexdigest() == expected


def test_response_ingestion_is_append_only_idempotent_and_semantic_only(
    tmp_path: Path,
) -> None:
    config = load_rectangle_config(CONFIG)
    result = build_semantic_review_batch(
        {"TEST": _rectangle_frame()},
        config=config,
        readiness=_readiness(config.source_hash),
        output_dir=tmp_path / "batch",
        batch_id="semantic-v1",
        batch_size=1,
    )
    with result.response_template_path.open(newline="", encoding="utf-8") as handle:
        fieldnames = next(csv.reader(handle))
    with result.response_template_path.open(newline="", encoding="utf-8") as handle:
        row = next(csv.DictReader(handle))
    row.update(
        decision="accept",
        rectangle_fidelity="yes",
        boundary_fidelity="yes",
        lfd_fidelity="yes",
        breakout_fidelity="yes",
        reviewer="Suman",
        reviewed_at="2026-07-17T00:00:00-05:00",
        outcome_hidden_attestation="true",
        no_future_consulted_attestation="true",
    )
    with result.response_template_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow(row)

    first = ingest_review_responses(batch_dir=result.output_dir)
    second = ingest_review_responses(batch_dir=result.output_dir)
    assert first.status == second.status == "complete"
    assert first.decision_log_path.read_text(encoding="utf-8").count("\n") == 1
    scorecard = json.loads(first.scorecard_path.read_text(encoding="utf-8"))
    assert scorecard["decision_counts"] == {"accept": 1}
    assert scorecard["economic_fields_present"] is False

    row["decision"] = "reject"
    row["rectangle_fidelity"] = "no"
    row["reason_codes"] = "not_rectangle"
    with result.response_template_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow(row)
    with pytest.raises(ValueError, match="Conflicting response"):
        ingest_review_responses(batch_dir=result.output_dir)

    existing_record = json.loads(
        first.decision_log_path.read_text(encoding="utf-8").strip()
    )
    existing_record["response_id"] = "tampered"
    first.decision_log_path.write_text(
        json.dumps(existing_record, sort_keys=True) + "\n", encoding="utf-8"
    )
    with pytest.raises(ValueError, match="response_id mismatch"):
        ingest_review_responses(batch_dir=result.output_dir)
