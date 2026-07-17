from __future__ import annotations

import ast
import csv
from dataclasses import asdict, replace
from datetime import datetime, time, timedelta, timezone
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
    _CalibrationCase,
    _calibration_diagnostics,
    build_semantic_calibration_batch_v2,
    build_semantic_review_batch,
    ingest_calibration_review_responses_v2,
    ingest_review_responses,
    render_calibration_obsidian_gate_v2,
    render_obsidian_review_card,
    validate_calibration_review_response_v2,
    validate_review_response,
    verify_semantic_calibration_batch_v2,
    verify_semantic_batch,
)
from src.research.classical_patterns.rectangle import enumerate_rectangles
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


def _flat_frame() -> pl.DataFrame:
    """Causal hard-negative fixture: positive ATR but no confirmed pivot touches."""

    dates = trading_dates(
        datetime(2021, 1, 4).date(), datetime(2021, 6, 30).date()
    )[:80]
    return pl.DataFrame(
        {
            "session_date": dates,
            "visible_at": [datetime.combine(day, time(21), tzinfo=timezone.utc) for day in dates],
            "symbol": ["FLAT"] * len(dates),
            "open": [100.0] * len(dates),
            "high": [102.0] * len(dates),
            "low": [98.0] * len(dates),
            "close": [100.0] * len(dates),
            "volume": [1_000_000.0] * len(dates),
            "source_bar_count": [390] * len(dates),
            "complete_session": [True] * len(dates),
        }
    )


def _readiness(
    config_hash: str, daily_by_symbol: dict[str, pl.DataFrame] | None = None
) -> DataReadinessReport:
    daily_by_symbol = daily_by_symbol or {"TEST": _rectangle_frame()}
    symbols = []
    for name, daily in sorted(daily_by_symbol.items()):
        symbols.append(
            SymbolReadiness(
                symbol=name,
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
        )
    payload = {
        "schema_version": "ClassicalPatternDataReadinessV1",
        "config_hash": config_hash,
        "requested_start": "2021-01-04",
        "requested_end": "2021-06-30",
        "adjustment_provenance": "unverified_provider_adjusted",
        "semantic_review_status": "ready",
        "economic_research_status": "blocked_unverified_adjustment_and_point_in_time_universe",
        "symbols": [asdict(symbol) for symbol in symbols],
    }
    report_hash = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return DataReadinessReport(
        generated_at="2026-07-17T00:00:00+00:00",
        symbols=tuple(symbols),
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


def test_v2_calibration_batch_is_class_hidden_deterministic_and_outcome_hidden(
    tmp_path: Path,
) -> None:
    config = load_rectangle_config(CONFIG)
    daily = {"TEST": _rectangle_frame(), "FLAT": _flat_frame()}
    first = build_semantic_calibration_batch_v2(
        daily,
        config=config,
        readiness=_readiness(config.source_hash, daily),
        output_dir=tmp_path / "first",
        batch_id="semantic-calibration-v2",
        confirmed_signal_count=1,
        qualified_no_trigger_count=1,
        rejected_geometry_count=1,
    )
    second = build_semantic_calibration_batch_v2(
        daily,
        config=config,
        readiness=_readiness(config.source_hash, daily),
        output_dir=tmp_path / "second",
        batch_id="semantic-calibration-v2",
        confirmed_signal_count=1,
        qualified_no_trigger_count=1,
        rejected_geometry_count=1,
    )
    assert first.canonical_hash == second.canonical_hash
    receipt = verify_semantic_calibration_batch_v2(first.output_dir)
    assert receipt["selected_count"] == 3
    manifest = json.loads(first.manifest_path.read_text(encoding="utf-8"))
    assert manifest["sampling"]["selected_counts"] == {
        "confirmed_signal": 1,
        "qualified_no_trigger": 1,
        "rejected_geometry": 1,
    }
    assert {item["hidden_class"] for item in manifest["private_cases"]} == set(
        manifest["sampling"]["requested_counts"]
    )
    rejected = next(item for item in manifest["private_cases"] if item["hidden_class"] == "rejected_geometry")
    assert rejected["hidden_reason"] == "insufficient_confirmed_boundary_touches"
    assert rejected["causal_attempt_direction"] is None
    assert all(value is None for value in rejected["causal_diagnostics"].values())
    for card in manifest["cards"]:
        text = (first.output_dir / card["card_path"]).read_text(encoding="utf-8")
        svg = (first.output_dir / card["chart_path"]).read_text(encoding="utf-8")
        for hidden in ("confirmed_signal", "qualified_no_trigger", "rejected_geometry", "insufficient_confirmed"):
            assert hidden not in text
            assert hidden not in svg
        assert "not necessarily a breakout" in text
        assert card["displayed_bar_count"] == card["lookback_sessions"] + 1
        assert f"Candidate window: `{card['displayed_bar_count']}` sessions" in text
        assert "inclusion is not a machine verdict" in text
        assert "Lookback context:" not in text
        assert "objective" not in text
        assert card["evaluation_date"] in svg


def test_v2_calibration_batch_fails_closed_on_shortage_tamper_and_future_poison(
    tmp_path: Path,
) -> None:
    config = load_rectangle_config(CONFIG)
    daily = {"TEST": _rectangle_frame(), "FLAT": _flat_frame()}
    with pytest.raises(ValueError, match="Insufficient distinct causal cases"):
        build_semantic_calibration_batch_v2(
            daily,
            config=config,
            readiness=_readiness(config.source_hash, daily),
            output_dir=tmp_path / "short",
            batch_id="semantic-calibration-v2-short",
            confirmed_signal_count=2,
            qualified_no_trigger_count=1,
            rejected_geometry_count=1,
        )
    result = build_semantic_calibration_batch_v2(
        daily,
        config=config,
        readiness=_readiness(config.source_hash, daily),
        output_dir=tmp_path / "batch",
        batch_id="semantic-calibration-v2",
        confirmed_signal_count=1,
        qualified_no_trigger_count=1,
        rejected_geometry_count=1,
    )
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    chart = result.output_dir / manifest["cards"][0]["chart_path"]
    chart.write_text(chart.read_text(encoding="utf-8") + "<!-- tampered -->\n", encoding="utf-8")
    with pytest.raises(ValueError, match="hash mismatch"):
        verify_semantic_calibration_batch_v2(result.output_dir)

    eligibility_end = datetime(2021, 4, 1).date()
    future_rows = []
    for frame in daily.values():
        last = frame.row(-1, named=True)
        for index in range(3):
            session_date = last["session_date"] + timedelta(days=index + 1)
            future_rows.append(
                {
                    **last,
                    "session_date": session_date,
                    "visible_at": datetime.combine(session_date, time(21), tzinfo=timezone.utc),
                    "high": float(last["high"]) + 50.0,
                    "low": float(last["low"]) - 50.0,
                    "close": float(last["close"]) + 50.0,
                }
            )
    poisoned = {
        "TEST": pl.concat([daily["TEST"], pl.DataFrame([row for row in future_rows if row["symbol"] == "TEST"])]),
        "FLAT": pl.concat([daily["FLAT"], pl.DataFrame([row for row in future_rows if row["symbol"] == "FLAT"])]),
    }
    clean = build_semantic_calibration_batch_v2(
        daily,
        config=config,
        readiness=_readiness(config.source_hash, daily),
        output_dir=tmp_path / "clean-prefix",
        batch_id="semantic-calibration-v2-prefix",
        confirmed_signal_count=1,
        qualified_no_trigger_count=1,
        rejected_geometry_count=1,
        eligibility_end=eligibility_end,
    )
    poison = build_semantic_calibration_batch_v2(
        poisoned,
        config=config,
        readiness=_readiness(config.source_hash, poisoned),
        output_dir=tmp_path / "poisoned-prefix",
        batch_id="semantic-calibration-v2-prefix",
        confirmed_signal_count=1,
        qualified_no_trigger_count=1,
        rejected_geometry_count=1,
        eligibility_end=eligibility_end,
    )
    clean_manifest = json.loads(clean.manifest_path.read_text(encoding="utf-8"))
    poison_manifest = json.loads(poison.manifest_path.read_text(encoding="utf-8"))
    assert [
        (card["source_id"], card["source_slice_hash"]) for card in clean_manifest["cards"]
    ] == [
        (card["source_id"], card["source_slice_hash"]) for card in poison_manifest["cards"]
    ]


def test_v2_calibration_obsidian_gate_is_small_self_contained_and_blind(
    tmp_path: Path,
) -> None:
    config = load_rectangle_config(CONFIG)
    daily = {"TEST": _rectangle_frame(), "FLAT": _flat_frame()}
    result = build_semantic_calibration_batch_v2(
        daily,
        config=config,
        readiness=_readiness(config.source_hash, daily),
        output_dir=tmp_path / "batch",
        batch_id="semantic-calibration-v2-gate",
        confirmed_signal_count=1,
        qualified_no_trigger_count=1,
        rejected_geometry_count=1,
    )
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    card_ids = [card["card_id"] for card in manifest["cards"][:2]]
    output = render_calibration_obsidian_gate_v2(
        batch_dir=result.output_dir,
        output_path=tmp_path / "gate.md",
        card_ids=card_ids,
    )
    text = output.read_text(encoding="utf-8")
    assert text.count("data:image/svg+xml;base64,") == 2
    assert text.count("Your call (add one pointy comment)") == 2
    assert all(f"classical-pattern-calibration-card:{card_id}" in text for card_id in card_ids)
    assert "confirmed_signal" not in text
    assert "qualified_no_trigger" not in text
    assert "rejected_geometry" not in text
    with pytest.raises(ValueError, match="duplicate"):
        render_calibration_obsidian_gate_v2(
            batch_dir=result.output_dir,
            output_path=tmp_path / "duplicate.md",
            card_ids=[card_ids[0], card_ids[0]],
        )


def test_v2_calibration_response_contract_is_reviewer_pass_scoped(tmp_path: Path) -> None:
    config = load_rectangle_config(CONFIG)
    daily = {"TEST": _rectangle_frame(), "FLAT": _flat_frame()}
    result = build_semantic_calibration_batch_v2(
        daily,
        config=config,
        readiness=_readiness(config.source_hash, daily),
        output_dir=tmp_path / "batch",
        batch_id="semantic-calibration-v2",
        confirmed_signal_count=1,
        qualified_no_trigger_count=1,
        rejected_geometry_count=1,
    )
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    with result.response_template_path.open(newline="", encoding="utf-8") as handle:
        row = next(csv.DictReader(handle))
    row.update(
        reviewer_id="suman",
        review_pass="2",
        strict_rectangle_validity="ambiguous",
        as_of_trade_worthiness="watch",
        reviewed_at="2026-07-17T00:00:00-05:00",
        outcome_hidden_attestation="true",
        no_future_consulted_attestation="true",
    )
    validate_calibration_review_response_v2(row, manifest)
    with result.response_template_path.open(newline="", encoding="utf-8") as handle:
        fieldnames = next(csv.reader(handle))
    with result.response_template_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow(row)
    first = ingest_calibration_review_responses_v2(batch_dir=result.output_dir)
    second = ingest_calibration_review_responses_v2(batch_dir=result.output_dir)
    assert first.reviewed_count == second.reviewed_count == 1
    assert first.decision_log_path.read_text(encoding="utf-8").count("\n") == 1
    with pytest.raises(ValueError, match="review_pass"):
        validate_calibration_review_response_v2(dict(row, review_pass="0"), manifest)


def test_v2_positive_diagnostics_separate_central_rail_excursions_from_full_triggers() -> None:
    config = load_rectangle_config(CONFIG)
    daily = _rectangle_frame()
    signal = enumerate_rectangles(daily, config).signals[0]
    candidate = signal.candidate
    assert candidate.direction.value == "long"
    full_trigger = (
        candidate.upper_edge + config.definition.breakout_buffer_atr * candidate.atr
    )
    central_excursion = (candidate.upper_boundary + full_trigger) / 2.0
    assert candidate.upper_boundary < central_excursion < full_trigger
    adjusted = daily.with_columns(
        pl.when(pl.int_range(pl.len()) == candidate.breakout_index - 1)
        .then(pl.lit(central_excursion))
        .otherwise(pl.col("close"))
        .alias("close")
    )
    case = _CalibrationCase(
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
    diagnostics = _calibration_diagnostics(case, adjusted, config)
    assert diagnostics["prior_central_rail_excursion_count"] >= 1
    assert diagnostics["prior_full_trigger_close_count"] == 0
    anchor_indices = (*candidate.upper_touch_indices, *candidate.lower_touch_indices)
    assert diagnostics["anchor_span_sessions"] == max(anchor_indices) - min(anchor_indices) + 1


def test_v2_excludes_v1_manifest_identities_and_records_path_independent_contract(
    tmp_path: Path,
) -> None:
    config = load_rectangle_config(CONFIG)
    daily = {"TEST": _rectangle_frame(), "FLAT": _flat_frame()}
    v1 = build_semantic_review_batch(
        {"TEST": daily["TEST"]},
        config=config,
        readiness=_readiness(config.source_hash, {"TEST": daily["TEST"]}),
        output_dir=tmp_path / "v1",
        batch_id="semantic-v1",
        batch_size=1,
    )
    with pytest.raises(ValueError, match="Insufficient distinct causal cases"):
        build_semantic_calibration_batch_v2(
            daily,
            config=config,
            readiness=_readiness(config.source_hash, daily),
            output_dir=tmp_path / "excluded-real-v1",
            batch_id="semantic-calibration-v2",
            confirmed_signal_count=1,
            qualified_no_trigger_count=1,
            rejected_geometry_count=1,
            exclude_manifests=[v1.manifest_path],
        )

    prior_manifest = tmp_path / "prior-v1.json"
    prior_manifest.write_text(
        json.dumps(
            {
                "schema_version": "ClassicalPatternSemanticBatchV1",
                "cards": [{"signal_id": "prior-signal-not-in-current-batch"}],
            }
        ),
        encoding="utf-8",
    )
    result = build_semantic_calibration_batch_v2(
        daily,
        config=config,
        readiness=_readiness(config.source_hash, daily),
        output_dir=tmp_path / "contract",
        batch_id="semantic-calibration-v2-contract",
        confirmed_signal_count=1,
        qualified_no_trigger_count=1,
        rejected_geometry_count=1,
        exclude_manifests=[prior_manifest],
    )
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    exclusions = manifest["exclusions"]
    assert exclusions["manifest_content_hashes"] == [
        hashlib.sha256(prior_manifest.read_bytes()).hexdigest()
    ]
    assert exclusions["excluded_identity_count"] == 1
    assert exclusions["excluded_source_ids"] == ["prior-signal-not-in-current-batch"]
    assert exclusions["excluded_selected_overlap"] == 0
    assert str(prior_manifest) not in json.dumps(exclusions)
    assert {
        hashlib.sha256(card["source_id"].encode("utf-8")).hexdigest()
        for card in manifest["cards"]
    }.isdisjoint(exclusions["excluded_identity_hashes"])


def test_v2_exclusion_manifest_and_contract_tampering_fail_closed(tmp_path: Path) -> None:
    config = load_rectangle_config(CONFIG)
    daily = {"TEST": _rectangle_frame(), "FLAT": _flat_frame()}
    unsupported = tmp_path / "unsupported.json"
    unsupported.write_text(json.dumps({"schema_version": "UnknownV9", "cards": []}), encoding="utf-8")
    with pytest.raises(ValueError, match="Unsupported exclusion manifest schema"):
        build_semantic_calibration_batch_v2(
            daily,
            config=config,
            readiness=_readiness(config.source_hash, daily),
            output_dir=tmp_path / "unsupported-output",
            batch_id="semantic-calibration-v2",
            confirmed_signal_count=1,
            qualified_no_trigger_count=1,
            rejected_geometry_count=1,
            exclude_manifests=[unsupported],
        )
    prior_manifest = tmp_path / "prior-v1.json"
    prior_manifest.write_text(
        json.dumps(
            {
                "schema_version": "ClassicalPatternSemanticBatchV1",
                "cards": [{"signal_id": "prior-signal-not-in-current-batch"}],
            }
        ),
        encoding="utf-8",
    )
    result = build_semantic_calibration_batch_v2(
        daily,
        config=config,
        readiness=_readiness(config.source_hash, daily),
        output_dir=tmp_path / "batch",
        batch_id="semantic-calibration-v2-contract",
        confirmed_signal_count=1,
        qualified_no_trigger_count=1,
        rejected_geometry_count=1,
        exclude_manifests=[prior_manifest],
    )
    receipt = json.loads(result.receipt_path.read_text(encoding="utf-8"))
    receipt["exclusions"]["excluded_selected_overlap"] = 1
    result.receipt_path.write_text(
        json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    with pytest.raises(ValueError, match="exclusion mismatch"):
        verify_semantic_calibration_batch_v2(result.output_dir)
