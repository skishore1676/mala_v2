from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import json
from pathlib import Path

import polars as pl
import pytest

from src.chronos.storage import LocalStorage
from src.research.classical_patterns.contracts import load_rectangle_config
from src.research.classical_patterns.readiness import (
    audit_local_cache,
    load_readiness_report,
    write_readiness_report,
)


CONFIG = Path("config/classical_patterns/rectangle_daily_v1.yaml")


def _minute_rows(day: date, *, count: int = 390) -> list[dict[str, float | int]]:
    start = datetime(day.year, day.month, day.day, 14, 30, tzinfo=timezone.utc)
    return [
        {
            "t": int((start + timedelta(minutes=index)).timestamp() * 1000),
            "o": 100.0,
            "h": 101.0,
            "l": 99.0,
            "c": 100.0,
            "v": 10,
        }
        for index in range(count)
    ]


def test_readiness_separates_semantic_pilot_from_economic_claims(tmp_path: Path) -> None:
    storage = LocalStorage(base_dir=tmp_path)
    for session_date in (
        date(2024, 1, 2),
        date(2024, 1, 3),
        date(2024, 1, 4),
        date(2024, 1, 5),
        date(2024, 1, 8),
    ):
        storage.save_bars("TEST", _minute_rows(session_date))
    report = audit_local_cache(
        symbols=["TEST"],
        config=load_rectangle_config(CONFIG),
        data_dir=tmp_path,
        minimum_complete_sessions=3,
        maximum_missing_fraction=1.0,
    )
    assert report.semantic_review_status == "ready"
    assert report.economic_research_status.startswith("blocked_")
    assert report.adjustment_provenance == "unverified_provider_adjusted"
    assert report.symbols[0].semantic_pilot_ready is True
    paths = write_readiness_report(report, tmp_path / "report")
    assert all(path.exists() for path in paths.values())
    assert load_readiness_report(paths["json"]) == report

    payload = json.loads(paths["json"].read_text(encoding="utf-8"))
    payload["config_hash"] = "tampered"
    paths["json"].write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="report hash mismatch"):
        load_readiness_report(paths["json"])


def test_readiness_reports_partial_sessions_and_short_coverage(tmp_path: Path) -> None:
    storage = LocalStorage(base_dir=tmp_path)
    storage.save_bars("PARTIAL", _minute_rows(date(2024, 1, 2), count=100))
    report = audit_local_cache(
        symbols=["PARTIAL"],
        config=load_rectangle_config(CONFIG),
        data_dir=tmp_path,
        minimum_complete_sessions=2,
    )
    row = report.symbols[0]
    assert row.complete_session_count == 0
    assert row.incomplete_session_count == 1
    assert row.semantic_pilot_ready is False
    assert "insufficient_complete_sessions" in row.readiness_reasons


def test_readiness_missing_symbol_fails_closed(tmp_path: Path) -> None:
    report = audit_local_cache(
        symbols=["MISSING"],
        config=load_rectangle_config(CONFIG),
        data_dir=tmp_path,
    )
    assert report.semantic_review_status == "insufficient"
    assert report.symbols[0].readiness_reasons == ("no_source_rows",)
