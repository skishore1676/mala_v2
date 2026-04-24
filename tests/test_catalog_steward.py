from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from src.research.catalog_steward import (
    STEWARD_COLUMNS,
    build_recommendations,
    push_recommendations_to_sheet,
    write_recommendation_artifacts,
)


def test_catalog_steward_recommendations_use_inclusive_shadow() -> None:
    rows = [
        _catalog_row(
            catalog_key="strong_live",
            expectancy="0.55",
            signal_count="60",
            execution_robustness="0.98",
            mc_total_r_p05=3.2,
        ),
        _catalog_row(
            catalog_key="shadow_candidate",
            expectancy="0.31",
            signal_count="22",
            execution_robustness="0.70",
            mc_total_r_p05=-4.0,
        ),
        _catalog_row(
            catalog_key="too_thin",
            expectancy="0.50",
            signal_count="19",
            execution_robustness="0.99",
            mc_total_r_p05=4.0,
        ),
        _catalog_row(
            catalog_key="weak_active",
            expectancy="0.04",
            signal_count="80",
            execution_robustness="0.35",
            mc_total_r_p05=None,
        ),
    ]
    active = [{"strategy": "weak_active", "enabled": "TRUE", "mode": "live"}]

    recommendations = {
        rec.catalog_key: rec
        for rec in build_recommendations(catalog_rows=rows, active_rows=active, reviewed_at="2026-04-24T12:00:00+00:00")
    }

    assert recommendations["strong_live"].recommendation == "live"
    assert recommendations["shadow_candidate"].recommendation == "shadow"
    assert recommendations["too_thin"].recommendation == "hold"
    assert recommendations["weak_active"].recommendation == "pause"


def test_catalog_steward_writes_artifacts_without_sheet_client(tmp_path: Path) -> None:
    recommendations = build_recommendations(
        catalog_rows=[_catalog_row(catalog_key="shadow_candidate", expectancy="0.31", signal_count="22")],
        active_rows=[],
        reviewed_at="2026-04-24T12:00:00+00:00",
        report_path="research/reports/catalog_steward/example.md",
    )

    md_path, csv_path = write_recommendation_artifacts(
        recommendations=recommendations,
        out_dir=tmp_path,
        reviewed_at="2026-04-24T12:00:00+00:00",
    )

    assert md_path.exists()
    assert csv_path.exists()
    assert "shadow_candidate" in md_path.read_text(encoding="utf-8")
    assert "steward_recommendation" in csv_path.read_text(encoding="utf-8")


def test_catalog_steward_demotes_duplicate_live_lanes_to_shadow() -> None:
    recommendations = build_recommendations(
        catalog_rows=[
            _catalog_row(
                catalog_key="better_amd",
                expectancy="0.60",
                signal_count="100",
                execution_robustness="0.99",
                mc_total_r_p05=12.0,
            ),
            _catalog_row(
                catalog_key="duplicate_amd",
                expectancy="0.55",
                signal_count="80",
                execution_robustness="0.98",
                mc_total_r_p05=10.0,
            ),
        ],
        active_rows=[],
        reviewed_at="2026-04-24T12:00:00+00:00",
    )

    by_key = {rec.catalog_key: rec for rec in recommendations}
    assert by_key["better_amd"].recommendation == "live"
    assert by_key["duplicate_amd"].recommendation == "shadow"
    assert "duplicate live lane" in by_key["duplicate_amd"].reason


def test_push_recommendations_updates_only_steward_columns() -> None:
    recommendations = build_recommendations(
        catalog_rows=[_catalog_row(catalog_key="shadow_candidate", expectancy="0.31", signal_count="22")],
        active_rows=[],
        reviewed_at="2026-04-24T12:00:00+00:00",
        report_path="research/reports/catalog_steward/example.md",
    )
    client = _FakeStewardClient(rows=[{"row_index": 2, "catalog_key": "shadow_candidate", "operator_notes": "human note"}])

    updated = push_recommendations_to_sheet(client=client, recommendations=recommendations)  # type: ignore[arg-type]

    assert updated == 1
    assert client.ensured_columns == [STEWARD_COLUMNS]
    assert client.updated_columns == STEWARD_COLUMNS
    assert client.updated_rows[0]["operator_notes"] == "human note"
    assert client.updated_rows[0]["steward_recommendation"] == "shadow"
    notes = json.loads(client.updated_rows[0]["steward_notes"])
    assert notes["report"] == "research/reports/catalog_steward/example.md"


def _catalog_row(
    *,
    catalog_key: str,
    expectancy: str,
    signal_count: str,
    execution_robustness: str = "0.80",
    mc_total_r_p05: float | None = None,
) -> dict[str, Any]:
    return {
        "catalog_key": catalog_key,
        "symbol": "SPY",
        "direction": "short",
        "strategy_key": "market_impulse",
        "lifecycle_status": "candidate",
        "bhiksha_ready": "TRUE",
        "expectancy": expectancy,
        "confidence": "0.51",
        "signal_count": signal_count,
        "execution_robustness": execution_robustness,
        "playbook_summary_json": json.dumps({"mc_metrics": {"mc_total_r_p05": mc_total_r_p05}}),
    }


class _FakeStewardClient:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows
        self.ensured_columns: list[list[str]] = []
        self.updated_rows: list[dict[str, Any]] = []
        self.updated_columns: list[str] = []

    def ensure_columns(self, columns: list[str]) -> list[str]:
        self.ensured_columns.append(columns)
        return columns

    def read_rows(self) -> list[dict[str, Any]]:
        return [dict(row) for row in self.rows]

    def batch_update_rows(self, *, rows: list[dict[str, Any]], columns: list[str]) -> dict[str, Any]:
        self.updated_rows = rows
        self.updated_columns = columns
        return {}
