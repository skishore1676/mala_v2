from __future__ import annotations

import csv
from datetime import datetime, timedelta, UTC
from pathlib import Path

from src.research.provider_panel import build_provider_panel_report, parse_provider_bar_spec


def test_provider_panel_builds_generic_m7_artifacts(tmp_path: Path) -> None:
    polygon_csv = tmp_path / "polygon.csv"
    schwab_csv = tmp_path / "schwab.csv"
    _write_bars(polygon_csv, provider="polygon", volume_base=1000, volume_jump=0)
    _write_bars(schwab_csv, provider="schwab", volume_base=1000, volume_jump=300)

    artifacts = build_provider_panel_report(
        provider_bar_csvs={"polygon": polygon_csv, "schwab": schwab_csv},
        out_dir=tmp_path / "out",
        relative_volume_window=5,
    )

    assert artifacts.report_md.exists()
    assert artifacts.panel_csv.exists()

    bar_rows = list(csv.DictReader(artifacts.bar_parity_csv.open()))
    assert bar_rows[0]["provider_pair"] == "polygon_vs_schwab"
    assert bar_rows[0]["aligned_bars"] == "25"
    assert float(bar_rows[0]["price_p95_pct_diff"]) < 0.001
    assert float(bar_rows[0]["volume_p95_pct_diff"]) > 0.10

    relative_rows = list(csv.DictReader(artifacts.relative_volume_csv.open()))
    assert any(row["aggregate_minutes"] == "3" for row in relative_rows)
    assert "gate_flip_rate_ge_1_2" in relative_rows[0]

    feature_rows = list(csv.DictReader(artifacts.feature_parity_csv.open()))
    assert any(row["feature_pct_column"] == "directional_mass_pct" for row in feature_rows)
    assert all(row["provider_pair"] == "polygon_vs_schwab" for row in feature_rows)


def test_parse_provider_bar_spec_requires_provider_name() -> None:
    provider, path = parse_provider_bar_spec("schwab=/tmp/schwab.csv")

    assert provider == "schwab"
    assert path == Path("/tmp/schwab.csv")


def _write_bars(path: Path, *, provider: str, volume_base: int, volume_jump: int) -> None:
    start = datetime(2026, 5, 1, 13, 30, tzinfo=UTC)
    rows = []
    for idx in range(25):
        close = 100.0 + idx * 0.01
        rows.append(
            {
                "provider": provider,
                "symbol": "AMD",
                "timestamp": (start + timedelta(minutes=idx)).isoformat(),
                "open": close - 0.01,
                "high": close + 0.02,
                "low": close - 0.02,
                "close": close,
                "volume": volume_base + (volume_jump if idx >= 12 else 0),
            }
        )
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
