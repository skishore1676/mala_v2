from __future__ import annotations

import csv
from pathlib import Path

from src.research.provider_volume_parity import build_provider_volume_parity_report


def test_provider_volume_parity_reports_relative_volume_flip_rates(tmp_path: Path) -> None:
    source = tmp_path / "provider_divergence"
    source.mkdir()
    _write_csv(
        source / "AMD_provider_divergence.csv",
        [
            {
                "timestamp": f"2026-05-01T13:{30 + idx:02d}:00+00:00",
                "date_ct": "2026-05-01",
                "session": "regular",
                "volume_schwab": schwab,
                "volume_polygon": polygon,
                "directional_mass_pct": "0.25",
                "vpoc_4h_pct": "0.01",
            }
            for idx, (schwab, polygon) in enumerate(
                [
                    (100, 100),
                    (100, 100),
                    (100, 100),
                    (130, 100),
                    (120, 100),
                    (100, 100),
                ]
            )
        ],
    )

    artifacts = build_provider_volume_parity_report(
        divergence_dir=source,
        out_dir=tmp_path / "out",
        relative_volume_window=3,
    )

    assert artifacts.report_md.exists()
    relative_rows = list(csv.DictReader(artifacts.relative_volume_csv.open()))
    amd_one_minute = [
        row for row in relative_rows
        if row["symbol"] == "AMD" and row["aggregate_minutes"] == "1"
    ][0]
    assert amd_one_minute["comparisons"] == "4"
    assert float(amd_one_minute["relative_volume_median_pct_diff"]) > 0
    assert "gate_flip_rate_ge_1_2" in amd_one_minute

    feature_rows = list(csv.DictReader(artifacts.feature_csv.open()))
    assert any(row["feature_pct_column"] == "directional_mass_pct" for row in feature_rows)


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
