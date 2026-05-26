from __future__ import annotations

import csv
from pathlib import Path

from src.research.provider_inputs import build_provider_input_csvs


def test_provider_inputs_split_wide_three_way_csv(tmp_path: Path) -> None:
    wide = tmp_path / "amd_three_way_ohlcv_regular_2026-05-22.csv"
    _write_csv(
        wide,
        [
            {
                "timestamp": "2026-05-22T13:30:00+00:00",
                "polygon_open": "100",
                "polygon_high": "101",
                "polygon_low": "99",
                "polygon_close": "100.5",
                "polygon_volume": "1000",
                "schwab_open": "100",
                "schwab_high": "101",
                "schwab_low": "99",
                "schwab_close": "100.5",
                "schwab_volume": "900",
                "public_open": "100",
                "public_high": "101",
                "public_low": "99",
                "public_close": "100.5",
                "public_volume": "910",
            }
        ],
    )

    artifacts = build_provider_input_csvs(wide_csvs=[wide], out_dir=tmp_path / "out")

    assert set(artifacts.provider_csvs) == {"polygon", "public", "schwab"}
    polygon_rows = list(csv.DictReader(artifacts.provider_csvs["polygon"].open()))
    assert polygon_rows[0]["provider"] == "polygon"
    assert polygon_rows[0]["symbol"] == "AMD"
    assert polygon_rows[0]["close"] == "100.5"


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
