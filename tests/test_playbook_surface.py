from __future__ import annotations

import csv
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import polars as pl

from src.research.playbook_surface import run_playbook_surface
from src.strategy.intraday_mean_reversion import PLAYBOOK_ID


def test_playbook_surface_writes_contract_artifacts(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    symbol_dir = data_dir / "IWM"
    symbol_dir.mkdir(parents=True)
    day = date(2025, 1, 2)
    rows = []
    start = datetime(2025, 1, 2, 14, 30, tzinfo=timezone.utc)
    for minute in range(80):
        close = 100.0 + (minute * 0.02)
        rows.append(
            {
                "timestamp": start + timedelta(minutes=minute),
                "ticker": "IWM",
                "open": close - 0.01,
                "high": close + 0.05,
                "low": close - 0.05,
                "close": close,
                "volume": 1000.0 + minute,
            }
        )
    pl.DataFrame(rows).write_parquet(symbol_dir / f"{day.isoformat()}.parquet")

    out_dir = tmp_path / "surface"
    result = run_playbook_surface(
        PLAYBOOK_ID,
        symbols=["IWM"],
        start=day,
        end=day,
        out_dir=out_dir,
        data_dir=data_dir,
        max_configs=1,
        max_events_per_bin=2,
    )

    assert result.out_dir == out_dir
    for filename in [
        "RECEIPT.md",
        "conditional_surface_by_symbol.csv",
        "feature_bins_by_symbol.csv",
        "sample_events.csv",
        "config.json",
    ]:
        assert (out_dir / filename).exists()

    with (out_dir / "conditional_surface_by_symbol.csv").open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert rows
    assert {"gap_state_filter", "volume_confirmation_filter", "match_grade"}.issubset(rows[0])
