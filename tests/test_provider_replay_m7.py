from __future__ import annotations

import csv
from datetime import datetime, timedelta, UTC
from pathlib import Path

from src.research.provider_replay_m7 import (
    M7_PROVIDER_REPLAY_BY_PAIR_CSV,
    M7_PROVIDER_REPLAY_CSV,
    build_m7_provider_replay,
    runtime_provider_from_bhiksha_config,
)
from src.research.provider_validation_m6 import (
    M7_PROVIDER_TRANSLATION_JSON,
    M7_PROVIDER_VALIDATION_CSV,
    build_m7_provider_validation,
)


def test_m7_provider_replay_feeds_provider_validation(tmp_path: Path) -> None:
    run_dir = tmp_path / "runs" / "compression-idea" / "2026-05-26T120000"
    run_dir.mkdir(parents=True)
    row = {
        "catalog_key": "compression-idea__amd_long",
        "ticker": "AMD",
        "direction": "long",
        "strategy": "Compression Expansion Breakout",
        "compression_window": "3",
        "breakout_lookback": "3",
        "compression_factor": "0.5",
        "use_volume_filter": "false",
        "use_time_filter": "false",
    }
    _write_csv(run_dir / "CATALOG_SELECTED.csv", [row])
    _write_csv(run_dir / "M5_execution.csv", [{key: value for key, value in row.items() if key != "catalog_key"}])

    panel_csv = tmp_path / "provider_bar_panel.csv"
    _write_provider_panel(panel_csv)

    replay = build_m7_provider_replay(
        run_dirs=[run_dir],
        provider_panel_csv=panel_csv,
        baseline_provider="polygon",
        runtime_provider="schwab",
        runtime_provider_source="test",
    )

    assert replay.replay_csvs == [run_dir / M7_PROVIDER_REPLAY_CSV]
    replay_rows = list(csv.DictReader((run_dir / M7_PROVIDER_REPLAY_CSV).open()))
    assert replay_rows[0]["catalog_key"] == "compression-idea__amd_long"
    assert replay_rows[0]["scenario"] == "provider_like"
    assert replay_rows[0]["provider_pair"] == "polygon_vs_schwab"
    assert replay_rows[0]["gate_provider_pair"] == "polygon_vs_schwab"
    assert replay_rows[0]["gate_pair_selection_reason"] == "runtime_provider_pair"
    assert replay_rows[0]["runtime_provider"] == "schwab"
    assert replay_rows[0]["diagnostic_worst_pair"] in {"polygon_vs_public", "polygon_vs_schwab"}
    assert replay_rows[0]["entry_overlap_rate_vs_baseline"] != ""
    assert replay_rows[0]["signal_evidence_status"] == "present"
    assert (run_dir / M7_PROVIDER_REPLAY_BY_PAIR_CSV).exists()

    validation = build_m7_provider_validation(
        run_dirs=[run_dir],
        provider_replay_csv=run_dir / M7_PROVIDER_REPLAY_CSV,
    )

    assert validation.provider_validation_csvs == [run_dir / M7_PROVIDER_VALIDATION_CSV]
    validation_rows = list(csv.DictReader((run_dir / M7_PROVIDER_VALIDATION_CSV).open()))
    assert validation_rows[0]["provider_validation_status"] in {"provider_pass", "provider_watch"}
    assert (run_dir / M7_PROVIDER_TRANSLATION_JSON).exists()


def test_runtime_provider_from_bhiksha_config(tmp_path: Path) -> None:
    config = tmp_path / "providers.yaml"
    config.write_text(
        "underlying_live_primary: schwab\nunderlying_backfill_primary: schwab\n",
        encoding="utf-8",
    )

    provider, source = runtime_provider_from_bhiksha_config(config)

    assert provider == "schwab"
    assert source.startswith("bhiksha_providers_config:")


def _write_provider_panel(path: Path) -> None:
    start = datetime(2026, 5, 22, 13, 30, tzinfo=UTC)
    rows = []
    for provider in ("polygon", "schwab", "public"):
        for idx in range(80):
            close = 100.0 + idx * 0.01
            if idx == 60:
                close += 1.0
            if provider == "public":
                close = 100.0
            volume = 1000 + idx
            if provider == "schwab" and idx % 10 == 0:
                volume += 100
            rows.append(
                {
                    "provider": provider,
                    "symbol": "AMD",
                    "timestamp": (start + timedelta(minutes=idx)).isoformat(),
                    "trade_date": "2026-05-22",
                    "session": "regular",
                    "open": close - 0.01,
                    "high": close + 0.02,
                    "low": close - 0.02,
                    "close": close,
                    "volume": volume,
                }
            )
    _write_csv(path, rows)


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
