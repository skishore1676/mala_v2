from __future__ import annotations

import csv
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

import polars as pl

from src.research.playbook_surface import _evaluate_one_event, _match_grade, run_playbook_surface
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


def test_match_grade_requires_calibration_and_holdout_confirmation() -> None:
    assert _match_grade(100, 80, 20, -0.01, 0.10, 0.55) == "partial"
    assert _match_grade(100, 80, 20, 0.01, 0.10, 0.55) == "favorable"


def test_sample_event_excursion_stops_at_evaluated_exit_path() -> None:
    trade_date = date(2025, 1, 2)
    rows = [
        {
            "timestamp": datetime(2025, 1, 2, 14, 30, tzinfo=timezone.utc),
            "_playbook_trade_date": trade_date,
            "_playbook_bar_time": time(9, 30),
            "signal_direction": "long",
            "close": 100.0,
            "low": 99.8,
            "high": 100.1,
            "playbook_reversal_low": 99.0,
            "playbook_stretch_value": 2.0,
            "impulse_regime_5m": "bullish",
            "gap_state": "flat",
            "forward_mfe_eod": 10.0,
            "forward_mae_eod": 10.0,
        },
        {
            "timestamp": datetime(2025, 1, 2, 14, 31, tzinfo=timezone.utc),
            "_playbook_trade_date": trade_date,
            "_playbook_bar_time": time(9, 31),
            "close": 99.5,
            "low": 99.0,
            "high": 100.2,
        },
    ]

    event = _evaluate_one_event(
        "IWM",
        "cfg",
        {
            "stop_family": "reversal_extreme",
            "exit_family": "fixed_1r",
            "stretch_source": "opening_vwap",
            "stretch_threshold": 1.5,
            "reversal_range_minutes": 5,
            "confirming_bars": 1,
        },
        rows,
        0,
    )

    assert event is not None
    assert event["pnl_r"] == "-1.0"
    assert event["max_favorable_excursion_r"] == "0.0"
    assert event["max_adverse_excursion_r"] == "1.0"
