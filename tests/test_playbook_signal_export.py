from __future__ import annotations

import csv
import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import polars as pl

from src.research.playbook_signal_export import export_playbook_signal_events
from src.strategy.intraday_mean_reversion import PLAYBOOK_ID


def test_export_playbook_signal_events_writes_config_specific_policy_id(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    symbol_dir = data_dir / "IWM"
    symbol_dir.mkdir(parents=True)
    day = date(2026, 5, 15)
    prior_day = date(2026, 5, 14)
    prior_rows = [
        {
            "timestamp": datetime(2026, 5, 14, 13, 30, tzinfo=timezone.utc) + timedelta(minutes=index),
            "ticker": "IWM",
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.0,
            "volume": 1000.0,
        }
        for index in range(6)
    ]
    pl.DataFrame(prior_rows).write_parquet(symbol_dir / f"{prior_day.isoformat()}.parquet")

    start = datetime(2026, 5, 15, 13, 30, tzinfo=timezone.utc)
    closes = [103.0, 104.0, 105.0, 104.0, 103.0, 102.5]
    rows = [
        {
            "timestamp": start + timedelta(minutes=index),
            "ticker": "IWM",
            "open": close,
            "high": high,
            "low": low,
            "close": close,
            "volume": 1000.0,
        }
        for index, (close, high, low) in enumerate(
            zip(closes, [103.2, 104.2, 105.2, 104.2, 103.2, 103.0], [102.9, 103.9, 104.9, 103.9, 102.9, 102.4], strict=True)
        )
    ]
    pl.DataFrame(rows).write_parquet(symbol_dir / f"{day.isoformat()}.parquet")
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    run_config = {
        "playbook_id": PLAYBOOK_ID,
        "symbols": ["IWM"],
        "start": prior_day.isoformat(),
        "end": day.isoformat(),
        "configs": {
            "cfg_short": {
                "stretch_source": "prior_rth_close_atr",
                "stretch_threshold": 2.0,
                "reversal_range_minutes": 5,
                "confirming_bars": 1,
                "velocity_periods_back": 5,
                "velocity_filter": "no_filter",
                "stage_filter": "no_filter",
                "gap_state_filter": "no_filter",
                "use_jerk_confirmation": False,
                "stop_family": "reversal_extreme",
                "exit_family": "fixed_1r",
            }
        },
    }
    (run_dir / "config.json").write_text(json.dumps(run_config) + "\n", encoding="utf-8")

    out_path = export_playbook_signal_events(
        run_dir,
        out_path=tmp_path / "mala_events.csv",
        data_dir=data_dir,
    )

    with out_path.open(newline="", encoding="utf-8") as handle:
        events = list(csv.DictReader(handle))
    assert len(events) == 1
    assert events[0]["policy_id"] == "cfg_short"
    assert events[0]["direction"] == "short"
