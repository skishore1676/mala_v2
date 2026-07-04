"""Tests for P1 playbook tagging (src/research/playbook_tagging.py)."""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import polars as pl
import pytest

from src.research.playbook_tagging import (
    SymbolBars,
    Trade,
    cluster_episodes,
    tag_episode,
)

ET = ZoneInfo("America/New_York")


def _trade(sym: str, dt: datetime, right: int, fill: str = "x") -> Trade:
    return Trade(
        underlying=sym, entry_dt=dt, thesis_dir=right, dte=0,
        holding_minutes=10.0, pnl=None, opening_fill_id=fill,
    )


def test_cluster_episodes_groups_within_gap_and_splits_on_direction():
    t0 = datetime(2026, 1, 5, 9, 40, tzinfo=ET)
    trades = [
        _trade("IWM", t0, 1),
        _trade("IWM", t0 + timedelta(minutes=4), 1),   # same episode (X2)
        _trade("IWM", t0 + timedelta(minutes=25), 1),  # new episode (>10m gap)
        _trade("IWM", t0 + timedelta(minutes=26), -1), # new episode (direction)
    ]
    eps = cluster_episodes(trades)
    assert [e.n_fills for e in sorted(eps, key=lambda e: e.entry_dt)] == [2, 1, 1]


def _base_features(**over) -> dict:
    f = dict(
        px=100.0, minutes_since_open=120, ret_5=0.0, ret_15=0.001, ret_30=0.002,
        ret_60=0.003, vwap_dist_pct=0.001, vwap_dist_atr=0.1, stretch_pctile=30.0,
        move_dir=1, extreme_px=101.0, fresh_extreme_min=60, failed_retest=False,
        run_len=1, flash_mag=0.5, vol_ratio_15=1.0, trend_side_frac_60=0.6,
        trend_dir=1, vma10_5m=100.0, vma10_dist_pct=0.01, touched_vma10=False,
        pullback_depth_frac=0.9, range_width_90m_pctile=60.0, gap_pct=0.0,
    )
    f.update(over)
    return f


def test_flash_high_on_fresh_violent_counter_move():
    f = _base_features(move_dir=1, fresh_extreme_min=5, flash_mag=3.0, run_len=5)
    tag, conf, reason = tag_episode(f, thesis_dir=-1)  # put fading an up-flash
    assert (tag, conf) == ("FLASH_REVERSAL", "HIGH")
    assert "F1" in reason


def test_exhaustion_high_on_percentile_stretch_with_failed_retest():
    f = _base_features(
        vwap_dist_atr=2.0, stretch_pctile=92.0, failed_retest=True,
        fresh_extreme_min=50, move_dir=1, flash_mag=0.8,
    )
    tag, conf, reason = tag_episode(f, thesis_dir=-1)
    assert (tag, conf) == ("EXHAUSTION_REVERSAL", "HIGH")
    assert "E1'" in reason


def test_trend_high_on_vma10_touch_with_trend():
    f = _base_features(
        trend_dir=1, trend_side_frac_60=0.9, touched_vma10=True,
        move_dir=-1, ret_15=-0.0005, fresh_extreme_min=3, flash_mag=0.4,
    )
    tag, conf, reason = tag_episode(f, thesis_dir=1)
    assert (tag, conf) == ("TREND_CONTINUATION", "HIGH")
    assert "10-VMA" in reason


def test_range_high_on_compressed_base():
    f = _base_features(
        range_width_90m_pctile=15.0, minutes_since_open=180,
        trend_side_frac_60=0.55, move_dir=1, ret_15=0.0001,
    )
    tag, conf, _ = tag_episode(f, thesis_dir=1)
    assert (tag, conf) == ("RANGE_EXPANSION", "HIGH")


def test_unclassified_when_nothing_matches_and_empty_features():
    assert tag_episode({}, 1)[0] == "UNCLASSIFIED"
    f = _base_features()
    assert tag_episode(f, thesis_dir=1)[0] != "FLASH_REVERSAL"


def test_symbolbars_minute_axis_not_int8_wrapped(tmp_path: Path):
    """Regression: polars dt.hour() is Int8; hour*60+minute must not wrap mod
    256 (09:30 ET became '58' and silently corrupted all indexing)."""
    ts = [
        datetime(2026, 1, 5, 14, 30 + m, tzinfo=ZoneInfo("UTC"))  # 09:30 ET
        for m in range(30)
    ]
    df = pl.DataFrame(
        {
            "timestamp": ts,
            "ticker": ["TST"] * 30,
            "open": [100.0] * 30, "high": [101.0] * 30, "low": [99.0] * 30,
            "close": [100.0 + i * 0.1 for i in range(30)],
            "volume": [1000.0] * 30, "vwap": [100.0] * 30,
            "transactions": [10] * 30,
        }
    )
    (tmp_path / "TST").mkdir()
    df.write_parquet(tmp_path / "TST" / "2026-01-05.parquet")
    sb = SymbolBars("TST", tmp_path)
    mins = sb.days[datetime(2026, 1, 5).date()]["min_of_day"]
    assert mins[0] == 9 * 60 + 30
    assert (np.diff(mins) > 0).all()
