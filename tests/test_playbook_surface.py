from __future__ import annotations

import csv
import json
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import polars as pl
import pytest

from src.newton.engine import PhysicsEngine
from src.research.playbook_consultation_log import (
    append_consultation_query,
    dedupe_consultation_rows,
    open_consultation_rows,
    replay_close_consultation_row,
    summarize_consultation_log,
    update_consultation_row,
)
from src.research.playbook_policy_card import build_policy_card
from src.research.playbook_operator_policy import load_operator_policy
from src.research.playbook_packet_registry import write_mean_reversion_playbook_packet
from src.research.playbook_packet_registry import write_mean_reversion_live_execution_packet
from src.research.playbook_packet_registry import write_mean_reversion_shadow_execution_packet
from src.research.playbook_surface import (
    _entry_signal_cache_key,
    _evaluate_one_event,
    _match_grade,
    _playbook_configs,
    run_playbook_surface,
)
from src.research.playbook_surface_query import (
    STATE_MANAGEMENT_FEATURES,
    _analog_quality,
    _evaluate_management_spec,
    _entry_window_scope,
    _parse_timestamp,
    _state_management_verdict,
    _state_percentiles,
    query_playbook_surface,
)
from src.research.playbook_surface_review import build_surface_review
from src.research.playbook_tradingview_review import build_tradingview_review
from src.research.playbook_visual_review import _event_groups, _write_pine_script
from src.research.shared_kernel import ensure_kernel_on_path
from src.strategy.intraday_mean_reversion import PLAYBOOK_ID

ensure_kernel_on_path()
from mala_bhiksha_kernel import PacketKind, PacketStatus, read_packet  # noqa: E402


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
    assert {
        "gap_state_filter",
        "volume_confirmation_filter",
        "match_grade",
        "criteria_failed_count",
        "criteria_failed",
    }.issubset(rows[0])


def test_playbook_packet_registry_writes_shared_kernel_packet(tmp_path: Path) -> None:
    run_dir = tmp_path / "surface_run"
    run_dir.mkdir()
    (run_dir / "config.json").write_text(
        json.dumps(
            {
                "playbook_id": PLAYBOOK_ID,
                "strategy": "Intraday Mean Reversion at Extremes",
                "symbols": ["IWM", "QQQ"],
                "start": "2024-01-01",
                "end": "2026-05-15",
                "config_count": 2,
                "config_generation": "test",
                "calibration_holdout_split": "test split",
                "feature_families_tested": {"stretch": ["opening_vwap_rth"]},
                "match_grade_thresholds": {"minimum_sample_count": 50},
            }
        )
        + "\n",
        encoding="utf-8",
    )
    with (run_dir / "conditional_surface_by_symbol.csv").open(
        "w",
        newline="",
        encoding="utf-8",
    ) as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "config_id",
                "stop_family",
                "exit_family",
                "match_grade",
                "sample_count",
                "calibration_expectancy_r",
                "holdout_expectancy_r",
                "holdout_win_rate",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "config_id": "cfg_best",
                "stop_family": "reversal_extreme",
                "exit_family": "fixed_1_5r",
                "match_grade": "favorable",
                "sample_count": "80",
                "calibration_expectancy_r": "0.18",
                "holdout_expectancy_r": "0.22",
                "holdout_win_rate": "0.62",
            }
        )
        writer.writerow(
            {
                "config_id": "cfg_second",
                "stop_family": "reversal_midpoint",
                "exit_family": "fixed_1r",
                "match_grade": "near_favorable",
                "sample_count": "70",
                "calibration_expectancy_r": "0.12",
                "holdout_expectancy_r": "0.14",
                "holdout_win_rate": "0.58",
            }
        )

    packet_root = tmp_path / "registry"
    packet_path = write_mean_reversion_playbook_packet(run_dir, packet_root=packet_root)
    loaded = read_packet(
        packet_root,
        packet_id="playbook.mean_reversion_at_extremes.iwm_qqq",
        version=1,
        kind=PacketKind.PLAYBOOK,
    )

    assert packet_path.exists()
    assert (packet_root / "packet_index.json").exists()
    assert loaded.playbook_id == PLAYBOOK_ID
    assert loaded.symbol_scope == ["IWM", "QQQ"]
    assert loaded.management_policies[0].policy_id == "reversal_extreme__fixed_1_5r"
    assert loaded.management_policies[0].parameters["stop_anchor"] == "underlying_reversal_extreme"
    assert loaded.management_policies[0].parameters["target_r"] == 1.5
    assert loaded.management_policies[1].rank == 2


def test_playbook_packet_registry_writes_shadow_execution_packet(tmp_path: Path) -> None:
    run_dir = _write_packet_registry_run(tmp_path)
    packet_root = tmp_path / "registry"
    playbook_packet_path = write_mean_reversion_playbook_packet(
        run_dir,
        packet_root=packet_root,
    )
    parity_report_path = tmp_path / "PARITY_REPORT.json"
    parity_report_path.write_text(
        json.dumps(
            {
                "report_id": "parity.playbook.mean_reversion_at_extremes.iwm_qqq.test",
                "packet_ref": {
                    "packet_id": "playbook.mean_reversion_at_extremes.iwm_qqq",
                    "version": 1,
                    "kind": "playbook",
                },
                "status": "passed",
                "compared_event_count": 21127,
            }
        )
        + "\n",
        encoding="utf-8",
    )

    execution_packet_path = write_mean_reversion_shadow_execution_packet(
        playbook_packet_path=playbook_packet_path,
        parity_report_path=parity_report_path,
        packet_root=packet_root,
    )
    loaded = read_packet(
        packet_root,
        packet_id="execution.mean_reversion_at_extremes.iwm_qqq",
        version=1,
        kind=PacketKind.EXECUTION,
    )

    assert execution_packet_path.exists()
    assert loaded.status == PacketStatus.REVIEW
    assert loaded.operator_approval.status == "pending"
    assert loaded.source_packet.packet_id == "playbook.mean_reversion_at_extremes.iwm_qqq"
    assert loaded.runtime_mode.value == "shadow"
    assert loaded.parity_report_id == "parity.playbook.mean_reversion_at_extremes.iwm_qqq.test"
    assert loaded.runtime_controls["allowed_management_policy_ids"] == [
        "reversal_extreme__fixed_1_5r",
        "reversal_midpoint__fixed_1r",
    ]
    assert loaded.runtime_controls["management_policy_specs_required"] is True
    assert (
        loaded.runtime_controls["management_policy_specs"]["reversal_extreme__fixed_1_5r"]["stop_anchor"]
        == "underlying_reversal_extreme"
    )
    assert loaded.runtime_controls["management_policy_specs"]["reversal_midpoint__fixed_1r"]["target_r"] == 1.0
    assert loaded.runtime_controls["live_automated_allowed"] is False


def test_playbook_packet_registry_writes_live_approval_gated_execution_packet(tmp_path: Path) -> None:
    run_dir = _write_packet_registry_run(tmp_path)
    packet_root = tmp_path / "registry"
    playbook_packet_path = write_mean_reversion_playbook_packet(
        run_dir,
        packet_root=packet_root,
    )
    parity_report_path = tmp_path / "PARITY_REPORT.json"
    parity_report_path.write_text(
        json.dumps(
            {
                "report_id": "parity.playbook.mean_reversion_at_extremes.iwm_qqq.test",
                "packet_ref": {
                    "packet_id": "playbook.mean_reversion_at_extremes.iwm_qqq",
                    "version": 1,
                    "kind": "playbook",
                },
                "status": "passed",
                "compared_event_count": 21127,
            }
        )
        + "\n",
        encoding="utf-8",
    )

    execution_packet_path = write_mean_reversion_live_execution_packet(
        playbook_packet_path=playbook_packet_path,
        parity_report_path=parity_report_path,
        packet_root=packet_root,
        status=PacketStatus.APPROVED,
        operator="Suman",
        operator_notes="Monday small-account pilot.",
    )
    loaded = read_packet(
        packet_root,
        packet_id="execution.mean_reversion_at_extremes.iwm_qqq",
        version=2,
        kind=PacketKind.EXECUTION,
    )

    assert execution_packet_path.exists()
    assert loaded.status == PacketStatus.APPROVED
    assert loaded.operator_approval.status == "approved"
    assert loaded.operator_approval.actor == "Suman"
    assert loaded.runtime_mode.value == "live_approval_gated"
    assert loaded.runtime_controls["shadow_only"] is False
    assert loaded.runtime_controls["live_automated_allowed"] is False
    assert loaded.runtime_controls["live_ticket_required"] is True
    assert loaded.runtime_controls["requires_underlying_stop_price"] is True
    assert loaded.runtime_controls["live_management_required"] is True
    assert loaded.runtime_controls["max_live_quantity"] == 1
    assert loaded.runtime_controls["max_trade_premium_usd"] == 300.0


def _write_packet_registry_run(tmp_path: Path) -> Path:
    run_dir = tmp_path / "surface_run"
    run_dir.mkdir()
    (run_dir / "config.json").write_text(
        json.dumps(
            {
                "playbook_id": PLAYBOOK_ID,
                "strategy": "Intraday Mean Reversion at Extremes",
                "symbols": ["IWM", "QQQ"],
                "start": "2024-01-01",
                "end": "2026-05-15",
                "config_count": 2,
                "config_generation": "test",
                "calibration_holdout_split": "test split",
                "feature_families_tested": {"stretch": ["opening_vwap_rth"]},
                "match_grade_thresholds": {"minimum_sample_count": 50},
            }
        )
        + "\n",
        encoding="utf-8",
    )
    with (run_dir / "conditional_surface_by_symbol.csv").open(
        "w",
        newline="",
        encoding="utf-8",
    ) as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "config_id",
                "stop_family",
                "exit_family",
                "match_grade",
                "sample_count",
                "calibration_expectancy_r",
                "holdout_expectancy_r",
                "holdout_win_rate",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "config_id": "cfg_best",
                "stop_family": "reversal_extreme",
                "exit_family": "fixed_1_5r",
                "match_grade": "favorable",
                "sample_count": "80",
                "calibration_expectancy_r": "0.18",
                "holdout_expectancy_r": "0.22",
                "holdout_win_rate": "0.62",
            }
        )
        writer.writerow(
            {
                "config_id": "cfg_second",
                "stop_family": "reversal_midpoint",
                "exit_family": "fixed_1r",
                "match_grade": "near_favorable",
                "sample_count": "70",
                "calibration_expectancy_r": "0.12",
                "holdout_expectancy_r": "0.14",
                "holdout_win_rate": "0.58",
            }
        )
    return run_dir


def test_match_grade_requires_calibration_and_holdout_confirmation() -> None:
    assert _match_grade(100, 80, 20, -0.01, 0.10, 0.55) == "partial"
    assert _match_grade(100, 80, 20, 0.01, 0.10, 0.55, 0.55) == "partial"
    assert _match_grade(100, 80, 20, 0.09, 0.12, 0.58, 0.59) == "near_favorable"
    assert _match_grade(100, 80, 20, 0.12, 0.14, 0.58, 0.60) == "favorable"


def test_entry_signal_cache_key_ignores_stop_and_exit_only() -> None:
    base = {
        "stretch_source": "opening_vwap",
        "stretch_threshold": 2.0,
        "stage_filter": "bullish",
        "stop_family": "reversal_extreme",
        "exit_family": "fixed_1r",
    }
    different_exit = dict(base, stop_family="reversal_midpoint", exit_family="fixed_2r")
    different_entry = dict(base, stage_filter="accumulation")

    assert _entry_signal_cache_key(base) == _entry_signal_cache_key(different_exit)
    assert _entry_signal_cache_key(base) != _entry_signal_cache_key(different_entry)


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
            "market_pulse_stage": "bullish",
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
    assert event["event_timestamp_et"] == "2025-01-02T09:30:00-05:00"
    assert event["pnl_r"] == "-1.0"
    assert event["max_favorable_excursion_r"] == "0.0"
    assert event["max_adverse_excursion_r"] == "1.0"


def test_market_pulse_flip_exit_uses_one_minute_stage() -> None:
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
            "market_pulse_stage": "accumulation",
            "market_pulse_stage_5m": "bullish",
            "gap_state": "flat",
        },
        {
            "timestamp": datetime(2025, 1, 2, 14, 31, tzinfo=timezone.utc),
            "_playbook_trade_date": trade_date,
            "_playbook_bar_time": time(9, 31),
            "close": 100.2,
            "low": 99.9,
            "high": 100.4,
            "market_pulse_stage": "accumulation",
            "market_pulse_stage_5m": "bearish",
        },
        {
            "timestamp": datetime(2025, 1, 2, 14, 32, tzinfo=timezone.utc),
            "_playbook_trade_date": trade_date,
            "_playbook_bar_time": time(9, 32),
            "close": 100.4,
            "low": 100.1,
            "high": 100.6,
            "market_pulse_stage": "bearish",
            "market_pulse_stage_5m": "bullish",
        },
    ]

    event = _evaluate_one_event(
        "QQQ",
        "cfg",
        {
            "stop_family": "reversal_extreme",
            "exit_family": "market_pulse_flip",
            "stretch_source": "opening_vwap",
            "stretch_threshold": 1.5,
            "reversal_range_minutes": 5,
            "confirming_bars": 1,
        },
        rows,
        0,
    )

    assert event is not None
    assert event["exit_family"] == "market_pulse_flip"
    assert event["exit_reference_price"] == "100.4"
    assert event["pnl_r"] == "0.4"
    assert "exit_reason=market_pulse_flip" in event["trigger_summary"]


def test_market_pulse_flip_exit_uses_configured_stage_timeframe() -> None:
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
            "market_pulse_stage": "accumulation",
            "market_pulse_stage_5m": "bullish",
            "gap_state": "flat",
        },
        {
            "timestamp": datetime(2025, 1, 2, 14, 31, tzinfo=timezone.utc),
            "_playbook_trade_date": trade_date,
            "_playbook_bar_time": time(9, 31),
            "close": 100.2,
            "low": 99.9,
            "high": 100.4,
            "market_pulse_stage": "accumulation",
            "market_pulse_stage_5m": "bearish",
        },
        {
            "timestamp": datetime(2025, 1, 2, 14, 32, tzinfo=timezone.utc),
            "_playbook_trade_date": trade_date,
            "_playbook_bar_time": time(9, 32),
            "close": 100.4,
            "low": 100.1,
            "high": 100.6,
            "market_pulse_stage": "bearish",
            "market_pulse_stage_5m": "bullish",
        },
    ]

    event = _evaluate_one_event(
        "QQQ",
        "cfg",
        {
            "stage_timeframe": "5m",
            "stop_family": "reversal_extreme",
            "exit_family": "market_pulse_flip",
            "stretch_source": "opening_vwap",
            "stretch_threshold": 1.5,
            "reversal_range_minutes": 5,
            "confirming_bars": 1,
        },
        rows,
        0,
    )

    assert event is not None
    assert event["exit_reference_price"] == "100.2"
    assert event["pnl_r"] == "0.2"
    assert "actual=bullish" in event["stage_summary"]


def test_market_pulse_flip_exit_is_directional_for_shorts() -> None:
    trade_date = date(2025, 1, 2)
    rows = [
        {
            "timestamp": datetime(2025, 1, 2, 14, 30, tzinfo=timezone.utc),
            "_playbook_trade_date": trade_date,
            "_playbook_bar_time": time(9, 30),
            "signal_direction": "short",
            "close": 100.0,
            "low": 99.9,
            "high": 100.2,
            "playbook_reversal_high": 101.0,
            "playbook_stretch_value": 2.0,
            "market_pulse_stage": "distribution",
            "gap_state": "flat",
        },
        {
            "timestamp": datetime(2025, 1, 2, 14, 31, tzinfo=timezone.utc),
            "_playbook_trade_date": trade_date,
            "_playbook_bar_time": time(9, 31),
            "close": 99.6,
            "low": 99.4,
            "high": 100.1,
            "market_pulse_stage": "bullish",
        },
    ]

    event = _evaluate_one_event(
        "QQQ",
        "cfg",
        {
            "stop_family": "reversal_extreme",
            "exit_family": "market_pulse_flip",
            "stretch_source": "opening_vwap",
            "stretch_threshold": 1.5,
            "reversal_range_minutes": 5,
            "confirming_bars": 1,
        },
        rows,
        0,
    )

    assert event is not None
    assert event["pnl_r"] == "0.4"
    assert "exit_reason=market_pulse_flip" in event["trigger_summary"]


def test_market_pulse_flip_is_in_balanced_surface_cap() -> None:
    configs = _playbook_configs(max_configs=64)
    assert "market_pulse_flip" in {config.get("exit_family") for config in configs}


def test_tradingview_review_queue_collapses_sample_event_variants(tmp_path: Path) -> None:
    run_dir = tmp_path / "surface"
    run_dir.mkdir()
    with (run_dir / "sample_events.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "symbol",
                "direction",
                "event_timestamp",
                "extension_summary",
                "gap_state",
                "exit_family",
                "outcome_label",
                "pnl_r",
                "max_favorable_excursion_r",
                "max_adverse_excursion_r",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "symbol": "IWM",
                "direction": "long",
                "event_timestamp": "2026-05-11T13:45:00+00:00",
                "extension_summary": "opening_vwap: trigger=-1.2",
                "gap_state": "flat",
                "exit_family": "fixed_1r",
                "outcome_label": "win",
                "pnl_r": "1.0",
                "max_favorable_excursion_r": "2.0",
                "max_adverse_excursion_r": "0.2",
            }
        )
        writer.writerow(
            {
                "symbol": "IWM",
                "direction": "long",
                "event_timestamp": "2026-05-11T13:45:00+00:00",
                "extension_summary": "opening_vwap: trigger=-1.2",
                "gap_state": "flat",
                "exit_family": "time_stop",
                "outcome_label": "loss",
                "pnl_r": "-1.0",
                "max_favorable_excursion_r": "1.5",
                "max_adverse_excursion_r": "1.0",
            }
        )

    result = build_tradingview_review(run_dir, max_events=5, tv_symbol_overrides={"IWM": "NYSEARCA:IWM"})

    with result.queue_csv.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 1
    assert rows[0]["variant_count"] == "2"
    assert rows[0]["event_timestamp_et"] == "2026-05-11T09:45:00-04:00"
    assert rows[0]["tv_symbol"] == "NYSEARCA:IWM"
    assert rows[0]["pnl_r_min"] == "-1"
    assert rows[0]["pnl_r_max"] == "1"
    assert "npm run -s tv -- symbol NYSEARCA:IWM" in result.command_file.read_text(encoding="utf-8")


def test_visual_review_pine_uses_preferred_representative(tmp_path: Path) -> None:
    entry = datetime(2026, 5, 6, 14, 8, tzinfo=timezone.utc)
    preferred_exit = datetime(2026, 5, 6, 14, 40, tzinfo=timezone.utc)
    alternate_exit = datetime(2026, 5, 6, 15, 0, tzinfo=timezone.utc)
    et = ZoneInfo("America/New_York")
    events = [
        {
            "event_id": "E0001",
            "config_id": "slow",
            "symbol": "QQQ",
            "trade_date": date(2026, 5, 6),
            "direction": "long",
            "entry_timestamp_utc": entry,
            "entry_timestamp_et": entry.astimezone(et),
            "entry_unix": int(entry.timestamp()),
            "entry_price": 689.16,
            "exit_timestamp_utc": alternate_exit,
            "exit_timestamp_et": alternate_exit.astimezone(et),
            "exit_unix": int(alternate_exit.timestamp()),
            "exit_price": 690.0,
            "exit_reason": "time_stop",
            "pnl_r": 0.5,
            "max_favorable_excursion_r": 1.2,
            "max_adverse_excursion_r": 0.3,
            "stop_price": 687.62,
            "target_price": None,
            "stop_family": "reversal_midpoint",
            "exit_family": "time_stop",
            "stretch_source": "opening_vwap",
            "stretch_threshold": 2.0,
        },
        {
            "event_id": "E0002",
            "config_id": "preferred",
            "symbol": "QQQ",
            "trade_date": date(2026, 5, 6),
            "direction": "long",
            "entry_timestamp_utc": entry,
            "entry_timestamp_et": entry.astimezone(et),
            "entry_unix": int(entry.timestamp()),
            "entry_price": 689.16,
            "exit_timestamp_utc": preferred_exit,
            "exit_timestamp_et": preferred_exit.astimezone(et),
            "exit_unix": int(preferred_exit.timestamp()),
            "exit_price": 690.7,
            "exit_reason": "target",
            "pnl_r": 1.0,
            "max_favorable_excursion_r": 1.0,
            "max_adverse_excursion_r": 0.71,
            "stop_price": 687.62,
            "target_price": 690.7,
            "stop_family": "reversal_extreme",
            "exit_family": "fixed_1r",
            "stretch_source": "opening_vwap",
            "stretch_threshold": 2.0,
        },
    ]
    groups = _event_groups(
        events,
        preferred_stop_family="reversal_extreme",
        preferred_exit_family="fixed_1r",
    )
    assert groups[0]["review_event"]["event_id"] == "E0002"
    assert groups[0]["group_id"] == "G01"

    pine_path = tmp_path / "review.pine"
    _write_pine_script(
        pine_path,
        symbol="QQQ",
        review_dates=[date(2026, 5, 6)],
        groups=groups,
    )

    source = pine_path.read_text(encoding="utf-8")
    assert source.startswith("//@version=6")
    assert "Mala 2.2 QQQ Clean Review 2026-05-06 to 2026-05-06" in source
    # Drawing primitives are emitted once under `if barstate.isfirst`,
    # not as per-bar plot()/plotshape()/bgcolor() calls.
    assert "if barstate.isfirst" in source
    assert "plotshape(" not in source
    assert "bgcolor(" not in source
    assert "plot.style_linebr" not in source
    entry_ts = 'timestamp("America/New_York", 2026, 5, 6, 10, 8)'
    exit_ts = 'timestamp("America/New_York", 2026, 5, 6, 10, 40)'
    assert entry_ts in source
    assert exit_ts in source
    # box.new wraps the trade extent between stop and target (G01 is long → target above, stop below).
    assert (
        f"box.new(left={entry_ts}, top=math.max(690.7, 687.62), "
        f"right={exit_ts}, bottom=math.min(690.7, 687.62)"
    ) in source
    # line.new emits entry / stop / target as separate annotations.
    assert f"line.new(x1={entry_ts}, y1=689.16, x2={exit_ts}, y2=689.16" in source
    assert f"line.new(x1={entry_ts}, y1=687.62, x2={exit_ts}, y2=687.62" in source
    assert f"line.new(x1={entry_ts}, y1=690.7, x2={exit_ts}, y2=690.7" in source
    # Long trade label sits below the bar with an upward-pointing label_up style.
    assert "yloc=yloc.belowbar" in source
    assert "style=label.style_label_up" in source
    assert 'text="G01 ▲"' in source
    # The alternate-config exit (11:00 ET) must not slip into the rendered script.
    assert 'timestamp("America/New_York", 2026, 5, 6, 11, 0)' not in source


def test_surface_review_pack_ranks_candidates_and_events(tmp_path: Path) -> None:
    run_dir = tmp_path / "surface"
    run_dir.mkdir()
    with (run_dir / "conditional_surface_by_symbol.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "config_id",
                "symbol",
                "direction",
                "entry_cutoff_et",
                "stage_filter",
                "gap_state_filter",
                "extension_family",
                "extension_bin",
                "reversal_range_minutes",
                "volume_confirmation_filter",
                "stop_family",
                "exit_family",
                "sample_count",
                "calibration_count",
                "holdout_count",
                "calibration_expectancy_r",
                "holdout_expectancy_r",
                "calibration_win_rate",
                "holdout_win_rate",
                "match_grade",
                "criteria_failed_count",
                "criteria_failed",
                "evidence_note",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "config_id": "clean",
                "symbol": "QQQ",
                "direction": "long",
                "entry_cutoff_et": "10:15",
                "stage_filter": "bullish",
                "gap_state_filter": "flat",
                "extension_family": "prior_close_atr",
                "extension_bin": "1.0",
                "reversal_range_minutes": "5",
                "volume_confirmation_filter": "no_filter",
                "stop_family": "reversal_extreme",
                "exit_family": "fixed_1r",
                "sample_count": "90",
                "calibration_count": "70",
                "holdout_count": "20",
                "calibration_expectancy_r": "0.09",
                "holdout_expectancy_r": "0.12",
                "calibration_win_rate": "0.56",
                "holdout_win_rate": "0.62",
                "match_grade": "near_favorable",
                "criteria_failed_count": "1",
                "criteria_failed": "calibration_expectancy_below_floor",
                "evidence_note": "effect size below 0.1R expectancy floor",
            }
        )
        writer.writerow(
            {
                "config_id": "thin",
                "symbol": "QQQ",
                "direction": "short",
                "extension_family": "opening_vwap",
                "extension_bin": "2.0",
                "exit_family": "fixed_1r",
                "sample_count": "9",
                "holdout_count": "2",
                "holdout_expectancy_r": "1.0",
                "match_grade": "insufficient",
            }
        )
    with (run_dir / "sample_events.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "config_id",
                "symbol",
                "direction",
                "event_timestamp",
                "entry_reference_price",
                "extension_summary",
                "stage_summary",
                "gap_state",
                "trigger_summary",
                "volume_confirmation_summary",
                "stop_reference_price",
                "exit_reference_price",
                "exit_family",
                "outcome_label",
                "pnl_r",
                "max_favorable_excursion_r",
                "max_adverse_excursion_r",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "config_id": "clean",
                "symbol": "IWM",
                "direction": "short",
                "event_timestamp": "2026-05-12T13:45:00+00:00",
                "entry_reference_price": "100",
                "extension_summary": "prior_close_atr: trigger=1.2",
                "stage_summary": "filter=bullish",
                "gap_state": "flat",
                "trigger_summary": "5m reversal",
                "volume_confirmation_summary": "no_filter",
                "stop_reference_price": "101",
                "exit_reference_price": "99",
                "exit_family": "fixed_1r",
                "outcome_label": "win",
                "pnl_r": "1.0",
                "max_favorable_excursion_r": "1.2",
                "max_adverse_excursion_r": "0.2",
            }
        )
        writer.writerow(
            {
                "config_id": "clean",
                "symbol": "QQQ",
                "direction": "long",
                "event_timestamp": "2026-05-11T13:45:00+00:00",
                "entry_reference_price": "100",
                "extension_summary": "prior_close_atr: trigger=-1.2",
                "stage_summary": "filter=bullish",
                "gap_state": "flat",
                "trigger_summary": "5m reversal",
                "volume_confirmation_summary": "no_filter",
                "stop_reference_price": "99",
                "exit_reference_price": "101",
                "exit_family": "fixed_1r",
                "outcome_label": "win",
                "pnl_r": "1.0",
                "max_favorable_excursion_r": "1.2",
                "max_adverse_excursion_r": "0.2",
            }
        )

    result = build_surface_review(run_dir, max_candidates=5)

    assert result.candidate_count == 1
    with result.candidate_csv.open(newline="", encoding="utf-8") as handle:
        candidates = list(csv.DictReader(handle))
    assert candidates[0]["config_id"] == "clean"
    assert candidates[0]["candidate_type"] == "clean_reversion_candidate"
    assert candidates[0]["match_grade"] == "near_favorable"
    assert candidates[0]["criteria_failed_count"] == "1"
    with result.chart_review_csv.open(newline="", encoding="utf-8") as handle:
        events = list(csv.DictReader(handle))
    assert len(events) == 1
    assert events[0]["candidate_rank"] == "1"
    assert events[0]["event_timestamp"] == "2026-05-11T13:45:00+00:00"
    assert events[0]["symbol"] == "QQQ"
    assert events[0]["direction"] == "long"
    assert "near-favorable leads" in result.review_md.read_text(encoding="utf-8")


def test_playbook_surface_query_writes_operator_review(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    symbol_dir = data_dir / "QQQ"
    symbol_dir.mkdir(parents=True)
    day = date(2025, 1, 2)
    start = datetime(2025, 1, 2, 14, 30, tzinfo=timezone.utc)
    closes = [100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 103.7, 103.4]
    rows = []
    for minute, close in enumerate(closes):
        rows.append(
            {
                "timestamp": start + timedelta(minutes=minute),
                "ticker": "QQQ",
                "open": close - 0.05,
                "high": close + 0.25,
                "low": close - 0.2,
                "close": close,
                "volume": 1000.0,
            }
        )
    pl.DataFrame(rows).write_parquet(symbol_dir / f"{day.isoformat()}.parquet")

    config = {
        "entry_window_start": "09:30",
        "entry_window_end": "10:15",
        "stretch_source": "opening_vwap",
        "stretch_threshold": 0.5,
        "z_score_window": 5,
        "reversal_range_minutes": 2,
        "confirming_bars": 1,
        "velocity_periods_back": 5,
        "velocity_filter": "no_filter",
        "stage_filter": "no_filter",
        "gap_state_filter": "no_filter",
        "use_jerk_confirmation": False,
        "relative_volume_threshold": None,
        "stop_family": "reversal_extreme",
        "exit_family": "fixed_1r",
    }
    run_dir = tmp_path / "surface"
    review_dir = run_dir / "surface_review"
    review_dir.mkdir(parents=True)
    (run_dir / "config.json").write_text(
        json_dumps(
            {
                "playbook_id": PLAYBOOK_ID,
                "configs": {"cfg1": config},
            }
        ),
        encoding="utf-8",
    )
    with (run_dir / "conditional_surface_by_symbol.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "config_id",
                "symbol",
                "direction",
                "entry_cutoff_et",
                "stage_filter",
                "gap_state_filter",
                "extension_family",
                "extension_bin",
                "reversal_range_minutes",
                "volume_confirmation_filter",
                "stop_family",
                "exit_family",
                "sample_count",
                "calibration_count",
                "holdout_count",
                "calibration_expectancy_r",
                "holdout_expectancy_r",
                "calibration_win_rate",
                "holdout_win_rate",
                "match_grade",
                "criteria_failed_count",
                "criteria_failed",
                "evidence_note",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "config_id": "cfg1",
                "symbol": "QQQ",
                "direction": "short",
                "entry_cutoff_et": "10:15",
                "stage_filter": "no_filter",
                "gap_state_filter": "no_filter",
                "extension_family": "opening_vwap",
                "extension_bin": "0.5",
                "reversal_range_minutes": "2",
                "volume_confirmation_filter": "no_filter",
                "stop_family": "reversal_extreme",
                "exit_family": "fixed_1r",
                "sample_count": "80",
                "calibration_count": "60",
                "holdout_count": "20",
                "calibration_expectancy_r": "0.06",
                "holdout_expectancy_r": "0.14",
                "calibration_win_rate": "0.53",
                "holdout_win_rate": "0.58",
                "match_grade": "partial",
                "criteria_failed_count": "2",
                "criteria_failed": "calibration_expectancy_below_floor | expectancy_drift_exceeds_bound",
                "evidence_note": "review candidate",
            }
        )
    with (review_dir / "candidate_regions.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "rank",
                "candidate_type",
                "review_priority",
                "config_id",
                "symbol",
                "direction",
                "score",
                "trader_note",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "rank": "1",
                "candidate_type": "clean_reversion_candidate",
                "review_priority": "high",
                "config_id": "cfg1",
                "symbol": "QQQ",
                "direction": "short",
                "score": "0.2",
                "trader_note": "test note",
            }
        )

    result = query_playbook_surface(
        run_dir,
        symbol="QQQ",
        direction="short",
        timestamp=datetime(2025, 1, 2, 9, 36, tzinfo=timezone(timedelta(hours=-5))),
        mode="signal",
        data_dir=data_dir,
    )

    assert result.verdict == "promising"
    assert result.active_matches == 1
    text = result.review_md.read_text(encoding="utf-8")
    assert "Management Packet" in text
    assert "stop at the reversal range extreme" in text

    with pytest.raises(ValueError, match="mode must be 'signal' or 'state-management'"):
        query_playbook_surface(
            run_dir,
            symbol="QQQ",
            direction="short",
            timestamp=datetime(2025, 1, 2, 9, 31, tzinfo=timezone(timedelta(hours=-5))),
            mode="legacy-management",
            data_dir=data_dir,
            out_dir=tmp_path / "override_query",
        )


def test_playbook_surface_query_parses_operator_timestamp() -> None:
    parsed = _parse_timestamp("2026-05-11 09:45 America/New_York")

    assert parsed.tzinfo is not None
    assert parsed.hour == 9
    assert parsed.minute == 45

    parsed_ct = _parse_timestamp("2026-04-21 08:50 America/Chicago")
    assert parsed_ct.tzinfo is not None
    assert parsed_ct.astimezone(ZoneInfo("America/New_York")).hour == 9
    assert parsed_ct.astimezone(ZoneInfo("America/New_York")).minute == 50


def test_playbook_surface_state_management_returns_cohort(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    symbol_dir = data_dir / "IWM"
    symbol_dir.mkdir(parents=True)
    start_day = date(2025, 1, 2)
    for day_offset in range(22):
        day = start_day + timedelta(days=day_offset)
        start = datetime(day.year, day.month, day.day, 14, 30, tzinfo=timezone.utc)
        rows = []
        base = 100.0 + day_offset * 0.15
        for minute in range(45):
            close = base + (minute * 0.03) - (0.25 if minute > 8 else 0.0)
            rows.append(
                {
                    "timestamp": start + timedelta(minutes=minute),
                    "ticker": "IWM",
                    "open": close - 0.03,
                    "high": close + 0.08,
                    "low": close - 0.08,
                    "close": close,
                    "volume": 1000.0 + minute,
                }
            )
        pl.DataFrame(rows).write_parquet(symbol_dir / f"{day.isoformat()}.parquet")

    run_dir = tmp_path / "surface"
    run_dir.mkdir()
    (run_dir / "config.json").write_text(
        json_dumps(
            {
                "playbook_id": PLAYBOOK_ID,
                "configs": {
                    "cfg": {
                        "entry_window_start": "09:30",
                        "entry_window_end": "10:15",
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    with (run_dir / "conditional_surface_by_symbol.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["config_id", "symbol", "direction"])
        writer.writeheader()

    result = query_playbook_surface(
        run_dir,
        symbol="IWM",
        direction="short",
        timestamp=datetime(2025, 1, 23, 9, 36, tzinfo=timezone(timedelta(hours=-5))),
        mode="state-management",
        data_dir=data_dir,
        analog_lookback_days=30,
        analog_count=8,
    )

    assert result.verdict in {
        "strong_reversion_lean",
        "reversion_lean",
        "mixed_cohort",
        "continuation_lean",
        "strong_continuation_risk",
        "too_thin",
    }
    text = result.review_md.read_text(encoding="utf-8")
    assert "Historical Analog Cohort" in text
    assert "Management Menu" in text
    assert "scalp_0.25pct" in text
    assert "Survived" in text
    assert "Operator Policy" in text
    assert "mean_reversion_intraday_operator_v1" in text
    assert "Similarity Recipe" in text
    assert "tradable target floor" in text
    assert "This mode does not ask whether a rule fired." in text
    payload = json.loads(result.json_path.read_text(encoding="utf-8"))
    assert payload["operator_policy"]["policy_id"] == "mean_reversion_intraday_operator_v1"
    assert payload["operator_policy"]["config"]["take_policy"]["min_cohort_n"] == 60
    with (run_dir / "consultation_log.csv").open(newline="", encoding="utf-8") as handle:
        log_rows = list(csv.DictReader(handle))
    assert log_rows
    assert {
        "query_id",
        "playbook_id",
        "actual_exit_ts_et",
        "operator_note",
        "updated_at_utc",
    }.issubset(log_rows[0])
    assert "suggested_exit" not in log_rows[0]
    assert {"selected_exit", "taken", "actual_pnl_r"}.issubset(log_rows[0])

    query_id = log_rows[0]["query_id"]
    update_consultation_row(
        run_dir,
        query_id=query_id,
        selected_exit="scalp_0.25pct",
        reported_survived_pct="46.8%",
        taken="Y",
        actual_exit_reason="target",
        actual_pnl_r="0.5",
        actual_time_to_exit="8",
        actual_exit_ts_et="2025-01-23T09:44:00-05:00",
        operator_note="test close",
    )
    with (run_dir / "consultation_log.csv").open(newline="", encoding="utf-8") as handle:
        closed_rows = list(csv.DictReader(handle))
    assert closed_rows[0]["selected_exit"] == "scalp_0.25pct"
    assert closed_rows[0]["taken"] == "Y"
    assert closed_rows[0]["actual_pnl_r"] == "0.5"
    assert closed_rows[0]["updated_at_utc"]
    assert open_consultation_rows(run_dir) == []
    summary = summarize_consultation_log(run_dir)
    assert summary.total_rows == 1
    assert summary.open_rows == 0
    assert summary.closed_rows == 1
    assert summary.takes == 1
    assert summary.average_taken_pnl_r == 0.5
    assert summary.next_action == "add_more_chart_first_rows"

    append_consultation_query(
        run_dir,
        {
            "timestamp_et": closed_rows[0]["query_ts_et"],
            "playbook_id": PLAYBOOK_ID,
            "symbol": "IWM",
            "direction": "short",
            "verdict": "mixed_cohort",
            "cohort": {"confidence": "moderate", "analog_count": "8"},
        },
        result.review_md,
        result.json_path,
    )
    dedupe_consultation_rows(run_dir)
    with (run_dir / "consultation_log.csv").open(newline="", encoding="utf-8") as handle:
        deduped_rows = list(csv.DictReader(handle))
    assert len(deduped_rows) == 1
    assert deduped_rows[0]["query_id"] == query_id
    assert deduped_rows[0]["actual_pnl_r"] == "0.5"


def test_replay_close_populates_historical_actuals(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    symbol_dir = data_dir / "IWM"
    symbol_dir.mkdir(parents=True)
    start_day = date(2025, 1, 2)
    for day_offset in range(22):
        day = start_day + timedelta(days=day_offset)
        start = datetime(day.year, day.month, day.day, 14, 30, tzinfo=timezone.utc)
        rows = []
        base = 100.0 + day_offset * 0.10
        for minute in range(50):
            close = base + minute * 0.01
            if day_offset == 21 and minute >= 7:
                close = base + 0.07 - (minute - 6) * 0.08
            rows.append(
                {
                    "timestamp": start + timedelta(minutes=minute),
                    "ticker": "IWM",
                    "open": close - 0.02,
                    "high": close + 0.06,
                    "low": close - 0.06,
                    "close": close,
                    "volume": 1000.0 + minute,
                }
            )
        pl.DataFrame(rows).write_parquet(symbol_dir / f"{day.isoformat()}.parquet")

    run_dir = tmp_path / "surface"
    run_dir.mkdir()
    (run_dir / "config.json").write_text(
        json_dumps(
            {
                "playbook_id": PLAYBOOK_ID,
                "configs": {
                    "cfg": {
                        "entry_window_start": "09:30",
                        "entry_window_end": "10:15",
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    with (run_dir / "conditional_surface_by_symbol.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["config_id", "symbol", "direction"])
        writer.writeheader()

    result = query_playbook_surface(
        run_dir,
        symbol="IWM",
        direction="short",
        timestamp=datetime(2025, 1, 23, 9, 36, tzinfo=timezone(timedelta(hours=-5))),
        mode="state-management",
        data_dir=data_dir,
        analog_lookback_days=30,
        analog_count=8,
    )
    query_id = result.out_dir.name
    payload = json.loads(result.json_path.read_text(encoding="utf-8"))
    selected_exit = payload["cohort"]["management_rows"][0]["exit_family"]

    close_result = replay_close_consultation_row(
        run_dir,
        query_id=query_id,
        taken="Y",
        selected_exit=selected_exit,
        operator_note="would take in replay",
        data_dir=data_dir,
    )

    assert close_result.actual_exit_reason in {"target", "stop", "time_stop_30m"}
    assert close_result.actual_pnl_r != ""
    assert close_result.actual_time_to_exit != ""
    assert close_result.actual_exit_ts_et.startswith("2025-01-23T")
    with (run_dir / "consultation_log.csv").open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert rows[0]["taken"] == "Y"
    assert rows[0]["selected_exit"] == selected_exit
    assert rows[0]["actual_exit_reason"] == close_result.actual_exit_reason
    assert rows[0]["operator_note"] == "would take in replay"


def test_replay_close_pass_marks_no_trade(tmp_path: Path) -> None:
    run_dir = tmp_path / "surface"
    query_dir = run_dir / "surface_queries" / "iwm_short_demo"
    query_dir.mkdir(parents=True)
    query_json = query_dir / "query_result.json"
    review_md = query_dir / "QUERY_REVIEW.md"
    query_json.write_text(
        json_dumps(
            {
                "timestamp_et": "2025-01-23T09:36:00-05:00",
                "timestamp_utc": "2025-01-23T14:36:00+00:00",
                "playbook_id": PLAYBOOK_ID,
                "symbol": "IWM",
                "direction": "short",
                "verdict": "mixed_cohort",
                "cohort": {"confidence": "moderate", "analog_count": "8"},
            }
        ),
        encoding="utf-8",
    )
    review_md.write_text("# Review\n", encoding="utf-8")
    append_consultation_query(
        run_dir,
        json.loads(query_json.read_text(encoding="utf-8")),
        review_md,
        query_json,
    )

    result = replay_close_consultation_row(
        run_dir,
        query_id="iwm_short_demo",
        taken="N",
        operator_note="would pass",
    )

    assert result.actual_exit_reason == "no_trade"
    with (run_dir / "consultation_log.csv").open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert rows[0]["taken"] == "N"
    assert rows[0]["actual_exit_reason"] == "no_trade"
    assert rows[0]["actual_pnl_r"] == ""
    assert rows[0]["operator_note"] == "would pass"


def test_playbook_policy_card_writes_deterministic_operator_card(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    query_dir = run_dir / "surface_queries" / "iwm_short_demo_state_management"
    query_dir.mkdir(parents=True)
    query_json = query_dir / "query_result.json"
    review_md = query_dir / "QUERY_REVIEW.md"
    review_md.write_text("# Review\n", encoding="utf-8")
    payload = {
        "playbook_id": PLAYBOOK_ID,
        "source_run": str(run_dir),
        "symbol": "IWM",
        "direction": "short",
        "timestamp_et": "2026-04-21T10:40:00-04:00",
        "verdict": "strong_reversion_lean",
        "cohort": {
            "confidence": "moderate",
            "analog_count": 75,
            "candidate_count": 112654,
            "similarity_median": "0.31",
            "similarity_tail": {"rank_200_similarity": "0.49"},
            "analog_quality": {"label": "tight"},
            "outcome_summary": {
                "15": {"reversion_pct": "61.3%"},
                "60": {"reversion_pct": "52.0%"},
                "eod": {"reversion_pct": "48.0%"},
            },
            "management_rows": [
                {
                    "exit_family": "scalp_0.25pct",
                    "survived_pct": "46.8%",
                    "median_target_move": "0.5715",
                    "median_stop_move": "0.5715",
                    "median_time_to_target_min": "12",
                    "stop_reference": "symmetric adverse",
                    "reward_risk": "1.0",
                }
            ],
        },
        "state_percentiles": {
            "reference_scope": "Prior historical bars for the same symbol/requested bias.",
            "metrics": [
                {
                    "label": "VWAP stretch",
                    "value": "0.42%",
                    "percentile": "86th",
                    "reference_n": "112654",
                },
                {
                    "label": "prior-close ATR stretch",
                    "value": "1.21",
                    "percentile": "78th",
                    "reference_n": "112654",
                },
            ],
        },
    }
    query_json.write_text(json_dumps(payload), encoding="utf-8")
    append_consultation_query(run_dir, payload, review_md, query_json)

    result = build_policy_card(query_json, update_log=True, run_dir=run_dir)

    assert result.policy == "take"
    text = result.markdown_path.read_text(encoding="utf-8")
    assert "READ:    strong_reversion_lean" in text
    assert "STATE:   VWAP stretch 86th (0.42%)" in text
    assert "ANALOG:  tight cohort" in text
    assert "POLICY:  take" in text
    assert "EXIT:    scalp_0.25pct" in text
    assert "EOD reversion erodes to 48%" in text
    assert "mean_reversion_intraday_operator_v1" in text
    policy_payload = json.loads(result.json_path.read_text(encoding="utf-8"))
    assert policy_payload["operator_policy"]["policy_id"] == "mean_reversion_intraday_operator_v1"
    with (run_dir / "consultation_log.csv").open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert rows[0]["selected_exit"] == "scalp_0.25pct"
    assert rows[0]["reported_survived_pct"] == "46.8%"
    assert rows[0]["taken"] == ""


def test_playbook_policy_card_does_not_prefill_pass_policy(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    query_dir = run_dir / "surface_queries" / "iwm_short_mixed_state_management"
    query_dir.mkdir(parents=True)
    query_json = query_dir / "query_result.json"
    review_md = query_dir / "QUERY_REVIEW.md"
    review_md.write_text("# Review\n", encoding="utf-8")
    payload = {
        "playbook_id": PLAYBOOK_ID,
        "source_run": str(run_dir),
        "symbol": "IWM",
        "direction": "short",
        "timestamp_et": "2026-04-21T09:50:00-04:00",
        "verdict": "mixed_cohort",
        "cohort": {
            "confidence": "moderate",
            "analog_count": 75,
            "candidate_count": 112654,
            "outcome_summary": {"15": {"reversion_pct": "48.0%"}, "eod": {"reversion_pct": "53.3%"}},
            "management_rows": [
                {
                    "exit_family": "scalp_0.25pct",
                    "survived_pct": "46.8%",
                    "median_target_move": "0.5715",
                    "median_stop_move": "0.5715",
                    "median_time_to_target_min": "12",
                    "stop_reference": "symmetric adverse",
                    "reward_risk": "1.0",
                }
            ],
        },
    }
    query_json.write_text(json_dumps(payload), encoding="utf-8")
    append_consultation_query(run_dir, payload, review_md, query_json)

    result = build_policy_card(query_json, update_log=True, run_dir=run_dir)

    assert result.policy == "pass"
    with (run_dir / "consultation_log.csv").open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert rows[0]["selected_exit"] == ""


def test_playbook_policy_card_can_load_operator_policy_override(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    query_dir = run_dir / "surface_queries" / "iwm_short_override_state_management"
    query_dir.mkdir(parents=True)
    query_json = query_dir / "query_result.json"
    policy_config = tmp_path / "strict_policy.yaml"
    policy_config.write_text(
        """
policy_id: strict_demo_policy
policy_version: v1
playbook_id: mean-reversion-at-extremes-intraday
cohort:
  min_forward_n: 15
  confidence_min_counts:
    moderate: 60
    light: 30
read_thresholds:
  decision_window: "15"
  mixed_band_pp: 10
  strong_reversion_edge_pp: 20
  strong_continuation_edge_pp: 20
take_policy:
  take_verdicts:
    - strong_reversion_lean
  min_confidence: moderate
  min_cohort_n: 60
  min_exit_survived_pct: 60
management:
  min_target_atr_fraction: 0.10
  min_target_price_fraction: 0.0010
  exit_selection: max_survived_then_target_move
""".strip()
        + "\n",
        encoding="utf-8",
    )
    query_json.write_text(
        json_dumps(
            {
                "playbook_id": PLAYBOOK_ID,
                "source_run": str(run_dir),
                "symbol": "IWM",
                "direction": "short",
                "timestamp_et": "2026-04-21T10:40:00-04:00",
                "verdict": "strong_reversion_lean",
                "cohort": {
                    "confidence": "moderate",
                    "analog_count": 75,
                    "candidate_count": 100,
                    "outcome_summary": {"15": {"reversion_pct": "62.0%"}, "eod": {"reversion_pct": "51.0%"}},
                    "management_rows": [
                        {
                            "exit_family": "scalp_0.25pct",
                            "survived_pct": "46.8%",
                            "median_target_move": "0.5715",
                            "median_stop_move": "0.5715",
                            "median_time_to_target_min": "12",
                            "stop_reference": "symmetric adverse",
                            "reward_risk": "1.0",
                        }
                    ],
                },
            }
        ),
        encoding="utf-8",
    )

    result = build_policy_card(query_json, operator_policy_config=policy_config)

    assert result.policy == "wait"
    card = json.loads(result.json_path.read_text(encoding="utf-8"))
    assert card["operator_policy"]["policy_id"] == "strict_demo_policy"
    assert "below 60%" in card["policy_reason"]


def test_state_management_features_are_causal_at_query_bar() -> None:
    rows = []
    for day_offset in range(18):
        day = date(2025, 1, 2) + timedelta(days=day_offset)
        start = datetime(day.year, day.month, day.day, 14, 30, tzinfo=timezone.utc)
        base = 100.0 + day_offset * 0.25
        for minute in range(70):
            close = base + minute * 0.015 + (0.3 if minute > 40 else 0.0)
            rows.append(
                {
                    "timestamp": start + timedelta(minutes=minute),
                    "ticker": "IWM",
                    "open": close - 0.02,
                    "high": close + 0.05,
                    "low": close - 0.05,
                    "close": close,
                    "volume": 1000.0 + minute * 3 + day_offset,
                }
            )
    df = pl.DataFrame(rows)
    query_ts = datetime(2025, 1, 19, 15, 0, tzinfo=timezone.utc)
    engine = PhysicsEngine()

    full = engine.enrich_for_features(df, set(STATE_MANAGEMENT_FEATURES))
    truncated = engine.enrich_for_features(
        df.filter(pl.col("timestamp") <= query_ts),
        set(STATE_MANAGEMENT_FEATURES),
    )

    full_row = full.filter(pl.col("timestamp") == query_ts).row(0, named=True)
    truncated_row = truncated.filter(pl.col("timestamp") == query_ts).row(0, named=True)
    for column in [
        "opening_vwap_rth",
        "daily_rth_atr_14",
        "atr_distance_from_prior_rth_close",
        "velocity_5",
        "velocity_15",
    ]:
        assert truncated_row[column] == pytest.approx(full_row[column])
    assert truncated_row["market_pulse_stage"] == full_row["market_pulse_stage"]


def test_playbook_surface_query_marks_out_of_window_scope() -> None:
    timestamp = _parse_timestamp("2026-05-12 12:30 America/New_York")

    scope = _entry_window_scope(
        [
            {"entry_window_end": "09:45"},
            {"entry_window_end": "11:00"},
        ],
        timestamp,
    )

    assert scope["entry_window_start_et"] == "09:30"
    assert scope["entry_window_end_et"] == "11:00"
    assert scope["query_time_et"] == "12:30"
    assert scope["in_entry_window"] == "no"


def test_state_percentiles_use_direction_aware_reference_rows() -> None:
    query_row = {
        "bias_vwap_distance_pct": 0.004,
        "bias_prior_close_atr": 1.2,
        "bias_velocity_5_atr": 0.06,
        "bias_velocity_15_atr": -0.01,
    }
    reference_rows = [
        {
            "bias_vwap_distance_pct": value / 1000,
            "bias_prior_close_atr": value / 10,
            "bias_velocity_5_atr": value / 100,
            "bias_velocity_15_atr": value / 100,
        }
        for value in [1, 2, 3, 4, 5]
    ]

    result = _state_percentiles(query_row, reference_rows)

    metrics = {row["feature"]: row for row in result["metrics"]}
    assert metrics["bias_vwap_distance_pct"]["percentile"] == "70th"
    assert metrics["bias_vwap_distance_pct"]["value"] == "0.40%"
    assert metrics["bias_prior_close_atr"]["percentile"] == "100th"
    assert metrics["bias_velocity_15_atr"]["percentile"] == "0th"


def test_analog_quality_labels_tight_and_loose_cohorts() -> None:
    tight = _analog_quality(
        0.31,
        {"selected_last_similarity": "0.42", "rank_200_similarity": "0.53"},
        75,
    )
    loose = _analog_quality(
        0.7,
        {"selected_last_similarity": "0.9", "rank_200_similarity": "1.5"},
        75,
    )

    assert tight["label"] == "tight"
    assert tight["rank_200_tail_spread"] == "0.11"
    assert loose["label"] == "loose"


def test_state_management_verdict_marks_out_of_window_as_context_only() -> None:
    verdict, reason = _state_management_verdict(
        "strong_reversion_lean",
        "Nearest analogs strongly favored reversion.",
        entry_window={
            "entry_window_start_et": "09:30",
            "entry_window_end_et": "11:00",
            "in_entry_window": "no",
        },
    )

    assert verdict == "out_of_window"
    assert "management context only" in reason
    assert "strong_reversion_lean" in reason


def test_consultation_management_treats_same_minute_target_stop_as_not_survived() -> None:
    trade_date = date(2025, 1, 2)
    rows = [
        {
            "timestamp": datetime(2025, 1, 2, 14, 30, tzinfo=timezone.utc),
            "_playbook_trade_date": trade_date,
            "_row_index": 0,
            "close": 100.0,
            "daily_rth_atr_14": 1.0,
        },
        {
            "timestamp": datetime(2025, 1, 2, 14, 31, tzinfo=timezone.utc),
            "_playbook_trade_date": trade_date,
            "_row_index": 1,
            "high": 100.30,
            "low": 99.70,
            "close": 100.0,
        },
    ]

    result = _evaluate_management_spec(
        rows,
        0,
        "long",
        "pct",
        0.0025,
        load_operator_policy(),
    )

    assert result is not None
    assert result["captured"] is True
    assert result["survived"] is False


def json_dumps(payload: dict[str, object]) -> str:
    import json

    return json.dumps(payload, indent=2, sort_keys=True) + "\n"
