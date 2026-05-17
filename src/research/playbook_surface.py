"""Build conditional surfaces for Mala 2.2 playbooks."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import polars as pl
from loguru import logger

from src.chronos.storage import LocalStorage
from src.config import DATA_DIR
from src.newton.engine import PhysicsEngine
from src.oracle.metrics import MetricsCalculator
from src.oracle.playbook_simulator import simulate_intraday_reversion_event
from src.strategy.base import required_feature_union
from src.strategy.factory import build_strategy
from src.strategy.intraday_mean_reversion import (
    EXIT_FAMILIES,
    GAP_STATE_FILTERS,
    PLAYBOOK_ID,
    STAGE_FILTERS,
    STOP_FAMILIES,
    STRATEGY_NAME,
    VELOCITY_FILTERS,
)
from src.time_utils import et_date_expr, et_time_expr

PLAYBOOK_STRETCH_SOURCES = ("opening_vwap_rth", "prior_rth_close_atr", "vpoc_4h")
ET = ZoneInfo("America/New_York")


MIN_SAMPLE_COUNT = 50
MIN_CALIBRATION_COUNT = 30
MIN_HOLDOUT_COUNT = 10
MIN_EXPECTANCY_R = 0.10
MIN_WIN_RATE = 0.55
MAX_EXPECTANCY_DRIFT_R = 0.05
MAX_WIN_RATE_DRIFT = 0.05

SURFACE_COLUMNS = [
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
]
FEATURE_BIN_COLUMNS = [
    "symbol",
    "direction",
    "feature",
    "bin_label",
    "bin_min",
    "bin_max",
    "sample_count",
    "expectancy_r",
    "win_rate",
    "holdout_expectancy_r",
    "holdout_win_rate",
]
SAMPLE_EVENT_COLUMNS = [
    "config_id",
    "symbol",
    "direction",
    "event_timestamp",
    "event_timestamp_et",
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
]


@dataclass(frozen=True, slots=True)
class PlaybookSurfaceResult:
    out_dir: Path
    config_count: int
    surface_rows: int
    sample_events: int


def run_playbook_surface(
    playbook: str,
    *,
    symbols: list[str],
    start: date,
    end: date,
    out_dir: Path,
    data_dir: Path | None = None,
    max_events_per_bin: int = 5,
    max_configs: int = 64,
) -> PlaybookSurfaceResult:
    if playbook != PLAYBOOK_ID:
        raise ValueError(f"Unsupported playbook {playbook!r}; expected {PLAYBOOK_ID!r}")
    if start > end:
        raise ValueError("start must be on or before end")

    out_dir.mkdir(parents=True, exist_ok=True)
    storage = LocalStorage(base_dir=data_dir or DATA_DIR)
    configs = _playbook_configs(max_configs=max_configs)
    config_by_id = {_config_id(config): config for config in configs}
    declared_surface = _declared_surface()
    strategy_records = []
    for config in configs:
        strategy = build_strategy(STRATEGY_NAME, config)
        features = frozenset(required_feature_union([strategy]))
        strategy_records.append((_config_id(config), config, strategy, features))

    surface_rows: list[dict[str, Any]] = []
    all_events: list[dict[str, Any]] = []
    tested_feature_families = {
        "stretch": sorted({str(config.get("stretch_source")) for config in configs}),
        "stage": sorted({str(config.get("stage_filter")) for config in configs}),
        "gap_state": sorted({str(config.get("gap_state_filter")) for config in configs}),
        "trigger": ["reversal_range_breakout", "confirming_bars", "jerk", "relative_volume"],
        "stop": sorted({str(config.get("stop_family")) for config in configs}),
        "exit": sorted({str(config.get("exit_family")) for config in configs}),
    }

    for raw_symbol in symbols:
        symbol = raw_symbol.strip().upper()
        if not symbol:
            continue
        bars = storage.load_bars(symbol, start=start, end=end)
        if bars.is_empty():
            logger.warning("No bars loaded for {}", symbol)
            surface_rows.extend(_no_data_rows(symbol, configs))
            continue

        enriched_by_feature_set: dict[frozenset[str], pl.DataFrame] = {}
        with_metrics_by_entry_key: dict[str, pl.DataFrame] = {}
        for config_id, config, strategy, features in strategy_records:
            try:
                if features not in enriched_by_feature_set:
                    enriched_by_feature_set[features] = PhysicsEngine().enrich_for_features(
                        bars,
                        set(features),
                    )
                enriched = enriched_by_feature_set[features]
                entry_key = _entry_signal_cache_key(config)
                if entry_key not in with_metrics_by_entry_key:
                    signals = strategy.generate_signals(enriched)
                    metrics = MetricsCalculator()
                    with_metrics_by_entry_key[entry_key] = metrics.add_directional_forward_metrics(
                        signals,
                        snapshot_windows=(30, 60),
                    )
                with_metrics = with_metrics_by_entry_key[entry_key]
                events = _evaluate_events(symbol, config_id, config, with_metrics)
            except Exception as exc:  # pragma: no cover - defensive receipt path
                logger.exception("Config {} failed for {}", config_id, symbol)
                surface_rows.extend(_error_rows(symbol, config_id, config, exc))
                continue

            all_events.extend(events)
            surface_rows.extend(_surface_rows_for_config(symbol, config_id, config, events))

    sample_events = _sample_events(all_events, max_events_per_bin=max_events_per_bin)
    feature_rows = _feature_bin_rows(all_events)

    _write_csv(out_dir / "conditional_surface_by_symbol.csv", surface_rows, SURFACE_COLUMNS)
    _write_csv(out_dir / "feature_bins_by_symbol.csv", feature_rows, FEATURE_BIN_COLUMNS)
    _write_csv(out_dir / "sample_events.csv", sample_events, SAMPLE_EVENT_COLUMNS)
    _write_config(
        out_dir / "config.json",
        {
            "playbook_id": playbook,
            "strategy": STRATEGY_NAME,
            "symbols": symbols,
            "start": start.isoformat(),
            "end": end.isoformat(),
            "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
            "max_configs": max_configs,
            "max_events_per_bin": max_events_per_bin,
            "config_count": len(configs),
            "configs": config_by_id,
            "config_generation": (
                "balanced_axis_sweep_v1: includes the prior config, one-axis feature "
                "coverage, stretch-threshold families by stretch source, and stop/exit "
                "coverage before any historical result ranking"
            ),
            "execution_optimization": (
                "Newton-enriched bars are cached by feature set; signal frames and forward "
                "metrics are cached by entry-condition config before stop/exit evaluation"
            ),
            "declared_surface": declared_surface,
            "feature_families_tested": tested_feature_families,
            "calibration_holdout_split": "per symbol/config/direction by first 80% of event dates",
            "match_grade_thresholds": {
                "minimum_sample_count": MIN_SAMPLE_COUNT,
                "minimum_calibration_count": MIN_CALIBRATION_COUNT,
                "minimum_holdout_count": MIN_HOLDOUT_COUNT,
                "minimum_expectancy_r": MIN_EXPECTANCY_R,
                "minimum_holdout_win_rate": MIN_WIN_RATE,
                "maximum_expectancy_drift_r": MAX_EXPECTANCY_DRIFT_R,
                "maximum_win_rate_drift": MAX_WIN_RATE_DRIFT,
            },
        },
    )
    _write_receipt(
        out_dir / "RECEIPT.md",
        playbook=playbook,
        symbols=symbols,
        start=start,
        end=end,
        config_count=len(configs),
        surface_rows=surface_rows,
        events=all_events,
        feature_families=tested_feature_families,
        declared_surface=declared_surface,
    )
    return PlaybookSurfaceResult(
        out_dir=out_dir,
        config_count=len(configs),
        surface_rows=len(surface_rows),
        sample_events=len(sample_events),
    )


def _playbook_configs(*, max_configs: int) -> list[dict[str, Any]]:
    base = build_strategy(STRATEGY_NAME)
    prior = base.search_spec.prior_config() if base.search_spec is not None else base.search_config()

    stretch_thresholds = {
        "opening_vwap_rth": [1.5, 2.0, 2.5, 3.0, 3.5],
        "vpoc_4h": [1.5, 2.0, 2.5, 3.0, 3.5],
        "prior_rth_close_atr": [0.75, 1.0, 1.25, 1.5, 2.0, 2.5, 3.0],
    }
    candidates: list[dict[str, Any]] = [dict(prior)]

    def add(**updates: Any) -> None:
        config = dict(prior)
        config.update(updates)
        if config.get("stretch_source") in {"opening_vwap_rth", "vpoc_4h"} and config.get(
            "stretch_threshold"
        ) == 0.75:
            config["stretch_threshold"] = 1.5
        candidates.append(config)

    for source in PLAYBOOK_STRETCH_SOURCES:
        for threshold in stretch_thresholds[source]:
            add(stretch_source=source, stretch_threshold=threshold)
    for stage_filter in STAGE_FILTERS:
        add(stage_filter=stage_filter, stretch_threshold=2.0)
    for gap_state_filter in GAP_STATE_FILTERS:
        add(gap_state_filter=gap_state_filter, stretch_threshold=2.0)
    for entry_window_end in ["09:45", "10:00", "10:15", "11:00"]:
        add(entry_window_end=entry_window_end, stretch_threshold=2.0)
    for reversal_range_minutes in [5, 15]:
        for confirming_bars in [1, 2]:
            add(
                reversal_range_minutes=reversal_range_minutes,
                confirming_bars=confirming_bars,
                stretch_threshold=2.0,
            )
    for velocity_periods_back in [1, 5, 15]:
        for velocity_filter in VELOCITY_FILTERS:
            add(
                velocity_periods_back=velocity_periods_back,
                velocity_filter=velocity_filter,
                stretch_threshold=2.0,
            )
    for relative_volume_threshold in [None, 1.0, 1.25, 1.5]:
        add(relative_volume_threshold=relative_volume_threshold, stretch_threshold=2.0)
    for use_jerk_confirmation in [True, False]:
        add(use_jerk_confirmation=use_jerk_confirmation, stretch_threshold=2.0)
    for stop_family in STOP_FAMILIES:
        for exit_family in EXIT_FAMILIES:
            add(stop_family=stop_family, exit_family=exit_family, stretch_threshold=2.0)

    ordered: list[dict[str, Any]] = []
    seen: set[str] = set()
    for config in candidates:
        key = json.dumps(config, sort_keys=True, default=str)
        if key in seen:
            continue
        seen.add(key)
        ordered.append(config)
    return _sample_balanced_configs(ordered, max_configs=max(1, max_configs))


def _sample_balanced_configs(configs: list[dict[str, Any]], *, max_configs: int) -> list[dict[str, Any]]:
    if len(configs) <= max_configs:
        return configs
    if max_configs <= 1:
        return configs[:1]
    required: list[dict[str, Any]] = [configs[0]]
    seen_keys = {json.dumps(configs[0], sort_keys=True, default=str)}
    coverage_targets = [
        ("stretch_source", list(PLAYBOOK_STRETCH_SOURCES)),
        ("stage_filter", list(STAGE_FILTERS)),
        ("gap_state_filter", list(GAP_STATE_FILTERS)),
        ("reversal_range_minutes", [5, 15]),
        ("confirming_bars", [1, 2]),
        ("velocity_periods_back", [1, 5, 15]),
        ("velocity_filter", list(VELOCITY_FILTERS)),
        ("relative_volume_threshold", [None, 1.0, 1.25, 1.5]),
        ("stop_family", list(STOP_FAMILIES)),
        ("exit_family", list(EXIT_FAMILIES)),
    ]
    for key, values in coverage_targets:
        for value in values:
            match = next((config for config in configs if config.get(key) == value), None)
            if match is None:
                continue
            match_key = json.dumps(match, sort_keys=True, default=str)
            if match_key in seen_keys:
                continue
            required.append(match)
            seen_keys.add(match_key)
            if len(required) >= max_configs:
                return required

    remaining = [
        config
        for config in configs
        if json.dumps(config, sort_keys=True, default=str) not in seen_keys
    ]
    slots = max_configs - len(required)
    if slots <= 0:
        return required[:max_configs]
    if len(remaining) <= slots:
        return required + remaining
    if slots == 1:
        return required + [remaining[0]]
    indexes = sorted({
        round(index * (len(remaining) - 1) / (slots - 1))
        for index in range(slots)
    })
    return required + [remaining[index] for index in indexes[:slots]]


def _declared_surface() -> dict[str, Any]:
    return {
        "entry_window_end": ["09:45", "10:00", "10:15", "11:00"],
        "stretch_source": list(PLAYBOOK_STRETCH_SOURCES),
        "z_score_thresholds": [1.5, 2.0, 2.5, 3.0, 3.5],
        "prior_rth_close_atr_thresholds": [0.75, 1.0, 1.25, 1.5, 2.0, 2.5, 3.0],
        "reversal_range_minutes": [5, 15],
        "confirming_bars": [1, 2],
        "velocity_periods_back": [1, 5, 15],
        "velocity_filter": list(VELOCITY_FILTERS),
        "stage_filter": list(STAGE_FILTERS),
        "gap_state_filter": list(GAP_STATE_FILTERS),
        "use_jerk_confirmation": [True, False],
        "relative_volume_threshold": [None, 1.0, 1.25, 1.5],
        "stop_family": list(STOP_FAMILIES),
        "exit_family": list(EXIT_FAMILIES),
        "multiple_comparisons_note": (
            "This balanced run is an exploratory surface. Treat favorable labels as review "
            "candidates, not feature inclusion proof, until the frozen feature audit applies "
            "multiple-comparisons accounting."
        ),
    }


def _entry_signal_cache_key(config: dict[str, Any]) -> str:
    entry_config = {
        key: value
        for key, value in config.items()
        if key not in {"stop_family", "exit_family"}
    }
    return json.dumps(entry_config, sort_keys=True, default=str)


def _evaluate_events(
    symbol: str,
    config_id: str,
    config: dict[str, Any],
    df: pl.DataFrame,
) -> list[dict[str, Any]]:
    if "signal" not in df.columns:
        return []
    prepared = df.with_row_index("_row_nr").with_columns(
        [
            et_date_expr("timestamp").alias("_playbook_trade_date"),
            et_time_expr("timestamp").alias("_playbook_bar_time"),
        ]
    )
    rows = prepared.to_dicts()
    events: list[dict[str, Any]] = []
    for row in rows:
        if not row.get("signal") or not row.get("signal_direction"):
            continue
        event = _evaluate_one_event(symbol, config_id, config, rows, int(row["_row_nr"]))
        if event is not None:
            events.append(event)
    return events


def _evaluate_one_event(
    symbol: str,
    config_id: str,
    config: dict[str, Any],
    rows: list[dict[str, Any]],
    entry_idx: int,
) -> dict[str, Any] | None:
    row = rows[entry_idx]
    direction = str(row["signal_direction"])
    exit_family = str(config.get("exit_family", "fixed_1r"))
    trade_path = simulate_intraday_reversion_event(
        row=row,
        future_rows=rows[entry_idx + 1 :],
        direction=direction,
        stop_family=str(config.get("stop_family", "reversal_extreme")),
        exit_family=exit_family,
        market_pulse_stage_column=_market_pulse_stage_column(
            str(config.get("stage_timeframe", "1m"))
        ),
    )
    if trade_path is None:
        return None
    trade_date = row.get("_playbook_trade_date")
    event_ts = row.get("timestamp")
    event_timestamp = event_ts.isoformat() if hasattr(event_ts, "isoformat") else str(event_ts)
    event_timestamp_et = _event_timestamp_et(event_ts)
    threshold = config.get("stretch_threshold", "")
    prior_extreme = (
        row.get("playbook_prior_min_stretch")
        if direction == "long"
        else row.get("playbook_prior_max_stretch")
    )
    raw_at_trigger = _float(row.get("playbook_stretch_raw"))
    reference_state = (
        "below_reference"
        if raw_at_trigger is not None and raw_at_trigger < 0
        else "above_reference"
        if raw_at_trigger is not None and raw_at_trigger > 0
        else "at_reference"
    )
    extension_summary = (
        f"{config.get('stretch_source')}: prior_extreme={_round(prior_extreme)}; "
        f"trigger={_round(row.get('playbook_stretch_value'))}; "
        f"raw_at_trigger={_round(raw_at_trigger, digits=6)}; "
        f"reference_state={reference_state}; "
        f"threshold={threshold}"
    )
    stage_summary = (
        f"filter={config.get('stage_filter', 'no_filter')}; "
        f"actual={row.get(_market_pulse_stage_column(str(config.get('stage_timeframe', '1m'))), '')}"
    )
    trigger_summary = (
        f"{config.get('reversal_range_minutes')}m reversal breakout; "
        f"confirming_bars={config.get('confirming_bars')}; "
        f"exit_reason={trade_path.exit_reason}"
    )
    return {
        "config_id": config_id,
        "symbol": symbol,
        "direction": direction,
        "event_timestamp": event_timestamp,
        "event_timestamp_et": event_timestamp_et,
        "trade_date": trade_date,
        "entry_reference_price": _round(trade_path.entry),
        "extension_summary": extension_summary,
        "stage_summary": stage_summary,
        "gap_state": row.get("gap_state_rth_open", row.get("gap_state", "")),
        "trigger_summary": trigger_summary,
        "volume_confirmation_summary": row.get("playbook_volume_confirmation_filter", ""),
        "stop_reference_price": _round(trade_path.stop),
        "exit_reference_price": _round(
            trade_path.target if trade_path.target is not None else trade_path.exit_price
        ),
        "exit_family": exit_family,
        "outcome_label": "win"
        if trade_path.pnl_r > 0
        else "loss"
        if trade_path.pnl_r < 0
        else "flat",
        "pnl_r": _round(trade_path.pnl_r),
        "max_favorable_excursion_r": _round(trade_path.max_favorable_excursion_r),
        "max_adverse_excursion_r": _round(trade_path.max_adverse_excursion_r),
    }


def _surface_rows_for_config(
    symbol: str,
    config_id: str,
    config: dict[str, Any],
    events: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for direction in ("long", "short"):
        subset = [event for event in events if event.get("direction") == direction]
        calibration, holdout = _calibration_holdout_split(subset)
        calibration_exp = _mean_r(calibration)
        holdout_exp = _mean_r(holdout)
        calibration_win = _win_rate(calibration)
        holdout_win = _win_rate(holdout)
        rows.append(
            {
                "config_id": config_id,
                "symbol": symbol,
                "direction": direction,
                "entry_cutoff_et": config.get("entry_window_end", "10:15"),
                "stage_filter": config.get("stage_filter", "no_filter"),
                "gap_state_filter": config.get("gap_state_filter", "no_filter"),
                "extension_family": config.get("stretch_source", ""),
                "extension_bin": config.get("stretch_threshold", ""),
                "reversal_range_minutes": config.get("reversal_range_minutes", ""),
                "volume_confirmation_filter": _config_volume_label(config),
                "stop_family": config.get("stop_family", ""),
                "exit_family": config.get("exit_family", ""),
                "sample_count": len(subset),
                "calibration_count": len(calibration),
                "holdout_count": len(holdout),
                "calibration_expectancy_r": _round(calibration_exp),
                "holdout_expectancy_r": _round(holdout_exp),
                "calibration_win_rate": _round(calibration_win),
                "holdout_win_rate": _round(holdout_win),
                "match_grade": _match_grade(
                    len(subset),
                    len(calibration),
                    len(holdout),
                    calibration_exp,
                    holdout_exp,
                    holdout_win,
                    calibration_win,
                ),
                "criteria_failed_count": len(
                    _criteria_failures(
                        len(subset),
                        len(calibration),
                        len(holdout),
                        calibration_exp,
                        holdout_exp,
                        holdout_win,
                        calibration_win,
                    )
                ),
                "criteria_failed": " | ".join(
                    _criteria_failures(
                        len(subset),
                        len(calibration),
                        len(holdout),
                        calibration_exp,
                        holdout_exp,
                        holdout_win,
                        calibration_win,
                    )
                ),
                "evidence_note": _evidence_note(
                    len(subset),
                    len(calibration),
                    len(holdout),
                    calibration_exp,
                    holdout_exp,
                    holdout_win,
                    calibration_win,
                ),
            }
        )
    return rows


def _calibration_holdout_split(events: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if not events:
        return [], []
    dates = sorted({event.get("trade_date") for event in events if event.get("trade_date") is not None})
    if len(dates) < 2:
        return events, []
    holdout_start = dates[max(1, int(len(dates) * 0.8))]
    calibration = [event for event in events if event.get("trade_date") < holdout_start]
    holdout = [event for event in events if event.get("trade_date") >= holdout_start]
    return calibration, holdout


def _feature_bin_rows(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for feature in ("gap_state", "volume_confirmation_summary", "exit_family"):
        groups: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
        for event in events:
            key = (
                str(event.get("symbol", "")),
                str(event.get("direction", "")),
                str(event.get(feature, "")),
            )
            groups.setdefault(key, []).append(event)
        for (symbol, direction, label), grouped in sorted(groups.items()):
            _, holdout = _calibration_holdout_split(grouped)
            rows.append(
                {
                    "symbol": symbol,
                    "direction": direction,
                    "feature": feature,
                    "bin_label": label,
                    "bin_min": "",
                    "bin_max": "",
                    "sample_count": len(grouped),
                    "expectancy_r": _round(_mean_r(grouped)),
                    "win_rate": _round(_win_rate(grouped)),
                    "holdout_expectancy_r": _round(_mean_r(holdout)),
                    "holdout_win_rate": _round(_win_rate(holdout)),
                }
            )
    return rows


def _sample_events(events: list[dict[str, Any]], *, max_events_per_bin: int) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for event in events:
        key = (
            str(event.get("symbol", "")),
            str(event.get("direction", "")),
            str(event.get("config_id", "")),
        )
        grouped.setdefault(key, []).append(event)
    samples: list[dict[str, Any]] = []
    for key in sorted(grouped):
        group = sorted(grouped[key], key=lambda item: str(item.get("event_timestamp", "")))
        samples.extend(group[:max(0, max_events_per_bin)])
    return [{column: event.get(column, "") for column in SAMPLE_EVENT_COLUMNS} for event in samples]


def _no_data_rows(symbol: str, configs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for config in configs[:1]:
        config_id = _config_id(config)
        rows.append(
            {
                "config_id": config_id,
                "symbol": symbol,
                "direction": "combined",
                "entry_cutoff_et": config.get("entry_window_end", ""),
                "stage_filter": config.get("stage_filter", ""),
                "gap_state_filter": config.get("gap_state_filter", ""),
                "extension_family": config.get("stretch_source", ""),
                "extension_bin": config.get("stretch_threshold", ""),
                "reversal_range_minutes": config.get("reversal_range_minutes", ""),
                "volume_confirmation_filter": _config_volume_label(config),
                "stop_family": config.get("stop_family", ""),
                "exit_family": config.get("exit_family", ""),
                "sample_count": 0,
                "calibration_count": 0,
                "holdout_count": 0,
                "calibration_expectancy_r": "",
                "holdout_expectancy_r": "",
                "calibration_win_rate": "",
                "holdout_win_rate": "",
                "match_grade": "insufficient",
                "criteria_failed_count": 1,
                "criteria_failed": "no_data_loaded",
                "evidence_note": "no data loaded for symbol/date range",
            }
        )
    return rows


def _error_rows(
    symbol: str,
    config_id: str,
    config: dict[str, Any],
    exc: Exception,
) -> list[dict[str, Any]]:
    rows = _surface_rows_for_config(symbol, config_id, config, [])
    for row in rows:
        row["evidence_note"] = f"config error: {exc}"
    return rows


def _match_grade(
    sample_count: int,
    calibration_count: int,
    holdout_count: int,
    calibration_expectancy: float | None,
    holdout_expectancy: float | None,
    holdout_win_rate: float | None,
    calibration_win_rate: float | None = None,
) -> str:
    failures = _criteria_failures(
        sample_count,
        calibration_count,
        holdout_count,
        calibration_expectancy,
        holdout_expectancy,
        holdout_win_rate,
        calibration_win_rate,
    )
    if (
        sample_count < MIN_SAMPLE_COUNT
        or calibration_count < MIN_CALIBRATION_COUNT
        or holdout_count < MIN_HOLDOUT_COUNT
        or calibration_expectancy is None
        or holdout_expectancy is None
    ):
        return "insufficient"
    if not failures:
        return "favorable"
    if len(failures) == 1 and (calibration_expectancy > 0 or holdout_expectancy > 0):
        return "near_favorable"
    if calibration_expectancy > 0 or holdout_expectancy > 0:
        return "partial"
    return "outside"


def _criteria_failures(
    sample_count: int,
    calibration_count: int,
    holdout_count: int,
    calibration_expectancy: float | None,
    holdout_expectancy: float | None,
    holdout_win_rate: float | None,
    calibration_win_rate: float | None = None,
) -> list[str]:
    failures: list[str] = []
    if sample_count < MIN_SAMPLE_COUNT:
        failures.append("thin_total_sample")
    if calibration_count < MIN_CALIBRATION_COUNT:
        failures.append("thin_calibration_sample")
    if holdout_count < MIN_HOLDOUT_COUNT:
        failures.append("thin_holdout_sample")
    if calibration_expectancy is None:
        failures.append("missing_calibration_expectancy")
    elif calibration_expectancy < MIN_EXPECTANCY_R:
        failures.append("calibration_expectancy_below_floor")
    if holdout_expectancy is None:
        failures.append("missing_holdout_expectancy")
    elif holdout_expectancy < MIN_EXPECTANCY_R:
        failures.append("holdout_expectancy_below_floor")
    if calibration_expectancy is not None and holdout_expectancy is not None:
        if abs(calibration_expectancy - holdout_expectancy) > MAX_EXPECTANCY_DRIFT_R:
            failures.append("expectancy_drift_exceeds_bound")
    if holdout_win_rate is None:
        failures.append("missing_holdout_win_rate")
    elif holdout_win_rate < MIN_WIN_RATE:
        failures.append("holdout_win_rate_below_floor")
    if calibration_win_rate is None:
        failures.append("missing_calibration_win_rate")
    elif holdout_win_rate is not None and abs(calibration_win_rate - holdout_win_rate) > MAX_WIN_RATE_DRIFT:
        failures.append("win_rate_drift_exceeds_bound")
    return failures


def _evidence_note(
    sample_count: int,
    calibration_count: int,
    holdout_count: int,
    calibration_expectancy: float | None,
    holdout_expectancy: float | None,
    holdout_win_rate: float | None = None,
    calibration_win_rate: float | None = None,
) -> str:
    failures = _criteria_failures(
        sample_count,
        calibration_count,
        holdout_count,
        calibration_expectancy,
        holdout_expectancy,
        holdout_win_rate,
        calibration_win_rate,
    )
    if sample_count == 0:
        return "no events for this parameter region"
    if sample_count < MIN_SAMPLE_COUNT:
        return f"thin total sample; need at least {MIN_SAMPLE_COUNT}"
    if calibration_count < MIN_CALIBRATION_COUNT:
        return f"thin calibration sample; need at least {MIN_CALIBRATION_COUNT}"
    if holdout_count < MIN_HOLDOUT_COUNT:
        return f"thin holdout sample; need at least {MIN_HOLDOUT_COUNT}"
    if calibration_expectancy is None or holdout_expectancy is None:
        return "missing calibration or holdout expectancy"
    if calibration_expectancy < MIN_EXPECTANCY_R or holdout_expectancy < MIN_EXPECTANCY_R:
        return f"effect size below {MIN_EXPECTANCY_R}R expectancy floor"
    if abs(calibration_expectancy - holdout_expectancy) > MAX_EXPECTANCY_DRIFT_R:
        return f"holdout expectancy drift exceeds {MAX_EXPECTANCY_DRIFT_R}R bound"
    if holdout_win_rate is None:
        return "missing holdout win rate"
    if holdout_win_rate < MIN_WIN_RATE:
        return f"holdout win rate below {MIN_WIN_RATE} floor"
    if calibration_win_rate is None:
        return "missing calibration win rate"
    if abs(calibration_win_rate - holdout_win_rate) > MAX_WIN_RATE_DRIFT:
        return f"win-rate drift exceeds {MAX_WIN_RATE_DRIFT} bound"
    if not failures:
        return "all strict criteria passed; requires multiple-comparisons gate before promotion"
    if (
        calibration_expectancy is not None
        and holdout_expectancy is not None
        and calibration_expectancy > 0
        and holdout_expectancy > 0
    ):
        return "positive calibration and holdout expectancy; inspect neighboring regions"
    if holdout_expectancy is not None and holdout_expectancy > 0:
        return "positive holdout expectancy but calibration did not confirm"
    if calibration_expectancy is not None and calibration_expectancy > 0:
        return "positive calibration expectancy but holdout did not confirm"
    return "holdout did not support this region"


def _mean_r(events: list[dict[str, Any]]) -> float | None:
    values = [_float(event.get("pnl_r")) for event in events]
    clean = [value for value in values if value is not None]
    if not clean:
        return None
    return sum(clean) / len(clean)


def _win_rate(events: list[dict[str, Any]]) -> float | None:
    values = [_float(event.get("pnl_r")) for event in events]
    clean = [value for value in values if value is not None]
    if not clean:
        return None
    return sum(1 for value in clean if value > 0) / len(clean)


def _config_id(config: dict[str, Any]) -> str:
    payload = json.dumps(config, sort_keys=True, default=str)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:10]


def _config_volume_label(config: dict[str, Any]) -> str:
    threshold = config.get("relative_volume_threshold")
    if threshold in (None, ""):
        return "no_filter"
    return f"rvol_gt_{float(threshold):g}"


def _float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(result) or math.isinf(result):
        return None
    return result


def _round(value: Any, digits: int = 4) -> str:
    number = _float(value)
    if number is None:
        return ""
    return str(round(number, digits))


def _event_timestamp_et(value: Any) -> str:
    if not isinstance(value, datetime):
        return ""
    timestamp = value
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=UTC)
    return timestamp.astimezone(ET).isoformat(timespec="seconds")


def _market_pulse_stage_column(timeframe: str) -> str:
    return "market_pulse_stage" if timeframe == "1m" else f"market_pulse_stage_{timeframe}"


def _write_csv(path: Path, rows: list[dict[str, Any]], columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in columns})


def _write_config(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def _stage_proxy_summary(surface_rows: list[dict[str, Any]]) -> list[str]:
    stage_counts: dict[str, int] = {}
    near_or_better: dict[str, int] = {}
    for row in surface_rows:
        stage = str(row.get("stage_filter", "") or "blank")
        stage_counts[stage] = stage_counts.get(stage, 0) + 1
        if row.get("match_grade") in {"favorable", "near_favorable"}:
            near_or_better[stage] = near_or_better.get(stage, 0) + 1

    lines = [
        "- implemented_stage_feature: `market_pulse_stage` from the 1m MarketPulse VWMA 8/21/34 plus VMA location",
        "- stage_values: `bullish`, `accumulation`, `distribution`, `bearish`",
    ]
    if stage_counts:
        counts = ", ".join(f"{stage}={count}" for stage, count in sorted(stage_counts.items()))
        lines.append(f"- rows_by_stage_filter: `{counts}`")
    if near_or_better:
        counts = ", ".join(f"{stage}={count}" for stage, count in sorted(near_or_better.items()))
        lines.append(f"- near_or_better_by_stage_filter: `{counts}`")
    else:
        lines.append("- near_or_better_by_stage_filter: `none`")
    lines.append(
        "- interpretation: this is now the MarketPulse stage axis, not the old impulse-regime proxy."
    )
    return lines


def _write_receipt(
    path: Path,
    *,
    playbook: str,
    symbols: list[str],
    start: date,
    end: date,
    config_count: int,
    surface_rows: list[dict[str, Any]],
    events: list[dict[str, Any]],
    feature_families: dict[str, Any],
    declared_surface: dict[str, Any],
) -> None:
    grades: dict[str, int] = {}
    for row in surface_rows:
        grades[str(row.get("match_grade", ""))] = grades.get(str(row.get("match_grade", "")), 0) + 1
    lines = [
        "# Intraday Mean Reversion Surface Receipt",
        "",
        f"- playbook: `{playbook}`",
        f"- strategy: `{STRATEGY_NAME}`",
        f"- symbols: `{','.join(symbols)}`",
        f"- date_range: `{start.isoformat()} -> {end.isoformat()}`",
        f"- configs_tested: `{config_count}`",
        f"- events: `{len(events)}`",
        "",
        "## Feature Families Tested",
        "",
    ]
    for key, value in feature_families.items():
        lines.append(f"- {key}: `{value}`")
    lines.extend(
        [
            "",
            "## Declared Surface",
            "",
            "- config_generation: `balanced_axis_sweep_v1`",
            f"- declared_stretch_sources: `{declared_surface['stretch_source']}`",
            f"- declared_stage_filters: `{declared_surface['stage_filter']}`",
            f"- declared_gap_filters: `{declared_surface['gap_state_filter']}`",
            f"- declared_z_score_thresholds: `{declared_surface['z_score_thresholds']}`",
            f"- declared_prior_rth_close_atr_thresholds: `{declared_surface['prior_rth_close_atr_thresholds']}`",
            "- multiple_comparisons: `not yet a feature-inclusion proof; review candidates only`",
            "- promotion_rule: `no candidate can promote from this receipt alone; use surface_review/SURFACE_REVIEW.md plus a locked-packet Bonferroni/FDR gate`",
            "",
            "## Match Grade Thresholds",
            "",
            f"- minimum_sample_count: `{MIN_SAMPLE_COUNT}`",
            f"- minimum_calibration_count: `{MIN_CALIBRATION_COUNT}`",
            f"- minimum_holdout_count: `{MIN_HOLDOUT_COUNT}`",
            f"- minimum_expectancy_r: `{MIN_EXPECTANCY_R}`",
            f"- minimum_holdout_win_rate: `{MIN_WIN_RATE}`",
            f"- maximum_expectancy_drift_r: `{MAX_EXPECTANCY_DRIFT_R}`",
            f"- maximum_win_rate_drift: `{MAX_WIN_RATE_DRIFT}`",
        ]
    )
    lines.extend(["", "## Match Grades", ""])
    for grade, count in sorted(grades.items()):
        lines.append(f"- {grade}: `{count}`")
    lines.extend(
        [
            "",
            "Grade meanings:",
            "",
            "- `favorable`: strict sample, calibration, holdout, effect, hit-rate, and drift criteria all passed; still requires multiple-comparisons review before promotion.",
            "- `near_favorable`: exactly one strict criterion failed while calibration or holdout expectancy remained positive; chart-review lead, not proof.",
            "- `partial`: positive signal exists but more than one strict criterion failed.",
            "- `outside`: no positive calibration or holdout expectancy after minimum evidence checks.",
            "- `insufficient`: sample or required metric is too thin/missing.",
            "",
            "## Stage Read",
            "",
        ]
    )
    lines.extend(_stage_proxy_summary(surface_rows))
    lines.extend(
        [
            "",
            "## Review Pack",
            "",
            "- candidate_review: `surface_review/SURFACE_REVIEW.md`",
            "- candidate_regions: `surface_review/candidate_regions.csv`",
            "- chart_review_events: `surface_review/chart_review_events.csv`",
            "- This receipt intentionally does not list top regions by holdout expectancy. A holdout-only leaderboard over-promotes tail-payoff and unstable pockets.",
        ]
    )
    lines.extend(
        [
            "",
            "## Notes",
            "",
            "- This is a conditional-surface artifact, not a Mala_Evidence_v1, active_strategy, or live authorization write.",
            "- Sample-event MFE/MAE is measured through the evaluated stop/target/time/EOD exit path.",
            "- Thin samples, sub-0.1R effects, and calibration/holdout drift are not treated as favorable edge.",
            "- Multiple-comparisons correction is a required pre-promotion gate once a candidate packet is locked.",
            "- Stop and thesis invalidation are still represented by one stop axis in this first slice; split `risk_stop` from `thesis_invalidation` before the playbook grows beyond chart-review leads.",
            "- Current-day matching, plotting, options, and Bhiksha loading are intentionally deferred.",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def _parse_symbols(raw: str) -> list[str]:
    return [item.strip().upper() for item in raw.split(",") if item.strip()]


def _parse_date(raw: str) -> date:
    return date.fromisoformat(raw)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("playbook", choices=[PLAYBOOK_ID])
    parser.add_argument("--symbols", required=True, help="Comma-separated symbols, e.g. IWM,QQQ")
    parser.add_argument("--start", required=True, type=_parse_date)
    parser.add_argument("--end", required=True, type=_parse_date)
    parser.add_argument("--out-dir", type=Path, default=None)
    parser.add_argument("--data-dir", type=Path, default=None)
    parser.add_argument("--max-events-per-bin", type=int, default=5)
    parser.add_argument("--max-configs", type=int, default=64)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    out_dir = args.out_dir or (
        Path("data/results/playbooks/mean_reversion_at_extremes")
        / datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    )
    result = run_playbook_surface(
        args.playbook,
        symbols=_parse_symbols(args.symbols),
        start=args.start,
        end=args.end,
        out_dir=out_dir,
        data_dir=args.data_dir,
        max_events_per_bin=args.max_events_per_bin,
        max_configs=args.max_configs,
    )
    print(f"PLAYBOOK_SURFACE_DIR={result.out_dir}")
    print(f"CONFIGS={result.config_count}")
    print(f"SURFACE_ROWS={result.surface_rows}")
    print(f"SAMPLE_EVENTS={result.sample_events}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
