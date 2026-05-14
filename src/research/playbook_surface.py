"""Build conditional surfaces for Mala 2.2 playbooks."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from pathlib import Path
from typing import Any

import polars as pl
from loguru import logger

from src.chronos.storage import LocalStorage
from src.config import DATA_DIR
from src.newton.engine import PhysicsEngine
from src.oracle.metrics import MetricsCalculator
from src.research.search_space import build_search_configs
from src.strategy.base import required_feature_union
from src.strategy.factory import build_strategy
from src.strategy.intraday_mean_reversion import PLAYBOOK_ID, STRATEGY_NAME
from src.time_utils import et_date_expr, et_time_expr


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
    max_configs: int = 32,
) -> PlaybookSurfaceResult:
    if playbook != PLAYBOOK_ID:
        raise ValueError(f"Unsupported playbook {playbook!r}; expected {PLAYBOOK_ID!r}")
    if start > end:
        raise ValueError("start must be on or before end")

    out_dir.mkdir(parents=True, exist_ok=True)
    storage = LocalStorage(base_dir=data_dir or DATA_DIR)
    configs = _playbook_configs(max_configs=max_configs)
    config_by_id = {_config_id(config): config for config in configs}

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

        for config in configs:
            config_id = _config_id(config)
            strategy = build_strategy(STRATEGY_NAME, config)
            try:
                features = required_feature_union([strategy])
                enriched = PhysicsEngine().enrich_for_features(bars, features)
                signals = strategy.generate_signals(enriched)
                metrics = MetricsCalculator()
                with_metrics = metrics.add_directional_forward_metrics(
                    signals,
                    snapshot_windows=(30, 60),
                )
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
            "feature_families_tested": tested_feature_families,
            "calibration_holdout_split": "per symbol/config/direction by first 80% of event dates",
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
    configs = build_search_configs(STRATEGY_NAME, mode="full", max_configs=max(1, max_configs))
    ordered: list[dict[str, Any]] = []
    seen: set[str] = set()
    for config in [prior, *configs]:
        key = json.dumps(config, sort_keys=True, default=str)
        if key in seen:
            continue
        seen.add(key)
        ordered.append(config)
        if len(ordered) >= max_configs:
            break
    return ordered


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
    entry = _float(row.get("close"))
    if entry is None:
        return None
    stop = _stop_price(row, direction, str(config.get("stop_family", "reversal_extreme")))
    if stop is None:
        return None
    risk = (entry - stop) if direction == "long" else (stop - entry)
    if risk <= 0:
        return None

    target = _target_price(row, direction, entry, risk, str(config.get("exit_family", "fixed_1r")))
    trade_date = row.get("_playbook_trade_date")
    exit_price = entry
    exit_reason = "eod"
    max_favorable = 0.0
    max_adverse = 0.0
    exit_family = str(config.get("exit_family", "fixed_1r"))
    time_stop = time(11, 30)

    for future in rows[entry_idx + 1 :]:
        if future.get("_playbook_trade_date") != trade_date:
            break
        high = _float(future.get("high"))
        low = _float(future.get("low"))
        close = _float(future.get("close"))
        if high is None or low is None or close is None:
            continue
        if direction == "long":
            if low <= stop:
                max_adverse = max(max_adverse, risk)
                exit_price = stop
                exit_reason = "stop"
                break
            if target is not None and high >= target:
                max_favorable = max(max_favorable, target - entry)
                max_adverse = max(max_adverse, max(0.0, entry - low))
                exit_price = target
                exit_reason = "target"
                break
            max_favorable = max(max_favorable, high - entry)
            max_adverse = max(max_adverse, entry - low)
        else:
            if high >= stop:
                max_adverse = max(max_adverse, risk)
                exit_price = stop
                exit_reason = "stop"
                break
            if target is not None and low <= target:
                max_favorable = max(max_favorable, entry - target)
                max_adverse = max(max_adverse, max(0.0, high - entry))
                exit_price = target
                exit_reason = "target"
                break
            max_favorable = max(max_favorable, entry - low)
            max_adverse = max(max_adverse, high - entry)
        exit_price = close
        bar_time = future.get("_playbook_bar_time")
        if exit_family == "time_stop" and isinstance(bar_time, time) and bar_time >= time_stop:
            exit_reason = "time_stop"
            break

    pnl = (exit_price - entry) if direction == "long" else (entry - exit_price)
    pnl_r = pnl / risk
    # For the sample-event review, excursion must stop when this evaluated
    # trade path stops. EOD forward excursions can include movement after a
    # target/stop and are handled separately by Oracle-level metrics.
    mfe_r = max_favorable / risk
    mae_r = max_adverse / risk
    event_ts = row.get("timestamp")
    event_timestamp = event_ts.isoformat() if hasattr(event_ts, "isoformat") else str(event_ts)
    threshold = config.get("stretch_threshold", "")
    prior_extreme = (
        row.get("playbook_prior_min_stretch")
        if direction == "long"
        else row.get("playbook_prior_max_stretch")
    )
    extension_summary = (
        f"{config.get('stretch_source')}: prior_extreme={_round(prior_extreme)}; "
        f"trigger={_round(row.get('playbook_stretch_value'))}; "
        f"threshold={threshold}"
    )
    stage_summary = (
        f"filter={config.get('stage_filter', 'no_filter')}; "
        f"actual={row.get('impulse_regime_5m', '')}"
    )
    trigger_summary = (
        f"{config.get('reversal_range_minutes')}m reversal breakout; "
        f"confirming_bars={config.get('confirming_bars')}; "
        f"exit_reason={exit_reason}"
    )
    return {
        "config_id": config_id,
        "symbol": symbol,
        "direction": direction,
        "event_timestamp": event_timestamp,
        "trade_date": trade_date,
        "entry_reference_price": _round(entry),
        "extension_summary": extension_summary,
        "stage_summary": stage_summary,
        "gap_state": row.get("gap_state", ""),
        "trigger_summary": trigger_summary,
        "volume_confirmation_summary": row.get("playbook_volume_confirmation_filter", ""),
        "stop_reference_price": _round(stop),
        "exit_reference_price": _round(target if target is not None else exit_price),
        "exit_family": exit_family,
        "outcome_label": "win" if pnl_r > 0 else "loss" if pnl_r < 0 else "flat",
        "pnl_r": _round(pnl_r),
        "max_favorable_excursion_r": _round(mfe_r),
        "max_adverse_excursion_r": _round(mae_r),
    }


def _stop_price(row: dict[str, Any], direction: str, stop_family: str) -> float | None:
    entry = _float(row.get("close"))
    if entry is None:
        return None
    if stop_family == "reversal_midpoint":
        return _float(row.get("playbook_reversal_midpoint"))
    if stop_family == "immediate_entry_bar_failure":
        return _float(row.get("low" if direction == "long" else "high"))
    return _float(row.get("playbook_reversal_low" if direction == "long" else "playbook_reversal_high"))


def _target_price(
    row: dict[str, Any],
    direction: str,
    entry: float,
    risk: float,
    exit_family: str,
) -> float | None:
    multiples = {"fixed_1r": 1.0, "fixed_1_5r": 1.5, "fixed_2r": 2.0}
    if exit_family in multiples:
        distance = risk * multiples[exit_family]
        return entry + distance if direction == "long" else entry - distance
    if exit_family == "vwap_return":
        target = _float(row.get("opening_vwap"))
    elif exit_family == "partial_retrace_50":
        reference = _float(row.get("playbook_reference_price")) or _float(row.get("opening_vwap"))
        if reference is None:
            return None
        target = entry + ((reference - entry) * 0.5)
    else:
        return None
    if target is None:
        return None
    if direction == "long" and target <= entry:
        return None
    if direction == "short" and target >= entry:
        return None
    return target


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
                ),
                "evidence_note": _evidence_note(
                    len(subset),
                    len(calibration),
                    len(holdout),
                    calibration_exp,
                    holdout_exp,
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
) -> str:
    if (
        sample_count < 10
        or calibration_count < 5
        or holdout_count < 3
        or calibration_expectancy is None
        or holdout_expectancy is None
    ):
        return "insufficient"
    if calibration_expectancy > 0 and holdout_expectancy > 0 and (holdout_win_rate or 0.0) >= 0.5:
        return "favorable"
    if calibration_expectancy > 0 or holdout_expectancy > 0:
        return "partial"
    return "outside"


def _evidence_note(
    sample_count: int,
    calibration_count: int,
    holdout_count: int,
    calibration_expectancy: float | None,
    holdout_expectancy: float | None,
) -> str:
    if sample_count == 0:
        return "no events for this parameter region"
    if sample_count < 10:
        return "thin total sample"
    if calibration_count < 5:
        return "thin calibration sample"
    if holdout_count < 3:
        return "thin holdout sample"
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


def _write_csv(path: Path, rows: list[dict[str, Any]], columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in columns})


def _write_config(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


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
) -> None:
    grades: dict[str, int] = {}
    for row in surface_rows:
        grades[str(row.get("match_grade", ""))] = grades.get(str(row.get("match_grade", "")), 0) + 1
    top = sorted(
        [row for row in surface_rows if row.get("match_grade") in {"favorable", "partial"}],
        key=lambda item: float(item.get("holdout_expectancy_r") or -999),
        reverse=True,
    )[:10]
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
    lines.extend(["", "## Match Grades", ""])
    for grade, count in sorted(grades.items()):
        lines.append(f"- {grade}: `{count}`")
    lines.extend(["", "## Top Regions", ""])
    if not top:
        lines.append("- No favorable or partial regions in this run.")
    else:
        for row in top:
            lines.append(
                "- {symbol} {direction} {extension_family}>{extension_bin} "
                "cutoff={entry_cutoff_et} stage={stage_filter} gap={gap_state_filter} "
                "trigger={reversal_range_minutes}m vol={volume_confirmation_filter} "
                "stop={stop_family} exit={exit_family} holdout_exp_r={holdout_expectancy_r} "
                "holdout_win={holdout_win_rate} n={sample_count}".format(**row)
            )
    lines.extend(
        [
            "",
            "## Notes",
            "",
            "- This is a conditional-surface artifact, not a Mala_Evidence_v1, active_strategy, or live authorization write.",
            "- Sample-event MFE/MAE is measured through the evaluated stop/target/time/EOD exit path.",
            "- Thin samples are marked `insufficient` instead of treated as edge.",
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
    parser.add_argument("--max-configs", type=int, default=32)
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
