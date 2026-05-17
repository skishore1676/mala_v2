"""Build TradingView visual-review packets for playbook surface events."""

from __future__ import annotations

import argparse
import csv
import json
import math
import shlex
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import polars as pl

from src.chronos.storage import LocalStorage
from src.config import DATA_DIR
from src.newton.engine import PhysicsEngine
from src.oracle.metrics import MetricsCalculator
from src.oracle.playbook_simulator import (
    _market_pulse_flip_exit,
    _stop_price,
    _target_price,
)
from src.research.playbook_surface import (
    _entry_signal_cache_key,
    _float,
)
from src.strategy.base import required_feature_union
from src.strategy.factory import build_strategy
from src.strategy.intraday_mean_reversion import PLAYBOOK_ID, STRATEGY_NAME
from src.time_utils import et_date_expr, et_time_expr, et_timestamp_expr


ET = ZoneInfo("America/New_York")
CT = ZoneInfo("America/Chicago")
DEFAULT_TRADINGVIEW_MCP_ROOT = "/Users/suman/code/openclaw-core/workspace-main/external/tradingview-mcp"

EVENT_COLUMNS = [
    "event_id",
    "group_id",
    "config_id",
    "symbol",
    "trade_date",
    "direction",
    "entry_timestamp_et",
    "entry_timestamp_ct",
    "entry_unix",
    "entry_price",
    "exit_timestamp_et",
    "exit_timestamp_ct",
    "exit_unix",
    "exit_price",
    "exit_reason",
    "pnl_r",
    "max_favorable_excursion_r",
    "max_adverse_excursion_r",
    "stop_price",
    "target_price",
    "stop_family",
    "exit_family",
    "stretch_source",
    "stretch_threshold",
    "stretch_value",
    "stretch_raw",
    "stage_filter",
    "stage_actual",
    "gap_state_filter",
    "gap_state",
    "reversal_range_minutes",
    "confirming_bars",
]

GROUP_COLUMNS = [
    "group_id",
    "symbol",
    "trade_date",
    "direction",
    "entry_timestamp_et",
    "entry_timestamp_ct",
    "entry_price",
    "review_event_id",
    "review_exit_timestamp_et",
    "review_exit_timestamp_ct",
    "review_exit_price",
    "review_exit_reason",
    "variant_count",
    "pnl_r_min",
    "pnl_r_max",
    "mfe_r_max",
    "mae_r_max",
    "exit_reasons",
    "exit_families",
    "stop_families",
    "stretch_sources",
]


@dataclass(frozen=True, slots=True)
class PlaybookVisualReviewResult:
    out_dir: Path
    event_count: int
    group_count: int
    event_csv: Path
    group_csv: Path
    pine_script: Path
    apply_script: Path
    draw_script: Path
    receipt: Path


def build_playbook_visual_review(
    run_dir: Path,
    *,
    symbol: str = "QQQ",
    days: int = 5,
    start: date | None = None,
    end: date | None = None,
    out_dir: Path | None = None,
    data_dir: Path | None = None,
    tv_symbol: str | None = None,
    timeframe: str = "5",
    preferred_stop_family: str = "reversal_extreme",
    preferred_exit_family: str = "fixed_1r",
    max_groups: int = 40,
    tradingview_mcp_root: str = DEFAULT_TRADINGVIEW_MCP_ROOT,
) -> PlaybookVisualReviewResult:
    """Create a TradingView overlay packet for recent playbook events."""

    config_path = run_dir / "config.json"
    if not config_path.exists():
        raise FileNotFoundError(f"config.json not found under {run_dir}")
    run_config = json.loads(config_path.read_text(encoding="utf-8"))
    if str(run_config.get("playbook_id", "")) != PLAYBOOK_ID:
        raise ValueError(f"Unsupported playbook for visual review: {run_config.get('playbook_id')!r}")
    if days <= 0:
        raise ValueError("days must be positive")
    if max_groups <= 0:
        raise ValueError("max_groups must be positive")

    normalized_symbol = symbol.strip().upper()
    storage = LocalStorage(base_dir=data_dir or DATA_DIR)
    review_dates = _select_dates(storage, normalized_symbol, days=days, start=start, end=end)
    if not review_dates:
        raise FileNotFoundError(f"No cached dates found for {normalized_symbol}")

    review_dir = out_dir or (
        run_dir
        / "tradingview_visual_review"
        / f"{normalized_symbol.lower()}_{review_dates[0]}_{review_dates[-1]}"
    )
    review_dir.mkdir(parents=True, exist_ok=True)

    bars = storage.load_bars(normalized_symbol, start=review_dates[0], end=review_dates[-1])
    configs = {
        str(config_id): dict(config)
        for config_id, config in dict(run_config.get("configs", {})).items()
    }
    events = _collect_events(
        bars,
        symbol=normalized_symbol,
        configs=configs,
        review_dates=set(review_dates),
    )
    groups = _event_groups(
        events,
        preferred_stop_family=preferred_stop_family,
        preferred_exit_family=preferred_exit_family,
    )[:max_groups]
    retained_event_ids = {
        str(event["event_id"])
        for group in groups
        for event in group["events"]
    }
    retained_events = [event for event in events if str(event["event_id"]) in retained_event_ids]

    event_csv = review_dir / "event_review.csv"
    group_csv = review_dir / "event_groups.csv"
    pine_script = review_dir / f"mala_{normalized_symbol.lower()}_playbook_review.pine"
    apply_script = review_dir / "apply_tradingview_overlay.sh"
    draw_script = review_dir / "draw_tradingview_groups.sh"
    receipt = review_dir / "TRADINGVIEW_VISUAL_REVIEW.md"

    _write_csv(event_csv, [_event_csv_row(event) for event in retained_events], EVENT_COLUMNS)
    _write_csv(group_csv, [_group_csv_row(group) for group in groups], GROUP_COLUMNS)
    _write_pine_script(pine_script, symbol=normalized_symbol, review_dates=review_dates, groups=groups)
    _write_apply_script(
        apply_script,
        pine_script=pine_script,
        symbol=tv_symbol or normalized_symbol,
        timeframe=timeframe,
        review_dates=review_dates,
        groups=groups,
        tradingview_mcp_root=tradingview_mcp_root,
    )
    _write_draw_script(
        draw_script,
        symbol=tv_symbol or normalized_symbol,
        timeframe=timeframe,
        groups=groups,
        tradingview_mcp_root=tradingview_mcp_root,
    )
    _write_receipt(
        receipt,
        run_dir=run_dir,
        symbol=normalized_symbol,
        tv_symbol=tv_symbol or normalized_symbol,
        timeframe=timeframe,
        review_dates=review_dates,
        event_csv=event_csv,
        group_csv=group_csv,
        pine_script=pine_script,
        apply_script=apply_script,
        draw_script=draw_script,
        event_count=len(retained_events),
        group_count=len(groups),
        preferred_stop_family=preferred_stop_family,
        preferred_exit_family=preferred_exit_family,
    )
    return PlaybookVisualReviewResult(
        out_dir=review_dir,
        event_count=len(retained_events),
        group_count=len(groups),
        event_csv=event_csv,
        group_csv=group_csv,
        pine_script=pine_script,
        apply_script=apply_script,
        draw_script=draw_script,
        receipt=receipt,
    )


def _select_dates(
    storage: LocalStorage,
    symbol: str,
    *,
    days: int,
    start: date | None,
    end: date | None,
) -> list[date]:
    dates = sorted(storage.existing_dates(symbol))
    if start is not None:
        dates = [item for item in dates if item >= start]
    if end is not None:
        dates = [item for item in dates if item <= end]
    return dates if start is not None or end is not None else dates[-days:]


def _collect_events(
    bars: pl.DataFrame,
    *,
    symbol: str,
    configs: dict[str, dict[str, Any]],
    review_dates: set[date],
) -> list[dict[str, Any]]:
    strategy_records = []
    for config_id, config in configs.items():
        try:
            strategy = build_strategy(STRATEGY_NAME, config)
        except ValueError as exc:
            if "Unsupported stage_filter" in str(exc):
                continue
            raise
        features = frozenset(required_feature_union([strategy]))
        strategy_records.append((config_id, config, strategy, features))

    enriched_by_feature_set: dict[frozenset[str], pl.DataFrame] = {}
    with_metrics_by_entry_key: dict[str, pl.DataFrame] = {}
    events: list[dict[str, Any]] = []
    for config_id, config, strategy, features in strategy_records:
        if features not in enriched_by_feature_set:
            enriched_by_feature_set[features] = PhysicsEngine().enrich_for_features(bars, set(features))
        enriched = enriched_by_feature_set[features]
        entry_key = _entry_signal_cache_key(config)
        if entry_key not in with_metrics_by_entry_key:
            signals = strategy.generate_signals(enriched)
            with_metrics_by_entry_key[entry_key] = MetricsCalculator().add_directional_forward_metrics(
                signals,
                snapshot_windows=(30, 60),
            )
        events.extend(
            _evaluate_config_events(
                symbol,
                config_id,
                config,
                with_metrics_by_entry_key[entry_key],
                review_dates=review_dates,
            )
        )

    events.sort(key=lambda event: (event["entry_timestamp_utc"], event["direction"], event["config_id"]))
    for index, event in enumerate(events, start=1):
        event["event_id"] = f"E{index:04d}"
    return events


def _evaluate_config_events(
    symbol: str,
    config_id: str,
    config: dict[str, Any],
    df: pl.DataFrame,
    *,
    review_dates: set[date],
) -> list[dict[str, Any]]:
    if "signal" not in df.columns:
        return []
    prepared = df.with_row_index("_row_nr").with_columns(
        [
            et_timestamp_expr("timestamp").alias("timestamp_et"),
            et_date_expr("timestamp").alias("_playbook_trade_date"),
            et_time_expr("timestamp").alias("_playbook_bar_time"),
        ]
    )
    rows = prepared.to_dicts()
    events: list[dict[str, Any]] = []
    for row in rows:
        if row.get("_playbook_trade_date") not in review_dates:
            continue
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
    entry_ts = _as_datetime(row.get("timestamp"))
    if entry_ts is None:
        return None
    exit_ts = entry_ts
    exit_price = entry
    exit_reason = "eod"
    max_favorable = 0.0
    max_adverse = 0.0
    exit_family = str(config.get("exit_family", "fixed_1r"))
    stage_column = _market_pulse_stage_column(str(config.get("stage_timeframe", "1m")))
    time_stop = time(11, 30)

    for future in rows[entry_idx + 1 :]:
        if future.get("_playbook_trade_date") != trade_date:
            break
        high = _float(future.get("high"))
        low = _float(future.get("low"))
        close = _float(future.get("close"))
        future_ts = _as_datetime(future.get("timestamp"))
        if high is None or low is None or close is None or future_ts is None:
            continue
        if direction == "long":
            if low <= stop:
                max_adverse = max(max_adverse, risk)
                exit_price = stop
                exit_ts = future_ts
                exit_reason = "stop"
                break
            if target is not None and high >= target:
                max_favorable = max(max_favorable, target - entry)
                max_adverse = max(max_adverse, max(0.0, entry - low))
                exit_price = target
                exit_ts = future_ts
                exit_reason = "target"
                break
            max_favorable = max(max_favorable, high - entry)
            max_adverse = max(max_adverse, entry - low)
        else:
            if high >= stop:
                max_adverse = max(max_adverse, risk)
                exit_price = stop
                exit_ts = future_ts
                exit_reason = "stop"
                break
            if target is not None and low <= target:
                max_favorable = max(max_favorable, entry - target)
                max_adverse = max(max_adverse, max(0.0, high - entry))
                exit_price = target
                exit_ts = future_ts
                exit_reason = "target"
                break
            max_favorable = max(max_favorable, entry - low)
            max_adverse = max(max_adverse, high - entry)
        exit_price = close
        exit_ts = future_ts
        bar_time = future.get("_playbook_bar_time")
        if exit_family == "market_pulse_flip" and _market_pulse_flip_exit(
            direction,
            future.get(stage_column),
        ):
            exit_reason = "market_pulse_flip"
            break
        if exit_family == "time_stop" and isinstance(bar_time, time) and bar_time >= time_stop:
            exit_reason = "time_stop"
            break

    pnl = (exit_price - entry) if direction == "long" else (entry - exit_price)
    pnl_r = pnl / risk
    raw_at_trigger = _float(row.get("playbook_stretch_raw"))
    return {
        "event_id": "",
        "group_id": "",
        "config_id": config_id,
        "symbol": symbol,
        "trade_date": trade_date,
        "direction": direction,
        "entry_timestamp_utc": entry_ts.astimezone(UTC),
        "entry_timestamp_et": entry_ts.astimezone(ET),
        "entry_unix": int(entry_ts.timestamp()),
        "entry_price": entry,
        "exit_timestamp_utc": exit_ts.astimezone(UTC),
        "exit_timestamp_et": exit_ts.astimezone(ET),
        "exit_unix": int(exit_ts.timestamp()),
        "exit_price": exit_price,
        "exit_reason": exit_reason,
        "pnl_r": pnl_r,
        "max_favorable_excursion_r": max_favorable / risk,
        "max_adverse_excursion_r": max_adverse / risk,
        "stop_price": stop,
        "target_price": target,
        "stop_family": str(config.get("stop_family", "")),
        "exit_family": exit_family,
        "stretch_source": str(config.get("stretch_source", "")),
        "stretch_threshold": config.get("stretch_threshold", ""),
        "stretch_value": _float(row.get("playbook_stretch_value")),
        "stretch_raw": raw_at_trigger,
        "stage_filter": str(config.get("stage_filter", "no_filter")),
        "stage_actual": row.get(stage_column, ""),
        "gap_state_filter": str(config.get("gap_state_filter", "no_filter")),
        "gap_state": row.get("gap_state", ""),
        "reversal_range_minutes": config.get("reversal_range_minutes", ""),
        "confirming_bars": config.get("confirming_bars", ""),
    }


def _market_pulse_stage_column(timeframe: str) -> str:
    return "market_pulse_stage" if timeframe == "1m" else f"market_pulse_stage_{timeframe}"


def _event_groups(
    events: list[dict[str, Any]],
    *,
    preferred_stop_family: str,
    preferred_exit_family: str,
) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, date, str, datetime], list[dict[str, Any]]] = {}
    for event in events:
        grouped.setdefault(
            (
                str(event["symbol"]),
                event["trade_date"],
                str(event["direction"]),
                event["entry_timestamp_utc"],
            ),
            [],
        ).append(event)

    groups: list[dict[str, Any]] = []
    sorted_items = sorted(grouped.items(), key=lambda item: (item[0][1], item[0][3], item[0][2]))
    for index, ((symbol, trade_date, direction, entry_timestamp, group_events),) in enumerate(
        [((key[0], key[1], key[2], key[3], value),) for key, value in sorted_items],
        start=1,
    ):
        group_id = f"G{index:02d}"
        for event in group_events:
            event["group_id"] = group_id
        representative = _choose_representative(
            group_events,
            preferred_stop_family=preferred_stop_family,
            preferred_exit_family=preferred_exit_family,
        )
        groups.append(
            {
                "group_id": group_id,
                "symbol": symbol,
                "trade_date": trade_date,
                "direction": direction,
                "entry_timestamp_utc": entry_timestamp,
                "entry_timestamp_et": entry_timestamp.astimezone(ET),
                "entry_price": representative["entry_price"],
                "review_event": representative,
                "events": sorted(group_events, key=lambda event: event["event_id"]),
            }
        )
    return groups


def _choose_representative(
    events: list[dict[str, Any]],
    *,
    preferred_stop_family: str,
    preferred_exit_family: str,
) -> dict[str, Any]:
    def score(event: dict[str, Any]) -> tuple[int, int, int, float, str]:
        threshold = _float(event.get("stretch_threshold"))
        threshold_distance = abs((threshold if threshold is not None else 99.0) - 2.0)
        return (
            0 if event.get("stop_family") == preferred_stop_family else 1,
            0 if event.get("exit_family") == preferred_exit_family else 1,
            0 if event.get("stretch_source") in {"opening_vwap_rth", "opening_vwap"} else 1,
            threshold_distance,
            str(event.get("config_id", "")),
        )

    return sorted(events, key=score)[0]


def _write_pine_script(
    path: Path,
    *,
    symbol: str,
    review_dates: list[date],
    groups: list[dict[str, Any]],
) -> None:
    title = f"Mala 2.2 {symbol} Clean Review {review_dates[0]} to {review_dates[-1]}"
    max_drawings = max(500, len(groups) * 6 + 50)
    lines = [
        "//@version=6",
        (
            f'indicator("{_pine_string(title)}", overlay=true, '
            f"max_boxes_count={max_drawings}, max_lines_count={max_drawings}, "
            f"max_labels_count={max_drawings})"
        ),
        "",
        'showBoxes  = input.bool(false, "Trade risk boxes")',
        'showLines  = input.bool(false, "Entry / stop / target lines")',
        'showLabels = input.bool(true, "Trade labels")',
        'showPulse  = input.bool(true, "Market Pulse VMA")',
        'showStack  = input.bool(false, "VWMA 8 / 21 / 34 stack")',
        'showVpoc   = input.bool(false, "Rolling VPOC approx")',
        'showPulseTable = input.bool(false, "Market Pulse table")',
        'vmaLength = input.int(10, "VMA length", minval=2)',
        'vpocLookbackBars = input.int(240, "VPOC lookback bars", minval=20, maxval=500)',
        'vpocPriceBucket = input.float(0.25, "VPOC price bucket", minval=0.01, step=0.01)',
        'vpocMaxBuckets = input.int(160, "VPOC max buckets", minval=20, maxval=500)',
        "",
        "longColor   = color.rgb(0, 160, 110)",
        "shortColor  = color.rgb(220, 70, 70)",
        "entryColor  = color.rgb(110, 110, 110)",
        "stopColor   = color.rgb(220, 70, 70)",
        "targetColor = color.rgb(0, 160, 110)",
        "",
        "diff = close - nz(close[1], close)",
        "upMove = math.max(diff, 0.0)",
        "downMove = math.max(-diff, 0.0)",
        "sumUp = math.sum(upMove, vmaLength)",
        "sumDown = math.sum(downMove, vmaLength)",
        "directionalStrength = (sumUp + sumDown) != 0.0 ? ((sumUp - sumDown) / (sumUp + sumDown)) * 100.0 : 0.0",
        "coeff = (2.0 / (vmaLength + 1.0)) * math.abs(directionalStrength) / 100.0",
        "var float pulseVma = na",
        "pulseVma := na(pulseVma[1]) ? close : coeff * close + (1.0 - coeff) * pulseVma[1]",
        "vwma8 = ta.vwma(close, 8)",
        "vwma21 = ta.vwma(close, 21)",
        "vwma34 = ta.vwma(close, 34)",
        "bullishStack = vwma8 > vwma21 and vwma21 > vwma34",
        "bearishStack = vwma8 < vwma21 and vwma21 < vwma34",
        "isAcceleration = bullishStack and close >= pulseVma",
        "isDeceleration = bearishStack and close <= pulseVma",
        "isAccumulation = not isAcceleration and close >= pulseVma",
        "stageText = isAcceleration ? \"Stage: Acceleration\" : isDeceleration ? \"Stage: Deceleration\" : isAccumulation ? \"Stage: Accumulation\" : \"Stage: Distribution\"",
        "stageColor = isAcceleration ? color.green : isDeceleration ? color.red : isAccumulation ? color.yellow : color.orange",
        "vmaColor = isAcceleration ? color.green : isDeceleration ? color.red : color.gray",
        "",
        "var float[] vpocVolumeBins = array.new_float(0)",
        "float rollingVpoc = na",
        "windowLowRaw = ta.lowest(low[1], vpocLookbackBars)",
        "windowHighRaw = ta.highest(high[1], vpocLookbackBars)",
        "if bar_index >= vpocLookbackBars + 1",
        "    windowLowBin = math.floor(windowLowRaw / vpocPriceBucket) * vpocPriceBucket",
        "    requiredBuckets = int(math.floor((windowHighRaw - windowLowBin) / vpocPriceBucket)) + 1",
        "    bucketCount = math.min(vpocMaxBuckets, math.max(1, requiredBuckets))",
        "    if array.size(vpocVolumeBins) != bucketCount",
        "        array.clear(vpocVolumeBins)",
        "        for bin = 0 to bucketCount - 1",
        "            array.push(vpocVolumeBins, 0.0)",
        "    else",
        "        for bin = 0 to bucketCount - 1",
        "            array.set(vpocVolumeBins, bin, 0.0)",
        "        for offset = 1 to vpocLookbackBars",
        "            typicalPrice = (high[offset] + low[offset] + close[offset]) / 3.0",
        "            rawIndex = math.round((typicalPrice - windowLowBin) / vpocPriceBucket)",
        "            binIndex = int(math.max(0.0, math.min(bucketCount - 1.0, rawIndex)))",
        "            array.set(vpocVolumeBins, binIndex, array.get(vpocVolumeBins, binIndex) + volume[offset])",
        "        maxVolume = array.get(vpocVolumeBins, 0)",
        "        maxIndex = 0",
        "        for bin = 1 to bucketCount - 1",
        "            binVolume = array.get(vpocVolumeBins, bin)",
        "            if binVolume > maxVolume",
        "                maxVolume := binVolume",
        "                maxIndex := bin",
        "        rollingVpoc := windowLowBin + maxIndex * vpocPriceBucket",
        "",
        "plot(showPulse ? pulseVma : na, \"Market Pulse VMA\", color=vmaColor, linewidth=2)",
        "plot(showStack ? vwma8 : na, \"VWMA 8\", color=color.new(color.rgb(0, 180, 120), 0), linewidth=1)",
        "plot(showStack ? vwma21 : na, \"VWMA 21\", color=color.new(color.rgb(255, 190, 40), 0), linewidth=1)",
        "plot(showStack ? vwma34 : na, \"VWMA 34\", color=color.new(color.rgb(210, 80, 80), 0), linewidth=1)",
        "plot(showVpoc ? rollingVpoc : na, \"Rolling VPOC approx\", color=color.new(color.aqua, 0), linewidth=2)",
        "",
        "var table pulseTable = table.new(position.top_right, 2, 4, border_width=1)",
        "if showPulseTable and barstate.islast",
        "    table.cell(pulseTable, 0, 0, \"Market Pulse\", text_color=color.white, bgcolor=stageColor)",
        "    table.cell(pulseTable, 1, 0, stageText, text_color=color.white, bgcolor=stageColor)",
        "    table.cell(pulseTable, 0, 1, \"VWMA stack\", text_color=color.white, bgcolor=color.new(color.black, 35))",
        "    table.cell(pulseTable, 1, 1, bullishStack ? \"bullish\" : bearishStack ? \"bearish\" : \"mixed\", text_color=color.white, bgcolor=color.new(color.black, 35))",
        "    table.cell(pulseTable, 0, 2, \"Close vs VMA\", text_color=color.white, bgcolor=color.new(color.black, 35))",
        "    table.cell(pulseTable, 1, 2, close >= pulseVma ? \"above\" : \"below\", text_color=color.white, bgcolor=color.new(color.black, 35))",
        "    table.cell(pulseTable, 0, 3, \"VPOC\", text_color=color.white, bgcolor=color.new(color.black, 35))",
        "    table.cell(pulseTable, 1, 3, na(rollingVpoc) ? \"warming\" : str.tostring(rollingVpoc, format.mintick), text_color=color.white, bgcolor=color.new(color.black, 35))",
        "else if not showPulseTable and barstate.islast",
        "    table.clear(pulseTable, 0, 0, 1, 3)",
        "",
        "var box malaBox = na",
        "var line malaLine = na",
        "var label malaLabel = na",
        "",
        "if barstate.isfirst",
    ]
    if not groups:
        lines.append("    na")
    for group in groups:
        event = group["review_event"]
        entry = event["entry_timestamp_et"]
        exit_ts = event["exit_timestamp_et"]
        group_id = str(group["group_id"])
        is_long = event["direction"] == "long"
        side_color = "longColor" if is_long else "shortColor"
        # Place the label off the candle so adjacent events don't stack on the same price:
        # long trades get the label below the bar, shorts above.
        yloc_label = "yloc.belowbar" if is_long else "yloc.abovebar"
        label_style = "label.style_label_up" if is_long else "label.style_label_down"
        arrow = "▲" if is_long else "▼"
        entry_time = _pine_timestamp(entry)
        exit_time = _pine_timestamp(exit_ts)
        entry_price = _pine_float(event["entry_price"])
        stop_price = _pine_float(event["stop_price"])
        target_price_raw = event.get("target_price")
        has_target = target_price_raw is not None
        target_price = _pine_float(target_price_raw) if has_target else _pine_float(event["exit_price"])
        box_top = stop_price if not is_long else target_price
        box_bottom = target_price if not is_long else stop_price
        # Pine's box.new wants top > bottom; swap if the trade direction inverted them.
        box_lines = [
            f"        malaBox := box.new(left={entry_time}, top=math.max({box_top}, {box_bottom}), "
            f"right={exit_time}, bottom=math.min({box_top}, {box_bottom}), "
            f"xloc=xloc.bar_time, border_color={side_color}, border_width=1, "
            f"bgcolor=color.new({side_color}, 92))",
        ]
        line_lines = [
            f"        malaLine := line.new(x1={entry_time}, y1={entry_price}, x2={exit_time}, y2={entry_price}, "
            f"xloc=xloc.bar_time, color=entryColor, width=1, style=line.style_solid)",
            f"        malaLine := line.new(x1={entry_time}, y1={stop_price}, x2={exit_time}, y2={stop_price}, "
            f"xloc=xloc.bar_time, color=stopColor, width=1, style=line.style_dashed)",
        ]
        if has_target:
            line_lines.append(
                f"        malaLine := line.new(x1={entry_time}, y1={target_price}, x2={exit_time}, y2={target_price}, "
                f"xloc=xloc.bar_time, color=targetColor, width=1, style=line.style_dashed)"
            )
        label_text = f"{group_id} {arrow}"
        label_lines = [
            f"        malaLabel := label.new(x={entry_time}, y={entry_price}, text=\"{label_text}\", "
            f"xloc=xloc.bar_time, yloc={yloc_label}, color=color.new({side_color}, 25), "
            f"textcolor=color.white, style={label_style}, size=size.small)",
            f"        malaLabel := label.new(x={exit_time}, y={_pine_float(event['exit_price'])}, text=\"{group_id} exit\", "
            f"xloc=xloc.bar_time, yloc=yloc.price, color=color.new(color.gray, 25), "
            "textcolor=color.white, style=label.style_label_left, size=size.tiny)",
        ]
        lines.append("")
        lines.append(f"    // {group_id} {event['direction']} {entry.isoformat(timespec='minutes')}")
        lines.append("    if showBoxes")
        lines.extend(box_lines)
        lines.append("    if showLines")
        lines.extend(line_lines)
        lines.append("    if showLabels")
        lines.extend(label_lines)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_apply_script(
    path: Path,
    *,
    pine_script: Path,
    symbol: str,
    timeframe: str,
    review_dates: list[date],
    groups: list[dict[str, Any]],
    tradingview_mcp_root: str,
) -> None:
    visible_from, visible_to = _review_visible_range(review_dates)
    scroll_to = groups[0]["entry_timestamp_et"].isoformat() if groups else visible_from.isoformat()
    lines = [
        "#!/usr/bin/env bash",
        "set -euo pipefail",
        "",
        f'PINE_FILE="${{1:-{shlex.quote(str(pine_script.resolve()))}}}"',
        f': "${{TRADINGVIEW_MCP_ROOT:={tradingview_mcp_root}}}"',
        ': "${TRADINGVIEW_CDP_HOST:=127.0.0.1}"',
        ': "${TRADINGVIEW_CDP_PORT:=9223}"',
        "",
        'cd "$TRADINGVIEW_MCP_ROOT"',
        "export TRADINGVIEW_CDP_HOST TRADINGVIEW_CDP_PORT",
        "npm run -s tv -- status",
        f"npm run -s tv -- symbol {_quote(symbol)}",
        f"npm run -s tv -- timeframe {_quote(timeframe)}",
        "npm run -s tv -- type Candles",
        f"npm run -s tv -- scroll {_quote(scroll_to)}",
        (
            "npm run -s tv -- range "
            f"--from {int(visible_from.timestamp())} --to {int(visible_to.timestamp())}"
        ),
        'npm run -s tv -- pine check --file "$PINE_FILE"',
        "npm run -s tv -- ui panel pine-editor open || true",
        'npm run -s tv -- pine set --file "$PINE_FILE"',
        "npm run -s tv -- pine compile",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    path.chmod(0o755)


def _write_draw_script(
    path: Path,
    *,
    symbol: str,
    timeframe: str,
    groups: list[dict[str, Any]],
    tradingview_mcp_root: str,
) -> None:
    lines = [
        "#!/usr/bin/env bash",
        "set -euo pipefail",
        "",
        f': "${{TRADINGVIEW_MCP_ROOT:={tradingview_mcp_root}}}"',
        ': "${TRADINGVIEW_CDP_HOST:=127.0.0.1}"',
        ': "${TRADINGVIEW_CDP_PORT:=9223}"',
        "",
        'cd "$TRADINGVIEW_MCP_ROOT"',
        "export TRADINGVIEW_CDP_HOST TRADINGVIEW_CDP_PORT",
        "npm run -s tv -- status",
        f"npm run -s tv -- symbol {_quote(symbol)}",
        f"npm run -s tv -- timeframe {_quote(timeframe)}",
        "npm run -s tv -- type Candles",
        "",
        "# This intentionally does not call `tv draw clear`; existing chart drawings are preserved.",
        "# Prefer the Pine overlay for normal review. Use this only when you want persistent drawings.",
        "",
    ]
    for group in groups:
        event = group["review_event"]
        entry_label = _entry_label(group).replace("\n", " | ")
        exit_label = _exit_label(group).replace("\n", " | ")
        entry_color = "#009966" if event["direction"] == "long" else "#d24646"
        lines.extend(
            [
                f"# {group['group_id']} {event['direction']} {event['entry_timestamp_et'].isoformat()}",
                _draw_command("text", event["entry_unix"], event["entry_price"], text=entry_label, color=entry_color),
                _draw_command("text", event["exit_unix"], event["exit_price"], text=exit_label, color="#666666"),
                _draw_command(
                    "trend_line",
                    event["entry_unix"],
                    event["entry_price"],
                    point2=(event["exit_unix"], event["exit_price"]),
                    color=entry_color,
                    width=2,
                ),
                _draw_command(
                    "trend_line",
                    event["entry_unix"],
                    event["stop_price"],
                    point2=(event["exit_unix"], event["stop_price"]),
                    color="#cc3333",
                    width=1,
                    linestyle=2,
                ),
            ]
        )
        if event.get("target_price") is not None:
            lines.append(
                _draw_command(
                    "trend_line",
                    event["entry_unix"],
                    event["target_price"],
                    point2=(event["exit_unix"], event["target_price"]),
                    color="#339966",
                    width=1,
                    linestyle=1,
                )
            )
        lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")
    path.chmod(0o755)


def _write_receipt(
    path: Path,
    *,
    run_dir: Path,
    symbol: str,
    tv_symbol: str,
    timeframe: str,
    review_dates: list[date],
    event_csv: Path,
    group_csv: Path,
    pine_script: Path,
    apply_script: Path,
    draw_script: Path,
    event_count: int,
    group_count: int,
    preferred_stop_family: str,
    preferred_exit_family: str,
) -> None:
    lines = [
        "# TradingView Visual Review",
        "",
        f"- source run: `{run_dir.resolve()}`",
        f"- symbol: `{symbol}`",
        f"- TradingView symbol: `{tv_symbol}`",
        f"- timeframe: `{timeframe}`",
        "- TradingView timezone expectation: `Exchange / New York (ET)`",
        "- CSV timezone note: both ET and CT timestamp columns are included",
        f"- dates: `{review_dates[0]} -> {review_dates[-1]}`",
        f"- event variants retained: `{event_count}`",
        f"- grouped entries: `{group_count}`",
        f"- representative preference: stop `{preferred_stop_family}`, exit `{preferred_exit_family}`",
        f"- event CSV: `{event_csv.resolve()}`",
        f"- group CSV: `{group_csv.resolve()}`",
        f"- Pine overlay: `{pine_script.resolve()}`",
        f"- apply script: `{apply_script.resolve()}`",
        f"- optional persistent drawing script: `{draw_script.resolve()}`",
        "",
        "Use the Pine overlay first. It draws one trade per group with entry / stop /",
        "target lines and a side-offset",
        "label. Drawing primitives are emitted once under `if barstate.isfirst`, so the chart",
        "stays clean even when several events fire within minutes of each other. Long-trade",
        "labels sit below the bar, short-trade labels sit above, which is what keeps adjacent",
        "events from stacking at the same price. Optional risk boxes are off by default;",
        "turn on `Trade risk boxes` only when you want to inspect the full stop-to-target",
        "corridor. Toggle `Entry / stop /",
        "target lines`, `Trade labels`, `VWMA 8 / 21 / 34 stack`, and `Market Pulse table`",
        "when you want deeper context. The default clean view shows only `Market Pulse VMA`.",
        "`Rolling VPOC approx` stays available as an off-by-default toggle, but VPOC is approximate in Pine because TradingView scripts",
        "bucket visible bars internally; the research engine remains the source of truth.",
        "Set TradingView's timezone",
        "to `Exchange` so the chart aligns with the ET timestamps. The direct draw script is",
        "optional and intentionally does not clear existing drawings.",
        "",
        "## Apply to TradingView",
        "",
        "```bash",
        f"{apply_script.resolve()}",
        "```",
        "",
        "If TradingView opens a Save Script dialog, click `Save`; the script is",
        "then available as a normal Pine study and can be added/removed from the",
        "chart like any other indicator.",
        "",
        "## Review Notes",
        "",
        "- `event_groups.csv` is the chart-reading index: one row per entry timestamp.",
        "- `event_review.csv` preserves every config/stop/exit variant behind each group.",
        "- The overlay uses small arrows/X marks; detailed labels stay out of the chart.",
        "- If you want to inspect a different stop/exit family, regenerate with",
        "  `--preferred-stop-family` or `--preferred-exit-family`.",
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def _event_csv_row(event: dict[str, Any]) -> dict[str, str]:
    return {
        "event_id": str(event["event_id"]),
        "group_id": str(event["group_id"]),
        "config_id": str(event["config_id"]),
        "symbol": str(event["symbol"]),
        "trade_date": _format_value(event["trade_date"]),
        "direction": str(event["direction"]),
        "entry_timestamp_et": event["entry_timestamp_et"].isoformat(timespec="seconds"),
        "entry_timestamp_ct": event["entry_timestamp_et"].astimezone(CT).isoformat(timespec="seconds"),
        "entry_unix": str(event["entry_unix"]),
        "entry_price": _format_float(event["entry_price"]),
        "exit_timestamp_et": event["exit_timestamp_et"].isoformat(timespec="seconds"),
        "exit_timestamp_ct": event["exit_timestamp_et"].astimezone(CT).isoformat(timespec="seconds"),
        "exit_unix": str(event["exit_unix"]),
        "exit_price": _format_float(event["exit_price"]),
        "exit_reason": str(event["exit_reason"]),
        "pnl_r": _format_float(event["pnl_r"]),
        "max_favorable_excursion_r": _format_float(event["max_favorable_excursion_r"]),
        "max_adverse_excursion_r": _format_float(event["max_adverse_excursion_r"]),
        "stop_price": _format_float(event["stop_price"]),
        "target_price": _format_float(event.get("target_price")),
        "stop_family": str(event["stop_family"]),
        "exit_family": str(event["exit_family"]),
        "stretch_source": str(event["stretch_source"]),
        "stretch_threshold": _format_value(event["stretch_threshold"]),
        "stretch_value": _format_float(event.get("stretch_value")),
        "stretch_raw": _format_float(event.get("stretch_raw")),
        "stage_filter": str(event["stage_filter"]),
        "stage_actual": str(event["stage_actual"]),
        "gap_state_filter": str(event["gap_state_filter"]),
        "gap_state": str(event["gap_state"]),
        "reversal_range_minutes": _format_value(event["reversal_range_minutes"]),
        "confirming_bars": _format_value(event["confirming_bars"]),
    }


def _group_csv_row(group: dict[str, Any]) -> dict[str, str]:
    events = group["events"]
    representative = group["review_event"]
    pnl_values = [_float(event.get("pnl_r")) for event in events]
    mfe_values = [_float(event.get("max_favorable_excursion_r")) for event in events]
    mae_values = [_float(event.get("max_adverse_excursion_r")) for event in events]
    return {
        "group_id": str(group["group_id"]),
        "symbol": str(group["symbol"]),
        "trade_date": _format_value(group["trade_date"]),
        "direction": str(group["direction"]),
        "entry_timestamp_et": group["entry_timestamp_et"].isoformat(timespec="seconds"),
        "entry_timestamp_ct": group["entry_timestamp_et"].astimezone(CT).isoformat(timespec="seconds"),
        "entry_price": _format_float(group["entry_price"]),
        "review_event_id": str(representative["event_id"]),
        "review_exit_timestamp_et": representative["exit_timestamp_et"].isoformat(timespec="seconds"),
        "review_exit_timestamp_ct": representative["exit_timestamp_et"].astimezone(CT).isoformat(timespec="seconds"),
        "review_exit_price": _format_float(representative["exit_price"]),
        "review_exit_reason": str(representative["exit_reason"]),
        "variant_count": str(len(events)),
        "pnl_r_min": _format_float(_min(pnl_values)),
        "pnl_r_max": _format_float(_max(pnl_values)),
        "mfe_r_max": _format_float(_max(mfe_values)),
        "mae_r_max": _format_float(_max(mae_values)),
        "exit_reasons": _join_unique(event.get("exit_reason", "") for event in events),
        "exit_families": _join_unique(event.get("exit_family", "") for event in events),
        "stop_families": _join_unique(event.get("stop_family", "") for event in events),
        "stretch_sources": _join_unique(event.get("stretch_source", "") for event in events),
    }


def _entry_label(group: dict[str, Any]) -> str:
    event = group["review_event"]
    return (
        f"{group['group_id']} {event['direction'].upper()} "
        f"{event['entry_timestamp_et'].strftime('%m/%d %H:%M ET')}\n"
        f"entry {event['entry_price']:.2f} stop {event['stop_price']:.2f}\n"
        f"variants {len(group['events'])} pnl { _format_float(_min([_float(e.get('pnl_r')) for e in group['events']])) }"
        f"..{ _format_float(_max([_float(e.get('pnl_r')) for e in group['events']])) }R\n"
        f"rep {event['event_id']} {event['stop_family']}/{event['exit_family']}"
    )


def _exit_label(group: dict[str, Any]) -> str:
    event = group["review_event"]
    return (
        f"{group['group_id']} EXIT {event['exit_reason']}\n"
        f"{event['exit_timestamp_et'].strftime('%H:%M ET')} {event['exit_price']:.2f}\n"
        f"{event['pnl_r']:.2f}R MFE {event['max_favorable_excursion_r']:.2f} MAE {event['max_adverse_excursion_r']:.2f}"
    )


def _draw_command(
    shape: str,
    timestamp: int,
    price: float | None,
    *,
    text: str | None = None,
    point2: tuple[int, float | None] | None = None,
    color: str,
    width: int = 1,
    linestyle: int | None = None,
) -> str:
    if price is None:
        return "# skipped drawing with missing price"
    overrides: dict[str, Any] = {"linecolor": color, "color": color, "textcolor": "#ffffff", "linewidth": width}
    if linestyle is not None:
        overrides["linestyle"] = linestyle
    command = [
        "npm run -s tv -- draw shape",
        f"--type {_quote(shape)}",
        f"--time {timestamp}",
        f"--price {_format_float(price)}",
        f"--overrides {_quote(json.dumps(overrides, separators=(',', ':')))}",
    ]
    if point2 is not None:
        time2, price2 = point2
        if price2 is None:
            return "# skipped two-point drawing with missing second price"
        command.extend([f"--time2 {time2}", f"--price2 {_format_float(price2)}"])
    if text is not None:
        command.append(f"--text {_quote(text)}")
    return " ".join(command)


def _review_visible_range(review_dates: list[date]) -> tuple[datetime, datetime]:
    start_dt = datetime.combine(review_dates[0], time(9, 15), tzinfo=ET)
    end_dt = datetime.combine(review_dates[-1], time(12, 30), tzinfo=ET)
    return start_dt, end_dt


def _as_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)
    return None


def _pine_timestamp(value: datetime) -> str:
    et_value = value.astimezone(ET)
    return (
        'timestamp("America/New_York", '
        f"{et_value.year}, {et_value.month}, {et_value.day}, {et_value.hour}, {et_value.minute})"
    )


def _pine_float(value: object) -> str:
    parsed = _float(value)
    return "na" if parsed is None else _format_float(parsed)


def _pine_string(value: str) -> str:
    return (
        value.replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\n", "\\n")
        .replace("\r", "")
    )


def _format_float(value: object) -> str:
    parsed = _float(value)
    if parsed is None:
        return ""
    return f"{parsed:.6f}".rstrip("0").rstrip(".")


def _format_value(value: object) -> str:
    if isinstance(value, date):
        return value.isoformat()
    if value is None:
        return ""
    return str(value)


def _min(values: list[float | None]) -> float | None:
    cleaned = [value for value in values if value is not None]
    return min(cleaned) if cleaned else None


def _max(values: list[float | None]) -> float | None:
    cleaned = [value for value in values if value is not None]
    return max(cleaned) if cleaned else None


def _join_unique(values: object) -> str:
    unique = sorted({str(value).strip() for value in values if str(value).strip()})
    return " | ".join(unique)


def _write_csv(path: Path, rows: list[dict[str, str]], columns: list[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def _quote(value: str) -> str:
    return shlex.quote(value)


def _parse_date(value: str | None) -> date | None:
    return date.fromisoformat(value) if value else None


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", required=True, type=Path, help="Playbook surface run directory")
    parser.add_argument("--symbol", default="QQQ")
    parser.add_argument("--tv-symbol", help="TradingView symbol override, e.g. NASDAQ:QQQ")
    parser.add_argument("--days", type=int, default=5, help="Last N cached dates when start/end are omitted")
    parser.add_argument("--start", help="Optional review start date, YYYY-MM-DD")
    parser.add_argument("--end", help="Optional review end date, YYYY-MM-DD")
    parser.add_argument("--out-dir", type=Path)
    parser.add_argument("--timeframe", default="5")
    parser.add_argument("--preferred-stop-family", default="reversal_extreme")
    parser.add_argument("--preferred-exit-family", default="fixed_1r")
    parser.add_argument("--max-groups", type=int, default=40)
    parser.add_argument("--tradingview-mcp-root", default=DEFAULT_TRADINGVIEW_MCP_ROOT)
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    result = build_playbook_visual_review(
        args.run_dir,
        symbol=args.symbol,
        tv_symbol=args.tv_symbol,
        days=args.days,
        start=_parse_date(args.start),
        end=_parse_date(args.end),
        out_dir=args.out_dir,
        timeframe=args.timeframe,
        preferred_stop_family=args.preferred_stop_family,
        preferred_exit_family=args.preferred_exit_family,
        max_groups=args.max_groups,
        tradingview_mcp_root=args.tradingview_mcp_root,
    )
    print(f"OUT_DIR={result.out_dir}")
    print(f"EVENTS={result.event_count}")
    print(f"GROUPS={result.group_count}")
    print(f"EVENT_CSV={result.event_csv}")
    print(f"GROUP_CSV={result.group_csv}")
    print(f"PINE_SCRIPT={result.pine_script}")
    print(f"APPLY_SCRIPT={result.apply_script}")
    print(f"DRAW_SCRIPT={result.draw_script}")
    print(f"RECEIPT={result.receipt}")


if __name__ == "__main__":
    main()
