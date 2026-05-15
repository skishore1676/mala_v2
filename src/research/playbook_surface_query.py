"""Query a playbook surface for one operator-supplied bias/timestamp.

The surface runner builds the historical map. This module answers the desk
question: "At this time, for this symbol/direction/playbook, does the current
state match any historically useful region, and how would the play be managed?"
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
import statistics
from dataclasses import dataclass
from datetime import UTC, datetime, time, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import polars as pl

from src.chronos.storage import LocalStorage
from src.config import DATA_DIR
from src.newton.engine import PhysicsEngine
from src.oracle.metrics import MetricsCalculator
from src.research.playbook_consultation_log import append_consultation_query
from src.research.playbook_operator_policy import OperatorPolicy, load_operator_policy
from src.research.playbook_surface import _entry_signal_cache_key
from src.strategy.base import required_feature_union
from src.strategy.factory import build_strategy
from src.strategy.intraday_mean_reversion import PLAYBOOK_ID, STRATEGY_NAME
from src.time_utils import et_timestamp_expr


NY = ZoneInfo("America/New_York")
CT = ZoneInfo("America/Chicago")


@dataclass(frozen=True, slots=True)
class PlaybookQueryResult:
    out_dir: Path
    review_md: Path
    json_path: Path
    verdict: str
    active_matches: int


class PlaybookQueryAdapter:
    """Playbook-specific translation hooks used by the generic query runner."""

    playbook_id: str
    strategy_name: str

    def entry_signal_cache_key(self, config: dict[str, Any]) -> str:
        return json.dumps(config, sort_keys=True, default=str)

    def state_snapshot(
        self,
        *,
        config: dict[str, Any],
        row: dict[str, Any],
        requested_direction: str,
    ) -> dict[str, str]:
        raise NotImplementedError

    def management_packet(self, surface_row: dict[str, str]) -> dict[str, str]:
        raise NotImplementedError

    def candidate_language(self, surface_row: dict[str, str], candidate_type: str) -> str:
        grade = surface_row.get("match_grade", "")
        if grade == "favorable":
            return "historically favorable by strict surface gates"
        if grade == "near_favorable":
            return "near-favorable by surface gates; one strict criterion missed"
        if candidate_type:
            return candidate_type
        if grade == "partial":
            return "positive but not fully confirmed"
        return grade or "ungraded"


class IntradayMeanReversionQueryAdapter(PlaybookQueryAdapter):
    playbook_id = PLAYBOOK_ID
    strategy_name = STRATEGY_NAME

    def entry_signal_cache_key(self, config: dict[str, Any]) -> str:
        return _entry_signal_cache_key(config)

    def state_snapshot(
        self,
        *,
        config: dict[str, Any],
        row: dict[str, Any],
        requested_direction: str,
    ) -> dict[str, str]:
        raw = _safe_float(row.get("playbook_stretch_raw"))
        reference_state = (
            "below_reference"
            if raw is not None and raw < 0
            else "above_reference"
            if raw is not None and raw > 0
            else "at_reference"
        )
        signal_direction = str(row.get("signal_direction") or "")
        return {
            "playbook": self.playbook_id,
            "state_read": "intraday extension reversion",
            "requested_direction": requested_direction,
            "close": _format_float(_safe_float(row.get("close"))),
            "stretch_source": str(config.get("stretch_source", "")),
            "stretch_threshold": _format_float(_safe_float(config.get("stretch_threshold"))),
            "stretch_value": _format_float(_safe_float(row.get("playbook_stretch_value"))),
            "stretch_raw": _format_float(raw, digits=6),
            "reference_state": reference_state,
            "stage_actual": _display(
                row.get(
                    "market_pulse_stage",
                    row.get("market_pulse_stage_5m", row.get("vwma_stage_5m")),
                )
            ),
            "stage_filter": str(config.get("stage_filter", "no_filter")),
            "gap_state": _display(row.get("gap_state")),
            "gap_state_filter": str(config.get("gap_state_filter", "no_filter")),
            "reversal_range_minutes": str(config.get("reversal_range_minutes", "")),
            "confirming_bars": str(config.get("confirming_bars", "")),
            "volume_filter": _volume_filter_label(config),
            "signal": "yes" if bool(row.get("signal")) else "no",
            "signal_direction": signal_direction,
            "matches_requested_direction": "yes" if signal_direction == requested_direction else "no",
        }

    def management_packet(self, surface_row: dict[str, str]) -> dict[str, str]:
        stop_family = surface_row.get("stop_family", "")
        exit_family = surface_row.get("exit_family", "")
        stop_text = {
            "reversal_extreme": "stop at the reversal range extreme",
            "reversal_midpoint": "stop at the reversal range midpoint",
            "immediate_entry_bar_failure": "invalidate on immediate entry-bar failure",
        }.get(stop_family, stop_family or "not specified")
        exit_text = {
            "fixed_1r": "take the primary exit at 1.0R",
            "fixed_1_5r": "take the primary exit at 1.5R",
            "fixed_2r": "take the primary exit at 2.0R",
            "vwap_return": "target return to opening VWAP",
            "partial_retrace_50": "target a 50% retrace toward the reference",
            "market_pulse_flip": (
                "exit when 1m MarketPulse flips against the trade "
                "(long to bearish, short to bullish)"
            ),
            "time_stop": "exit by the playbook time stop",
        }.get(exit_family, exit_family or "not specified")
        return {
            "entry": (
                f"{surface_row.get('reversal_range_minutes', '')}m reversal-range breakout "
                f"before {surface_row.get('entry_cutoff_et', '')} ET"
            ),
            "stop": stop_text,
            "exit": exit_text,
            "context": (
                f"extension={surface_row.get('extension_family', '')}>"
                f"{surface_row.get('extension_bin', '')}; "
                f"stage={surface_row.get('stage_filter', '')}; "
                f"gap={surface_row.get('gap_state_filter', '')}; "
                f"volume={surface_row.get('volume_confirmation_filter', '')}"
            ),
        }

ADAPTERS: dict[str, PlaybookQueryAdapter] = {
    PLAYBOOK_ID: IntradayMeanReversionQueryAdapter(),
}


def query_playbook_surface(
    run_dir: Path,
    *,
    symbol: str,
    direction: str,
    timestamp: datetime,
    mode: str = "state-management",
    out_dir: Path | None = None,
    data_dir: Path | None = None,
    max_nearest_seconds: int = 90,
    top_candidates: int = 8,
    analog_lookback_days: int = 1825,
    analog_count: int = 75,
    write_log: bool = True,
    operator_policy_config: Path | None = None,
) -> PlaybookQueryResult:
    """Build an operator query artifact for one playbook/symbol/direction/timestamp."""
    if mode not in {"signal", "state-management"}:
        raise ValueError("mode must be 'signal' or 'state-management'")

    config_path = run_dir / "config.json"
    surface_path = run_dir / "conditional_surface_by_symbol.csv"
    if not config_path.exists():
        raise FileNotFoundError(f"config.json not found under {run_dir}")
    if not surface_path.exists():
        raise FileNotFoundError(f"conditional_surface_by_symbol.csv not found under {run_dir}")

    run_config = json.loads(config_path.read_text(encoding="utf-8"))
    playbook_id = str(run_config.get("playbook_id", ""))
    adapter = ADAPTERS.get(playbook_id)
    if adapter is None:
        raise ValueError(
            f"Unsupported playbook {playbook_id!r}; available adapters: {sorted(ADAPTERS)}"
        )
    operator_policy = load_operator_policy(
        playbook_id=playbook_id,
        path=operator_policy_config,
    )

    normalized_symbol = symbol.strip().upper()
    normalized_direction = _normalize_direction(direction)
    query_ts = _ensure_aware(timestamp).astimezone(NY)
    query_utc = query_ts.astimezone(UTC)
    query_date = query_ts.date()
    query_dir = out_dir or _default_out_dir(
        run_dir,
        normalized_symbol,
        normalized_direction,
        query_ts,
        mode=mode,
    )
    query_dir.mkdir(parents=True, exist_ok=True)

    configs_by_id = {
        str(config_id): dict(config)
        for config_id, config in dict(run_config.get("configs", {})).items()
    }
    entry_window = _entry_window_scope(configs_by_id.values(), query_ts)
    if mode == "state-management":
        payload = _state_management_payload(
            playbook_id=playbook_id,
            run_dir=run_dir,
            symbol=normalized_symbol,
            direction=normalized_direction,
            query_ts=query_ts,
            query_utc=query_utc,
            entry_window=entry_window,
            data_dir=data_dir,
            max_nearest_seconds=max_nearest_seconds,
            analog_lookback_days=analog_lookback_days,
            analog_count=analog_count,
            operator_policy=operator_policy,
        )
        review_md = query_dir / "QUERY_REVIEW.md"
        json_path = query_dir / "query_result.json"
        json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        _write_query_md(review_md, payload, adapter)
        if write_log:
            append_consultation_query(run_dir, payload, review_md, json_path)
        return PlaybookQueryResult(
            out_dir=query_dir,
            review_md=review_md,
            json_path=json_path,
            verdict=str(payload["verdict"]),
            active_matches=0,
        )

    surface_rows = [
        row
        for row in _read_csv(surface_path)
        if row.get("symbol", "").upper() == normalized_symbol
        and row.get("direction", "").lower() == normalized_direction
    ]
    candidate_rows = _candidate_rows(run_dir, normalized_symbol, normalized_direction)
    candidate_by_key = {
        _surface_key(row): row
        for row in candidate_rows
    }

    bars = LocalStorage(base_dir=data_dir or DATA_DIR).load_bars(
        normalized_symbol,
        start=query_date - timedelta(days=45),
        end=query_date,
    )
    if bars.is_empty():
        raise FileNotFoundError(f"No bars found for {normalized_symbol} on {query_date}")

    signal_rows_by_config = _signal_rows_at_timestamp(
        adapter,
        configs_by_id,
        bars,
        query_utc=query_utc,
        max_nearest_seconds=max_nearest_seconds,
    )
    active_config_ids = {
        config_id
        for config_id, row in signal_rows_by_config.items()
        if bool(row.get("signal")) and str(row.get("signal_direction")) == normalized_direction
    }
    active_rows = [
        _decorate_surface_row(row, candidate_by_key)
        for row in surface_rows
        if row.get("config_id") in active_config_ids
    ]
    active_rows = sorted(active_rows, key=_query_row_sort_key)
    top_context_rows = sorted(
        [_decorate_surface_row(row, candidate_by_key) for row in surface_rows],
        key=_query_row_sort_key,
    )[: max(1, top_candidates)]
    representative_rows = _representative_state_rows(
        adapter,
        configs_by_id,
        signal_rows_by_config,
        normalized_direction,
    )
    verdict, verdict_reason = _query_verdict(active_rows, entry_window=entry_window)
    management = adapter.management_packet(active_rows[0]) if active_rows else {}

    payload = {
        "playbook_id": playbook_id,
        "source_run": str(run_dir),
        "mode": mode,
        "symbol": normalized_symbol,
        "direction": normalized_direction,
        "timestamp_et": query_ts.isoformat(),
        "timestamp_utc": query_utc.isoformat(),
        "entry_window": entry_window,
        "verdict": verdict,
        "verdict_reason": verdict_reason,
        "active_match_count": len(active_rows),
        "state_snapshots": representative_rows,
        "active_matches": active_rows,
        "management_packet": management,
        "top_historical_candidates": top_context_rows,
    }

    review_md = query_dir / "QUERY_REVIEW.md"
    json_path = query_dir / "query_result.json"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    _write_query_md(review_md, payload, adapter)

    return PlaybookQueryResult(
        out_dir=query_dir,
        review_md=review_md,
        json_path=json_path,
        verdict=verdict,
        active_matches=len(active_rows),
    )


STATE_MANAGEMENT_FEATURES = {
    "opening_vwap_rth",
    "prior_rth_close_atr",
    "velocity_5",
    "velocity_15",
    "market_pulse_stage",
}
STATE_FORWARD_WINDOWS: tuple[int | str, ...] = (5, 10, 15, 30, 60, "eod")
STATE_MANAGEMENT_EXIT_SPECS: tuple[tuple[str, str, str, float], ...] = (
    ("scalp_0.15pct", "underlying move 0.15%", "pct", 0.0015),
    ("scalp_0.25pct", "underlying move 0.25%", "pct", 0.0025),
    ("scalp_0.35pct", "underlying move 0.35%", "pct", 0.0035),
    ("retrace_to_vwap_25pct", "25% retrace toward opening VWAP", "vwap_retrace", 0.25),
    ("retrace_to_vwap_50pct", "50% retrace toward opening VWAP", "vwap_retrace", 0.50),
    ("vwap_return", "return to opening VWAP", "vwap_retrace", 1.0),
)
SIMILARITY_FEATURE_CONFIG = [
    {"feature": "bias_prior_close_atr", "scale": 0.45, "weight": 1.0},
    {"feature": "bias_vwap_distance_pct", "scale": 0.0035, "weight": 1.0},
    {"feature": "bias_velocity_5_atr", "scale": 0.10, "weight": 0.65},
    {"feature": "bias_velocity_15_atr", "scale": 0.16, "weight": 0.45},
    {"feature": "_minute_of_day", "scale": 25.0, "weight": 0.8},
]
SIMILARITY_STAGE_MISMATCH_PENALTY = 0.45
SIMILARITY_GAP_MISMATCH_PENALTY = 0.35


def _state_management_payload(
    *,
    playbook_id: str,
    run_dir: Path,
    symbol: str,
    direction: str,
    query_ts: datetime,
    query_utc: datetime,
    entry_window: dict[str, str],
    data_dir: Path | None,
    max_nearest_seconds: int,
    analog_lookback_days: int,
    analog_count: int,
    operator_policy: OperatorPolicy,
) -> dict[str, Any]:
    query_date = query_ts.date()
    bars = LocalStorage(base_dir=data_dir or DATA_DIR).load_bars(
        symbol,
        start=query_date - timedelta(days=max(30, analog_lookback_days)),
        end=query_date,
    )
    if bars.is_empty():
        raise FileNotFoundError(f"No bars found for {symbol} on or before {query_date}")

    enriched = PhysicsEngine().enrich_for_features(bars, STATE_MANAGEMENT_FEATURES)
    frame = _prepare_state_management_frame(enriched, direction)
    rows = frame.to_dicts()
    if not rows:
        raise ValueError("No rows available after state-management feature enrichment")
    query_row = _nearest_state_row(rows, query_utc, max_nearest_seconds)
    analog_candidates = _analog_candidate_rows(
        rows,
        query_row=query_row,
        query_utc=query_utc,
        entry_window=entry_window,
        direction=direction,
    )
    analog_rows = analog_candidates[: max(1, analog_count)]
    cohort = [_analog_payload(rows, analog, direction) for analog in analog_rows]
    management_rows = _cohort_management_rows(
        rows,
        analog_rows,
        query_row,
        direction,
        operator_policy,
    )
    outcome_summary = _cohort_outcome_summary(cohort)
    verdict, verdict_reason = _cohort_verdict(outcome_summary, operator_policy)
    current_state = _state_snapshot_for_desk(query_row, direction)
    return {
        "playbook_id": playbook_id,
        "source_run": str(run_dir),
        "mode": "state-management",
        "symbol": symbol,
        "direction": direction,
        "timestamp_et": query_ts.isoformat(),
        "timestamp_utc": query_utc.isoformat(),
        "entry_window": entry_window,
        "verdict": verdict,
        "verdict_reason": verdict_reason,
        "active_match_count": 0,
        "current_state": current_state,
        "operator_policy": operator_policy.to_payload(),
        "similarity_config": _similarity_config_payload(),
        "cohort": {
            "requested_count": analog_count,
            "candidate_count": len(analog_candidates),
            "analog_count": len(cohort),
            "confidence": operator_policy.confidence_for_count(len(cohort)),
            "similarity_median": _format_float(_median([row.get("similarity") for row in cohort])),
            "similarity_tail": _similarity_tail(analog_candidates, analog_count),
            "outcome_summary": outcome_summary,
            "management_rows": management_rows,
            "analogs": cohort[: min(analog_count, 20)],
        },
    }


def _prepare_state_management_frame(df: pl.DataFrame, direction: str) -> pl.DataFrame:
    direction_sign = 1.0 if direction == "short" else -1.0
    atr_col = "daily_rth_atr_14"
    return (
        df.with_columns(et_timestamp_expr("timestamp").alias("_timestamp_et"))
        .with_row_index("_row_index")
        .with_columns(
            [
                (pl.col("_timestamp_et").dt.hour() * 60 + pl.col("_timestamp_et").dt.minute())
                .alias("_minute_of_day"),
                pl.when(pl.col("opening_vwap_rth") > 0)
                .then((pl.col("close") - pl.col("opening_vwap_rth")) / pl.col("opening_vwap_rth"))
                .otherwise(None)
                .alias("vwap_distance_pct"),
                pl.when(pl.col(atr_col) > 0)
                .then(pl.col("velocity_5") / pl.col(atr_col))
                .otherwise(None)
                .alias("velocity_5_atr"),
                pl.when(pl.col(atr_col) > 0)
                .then(pl.col("velocity_15") / pl.col(atr_col))
                .otherwise(None)
                .alias("velocity_15_atr"),
            ]
        )
        .with_columns(
            [
                (pl.col("atr_distance_from_prior_rth_close") * direction_sign).alias(
                    "bias_prior_close_atr"
                ),
                (pl.col("vwap_distance_pct") * direction_sign).alias("bias_vwap_distance_pct"),
                (pl.col("velocity_5_atr") * direction_sign).alias("bias_velocity_5_atr"),
                (pl.col("velocity_15_atr") * direction_sign).alias("bias_velocity_15_atr"),
            ]
        )
    )


def _nearest_state_row(
    rows: list[dict[str, Any]],
    query_utc: datetime,
    max_nearest_seconds: int,
) -> dict[str, Any]:
    best = min(
        rows,
        key=lambda row: abs((_ensure_aware(row["timestamp"]).astimezone(UTC) - query_utc).total_seconds()),
    )
    distance = abs((_ensure_aware(best["timestamp"]).astimezone(UTC) - query_utc).total_seconds())
    if distance > max_nearest_seconds:
        raise ValueError(
            f"Nearest bar is {int(distance)} seconds from query timestamp; "
            f"max allowed is {max_nearest_seconds}"
        )
    return best


def _analog_candidate_rows(
    rows: list[dict[str, Any]],
    *,
    query_row: dict[str, Any],
    query_utc: datetime,
    entry_window: dict[str, str],
    direction: str,
) -> list[dict[str, Any]]:
    query_date = _ensure_aware(query_row["timestamp"]).astimezone(NY).date()
    candidates: list[dict[str, Any]] = []
    for row in rows:
        row_ts = _ensure_aware(row["timestamp"]).astimezone(UTC)
        row_date = row_ts.astimezone(NY).date()
        if row_ts >= query_utc or row_date == query_date:
            continue
        if not _row_in_entry_window(row, entry_window):
            continue
        if _safe_float(row.get("close")) is None:
            continue
        distance = _state_distance(row, query_row)
        if distance is None:
            continue
        enriched = dict(row)
        enriched["similarity"] = distance
        candidates.append(enriched)
    candidates.sort(key=lambda row: row["similarity"])
    return candidates


def _similarity_config_payload() -> dict[str, Any]:
    return {
        "method": "weighted_euclidean_direction_aware_state",
        "features": SIMILARITY_FEATURE_CONFIG,
        "stage_mismatch_penalty": SIMILARITY_STAGE_MISMATCH_PENALTY,
        "gap_mismatch_penalty": SIMILARITY_GAP_MISMATCH_PENALTY,
        "note": (
            "Hard-coded v1 desk scales; recorded for audit. Lower distance means a closer "
            "state analog for the requested bias."
        ),
    }


def _similarity_tail(
    candidates: list[dict[str, Any]],
    analog_count: int,
) -> dict[str, str]:
    def by_rank(rank: int) -> str:
        if len(candidates) < rank:
            return ""
        return _format_float(_safe_float(candidates[rank - 1].get("similarity")))

    return {
        "selected_last_similarity": by_rank(max(1, analog_count)),
        "rank_25_similarity": by_rank(25),
        "rank_75_similarity": by_rank(75),
        "rank_200_similarity": by_rank(200),
    }


def _row_in_entry_window(row: dict[str, Any], entry_window: dict[str, str]) -> bool:
    start = _parse_time_value(entry_window.get("entry_window_start_et"))
    end = _parse_time_value(entry_window.get("entry_window_end_et"))
    if start is None or end is None:
        return True
    ts = row.get("_timestamp_et")
    if not isinstance(ts, datetime):
        return True
    bar_time = ts.time()
    return start <= bar_time <= end


def _state_distance(row: dict[str, Any], query_row: dict[str, Any]) -> float | None:
    total = 0.0
    used = 0
    for spec in SIMILARITY_FEATURE_CONFIG:
        key = str(spec["feature"])
        scale = float(spec["scale"])
        weight = float(spec["weight"])
        row_value = _safe_float(row.get(key))
        query_value = _safe_float(query_row.get(key))
        if row_value is None or query_value is None:
            total += weight * 1.0
            continue
        total += weight * ((row_value - query_value) / scale) ** 2
        used += 1
    if used < 2:
        return None
    if _display(row.get("market_pulse_stage")) != _display(query_row.get("market_pulse_stage")):
        total += SIMILARITY_STAGE_MISMATCH_PENALTY
    if _display(row.get("gap_state_rth_open")) != _display(query_row.get("gap_state_rth_open")):
        total += SIMILARITY_GAP_MISMATCH_PENALTY
    return math.sqrt(total)


def _analog_payload(
    rows: list[dict[str, Any]],
    row: dict[str, Any],
    direction: str,
) -> dict[str, Any]:
    outcomes = _forward_outcomes(rows, int(row["_row_index"]), direction, STATE_FORWARD_WINDOWS)
    ts = _ensure_aware(row["timestamp"]).astimezone(NY)
    return {
        "timestamp_et": ts.isoformat(),
        "similarity": round(float(row["similarity"]), 4),
        "close": _format_float(_safe_float(row.get("close"))),
        "stage": _display(row.get("market_pulse_stage")),
        "gap_state": _display(row.get("gap_state_rth_open")),
        "bias_prior_close_atr": _format_float(_safe_float(row.get("bias_prior_close_atr"))),
        "bias_vwap_distance_pct": _format_pct(_safe_float(row.get("bias_vwap_distance_pct"))),
        "outcomes": outcomes,
    }


def _forward_outcomes(
    rows: list[dict[str, Any]],
    row_index: int,
    direction: str,
    windows: tuple[int | str, ...],
) -> dict[str, dict[str, str]]:
    row = rows[row_index]
    entry = _safe_float(row.get("close"))
    if entry is None:
        return {}
    out: dict[str, dict[str, str]] = {}
    for window in windows:
        future = _future_rows_same_day(rows, row_index, window)
        mfe, mae = _mfe_mae_for_future(entry, future, direction)
        window_key = str(window)
        out[window_key] = {
            "mfe": _format_float(mfe),
            "mae": _format_float(mae),
            "mfe_pct": _format_pct((mfe / entry) if mfe is not None else None),
            "mae_pct": _format_pct((mae / entry) if mae is not None else None),
        }
    return out


def _future_rows_same_day(
    rows: list[dict[str, Any]],
    row_index: int,
    window: int | str,
) -> list[dict[str, Any]]:
    current_ts = _ensure_aware(rows[row_index]["timestamp"]).astimezone(NY)
    current_date = current_ts.date()
    future: list[dict[str, Any]] = []
    end_index = len(rows) if str(window).lower() == "eod" else min(len(rows), row_index + int(window) + 1)
    for idx in range(row_index + 1, end_index):
        ts = _ensure_aware(rows[idx]["timestamp"]).astimezone(NY)
        if ts.date() != current_date:
            break
        future.append(rows[idx])
    return future


def _mfe_mae_for_future(
    entry: float,
    future: list[dict[str, Any]],
    direction: str,
) -> tuple[float | None, float | None]:
    highs = [_safe_float(row.get("high")) for row in future]
    lows = [_safe_float(row.get("low")) for row in future]
    highs = [value for value in highs if value is not None]
    lows = [value for value in lows if value is not None]
    if not highs or not lows:
        return None, None
    if direction == "long":
        return max(highs) - entry, entry - min(lows)
    return entry - min(lows), max(highs) - entry


def _cohort_outcome_summary(cohort: list[dict[str, Any]]) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    for window in STATE_FORWARD_WINDOWS:
        window_key = str(window)
        mfe_values = [
            _safe_float(row.get("outcomes", {}).get(window_key, {}).get("mfe"))
            for row in cohort
        ]
        mae_values = [
            _safe_float(row.get("outcomes", {}).get(window_key, {}).get("mae"))
            for row in cohort
        ]
        mfe_values = [value for value in mfe_values if value is not None]
        mae_values = [value for value in mae_values if value is not None]
        paired = [
            (
                _safe_float(row.get("outcomes", {}).get(window_key, {}).get("mfe")),
                _safe_float(row.get("outcomes", {}).get(window_key, {}).get("mae")),
            )
            for row in cohort
        ]
        paired = [(mfe, mae) for mfe, mae in paired if mfe is not None and mae is not None]
        reversion = sum(1 for mfe, mae in paired if mfe > mae and mfe > 0)
        continuation = sum(1 for mfe, mae in paired if mae > mfe and mae > 0)
        chop = max(0, len(paired) - reversion - continuation)
        summary[window_key] = {
            "n": len(paired),
            "median_mfe": _format_float(_median(mfe_values)),
            "median_mae": _format_float(_median(mae_values)),
            "p75_mfe": _format_float(_quantile(mfe_values, 0.75)),
            "p75_mae": _format_float(_quantile(mae_values, 0.75)),
            "reversion_pct": _format_pct_fraction(reversion, len(paired)),
            "continuation_pct": _format_pct_fraction(continuation, len(paired)),
            "chop_pct": _format_pct_fraction(chop, len(paired)),
        }
    return summary


def _cohort_management_rows(
    rows: list[dict[str, Any]],
    analog_rows: list[dict[str, Any]],
    query_row: dict[str, Any],
    direction: str,
    operator_policy: OperatorPolicy,
) -> list[dict[str, str]]:
    output: list[dict[str, str]] = []
    for name, label, kind, value in STATE_MANAGEMENT_EXIT_SPECS:
        query_target = _management_target_move(query_row, kind, value)
        query_entry = _safe_float(query_row.get("close"))
        if (
            query_target is None
            or query_entry is None
            or query_target < _management_target_floor(query_row, query_entry, operator_policy)
        ):
            continue
        evaluations = [
            _evaluate_management_spec(
                rows,
                int(row["_row_index"]),
                direction,
                kind,
                value,
                operator_policy,
            )
            for row in analog_rows
        ]
        evaluations = [item for item in evaluations if item is not None]
        if not evaluations:
            continue
        captured = [item for item in evaluations if item["captured"]]
        survived = [item for item in evaluations if item["survived"]]
        times = [item["time_to_target"] for item in captured if item["time_to_target"] is not None]
        output.append(
            {
                "exit_family": name,
                "description": label,
                "n": str(len(evaluations)),
                "survived_pct": _format_pct_fraction(len(survived), len(evaluations)),
                "captured_pct": _format_pct_fraction(len(captured), len(evaluations)),
                "median_time_to_target_min": _format_float(_median(times)),
                "median_target_move": _format_float(
                    _median([item["target_move"] for item in evaluations])
                ),
                "stop_reference": "symmetric adverse move",
                "median_stop_move": _format_float(
                    _median([item["stop_move"] for item in evaluations])
                ),
                "reward_risk": "1.0",
                "median_heat_30m": _format_float(_median([item["mae_30"] for item in evaluations])),
            }
        )
    return sorted(
        output,
        key=lambda row: (
            _parse_pct(row.get("survived_pct")) or -1.0,
            _parse_pct(row.get("captured_pct")) or -1.0,
        ),
        reverse=True,
    )


def _evaluate_management_spec(
    rows: list[dict[str, Any]],
    row_index: int,
    direction: str,
    kind: str,
    value: float,
    operator_policy: OperatorPolicy,
) -> dict[str, Any] | None:
    row = rows[row_index]
    entry = _safe_float(row.get("close"))
    if entry is None:
        return None
    target_move = _management_target_move(row, kind, value)
    if target_move is None or target_move <= 0:
        return None
    target_floor = _management_target_floor(row, entry, operator_policy)
    if target_move < target_floor:
        return None
    future = _future_rows_same_day(rows, row_index, 30)
    mfe_30, mae_30 = _mfe_mae_for_future(entry, future, direction)
    target_time = _time_to_move(entry, future, direction, target_move, favorable=True)
    adverse_time = _time_to_move(entry, future, direction, target_move, favorable=False)
    captured = target_time is not None
    survived = captured and (adverse_time is None or target_time <= adverse_time)
    return {
        "captured": captured,
        "survived": survived,
        "time_to_target": target_time,
        "target_move": target_move,
        "stop_move": target_move,
        "mae_30": mae_30 if mae_30 is not None else 0.0,
        "mfe_30": mfe_30 if mfe_30 is not None else 0.0,
    }


def _management_target_move(row: dict[str, Any], kind: str, value: float) -> float | None:
    entry = _safe_float(row.get("close"))
    if entry is None:
        return None
    if kind == "pct":
        return entry * value
    reference = _safe_float(row.get("opening_vwap_rth"))
    if reference is None:
        reference = _safe_float(row.get("opening_vwap"))
    if reference is None:
        return None
    return abs(entry - reference) * value


def _management_target_floor(
    row: dict[str, Any],
    entry: float,
    operator_policy: OperatorPolicy,
) -> float:
    atr = _safe_float(row.get("daily_rth_atr_14"))
    if atr is None:
        atr = _safe_float(row.get("daily_atr_14"))
    price_floor = entry * operator_policy.min_target_price_fraction
    if atr is None or atr <= 0:
        return price_floor
    return max(atr * operator_policy.min_target_atr_fraction, price_floor)


def _time_to_move(
    entry: float,
    future: list[dict[str, Any]],
    direction: str,
    move: float,
    *,
    favorable: bool,
) -> int | None:
    for minute, row in enumerate(future, start=1):
        high = _safe_float(row.get("high"))
        low = _safe_float(row.get("low"))
        if high is None or low is None:
            continue
        if direction == "long":
            hit = high >= entry + move if favorable else low <= entry - move
        else:
            hit = low <= entry - move if favorable else high >= entry + move
        if hit:
            return minute
    return None


def _state_snapshot_for_desk(row: dict[str, Any], direction: str) -> dict[str, str]:
    ts = _ensure_aware(row["timestamp"]).astimezone(NY)
    return {
        "timestamp_et": ts.isoformat(),
        "bias": direction,
        "close": _format_float(_safe_float(row.get("close"))),
        "opening_vwap": _format_float(_safe_float(row.get("opening_vwap_rth"))),
        "prior_close": _format_float(_safe_float(row.get("prior_rth_close"))),
        "daily_atr_14": _format_float(_safe_float(row.get("daily_rth_atr_14"))),
        "prior_close_atr": _format_float(
            _safe_float(row.get("atr_distance_from_prior_rth_close"))
        ),
        "bias_prior_close_atr": _format_float(_safe_float(row.get("bias_prior_close_atr"))),
        "vwap_distance": _format_pct(_safe_float(row.get("vwap_distance_pct"))),
        "bias_vwap_distance": _format_pct(_safe_float(row.get("bias_vwap_distance_pct"))),
        "velocity_5_atr": _format_float(_safe_float(row.get("bias_velocity_5_atr"))),
        "velocity_15_atr": _format_float(_safe_float(row.get("bias_velocity_15_atr"))),
        "market_pulse_stage": _display(row.get("market_pulse_stage")),
        "gap_state": _display(row.get("gap_state_rth_open")),
    }


def _cohort_verdict(
    outcome_summary: dict[str, Any],
    operator_policy: OperatorPolicy,
) -> tuple[str, str]:
    window_label = operator_policy.decision_window
    window = outcome_summary.get(window_label, {})
    n = _safe_int(window.get("n"))
    reversion = _parse_pct(window.get("reversion_pct"))
    continuation = _parse_pct(window.get("continuation_pct"))
    if n < operator_policy.min_forward_n:
        return "too_thin", (
            f"Only {n} analogs had usable {window_label}-minute forward data; "
            f"policy requires {operator_policy.min_forward_n}."
        )
    if reversion is None or continuation is None:
        return "too_thin", f"Not enough analogs had usable {window_label}-minute forward data."
    edge = reversion - continuation
    if abs(edge) < operator_policy.mixed_band:
        return "mixed_cohort", "Nearest analogs were split; no clean directional edge."
    if edge >= operator_policy.strong_reversion_edge:
        return "strong_reversion_lean", (
            f"Nearest analogs strongly favored reversion over the first {window_label} minutes."
        )
    if edge > 0:
        return "reversion_lean", (
            f"Nearest analogs leaned toward reversion over the first {window_label} minutes."
        )
    if edge <= -operator_policy.strong_continuation_edge:
        return "strong_continuation_risk", "Nearest analogs strongly favored continuation against the requested bias."
    return "continuation_lean", "Nearest analogs leaned against the requested reversion bias."


def _signal_rows_at_timestamp(
    adapter: PlaybookQueryAdapter,
    configs_by_id: dict[str, dict[str, Any]],
    bars: pl.DataFrame,
    *,
    query_utc: datetime,
    max_nearest_seconds: int,
) -> dict[str, dict[str, Any]]:
    enriched_by_features: dict[frozenset[str], pl.DataFrame] = {}
    signals_by_entry: dict[str, pl.DataFrame] = {}
    output: dict[str, dict[str, Any]] = {}
    for config_id, config in configs_by_id.items():
        strategy = build_strategy(adapter.strategy_name, config)
        features = frozenset(required_feature_union([strategy]))
        if features not in enriched_by_features:
            enriched_by_features[features] = PhysicsEngine().enrich_for_features(bars, set(features))
        entry_key = adapter.entry_signal_cache_key(config)
        if entry_key not in signals_by_entry:
            signals = strategy.generate_signals(enriched_by_features[features])
            signals_by_entry[entry_key] = MetricsCalculator().add_directional_forward_metrics(
                signals,
                snapshot_windows=(30, 60),
            )
        output[config_id] = _nearest_row(signals_by_entry[entry_key], query_utc, max_nearest_seconds)
    return output


def _nearest_row(df: pl.DataFrame, query_utc: datetime, max_nearest_seconds: int) -> dict[str, Any]:
    prepared = df.with_columns(et_timestamp_expr("timestamp").alias("_timestamp_et"))
    rows = prepared.to_dicts()
    if not rows:
        raise ValueError("No rows available after signal generation")
    best = min(
        rows,
        key=lambda row: abs((_ensure_aware(row["timestamp"]).astimezone(UTC) - query_utc).total_seconds()),
    )
    distance = abs((_ensure_aware(best["timestamp"]).astimezone(UTC) - query_utc).total_seconds())
    if distance > max_nearest_seconds:
        raise ValueError(
            f"Nearest bar is {int(distance)} seconds from query timestamp; "
            f"max allowed is {max_nearest_seconds}"
        )
    return best


def _representative_state_rows(
    adapter: PlaybookQueryAdapter,
    configs_by_id: dict[str, dict[str, Any]],
    signal_rows_by_config: dict[str, dict[str, Any]],
    direction: str,
) -> list[dict[str, str]]:
    snapshots: list[dict[str, str]] = []
    seen: set[tuple[str, str, str, str]] = set()
    for config_id, config in configs_by_id.items():
        row = signal_rows_by_config.get(config_id)
        if row is None:
            continue
        snapshot = adapter.state_snapshot(config=config, row=row, requested_direction=direction)
        key = (
            snapshot.get("stretch_source", ""),
            snapshot.get("stretch_threshold", ""),
            snapshot.get("stage_filter", ""),
            snapshot.get("gap_state_filter", ""),
        )
        if key in seen:
            continue
        seen.add(key)
        snapshots.append(snapshot)
        if len(snapshots) >= 12:
            break
    return snapshots


def _decorate_surface_row(
    row: dict[str, str],
    candidate_by_key: dict[tuple[str, str, str], dict[str, str]],
) -> dict[str, str]:
    candidate = candidate_by_key.get(_surface_key(row), {})
    decorated = dict(row)
    for key in ("rank", "candidate_type", "review_priority", "score", "trader_note"):
        decorated[key] = candidate.get(key, "")
    return decorated


def _candidate_rows(run_dir: Path, symbol: str, direction: str) -> list[dict[str, str]]:
    path = run_dir / "surface_review" / "candidate_regions.csv"
    if not path.exists():
        return []
    return [
        row
        for row in _read_csv(path)
        if row.get("symbol", "").upper() == symbol
        and row.get("direction", "").lower() == direction
    ]


def _entry_window_scope(configs: Any, query_ts: datetime) -> dict[str, str]:
    starts: list[time] = []
    ends: list[time] = []
    for config in configs:
        start = _parse_time_value(config.get("entry_window_start", ""))
        end = _parse_time_value(config.get("entry_window_end", ""))
        if start is not None:
            starts.append(start)
        if end is not None:
            ends.append(end)
    if not starts and ends:
        starts = [time(9, 30)]
    if not ends and starts:
        ends = [max(starts)]
    if not starts or not ends:
        return {
            "entry_window_start_et": "",
            "entry_window_end_et": "",
            "query_time_et": query_ts.time().isoformat(timespec="minutes"),
            "in_entry_window": "unknown",
        }
    window_start = min(starts)
    window_end = max(ends)
    query_time = query_ts.time()
    in_window = window_start <= query_time <= window_end
    return {
        "entry_window_start_et": window_start.isoformat(timespec="minutes"),
        "entry_window_end_et": window_end.isoformat(timespec="minutes"),
        "query_time_et": query_time.isoformat(timespec="minutes"),
        "in_entry_window": "yes" if in_window else "no",
    }


def _parse_time_value(raw: Any) -> time | None:
    if isinstance(raw, time):
        return raw
    if raw in (None, ""):
        return None
    try:
        return time.fromisoformat(str(raw))
    except ValueError:
        return None


def _query_verdict(
    active_rows: list[dict[str, str]],
    *,
    entry_window: dict[str, str],
) -> tuple[str, str]:
    if entry_window.get("in_entry_window") == "no":
        return (
            "out_of_window",
            "This timestamp is outside the searched entry window "
            f"({entry_window.get('entry_window_start_et')} to "
            f"{entry_window.get('entry_window_end_et')} ET); use this only as "
            "state/management context, not a fresh opening-playbook entry.",
        )
    if not active_rows:
        return (
            "wait_no_trigger",
            "No evaluated config produced an entry signal for the requested direction at this timestamp.",
        )
    best = active_rows[0]
    candidate_type = best.get("candidate_type", "")
    holdout_exp = _safe_float(best.get("holdout_expectancy_r"))
    holdout_win = _safe_float(best.get("holdout_win_rate"))
    if best.get("match_grade") == "favorable":
        return "good", "An active match also passed the strict favorable surface grade."
    if candidate_type == "clean_reversion_candidate":
        return "promising", "An active match aligns with a clean review candidate, but not strict proof."
    if holdout_exp is not None and holdout_exp >= 0.10 and holdout_win is not None and holdout_win >= 0.55:
        return "promising", "An active match has positive holdout effect and acceptable hit rate."
    if holdout_exp is not None and holdout_exp > 0:
        return "weak", "An active match has positive holdout expectancy but weak confirmation."
    return "skip", "The active match did not show positive historical support."


def _query_row_sort_key(row: dict[str, str]) -> tuple[int, float, float, int]:
    candidate_priority = {
        "clean_reversion_candidate": 0,
        "weak_positive_reversion": 1,
        "holdout_only_suspect": 2,
        "tail_payoff_review": 3,
        "positive_but_messy": 4,
    }.get(row.get("candidate_type", ""), 5)
    grade_priority = {"favorable": 0, "near_favorable": 1, "partial": 2, "outside": 3, "insufficient": 4}.get(
        row.get("match_grade", ""),
        5,
    )
    score = _safe_float(row.get("score"))
    holdout = _safe_float(row.get("holdout_expectancy_r"))
    sample = _safe_int(row.get("sample_count"))
    return (min(candidate_priority, grade_priority), -(score if score is not None else -999), -(holdout if holdout is not None else -999), -sample)


def _surface_key(row: dict[str, str]) -> tuple[str, str, str]:
    return (
        row.get("config_id", ""),
        row.get("symbol", ""),
        row.get("direction", ""),
    )


def _write_query_md(path: Path, payload: dict[str, Any], adapter: PlaybookQueryAdapter) -> None:
    if payload.get("mode") == "state-management":
        _write_state_management_md(path, payload)
        return
    lines = [
        "# Playbook Surface Query",
        "",
        f"- source run: `{payload['source_run']}`",
        f"- playbook: `{payload['playbook_id']}`",
        f"- mode: `{payload.get('mode', 'signal')}`",
        f"- question: `{payload['direction']} {payload['symbol']} at {payload['timestamp_et']}`",
        f"- verdict: `{payload['verdict']}`",
        f"- active matches: `{payload['active_match_count']}`",
        f"- reason: {payload['verdict_reason']}",
        f"- searched entry window: `{payload.get('entry_window', {}).get('entry_window_start_et', '')}"
        f" -> {payload.get('entry_window', {}).get('entry_window_end_et', '')} ET`",
        f"- query inside entry window: `{payload.get('entry_window', {}).get('in_entry_window', '')}`",
        "",
        "## Current State",
        "",
    ]
    snapshots = payload.get("state_snapshots", [])
    if not snapshots:
        lines.append("- No state snapshot was generated.")
    else:
        for snapshot in snapshots[:8]:
            lines.append(
                "- {stretch_source}>{stretch_threshold}: value={stretch_value}, raw={stretch_raw}, "
                "reference={reference_state}, stage={stage_actual} ({stage_filter}), "
                "gap={gap_state} ({gap_state_filter}), signal={signal}/{signal_direction}".format(
                    **snapshot
                )
            )
    lines.extend(["", "## Active Matches", ""])
    active_rows = payload.get("active_matches", [])
    if not active_rows:
        lines.append("- No active surface row matched the requested direction at this timestamp.")
    else:
        for row in active_rows[:8]:
            lines.append(_surface_row_line(row, adapter))
    management = payload.get("management_packet", {})
    lines.extend(["", "## Management Packet", ""])
    if management:
        for key in ("entry", "stop", "exit", "context"):
            lines.append(f"- {key}: {management.get(key, '')}")
    else:
        lines.append("- No management packet because there was no active matched entry.")
    lines.extend(["", "## Top Historical Candidates For This Bias", ""])
    for row in payload.get("top_historical_candidates", [])[:8]:
        lines.append(_surface_row_line(row, adapter))
    lines.extend(
        [
            "",
            "## Operator Read",
            "",
            "- `good` means active state plus strict surface support.",
            "- `promising` means active state plus useful historical support, but still chart-review territory.",
            "- `weak` means the timestamp fired, but the evidence is not strong enough to treat as a rule.",
            "- `wait_no_trigger` means the bias may be valid, but this timestamp did not satisfy the playbook entry trigger.",
            "- `out_of_window` means the timestamp is outside this playbook's searched entry window; use a different playbook or a position-management query.",
            "- `skip` means the active match is historically unsupported or negative.",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def _write_state_management_md(path: Path, payload: dict[str, Any]) -> None:
    cohort = payload.get("cohort", {})
    state = payload.get("current_state", {})
    summary = cohort.get("outcome_summary", {})
    similarity = payload.get("similarity_config", {})
    operator_policy = payload.get("operator_policy", {})
    policy_config = operator_policy.get("config", {}) if isinstance(operator_policy, dict) else {}
    management_policy = policy_config.get("management", {}) if isinstance(policy_config, dict) else {}
    min_target_atr_fraction = float(management_policy.get("min_target_atr_fraction", 0.10))
    min_target_price_fraction = float(management_policy.get("min_target_price_fraction", 0.0010))
    tail = cohort.get("similarity_tail", {})
    lines = [
        "# Playbook State Management Query",
        "",
        f"- source run: `{payload['source_run']}`",
        f"- playbook: `{payload['playbook_id']}`",
        f"- mode: `{payload.get('mode', '')}`",
        f"- question: `{payload['direction']} {payload['symbol']} at {payload['timestamp_et']}`",
        f"- desk read: `{payload['verdict']}`",
        f"- reason: {payload['verdict_reason']}",
        f"- analogs: `{cohort.get('analog_count', 0)}` of requested `{cohort.get('requested_count', 0)}`",
        f"- candidate analogs in scope: `{cohort.get('candidate_count', 0)}`",
        f"- confidence: `{cohort.get('confidence', '')}`",
        f"- median similarity: `{cohort.get('similarity_median', '')}`",
        f"- selected-tail similarity: `{tail.get('selected_last_similarity', '')}`; rank-200 similarity: `{tail.get('rank_200_similarity', '')}`",
        f"- searched entry window: `{payload.get('entry_window', {}).get('entry_window_start_et', '')}"
        f" -> {payload.get('entry_window', {}).get('entry_window_end_et', '')} ET`",
        "",
        "## Current State",
        "",
        f"- price: `{state.get('close', '')}`; RTH opening VWAP: `{state.get('opening_vwap', '')}`; prior RTH close: `{state.get('prior_close', '')}`",
        f"- bias-adjusted prior-RTH-close stretch: `{state.get('bias_prior_close_atr', '')} ATR`; bias-adjusted VWAP distance: `{state.get('bias_vwap_distance', '')}`",
        f"- MarketPulse: `{state.get('market_pulse_stage', '')}`; gap: `{state.get('gap_state', '')}`",
        f"- bias-adjusted velocity: `5m {state.get('velocity_5_atr', '')} ATR`, `15m {state.get('velocity_15_atr', '')} ATR`",
        "",
        "## Historical Analog Cohort",
        "",
        "| Window | n | Median MFE | Median MAE | P75 MFE | P75 MAE | Reversion | Continuation | Chop |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for window in STATE_FORWARD_WINDOWS:
        window_label = f"{window}m" if isinstance(window, int) else str(window).upper()
        row = summary.get(str(window), {})
        lines.append(
            f"| {window_label} | {row.get('n', '')} | {row.get('median_mfe', '')} | "
            f"{row.get('median_mae', '')} | {row.get('p75_mfe', '')} | "
            f"{row.get('p75_mae', '')} | {row.get('reversion_pct', '')} | "
            f"{row.get('continuation_pct', '')} | {row.get('chop_pct', '')} |"
        )
    lines.extend(
        [
            "",
            "## Management Menu",
            "",
            "| Exit family | Survived | Captured | Stop reference | Reward:risk | Median time | Median target | Median stop | Median heat 30m |",
            "| --- | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in cohort.get("management_rows", []):
        lines.append(
            f"| `{row.get('exit_family', '')}` | {row.get('survived_pct', '')} | "
            f"{row.get('captured_pct', '')} | {row.get('stop_reference', '')} | "
            f"{row.get('reward_risk', '')}:1 | {row.get('median_time_to_target_min', '')}m | "
            f"{row.get('median_target_move', '')} | {row.get('median_stop_move', '')} | "
            f"{row.get('median_heat_30m', '')} |"
        )
    lines.extend(
        [
            "",
            "## Nearest Analogs",
            "",
            "| Timestamp ET | Similarity | Stage | Gap | Bias ATR | Bias VWAP | 15m MFE | 15m MAE |",
            "| --- | ---: | --- | --- | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in cohort.get("analogs", [])[:12]:
        outcome = row.get("outcomes", {}).get("15", {})
        lines.append(
            f"| {row.get('timestamp_et', '')} | {row.get('similarity', '')} | "
            f"{row.get('stage', '')} | {row.get('gap_state', '')} | "
            f"{row.get('bias_prior_close_atr', '')} | {row.get('bias_vwap_distance_pct', '')} | "
            f"{outcome.get('mfe', '')} | {outcome.get('mae', '')} |"
        )
    lines.extend(
        [
            "",
            "## Operator Read",
            "",
            "- This mode does not ask whether a rule fired.",
            "- It asks what happened after the closest historical states for the same symbol and requested bias.",
            "- `survived` means the target was touched before a symmetric adverse move of the same size.",
            "- `captured` means the target was touched within 30 minutes, even if comparable heat appeared first.",
            (
                "- Management rows below the tradable target floor are omitted. The current floor is "
                f"`max({min_target_atr_fraction:g} * daily_rth_atr_14, "
                f"{min_target_price_fraction:.2%} * price)` so tiny VWAP retraces do not "
                "look like edge just because bar noise touched them first."
            ),
            "- This is underlying-thesis evidence only; options translation remains a separate layer.",
            "",
            "## Operator Policy",
            "",
            f"- policy_id: `{operator_policy.get('policy_id', '')}`",
            f"- policy_version: `{operator_policy.get('policy_version', '')}`",
            f"- source_path: `{operator_policy.get('source_path', '')}`",
            f"- rule_id: `{operator_policy.get('rule_id', '')}`",
            "",
            "## Similarity Recipe",
            "",
            f"- method: `{similarity.get('method', '')}`",
            f"- stage mismatch penalty: `{similarity.get('stage_mismatch_penalty', '')}`",
            f"- gap mismatch penalty: `{similarity.get('gap_mismatch_penalty', '')}`",
            f"- note: {similarity.get('note', '')}",
            "",
            "| Feature | Scale | Weight |",
            "| --- | ---: | ---: |",
        ]
    )
    for spec in similarity.get("features", []):
        lines.append(
            f"| `{spec.get('feature', '')}` | {spec.get('scale', '')} | {spec.get('weight', '')} |"
        )
    lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def _surface_row_line(row: dict[str, str], adapter: PlaybookQueryAdapter) -> str:
    candidate = adapter.candidate_language(row, row.get("candidate_type", ""))
    return (
        "- `{candidate}` {symbol} {direction} {extension_family}>{extension_bin} "
        "stage={stage_filter} gap={gap_state_filter} stop={stop_family} exit={exit_family} "
        "n={sample_count}, cal={calibration_expectancy_r}R, hold={holdout_expectancy_r}R, "
        "hold_win={holdout_win_rate}".format(candidate=candidate, **row)
    )


def _default_out_dir(
    run_dir: Path,
    symbol: str,
    direction: str,
    timestamp: datetime,
    *,
    mode: str = "signal",
) -> Path:
    slug_ts = timestamp.astimezone(NY).strftime("%Y%m%dT%H%M%S_ET")
    suffix = "" if mode == "signal" else f"_{mode.replace('-', '_')}"
    return run_dir / "surface_queries" / f"{symbol.lower()}_{direction}_{slug_ts}{suffix}"


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _normalize_direction(raw: str) -> str:
    direction = raw.strip().lower()
    if direction not in {"long", "short"}:
        raise ValueError("direction must be 'long' or 'short'")
    return direction


def _parse_timestamp(raw: str) -> datetime:
    value = raw.strip()
    zone = NY
    suffix_zones = {
        " America/New_York": NY,
        " ET": NY,
        " EST": NY,
        " EDT": NY,
        " America/Chicago": CT,
        " CT": CT,
        " CST": CT,
        " CDT": CT,
    }
    for suffix, suffix_zone in suffix_zones.items():
        if value.endswith(suffix):
            value = value[: -len(suffix)].strip()
            zone = suffix_zone
            break
    value = value.replace("Z", "+00:00")
    if re.fullmatch(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}(:\d{2})?", value):
        value = value.replace(" ", "T")
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=zone)
    return parsed


def _ensure_aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value


def _safe_float(raw: Any) -> float | None:
    if raw in (None, ""):
        return None
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return None
    if math.isnan(value) or math.isinf(value):
        return None
    return value


def _safe_int(raw: Any) -> int:
    value = _safe_float(raw)
    return int(value) if value is not None else 0


def _median(values: list[Any]) -> float | None:
    cleaned = [_safe_float(value) for value in values]
    cleaned = [value for value in cleaned if value is not None]
    if not cleaned:
        return None
    return float(statistics.median(cleaned))


def _quantile(values: list[Any], q: float) -> float | None:
    cleaned = sorted(value for value in (_safe_float(value) for value in values) if value is not None)
    if not cleaned:
        return None
    if len(cleaned) == 1:
        return float(cleaned[0])
    position = (len(cleaned) - 1) * q
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return float(cleaned[int(position)])
    weight = position - lower
    return float(cleaned[lower] * (1 - weight) + cleaned[upper] * weight)


def _format_float(value: float | None, *, digits: int = 4) -> str:
    if value is None:
        return ""
    return f"{value:.{digits}f}".rstrip("0").rstrip(".")


def _format_pct(value: float | None, *, digits: int = 2) -> str:
    if value is None:
        return ""
    return f"{value * 100:.{digits}f}%"


def _format_pct_fraction(numerator: int, denominator: int, *, digits: int = 1) -> str:
    if denominator <= 0:
        return ""
    return f"{(numerator / denominator) * 100:.{digits}f}%"


def _parse_pct(raw: Any) -> float | None:
    if raw in (None, ""):
        return None
    value = str(raw).strip()
    if value.endswith("%"):
        value = value[:-1]
    parsed = _safe_float(value)
    if parsed is None:
        return None
    return parsed / 100.0


def _display(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _volume_filter_label(config: dict[str, Any]) -> str:
    threshold = config.get("relative_volume_threshold")
    if threshold in (None, ""):
        return "no_filter"
    return f"rvol_gt_{float(threshold):g}"


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", required=True, type=Path)
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--direction", required=True, choices=["long", "short"])
    parser.add_argument("--timestamp", required=True, type=_parse_timestamp)
    parser.add_argument(
        "--mode",
        choices=["signal", "state-management"],
        default="state-management",
        help=(
            "state-management returns nearest historical analogs and empirical "
            "management rows; signal is a sparse rule-firing debug mode"
        ),
    )
    parser.add_argument("--out-dir", type=Path, default=None)
    parser.add_argument("--data-dir", type=Path, default=None)
    parser.add_argument("--max-nearest-seconds", type=int, default=90)
    parser.add_argument("--top-candidates", type=int, default=8)
    parser.add_argument("--analog-lookback-days", type=int, default=1825)
    parser.add_argument("--analog-count", type=int, default=75)
    parser.add_argument(
        "--operator-policy-config",
        type=Path,
        default=None,
        help="Optional YAML policy file for state-management desk reads and management filters.",
    )
    parser.add_argument(
        "--no-log",
        action="store_true",
        help="Write query artifacts without appending consultation_log.csv",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    result = query_playbook_surface(
        args.run_dir,
        symbol=args.symbol,
        direction=args.direction,
        timestamp=args.timestamp,
        mode=args.mode,
        out_dir=args.out_dir,
        data_dir=args.data_dir,
        max_nearest_seconds=args.max_nearest_seconds,
        top_candidates=args.top_candidates,
        analog_lookback_days=args.analog_lookback_days,
        analog_count=args.analog_count,
        write_log=not args.no_log,
        operator_policy_config=args.operator_policy_config,
    )
    print(f"QUERY_REVIEW={result.review_md}")
    print(f"QUERY_JSON={result.json_path}")
    print(f"VERDICT={result.verdict}")
    print(f"ACTIVE_MATCHES={result.active_matches}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
