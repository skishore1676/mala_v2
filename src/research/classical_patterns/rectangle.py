"""Causal daily horizontal-rectangle enumeration."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
import hashlib
from math import ceil
from statistics import median
from typing import Iterable, Sequence

import numpy as np
import polars as pl

from .contracts import (
    BreakoutDirection,
    EnumerationRecord,
    RectangleCandidate,
    RectangleResearchConfig,
    RectangleSignal,
)


@dataclass(frozen=True, slots=True)
class EnumerationResult:
    records: tuple[EnumerationRecord, ...]
    candidates: tuple[RectangleCandidate, ...]
    signals: tuple[RectangleSignal, ...]

    @property
    def scanned_window_count(self) -> int:
        return len(self.records)

    @property
    def valid_non_signal_count(self) -> int:
        return sum(record.status == "valid_no_breakout" for record in self.records)

    @property
    def rejected_window_count(self) -> int:
        return sum(record.status == "rejected" for record in self.records)

    @property
    def cluster_duplicate_count(self) -> int:
        return len(self.candidates) - len(self.signals)


@dataclass(frozen=True, slots=True)
class _Pivot:
    index: int
    price: float


@dataclass(frozen=True, slots=True)
class _BoundaryCluster:
    level: float
    indices: tuple[int, ...]
    prices: tuple[float, ...]
    dispersion_atr: float


@dataclass(frozen=True, slots=True)
class _Geometry:
    upper: _BoundaryCluster
    lower: _BoundaryCluster
    upper_edge: float
    lower_edge: float
    tolerance: float
    height: float
    height_atr: float
    drift_fraction: float
    alternations: int
    containment: float
    dispersion: float
    latest_touch_age: int

    def score(self) -> tuple[float | int | tuple[int, ...], ...]:
        return (
            -min(len(self.upper.indices), len(self.lower.indices)),
            -self.alternations,
            -self.containment,
            self.dispersion,
            self.latest_touch_age,
            self.upper.indices,
            self.lower.indices,
        )


def enumerate_rectangles(
    daily_bars: pl.DataFrame,
    config: RectangleResearchConfig,
) -> EnumerationResult:
    """Enumerate every causal window and retain one signal per breakout cluster."""

    required = {
        "session_date",
        "visible_at",
        "symbol",
        "open",
        "high",
        "low",
        "close",
        "volume",
    }
    missing = required - set(daily_bars.columns)
    if missing:
        raise ValueError(f"Daily bars are missing columns: {sorted(missing)}")
    if daily_bars.is_empty():
        return EnumerationResult(records=(), candidates=(), signals=())
    symbols = daily_bars.get_column("symbol").unique().to_list()
    if len(symbols) != 1:
        raise ValueError("enumerate_rectangles expects exactly one symbol per call.")

    rows = daily_bars.sort("session_date").to_dicts()
    symbol = str(symbols[0])
    definition = config.definition
    true_ranges = _true_ranges(rows)
    records: list[EnumerationRecord] = []
    candidates: list[RectangleCandidate] = []

    for breakout_index in range(len(rows)):
        for lookback in definition.lookback_sessions:
            start = breakout_index - lookback
            record_id = f"{symbol}:{breakout_index}:{lookback}"
            if start < 0 or breakout_index < definition.atr_lookback_sessions + 1:
                continue
            atr_values = true_ranges[
                breakout_index - definition.atr_lookback_sessions : breakout_index
            ]
            if len(atr_values) != definition.atr_lookback_sessions or any(
                not np.isfinite(value) or value <= 0 for value in atr_values
            ):
                records.append(
                    _record(rows, symbol, breakout_index, lookback, record_id, "rejected", "invalid_atr")
                )
                continue
            atr = float(np.mean(atr_values))
            window_indices = tuple(range(start, breakout_index))
            geometries, reason = _qualifying_geometries(
                rows,
                window_indices=window_indices,
                atr=atr,
                config=config,
            )
            if not geometries:
                records.append(
                    _record(rows, symbol, breakout_index, lookback, record_id, "rejected", reason)
                )
                continue

            geometry = min(geometries, key=_Geometry.score)
            breakout_close = float(rows[breakout_index]["close"])
            buffer = definition.breakout_buffer_atr * atr
            direction: BreakoutDirection | None = None
            if breakout_close > geometry.upper_edge + buffer:
                direction = BreakoutDirection.LONG
            elif breakout_close < geometry.lower_edge - buffer:
                direction = BreakoutDirection.SHORT

            if direction is None:
                records.append(
                    _record(
                        rows,
                        symbol,
                        breakout_index,
                        lookback,
                        record_id,
                        "valid_no_breakout",
                        "no_close_confirmed_breakout",
                        qualifying_geometry_count=len(geometries),
                    )
                )
                continue

            candidate = _build_candidate(
                rows,
                symbol=symbol,
                breakout_index=breakout_index,
                lookback=lookback,
                atr=atr,
                geometry=geometry,
                direction=direction,
                config=config,
            )
            if candidate is None:
                records.append(
                    _record(
                        rows,
                        symbol,
                        breakout_index,
                        lookback,
                        record_id,
                        "rejected",
                        "no_lfd",
                        direction,
                        qualifying_geometry_count=len(geometries),
                    )
                )
                continue
            candidates.append(candidate)
            records.append(
                _record(
                    rows,
                    symbol,
                    breakout_index,
                    lookback,
                    record_id,
                    "candidate",
                    "close_confirmed_breakout",
                    direction,
                    candidate.candidate_id,
                    len(geometries),
                )
            )

    signals = _select_representatives(candidates)
    return EnumerationResult(
        records=tuple(records),
        candidates=tuple(sorted(candidates, key=lambda item: item.candidate_id)),
        signals=tuple(signals),
    )


def _qualifying_geometries(
    rows: Sequence[dict[str, object]],
    *,
    window_indices: tuple[int, ...],
    atr: float,
    config: RectangleResearchConfig,
) -> tuple[list[_Geometry], str]:
    definition = config.definition
    span = definition.pivot_span_sessions
    highs = _confirmed_pivots(rows, window_indices, "high", span, high=True)
    lows = _confirmed_pivots(rows, window_indices, "low", span, high=False)
    tolerance = definition.boundary_tolerance_atr * atr
    upper_clusters = _cluster_pivots(
        highs,
        tolerance=tolerance,
        atr=atr,
        minimum_touches=definition.minimum_boundary_touches,
        minimum_separation=definition.minimum_touch_separation_sessions,
    )
    lower_clusters = _cluster_pivots(
        lows,
        tolerance=tolerance,
        atr=atr,
        minimum_touches=definition.minimum_boundary_touches,
        minimum_separation=definition.minimum_touch_separation_sessions,
    )
    if not upper_clusters or not lower_clusters:
        return [], "insufficient_confirmed_boundary_touches"

    closes = np.array([float(rows[index]["close"]) for index in window_indices], dtype=float)
    center = float(np.median(closes))
    max_touch_age = ceil(len(window_indices) * definition.maximum_latest_touch_age_fraction)
    end_index = window_indices[-1]
    geometries: list[_Geometry] = []

    for upper in upper_clusters:
        for lower in lower_clusters:
            if not (upper.level > center > lower.level):
                continue
            height = upper.level - lower.level
            if height <= 2 * tolerance:
                continue
            height_atr = height / atr
            if not definition.minimum_height_atr <= height_atr <= definition.maximum_height_atr:
                continue
            if end_index - max(upper.indices) > max_touch_age:
                continue
            if end_index - max(lower.indices) > max_touch_age:
                continue

            alternations = _touch_alternations(upper.indices, lower.indices)
            if alternations < definition.minimum_touch_alternations:
                continue
            containment = float(np.mean((closes >= lower.level) & (closes <= upper.level)))
            if containment < definition.minimum_center_close_containment:
                continue
            upper_edge = upper.level + tolerance
            lower_edge = lower.level - tolerance
            if np.any((closes < lower_edge) | (closes > upper_edge)):
                continue
            drift_fraction = _ols_drift_fraction(closes, height)
            if drift_fraction > definition.maximum_close_drift_height_fraction:
                continue
            geometries.append(
                _Geometry(
                    upper=upper,
                    lower=lower,
                    upper_edge=upper_edge,
                    lower_edge=lower_edge,
                    tolerance=tolerance,
                    height=height,
                    height_atr=height_atr,
                    drift_fraction=drift_fraction,
                    alternations=alternations,
                    containment=containment,
                    dispersion=(upper.dispersion_atr + lower.dispersion_atr) / 2.0,
                    latest_touch_age=max(
                        end_index - max(upper.indices),
                        end_index - max(lower.indices),
                    ),
                )
            )
    if not geometries:
        return [], "no_geometry_passed_frozen_semantics"
    return geometries, "qualified"


def _confirmed_pivots(
    rows: Sequence[dict[str, object]],
    window_indices: tuple[int, ...],
    column: str,
    span: int,
    *,
    high: bool,
) -> tuple[_Pivot, ...]:
    start, end = window_indices[0], window_indices[-1]
    pivots: list[_Pivot] = []
    for index in range(start + span, end - span + 1):
        value = float(rows[index][column])
        neighborhood = [float(rows[other][column]) for other in range(index - span, index + span + 1)]
        extreme = max(neighborhood) if high else min(neighborhood)
        if value != extreme:
            continue
        # Equal-price plateaus resolve to the earliest confirmed bar.
        if any(float(rows[other][column]) == value for other in range(index - span, index)):
            continue
        pivots.append(_Pivot(index=index, price=value))
    return tuple(pivots)


def _cluster_pivots(
    pivots: Sequence[_Pivot],
    *,
    tolerance: float,
    atr: float,
    minimum_touches: int,
    minimum_separation: int,
) -> tuple[_BoundaryCluster, ...]:
    clusters: dict[tuple[int, ...], _BoundaryCluster] = {}
    for seed in pivots:
        initial = [pivot for pivot in pivots if abs(pivot.price - seed.price) <= tolerance]
        if len(initial) < minimum_touches:
            continue
        level = float(median(pivot.price for pivot in initial))
        members = [pivot for pivot in pivots if abs(pivot.price - level) <= tolerance]
        separated = _separate_touches(members, minimum_separation)
        if len(separated) < minimum_touches:
            continue
        member_key = tuple(pivot.index for pivot in separated)
        prices = tuple(pivot.price for pivot in separated)
        level = float(median(prices))
        dispersion = float(np.mean([abs(price - level) for price in prices]) / atr)
        clusters[member_key] = _BoundaryCluster(
            level=level,
            indices=member_key,
            prices=prices,
            dispersion_atr=dispersion,
        )
    return tuple(sorted(clusters.values(), key=lambda item: (item.level, item.indices)))


def _separate_touches(pivots: Sequence[_Pivot], minimum_separation: int) -> tuple[_Pivot, ...]:
    selected: list[_Pivot] = []
    for pivot in sorted(pivots, key=lambda item: item.index):
        if not selected or pivot.index - selected[-1].index >= minimum_separation:
            selected.append(pivot)
    return tuple(selected)


def _touch_alternations(upper: Iterable[int], lower: Iterable[int]) -> int:
    ordered = sorted([(index, "upper") for index in upper] + [(index, "lower") for index in lower])
    collapsed: list[str] = []
    for _, side in ordered:
        if not collapsed or collapsed[-1] != side:
            collapsed.append(side)
    return max(0, len(collapsed) - 1)


def _ols_drift_fraction(closes: np.ndarray, height: float) -> float:
    if len(closes) < 2 or height <= 0:
        return float("inf")
    x = np.arange(len(closes), dtype=float)
    slope = float(np.polyfit(x, closes, 1)[0])
    return abs(slope * (len(closes) - 1)) / height


def _build_candidate(
    rows: Sequence[dict[str, object]],
    *,
    symbol: str,
    breakout_index: int,
    lookback: int,
    atr: float,
    geometry: _Geometry,
    direction: BreakoutDirection,
    config: RectangleResearchConfig,
) -> RectangleCandidate | None:
    start = breakout_index - lookback
    lfd_index: int | None = None
    for index in range(breakout_index - 1, start - 1, -1):
        high = float(rows[index]["high"])
        low = float(rows[index]["low"])
        if low >= geometry.lower_edge and high <= geometry.upper_edge:
            lfd_index = index
            break
    if lfd_index is None:
        return None

    if direction is BreakoutDirection.LONG:
        breakout_boundary = geometry.upper_edge
        base_stop = float(rows[lfd_index]["low"])
        negation = geometry.lower_edge - config.definition.negation_buffer_atr * atr
        objective = geometry.upper.level + geometry.height * config.definition.objective_height_multiple
    else:
        breakout_boundary = geometry.lower_edge
        base_stop = float(rows[lfd_index]["high"])
        negation = geometry.upper_edge + config.definition.negation_buffer_atr * atr
        objective = geometry.lower.level - geometry.height * config.definition.objective_height_multiple

    breakout_row = rows[breakout_index]
    codes = _breakout_bar_diagnostic_codes(
        breakout_row,
        direction=direction,
        lfd_level=base_stop,
        negation=negation,
        objective=objective,
    )
    candidate_id = _candidate_id(
        symbol,
        breakout_row["session_date"],
        direction.value,
        lookback,
        geometry.upper.level,
        geometry.lower.level,
        geometry.upper.indices,
        geometry.lower.indices,
    )
    return RectangleCandidate(
        candidate_id=candidate_id,
        symbol=symbol,
        direction=direction,
        breakout_index=breakout_index,
        breakout_date=_as_date(breakout_row["session_date"]),
        breakout_time=_as_datetime(breakout_row["visible_at"]),
        breakout_close=float(breakout_row["close"]),
        pattern_start_date=_as_date(rows[start]["session_date"]),
        pattern_end_date=_as_date(rows[breakout_index - 1]["session_date"]),
        lookback_sessions=lookback,
        upper_boundary=geometry.upper.level,
        lower_boundary=geometry.lower.level,
        upper_edge=geometry.upper_edge,
        lower_edge=geometry.lower_edge,
        boundary_tolerance=geometry.tolerance,
        breakout_boundary=breakout_boundary,
        atr=atr,
        height=geometry.height,
        height_atr=geometry.height_atr,
        close_drift_fraction=geometry.drift_fraction,
        touch_alternations=geometry.alternations,
        center_close_containment=geometry.containment,
        boundary_dispersion=geometry.dispersion,
        latest_touch_age_sessions=geometry.latest_touch_age,
        upper_touch_indices=geometry.upper.indices,
        lower_touch_indices=geometry.lower.indices,
        lfd_index=lfd_index,
        lfd_date=_as_date(rows[lfd_index]["session_date"]),
        lfd_high=float(rows[lfd_index]["high"]),
        lfd_low=float(rows[lfd_index]["low"]),
        base_stop=base_stop,
        structural_negation=negation,
        objective=objective,
        split=config.splits.label(_as_date(breakout_row["session_date"])),
        tradeable=True,
        breakout_bar_diagnostic_codes=tuple(codes),
    )


def _breakout_bar_diagnostic_codes(
    row: dict[str, object],
    *,
    direction: BreakoutDirection,
    lfd_level: float,
    negation: float,
    objective: float,
) -> list[str]:
    high = float(row["high"])
    low = float(row["low"])
    codes: list[str] = []
    if direction is BreakoutDirection.LONG:
        if low <= lfd_level:
            codes.append("breakout_bar_spans_lfd")
        if low <= negation:
            codes.append("breakout_bar_spans_negation")
        if high >= objective:
            codes.append("objective_touched_before_confirmation")
    else:
        if high >= lfd_level:
            codes.append("breakout_bar_spans_lfd")
        if high >= negation:
            codes.append("breakout_bar_spans_negation")
        if low <= objective:
            codes.append("objective_touched_before_confirmation")
    return codes


def _select_representatives(candidates: Sequence[RectangleCandidate]) -> list[RectangleSignal]:
    grouped: dict[tuple[str, int, BreakoutDirection], list[RectangleCandidate]] = {}
    for candidate in candidates:
        grouped.setdefault(
            (candidate.symbol, candidate.breakout_index, candidate.direction), []
        ).append(candidate)
    signals: list[RectangleSignal] = []
    for key in sorted(grouped, key=lambda item: (item[0], item[1], item[2].value)):
        cohort = grouped[key]
        selected = min(cohort, key=RectangleCandidate.representative_key)
        signal_id = f"signal:{selected.candidate_id}"
        signals.append(
            RectangleSignal(
                signal_id=signal_id,
                candidate=selected,
                cluster_candidate_count=len(cohort),
            )
        )
    return signals


def _true_ranges(rows: Sequence[dict[str, object]]) -> list[float]:
    values: list[float] = []
    previous_close: float | None = None
    for row in rows:
        high = float(row["high"])
        low = float(row["low"])
        if previous_close is None:
            value = high - low
        else:
            value = max(high - low, abs(high - previous_close), abs(low - previous_close))
        values.append(float(value))
        previous_close = float(row["close"])
    return values


def _record(
    rows: Sequence[dict[str, object]],
    symbol: str,
    breakout_index: int,
    lookback: int,
    record_id: str,
    status: str,
    reason: str,
    direction: BreakoutDirection | None = None,
    candidate_id: str | None = None,
    qualifying_geometry_count: int = 0,
) -> EnumerationRecord:
    return EnumerationRecord(
        record_id=record_id,
        symbol=symbol,
        breakout_index=breakout_index,
        breakout_date=_as_date(rows[breakout_index]["session_date"]),
        lookback_sessions=lookback,
        direction=direction,
        status=status,
        reason=reason,
        candidate_id=candidate_id,
        qualifying_geometry_count=qualifying_geometry_count,
    )


def _candidate_id(*parts: object) -> str:
    encoded = "|".join(str(part) for part in parts).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()[:20]


def _as_date(value: object) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def _as_datetime(value: object) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value))
