"""Playbook-level trade path simulation owned by Oracle."""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import time
from typing import Any


@dataclass(frozen=True, slots=True)
class PlaybookTradePath:
    entry: float
    stop: float
    risk: float
    target: float | None
    exit_price: float
    exit_reason: str
    pnl_r: float
    max_favorable_excursion_r: float
    max_adverse_excursion_r: float


def simulate_intraday_reversion_event(
    *,
    row: dict[str, Any],
    future_rows: list[dict[str, Any]],
    direction: str,
    stop_family: str,
    exit_family: str,
    trade_date_key: str = "_playbook_trade_date",
    bar_time_key: str = "_playbook_bar_time",
    time_stop: time = time(11, 30),
) -> PlaybookTradePath | None:
    """Simulate the evaluated playbook trade path from one entry row."""

    entry = _float(row.get("close"))
    if entry is None:
        return None
    stop = _stop_price(row, direction, stop_family)
    if stop is None:
        return None
    risk = (entry - stop) if direction == "long" else (stop - entry)
    if risk <= 0:
        return None

    target = _target_price(row, direction, entry, risk, exit_family)
    trade_date = row.get(trade_date_key)
    exit_price = entry
    exit_reason = "eod"
    max_favorable = 0.0
    max_adverse = 0.0

    for future in future_rows:
        if future.get(trade_date_key) != trade_date:
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
        bar_time = future.get(bar_time_key)
        if exit_family == "market_pulse_flip" and _market_pulse_flip_exit(
            direction,
            future.get("market_pulse_stage"),
        ):
            exit_reason = "market_pulse_flip"
            break
        if exit_family == "time_stop" and isinstance(bar_time, time) and bar_time >= time_stop:
            exit_reason = "time_stop"
            break

    pnl = (exit_price - entry) if direction == "long" else (entry - exit_price)
    return PlaybookTradePath(
        entry=entry,
        stop=stop,
        risk=risk,
        target=target,
        exit_price=exit_price,
        exit_reason=exit_reason,
        pnl_r=pnl / risk,
        max_favorable_excursion_r=max_favorable / risk,
        max_adverse_excursion_r=max_adverse / risk,
    )


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
        target = _vwap_reference(row)
    elif exit_family == "partial_retrace_50":
        reference = _float(row.get("playbook_reference_price")) or _vwap_reference(row)
        if reference is None:
            return None
        target = entry + ((reference - entry) * 0.5)
    elif exit_family == "market_pulse_flip":
        return None
    else:
        return None
    if target is None:
        return None
    if direction == "long" and target <= entry:
        return None
    if direction == "short" and target >= entry:
        return None
    return target


def _vwap_reference(row: dict[str, Any]) -> float | None:
    return _float(row.get("opening_vwap_rth")) or _float(row.get("opening_vwap"))


def _market_pulse_flip_exit(direction: str, stage: object) -> bool:
    normalized = str(stage or "").strip().lower()
    return (direction == "long" and normalized == "bearish") or (
        direction == "short" and normalized == "bullish"
    )


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
