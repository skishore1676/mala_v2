"""Daily Range Expansion analogue for deterministic rectangle breakouts.

This is deliberately not the minute-level option exit profile. It carries the
profile's payoff shape onto daily underlying bars while keeping the rectangle
experiment's Last Full Day initial risk and twenty-session horizon.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import polars as pl

from src.research.classical_patterns.contracts import (
    BreakoutDirection,
    RectangleResearchConfig,
    RectangleSignal,
)


@dataclass(frozen=True, slots=True)
class RangeExpansionTradeResult:
    signal_id: str
    variant_id: str
    direction: BreakoutDirection
    status: str
    entry_date: date | None
    entry_price: float | None
    stop_price: float
    target_1_price: float | None
    target_2_price: float | None
    target_1_date: date | None
    exit_date: date | None
    exit_price: float | None
    exit_reason: str
    bars_held: int
    gross_pnl: float | None
    net_pnl: float | None
    net_return: float | None
    net_r: float | None
    mfe: float | None
    mae: float | None
    target_1_quantity: float


def simulate_daily_range_expansion_trade(
    signal: RectangleSignal,
    daily_bars: pl.DataFrame,
    *,
    stop_buffer_atr: float,
    config: RectangleResearchConfig,
    target_1_r: float = 1.0,
    target_2_r: float = 2.0,
    target_1_quantity: float = 0.40,
    giveback_arm_r: float = 1.0,
    giveback_retrace_fraction: float = 0.75,
) -> RangeExpansionTradeResult:
    """Simulate the frozen daily Range Expansion exit analogue.

    Priority is conservative on daily bars: opening gaps, the stop active at
    the start of the bar, T1, T2, prior-bar high-water giveback, then the
    twenty-session horizon. A breakeven stop armed by an intraday T1 becomes
    active on the next bar because daily OHLC cannot resolve the post-T1 path.
    """

    if stop_buffer_atr < 0:
        raise ValueError("stop_buffer_atr must be non-negative.")
    if not 0.0 < target_1_quantity < 1.0:
        raise ValueError("target_1_quantity must be strictly between zero and one.")
    if not 0.0 <= giveback_retrace_fraction <= 1.0:
        raise ValueError("giveback_retrace_fraction must be between zero and one.")

    candidate = signal.candidate
    variant_id = f"range_expansion_lfd_buffer_{stop_buffer_atr:.2f}atr".replace(".", "p")
    rows = daily_bars.sort("session_date").to_dicts()
    long = candidate.direction is BreakoutDirection.LONG
    stop = (
        candidate.base_stop - stop_buffer_atr * candidate.atr
        if long
        else candidate.base_stop + stop_buffer_atr * candidate.atr
    )

    if (long and stop <= candidate.structural_negation) or (
        not long and stop >= candidate.structural_negation
    ):
        return _no_trade(signal, variant_id, stop, "stop_not_inside_structural_negation")

    entry_index = candidate.breakout_index + 1
    if entry_index >= len(rows):
        return _no_trade(signal, variant_id, stop, "next_session_missing", status="no_fill")
    entry_row = rows[entry_index]
    raw_open = float(entry_row["open"])
    if (long and raw_open <= stop) or (not long and raw_open >= stop):
        return _no_trade(signal, variant_id, stop, "entry_open_through_stop")

    entry_price = _adverse_fill(
        raw_open,
        candidate.direction,
        entering=True,
        bps=config.execution.slippage_bps_each_side,
    )
    initial_risk = (entry_price - stop) if long else (stop - entry_price)
    if initial_risk <= 0:
        return _no_trade(signal, variant_id, stop, "nonpositive_effective_risk")

    sign = 1.0 if long else -1.0
    target_1 = entry_price + sign * target_1_r * initial_risk
    target_2 = entry_price + sign * target_2_r * initial_risk
    max_index = min(
        len(rows) - 1,
        entry_index + config.definition.maximum_trade_sessions - 1,
    )
    best_price = entry_price
    target_1_fill: float | None = None
    target_1_date: date | None = None
    mfe = 0.0
    mae = 0.0

    for index in range(entry_index, max_index + 1):
        row = rows[index]
        row_date = _as_date(row["session_date"])
        open_price = float(row["open"])
        high = float(row["high"])
        low = float(row["low"])
        close = float(row["close"])
        active_stop = entry_price if target_1_fill is not None else stop

        if index > entry_index:
            if (long and open_price <= active_stop) or (not long and open_price >= active_stop):
                reason = "gap_breakeven_stop" if target_1_fill is not None else "gap_stop"
                return _closed(
                    signal, variant_id, stop, target_1, target_2, target_1_fill,
                    target_1_date, target_1_quantity, entry_row, entry_price, row,
                    _adverse_fill(open_price, candidate.direction, entering=False,
                                  bps=config.execution.slippage_bps_each_side),
                    reason, index - entry_index + 1, initial_risk, mfe, mae, config,
                )

            if target_1_fill is None and ((long and open_price >= target_1) or (not long and open_price <= target_1)):
                target_1_fill = _adverse_fill(
                    target_1, candidate.direction, entering=False,
                    bps=config.execution.slippage_bps_each_side,
                )
                target_1_date = row_date
                if (long and open_price >= target_2) or (not long and open_price <= target_2):
                    return _closed(
                        signal, variant_id, stop, target_1, target_2, target_1_fill,
                        target_1_date, target_1_quantity, entry_row, entry_price, row,
                        _adverse_fill(target_2, candidate.direction, entering=False,
                                      bps=config.execution.slippage_bps_each_side),
                        "gap_target_2_capped", index - entry_index + 1, initial_risk,
                        mfe, mae, config,
                    )
            elif target_1_fill is not None and (
                (long and open_price >= target_2) or (not long and open_price <= target_2)
            ):
                return _closed(
                    signal, variant_id, stop, target_1, target_2, target_1_fill,
                    target_1_date, target_1_quantity, entry_row, entry_price, row,
                    _adverse_fill(target_2, candidate.direction, entering=False,
                                  bps=config.execution.slippage_bps_each_side),
                    "gap_target_2_capped", index - entry_index + 1, initial_risk,
                    mfe, mae, config,
                )

        if long:
            mfe = max(mfe, high - entry_price)
            mae = max(mae, entry_price - low)
        else:
            mfe = max(mfe, entry_price - low)
            mae = max(mae, high - entry_price)

        active_stop = entry_price if target_1_fill is not None else stop
        stop_touched = low <= active_stop if long else high >= active_stop
        if stop_touched:
            reason = "breakeven_stop" if target_1_fill is not None else "stop"
            return _closed(
                signal, variant_id, stop, target_1, target_2, target_1_fill,
                target_1_date, target_1_quantity, entry_row, entry_price, row,
                _adverse_fill(active_stop, candidate.direction, entering=False,
                              bps=config.execution.slippage_bps_each_side),
                reason, index - entry_index + 1, initial_risk, mfe, mae, config,
            )

        if target_1_fill is None:
            target_1_touched = high >= target_1 if long else low <= target_1
            if target_1_touched:
                target_1_fill = _adverse_fill(
                    target_1, candidate.direction, entering=False,
                    bps=config.execution.slippage_bps_each_side,
                )
                target_1_date = row_date

        target_2_touched = high >= target_2 if long else low <= target_2
        if target_1_fill is not None and target_2_touched:
            return _closed(
                signal, variant_id, stop, target_1, target_2, target_1_fill,
                target_1_date, target_1_quantity, entry_row, entry_price, row,
                _adverse_fill(target_2, candidate.direction, entering=False,
                              bps=config.execution.slippage_bps_each_side),
                "target_2", index - entry_index + 1, initial_risk, mfe, mae, config,
            )

        prior_best_move = (best_price - entry_price) if long else (entry_price - best_price)
        prior_best_r = prior_best_move / initial_risk
        if prior_best_r >= giveback_arm_r:
            floor_r = prior_best_r * (1.0 - giveback_retrace_fraction)
            current_r = ((close - entry_price) if long else (entry_price - close)) / initial_risk
            if current_r <= floor_r:
                return _closed(
                    signal, variant_id, stop, target_1, target_2, target_1_fill,
                    target_1_date, target_1_quantity, entry_row, entry_price, row,
                    _adverse_fill(close, candidate.direction, entering=False,
                                  bps=config.execution.slippage_bps_each_side),
                    "loose_high_water_giveback", index - entry_index + 1,
                    initial_risk, mfe, mae, config,
                )

        best_price = max(best_price, high) if long else min(best_price, low)

    reached_horizon = max_index - entry_index + 1 >= config.definition.maximum_trade_sessions
    if reached_horizon:
        row = rows[max_index]
        return _closed(
            signal, variant_id, stop, target_1, target_2, target_1_fill,
            target_1_date, target_1_quantity, entry_row, entry_price, row,
            _adverse_fill(float(row["close"]), candidate.direction, entering=False,
                          bps=config.execution.slippage_bps_each_side),
            "trade_horizon", max_index - entry_index + 1, initial_risk,
            mfe, mae, config,
        )

    return RangeExpansionTradeResult(
        signal_id=signal.signal_id,
        variant_id=variant_id,
        direction=candidate.direction,
        status="censored",
        entry_date=_as_date(entry_row["session_date"]),
        entry_price=entry_price,
        stop_price=stop,
        target_1_price=target_1,
        target_2_price=target_2,
        target_1_date=target_1_date,
        exit_date=None,
        exit_price=None,
        exit_reason="data_end",
        bars_held=max_index - entry_index + 1,
        gross_pnl=None,
        net_pnl=None,
        net_return=None,
        net_r=None,
        mfe=mfe,
        mae=mae,
        target_1_quantity=target_1_quantity,
    )


def _closed(
    signal: RectangleSignal,
    variant_id: str,
    stop: float,
    target_1: float,
    target_2: float,
    target_1_fill: float | None,
    target_1_date: date | None,
    target_1_quantity: float,
    entry_row: dict[str, object],
    entry_price: float,
    exit_row: dict[str, object],
    runner_exit_price: float,
    exit_reason: str,
    bars_held: int,
    initial_risk: float,
    mfe: float,
    mae: float,
    config: RectangleResearchConfig,
) -> RangeExpansionTradeResult:
    exit_price = runner_exit_price
    if target_1_fill is not None:
        exit_price = (
            target_1_quantity * target_1_fill
            + (1.0 - target_1_quantity) * runner_exit_price
        )
    direction_multiplier = 1.0 if signal.candidate.direction is BreakoutDirection.LONG else -1.0
    gross = direction_multiplier * (exit_price - entry_price)
    explicit_cost = entry_price * config.execution.round_trip_cost_bps / 10_000.0
    net = gross - explicit_cost
    return RangeExpansionTradeResult(
        signal_id=signal.signal_id,
        variant_id=variant_id,
        direction=signal.candidate.direction,
        status="closed",
        entry_date=_as_date(entry_row["session_date"]),
        entry_price=entry_price,
        stop_price=stop,
        target_1_price=target_1,
        target_2_price=target_2,
        target_1_date=target_1_date,
        exit_date=_as_date(exit_row["session_date"]),
        exit_price=exit_price,
        exit_reason=exit_reason,
        bars_held=bars_held,
        gross_pnl=gross,
        net_pnl=net,
        net_return=net / entry_price,
        net_r=net / initial_risk,
        mfe=mfe,
        mae=mae,
        target_1_quantity=target_1_quantity,
    )


def _no_trade(
    signal: RectangleSignal,
    variant_id: str,
    stop: float,
    reason: str,
    *,
    status: str = "no_trade",
) -> RangeExpansionTradeResult:
    return RangeExpansionTradeResult(
        signal_id=signal.signal_id,
        variant_id=variant_id,
        direction=signal.candidate.direction,
        status=status,
        entry_date=None,
        entry_price=None,
        stop_price=stop,
        target_1_price=None,
        target_2_price=None,
        target_1_date=None,
        exit_date=None,
        exit_price=None,
        exit_reason=reason,
        bars_held=0,
        gross_pnl=None,
        net_pnl=None,
        net_return=None,
        net_r=None,
        mfe=None,
        mae=None,
        target_1_quantity=0.40,
    )


def _adverse_fill(
    price: float,
    direction: BreakoutDirection,
    *,
    entering: bool,
    bps: float,
) -> float:
    fraction = bps / 10_000.0
    if direction is BreakoutDirection.LONG:
        return price * (1.0 + fraction if entering else 1.0 - fraction)
    return price * (1.0 - fraction if entering else 1.0 + fraction)


def _as_date(value: object) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))
