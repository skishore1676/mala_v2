"""Dedicated multi-session simulator for daily rectangle breakouts."""

from __future__ import annotations

from datetime import date

import polars as pl

from src.research.classical_patterns.contracts import (
    BreakoutDirection,
    RectangleResearchConfig,
    RectangleSignal,
    TradeResult,
)


def simulate_rectangle_trade(
    signal: RectangleSignal,
    daily_bars: pl.DataFrame,
    *,
    stop_buffer_atr: float,
    config: RectangleResearchConfig,
) -> TradeResult:
    """Simulate one next-open, zero-reentry underlying trade variant."""

    if stop_buffer_atr < 0:
        raise ValueError("stop_buffer_atr must be non-negative.")
    candidate = signal.candidate
    variant_id = f"lfd_buffer_{stop_buffer_atr:.2f}atr".replace(".", "p")
    rows = daily_bars.sort("session_date").to_dicts()
    stop = (
        candidate.base_stop - stop_buffer_atr * candidate.atr
        if candidate.direction is BreakoutDirection.LONG
        else candidate.base_stop + stop_buffer_atr * candidate.atr
    )
    target = candidate.objective

    # `RectangleCandidate.tradeable` is a deprecated V1 artifact field retained
    # only for schema compatibility. Economic eligibility is derived here from
    # bars plus frozen config; no human/model review field may suppress a signal.
    if candidate.direction is BreakoutDirection.LONG and stop <= candidate.structural_negation:
        return _no_trade(signal, variant_id, stop, target, "stop_not_inside_structural_negation")
    if candidate.direction is BreakoutDirection.SHORT and stop >= candidate.structural_negation:
        return _no_trade(signal, variant_id, stop, target, "stop_not_inside_structural_negation")

    entry_index = candidate.breakout_index + 1
    if entry_index >= len(rows):
        return _no_trade(signal, variant_id, stop, target, "next_session_missing", status="no_fill")
    entry_row = rows[entry_index]
    raw_open = float(entry_row["open"])
    if candidate.direction is BreakoutDirection.LONG:
        if raw_open <= stop:
            return _no_trade(signal, variant_id, stop, target, "entry_open_through_stop")
        if raw_open >= target:
            return _no_trade(signal, variant_id, stop, target, "entry_open_at_or_beyond_objective")
    else:
        if raw_open >= stop:
            return _no_trade(signal, variant_id, stop, target, "entry_open_through_stop")
        if raw_open <= target:
            return _no_trade(signal, variant_id, stop, target, "entry_open_at_or_beyond_objective")

    entry_price = _adverse_fill(
        raw_open,
        candidate.direction,
        entering=True,
        bps=config.execution.slippage_bps_each_side,
    )
    initial_risk = abs(entry_price - stop)
    reward = (
        target - entry_price
        if candidate.direction is BreakoutDirection.LONG
        else entry_price - target
    )
    if initial_risk <= 0:
        return _no_trade(signal, variant_id, stop, target, "nonpositive_effective_risk")
    if reward <= 0:
        return _no_trade(signal, variant_id, stop, target, "nonpositive_effective_reward")

    mfe = 0.0
    mae = 0.0
    max_index = min(
        len(rows) - 1,
        entry_index + config.definition.maximum_trade_sessions - 1,
    )
    for index in range(entry_index, max_index + 1):
        row = rows[index]
        high = float(row["high"])
        low = float(row["low"])
        open_price = float(row["open"])

        # Opening gaps are observable before the rest of the daily range.
        if index > entry_index:
            if candidate.direction is BreakoutDirection.LONG and open_price <= stop:
                return _closed_trade(
                    signal,
                    variant_id,
                    stop,
                    target,
                    entry_row,
                    entry_price,
                    row,
                    _adverse_fill(open_price, candidate.direction, entering=False, bps=config.execution.slippage_bps_each_side),
                    "gap_stop",
                    index - entry_index + 1,
                    initial_risk,
                    mfe,
                    mae,
                    config,
                )
            if candidate.direction is BreakoutDirection.SHORT and open_price >= stop:
                return _closed_trade(
                    signal,
                    variant_id,
                    stop,
                    target,
                    entry_row,
                    entry_price,
                    row,
                    _adverse_fill(open_price, candidate.direction, entering=False, bps=config.execution.slippage_bps_each_side),
                    "gap_stop",
                    index - entry_index + 1,
                    initial_risk,
                    mfe,
                    mae,
                    config,
                )
            if candidate.direction is BreakoutDirection.LONG and open_price >= target:
                return _closed_trade(
                    signal,
                    variant_id,
                    stop,
                    target,
                    entry_row,
                    entry_price,
                    row,
                    _adverse_fill(target, candidate.direction, entering=False, bps=config.execution.slippage_bps_each_side),
                    "gap_objective_capped",
                    index - entry_index + 1,
                    initial_risk,
                    mfe,
                    mae,
                    config,
                )
            if candidate.direction is BreakoutDirection.SHORT and open_price <= target:
                return _closed_trade(
                    signal,
                    variant_id,
                    stop,
                    target,
                    entry_row,
                    entry_price,
                    row,
                    _adverse_fill(target, candidate.direction, entering=False, bps=config.execution.slippage_bps_each_side),
                    "gap_objective_capped",
                    index - entry_index + 1,
                    initial_risk,
                    mfe,
                    mae,
                    config,
                )

        # Only incorporate the daily range if the position survived that
        # session's opening print. A gap exit cannot borrow later intraday
        # highs/lows for MFE or MAE.
        if candidate.direction is BreakoutDirection.LONG:
            mfe = max(mfe, high - entry_price)
            mae = max(mae, entry_price - low)
        else:
            mfe = max(mfe, entry_price - low)
            mae = max(mae, high - entry_price)

        stop_touched = low <= stop if candidate.direction is BreakoutDirection.LONG else high >= stop
        target_touched = high >= target if candidate.direction is BreakoutDirection.LONG else low <= target
        if stop_touched:  # Deliberately pessimistic when both occur in one bar.
            return _closed_trade(
                signal,
                variant_id,
                stop,
                target,
                entry_row,
                entry_price,
                row,
                _adverse_fill(stop, candidate.direction, entering=False, bps=config.execution.slippage_bps_each_side),
                "stop" if not target_touched else "same_bar_stop_first",
                index - entry_index + 1,
                initial_risk,
                mfe,
                mae,
                config,
            )
        if target_touched:
            return _closed_trade(
                signal,
                variant_id,
                stop,
                target,
                entry_row,
                entry_price,
                row,
                _adverse_fill(target, candidate.direction, entering=False, bps=config.execution.slippage_bps_each_side),
                "objective",
                index - entry_index + 1,
                initial_risk,
                mfe,
                mae,
                config,
            )

    reached_horizon = max_index - entry_index + 1 >= config.definition.maximum_trade_sessions
    if reached_horizon:
        row = rows[max_index]
        exit_price = _adverse_fill(
            float(row["close"]),
            candidate.direction,
            entering=False,
            bps=config.execution.slippage_bps_each_side,
        )
        return _closed_trade(
            signal,
            variant_id,
            stop,
            target,
            entry_row,
            entry_price,
            row,
            exit_price,
            "trade_horizon",
            max_index - entry_index + 1,
            initial_risk,
            mfe,
            mae,
            config,
        )

    return TradeResult(
        signal_id=signal.signal_id,
        variant_id=variant_id,
        direction=candidate.direction,
        status="censored",
        entry_date=_as_date(entry_row["session_date"]),
        entry_price=entry_price,
        stop_price=stop,
        target_price=target,
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
    )


def _closed_trade(
    signal: RectangleSignal,
    variant_id: str,
    stop: float,
    target: float,
    entry_row: dict[str, object],
    entry_price: float,
    exit_row: dict[str, object],
    exit_price: float,
    exit_reason: str,
    bars_held: int,
    initial_risk: float,
    mfe: float,
    mae: float,
    config: RectangleResearchConfig,
) -> TradeResult:
    direction_multiplier = 1.0 if signal.candidate.direction is BreakoutDirection.LONG else -1.0
    gross = direction_multiplier * (exit_price - entry_price)
    explicit_cost = entry_price * config.execution.round_trip_cost_bps / 10_000.0
    net = gross - explicit_cost
    return TradeResult(
        signal_id=signal.signal_id,
        variant_id=variant_id,
        direction=signal.candidate.direction,
        status="closed",
        entry_date=_as_date(entry_row["session_date"]),
        entry_price=entry_price,
        stop_price=stop,
        target_price=target,
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
    )


def _no_trade(
    signal: RectangleSignal,
    variant_id: str,
    stop: float,
    target: float,
    reason: str,
    *,
    status: str = "no_trade",
) -> TradeResult:
    return TradeResult(
        signal_id=signal.signal_id,
        variant_id=variant_id,
        direction=signal.candidate.direction,
        status=status,
        entry_date=None,
        entry_price=None,
        stop_price=stop,
        target_price=target,
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
