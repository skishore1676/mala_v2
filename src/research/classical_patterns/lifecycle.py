"""Post-breakout lifecycle classification independent of trade fills."""

from __future__ import annotations

from datetime import date

import polars as pl

from .contracts import (
    BreakoutDirection,
    BreakoutOutcome,
    LifecycleEvent,
    LifecycleState,
    OutcomeResult,
    RectangleResearchConfig,
    RectangleCandidate,
    RectangleSignal,
)


def derive_lifecycle(
    signal: RectangleSignal,
    daily_bars: pl.DataFrame,
    config: RectangleResearchConfig,
) -> tuple[tuple[LifecycleEvent, ...], OutcomeResult]:
    """Classify one breakout using only sessions after its confirming close."""

    rows = daily_bars.sort("session_date").to_dicts()
    candidate = signal.candidate
    if candidate.breakout_index >= len(rows):
        raise ValueError(f"Signal {signal.signal_id} breakout index is outside daily bars.")

    events: list[LifecycleEvent] = [
        LifecycleEvent(
            signal_id=signal.signal_id,
            event_index=candidate.breakout_index,
            event_date=candidate.breakout_date,
            state=LifecycleState.BREAKOUT,
            price=candidate.breakout_close,
            detail="close_confirmed_breakout",
        )
    ]
    retested = False
    lfd_violated = False
    observed = 0
    terminal_limit = config.definition.maximum_lifecycle_sessions
    final_index = min(len(rows) - 1, candidate.breakout_index + terminal_limit)

    for index in range(candidate.breakout_index + 1, final_index + 1):
        row = rows[index]
        event_date = _as_date(row["session_date"])
        observed += 1
        objective_now, negation_now, retest_now, lfd_now = _thresholds(candidate, row)

        first_retest_same_bar = retest_now and not retested
        first_lfd_same_bar = lfd_now and not lfd_violated
        if objective_now and (
            negation_now or first_retest_same_bar or first_lfd_same_bar
        ):
            events.append(
                LifecycleEvent(
                    signal_id=signal.signal_id,
                    event_index=index,
                    event_date=event_date,
                    state=LifecycleState.UNRESOLVED,
                    price=None,
                    detail=_same_bar_detail(
                        negation=negation_now,
                        first_retest=first_retest_same_bar,
                        first_lfd=first_lfd_same_bar,
                    ),
                )
            )
            return tuple(events), OutcomeResult(
                signal_id=signal.signal_id,
                outcome=BreakoutOutcome.UNRESOLVED,
                terminal_date=event_date,
                sessions_observed=observed,
                boundary_retested=retested or retest_now,
                lfd_violated=lfd_violated or lfd_now,
                terminal_reason="same_bar_path_order_unknown",
            )

        if retest_now and not retested:
            retested = True
            events.append(
                LifecycleEvent(
                    signal_id=signal.signal_id,
                    event_index=index,
                    event_date=event_date,
                    state=LifecycleState.BOUNDARY_RETEST,
                    price=candidate.breakout_boundary,
                    detail="intraday_boundary_touch",
                )
            )
        if lfd_now and not lfd_violated:
            lfd_violated = True
            events.append(
                LifecycleEvent(
                    signal_id=signal.signal_id,
                    event_index=index,
                    event_date=event_date,
                    state=LifecycleState.LFD_VIOLATED,
                    price=candidate.base_stop,
                    detail="raw_lfd_level_touched",
                )
            )

        if negation_now:
            events.append(
                LifecycleEvent(
                    signal_id=signal.signal_id,
                    event_index=index,
                    event_date=event_date,
                    state=LifecycleState.NEGATED,
                    price=candidate.structural_negation,
                    detail="structural_negation_before_objective",
                )
            )
            return tuple(events), OutcomeResult(
                signal_id=signal.signal_id,
                outcome=BreakoutOutcome.TYPE_4,
                terminal_date=event_date,
                sessions_observed=observed,
                boundary_retested=retested,
                lfd_violated=lfd_violated,
                terminal_reason="structural_negation",
            )

        if objective_now:
            outcome = (
                BreakoutOutcome.TYPE_3
                if lfd_violated
                else BreakoutOutcome.TYPE_2
                if retested
                else BreakoutOutcome.TYPE_1
            )
            events.append(
                LifecycleEvent(
                    signal_id=signal.signal_id,
                    event_index=index,
                    event_date=event_date,
                    state=LifecycleState.OBJECTIVE_HIT,
                    price=candidate.objective,
                    detail=f"measured_objective:{outcome.value}",
                )
            )
            return tuple(events), OutcomeResult(
                signal_id=signal.signal_id,
                outcome=outcome,
                terminal_date=event_date,
                sessions_observed=observed,
                boundary_retested=retested,
                lfd_violated=lfd_violated,
                terminal_reason="measured_objective",
            )

    reached_horizon = observed >= terminal_limit
    state = LifecycleState.EXPIRED if reached_horizon else LifecycleState.CENSORED
    reason = "lifecycle_horizon" if reached_horizon else "data_end"
    terminal_date = (
        _as_date(rows[final_index]["session_date"])
        if final_index >= candidate.breakout_index
        else candidate.breakout_date
    )
    events.append(
        LifecycleEvent(
            signal_id=signal.signal_id,
            event_index=final_index,
            event_date=terminal_date,
            state=state,
            price=None,
            detail=reason,
        )
    )
    return tuple(events), OutcomeResult(
        signal_id=signal.signal_id,
        outcome=BreakoutOutcome.CENSORED,
        terminal_date=terminal_date,
        sessions_observed=observed,
        boundary_retested=retested,
        lfd_violated=lfd_violated,
        terminal_reason=reason,
    )


def _thresholds(
    candidate: RectangleCandidate, row: dict[str, object]
) -> tuple[bool, bool, bool, bool]:
    high = float(row["high"])
    low = float(row["low"])
    if candidate.direction is BreakoutDirection.LONG:
        return (
            high >= candidate.objective,
            low <= candidate.structural_negation,
            low <= candidate.breakout_boundary,
            low <= candidate.base_stop,
        )
    return (
        low <= candidate.objective,
        high >= candidate.structural_negation,
        high >= candidate.breakout_boundary,
        high >= candidate.base_stop,
    )


def _same_bar_detail(*, negation: bool, first_retest: bool, first_lfd: bool) -> str:
    thresholds: list[str] = ["objective"]
    if negation:
        thresholds.append("negation")
    if first_lfd:
        thresholds.append("first_lfd_violation")
    elif first_retest:
        thresholds.append("first_boundary_retest")
    return "same_bar:" + "+".join(thresholds)


def _as_date(value: object) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))
