"""Bhiksha signal timing and realized EV audit.

This report joins Bhiksha runtime events back to the compiled active-plan
metadata that came from Mala evidence. It is intentionally operational: it
answers whether Bhiksha's strategy path fired and entered coherently, then
compares realized option outcomes with the evidence expectancy attached to the
deployment at runtime.
"""

from __future__ import annotations

import csv
import inspect
import json
import math
import sqlite3
from collections import Counter, defaultdict
from contextlib import closing
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import polars as pl

from src.chronos.storage import LocalStorage
from src.config import DATA_DIR
from src.newton.engine import PhysicsEngine
from src.newton.transforms import acceleration_column_name, jerk_column_name, velocity_column_name
from src.strategy.factory import build_strategy


ET = ZoneInfo("America/New_York")
STRATEGY_NAME_BY_KEY = {
    "elastic_band_reversion": "Elastic Band Reversion",
    "jerk_pivot_momentum": "Jerk-Pivot Momentum (tight)",
    "market_impulse": "Market Impulse (Cross & Reclaim)",
    "opening_drive_classifier": "Opening Drive Classifier",
}
LEGACY_BHIKSHA_WARMUP_BUFFER_DAYS = 3
DEFAULT_REPLAY_WARMUP_TRADING_DAYS = 7


@dataclass(slots=True, frozen=True)
class BhikshaSignalEvArtifacts:
    report_md: Path
    trade_csv: Path
    deployment_csv: Path
    signal_csv: Path
    counterfactual_csv: Path
    counterfactual_summary_csv: Path


def build_bhiksha_signal_ev_report(
    *,
    db_path: str | Path,
    out_dir: str | Path,
    lookback_days: int = 21,
    max_signal_lag_minutes: int = 5,
    same_bar_replay: bool = False,
    counterfactual_replay: bool = False,
    data_dir: str | Path | None = None,
    replay_warmup_days: int = 0,
) -> BhikshaSignalEvArtifacts:
    """Build a signal/EV scorecard from Bhiksha's SQLite runtime database."""

    db = Path(db_path)
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    events = _load_events(db)
    trade_sessions = _load_trade_sessions(db)
    max_seen_at = _max_seen_at(events, trade_sessions)
    start_at = (max_seen_at - timedelta(days=lookback_days)) if max_seen_at else None
    scoped_events = [event for event in events if start_at is None or _parse_dt(event["created_at"]) >= start_at]
    scoped_trade_sessions = [
        trade
        for trade in trade_sessions
        if start_at is None or _best_trade_time(trade) is None or _best_trade_time(trade) >= start_at
    ]

    timeline = _build_deployment_timeline(events)
    replay_cache = _ReplayCache(Path(data_dir) if data_dir else DATA_DIR) if same_bar_replay or counterfactual_replay else None
    signal_rows = _signal_rows(
        scoped_events,
        timeline,
        replay_cache=replay_cache,
        replay_warmup_days=replay_warmup_days,
    )
    evaluation_rows = _signal_evaluation_rows(scoped_events)
    trade_rows = _trade_rows(
        scoped_events,
        scoped_trade_sessions,
        timeline,
        signal_rows,
        max_signal_lag=timedelta(minutes=max_signal_lag_minutes),
    )
    counterfactual_rows = _counterfactual_rows(
        timeline,
        signal_rows,
        replay_cache=replay_cache if counterfactual_replay else None,
        replay_warmup_days=replay_warmup_days,
        start_at=start_at,
        evaluation_rows=evaluation_rows,
    )
    counterfactual_summary_rows = _counterfactual_summary_rows(counterfactual_rows, trade_rows)
    deployment_rows = _deployment_rows(signal_rows, trade_rows)

    signal_csv = out / "bhiksha_signal_events.csv"
    trade_csv = out / "bhiksha_signal_ev_trades.csv"
    deployment_csv = out / "bhiksha_signal_ev_deployments.csv"
    counterfactual_csv = out / "bhiksha_signal_counterfactual.csv"
    counterfactual_summary_csv = out / "bhiksha_signal_counterfactual_summary.csv"
    report_md = out / "BHIKSHA_SIGNAL_EV_REPORT.md"

    _write_csv(signal_csv, signal_rows)
    _write_csv(trade_csv, trade_rows)
    _write_csv(deployment_csv, deployment_rows)
    _write_csv(counterfactual_csv, counterfactual_rows)
    _write_csv(counterfactual_summary_csv, counterfactual_summary_rows)
    report_md.write_text(
        _render_report(
            db_path=db,
            lookback_days=lookback_days,
            max_seen_at=max_seen_at,
            same_bar_replay=same_bar_replay,
            counterfactual_replay=counterfactual_replay,
            signal_rows=signal_rows,
            trade_rows=trade_rows,
            deployment_rows=deployment_rows,
            counterfactual_rows=counterfactual_rows,
            counterfactual_summary_rows=counterfactual_summary_rows,
            signal_csv=signal_csv,
            trade_csv=trade_csv,
            deployment_csv=deployment_csv,
            counterfactual_csv=counterfactual_csv,
            counterfactual_summary_csv=counterfactual_summary_csv,
        ),
        encoding="utf-8",
    )

    return BhikshaSignalEvArtifacts(
        report_md=report_md,
        trade_csv=trade_csv,
        deployment_csv=deployment_csv,
        signal_csv=signal_csv,
        counterfactual_csv=counterfactual_csv,
        counterfactual_summary_csv=counterfactual_summary_csv,
    )


def _load_events(db_path: Path) -> list[dict[str, Any]]:
    if not db_path.exists() or db_path.stat().st_size == 0:
        return []
    with closing(sqlite3.connect(db_path)) as conn:
        rows = conn.execute("SELECT created_at, event_type, payload FROM events ORDER BY id").fetchall()
    events: list[dict[str, Any]] = []
    for created_at, event_type, payload_text in rows:
        try:
            payload = json.loads(payload_text)
        except (TypeError, json.JSONDecodeError):
            payload = {}
        events.append(
            {
                "created_at": str(created_at),
                "event_type": str(event_type),
                "payload": payload if isinstance(payload, dict) else {},
            }
        )
    return events


def _load_trade_sessions(db_path: Path) -> list[dict[str, Any]]:
    if not db_path.exists() or db_path.stat().st_size == 0:
        return []
    with closing(sqlite3.connect(db_path)) as conn:
        table = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='trade_sessions'"
        ).fetchone()
        if table is None:
            return []
        columns = [str(row[1]) for row in conn.execute("PRAGMA table_info(trade_sessions)").fetchall()]
        rows = conn.execute("SELECT * FROM trade_sessions").fetchall()
    return [dict(zip(columns, row, strict=False)) for row in rows]


def _build_deployment_timeline(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    snapshots: list[dict[str, Any]] = []
    for event in events:
        if event["event_type"] != "startup_config":
            continue
        payload = event["payload"]
        active_plan = payload.get("active_plan") if isinstance(payload.get("active_plan"), dict) else {}
        deployments = active_plan.get("deployments") or payload.get("deployments") or []
        if not isinstance(deployments, list):
            continue
        deployment_map = {
            str(deployment.get("deployment_id")): deployment
            for deployment in deployments
            if isinstance(deployment, dict) and deployment.get("deployment_id")
        }
        snapshots.append(
            {
                "created_at": _parse_dt(event["created_at"]),
                "active_plan_id": active_plan.get("active_plan_id", ""),
                "trading_date": active_plan.get("trading_date", ""),
                "app": payload.get("app") if isinstance(payload.get("app"), dict) else {},
                "warmup": payload.get("warmup") if isinstance(payload.get("warmup"), dict) else {},
                "deployments": deployment_map,
            }
        )
    snapshots.sort(key=lambda row: row["created_at"])
    return snapshots


def _deployment_for_time(
    timeline: list[dict[str, Any]],
    deployment_id: str,
    when: datetime | None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    fallback: tuple[dict[str, Any], dict[str, Any]] | None = None
    for snapshot in timeline:
        if when is not None and snapshot["created_at"] > when:
            break
        deployment = snapshot["deployments"].get(deployment_id)
        if deployment is not None:
            fallback = snapshot, deployment
    if fallback is None:
        return {}, {}
    return fallback


def _resolve_replay_warmup_trading_days(
    *,
    snapshot: dict[str, Any],
    deployment_id: str,
    deployment: dict[str, Any],
    override_days: int,
) -> int:
    if override_days > 0:
        return override_days
    warmup = snapshot.get("warmup") if isinstance(snapshot.get("warmup"), dict) else {}
    by_deployment = warmup.get("by_deployment") if isinstance(warmup.get("by_deployment"), dict) else {}
    deployment_days = _as_int_or_none(by_deployment.get(deployment_id))
    if deployment_days is not None and deployment_days > 0:
        return deployment_days
    symbol = str(deployment.get("symbol") or _metadata(deployment).get("symbol") or "")
    by_symbol = warmup.get("by_symbol") if isinstance(warmup.get("by_symbol"), dict) else {}
    symbol_days = _as_int_or_none(by_symbol.get(symbol))
    if symbol_days is not None and symbol_days > 0:
        return symbol_days
    effective_days = _as_int_or_none(warmup.get("effective_trading_days"))
    if effective_days is not None and effective_days > 0:
        return effective_days
    app = snapshot.get("app") if isinstance(snapshot.get("app"), dict) else {}
    base_days = _as_int_or_none(app.get("warmup_trading_days"))
    if base_days is not None:
        return max(base_days, 0) + LEGACY_BHIKSHA_WARMUP_BUFFER_DAYS
    return DEFAULT_REPLAY_WARMUP_TRADING_DAYS


def _replay_start_date(anchor: date, trading_days: int) -> date:
    return _trading_days_ago(anchor, max(int(trading_days), 1))


def _trading_days_ago(anchor: date, trading_days: int) -> date:
    candidate = anchor
    if not _is_trading_day(candidate):
        candidate = _previous_trading_day(candidate)
    for _ in range(max(trading_days - 1, 0)):
        candidate = _previous_trading_day(candidate)
    return candidate


def _previous_trading_day(value: date) -> date:
    candidate = value - timedelta(days=1)
    while not _is_trading_day(candidate):
        candidate -= timedelta(days=1)
    return candidate


def _is_trading_day(value: date) -> bool:
    return value.weekday() < 5 and value not in _nyse_holidays(value.year)


def _nyse_holidays(year: int) -> set[date]:
    first_monday_may = _nth_weekday_of_month(year, 5, 0, 1)
    last_monday_may = first_monday_may + timedelta(weeks=4)
    if last_monday_may.month != 5:
        last_monday_may = first_monday_may + timedelta(weeks=3)
    return {
        _observe(date(year, 1, 1)),
        _nth_weekday_of_month(year, 1, 0, 3),
        _nth_weekday_of_month(year, 2, 0, 3),
        _easter(year) - timedelta(days=2),
        last_monday_may,
        _observe(date(year, 6, 19)),
        _observe(date(year, 7, 4)),
        _nth_weekday_of_month(year, 9, 0, 1),
        _nth_weekday_of_month(year, 11, 3, 4),
        _observe(date(year, 12, 25)),
    }


def _nth_weekday_of_month(year: int, month: int, weekday: int, n: int) -> date:
    first_day = date(year, month, 1)
    offset = (weekday - first_day.weekday()) % 7
    return first_day + timedelta(days=offset, weeks=n - 1)


def _observe(value: date) -> date:
    if value.weekday() == 5:
        return value - timedelta(days=1)
    if value.weekday() == 6:
        return value + timedelta(days=1)
    return value


def _easter(year: int) -> date:
    a = year % 19
    b, c = divmod(year, 100)
    d, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return date(year, month, day)


def _as_int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _signal_rows(
    events: list[dict[str, Any]],
    timeline: list[dict[str, Any]],
    *,
    replay_cache: "_ReplayCache | None",
    replay_warmup_days: int,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for event in events:
        if event["event_type"] != "signal_decision":
            continue
        payload = event["payload"]
        if not payload.get("signal"):
            continue
        deployment_id = str(payload.get("deployment_id", ""))
        event_at = _parse_dt(event["created_at"])
        signal_at = _parse_dt(str(payload.get("timestamp") or event["created_at"]))
        snapshot, deployment = _deployment_for_time(timeline, deployment_id, event_at)
        metadata = _metadata(deployment)
        expected_direction = _expected_direction(deployment, metadata)
        direction = str(payload.get("direction", ""))
        signal_window = _signal_window(metadata)
        replay = _same_bar_replay(
            payload=payload,
            snapshot=snapshot,
            deployment=deployment,
            signal_at=signal_at,
            replay_cache=replay_cache,
            replay_warmup_days=replay_warmup_days,
        )
        row = {
            "created_at": event_at.isoformat(),
            "signal_at": signal_at.isoformat(),
            "signal_at_et": _format_et(signal_at),
            "deployment_id": deployment_id,
            "symbol": payload.get("symbol", ""),
            "direction": direction,
            "catalog_key": metadata.get("catalog_key", ""),
            "strategy_key": _nested_get(deployment, ["strategy", "key"], ""),
            "strategy_name": _mala_evidence(metadata).get("strategy_name", ""),
            "active_plan_id": snapshot.get("active_plan_id", "") if snapshot else "",
            "authorization_mode": metadata.get("authorization_mode", ""),
            "expected_direction": expected_direction,
            "direction_match": _yes_no(not expected_direction or direction == expected_direction),
            "signal_window_et": signal_window,
            "inside_signal_window": _yes_no(_inside_window(signal_at, signal_window)),
            "reason": ",".join(str(reason) for reason in payload.get("reason") or []),
            "features": _compact_json(payload.get("features") or {}),
            "concordance_status": _signal_concordance_status(direction, expected_direction, signal_at, signal_window, metadata),
            **replay,
        }
        rows.append(row)
    rows.sort(key=lambda row: row["signal_at"])
    return rows


def _signal_evaluation_rows(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return all Bhiksha runtime signal evaluations when the newer event exists."""

    rows: list[dict[str, Any]] = []
    for event in events:
        if event["event_type"] != "signal_evaluation":
            continue
        payload = event["payload"]
        deployment_id = str(payload.get("deployment_id", ""))
        evaluated_at = _parse_dt_or_none(str(payload.get("timestamp") or event["created_at"]))
        if not deployment_id or evaluated_at is None:
            continue
        rows.append(
            {
                "created_at": _parse_dt(event["created_at"]).isoformat(),
                "evaluated_at": evaluated_at.isoformat(),
                "deployment_id": deployment_id,
                "symbol": payload.get("symbol", ""),
                "signal": _yes_no(bool(payload.get("signal"))),
                "direction": payload.get("direction", ""),
                "reason": ",".join(str(reason) for reason in payload.get("reason") or []),
                "features": _compact_json(payload.get("features") or {}),
            }
        )
    rows.sort(key=lambda row: row["evaluated_at"])
    return rows


def _trade_rows(
    events: list[dict[str, Any]],
    trade_sessions: list[dict[str, Any]],
    timeline: list[dict[str, Any]],
    signal_rows: list[dict[str, Any]],
    *,
    max_signal_lag: timedelta,
) -> list[dict[str, Any]]:
    session_by_trade_id = {str(row.get("trade_id")): row for row in trade_sessions if row.get("trade_id")}
    signals_by_deployment: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in signal_rows:
        signals_by_deployment[str(row.get("deployment_id", ""))].append(row)

    rows: list[dict[str, Any]] = []
    for event in events:
        if event["event_type"] != "trade_plan":
            continue
        payload = event["payload"]
        event_at = _parse_dt(event["created_at"])
        deployment_id = str(payload.get("deployment_id", ""))
        trade_id = str(payload.get("trade_id", ""))
        session = session_by_trade_id.get(trade_id, {})
        entry_at = (
            _parse_dt_or_none(str(session.get("entry_timestamp") or ""))
            or _parse_dt_or_none(str(payload.get("entry_timestamp") or ""))
            or event_at
        )
        signal = _nearest_signal(signals_by_deployment.get(deployment_id, []), entry_at, max_signal_lag=max_signal_lag)
        snapshot, deployment = _deployment_for_time(timeline, deployment_id, event_at)
        metadata = _metadata(deployment)
        evidence = _mala_evidence(metadata)
        thesis_metrics = evidence.get("thesis_exit_metrics") if isinstance(evidence.get("thesis_exit_metrics"), dict) else {}
        entry_price = _first_float(session.get("entry_price"), payload.get("estimated_entry_price"), payload.get("entry_price"))
        exit_price = _first_float(session.get("exit_price"), payload.get("exit_price"))
        quantity = int(_first_float(session.get("quantity"), payload.get("quantity"), 0) or 0)
        stop_loss_pct = _first_float(
            _nested_get(deployment, ["exit", "stop_loss_pct"], None),
            _nested_get(deployment, ["risk", "stop_loss_pct"], None),
            0.35,
        ) or 0.35
        realized_pnl = _realized_pnl(entry_price, exit_price, quantity)
        option_return_pct = _option_return_pct(entry_price, exit_price)
        realized_stop_r = (option_return_pct / stop_loss_pct) if option_return_pct is not None and stop_loss_pct else None
        base_expectancy = _first_float(metadata.get("expectancy"))
        thesis_expectancy = _first_float(thesis_metrics.get("expectancy"))
        expected_r = _first_float(thesis_expectancy, base_expectancy)
        signal_at = _parse_dt_or_none(str(signal.get("signal_at", ""))) if signal else None
        lag_seconds = int((entry_at - signal_at).total_seconds()) if signal_at else None
        row = {
            "trade_id": trade_id,
            "deployment_id": deployment_id,
            "symbol": payload.get("symbol") or session.get("symbol", ""),
            "direction": payload.get("direction", ""),
            "option_symbol": payload.get("option_symbol") or session.get("option_symbol", ""),
            "quantity": quantity,
            "status": session.get("status", "planned_only"),
            "entry_at": entry_at.isoformat(),
            "entry_at_et": _format_et(entry_at),
            "signal_at": signal.get("signal_at", "") if signal else "",
            "signal_at_et": signal.get("signal_at_et", "") if signal else "",
            "signal_to_entry_seconds": "" if lag_seconds is None else lag_seconds,
            "signal_match_status": _signal_match_status(signal, lag_seconds, max_signal_lag),
            "runtime_signal_reason": signal.get("reason", "") if signal else "",
            "mala_same_bar_replay_status": signal.get("mala_same_bar_replay_status", "") if signal else "",
            "mala_same_bar_replay_signal": signal.get("mala_same_bar_replay_signal", "") if signal else "",
            "mala_same_bar_replay_direction": signal.get("mala_same_bar_replay_direction", "") if signal else "",
            "trade_plan_created_at": event_at.isoformat(),
            "entry_price": _round_or_blank(entry_price),
            "exit_price": _round_or_blank(exit_price),
            "exit_filled_at": session.get("exit_filled_at", ""),
            "realized_pnl_usd": _round_or_blank(realized_pnl),
            "capital_at_entry_usd": _round_or_blank((entry_price or 0) * quantity * 100 if entry_price and quantity else None),
            "option_return_pct": _round_or_blank(option_return_pct),
            "realized_stop_r": _round_or_blank(realized_stop_r),
            "mala_base_expectancy_r": _round_or_blank(base_expectancy),
            "mala_thesis_expectancy_r": _round_or_blank(thesis_expectancy),
            "mala_expected_r_used": _round_or_blank(expected_r),
            "mala_thesis_win_rate": _round_or_blank(_first_float(thesis_metrics.get("win_rate"))),
            "mala_thesis_trade_count": thesis_metrics.get("trade_count", ""),
            "catalog_key": metadata.get("catalog_key", ""),
            "strategy_key": _nested_get(deployment, ["strategy", "key"], ""),
            "strategy_name": evidence.get("strategy_name", ""),
            "authorization_mode": metadata.get("authorization_mode", ""),
            "active_plan_id": snapshot.get("active_plan_id", "") if snapshot else "",
            "signal_window_et": evidence.get("signal_window_et", ""),
            "entry_inside_signal_window": _yes_no(_inside_window(entry_at, str(evidence.get("signal_window_et", "")))),
            "trade_plan_risk_reasons": ",".join(str(reason) for reason in payload.get("risk_reasons") or []),
            "ev_alignment": _ev_alignment(expected_r, realized_stop_r, str(session.get("status", "planned_only"))),
            "concordance_status": _trade_concordance_status(signal, signal and signal.get("concordance_status"), lag_seconds, max_signal_lag),
        }
        rows.append(row)
    rows.sort(key=lambda row: row["entry_at"])
    return rows


def _deployment_rows(signal_rows: list[dict[str, Any]], trade_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deployments = sorted(
        {str(row.get("deployment_id", "")) for row in signal_rows + trade_rows if row.get("deployment_id")}
    )
    rows: list[dict[str, Any]] = []
    for deployment_id in deployments:
        signals = [row for row in signal_rows if row.get("deployment_id") == deployment_id]
        trades = [row for row in trade_rows if row.get("deployment_id") == deployment_id]
        closed = [row for row in trades if row.get("status") == "closed" and row.get("realized_stop_r") != ""]
        realized_rs = [_first_float(row.get("realized_stop_r")) for row in closed]
        realized_rs = [value for value in realized_rs if value is not None]
        pnls = [_first_float(row.get("realized_pnl_usd")) for row in closed]
        pnls = [value for value in pnls if value is not None]
        expected_values = [_first_float(row.get("mala_expected_r_used")) for row in trades]
        expected_values = [value for value in expected_values if value is not None]
        representative = (trades or signals or [{}])[-1]
        timing_misses = sum(1 for row in trades if row.get("signal_match_status") != "matched")
        adverse = sum(1 for row in trades if row.get("ev_alignment") == "adverse_trade")
        positive = sum(1 for row in trades if row.get("ev_alignment") == "positive_trade")
        rows.append(
            {
                "deployment_id": deployment_id,
                "symbol": representative.get("symbol", ""),
                "direction": representative.get("direction", ""),
                "catalog_key": representative.get("catalog_key", ""),
                "strategy_key": representative.get("strategy_key", ""),
                "strategy_name": representative.get("strategy_name", ""),
                "authorization_mode": representative.get("authorization_mode", ""),
                "signal_count": len(signals),
                "trade_plan_count": len(trades),
                "closed_trade_count": len(closed),
                "matched_trade_count": sum(1 for row in trades if row.get("signal_match_status") == "matched"),
                "timing_miss_count": timing_misses,
                "same_bar_replay_match_count": sum(
                    1 for row in signals if row.get("mala_same_bar_replay_status") == "match"
                ),
                "same_bar_replay_no_signal_count": sum(
                    1 for row in signals if row.get("mala_same_bar_replay_status") == "no_mala_signal"
                ),
                "same_bar_replay_missing_bar_count": sum(
                    1 for row in signals if row.get("mala_same_bar_replay_status") == "missing_bar"
                ),
                "total_realized_pnl_usd": _round_or_blank(sum(pnls) if pnls else None),
                "avg_realized_stop_r": _round_or_blank(sum(realized_rs) / len(realized_rs) if realized_rs else None),
                "win_rate_realized": _round_or_blank(sum(1 for value in realized_rs if value > 0) / len(realized_rs) if realized_rs else None),
                "mala_expected_r_used": _round_or_blank(sum(expected_values) / len(expected_values) if expected_values else None),
                "positive_trade_count": positive,
                "adverse_trade_count": adverse,
                "operator_verdict": _deployment_verdict(len(closed), realized_rs, expected_values, timing_misses, adverse),
            }
        )
    rows.sort(key=lambda row: (row["operator_verdict"], str(row["deployment_id"])))
    return rows


def _counterfactual_rows(
    timeline: list[dict[str, Any]],
    signal_rows: list[dict[str, Any]],
    *,
    replay_cache: "_ReplayCache | None",
    replay_warmup_days: int,
    start_at: datetime | None,
    evaluation_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if replay_cache is None:
        return []

    actual_by_key: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in signal_rows:
        signal_at = _parse_dt_or_none(str(row.get("signal_at", "")))
        if signal_at is None:
            continue
        actual_by_key[(str(row.get("deployment_id", "")), _bar_key(signal_at))].append(row)

    evaluation_by_key: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in evaluation_rows:
        evaluated_at = _parse_dt_or_none(str(row.get("evaluated_at", "")))
        if evaluated_at is None:
            continue
        evaluation_by_key[(str(row.get("deployment_id", "")), _bar_key(evaluated_at))].append(row)

    seen_deployments: set[tuple[str, str, str]] = set()
    matched_actual_keys: set[tuple[str, str]] = set()
    rows: list[dict[str, Any]] = []
    for snapshot in timeline:
        trading_date = str(snapshot.get("trading_date") or "")
        if not trading_date:
            continue
        if start_at is not None:
            day_end = _parse_dt_or_none(f"{trading_date}T23:59:59+00:00")
            if day_end is not None and day_end < start_at:
                continue
        for deployment_id, deployment in snapshot.get("deployments", {}).items():
            key = (str(snapshot.get("active_plan_id", "")), trading_date, str(deployment_id))
            if key in seen_deployments:
                continue
            seen_deployments.add(key)
            rows.extend(
                _counterfactual_deployment_day_rows(
                    snapshot=snapshot,
                    deployment_id=str(deployment_id),
                    deployment=deployment,
                    trading_date=trading_date,
                    actual_by_key=actual_by_key,
                    evaluation_by_key=evaluation_by_key,
                    matched_actual_keys=matched_actual_keys,
                    replay_cache=replay_cache,
                    replay_warmup_days=replay_warmup_days,
                )
            )

    for signal in signal_rows:
        signal_at = _parse_dt_or_none(str(signal.get("signal_at", "")))
        if signal_at is None:
            continue
        key = (str(signal.get("deployment_id", "")), _bar_key(signal_at))
        if key in matched_actual_keys:
            continue
        row = {
            "counterfactual_status": "extra_bhiksha_signal",
            "active_plan_id": signal.get("active_plan_id", ""),
            "trading_date": signal_at.astimezone(ET).date().isoformat(),
            "deployment_id": signal.get("deployment_id", ""),
            "symbol": signal.get("symbol", ""),
            "strategy_key": signal.get("strategy_key", ""),
            "strategy_name": signal.get("strategy_name", ""),
            "catalog_key": signal.get("catalog_key", ""),
            "expected_direction": signal.get("expected_direction", ""),
            "mala_signal_at": "",
            "mala_signal_at_et": "",
            "mala_direction": "",
            "bhiksha_signal_at": signal.get("signal_at", ""),
            "bhiksha_signal_at_et": signal.get("signal_at_et", ""),
            "bhiksha_direction": signal.get("direction", ""),
            "bhiksha_reason": signal.get("reason", ""),
            "mala_signal_window_et": signal.get("signal_window_et", ""),
            "mala_replay_status": signal.get("mala_same_bar_replay_status", ""),
            "mala_replay_error": signal.get("mala_same_bar_replay_error", ""),
            "mala_feature_mismatch_count": signal.get("mala_same_bar_feature_mismatch_count", ""),
            "mala_feature_max_pct_diff": signal.get("mala_same_bar_feature_max_pct_diff", ""),
            "mala_feature_worst": signal.get("mala_same_bar_feature_worst", ""),
            "mala_feature_diffs": signal.get("mala_same_bar_feature_diffs", ""),
        }
        row["root_cause"] = _counterfactual_root_cause(row)
        rows.append(row)
    rows.sort(
        key=lambda row: (
            str(row.get("trading_date", "")),
            str(row.get("deployment_id", "")),
            str(row.get("mala_signal_at") or row.get("bhiksha_signal_at") or ""),
            str(row.get("counterfactual_status", "")),
        )
    )
    return rows


def _counterfactual_deployment_day_rows(
    *,
    snapshot: dict[str, Any],
    deployment_id: str,
    deployment: dict[str, Any],
    trading_date: str,
    actual_by_key: dict[tuple[str, str], list[dict[str, Any]]],
    evaluation_by_key: dict[tuple[str, str], list[dict[str, Any]]],
    matched_actual_keys: set[tuple[str, str]],
    replay_cache: "_ReplayCache",
    replay_warmup_days: int,
) -> list[dict[str, Any]]:
    metadata = _metadata(deployment)
    evidence = _mala_evidence(metadata)
    strategy_key = str(_nested_get(deployment, ["strategy", "key"], ""))
    strategy_name = STRATEGY_NAME_BY_KEY.get(strategy_key)
    symbol = str(deployment.get("symbol") or metadata.get("symbol") or "")
    expected_direction = _expected_direction(deployment, metadata)
    signal_window = _signal_window(metadata)
    thesis_metrics = _thesis_metrics(metadata)
    base_row = {
        "active_plan_id": snapshot.get("active_plan_id", ""),
        "trading_date": trading_date,
        "deployment_id": deployment_id,
        "symbol": symbol,
        "strategy_key": strategy_key,
        "strategy_name": evidence.get("strategy_name", strategy_name or ""),
        "catalog_key": metadata.get("catalog_key", ""),
        "expected_direction": expected_direction,
        "mala_signal_window_et": signal_window,
        "mala_thesis_exit_policy": _nested_get(deployment, ["exit", "thesis_exit_policy"], "")
        or metadata.get("thesis_exit_policy", ""),
        "mala_thesis_expectancy_r": _round_or_blank(
            _first_float(thesis_metrics.get("expectancy"), metadata.get("expectancy"))
        ),
        "mala_thesis_win_rate": _round_or_blank(_first_float(thesis_metrics.get("win_rate"))),
        "mala_thesis_trade_count": thesis_metrics.get("trade_count", ""),
    }
    replay_warmup_trading_days = _resolve_replay_warmup_trading_days(
        snapshot=snapshot,
        deployment_id=deployment_id,
        deployment=deployment,
        override_days=replay_warmup_days,
    )
    replay_start_date = _replay_start_date(
        datetime.fromisoformat(trading_date).date(),
        replay_warmup_trading_days,
    )
    base_row |= {
        "mala_replay_warmup_trading_days": replay_warmup_trading_days,
        "mala_replay_start_date": replay_start_date.isoformat(),
    }
    if not strategy_name:
        return [
            base_row
            | {
                "counterfactual_status": "unsupported_strategy",
                "mala_replay_status": "unsupported_strategy",
                "mala_replay_error": f"strategy_key={strategy_key}",
                "root_cause": "unsupported_strategy_adapter",
            }
        ]

    day = datetime.fromisoformat(trading_date).date()
    params = _normalized_params(_nested_get(deployment, ["strategy", "params"], {}) or {})
    try:
        frame = replay_cache.signal_frame(
            symbol=symbol,
            start_date=replay_start_date,
            end_date=day,
            strategy_name=strategy_name,
            params=params,
        )
        if frame.is_empty():
            row = base_row | {"counterfactual_status": "missing_bars", "mala_replay_status": "missing_bars"}
            row["root_cause"] = _counterfactual_root_cause(row)
            return [row]
        day_signals = _mala_day_signals(frame, trading_date, expected_direction, signal_window)
    except Exception as exc:
        return [
            base_row
            | {
                "counterfactual_status": "replay_error",
                "mala_replay_status": "replay_error",
                "mala_replay_error": str(exc)[:240],
                "root_cause": "mala_replay_error",
            }
        ]

    rows: list[dict[str, Any]] = []
    for signal in day_signals:
        signal_at = signal["signal_at"]
        bar_key = _bar_key(signal_at)
        actuals = actual_by_key.get((deployment_id, bar_key), [])
        actual = _matching_actual_signal(actuals, str(signal.get("direction", "")), expected_direction)
        if actual:
            matched_actual_keys.add((deployment_id, bar_key))
            status = "matched_actual"
        elif actuals:
            matched_actual_keys.add((deployment_id, bar_key))
            actual = actuals[0]
            status = "actual_direction_mismatch"
        else:
            status = "missed_by_bhiksha"
        evaluation = _nearest_evaluation(evaluation_by_key.get((deployment_id, bar_key), []), signal_at)
        rows.append(
            _with_root_cause(
                base_row
                | {
                    "counterfactual_status": status,
                    "mala_replay_status": "signal",
                    "mala_signal_at": signal_at.isoformat(),
                    "mala_signal_at_et": _format_et(signal_at),
                    "mala_direction": signal.get("direction", ""),
                    "mala_underlying_entry_price": signal.get("mala_underlying_entry_price", ""),
                    "mala_underlying_mfe_points": signal.get("mala_underlying_mfe_points", ""),
                    "mala_underlying_mae_points": signal.get("mala_underlying_mae_points", ""),
                    "mala_underlying_mfe_pct": signal.get("mala_underlying_mfe_pct", ""),
                    "mala_underlying_mae_pct": signal.get("mala_underlying_mae_pct", ""),
                    "mala_underlying_mfe_mae_ratio": signal.get("mala_underlying_mfe_mae_ratio", ""),
                    "mala_underlying_excursion_win": signal.get("mala_underlying_excursion_win", ""),
                    "mala_underlying_measure": signal.get("mala_underlying_measure", ""),
                    "bhiksha_signal_at": actual.get("signal_at", "") if actual else "",
                    "bhiksha_signal_at_et": actual.get("signal_at_et", "") if actual else "",
                    "bhiksha_direction": actual.get("direction", "") if actual else "",
                    "bhiksha_reason": actual.get("reason", "") if actual else "",
                    "bhiksha_evaluation_at": evaluation.get("evaluated_at", "") if evaluation else "",
                    "bhiksha_evaluation_signal": evaluation.get("signal", "") if evaluation else "",
                    "bhiksha_evaluation_direction": evaluation.get("direction", "") if evaluation else "",
                    "bhiksha_evaluation_reason": evaluation.get("reason", "") if evaluation else "",
                }
            )
        )
    if not rows:
        row = base_row | {"counterfactual_status": "no_mala_signals", "mala_replay_status": "no_signal"}
        row["root_cause"] = _counterfactual_root_cause(row)
        rows.append(row)
    return rows


def _mala_day_signals(
    frame: pl.DataFrame,
    trading_date: str,
    expected_direction: str,
    signal_window: str,
) -> list[dict[str, Any]]:
    if "timestamp" not in frame.columns or "signal" not in frame.columns:
        return []
    day_frame = frame.with_columns(
        pl.col("timestamp").dt.convert_time_zone("America/New_York").dt.date().cast(pl.Utf8).alias("_et_date")
    ).filter(pl.col("_et_date") == trading_date)
    if day_frame.is_empty():
        return []
    day_rows = day_frame.sort("timestamp").iter_rows(named=True)
    day_records = list(day_rows)
    keyed = pl.DataFrame(day_records).filter(pl.col("signal").fill_null(False))
    if expected_direction and "signal_direction" in keyed.columns:
        keyed = keyed.filter(pl.col("signal_direction") == expected_direction)
    row_index_by_key = {
        _bar_key(_ensure_aware_datetime(row["timestamp"])): index
        for index, row in enumerate(day_records)
        if isinstance(row.get("timestamp"), datetime)
    }
    signals: list[dict[str, Any]] = []
    for row in keyed.iter_rows(named=True):
        signal_at = row.get("timestamp")
        if not isinstance(signal_at, datetime):
            continue
        if signal_at.tzinfo is None:
            signal_at = signal_at.replace(tzinfo=timezone.utc)
        if not _inside_window(signal_at, signal_window):
            continue
        direction = "" if row.get("signal_direction") is None else str(row.get("signal_direction"))
        underlying = _underlying_counterfactual_metrics(
            day_records=day_records,
            signal_index=row_index_by_key.get(_bar_key(signal_at)),
            direction=direction or expected_direction,
        )
        signals.append(
            {
                "signal_at": signal_at.astimezone(timezone.utc),
                "direction": direction,
                **underlying,
            }
        )
    signals.sort(key=lambda row: row["signal_at"])
    return signals


def _underlying_counterfactual_metrics(
    *,
    day_records: list[dict[str, Any]],
    signal_index: int | None,
    direction: str,
) -> dict[str, Any]:
    base = {
        "mala_underlying_entry_price": "",
        "mala_underlying_mfe_points": "",
        "mala_underlying_mae_points": "",
        "mala_underlying_mfe_pct": "",
        "mala_underlying_mae_pct": "",
        "mala_underlying_mfe_mae_ratio": "",
        "mala_underlying_excursion_win": "",
        "mala_underlying_measure": "signal_close_to_eod",
    }
    if signal_index is None or signal_index >= len(day_records):
        return base
    signal_row = day_records[signal_index]
    entry = _finite_float(signal_row.get("close"))
    if entry is None or entry == 0:
        return base
    forward = day_records[signal_index + 1 :]
    if not forward:
        return base | {"mala_underlying_entry_price": _round_or_blank(entry)}
    highs = [_finite_float(row.get("high")) for row in forward]
    lows = [_finite_float(row.get("low")) for row in forward]
    highs = [value for value in highs if value is not None]
    lows = [value for value in lows if value is not None]
    if not highs or not lows:
        return base | {"mala_underlying_entry_price": _round_or_blank(entry)}
    normalized_direction = direction.lower()
    if normalized_direction == "short":
        mfe = entry - min(lows)
        mae = max(highs) - entry
    else:
        mfe = max(highs) - entry
        mae = entry - min(lows)
    mfe = max(0.0, float(mfe))
    mae = max(0.0, float(mae))
    ratio = None
    if mae > 0:
        ratio = mfe / mae
    elif mfe > 0:
        ratio = float("inf")
    win = mfe > 0 and (mae == 0 or mfe >= 2 * mae)
    return base | {
        "mala_underlying_entry_price": _round_or_blank(entry),
        "mala_underlying_mfe_points": _round_or_blank(mfe),
        "mala_underlying_mae_points": _round_or_blank(mae),
        "mala_underlying_mfe_pct": _round_or_blank(mfe / entry),
        "mala_underlying_mae_pct": _round_or_blank(mae / entry),
        "mala_underlying_mfe_mae_ratio": "inf" if ratio == float("inf") else _round_or_blank(ratio),
        "mala_underlying_excursion_win": _yes_no(win),
    }


def _matching_actual_signal(
    actuals: list[dict[str, Any]],
    mala_direction: str,
    expected_direction: str,
) -> dict[str, Any]:
    for actual in actuals:
        actual_direction = str(actual.get("direction", ""))
        if mala_direction and actual_direction == mala_direction:
            return actual
        if not mala_direction and expected_direction and actual_direction == expected_direction:
            return actual
    return {}


def _nearest_evaluation(evaluations: list[dict[str, Any]], target_at: datetime) -> dict[str, Any]:
    if not evaluations:
        return {}
    best: dict[str, Any] = {}
    best_abs_seconds: float | None = None
    for evaluation in evaluations:
        evaluated_at = _parse_dt_or_none(str(evaluation.get("evaluated_at", "")))
        if evaluated_at is None:
            continue
        abs_seconds = abs((evaluated_at - target_at).total_seconds())
        if best_abs_seconds is None or abs_seconds < best_abs_seconds:
            best = evaluation
            best_abs_seconds = abs_seconds
    return best


def _with_root_cause(row: dict[str, Any]) -> dict[str, Any]:
    row["root_cause"] = _counterfactual_root_cause(row)
    return row


def _counterfactual_root_cause(row: dict[str, Any]) -> str:
    status = str(row.get("counterfactual_status", ""))
    if status == "matched_actual":
        return "matched"
    if status == "actual_direction_mismatch":
        return "runtime_direction_mismatch"
    if status == "unsupported_strategy":
        return "unsupported_strategy_adapter"
    if status in {"missing_bars", "missing_bar"}:
        return "mala_cache_gap"
    if status == "replay_error":
        return "mala_replay_error"
    if status == "extra_bhiksha_signal":
        replay_status = str(row.get("mala_replay_status", ""))
        if replay_status == "unsupported_strategy":
            return "unsupported_strategy_adapter"
        if replay_status in {"missing_bars", "missing_bar"}:
            return "mala_cache_gap"
        if replay_status == "replay_error":
            return "mala_replay_error"
        mismatch_count = int(_first_float(row.get("mala_feature_mismatch_count"), 0) or 0)
        if mismatch_count > 0:
            worst = str(row.get("mala_feature_worst", "")).lower()
            if "volume" in worst:
                return "provider_feature_mismatch_volume"
            if any(token in worst for token in ("velocity", "accel", "jerk", "directional_mass", "vpoc")):
                return "provider_feature_mismatch_kinematic"
            return "provider_feature_mismatch"
        if replay_status == "no_mala_signal":
            return "same_bar_strategy_mismatch"
        return "extra_signal_unclassified"
    if status == "missed_by_bhiksha":
        signal = str(row.get("bhiksha_evaluation_signal", ""))
        if signal == "yes":
            return "runtime_direction_or_matching_gap"
        reason = str(row.get("bhiksha_evaluation_reason", "")).lower()
        if not reason:
            return "no_runtime_evaluation_observed"
        if any(token in reason for token in ("time_window", "entry_window", "minutes")):
            return "runtime_time_window_gate"
        if "volume" in reason:
            return "runtime_volume_gate"
        if "regime" in reason:
            return "runtime_regime_gate"
        if any(token in reason for token in ("velocity", "accel", "jerk", "directional_mass", "vpoc", "stretch")):
            return "runtime_strategy_feature_gate"
        if any(token in reason for token in ("cooldown", "position", "cash", "lifecycle", "reconciliation")):
            return "runtime_risk_or_lifecycle_gate"
        return "runtime_strategy_gate"
    if status == "no_mala_signals":
        return "no_mala_signal_expected"
    return status or "unknown"


def _counterfactual_summary_rows(
    counterfactual_rows: list[dict[str, Any]],
    trade_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    deployments = sorted(
        {str(row.get("deployment_id", "")) for row in counterfactual_rows + trade_rows if row.get("deployment_id")}
    )
    rows: list[dict[str, Any]] = []
    for deployment_id in deployments:
        cf = [row for row in counterfactual_rows if row.get("deployment_id") == deployment_id]
        trades = [row for row in trade_rows if row.get("deployment_id") == deployment_id]
        representative = (trades or cf or [{}])[-1]
        clean_trades = [row for row in trades if row.get("mala_same_bar_replay_status") == "match"]
        extra_trades = [row for row in trades if row.get("mala_same_bar_replay_status") == "no_mala_signal"]
        clean_ev = _realized_trade_summary(clean_trades)
        extra_ev = _realized_trade_summary(extra_trades)
        expected = [row for row in cf if row.get("counterfactual_status") in {"matched_actual", "missed_by_bhiksha", "actual_direction_mismatch"}]
        matched = [row for row in cf if row.get("counterfactual_status") == "matched_actual"]
        missed = [row for row in cf if row.get("counterfactual_status") == "missed_by_bhiksha"]
        extras = [row for row in cf if row.get("counterfactual_status") == "extra_bhiksha_signal"]
        root_causes = Counter(str(row.get("root_cause", "")) for row in missed + extras if row.get("root_cause"))
        underlying_mfes = _float_values(row.get("mala_underlying_mfe_points") for row in expected)
        underlying_maes = _float_values(row.get("mala_underlying_mae_points") for row in expected)
        underlying_win_values = [row.get("mala_underlying_excursion_win") for row in expected]
        underlying_win_count = sum(1 for value in underlying_win_values if value == "yes")
        underlying_win_sample = sum(1 for value in underlying_win_values if value in {"yes", "no"})
        thesis_expectancies = _float_values(row.get("mala_thesis_expectancy_r") for row in expected)
        rows.append(
            {
                "deployment_id": deployment_id,
                "symbol": representative.get("symbol", ""),
                "strategy_key": representative.get("strategy_key", ""),
                "catalog_key": representative.get("catalog_key", ""),
                "authorization_mode": representative.get("authorization_mode", ""),
                "mala_expected_signals": len(expected),
                "matched_actual_signals": len(matched),
                "missed_mala_signals": len(missed),
                "extra_bhiksha_signals": len(extras),
                "actual_match_rate": _round_or_blank(len(matched) / len(expected) if expected else None),
                "mala_counterfactual_win_rate": _round_or_blank(
                    underlying_win_count / underlying_win_sample if underlying_win_sample else None
                ),
                "mala_avg_underlying_mfe_points": _round_or_blank(_mean(underlying_mfes)),
                "mala_avg_underlying_mae_points": _round_or_blank(_mean(underlying_maes)),
                "mala_avg_thesis_expectancy_r": _round_or_blank(_mean(thesis_expectancies)),
                "clean_trade_count": clean_ev["count"],
                "clean_win_rate": _round_or_blank(clean_ev["win_rate"]),
                "clean_avg_realized_stop_r": _round_or_blank(clean_ev["avg_r"]),
                "clean_total_pnl_usd": _round_or_blank(clean_ev["pnl"]),
                "extra_trade_count": extra_ev["count"],
                "extra_win_rate": _round_or_blank(extra_ev["win_rate"]),
                "extra_avg_realized_stop_r": _round_or_blank(extra_ev["avg_r"]),
                "extra_total_pnl_usd": _round_or_blank(extra_ev["pnl"]),
                "top_root_causes": _compact_counter(root_causes, limit=3),
                "counterfactual_focus": _counterfactual_focus(len(expected), len(matched), len(missed), len(extras), clean_ev, extra_ev),
            }
        )
    rows.sort(key=lambda row: (str(row["counterfactual_focus"]), str(row["deployment_id"])))
    return rows


def _realized_trade_summary(trades: list[dict[str, Any]]) -> dict[str, Any]:
    closed = [row for row in trades if row.get("status") == "closed" and row.get("realized_stop_r") != ""]
    values = [_first_float(row.get("realized_stop_r")) for row in closed]
    values = [value for value in values if value is not None]
    pnls = [_first_float(row.get("realized_pnl_usd")) for row in closed]
    pnls = [value for value in pnls if value is not None]
    return {
        "count": len(values),
        "win_rate": sum(1 for value in values if value > 0) / len(values) if values else None,
        "avg_r": sum(values) / len(values) if values else None,
        "pnl": sum(pnls) if pnls else None,
    }


def _counterfactual_focus(
    expected: int,
    matched: int,
    missed: int,
    extras: int,
    clean_ev: dict[str, Any],
    extra_ev: dict[str, Any],
) -> str:
    match_rate = matched / expected if expected else 1.0
    clean_avg = clean_ev.get("avg_r")
    extra_avg = extra_ev.get("avg_r")
    if expected and (missed >= 2 or match_rate < 0.80):
        return "architecture_false_negative_review"
    if extras >= 2 and (extra_avg is None or extra_avg < 0):
        return "architecture_false_positive_review"
    if clean_avg is not None and clean_avg < 0:
        return "strategy_or_execution_review"
    if clean_avg is not None and clean_avg > 0:
        return "clean_signal_positive"
    return "needs_more_sample"


def _bar_key(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(second=0, microsecond=0).isoformat()


def _render_report(
    *,
    db_path: Path,
    lookback_days: int,
    max_seen_at: datetime | None,
    same_bar_replay: bool,
    counterfactual_replay: bool,
    signal_rows: list[dict[str, Any]],
    trade_rows: list[dict[str, Any]],
    deployment_rows: list[dict[str, Any]],
    counterfactual_rows: list[dict[str, Any]],
    counterfactual_summary_rows: list[dict[str, Any]],
    signal_csv: Path,
    trade_csv: Path,
    deployment_csv: Path,
    counterfactual_csv: Path,
    counterfactual_summary_csv: Path,
) -> str:
    closed = [row for row in trade_rows if row.get("status") == "closed"]
    realized_closed = [row for row in closed if row.get("realized_stop_r") != ""]
    total_pnl = sum(_first_float(row.get("realized_pnl_usd")) or 0.0 for row in realized_closed)
    matched = sum(1 for row in trade_rows if row.get("signal_match_status") == "matched")
    adverse = sum(1 for row in trade_rows if row.get("ev_alignment") == "adverse_trade")
    positive = sum(1 for row in trade_rows if row.get("ev_alignment") == "positive_trade")
    concordance_counts = Counter(row.get("concordance_status", "") for row in signal_rows)
    replay_counts = Counter(row.get("mala_same_bar_replay_status", "") for row in signal_rows)
    counterfactual_counts = Counter(row.get("counterfactual_status", "") for row in counterfactual_rows)
    root_cause_counts = Counter(row.get("root_cause", "") for row in counterfactual_rows if row.get("root_cause"))
    clean_ev = _realized_trade_summary([row for row in trade_rows if row.get("mala_same_bar_replay_status") == "match"])
    extra_ev = _realized_trade_summary([row for row in trade_rows if row.get("mala_same_bar_replay_status") == "no_mala_signal"])
    lines = [
        "# Bhiksha Signal Concordance and Realized EV",
        "",
        f"- generated_from_db: `{db_path}`",
        f"- lookback_days: `{lookback_days}`",
        f"- latest_runtime_at: `{max_seen_at.isoformat() if max_seen_at else ''}`",
        f"- true_signal_events: `{len(signal_rows)}`",
        f"- trade_plans: `{len(trade_rows)}`",
        f"- closed_trades: `{len(closed)}`",
        f"- closed_trades_with_realized_ev: `{len(realized_closed)}`",
        f"- matched_signal_to_entry: `{matched}`",
        f"- total_realized_pnl_usd: `{round(total_pnl, 2)}`",
        f"- positive_trades_vs_evidence: `{positive}`",
        f"- adverse_trades_vs_evidence: `{adverse}`",
        f"- signal_csv: `{signal_csv}`",
        f"- trade_csv: `{trade_csv}`",
        f"- deployment_csv: `{deployment_csv}`",
        f"- counterfactual_csv: `{counterfactual_csv}`",
        f"- counterfactual_summary_csv: `{counterfactual_summary_csv}`",
        "",
        "## Method",
        "",
        "- Joins `signal_decision`, `trade_plan`, and `trade_sessions` from Bhiksha SQLite.",
        "- Uses the latest prior `startup_config.active_plan.deployments` snapshot to attach Mala catalog metadata, expected R, thesis win rate, signal window, exit profile, and risk profile to each runtime event.",
        "- Treats signal concordance as a compiled-runtime check: Bhiksha fired through the Mala-sourced deployment, in the expected direction, inside the Mala signal window.",
        (
            "- Same-bar Mala replay is enabled: cached 1-minute bars are enriched with Newton features, the compiled Mala strategy params are rerun, and the Bhiksha signal bar is checked for a same-direction Mala signal. Replay warmup uses the Bhiksha startup warmup contract when present, otherwise the legacy runtime warmup (`app.warmup_trading_days + 3`)."
            if same_bar_replay
            else "- Same-bar Mala replay is disabled for this run. Re-run with `--same-bar-replay` to independently verify cached Mala signals."
        ),
        (
            "- Counterfactual replay is enabled: each active-plan deployment/day is replayed across its Mala signal window with the Bhiksha startup warmup contract, then expected Mala signals are compared with actual Bhiksha signal events on the same minute bar."
            if counterfactual_replay
            else "- Counterfactual replay is disabled for this run. Re-run with `--counterfactual-replay` to measure missed Mala signals and extra Bhiksha signals."
        ),
        "- Converts long option fills into realized premium PnL and an option-stop-R proxy: option return divided by configured `stop_loss_pct`.",
        "",
        "## Signal Concordance",
        "",
        "| Status | Count |",
        "|---|---:|",
    ]
    for status, count in concordance_counts.most_common():
        lines.append(f"| {status or 'unknown'} | {count} |")
    if same_bar_replay:
        lines.extend(["", "## Same-Bar Mala Replay", "", "| Status | Count |", "|---|---:|"])
        for status, count in replay_counts.most_common():
            lines.append(f"| {status or 'unknown'} | {count} |")

    if counterfactual_replay:
        expected_count = sum(
            count
            for status, count in counterfactual_counts.items()
            if status in {"matched_actual", "missed_by_bhiksha", "actual_direction_mismatch"}
        )
        lines.extend(
            [
                "",
                "## Counterfactual Mala Replay",
                "",
                f"- mala_expected_signals: `{expected_count}`",
                f"- matched_actual_signals: `{counterfactual_counts.get('matched_actual', 0)}`",
                f"- missed_mala_signals: `{counterfactual_counts.get('missed_by_bhiksha', 0)}`",
                f"- extra_bhiksha_signals: `{counterfactual_counts.get('extra_bhiksha_signal', 0)}`",
                f"- clean_matched_trades: `{clean_ev['count']}` win_rate=`{_round_or_blank(clean_ev['win_rate'])}` avg_realized_r=`{_round_or_blank(clean_ev['avg_r'])}` pnl=`{_round_or_blank(clean_ev['pnl'])}`",
                f"- extra_bhiksha_trades: `{extra_ev['count']}` win_rate=`{_round_or_blank(extra_ev['win_rate'])}` avg_realized_r=`{_round_or_blank(extra_ev['avg_r'])}` pnl=`{_round_or_blank(extra_ev['pnl'])}`",
                "",
                "| Status | Count |",
                "|---|---:|",
            ]
        )
        for status, count in counterfactual_counts.most_common():
            lines.append(f"| {status or 'unknown'} | {count} |")
        lines.extend(["", "### Root-Cause Buckets", "", "| Root Cause | Count |", "|---|---:|"])
        for cause, count in root_cause_counts.most_common():
            lines.append(f"| {cause or 'unknown'} | {count} |")
        lines.extend(
            [
                "",
                "### Counterfactual Deployment Focus",
                "",
                "| Deployment | Expected | Matched | Missed | Extra | Mala Win | Mala MFE | Mala MAE | Mala Exp R | Clean Trades | Clean Avg R | Extra Trades | Extra Avg R | Top Causes | Focus |",
                "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|",
            ]
        )
        for row in counterfactual_summary_rows[:25]:
            lines.append(
                f"| {row['deployment_id']} | {row['mala_expected_signals']} | {row['matched_actual_signals']} | "
                f"{row['missed_mala_signals']} | {row['extra_bhiksha_signals']} | {row['mala_counterfactual_win_rate']} | "
                f"{row['mala_avg_underlying_mfe_points']} | {row['mala_avg_underlying_mae_points']} | "
                f"{row['mala_avg_thesis_expectancy_r']} | {row['clean_trade_count']} | "
                f"{row['clean_avg_realized_stop_r']} | {row['extra_trade_count']} | {row['extra_avg_realized_stop_r']} | "
                f"{row['top_root_causes']} | {row['counterfactual_focus']} |"
            )

    lines.extend(
        [
            "",
            "## Deployment Scorecard",
            "",
            "| Deployment | Signals | Trades | Closed | Matched | PnL | Avg Realized R | Mala Exp R | Verdict |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---|",
        ]
    )
    for row in deployment_rows[:25]:
        lines.append(
            f"| {row['deployment_id']} | {row['signal_count']} | {row['trade_plan_count']} | "
            f"{row['closed_trade_count']} | {row['matched_trade_count']} | {row['total_realized_pnl_usd']} | "
            f"{row['avg_realized_stop_r']} | {row['mala_expected_r_used']} | {row['operator_verdict']} |"
        )

    lines.extend(
        [
            "",
            "## Closed Trade Details",
            "",
            "| Entry ET | Symbol | Deployment | Option | PnL | Realized R | Mala Exp R | Alignment |",
            "|---|---|---|---|---:|---:|---:|---|",
        ]
    )
    for row in closed[-25:]:
        lines.append(
            f"| {row['entry_at_et']} | {row['symbol']} | {row['deployment_id']} | {row['option_symbol']} | "
            f"{row['realized_pnl_usd']} | {row['realized_stop_r']} | {row['mala_expected_r_used']} | {row['ev_alignment']} |"
        )

    lines.extend(
        [
            "",
            "## Operator Read",
            "",
            "- Use this report to separate plumbing from alpha: timing misses and missing trade-session rows are plumbing; adverse realized R against positive Mala expectancy is strategy/execution evidence.",
            "- Do not promote from this report alone. Two weeks of shadow should first show clean concordance, realistic fills/spreads, and no repeated lifecycle defects.",
            "- If same-bar replay is missing bars, backfill cached minute data before treating those rows as mismatches.",
        ]
    )
    return "\n".join(lines) + "\n"


class _ReplayCache:
    def __init__(self, data_dir: Path) -> None:
        self.storage = LocalStorage(data_dir)
        self._bars: dict[tuple[str, str, str], pl.DataFrame] = {}
        self._signals: dict[tuple[str, str, str, str, str], pl.DataFrame] = {}

    def load_bars(self, symbol: str, start_date, end_date) -> pl.DataFrame:
        key = (symbol.upper(), str(start_date), str(end_date))
        if key not in self._bars:
            self._bars[key] = self.storage.load_bars(symbol, start_date, end_date)
        return self._bars[key]

    def signal_frame(
        self,
        *,
        symbol: str,
        start_date,
        end_date,
        strategy_name: str,
        params: dict[str, Any],
    ) -> pl.DataFrame:
        params_key = json.dumps(params, sort_keys=True, default=str)
        key = (symbol.upper(), str(start_date), str(end_date), strategy_name, params_key)
        if key in self._signals:
            return self._signals[key]
        raw = self.load_bars(symbol, start_date, end_date)
        if raw.is_empty():
            self._signals[key] = pl.DataFrame()
            return self._signals[key]
        strategy = build_strategy(strategy_name, _constructor_params(strategy_name, params))
        required = set(strategy.required_features) | set(strategy.feature_requests)
        enriched = PhysicsEngine().enrich_for_features(raw, required)
        self._signals[key] = strategy.generate_signals(enriched)
        return self._signals[key]


def _same_bar_replay(
    *,
    payload: dict[str, Any],
    snapshot: dict[str, Any],
    deployment: dict[str, Any],
    signal_at: datetime,
    replay_cache: _ReplayCache | None,
    replay_warmup_days: int,
) -> dict[str, Any]:
    base = {
        "mala_same_bar_replay_status": "not_requested",
        "mala_same_bar_replay_signal": "",
        "mala_same_bar_replay_direction": "",
        "mala_same_bar_replay_strategy": "",
        "mala_same_bar_replay_bars": "",
        "mala_same_bar_replay_error": "",
        "mala_same_bar_feature_compared": "",
        "mala_same_bar_feature_mismatch_count": "",
        "mala_same_bar_feature_max_pct_diff": "",
        "mala_same_bar_feature_worst": "",
        "mala_same_bar_feature_diffs": "",
        "mala_same_bar_replay_warmup_trading_days": "",
        "mala_same_bar_replay_start_date": "",
    }
    if replay_cache is None:
        return base
    strategy_key = str(_nested_get(deployment, ["strategy", "key"], ""))
    strategy_name = STRATEGY_NAME_BY_KEY.get(strategy_key)
    if not strategy_name:
        return base | {
            "mala_same_bar_replay_status": "unsupported_strategy",
            "mala_same_bar_replay_error": f"strategy_key={strategy_key}",
        }
    params = _normalized_params(_nested_get(deployment, ["strategy", "params"], {}) or {})
    signal_date = signal_at.astimezone(ET).date()
    replay_warmup_trading_days = _resolve_replay_warmup_trading_days(
        snapshot=snapshot,
        deployment_id=str(payload.get("deployment_id", "")),
        deployment=deployment,
        override_days=replay_warmup_days,
    )
    start_date = _replay_start_date(signal_date, replay_warmup_trading_days)
    base |= {
        "mala_same_bar_replay_warmup_trading_days": replay_warmup_trading_days,
        "mala_same_bar_replay_start_date": start_date.isoformat(),
    }
    expected_direction = str(payload.get("direction", ""))
    try:
        frame = replay_cache.signal_frame(
            symbol=str(payload.get("symbol", "")),
            start_date=start_date,
            end_date=signal_date,
            strategy_name=strategy_name,
            params=params,
        )
        if frame.is_empty():
            return base | {
                "mala_same_bar_replay_status": "missing_bars",
                "mala_same_bar_replay_strategy": strategy_name,
            }
        replay_ts = signal_at.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
        keyed = frame.with_columns(
            pl.col("timestamp").dt.convert_time_zone("UTC").dt.strftime("%Y-%m-%dT%H:%M:%S").alias("_replay_ts")
        )
        bar = keyed.filter(pl.col("_replay_ts") == replay_ts)
        if bar.is_empty():
            return base | {
                "mala_same_bar_replay_status": "missing_bar",
                "mala_same_bar_replay_strategy": strategy_name,
                "mala_same_bar_replay_bars": str(frame.height),
            }
        feature_replay = _same_bar_feature_replay(
            payload_features=payload.get("features") if isinstance(payload.get("features"), dict) else {},
            bar=bar.row(0, named=True),
            params=params,
        )
        signal = bool(bar.select(pl.col("signal").fill_null(False)).item(0, 0))
        direction = ""
        if "signal_direction" in bar.columns:
            raw_direction = bar.select("signal_direction").item(0, 0)
            direction = "" if raw_direction is None else str(raw_direction)
        status = "match" if signal and (not expected_direction or direction == expected_direction) else "no_mala_signal"
        if signal and expected_direction and direction and direction != expected_direction:
            status = "direction_mismatch"
        return base | {
            "mala_same_bar_replay_status": status,
            "mala_same_bar_replay_signal": _yes_no(signal),
            "mala_same_bar_replay_direction": direction,
            "mala_same_bar_replay_strategy": strategy_name,
            "mala_same_bar_replay_bars": str(frame.height),
        } | feature_replay
    except Exception as exc:
        return base | {
            "mala_same_bar_replay_status": "replay_error",
            "mala_same_bar_replay_strategy": strategy_name,
            "mala_same_bar_replay_error": str(exc)[:240],
        }


def _same_bar_feature_replay(
    *,
    payload_features: dict[str, Any],
    bar: dict[str, Any],
    params: dict[str, Any],
) -> dict[str, Any]:
    comparisons: list[dict[str, Any]] = []
    for runtime_key, runtime_value in sorted(payload_features.items()):
        runtime_number = _finite_float(runtime_value)
        if runtime_number is None:
            continue
        replay_key = _replay_feature_key(runtime_key, bar, params)
        if replay_key is None:
            continue
        replay_number = _finite_float(bar.get(replay_key))
        if replay_number is None:
            continue
        diff = runtime_number - replay_number
        pct = _pct_diff(runtime_number, replay_number)
        comparisons.append(
            {
                "runtime_feature": runtime_key,
                "mala_feature": replay_key,
                "runtime": round(runtime_number, 6),
                "mala": round(replay_number, 6),
                "diff": round(diff, 6),
                "pct": round(pct, 6),
            }
        )
    mismatches = [item for item in comparisons if abs(float(item["diff"])) > 1e-9]
    worst = max(comparisons, key=lambda item: float(item["pct"]), default=None)
    return {
        "mala_same_bar_feature_compared": str(len(comparisons)),
        "mala_same_bar_feature_mismatch_count": str(len(mismatches)),
        "mala_same_bar_feature_max_pct_diff": "" if worst is None else str(worst["pct"]),
        "mala_same_bar_feature_worst": "" if worst is None else str(worst["runtime_feature"]),
        "mala_same_bar_feature_diffs": _compact_json(comparisons),
    }


def _replay_feature_key(runtime_key: str, bar: dict[str, Any], params: dict[str, Any]) -> str | None:
    if runtime_key in bar:
        return runtime_key
    periods_back = int(_first_float(params.get("kinematic_periods_back"), 1) or 1)
    aliases = {
        "velocity": velocity_column_name(periods_back),
        "accel": acceleration_column_name(periods_back),
        "acceleration": acceleration_column_name(periods_back),
        "jerk": jerk_column_name(periods_back),
    }
    candidate = aliases.get(runtime_key)
    if candidate in bar:
        return candidate
    return None


def _finite_float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _pct_diff(a: float, b: float) -> float:
    if a == 0 and b == 0:
        return 0.0
    return abs(a - b) / max(abs(a), abs(b), 1e-12)


def _nearest_signal(
    signals: list[dict[str, Any]],
    entry_at: datetime,
    *,
    max_signal_lag: timedelta,
) -> dict[str, Any]:
    best: dict[str, Any] = {}
    best_abs_seconds: float | None = None
    for signal in signals:
        signal_at = _parse_dt_or_none(str(signal.get("signal_at", "")))
        if signal_at is None:
            continue
        lag = entry_at - signal_at
        if lag < -timedelta(seconds=30) or lag > max_signal_lag:
            continue
        abs_seconds = abs(lag.total_seconds())
        if best_abs_seconds is None or abs_seconds < best_abs_seconds:
            best = signal
            best_abs_seconds = abs_seconds
    return best


def _signal_match_status(signal: dict[str, Any], lag_seconds: int | None, max_signal_lag: timedelta) -> str:
    if not signal:
        return "missing_signal"
    if lag_seconds is None:
        return "unknown"
    if lag_seconds < -30:
        return "entry_before_signal"
    if lag_seconds > max_signal_lag.total_seconds():
        return "signal_too_stale"
    return "matched"


def _signal_concordance_status(
    direction: str,
    expected_direction: str,
    signal_at: datetime,
    signal_window: str,
    metadata: dict[str, Any],
) -> str:
    if not metadata:
        return "missing_mala_metadata"
    if expected_direction and direction != expected_direction:
        return "direction_mismatch"
    if signal_window and not _inside_window(signal_at, signal_window):
        return "outside_signal_window"
    return "ok"


def _trade_concordance_status(
    signal: dict[str, Any],
    signal_concordance: Any,
    lag_seconds: int | None,
    max_signal_lag: timedelta,
) -> str:
    match = _signal_match_status(signal, lag_seconds, max_signal_lag)
    if match != "matched":
        return match
    return str(signal_concordance or "ok")


def _deployment_verdict(
    closed_count: int,
    realized_rs: list[float],
    expected_values: list[float],
    timing_misses: int,
    adverse: int,
) -> str:
    if timing_misses:
        return "plumbing_review"
    if closed_count == 0:
        return "needs_shadow_sample"
    avg_realized = sum(realized_rs) / len(realized_rs) if realized_rs else 0.0
    avg_expected = sum(expected_values) / len(expected_values) if expected_values else None
    if closed_count < 3:
        return "small_sample_positive" if avg_realized > 0 else "small_sample_adverse"
    if avg_expected is not None and avg_expected > 0 and avg_realized < 0:
        return "ev_divergence_review"
    if adverse and adverse >= max(2, closed_count // 2):
        return "mixed_ev_review" if avg_realized > 0 else "ev_divergence_review"
    if avg_realized > 0:
        return "shadow_evidence_positive"
    return "watch"


def _ev_alignment(expected_r: float | None, realized_stop_r: float | None, status: str) -> str:
    if status != "closed":
        return "open_or_unrealized"
    if expected_r is None or realized_stop_r is None:
        return "unknown"
    if expected_r > 0 and realized_stop_r > 0:
        return "positive_trade"
    if expected_r > 0 and realized_stop_r < 0:
        return "adverse_trade"
    if expected_r <= 0 and realized_stop_r > 0:
        return "better_than_expected"
    return "in_family"


def _metadata(deployment: dict[str, Any]) -> dict[str, Any]:
    source = deployment.get("source") if isinstance(deployment, dict) else {}
    metadata = source.get("metadata") if isinstance(source, dict) else {}
    return metadata if isinstance(metadata, dict) else {}


def _mala_evidence(metadata: dict[str, Any]) -> dict[str, Any]:
    playbook = metadata.get("playbook_summary") if isinstance(metadata.get("playbook_summary"), dict) else {}
    evidence = playbook.get("mala_evidence") if isinstance(playbook.get("mala_evidence"), dict) else {}
    return evidence if isinstance(evidence, dict) else {}


def _thesis_metrics(metadata: dict[str, Any]) -> dict[str, Any]:
    evidence = _mala_evidence(metadata)
    metrics = evidence.get("thesis_exit_metrics")
    return metrics if isinstance(metrics, dict) else {}


def _expected_direction(deployment: dict[str, Any], metadata: dict[str, Any]) -> str:
    return str(metadata.get("direction") or _nested_get(deployment, ["strategy", "params", "direction"], "") or "")


def _signal_window(metadata: dict[str, Any]) -> str:
    return str(_mala_evidence(metadata).get("signal_window_et") or metadata.get("signal_window_et") or "")


def _inside_window(value: datetime, window: str) -> bool:
    if not window or "-" not in window:
        return True
    start_text, end_text = window.split("-", 1)
    start = _parse_time(start_text)
    end = _parse_time(end_text)
    if start is None or end is None:
        return True
    local = value.astimezone(ET).time().replace(second=0, microsecond=0)
    return start <= local <= end


def _parse_time(value: str) -> time | None:
    try:
        hour, minute = value.strip().split(":", 1)
        return time(int(hour), int(minute))
    except (ValueError, AttributeError):
        return None


def _max_seen_at(events: list[dict[str, Any]], trade_sessions: list[dict[str, Any]]) -> datetime | None:
    times = [_parse_dt(event["created_at"]) for event in events if event.get("created_at")]
    times.extend(time_value for row in trade_sessions if (time_value := _best_trade_time(row)) is not None)
    return max(times) if times else None


def _best_trade_time(row: dict[str, Any]) -> datetime | None:
    for key in ("updated_at", "exit_filled_at", "entry_timestamp"):
        if value := row.get(key):
            parsed = _parse_dt_or_none(str(value))
            if parsed is not None:
                return parsed
    return None


def _realized_pnl(entry_price: float | None, exit_price: float | None, quantity: int) -> float | None:
    if entry_price is None or exit_price is None or not quantity:
        return None
    return (exit_price - entry_price) * quantity * 100


def _option_return_pct(entry_price: float | None, exit_price: float | None) -> float | None:
    if entry_price is None or exit_price is None or entry_price == 0:
        return None
    return (exit_price - entry_price) / entry_price


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    headers = sorted({key for row in rows for key in row})
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)


def _parse_dt(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=ET)
    return parsed


def _parse_dt_or_none(value: str) -> datetime | None:
    if not value:
        return None
    try:
        return _parse_dt(value)
    except ValueError:
        return None


def _format_et(value: datetime) -> str:
    return value.astimezone(ET).strftime("%Y-%m-%d %H:%M:%S %Z")


def _ensure_aware_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def _nested_get(root: dict[str, Any], path: list[str], default: Any = "") -> Any:
    value: Any = root
    for part in path:
        if not isinstance(value, dict):
            return default
        value = value.get(part)
    return default if value is None else value


def _constructor_params(strategy_name: str, params: dict[str, Any]) -> dict[str, Any]:
    strategy = build_strategy(strategy_name, {})
    signature = inspect.signature(type(strategy).__init__)
    allowed = {
        name
        for name, parameter in signature.parameters.items()
        if name != "self"
        and parameter.kind in {inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY}
    }
    return {key: value for key, value in params.items() if key in allowed}


def _normalized_params(params: dict[str, Any]) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for key, value in params.items():
        if value in (None, ""):
            continue
        if key == "vwma_periods" and isinstance(value, list | tuple):
            normalized[key] = tuple(int(part) for part in value)
            continue
        normalized[key] = _coerce_param_value(key, value)
    return normalized


def _coerce_param_value(key: str, value: Any) -> Any:
    if isinstance(value, bool | int | float | list | tuple | dict):
        return value
    text = str(value).strip()
    lowered = text.lower()
    if lowered in {"true", "false"}:
        return lowered == "true"
    if key.endswith("_minutes") or key.endswith("_window") or key in {
        "confirmation_window_bars",
        "entry_buffer_minutes",
        "entry_window_minutes",
        "jerk_lookback",
        "kinematic_periods_back",
        "market_open_hour",
        "market_open_minute",
        "min_bars_after_pierce",
        "opening_window_minutes",
        "relative_volume_period",
        "reclaim_window_bars",
        "vma_length",
        "volume_ma_period",
        "z_score_window",
    }:
        try:
            return int(float(text))
        except ValueError:
            return value
    if key.endswith("_pct") or key.startswith("min_") or key.startswith("max_") or key in {
        "breakout_buffer_pct",
        "confirmation_margin_pct",
        "min_close_location",
        "volume_multiplier",
        "vpoc_proximity_pct",
        "z_score_threshold",
    }:
        try:
            return float(text)
        except ValueError:
            return value
    return value


def _first_float(*values: Any) -> float | None:
    for value in values:
        try:
            if value == "":
                continue
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def _float_values(values: Any) -> list[float]:
    parsed = [_first_float(value) for value in values]
    return [value for value in parsed if value is not None]


def _mean(values: list[float]) -> float | None:
    return sum(values) / len(values) if values else None


def _round_or_blank(value: float | None) -> str:
    if value is None:
        return ""
    return str(round(value, 4))


def _yes_no(value: bool) -> str:
    return "yes" if value else "no"


def _compact_json(value: Any) -> str:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"))
    except TypeError:
        return ""


def _compact_counter(counter: Counter[str], *, limit: int) -> str:
    return ",".join(f"{key}:{count}" for key, count in counter.most_common(limit))


__all__ = ["BhikshaSignalEvArtifacts", "build_bhiksha_signal_ev_report"]
