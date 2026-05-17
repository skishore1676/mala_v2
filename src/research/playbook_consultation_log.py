"""Append and close playbook consultation journal rows.

The surface query answers the trader's timestamp question. This module keeps the
feedback loop: what was asked, what the desk reported, whether the trader acted,
and what happened after the decision.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from src.chronos.storage import LocalStorage
from src.config import DATA_DIR
from src.newton.engine import PhysicsEngine
from src.research.playbook_operator_policy import operator_policy_from_payload


CONSULTATION_LOG_NAME = "consultation_log.csv"
CONSULTATION_LOG_FIELDS = [
    "query_id",
    "query_ts_et",
    "playbook_id",
    "symbol",
    "direction",
    "desk_read",
    "confidence",
    "cohort_n",
    "selected_exit",
    "reported_survived_pct",
    "taken",
    "actual_exit_reason",
    "actual_pnl_r",
    "actual_time_to_exit",
    "actual_exit_ts_et",
    "operator_note",
    "updated_at_utc",
    "review_md",
    "query_json",
]
NY = ZoneInfo("America/New_York")


@dataclass(frozen=True, slots=True)
class ReplayCloseResult:
    log_path: Path
    query_id: str
    actual_exit_reason: str
    actual_pnl_r: str
    actual_time_to_exit: str
    actual_exit_ts_et: str


@dataclass(frozen=True, slots=True)
class ConsultationLogSummary:
    log_path: Path
    total_rows: int
    open_rows: int
    closed_rows: int
    takes: int
    passes: int
    taken_pnl_r_count: int
    average_taken_pnl_r: float | None
    next_action: str


def append_consultation_query(
    run_dir: Path,
    payload: dict[str, Any],
    review_md: Path,
    json_path: Path,
) -> Path:
    """Append a query row and return the log path."""
    log_path = run_dir / CONSULTATION_LOG_NAME
    rows = _read_and_migrate(log_path)
    rows.append(
        {
            "query_id": _query_id_from_json_path(json_path),
            "query_ts_et": str(payload.get("timestamp_et", "")),
            "playbook_id": str(payload.get("playbook_id", "")),
            "symbol": str(payload.get("symbol", "")),
            "direction": str(payload.get("direction", "")),
            "desk_read": str(payload.get("verdict", "")),
            "confidence": str(payload.get("cohort", {}).get("confidence", "")),
            "cohort_n": str(payload.get("cohort", {}).get("analog_count", "")),
            "selected_exit": "",
            "reported_survived_pct": "",
            "taken": "",
            "actual_exit_reason": "",
            "actual_pnl_r": "",
            "actual_time_to_exit": "",
            "actual_exit_ts_et": "",
            "operator_note": "",
            "updated_at_utc": "",
            "review_md": str(review_md),
            "query_json": str(json_path),
        }
    )
    _write_rows(log_path, rows)
    return log_path


def update_consultation_row(
    run_dir: Path,
    *,
    query_id: str,
    selected_exit: str = "",
    reported_survived_pct: str = "",
    taken: str = "",
    actual_exit_reason: str = "",
    actual_pnl_r: str = "",
    actual_time_to_exit: str = "",
    actual_exit_ts_et: str = "",
    operator_note: str = "",
) -> Path:
    """Update one consultation row by query id."""
    log_path = run_dir / CONSULTATION_LOG_NAME
    rows = _read_and_migrate(log_path)
    if not rows:
        raise FileNotFoundError(f"No consultation rows found in {log_path}")
    matches = [index for index, row in enumerate(rows) if row.get("query_id") == query_id]
    if not matches:
        raise ValueError(f"No consultation row found for query_id={query_id!r}")
    index = matches[-1]
    updates = {
        "selected_exit": selected_exit,
        "reported_survived_pct": reported_survived_pct,
        "taken": _normalize_taken(taken),
        "actual_exit_reason": actual_exit_reason,
        "actual_pnl_r": actual_pnl_r,
        "actual_time_to_exit": actual_time_to_exit,
        "actual_exit_ts_et": actual_exit_ts_et,
        "operator_note": operator_note,
    }
    for key, value in updates.items():
        if value != "":
            rows[index][key] = value
    rows[index]["updated_at_utc"] = datetime.now(UTC).isoformat()
    _write_rows(log_path, rows)
    return log_path


def replay_close_consultation_row(
    run_dir: Path,
    *,
    query_id: str,
    taken: str,
    selected_exit: str = "",
    operator_note: str = "",
    data_dir: Path | None = None,
    max_nearest_seconds: int = 90,
) -> ReplayCloseResult:
    """Close a historical replay row by computing actuals from cached bars."""
    normalized_taken = _normalize_taken(taken)
    if normalized_taken == "N":
        path = update_consultation_row(
            run_dir,
            query_id=query_id,
            taken="N",
            actual_exit_reason="no_trade",
            operator_note=operator_note,
        )
        return ReplayCloseResult(
            log_path=path,
            query_id=query_id,
            actual_exit_reason="no_trade",
            actual_pnl_r="",
            actual_time_to_exit="",
            actual_exit_ts_et="",
        )
    if normalized_taken != "Y":
        raise ValueError("--taken is required for replay-close and must be Y or N")

    row = _latest_row_for_query(run_dir, query_id)
    effective_selected_exit = selected_exit or row.get("selected_exit", "")
    if not effective_selected_exit:
        raise ValueError("--selected-exit is required when --taken Y")

    query_json = _query_json_path(run_dir, row, query_id)
    payload = json.loads(query_json.read_text(encoding="utf-8"))
    outcome = _historical_management_outcome(
        payload,
        selected_exit=effective_selected_exit,
        data_dir=data_dir,
        max_nearest_seconds=max_nearest_seconds,
    )
    reported_survived = row.get("reported_survived_pct") or _reported_survived_pct(
        payload,
        effective_selected_exit,
    )
    path = update_consultation_row(
        run_dir,
        query_id=query_id,
        selected_exit=effective_selected_exit,
        reported_survived_pct=reported_survived,
        taken="Y",
        actual_exit_reason=outcome["actual_exit_reason"],
        actual_pnl_r=outcome["actual_pnl_r"],
        actual_time_to_exit=outcome["actual_time_to_exit"],
        actual_exit_ts_et=outcome["actual_exit_ts_et"],
        operator_note=operator_note,
    )
    return ReplayCloseResult(
        log_path=path,
        query_id=query_id,
        actual_exit_reason=outcome["actual_exit_reason"],
        actual_pnl_r=outcome["actual_pnl_r"],
        actual_time_to_exit=outcome["actual_time_to_exit"],
        actual_exit_ts_et=outcome["actual_exit_ts_et"],
    )


def dedupe_consultation_rows(run_dir: Path) -> Path:
    """Keep the latest row per query id."""
    log_path = run_dir / CONSULTATION_LOG_NAME
    rows = _read_and_migrate(log_path)
    latest_by_query: dict[str, dict[str, str]] = {}
    passthrough: list[dict[str, str]] = []
    for row in rows:
        query_id = row.get("query_id", "")
        if not query_id:
            passthrough.append(row)
            continue
        latest_by_query[query_id] = _preferred_row(latest_by_query.get(query_id), row)
    _write_rows(log_path, passthrough + list(latest_by_query.values()))
    return log_path


def _preferred_row(existing: dict[str, str] | None, candidate: dict[str, str]) -> dict[str, str]:
    if existing is None:
        return candidate
    existing_score = _completion_score(existing)
    candidate_score = _completion_score(candidate)
    if candidate_score >= existing_score:
        return candidate
    return existing


def _completion_score(row: dict[str, str]) -> int:
    keys = [
        "taken",
        "selected_exit",
        "actual_exit_reason",
        "actual_pnl_r",
        "actual_time_to_exit",
        "operator_note",
        "updated_at_utc",
    ]
    return sum(1 for key in keys if row.get(key))


def open_consultation_rows(run_dir: Path) -> list[dict[str, str]]:
    rows = _read_and_migrate(run_dir / CONSULTATION_LOG_NAME)
    return [
        row
        for row in rows
        if not row.get("taken")
        and not row.get("actual_exit_reason")
        and not row.get("actual_pnl_r")
    ]


def summarize_consultation_log(run_dir: Path, *, target_closed_rows: int = 8) -> ConsultationLogSummary:
    """Summarize operator replay progress and return the next useful action."""
    log_path = run_dir / CONSULTATION_LOG_NAME
    rows = _read_and_migrate(log_path)
    open_rows = [
        row
        for row in rows
        if not row.get("taken")
        and not row.get("actual_exit_reason")
        and not row.get("actual_pnl_r")
    ]
    closed_rows = [row for row in rows if row not in open_rows]
    takes = [row for row in closed_rows if _row_taken(row) == "Y"]
    passes = [row for row in closed_rows if _row_taken(row) == "N"]
    pnl_values = [
        float(row["actual_pnl_r"])
        for row in takes
        if _is_float_text(row.get("actual_pnl_r", ""))
    ]
    average_taken_pnl = sum(pnl_values) / len(pnl_values) if pnl_values else None
    return ConsultationLogSummary(
        log_path=log_path,
        total_rows=len(rows),
        open_rows=len(open_rows),
        closed_rows=len(closed_rows),
        takes=len(takes),
        passes=len(passes),
        taken_pnl_r_count=len(pnl_values),
        average_taken_pnl_r=average_taken_pnl,
        next_action=_consultation_next_action(
            total_rows=len(rows),
            open_rows=len(open_rows),
            closed_rows=len(closed_rows),
            target_closed_rows=target_closed_rows,
        ),
    )


def _row_taken(row: dict[str, str]) -> str:
    try:
        return _normalize_taken(row.get("taken", ""))
    except ValueError:
        return ""


def _is_float_text(value: str) -> bool:
    try:
        float(value)
    except (TypeError, ValueError):
        return False
    return True


def _consultation_next_action(
    *,
    total_rows: int,
    open_rows: int,
    closed_rows: int,
    target_closed_rows: int,
) -> str:
    if total_rows == 0:
        return "start_chart_first_query"
    if open_rows:
        return "close_open_consultation_rows"
    if closed_rows < target_closed_rows:
        return "add_more_chart_first_rows"
    return "review_closed_batch_before_promotion"


def _latest_row_for_query(run_dir: Path, query_id: str) -> dict[str, str]:
    rows = _read_and_migrate(run_dir / CONSULTATION_LOG_NAME)
    matches = [row for row in rows if row.get("query_id") == query_id]
    if not matches:
        raise ValueError(f"No consultation row found for query_id={query_id!r}")
    return matches[-1]


def _query_json_path(run_dir: Path, row: dict[str, str], query_id: str) -> Path:
    raw = row.get("query_json", "")
    if raw:
        path = Path(raw)
        if path.exists():
            return path
    path = run_dir / "surface_queries" / query_id / "query_result.json"
    if path.exists():
        return path
    raise FileNotFoundError(f"query_result.json not found for query_id={query_id!r}")


def _historical_management_outcome(
    payload: dict[str, Any],
    *,
    selected_exit: str,
    data_dir: Path | None,
    max_nearest_seconds: int,
) -> dict[str, str]:
    from src.research.playbook_surface_query import (
        STATE_MANAGEMENT_EXIT_SPECS,
        STATE_MANAGEMENT_FEATURES,
        _ensure_aware,
        _future_rows_same_day,
        _management_target_floor,
        _management_target_move,
        _nearest_state_row,
        _prepare_state_management_frame,
        _safe_float,
        _time_to_move,
    )

    spec = next(
        (item for item in STATE_MANAGEMENT_EXIT_SPECS if item[0] == selected_exit),
        None,
    )
    if spec is None:
        raise ValueError(f"selected_exit {selected_exit!r} is not in the management menu")
    _, _, kind, value = spec
    symbol = str(payload.get("symbol", "")).upper()
    direction = str(payload.get("direction", "")).lower()
    if direction not in {"long", "short"}:
        raise ValueError(f"Unsupported direction in query payload: {direction!r}")
    query_utc = _ensure_aware(datetime.fromisoformat(str(payload["timestamp_utc"]))).astimezone(UTC)
    query_date = query_utc.astimezone(NY).date()
    bars = LocalStorage(base_dir=data_dir or DATA_DIR).load_bars(
        symbol,
        start=query_date - timedelta(days=30),
        end=query_date,
    )
    if bars.is_empty():
        raise FileNotFoundError(f"No bars found for {symbol} on or before {query_date}")
    enriched = PhysicsEngine().enrich_for_features(bars, STATE_MANAGEMENT_FEATURES)
    frame = _prepare_state_management_frame(enriched, direction)
    rows = frame.to_dicts()
    query_row = _nearest_state_row(rows, query_utc, max_nearest_seconds)
    operator_policy = operator_policy_from_payload(payload)
    entry = _safe_float(query_row.get("close"))
    if entry is None:
        raise ValueError("Could not recover query entry price from historical bars")
    target_move = _management_target_move(query_row, kind, value)
    if target_move is None or target_move <= 0:
        raise ValueError(f"Could not compute target move for selected_exit={selected_exit!r}")
    target_floor = _management_target_floor(query_row, entry, operator_policy)
    if target_move < target_floor:
        raise ValueError(
            f"selected_exit {selected_exit!r} target move {target_move:.4f} is below "
            f"the policy floor {target_floor:.4f}"
        )
    row_index = int(query_row["_row_index"])
    future = _future_rows_same_day(rows, row_index, 30)
    if not future:
        return {
            "actual_exit_reason": "no_forward_data",
            "actual_pnl_r": "",
            "actual_time_to_exit": "",
            "actual_exit_ts_et": "",
        }
    target_time = _time_to_move(entry, future, direction, target_move, favorable=True)
    adverse_time = _time_to_move(entry, future, direction, target_move, favorable=False)
    if target_time is not None and (adverse_time is None or target_time < adverse_time):
        return _historical_outcome_row(future, target_time, "target", 1.0)
    if adverse_time is not None:
        return _historical_outcome_row(future, adverse_time, "stop", -1.0)

    exit_row = future[-1]
    close = _safe_float(exit_row.get("close"))
    pnl_r = 0.0
    if close is not None:
        pnl = close - entry if direction == "long" else entry - close
        pnl_r = pnl / target_move
    return _historical_outcome_row(future, len(future), "time_stop_30m", pnl_r)


def _historical_outcome_row(
    future: list[dict[str, Any]],
    minutes: int,
    reason: str,
    pnl_r: float,
) -> dict[str, str]:
    exit_index = max(0, min(minutes - 1, len(future) - 1))
    exit_ts = future[exit_index].get("timestamp")
    exit_ts_et = ""
    if isinstance(exit_ts, datetime):
        timestamp = exit_ts if exit_ts.tzinfo is not None else exit_ts.replace(tzinfo=UTC)
        exit_ts_et = timestamp.astimezone(NY).isoformat(timespec="seconds")
    return {
        "actual_exit_reason": reason,
        "actual_pnl_r": _format_replay_float(pnl_r),
        "actual_time_to_exit": str(minutes),
        "actual_exit_ts_et": exit_ts_et,
    }


def _reported_survived_pct(payload: dict[str, Any], selected_exit: str) -> str:
    for row in payload.get("cohort", {}).get("management_rows", []):
        if row.get("exit_family") == selected_exit:
            return str(row.get("survived_pct", ""))
    return ""


def _format_replay_float(value: float) -> str:
    if math.isnan(value) or math.isinf(value):
        return ""
    return f"{value:.4f}".rstrip("0").rstrip(".")


def _read_and_migrate(log_path: Path) -> list[dict[str, str]]:
    if not log_path.exists():
        return []
    with log_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        raw_rows = list(reader)
    rows = [_migrate_row(row) for row in raw_rows]
    _write_rows(log_path, rows)
    return rows


def _migrate_row(row: dict[str, str]) -> dict[str, str]:
    migrated = {field: row.get(field, "") for field in CONSULTATION_LOG_FIELDS}
    if not migrated["query_id"]:
        migrated["query_id"] = _query_id_from_json_path(Path(row.get("query_json", "")))
    if not migrated["playbook_id"]:
        migrated["playbook_id"] = row.get("playbook", "") or _playbook_id_from_json_path(
            Path(row.get("query_json", ""))
        )
    return migrated


def _write_rows(log_path: Path, rows: list[dict[str, str]]) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CONSULTATION_LOG_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in CONSULTATION_LOG_FIELDS})


def _query_id_from_json_path(path: Path) -> str:
    if path and str(path) not in {"", "."}:
        parent = path.parent.name
        if parent:
            return parent
    return ""


def _playbook_id_from_json_path(path: Path) -> str:
    if not path.exists():
        return ""
    try:
        import json

        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return ""
    return str(payload.get("playbook_id", ""))


def _normalize_taken(raw: str) -> str:
    if raw == "":
        return ""
    value = raw.strip().upper()
    if value in {"Y", "YES", "TRUE", "1", "TAKEN"}:
        return "Y"
    if value in {"N", "NO", "FALSE", "0", "SKIPPED"}:
        return "N"
    raise ValueError("--taken must be Y or N")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    list_parser = subparsers.add_parser("list", help="List consultation rows")
    list_parser.add_argument("--run-dir", required=True, type=Path)
    list_parser.add_argument("--open-only", action="store_true")

    status_parser = subparsers.add_parser("status", help="Summarize consultation progress")
    status_parser.add_argument("--run-dir", required=True, type=Path)
    status_parser.add_argument("--target-closed-rows", type=int, default=8)

    close_parser = subparsers.add_parser("close", help="Update a consultation row")
    close_parser.add_argument("--run-dir", required=True, type=Path)
    close_parser.add_argument("--query-id", required=True)
    close_parser.add_argument("--selected-exit", default="")
    close_parser.add_argument("--reported-survived-pct", default="")
    close_parser.add_argument("--taken", default="")
    close_parser.add_argument("--actual-exit-reason", default="")
    close_parser.add_argument("--actual-pnl-r", default="")
    close_parser.add_argument("--actual-time-to-exit", default="")
    close_parser.add_argument("--actual-exit-ts-et", default="")
    close_parser.add_argument("--operator-note", default="")

    replay_parser = subparsers.add_parser(
        "replay-close",
        help="Close a historical replay row by computing actuals from bars",
    )
    replay_parser.add_argument("--run-dir", required=True, type=Path)
    replay_parser.add_argument("--query-id", required=True)
    replay_parser.add_argument("--taken", required=True)
    replay_parser.add_argument("--selected-exit", default="")
    replay_parser.add_argument("--operator-note", default="")
    replay_parser.add_argument("--data-dir", type=Path, default=None)
    replay_parser.add_argument("--max-nearest-seconds", type=int, default=90)
    replay_parser.add_argument(
        "--historical",
        action="store_true",
        help="Explicit marker for replay workflow; replay-close always uses historical bars.",
    )

    dedupe_parser = subparsers.add_parser("dedupe", help="Keep the latest row per query id")
    dedupe_parser.add_argument("--run-dir", required=True, type=Path)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    if args.command == "list":
        rows = open_consultation_rows(args.run_dir) if args.open_only else _read_and_migrate(
            args.run_dir / CONSULTATION_LOG_NAME
        )
        for row in rows:
            fields = [
                row.get("query_id", ""),
                row.get("query_ts_et", ""),
                row.get("symbol", ""),
                row.get("direction", ""),
                row.get("desk_read", ""),
                row.get("taken", ""),
                row.get("actual_pnl_r", ""),
            ]
            print("\t".join(fields))
        return 0
    if args.command == "status":
        summary = summarize_consultation_log(
            args.run_dir,
            target_closed_rows=args.target_closed_rows,
        )
        print(f"LOG={summary.log_path}")
        print(f"TOTAL_ROWS={summary.total_rows}")
        print(f"OPEN_ROWS={summary.open_rows}")
        print(f"CLOSED_ROWS={summary.closed_rows}")
        print(f"TAKES={summary.takes}")
        print(f"PASSES={summary.passes}")
        print(f"TAKEN_PNL_R_COUNT={summary.taken_pnl_r_count}")
        if summary.average_taken_pnl_r is not None:
            print(f"AVERAGE_TAKEN_PNL_R={_format_replay_float(summary.average_taken_pnl_r)}")
        print(f"NEXT_ACTION={summary.next_action}")
        return 0
    if args.command == "close":
        path = update_consultation_row(
            args.run_dir,
            query_id=args.query_id,
            selected_exit=args.selected_exit,
            reported_survived_pct=args.reported_survived_pct,
            taken=args.taken,
            actual_exit_reason=args.actual_exit_reason,
            actual_pnl_r=args.actual_pnl_r,
            actual_time_to_exit=args.actual_time_to_exit,
            actual_exit_ts_et=args.actual_exit_ts_et,
            operator_note=args.operator_note,
        )
        print(f"UPDATED={path}")
        print(f"QUERY_ID={args.query_id}")
        return 0
    if args.command == "replay-close":
        result = replay_close_consultation_row(
            args.run_dir,
            query_id=args.query_id,
            taken=args.taken,
            selected_exit=args.selected_exit,
            operator_note=args.operator_note,
            data_dir=args.data_dir,
            max_nearest_seconds=args.max_nearest_seconds,
        )
        print(f"UPDATED={result.log_path}")
        print(f"QUERY_ID={result.query_id}")
        print(f"ACTUAL_EXIT_REASON={result.actual_exit_reason}")
        if result.actual_pnl_r:
            print(f"ACTUAL_PNL_R={result.actual_pnl_r}")
        if result.actual_time_to_exit:
            print(f"ACTUAL_TIME_TO_EXIT={result.actual_time_to_exit}")
        if result.actual_exit_ts_et:
            print(f"ACTUAL_EXIT_TS_ET={result.actual_exit_ts_et}")
        return 0
    if args.command == "dedupe":
        path = dedupe_consultation_rows(args.run_dir)
        print(f"DEDUPED={path}")
        return 0
    raise AssertionError(f"Unhandled command {args.command!r}")


if __name__ == "__main__":
    raise SystemExit(main())
