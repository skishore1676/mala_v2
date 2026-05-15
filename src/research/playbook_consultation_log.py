"""Append and close playbook consultation journal rows.

The surface query answers the trader's timestamp question. This module keeps the
feedback loop: what was asked, what the desk reported, whether the trader acted,
and what happened after the decision.
"""

from __future__ import annotations

import argparse
import csv
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


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
            print(
                "\t".join(
                    [
                        row.get("query_id", ""),
                        row.get("query_ts_et", ""),
                        row.get("symbol", ""),
                        row.get("direction", ""),
                        row.get("desk_read", ""),
                        row.get("taken", ""),
                        row.get("actual_pnl_r", ""),
                    ]
                )
            )
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
    if args.command == "dedupe":
        path = dedupe_consultation_rows(args.run_dir)
        print(f"DEDUPED={path}")
        return 0
    raise AssertionError(f"Unhandled command {args.command!r}")


if __name__ == "__main__":
    raise SystemExit(main())
