"""Historical Bhiksha plumbing triage from runtime events and logs."""

from __future__ import annotations

import csv
import json
import re
import sqlite3
from collections import Counter, defaultdict
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any


LOG_PATTERNS = {
    "descriptor_exhaustion": re.compile(r"Errno 24|too many open files|unable to open database", re.I),
    "auth_token": re.compile(
        r"refresh_token|token has expired|expired token|401 unauthorized|403 forbidden|unauthorized|forbidden",
        re.I,
    ),
    "provider_rate_limit": re.compile(r"429 Too Many Requests", re.I),
    "provider_bad_request": re.compile(r"400 Bad Request", re.I),
    "traceback": re.compile(r"Traceback", re.I),
    "error_line": re.compile(r"\bERROR\b", re.I),
}


@dataclass(slots=True, frozen=True)
class BhikshaPlumbingTriageArtifacts:
    report_md: Path
    issue_csv: Path
    day_csv: Path
    trade_block_csv: Path


def build_bhiksha_plumbing_triage(
    *,
    db_path: str | Path,
    logs_dir: str | Path,
    out_dir: str | Path,
    lookback_days: int = 21,
) -> BhikshaPlumbingTriageArtifacts:
    db = Path(db_path)
    logs = Path(logs_dir)
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    events = _load_events(db)
    max_event_at = max((_parse_dt(event["created_at"]) for event in events if event.get("created_at")), default=None)
    start_at = (max_event_at - timedelta(days=lookback_days)) if max_event_at else None
    scoped_events = [
        event for event in events
        if start_at is None or _parse_dt(event["created_at"]) >= start_at
    ]
    log_hits = _scan_logs(logs, start_at=start_at)
    day_rows = _day_rows(scoped_events)
    trade_block_rows = _trade_block_rows(scoped_events)
    issue_rows = _issue_family_rows(scoped_events, log_hits, max_event_at=max_event_at)

    issue_csv = out / "bhiksha_plumbing_issue_families.csv"
    day_csv = out / "bhiksha_plumbing_event_days.csv"
    trade_block_csv = out / "bhiksha_plumbing_trade_blocks.csv"
    report_md = out / "BHIKSHA_PLUMBING_TRIAGE.md"

    _write_csv(issue_csv, issue_rows)
    _write_csv(day_csv, day_rows)
    _write_csv(trade_block_csv, trade_block_rows)
    report_md.write_text(
        _render_report(
            db_path=db,
            logs_dir=logs,
            lookback_days=lookback_days,
            events=scoped_events,
            issue_rows=issue_rows,
            day_rows=day_rows,
            trade_block_rows=trade_block_rows,
            issue_csv=issue_csv,
            day_csv=day_csv,
            trade_block_csv=trade_block_csv,
            max_event_at=max_event_at,
        ),
        encoding="utf-8",
    )
    return BhikshaPlumbingTriageArtifacts(
        report_md=report_md,
        issue_csv=issue_csv,
        day_csv=day_csv,
        trade_block_csv=trade_block_csv,
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
        except json.JSONDecodeError:
            payload = {}
        events.append(
            {
                "created_at": str(created_at),
                "event_type": str(event_type),
                "payload": payload if isinstance(payload, dict) else {},
            }
        )
    return events


def _scan_logs(logs_dir: Path, *, start_at: datetime | None) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in sorted(logs_dir.glob("trade_session_*.log")):
        log_date = _date_from_log_name(path.name)
        if start_at is not None and log_date is not None and log_date < start_at.date():
            continue
        text = path.read_text(errors="ignore")
        for family, pattern in LOG_PATTERNS.items():
            count = len(pattern.findall(text))
            if count:
                rows.append(
                    {
                        "family": family,
                        "date": str(log_date or ""),
                        "source": path.name,
                        "count": count,
                    }
                )
    return rows


def _day_rows(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts: dict[str, Counter[str]] = defaultdict(Counter)
    for event in events:
        if event["event_type"] == "runtime_metric":
            continue
        counts[event["created_at"][:10]][event["event_type"]] += 1
    event_types = sorted({event_type for counter in counts.values() for event_type in counter})
    rows: list[dict[str, Any]] = []
    for day, counter in sorted(counts.items()):
        row = {"date": day, "total_non_metric": sum(counter.values())}
        row.update({event_type: counter.get(event_type, 0) for event_type in event_types})
        rows.append(row)
    return rows


def _trade_block_rows(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for event in events:
        if event["event_type"] != "trade_plan":
            continue
        payload = event["payload"]
        reasons = payload.get("risk_reasons") or []
        if not reasons or reasons == ["approved"]:
            continue
        details = payload.get("risk_details") or {}
        rows.append(
            {
                "created_at": event["created_at"],
                "symbol": payload.get("symbol", ""),
                "deployment_id": payload.get("deployment_id", ""),
                "reasons": ",".join(str(reason) for reason in reasons),
                "max_premium": details.get("max_premium", ""),
                "entry_price": details.get("entry_price", ""),
                "min_contract_cost": details.get("min_contract_cost", ""),
                "required_cash": details.get("required_cash", ""),
                "remaining_budget": details.get("remaining_budget", ""),
            }
        )
    return rows


def _issue_family_rows(
    events: list[dict[str, Any]],
    log_hits: list[dict[str, Any]],
    *,
    max_event_at: datetime | None,
) -> list[dict[str, Any]]:
    families: dict[str, dict[str, Any]] = {}

    def add(family: str, when: str, count: int, evidence: str, status: str, next_action: str) -> None:
        row = families.setdefault(
            family,
            {
                "family": family,
                "count": 0,
                "first_seen": when,
                "last_seen": when,
                "status": status,
                "evidence": evidence,
                "next_action": next_action,
            },
        )
        row["count"] += count
        if when and (not row["first_seen"] or when < row["first_seen"]):
            row["first_seen"] = when
        if when and (not row["last_seen"] or when > row["last_seen"]):
            row["last_seen"] = when
        if evidence not in row["evidence"]:
            row["evidence"] = f"{row['evidence']}; {evidence}"

    for hit in log_hits:
        date = str(hit["date"])
        family = str(hit["family"])
        if family == "descriptor_exhaustion":
            add(
                family,
                date,
                int(hit["count"]),
                f"{hit['source']} matched descriptor/database errors",
                "likely_fixed_or_quiet",
                "Monitor next live run; reopen only if Errno 24 or SQLite open failures return.",
            )
        elif family == "provider_rate_limit":
            add(
                family,
                date,
                int(hit["count"]),
                f"{hit['source']} had Schwab 429s",
                "watch",
                "Keep Public/Schwab-heavy jobs deconflicted; add backoff if 429s recur.",
            )
        elif family == "provider_bad_request":
            add(
                family,
                date,
                int(hit["count"]),
                f"{hit['source']} had Schwab 400s",
                "needs_triage_if_repeats",
                "Treat weekend/off-session 400s as low priority unless they appear during RTH.",
            )
        elif family == "auth_token":
            add(
                family,
                date,
                int(hit["count"]),
                f"{hit['source']} had token/auth text",
                "watch",
                "Verify healthcheck before each live/shadow session.",
            )

    for event in events:
        payload = event["payload"]
        event_type = event["event_type"]
        when = event["created_at"][:10]
        if event_type == "runtime_issue":
            category = str(payload.get("category", "exception"))
            error = str(payload.get("error") or payload)[:240].replace("\n", " ")
            if category == "protection_restore":
                add(
                    "protection_restore_conflict",
                    when,
                    1,
                    error,
                    "needs_deeper_review",
                    "Review restore/close-order coordination; keep out of live promotion decisions until no repeated restore conflicts.",
                )
            elif category == "auth":
                add(
                    "auth_token",
                    when,
                    1,
                    error,
                    "watch",
                    "Operational auth issue; verify current token health before running.",
                )
            elif category == "data":
                family = "provider_rate_limit" if "429" in error else "provider_bad_request" if "400" in error else "data_provider"
                add(
                    family,
                    when,
                    1,
                    error,
                    "watch",
                    "Track provider failures in daily report; fix if they block observations or entries during market hours.",
                )
            elif category == "reconciliation":
                add(
                    "reconciliation_noise",
                    when,
                    1,
                    error,
                    "likely_fixed_or_benign",
                    "Keep in report but prioritize only if paired with stale portfolio or orphan positions.",
                )
        elif event_type == "sheet_status_writeback_failure":
            add(
                "sheet_status_writeback_failure",
                when,
                1,
                str(payload.get("error") or payload)[:240],
                "watch",
                "Telegram/report path is primary; fix Sheet status writeback only if it recurs.",
            )
        elif event_type == "entry_fill_timeout_reconcile":
            add(
                "entry_fill_timeout_reconcile",
                when,
                1,
                str(payload.get("option_symbol") or payload.get("order_id") or payload)[:240],
                "working_as_recovery_path",
                "Keep as monitored plumbing; both historical timeouts later recovered.",
            )
        elif event_type == "protection_restore_skipped":
            add(
                "protection_restore_skip_guard",
                when,
                1,
                str(payload.get("reason") or payload)[:240],
                "mitigation_present",
                "This is a useful guard; verify it replaces repeated failed restore attempts.",
            )
        elif event_type == "trade_plan":
            reasons = payload.get("risk_reasons") or []
            details = payload.get("risk_details") or {}
            if reasons == ["insufficient_budget_for_single_contract"]:
                min_cost = _safe_float(details.get("min_contract_cost"))
                status = "mitigated_by_2000_shadow_cap" if min_cost is None or min_cost <= 2000 else "still_possible_above_2000"
                add(
                    "premium_cap_too_low",
                    when,
                    1,
                    f"{payload.get('symbol')} min_contract_cost={details.get('min_contract_cost')} max={details.get('max_premium')}",
                    status,
                    "Use $2000 for shadow; if >$2000 still blocks, record as tradability evidence rather than plumbing failure.",
                )
            elif reasons == ["insufficient_internal_settled_cash_budget"]:
                add(
                    "live_cash_guard_block",
                    when,
                    1,
                    f"required={details.get('required_cash')} remaining={details.get('remaining_budget')}",
                    "correct_live_guardrail",
                    "Do not disable for live; shadow bypass is already in dry-run/simulate path.",
                )
            elif reasons == ["execution_window_blocked"]:
                add(
                    "execution_window_blocked",
                    when,
                    1,
                    str(payload.get("deployment_id") or ""),
                    "review_if_repeats",
                    "Check strategy signal window vs compiled execution window if this recurs.",
                )
            elif reasons == ["max_open_positions_total_reached"]:
                add(
                    "position_limit_block",
                    when,
                    1,
                    str(payload.get("deployment_id") or ""),
                    "correct_risk_guardrail",
                    "Expected for live; shadow portfolio simulation should track separately.",
                )

    rows = list(families.values())
    for row in rows:
        row["recency_days"] = _recency_days(row["last_seen"], max_event_at)
    rows.sort(key=lambda row: (_status_rank(row["status"]), -int(row["count"]), str(row["family"])))
    return rows


def _render_report(
    *,
    db_path: Path,
    logs_dir: Path,
    lookback_days: int,
    events: list[dict[str, Any]],
    issue_rows: list[dict[str, Any]],
    day_rows: list[dict[str, Any]],
    trade_block_rows: list[dict[str, Any]],
    issue_csv: Path,
    day_csv: Path,
    trade_block_csv: Path,
    max_event_at: datetime | None,
) -> str:
    event_counts = Counter(event["event_type"] for event in events if event["event_type"] != "runtime_metric")
    signal_true = sum(
        1 for event in events
        if event["event_type"] == "signal_decision" and bool(event["payload"].get("signal"))
    )
    lines = [
        "# Bhiksha Historical Plumbing Triage",
        "",
        f"- generated_from_db: `{db_path}`",
        f"- logs_dir: `{logs_dir}`",
        f"- lookback_days: `{lookback_days}`",
        f"- latest_event_at: `{max_event_at.isoformat() if max_event_at else ''}`",
        f"- non_metric_events: `{sum(event_counts.values())}`",
        f"- true_signals: `{signal_true}`",
        f"- trade_plans: `{event_counts.get('trade_plan', 0)}`",
        f"- runtime_issues: `{event_counts.get('runtime_issue', 0)}`",
        f"- issue_csv: `{issue_csv}`",
        f"- day_csv: `{day_csv}`",
        f"- trade_block_csv: `{trade_block_csv}`",
        "",
        "## Issue Families",
        "",
        "| Family | Count | Last Seen | Status | Next Action |",
        "|---|---:|---|---|---|",
    ]
    for row in issue_rows:
        lines.append(
            f"| {row['family']} | {row['count']} | {row['last_seen']} | {row['status']} | {row['next_action']} |"
        )
    lines.extend(["", "## Daily Event Counts", "", "| Date | Non-Metric | Signals | Plans | Runtime Issues | Blocks |", "|---|---:|---:|---:|---:|---:|"])
    for row in day_rows:
        lines.append(
            f"| {row['date']} | {row['total_non_metric']} | {row.get('signal_decision', 0)} | "
            f"{row.get('trade_plan', 0)} | {row.get('runtime_issue', 0)} | {row.get('lifecycle_entry_blocked', 0)} |"
        )
    lines.extend(["", "## Top Trade Blocks", "", "| Reason | Count | Latest Example |", "|---|---:|---|"])
    reason_counts = Counter(row["reasons"] for row in trade_block_rows)
    latest_by_reason = {row["reasons"]: row for row in trade_block_rows}
    for reason, count in reason_counts.most_common(10):
        latest = latest_by_reason[reason]
        detail = f"{latest['created_at']} {latest['symbol']} {latest['deployment_id']}"
        if latest.get("min_contract_cost"):
            detail += f" min_cost={latest['min_contract_cost']}"
        lines.append(f"| {reason} | {count} | {detail} |")
    lines.extend(
        [
            "",
            "## Operator Read",
            "",
            "- Fixed or likely quiet: descriptor exhaustion, auth expiry, old sheet writeback SSL failure, low premium cap for most 7-21 DTE shadow contracts.",
            "- Needs deeper review before live promotion: protection restore conflicts around active close orders and repeated unprotected/protected lifecycle churn.",
            "- Watch daily: Schwab 429/400 data failures, entry fill timeout reconciliation, and any missing observation packets.",
            "- Not a bug by itself: live cash guard blocks and live position-limit blocks.",
        ]
    )
    return "\n".join(lines) + "\n"


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    headers = sorted({key for row in rows for key in row})
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)


def _parse_dt(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _date_from_log_name(name: str):
    match = re.search(r"(\d{4}-\d{2}-\d{2})", name)
    if not match:
        return None
    return datetime.strptime(match.group(1), "%Y-%m-%d").date()


def _safe_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _recency_days(last_seen: str, max_event_at: datetime | None) -> str:
    if not last_seen or max_event_at is None:
        return ""
    try:
        seen = datetime.strptime(last_seen, "%Y-%m-%d").replace(tzinfo=max_event_at.tzinfo)
    except ValueError:
        return ""
    return str(max(0, (max_event_at.date() - seen.date()).days))


def _status_rank(status: str) -> int:
    return {
        "needs_deeper_review": 0,
        "still_possible_above_2000": 1,
        "watch": 2,
        "needs_triage_if_repeats": 3,
        "review_if_repeats": 4,
        "mitigation_present": 5,
        "working_as_recovery_path": 6,
        "correct_live_guardrail": 7,
        "correct_risk_guardrail": 8,
        "mitigated_by_2000_shadow_cap": 9,
        "likely_fixed_or_quiet": 10,
        "likely_fixed_or_benign": 11,
    }.get(status, 50)


__all__ = ["BhikshaPlumbingTriageArtifacts", "build_bhiksha_plumbing_triage"]
