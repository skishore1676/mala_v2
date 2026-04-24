"""Local orchestration loop for Mala research operations.

The orchestrator is deliberately conservative. It consumes Research Ops'
deterministic next-action queue, executes only safe non-mutating steps, and
writes a reasoning brief for the agent/human checkpoint before any research
execution, retune, publish, board write, or repair.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.config import settings
from src.research.research_ops import (
    CONTROL_OPERATOR_ACTIONS,
    CONTROL_SHEET_HEADERS,
    DEFAULT_DISPOSITIONS_PATH,
    DEFAULT_CONTROL_SHEET_NAME,
    DEFAULT_HYPOTHESES_DIR,
    DEFAULT_OUT_DIR,
    DEFAULT_RUNS_DIR,
    NextAction,
    _build_with_optional_sheets,
    build_next_actions,
    build_control_rows,
)
from src.research.google_sheets import GoogleSheetTableClient


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_ORCH_DIR = DEFAULT_OUT_DIR / "orchestrator"
SAFE_AUTO_ACTIONS = {"retune_plan", "publish_pending", "sync_board"}


@dataclass(slots=True)
class OrchestratorResult:
    generated_at: str
    mode: str
    selected_action: dict[str, Any] | None
    executed: str
    command: str
    returncode: int | None
    stdout_tail: str
    stderr_tail: str
    reasoning_brief: str
    report_path: str
    json_path: str
    control_action: str = ""


def _timestamp() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def _file_stamp(value: str) -> str:
    return value.replace(":", "").replace("-", "").replace("+", "Z")


def _tail(text: str, *, max_chars: int = 4000) -> str:
    return text[-max_chars:] if len(text) > max_chars else text


def _shell_join(command: list[str]) -> str:
    return " ".join(command)


def _command_for_action(action: NextAction, args: argparse.Namespace) -> list[str] | None:
    python = sys.executable
    if action.action_type == "retune_plan":
        hypothesis_path = Path(args.hypotheses_dir) / f"{action.key}.md"
        hypothesis_arg = str(hypothesis_path) if hypothesis_path.exists() else action.key
        return [
            python,
            "-m",
            "src.research.research_runner",
            "retune-plan",
            "--hypothesis",
            hypothesis_arg,
        ]
    if action.action_type == "publish_pending":
        command = [
            python,
            "-m",
            "src.research.research_ops",
            "publish-pending",
            "--catalog-key",
            action.key,
            "--dry-run",
        ]
        if args.catalog_sheet_id:
            command.extend(["--catalog-sheet-id", args.catalog_sheet_id])
        if args.catalog_sheet_name:
            command.extend(["--catalog-sheet-name", args.catalog_sheet_name])
        if args.google_credentials:
            command.extend(["--google-credentials", args.google_credentials])
        if args.catalog_google_credentials:
            command.extend(["--catalog-google-credentials", args.catalog_google_credentials])
        return command
    if action.action_type == "sync_board":
        command = [
            python,
            "-m",
            "src.research.research_ops",
            "sync-board",
            "--dry-run",
        ]
        if args.board_sheet_id:
            command.extend(["--board-sheet-id", args.board_sheet_id])
        if args.board_scout_sheet:
            command.extend(["--board-scout-sheet", args.board_scout_sheet])
        if args.board_google_credentials:
            command.extend(["--board-google-credentials", args.board_google_credentials])
        return command
    return None


def _control_client(args: argparse.Namespace) -> GoogleSheetTableClient:
    credentials = args.control_google_credentials or args.google_credentials
    sheet_id = args.control_sheet_id or args.board_sheet_id
    if not sheet_id:
        raise SystemExit("--control-sheet-id or --board-sheet-id is required with --with-control-sheet")
    if not credentials:
        raise SystemExit("--google-credentials or --control-google-credentials is required with --with-control-sheet")
    return GoogleSheetTableClient(
        spreadsheet_id=sheet_id,
        sheet_name=args.control_sheet_name,
        credentials_path=Path(credentials),
    )


def _sync_control_sheet(
    *,
    client: GoogleSheetTableClient,
    actions: list[NextAction],
    generated_at: str,
) -> list[dict[str, Any]]:
    client.ensure_sheet_exists()
    existing = client.read_rows(range_suffix="A1:ZZ5000")
    rows = build_control_rows(actions=actions, generated_at=generated_at, existing_rows=existing)
    client.overwrite_table(headers=CONTROL_SHEET_HEADERS, rows=rows)
    return client.read_rows(range_suffix="A1:ZZ5000")


def _select_control_row(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    candidates: list[dict[str, Any]] = []
    for row in rows:
        operator_action = str(row.get("operator_action", "")).strip().upper()
        if operator_action and operator_action in CONTROL_OPERATOR_ACTIONS:
            candidates.append(row)
    candidates.sort(key=lambda row: int(row.get("rank") or 999999))
    return candidates[0] if candidates else None


def _action_from_control_row(row: dict[str, Any]) -> NextAction:
    return NextAction(
        rank=int(row.get("rank") or 0),
        priority=str(row.get("priority", "")),
        action_type=str(row.get("action_type", "")),
        key=str(row.get("key", "")),
        reason=str(row.get("reason", "")),
        suggested_command=str(row.get("suggested_command", "")),
        requires_approval=str(row.get("requires_approval", "yes")),
        mutates_external_state=str(row.get("mutates_external_state", "no")),
    )


def _command_for_control_row(row: dict[str, Any], args: argparse.Namespace) -> list[str] | None:
    action = _action_from_control_row(row)
    operator_action = str(row.get("operator_action", "")).strip().upper()
    python = sys.executable
    if operator_action == "SKIP":
        return None
    if operator_action == "APPROVE_RETUNE" and action.action_type == "retune_plan":
        hypothesis_path = Path(args.hypotheses_dir) / f"{action.key}.md"
        hypothesis_arg = str(hypothesis_path) if hypothesis_path.exists() else action.key
        return [
            python,
            "-m",
            "src.research.research_runner",
            "retune-approved",
            "--hypothesis",
            hypothesis_arg,
        ]
    if operator_action == "MARK_STALE" and action.action_type in {"repair_run_summary", "inspect_terminal"}:
        category = "run_missing_summary" if action.action_type == "repair_run_summary" else "terminal_without_artifacts"
        return [
            python,
            "-m",
            "src.research.research_ops",
            "mark-stale",
            "--category",
            category,
            "--key",
            action.key,
            "--reason",
            "operator marked stale from Research_Control",
            "--operator",
            "research_control",
        ]
    if operator_action == "APPROVE_PUBLISH" and action.action_type == "publish_pending":
        command = [
            python,
            "-m",
            "src.research.research_ops",
            "publish-pending",
            "--catalog-key",
            action.key,
            "--apply",
        ]
        if args.catalog_sheet_id:
            command.extend(["--catalog-sheet-id", args.catalog_sheet_id])
        if args.catalog_sheet_name:
            command.extend(["--catalog-sheet-name", args.catalog_sheet_name])
        if args.google_credentials:
            command.extend(["--google-credentials", args.google_credentials])
        if args.catalog_google_credentials:
            command.extend(["--catalog-google-credentials", args.catalog_google_credentials])
        return command
    if operator_action == "APPROVE_BOARD_SYNC" and action.action_type == "sync_board":
        command = [
            python,
            "-m",
            "src.research.research_ops",
            "sync-board",
            "--task-id",
            action.key,
            "--apply",
        ]
        if args.board_sheet_id:
            command.extend(["--board-sheet-id", args.board_sheet_id])
        if args.board_scout_sheet:
            command.extend(["--board-scout-sheet", args.board_scout_sheet])
        if args.board_google_credentials:
            command.extend(["--board-google-credentials", args.board_google_credentials])
        return command
    return None


def _update_control_row(
    *,
    client: GoogleSheetTableClient,
    row: dict[str, Any],
    status: str,
    report_path: str,
    clear_operator_action: bool,
) -> None:
    if not row:
        return
    next_row = dict(row)
    next_row["status"] = status
    next_row["last_report_path"] = report_path
    next_row["updated_at"] = _timestamp()
    if clear_operator_action:
        next_row["operator_action"] = ""
    client.batch_update_rows(
        rows=[next_row],
        columns=["operator_action", "status", "last_report_path", "updated_at"],
    )


def _reasoning_brief(action: NextAction | None, executed: str) -> str:
    if action is None:
        return "No queued action. Agent reasoning checkpoint: review whether new hypotheses should be scouted."

    if action.action_type == "retune_plan":
        return (
            "Agent reasoning checkpoint: inspect the latest artifacts for this retune candidate, "
            "explain why the prior run failed or thinned out, and propose a bounded retune before "
            "running `retune-approved`."
        )
    if action.action_type == "run_m1":
        return (
            "Agent reasoning checkpoint: confirm the hypothesis is config-only, check for similar "
            "dead ends, then ask for approval before running M1."
        )
    if action.action_type == "publish_pending":
        return (
            "Agent reasoning checkpoint: dry-run only has been allowed. Review duplicate lanes, "
            "exit reliability, and Bhiksha readiness before applying Strategy_Catalog writes."
        )
    if action.action_type == "sync_board":
        return (
            "Agent reasoning checkpoint: dry-run only has been allowed. Confirm the board update "
            "matches Mala local truth before applying Google Sheet changes."
        )
    if action.action_type == "repair_run_summary":
        return (
            "Agent reasoning checkpoint: inspect the run artifacts and decide whether to repair a "
            "summary artifact, rerun reporting, or mark the run as unusable evidence."
        )
    if action.action_type == "inspect_terminal":
        return (
            "Agent reasoning checkpoint: inspect the hypothesis history before trusting the terminal "
            "state because no artifact directory backs it."
        )
    if action.action_type == "resume_or_normalize":
        return (
            "Agent reasoning checkpoint: inspect current state/decision and choose explicit resume "
            "or normalize the hypothesis before new work."
        )
    return f"Agent reasoning checkpoint: selected action `{action.action_type}` was {executed}."


def _write_reports(result: OrchestratorResult, out_dir: Path) -> OrchestratorResult:
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = _file_stamp(result.generated_at)
    md_path = out_dir / f"orchestrator-{stamp}.md"
    json_path = out_dir / f"orchestrator-{stamp}.json"
    selected = result.selected_action or {}
    lines = [
        "# Mala Local Orchestrator",
        "",
        f"- generated_at: `{result.generated_at}`",
        f"- mode: `{result.mode}`",
        f"- executed: `{result.executed}`",
        f"- control_action: `{result.control_action}`",
        f"- command: `{result.command}`",
        f"- returncode: `{result.returncode if result.returncode is not None else ''}`",
        "",
        "## Selected Action",
        "",
        f"- rank: `{selected.get('rank', '')}`",
        f"- priority: `{selected.get('priority', '')}`",
        f"- action_type: `{selected.get('action_type', '')}`",
        f"- key: `{selected.get('key', '')}`",
        f"- reason: {selected.get('reason', '')}",
        "",
        "## Reasoning Brief",
        "",
        result.reasoning_brief,
    ]
    if result.stdout_tail:
        lines.extend(["", "## Stdout Tail", "", "```text", result.stdout_tail, "```"])
    if result.stderr_tail:
        lines.extend(["", "## Stderr Tail", "", "```text", result.stderr_tail, "```"])

    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    payload = asdict(result)
    payload["report_path"] = str(md_path)
    payload["json_path"] = str(json_path)
    json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return OrchestratorResult(
        generated_at=result.generated_at,
        mode=result.mode,
        selected_action=result.selected_action,
        executed=result.executed,
        command=result.command,
        returncode=result.returncode,
        stdout_tail=result.stdout_tail,
        stderr_tail=result.stderr_tail,
        reasoning_brief=result.reasoning_brief,
        report_path=str(md_path),
        json_path=str(json_path),
        control_action=result.control_action,
    )


def run_once(args: argparse.Namespace) -> OrchestratorResult:
    ledger = _build_with_optional_sheets(args)
    actions = build_next_actions(ledger)
    selected = actions[0] if actions else None
    generated_at = _timestamp()
    control_client: GoogleSheetTableClient | None = None
    control_row: dict[str, Any] | None = None
    control_action = ""

    if args.with_control_sheet:
        control_client = _control_client(args)
        control_rows = _sync_control_sheet(
            client=control_client,
            actions=actions,
            generated_at=generated_at,
        )
        control_row = _select_control_row(control_rows)
        if control_row:
            selected = _action_from_control_row(control_row)
            control_action = str(control_row.get("operator_action", "")).strip().upper()

    executed = "none"
    command_text = ""
    returncode: int | None = None
    stdout_tail = ""
    stderr_tail = ""

    if selected is not None:
        command = _command_for_control_row(control_row, args) if control_row else _command_for_action(selected, args)
        if command is None:
            executed = "skipped_by_control" if control_action == "SKIP" else "blocked_for_reasoning"
        elif args.mode == "dry-run":
            executed = "planned"
            command_text = _shell_join(command)
        elif args.mode == "apply-safe" and (control_row or selected.action_type in SAFE_AUTO_ACTIONS):
            completed = subprocess.run(
                command,
                cwd=REPO_ROOT,
                check=False,
                capture_output=True,
                text=True,
            )
            executed = "ran_safe_command"
            command_text = _shell_join(command)
            returncode = completed.returncode
            stdout_tail = _tail(completed.stdout)
            stderr_tail = _tail(completed.stderr)
        else:
            executed = "blocked_for_approval"
            command_text = _shell_join(command)

    result = OrchestratorResult(
        generated_at=generated_at,
        mode=args.mode,
        selected_action=asdict(selected) if selected else None,
        executed=executed,
        command=command_text,
        returncode=returncode,
        stdout_tail=stdout_tail,
        stderr_tail=stderr_tail,
        reasoning_brief=_reasoning_brief(selected, executed),
        report_path="",
        json_path="",
        control_action=control_action,
    )
    result = _write_reports(result, Path(args.orchestrator_out_dir))
    if control_client is not None and control_row is not None:
        status = executed
        if returncode not in (None, 0):
            status = f"failed:{returncode}"
        _update_control_row(
            client=control_client,
            row=control_row,
            status=status,
            report_path=result.report_path,
            clear_operator_action=args.mode == "apply-safe",
        )
    return result


def cmd_once(args: argparse.Namespace) -> int:
    result = run_once(args)
    print(f"ORCHESTRATOR_REPORT={result.report_path}")
    print(f"ORCHESTRATOR_JSON={result.json_path}")
    print(f"ORCHESTRATOR_EXECUTED={result.executed}")
    if result.selected_action:
        print(f"ORCHESTRATOR_ACTION={result.selected_action.get('action_type')}:{result.selected_action.get('key')}")
    else:
        print("ORCHESTRATOR_ACTION=none")
    if result.control_action:
        print(f"ORCHESTRATOR_CONTROL_ACTION={result.control_action}")
    return int(result.returncode or 0)


def cmd_daemon(args: argparse.Namespace) -> int:
    iterations = 0
    while args.max_iterations <= 0 or iterations < args.max_iterations:
        result = run_once(args)
        print(
            f"{result.generated_at} executed={result.executed} "
            f"action={(result.selected_action or {}).get('action_type', 'none')}"
        )
        iterations += 1
        if args.max_iterations > 0 and iterations >= args.max_iterations:
            break
        time.sleep(max(1, int(args.interval_seconds)))
    return 0


def _add_common_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--hypotheses-dir", default=str(DEFAULT_HYPOTHESES_DIR))
    parser.add_argument("--runs-dir", default=str(DEFAULT_RUNS_DIR))
    parser.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR))
    parser.add_argument("--orchestrator-out-dir", default=str(DEFAULT_ORCH_DIR))
    parser.add_argument(
        "--dispositions-path",
        default=str(DEFAULT_DISPOSITIONS_PATH),
    )
    parser.add_argument("--google-credentials", default=settings.google_api_credentials_path)
    parser.add_argument("--catalog-google-credentials", default="")
    parser.add_argument("--catalog-sheet-id", default=settings.strategy_catalog_sheet_id)
    parser.add_argument("--catalog-sheet-name", default=settings.strategy_catalog_sheet_name)
    parser.add_argument("--board-google-credentials", default="")
    parser.add_argument("--board-sheet-id", default="")
    parser.add_argument("--board-scout-sheet", default="Scout_Queue")
    parser.add_argument("--control-google-credentials", default="")
    parser.add_argument("--control-sheet-id", default="")
    parser.add_argument("--control-sheet-name", default=DEFAULT_CONTROL_SHEET_NAME)
    parser.add_argument("--with-control-sheet", action="store_true")
    parser.add_argument("--with-catalog", action="store_true")
    parser.add_argument("--with-board", action="store_true")
    parser.add_argument(
        "--mode",
        choices=["dry-run", "apply-safe"],
        default="dry-run",
        help="dry-run writes a plan only; apply-safe runs only non-mutating/dry-run commands.",
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    once = subparsers.add_parser("once", help="Run one orchestration selection cycle.")
    _add_common_args(once)
    once.set_defaults(func=cmd_once)

    daemon = subparsers.add_parser("daemon", help="Run repeated orchestration cycles.")
    _add_common_args(daemon)
    daemon.add_argument("--interval-seconds", type=int, default=1800)
    daemon.add_argument("--max-iterations", type=int, default=0)
    daemon.set_defaults(func=cmd_daemon)

    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
