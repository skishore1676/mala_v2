"""Research operations ledger and hot-start tools.

This module keeps Mala's research memory reconstructable from local evidence:
hypothesis markdown files plus run artifacts under data/results/hypothesis_runs.
Google Sheets can mirror summaries, but local artifacts remain canonical.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.config import settings
from src.research.google_sheets import GoogleSheetTableClient


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_HYPOTHESES_DIR = REPO_ROOT / "research" / "hypotheses"
DEFAULT_RUNS_DIR = REPO_ROOT / "data" / "results" / "hypothesis_runs"
DEFAULT_OUT_DIR = REPO_ROOT / "data" / "results" / "research_ops"

STAGE_FILES = {
    "M1": ("M1_top.csv", "M1_aggregate.csv", "M1_detail.csv"),
    "M2": ("M2_promoted.csv", "M2_gate_report.csv", "M2_convergence.csv"),
    "M3": ("M3_walk_forward.csv",),
    "M4": ("M4_promoted.csv", "M4_holdout.csv"),
    "M5": ("M5_execution.csv",),
}


@dataclass(slots=True)
class HypothesisLedgerRow:
    hypothesis_id: str
    file_path: str
    state: str
    decision: str
    symbol_scope: str
    strategy: str
    max_stage: str
    last_run: str
    latest_run_ts: str
    latest_stage: str
    latest_artifact_dir: str
    run_count: int
    catalog_candidate_count: int


@dataclass(slots=True)
class RunLedgerRow:
    hypothesis_id: str
    run_ts: str
    artifact_dir: str
    stages_detected: str
    terminal_stage: str
    decision: str
    summary_path: str
    catalog_selected_count: int
    m5_execution_rows: int
    m4_promoted_rows: int
    m2_promoted_rows: int
    artifact_files: str


@dataclass(slots=True)
class PromotedLedgerRow:
    catalog_key: str
    hypothesis_id: str
    run_ts: str
    artifact_dir: str
    ticker: str
    direction: str
    strategy: str
    execution_profile: str
    recommendation_tier: str
    exit_reliability: str
    selected_exit_policy: str
    mc_prob_positive_exp: str
    mc_exp_r_p50: str
    base_exp_r: str
    holdout_trades: str
    holdout_win_rate: str
    in_strategy_catalog: str = ""


@dataclass(slots=True)
class HotStartFinding:
    severity: str
    category: str
    key: str
    detail: str
    next_action: str


@dataclass(slots=True)
class ResearchLedger:
    generated_at: str
    hypotheses: list[HypothesisLedgerRow]
    runs: list[RunLedgerRow]
    promoted: list[PromotedLedgerRow]
    findings: list[HotStartFinding]


def _field(text: str, name: str, default: str = "") -> str:
    match = re.search(rf"^- {re.escape(name)}:\s*`([^`]*)`", text, re.MULTILINE)
    return match.group(1).strip() if match else default


def _read_hypothesis_file(path: Path) -> dict[str, str]:
    text = path.read_text(encoding="utf-8")
    return {
        "hypothesis_id": _field(text, "id", path.stem),
        "state": _field(text, "state", "pending"),
        "decision": _field(text, "decision", ""),
        "symbol_scope": _field(text, "symbol_scope", "SPY"),
        "strategy": _field(text, "strategy", ""),
        "max_stage": _field(text, "max_stage", "M5"),
        "last_run": _field(text, "last_run", ""),
    }


def _csv_row_count(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open("r", encoding="utf-8", newline="") as handle:
        return max(0, sum(1 for _ in handle) - 1)


def _read_csv_dicts(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _detected_stages(run_dir: Path) -> list[str]:
    stages: list[str] = []
    for stage, filenames in STAGE_FILES.items():
        if any((run_dir / filename).exists() for filename in filenames):
            stages.append(stage)
    return stages


def _summary_decision(summary_path: Path) -> str:
    if not summary_path.exists():
        return ""
    text = summary_path.read_text(encoding="utf-8", errors="replace")
    match = re.search(r"^- decision:\s*`([^`]*)`", text, re.MULTILINE)
    return match.group(1).strip() if match else ""


def _run_dirs_for(runs_dir: Path, hypothesis_id: str) -> list[Path]:
    root = runs_dir / hypothesis_id
    if not root.exists():
        return []
    return sorted([path for path in root.iterdir() if path.is_dir()])


def _relative(path: Path) -> str:
    try:
        return str(path.relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def build_ledger(
    *,
    hypotheses_dir: Path = DEFAULT_HYPOTHESES_DIR,
    runs_dir: Path = DEFAULT_RUNS_DIR,
    strategy_catalog_rows: list[dict[str, Any]] | None = None,
) -> ResearchLedger:
    generated_at = datetime.now(UTC).replace(microsecond=0).isoformat()
    catalog_keys = {
        str(row.get("catalog_key", "")).strip()
        for row in strategy_catalog_rows or []
        if str(row.get("catalog_key", "")).strip()
    }

    hypotheses: list[HypothesisLedgerRow] = []
    runs: list[RunLedgerRow] = []
    promoted: list[PromotedLedgerRow] = []

    for hyp_path in sorted(hypotheses_dir.glob("*.md")):
        if hyp_path.name == "TEMPLATE.md":
            continue
        meta = _read_hypothesis_file(hyp_path)
        hypothesis_id = meta["hypothesis_id"]
        run_dirs = _run_dirs_for(runs_dir, hypothesis_id)
        latest = run_dirs[-1] if run_dirs else None

        for run_dir in run_dirs:
            stages = _detected_stages(run_dir)
            artifact_files = sorted(path.name for path in run_dir.iterdir() if path.is_file())
            run_ts = run_dir.name
            summary_path = run_dir / "RUN_SUMMARY.md"
            catalog_rows = _read_csv_dicts(run_dir / "CATALOG_SELECTED.csv")
            for row in catalog_rows:
                catalog_key = str(row.get("catalog_key", "")).strip()
                promoted.append(
                    PromotedLedgerRow(
                        catalog_key=catalog_key,
                        hypothesis_id=hypothesis_id,
                        run_ts=run_ts,
                        artifact_dir=_relative(run_dir),
                        ticker=str(row.get("ticker", "")),
                        direction=str(row.get("direction", "")),
                        strategy=str(row.get("strategy", "")),
                        execution_profile=str(row.get("execution_profile", "")),
                        recommendation_tier=str(row.get("recommendation_tier", "")),
                        exit_reliability=str(row.get("exit_reliability", "")),
                        selected_exit_policy=str(row.get("selected_exit_policy", "")),
                        mc_prob_positive_exp=str(row.get("mc_prob_positive_exp", "")),
                        mc_exp_r_p50=str(row.get("mc_exp_r_p50", "")),
                        base_exp_r=str(row.get("base_exp_r", "")),
                        holdout_trades=str(row.get("holdout_trades", "")),
                        holdout_win_rate=str(row.get("holdout_win_rate", "")),
                        in_strategy_catalog=(
                            "yes" if catalog_key and catalog_key in catalog_keys
                            else "no" if catalog_keys else ""
                        ),
                    )
                )

            runs.append(
                RunLedgerRow(
                    hypothesis_id=hypothesis_id,
                    run_ts=run_ts,
                    artifact_dir=_relative(run_dir),
                    stages_detected=" -> ".join(stages),
                    terminal_stage=stages[-1] if stages else "none",
                    decision=_summary_decision(summary_path),
                    summary_path=_relative(summary_path) if summary_path.exists() else "",
                    catalog_selected_count=len(catalog_rows),
                    m5_execution_rows=_csv_row_count(run_dir / "M5_execution.csv"),
                    m4_promoted_rows=_csv_row_count(run_dir / "M4_promoted.csv"),
                    m2_promoted_rows=_csv_row_count(run_dir / "M2_promoted.csv"),
                    artifact_files=", ".join(artifact_files),
                )
            )

        latest_stages = _detected_stages(latest) if latest else []
        hypotheses.append(
            HypothesisLedgerRow(
                hypothesis_id=hypothesis_id,
                file_path=_relative(hyp_path),
                state=meta["state"],
                decision=meta["decision"],
                symbol_scope=meta["symbol_scope"],
                strategy=meta["strategy"],
                max_stage=meta["max_stage"],
                last_run=meta["last_run"],
                latest_run_ts=latest.name if latest else "",
                latest_stage=latest_stages[-1] if latest_stages else "none",
                latest_artifact_dir=_relative(latest) if latest else "",
                run_count=len(run_dirs),
                catalog_candidate_count=sum(
                    1 for row in promoted if row.hypothesis_id == hypothesis_id
                ),
            )
        )

    findings = build_hot_start_findings(
        hypotheses=hypotheses,
        runs=runs,
        promoted=promoted,
    )
    return ResearchLedger(
        generated_at=generated_at,
        hypotheses=hypotheses,
        runs=runs,
        promoted=promoted,
        findings=findings,
    )


def build_hot_start_findings(
    *,
    hypotheses: list[HypothesisLedgerRow],
    runs: list[RunLedgerRow],
    promoted: list[PromotedLedgerRow],
    board_rows: list[dict[str, Any]] | None = None,
) -> list[HotStartFinding]:
    findings: list[HotStartFinding] = []
    latest_by_hyp = {row.hypothesis_id: row for row in hypotheses}

    for row in hypotheses:
        if row.state == "running":
            findings.append(
                HotStartFinding(
                    severity="high",
                    category="running_hypothesis",
                    key=row.hypothesis_id,
                    detail=f"state=running decision={row.decision or '<empty>'} latest_stage={row.latest_stage}",
                    next_action="Resume with explicit --max-stage or normalize state before new work.",
                )
            )
        if row.state in {"completed", "kill", "retune"} and not row.latest_artifact_dir:
            findings.append(
                HotStartFinding(
                    severity="medium",
                    category="terminal_without_artifacts",
                    key=row.hypothesis_id,
                    detail=f"state={row.state} but no run directory was found.",
                    next_action="Inspect hypothesis history before trusting this terminal state.",
                )
            )

    for run in runs:
        if run.terminal_stage in {"M2", "M3", "M4", "M5"} and not run.summary_path:
            findings.append(
                HotStartFinding(
                    severity="high",
                    category="run_missing_summary",
                    key=f"{run.hypothesis_id}/{run.run_ts}",
                    detail=f"Run reached {run.terminal_stage} but RUN_SUMMARY.md is absent.",
                    next_action="Repair or rerun the reporting step before using this run as evidence.",
                )
            )

    latest_promoted_by_key: dict[str, PromotedLedgerRow] = {}
    for row in promoted:
        if row.catalog_key:
            latest_promoted_by_key[row.catalog_key] = row

    for row in latest_promoted_by_key.values():
        if row.in_strategy_catalog == "no":
            findings.append(
                HotStartFinding(
                    severity="medium",
                    category="catalog_publish_pending",
                    key=row.catalog_key,
                    detail=f"{row.ticker} {row.direction} {row.strategy} promoted in {row.run_ts} but absent from Strategy_Catalog.",
                    next_action="Review dedupe by symbol/direction/strategy, then publish if still valid.",
                )
            )

    for board_row in board_rows or []:
        task_id = str(board_row.get("Task_ID", "")).strip()
        if not task_id:
            continue
        matched = _match_board_row_to_hypothesis(board_row, latest_by_hyp)
        if matched is None:
            continue
        operator_action = str(board_row.get("Operator_Action", "")).strip()
        agent_state = str(board_row.get("Agent_State", "")).strip()
        if matched.state in {"completed", "kill", "retune"} and (
            operator_action or agent_state.upper().startswith(("APPROVED", "RUNNING"))
        ):
            findings.append(
                HotStartFinding(
                    severity="medium",
                    category="board_state_stale",
                    key=task_id,
                    detail=(
                        f"Board row maps to {matched.hypothesis_id}, now "
                        f"state={matched.state} decision={matched.decision}."
                    ),
                    next_action="Sync Scout_Queue/Quant_Ledger from Mala ledger.",
                )
            )

    return sorted(findings, key=lambda item: (item.severity != "high", item.category, item.key))


def _match_board_row_to_hypothesis(
    board_row: dict[str, Any],
    latest_by_hypothesis: dict[str, HypothesisLedgerRow],
) -> HypothesisLedgerRow | None:
    haystack = " ".join(
        str(board_row.get(key, ""))
        for key in ("Task_ID", "Strategy_Name", "Asset_Focus", "Hypothesis", "Suggested_Config")
    ).lower()
    for hypothesis_id, row in latest_by_hypothesis.items():
        if hypothesis_id.lower() in haystack:
            return row
    return None


def write_csv_tables(ledger: ResearchLedger, out_dir: Path) -> dict[str, Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    tables: dict[str, list[dict[str, Any]]] = {
        "hypotheses": [asdict(row) for row in ledger.hypotheses],
        "runs": [asdict(row) for row in ledger.runs],
        "promoted": [asdict(row) for row in ledger.promoted],
        "hot_start": [asdict(row) for row in ledger.findings],
    }
    paths: dict[str, Path] = {}
    for name, rows in tables.items():
        path = out_dir / f"{name}.csv"
        _write_csv(path, rows)
        paths[name] = path
    return paths


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    headers = list(rows[0].keys()) if rows else []
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        if headers:
            writer.writeheader()
            writer.writerows(rows)


def write_workbook(ledger: ResearchLedger, path: Path) -> Path:
    import xlsxwriter

    path.parent.mkdir(parents=True, exist_ok=True)
    workbook = xlsxwriter.Workbook(str(path), {"nan_inf_to_errors": True})
    header_fmt = workbook.add_format({"bold": True, "bg_color": "#1F4E78", "font_color": "white"})
    title_fmt = workbook.add_format({"bold": True, "font_size": 16})
    count_fmt = workbook.add_format({"bold": True, "bg_color": "#D9EAF7"})

    summary = workbook.add_worksheet("Summary")
    summary.write("A1", "Mala Research Ledger", title_fmt)
    summary.write("A2", "Generated At")
    summary.write("B2", ledger.generated_at)
    state_counts = Counter(row.state for row in ledger.hypotheses)
    decision_counts = Counter(row.decision for row in ledger.hypotheses)
    metrics = [
        ("Hypotheses", len(ledger.hypotheses)),
        ("Runs", len(ledger.runs)),
        ("Promoted Candidates", len(ledger.promoted)),
        ("Hot-Start Findings", len(ledger.findings)),
        ("Completed", state_counts.get("completed", 0)),
        ("Killed", state_counts.get("kill", 0)),
        ("Retune", state_counts.get("retune", 0)),
        ("Running", state_counts.get("running", 0)),
        ("Promote Decisions", decision_counts.get("promote", 0)),
    ]
    for idx, (label, value) in enumerate(metrics, start=4):
        summary.write(idx, 0, label, count_fmt)
        summary.write(idx, 1, value)
    summary.set_column(0, 0, 26)
    summary.set_column(1, 1, 22)

    _write_sheet(workbook, "Hypotheses", [asdict(row) for row in ledger.hypotheses], header_fmt)
    _write_sheet(workbook, "Runs", [asdict(row) for row in ledger.runs], header_fmt)
    _write_sheet(workbook, "Promoted", [asdict(row) for row in ledger.promoted], header_fmt)
    _write_sheet(workbook, "Hot_Start", [asdict(row) for row in ledger.findings], header_fmt)

    workbook.close()
    return path


def _write_sheet(
    workbook: Any,
    sheet_name: str,
    rows: list[dict[str, Any]],
    header_fmt: Any,
) -> None:
    worksheet = workbook.add_worksheet(sheet_name[:31])
    if not rows:
        worksheet.write(0, 0, "No rows")
        return
    headers = list(rows[0].keys())
    for col_idx, header in enumerate(headers):
        worksheet.write(0, col_idx, header, header_fmt)
    for row_idx, row in enumerate(rows, start=1):
        for col_idx, header in enumerate(headers):
            value = row.get(header, "")
            if isinstance(value, (int, float)):
                worksheet.write_number(row_idx, col_idx, value)
            else:
                worksheet.write_string(row_idx, col_idx, "" if value is None else str(value))
    worksheet.freeze_panes(1, 0)
    worksheet.autofilter(0, 0, max(len(rows), 1), len(headers) - 1)
    for col_idx, header in enumerate(headers):
        max_len = max(len(header), *(len(str(row.get(header, ""))) for row in rows[:500]))
        worksheet.set_column(col_idx, col_idx, min(max(max_len + 2, 10), 60))


def write_hot_start_report(
    *,
    ledger: ResearchLedger,
    path: Path,
) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Mala Research Hot Start",
        "",
        f"- generated_at: `{ledger.generated_at}`",
        f"- hypotheses: `{len(ledger.hypotheses)}`",
        f"- runs: `{len(ledger.runs)}`",
        f"- promoted_candidates: `{len(ledger.promoted)}`",
        f"- findings: `{len(ledger.findings)}`",
        "",
        "## Findings",
    ]
    if not ledger.findings:
        lines.append("- No hot-start findings.")
    for finding in ledger.findings:
        lines.append(
            f"- **{finding.severity.upper()} / {finding.category}** `{finding.key}`: "
            f"{finding.detail} Next: {finding.next_action}"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def _read_strategy_catalog_rows(args: argparse.Namespace) -> list[dict[str, Any]]:
    credentials = args.catalog_google_credentials or args.google_credentials
    if not args.catalog_sheet_id or not credentials:
        return []
    client = GoogleSheetTableClient(
        spreadsheet_id=args.catalog_sheet_id,
        sheet_name=args.catalog_sheet_name,
        credentials_path=Path(credentials),
    )
    return client.read_rows(range_suffix="A1:ZZ5000")


def _read_board_rows(args: argparse.Namespace) -> list[dict[str, Any]]:
    credentials = args.board_google_credentials or args.google_credentials
    if not args.board_sheet_id or not credentials:
        return []
    client = GoogleSheetTableClient(
        spreadsheet_id=args.board_sheet_id,
        sheet_name=args.board_scout_sheet,
        credentials_path=Path(credentials),
    )
    return client.read_rows(range_suffix="A1:ZZ5000")


def _build_with_optional_sheets(args: argparse.Namespace) -> ResearchLedger:
    catalog_rows = _read_strategy_catalog_rows(args) if args.with_catalog else []
    ledger = build_ledger(
        hypotheses_dir=Path(args.hypotheses_dir),
        runs_dir=Path(args.runs_dir),
        strategy_catalog_rows=catalog_rows,
    )
    if args.with_board:
        board_rows = _read_board_rows(args)
        ledger.findings = build_hot_start_findings(
            hypotheses=ledger.hypotheses,
            runs=ledger.runs,
            promoted=ledger.promoted,
            board_rows=board_rows,
        )
    return ledger


def cmd_backfill(args: argparse.Namespace) -> int:
    ledger = _build_with_optional_sheets(args)
    out_dir = Path(args.out_dir)
    csv_dir = out_dir / "csv"
    workbook_path = Path(args.workbook) if args.workbook else out_dir / "research_ledger.xlsx"
    write_csv_tables(ledger, csv_dir)
    write_workbook(ledger, workbook_path)
    report_path = write_hot_start_report(ledger=ledger, path=out_dir / "hot_start.md")
    print(f"LEDGER_XLSX={workbook_path}")
    print(f"LEDGER_CSV_DIR={csv_dir}")
    print(f"HOT_START_REPORT={report_path}")
    print(f"HYPOTHESES={len(ledger.hypotheses)}")
    print(f"RUNS={len(ledger.runs)}")
    print(f"PROMOTED={len(ledger.promoted)}")
    print(f"FINDINGS={len(ledger.findings)}")
    return 0


def cmd_hot_start(args: argparse.Namespace) -> int:
    ledger = _build_with_optional_sheets(args)
    out_dir = Path(args.out_dir)
    report_path = write_hot_start_report(ledger=ledger, path=Path(args.report) if args.report else out_dir / "hot_start.md")
    _write_csv(out_dir / "hot_start.csv", [asdict(row) for row in ledger.findings])
    print(f"HOT_START_REPORT={report_path}")
    print(f"FINDINGS={len(ledger.findings)}")
    high = sum(1 for row in ledger.findings if row.severity == "high")
    print(f"HIGH_FINDINGS={high}")
    return 0


def _add_common_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--hypotheses-dir", default=str(DEFAULT_HYPOTHESES_DIR))
    parser.add_argument("--runs-dir", default=str(DEFAULT_RUNS_DIR))
    parser.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR))
    parser.add_argument("--google-credentials", default=settings.google_api_credentials_path)
    parser.add_argument("--catalog-google-credentials", default="")
    parser.add_argument("--catalog-sheet-id", default=settings.strategy_catalog_sheet_id)
    parser.add_argument("--catalog-sheet-name", default=settings.strategy_catalog_sheet_name)
    parser.add_argument("--board-google-credentials", default="")
    parser.add_argument("--board-sheet-id", default="")
    parser.add_argument("--board-scout-sheet", default="Scout_Queue")
    parser.add_argument("--with-catalog", action="store_true", help="Read Strategy_Catalog and mark promoted rows present/absent.")
    parser.add_argument("--with-board", action="store_true", help="Read Scout_Queue and include stale-board findings.")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    backfill = subparsers.add_parser("backfill", help="Rebuild the local research ledger from artifacts.")
    _add_common_args(backfill)
    backfill.add_argument("--workbook", default="")
    backfill.set_defaults(func=cmd_backfill)

    hot_start = subparsers.add_parser("hot-start", help="Write a hot-start reconciliation report.")
    _add_common_args(hot_start)
    hot_start.add_argument("--report", default="")
    hot_start.set_defaults(func=cmd_hot_start)

    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
