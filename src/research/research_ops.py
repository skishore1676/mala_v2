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
from src.research.catalog import upsert_strategy_catalog
from src.research.google_sheets import GoogleSheetTableClient
from src.research.research_runner import create_hypothesis_file
from src.research.search_space import build_search_configs, search_param_keys
from src.research.time_utils import sheet_timestamp
from src.strategy.factory import available_strategy_names


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_HYPOTHESES_DIR = REPO_ROOT / "research" / "hypotheses"
DEFAULT_RUNS_DIR = REPO_ROOT / "data" / "results" / "hypothesis_runs"
DEFAULT_OUT_DIR = REPO_ROOT / "data" / "results" / "research_ops"
DEFAULT_DISPOSITIONS_PATH = REPO_ROOT / "research" / "reports" / "research_ops" / "finding_dispositions.jsonl"
DEFAULT_CONTROL_SHEET_NAME = "Research_Control"
DEFAULT_INTAKE_SHEET_NAME = "Research_Intake"

CONTROL_SHEET_HEADERS = [
    "action_id",
    "rank",
    "priority",
    "action_type",
    "key",
    "reason",
    "suggested_command",
    "requires_approval",
    "mutates_external_state",
    "operator_action",
    "status",
    "brief_recommendation",
    "brief_summary",
    "brief_path",
    "last_report_path",
    "updated_at",
    "generated_at",
]

CONTROL_OPERATOR_ACTIONS = {
    "",
    "APPROVE_RETUNE",
    "APPROVE_PUBLISH",
    "APPROVE_BOARD_SYNC",
    "APPROVE_SURFACE_EXPANSION",
    "MARK_STALE",
    "SKIP",
}

INTAKE_SHEET_HEADERS = [
    "intake_id",
    "title",
    "hypothesis_id",
    "strategy",
    "symbol_scope",
    "thesis",
    "rules",
    "notes",
    "max_stage",
    "operator_action",
    "status",
    "feasibility_tag",
    "feasibility_summary",
    "search_param_keys",
    "discovery_config_count",
    "retune_config_count",
    "hypothesis_path",
    "report_path",
    "updated_at",
    "created_at",
]

INTAKE_OPERATOR_ACTIONS = {
    "",
    "EVALUATE",
    "APPROVE_CREATE_HYPOTHESIS",
    "SKIP",
}

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
class FindingDisposition:
    created_at: str
    status: str
    key: str
    category: str
    reason: str
    operator: str = ""


@dataclass(slots=True)
class NextAction:
    rank: int
    priority: str
    action_type: str
    key: str
    reason: str
    suggested_command: str
    requires_approval: str
    mutates_external_state: str


@dataclass(slots=True)
class ActionBrief:
    generated_at: str
    action_id: str
    action_type: str
    key: str
    hypothesis_id: str
    recommendation: str
    suggested_operator_action: str
    summary: str
    suggested_command: str
    report_path: str
    evidence: list[str]
    surface_proposal: list[str]
    sources: list[str]


@dataclass(slots=True)
class SurfaceExpansionPlan:
    generated_at: str
    action_id: str
    key: str
    hypothesis_id: str
    strategy: str
    symbol_scope: str
    feasibility_tag: str
    recommendation: str
    next_operator_action: str
    summary: str
    proposed_bounds: list[str]
    rationale: list[str]
    validation_steps: list[str]
    sources: list[str]
    report_path: str
    json_path: str


@dataclass(slots=True)
class HypothesisIntakeEvaluation:
    intake_id: str
    title: str
    hypothesis_id: str
    strategy: str
    symbol_scope: str
    max_stage: str
    feasibility_tag: str
    feasibility_summary: str
    search_param_keys: str
    discovery_config_count: int
    retune_config_count: int
    hypothesis_path: str = ""
    report_path: str = ""


@dataclass(slots=True)
class ResearchDigest:
    generated_at: str
    days: int
    report_path: str
    hypotheses_by_state: dict[str, int]
    next_actions_by_type: dict[str, int]
    findings_by_category: dict[str, int]
    recent_runs: int
    pending_control_actions: int
    pending_intake_actions: int


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


def _read_text(path: Path, *, max_chars: int = 12000) -> str:
    if not path.exists():
        return ""
    text = path.read_text(encoding="utf-8", errors="replace")
    return text[:max_chars]


def _artifact_path(value: str) -> Path | None:
    if not value:
        return None
    path = Path(value)
    return path if path.is_absolute() else REPO_ROOT / path


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        text = str(value).strip().replace("%", "")
        return float(text)
    except (TypeError, ValueError):
        return default


def _truthy(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y", "pass", "passed"}


def _brief_cell(text: str, *, max_chars: int = 450) -> str:
    collapsed = re.sub(r"\s+", " ", text).strip()
    return collapsed[: max_chars - 3] + "..." if len(collapsed) > max_chars else collapsed


def _format_number(value: Any, *, digits: int = 4) -> str:
    if value in (None, ""):
        return ""
    number = _to_float(value, default=float("nan"))
    if number != number:
        return str(value)
    return f"{number:.{digits}f}"


def _slug(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.strip().lower()).strip("-")
    return slug or "untitled-hypothesis"


def _split_multiline_cell(value: Any) -> list[str]:
    text = str(value or "").strip()
    if not text:
        return []
    parts = re.split(r"[\n;]+", text)
    return [part.strip(" -\t") for part in parts if part.strip(" -\t")]


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


def read_dispositions(path: Path = DEFAULT_DISPOSITIONS_PATH) -> list[FindingDisposition]:
    """Read the append-only finding disposition ledger."""
    if not path.exists():
        return []
    rows: list[FindingDisposition] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            rows.append(
                FindingDisposition(
                    created_at=str(payload.get("created_at", "")),
                    status=str(payload.get("status", "")),
                    key=str(payload.get("key", "")),
                    category=str(payload.get("category", "")),
                    reason=str(payload.get("reason", "")),
                    operator=str(payload.get("operator", "")),
                )
            )
    return rows


def append_disposition(
    *,
    path: Path = DEFAULT_DISPOSITIONS_PATH,
    key: str,
    category: str = "",
    status: str,
    reason: str,
    operator: str = "",
) -> FindingDisposition:
    disposition = FindingDisposition(
        created_at=datetime.now(UTC).replace(microsecond=0).isoformat(),
        status=status,
        key=key,
        category=category,
        reason=reason,
        operator=operator,
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(asdict(disposition), sort_keys=True) + "\n")
    return disposition


def _latest_disposition_by_target(
    dispositions: list[FindingDisposition],
) -> dict[tuple[str, str], FindingDisposition]:
    latest: dict[tuple[str, str], FindingDisposition] = {}
    for disposition in dispositions:
        if not disposition.key:
            continue
        latest[(disposition.category, disposition.key)] = disposition
    return latest


def _finding_is_disposed(
    finding: HotStartFinding,
    dispositions: list[FindingDisposition],
) -> bool:
    latest = _latest_disposition_by_target(dispositions)
    disposition = latest.get((finding.category, finding.key)) or latest.get(("", finding.key))
    if disposition is None:
        return False
    return disposition.status in {"stale", "archived", "ignore"}


def build_ledger(
    *,
    hypotheses_dir: Path = DEFAULT_HYPOTHESES_DIR,
    runs_dir: Path = DEFAULT_RUNS_DIR,
    strategy_catalog_rows: list[dict[str, Any]] | None = None,
    dispositions: list[FindingDisposition] | None = None,
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
        dispositions=dispositions,
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
    dispositions: list[FindingDisposition] | None = None,
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
        if row.in_strategy_catalog == "no" and row.recommendation_tier in {"promote", "shadow"}:
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

    filtered = [
        finding
        for finding in findings
        if not _finding_is_disposed(finding, dispositions or [])
    ]
    return sorted(filtered, key=lambda item: (item.severity != "high", item.category, item.key))


def build_next_actions(ledger: ResearchLedger) -> list[NextAction]:
    """Turn the ledger and hot-start findings into a small operator queue."""
    actions: list[NextAction] = []
    seen: set[tuple[str, str]] = set()

    def add(
        *,
        priority: str,
        action_type: str,
        key: str,
        reason: str,
        suggested_command: str,
        requires_approval: str = "yes",
        mutates_external_state: str = "no",
    ) -> None:
        marker = (action_type, key)
        if marker in seen:
            return
        seen.add(marker)
        actions.append(
            NextAction(
                rank=0,
                priority=priority,
                action_type=action_type,
                key=key,
                reason=reason,
                suggested_command=suggested_command,
                requires_approval=requires_approval,
                mutates_external_state=mutates_external_state,
            )
        )

    for finding in ledger.findings:
        if finding.category == "catalog_publish_pending":
            add(
                priority="high" if finding.severity == "high" else "medium",
                action_type="publish_pending",
                key=finding.key,
                reason=finding.detail,
                suggested_command=(
                    "python -m src.research.research_ops publish-pending "
                    f"--catalog-key {finding.key} --dry-run"
                ),
                requires_approval="yes",
                mutates_external_state="yes",
            )
        elif finding.category == "board_state_stale":
            add(
                priority="medium",
                action_type="sync_board",
                key=finding.key,
                reason=finding.detail,
                suggested_command="python -m src.research.research_ops sync-board --dry-run",
                requires_approval="yes",
                mutates_external_state="yes",
            )
        elif finding.category == "run_missing_summary":
            add(
                priority="high",
                action_type="repair_run_summary",
                key=finding.key,
                reason=finding.detail,
                suggested_command=(
                    "python -m src.research.research_ops mark-stale "
                    f"--category run_missing_summary --key {finding.key} "
                    "--reason \"missing RUN_SUMMARY; old run; not used as evidence\""
                ),
                requires_approval="yes",
            )
        elif finding.category == "running_hypothesis":
            add(
                priority="high",
                action_type="resume_or_normalize",
                key=finding.key,
                reason=finding.detail,
                suggested_command=f"python -m src.research.research_runner continue-approved --hypothesis {finding.key}",
                requires_approval="yes",
            )
        elif finding.category == "terminal_without_artifacts":
            add(
                priority="medium",
                action_type="inspect_terminal",
                key=finding.key,
                reason=finding.detail,
                suggested_command=(
                    "python -m src.research.research_ops mark-stale "
                    f"--category terminal_without_artifacts --key {finding.key} "
                    "--reason \"terminal hypothesis has no artifacts; not used as evidence\""
                ),
                requires_approval="yes",
            )

    for row in ledger.hypotheses:
        if row.state == "pending":
            add(
                priority="medium",
                action_type="run_m1",
                key=row.hypothesis_id,
                reason=f"Pending hypothesis for {row.symbol_scope} / {row.strategy}.",
                suggested_command=(
                    "python -m src.research.research_runner run-m1 "
                    f"--hypothesis research/hypotheses/{row.hypothesis_id}.md"
                ),
                requires_approval="yes",
            )
        elif row.state == "retune":
            add(
                priority="medium",
                action_type="retune_plan",
                key=row.hypothesis_id,
                reason=f"Retune requested after latest_stage={row.latest_stage} decision={row.decision}.",
                suggested_command=(
                    "python -m src.research.research_runner retune-plan "
                    f"--hypothesis research/hypotheses/{row.hypothesis_id}.md"
                ),
                requires_approval="yes",
            )

    priority_order = {"high": 0, "medium": 1, "low": 2}
    action_order = {
        "publish_pending": 0,
        "sync_board": 1,
        "repair_run_summary": 2,
        "resume_or_normalize": 3,
        "retune_plan": 4,
        "run_m1": 5,
        "inspect_terminal": 6,
    }
    actions.sort(
        key=lambda row: (
            priority_order.get(row.priority, 9),
            action_order.get(row.action_type, 9),
            row.key,
        )
    )
    return [
        NextAction(
            rank=index,
            priority=row.priority,
            action_type=row.action_type,
            key=row.key,
            reason=row.reason,
            suggested_command=row.suggested_command,
            requires_approval=row.requires_approval,
            mutates_external_state=row.mutates_external_state,
        )
        for index, row in enumerate(actions, start=1)
    ]


def action_id(action: NextAction | dict[str, Any]) -> str:
    action_type = action.action_type if isinstance(action, NextAction) else str(action.get("action_type", ""))
    key = action.key if isinstance(action, NextAction) else str(action.get("key", ""))
    return f"{action_type}:{key}"


def build_control_rows(
    *,
    actions: list[NextAction],
    generated_at: str,
    existing_rows: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Build Research_Control rows while preserving operator-entered fields."""
    existing_by_id = {
        str(row.get("action_id", "")).strip(): row
        for row in existing_rows or []
        if str(row.get("action_id", "")).strip()
    }
    rows: list[dict[str, Any]] = []
    for item in actions:
        existing = existing_by_id.get(action_id(item), {})
        operator_action = str(existing.get("operator_action", "")).strip().upper()
        if operator_action not in CONTROL_OPERATOR_ACTIONS:
            operator_action = ""
        rows.append(
            {
                "action_id": action_id(item),
                "rank": item.rank,
                "priority": item.priority,
                "action_type": item.action_type,
                "key": item.key,
                "reason": item.reason,
                "suggested_command": item.suggested_command,
                "requires_approval": item.requires_approval,
                "mutates_external_state": item.mutates_external_state,
                "operator_action": operator_action,
                "status": str(existing.get("status", "") or "queued"),
                "brief_recommendation": str(existing.get("brief_recommendation", "")),
                "brief_summary": str(existing.get("brief_summary", "")),
                "brief_path": str(existing.get("brief_path", "")),
                "last_report_path": str(existing.get("last_report_path", "")),
                "updated_at": sheet_timestamp(),
                "generated_at": generated_at,
            }
        )
    return rows


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


def _split_action_key(value: str) -> tuple[str, str]:
    if ":" in value:
        action_type, key = value.split(":", 1)
        return action_type.strip(), key.strip()
    return "", value.strip()


def _find_action_for_brief(
    *,
    actions: list[NextAction],
    key: str,
    action_type: str = "",
) -> NextAction | None:
    for action in actions:
        if action.key != key:
            continue
        if action_type and action.action_type != action_type:
            continue
        return action
    return None


def _find_hypothesis(ledger: ResearchLedger, key: str) -> HypothesisLedgerRow | None:
    for row in ledger.hypotheses:
        if row.hypothesis_id == key:
            return row
    return None


def _latest_run(ledger: ResearchLedger, hypothesis_id: str) -> RunLedgerRow | None:
    rows = [row for row in ledger.runs if row.hypothesis_id == hypothesis_id]
    return sorted(rows, key=lambda row: row.run_ts)[-1] if rows else None


def _sorted_metric_rows(
    rows: list[dict[str, str]],
    *,
    primary: str,
    secondary: str = "avg_test_exp_r",
    limit: int = 3,
) -> list[dict[str, str]]:
    return sorted(
        rows,
        key=lambda row: (
            _to_float(row.get(primary), default=-999999),
            _to_float(row.get(secondary), default=-999999),
        ),
        reverse=True,
    )[:limit]


def _compact_config(row: dict[str, str]) -> str:
    skip = {
        "ticker",
        "strategy",
        "direction",
        "oos_windows",
        "oos_signals",
        "avg_test_exp_r",
        "pct_positive_oos_windows",
        "avg_test_confidence",
        "avg_test_mfe_mae_ratio",
        "m1_score",
        "observed_cost_points",
        "min_oos_windows",
        "min_oos_signals",
        "min_avg_test_exp_r",
        "mean_avg_test_exp_r",
        "min_pct_positive_oos_windows",
        "mean_pct_positive_oos_windows",
        "mean_test_confidence",
        "has_all_cost_points",
        "passes_window_gate",
        "passes_signal_gate",
        "passes_stability_gate",
        "passes_exp_gate",
        "passes_all_gates",
        "decision",
        "score",
    }
    parts = [f"{key}={value}" for key, value in row.items() if key not in skip and str(value).strip()]
    return ", ".join(parts[:8])


def _m1_evidence_lines(m1_rows: list[dict[str, str]]) -> list[str]:
    if not m1_rows:
        return ["M1_top.csv is absent or empty for the latest run."]
    lines = [f"M1_top.csv has {len(m1_rows)} candidate rows."]
    for row in _sorted_metric_rows(m1_rows, primary="m1_score"):
        lines.append(
            (
                "M1 top: "
                f"{row.get('ticker', '')} {row.get('direction', '')} "
                f"exp_r={_format_number(row.get('avg_test_exp_r'))} "
                f"pct_pos={_format_number(row.get('pct_positive_oos_windows'), digits=2)} "
                f"signals={row.get('oos_signals', '')} "
                f"config=({_compact_config(row)})"
            ).strip()
        )
    return lines


def _m2_evidence_lines(m2_rows: list[dict[str, str]]) -> list[str]:
    if not m2_rows:
        return ["M2_gate_report.csv is absent or empty for the latest run."]
    pass_all = sum(1 for row in m2_rows if _truthy(row.get("passes_all_gates")))
    pass_exp = sum(1 for row in m2_rows if _truthy(row.get("passes_exp_gate")))
    pass_stability = sum(1 for row in m2_rows if _truthy(row.get("passes_stability_gate")))
    lines = [
        (
            f"M2_gate_report.csv has {len(m2_rows)} rows; "
            f"passes_all={pass_all}, passes_exp={pass_exp}, passes_stability={pass_stability}."
        )
    ]
    for row in _sorted_metric_rows(m2_rows, primary="score", secondary="min_avg_test_exp_r"):
        lines.append(
            (
                "M2 best: "
                f"{row.get('ticker', '')} {row.get('direction', '')} "
                f"min_exp_r={_format_number(row.get('min_avg_test_exp_r'))} "
                f"min_pct_pos={_format_number(row.get('min_pct_positive_oos_windows'), digits=2)} "
                f"all={row.get('passes_all_gates', '')} "
                f"exp={row.get('passes_exp_gate', '')} "
                f"stability={row.get('passes_stability_gate', '')} "
                f"config=({_compact_config(row)})"
            ).strip()
        )
    return lines


def _surface_proposal(
    *,
    hypothesis: HypothesisLedgerRow | None,
    action_type: str,
    m1_rows: list[dict[str, str]],
    m2_rows: list[dict[str, str]],
) -> list[str]:
    if hypothesis is None:
        return ["No matching hypothesis row; inspect the action key before changing search space."]
    strategy = hypothesis.strategy.lower()
    if action_type != "retune_plan":
        return ["No parameter-surface change is proposed for this action type."]

    proposal: list[str] = []
    if m2_rows:
        pass_exp = [row for row in m2_rows if _truthy(row.get("passes_exp_gate"))]
        pass_stability = [row for row in m2_rows if _truthy(row.get("passes_stability_gate"))]
        pass_all = [row for row in m2_rows if _truthy(row.get("passes_all_gates"))]
        if pass_all:
            proposal.append("Surface does not need expansion before continuation; at least one M2 row already passed all gates.")
        elif pass_exp and not pass_stability:
            proposal.append("Evidence is edge-positive but cost/window stability is weak; prefer a bounded retune or surface expansion over blind rerun.")
        elif not pass_exp:
            proposal.append("M2 did not preserve positive expectancy across cost points; favor kill/skip unless M1 shows a clear alternate cluster.")

    best_source = _sorted_metric_rows(m1_rows, primary="m1_score", limit=1) or _sorted_metric_rows(
        m2_rows,
        primary="score",
        secondary="min_avg_test_exp_r",
        limit=1,
    )
    best = best_source[0] if best_source else {}
    if "market impulse" in strategy:
        config = _compact_config(best) if best else ""
        if config:
            proposal.append(f"Market Impulse tuning center from best evidence: {config}.")
        proposal.append(
            "If approving surface expansion, keep it config-only: tighten around observed timeframe/VWMA/threshold clusters, "
            "then rerun M1 before any M2 continuation."
        )
    elif best:
        proposal.append(f"Use the best observed config as the retune center: {_compact_config(best)}.")
    else:
        proposal.append("No candidate-level CSV evidence was found; rerun or mark stale before expanding search space.")
    return proposal


def _brief_recommendation(
    *,
    hypothesis: HypothesisLedgerRow | None,
    action: NextAction | None,
    m1_rows: list[dict[str, str]],
    m2_rows: list[dict[str, str]],
    summary_text: str,
) -> tuple[str, str, str]:
    action_type = action.action_type if action else ""
    if action_type == "publish_pending":
        return "PUBLISH_REVIEW", "APPROVE_PUBLISH", "Catalog write is external state; dedupe and review execution fields before applying."
    if action_type == "sync_board":
        return "BOARD_SYNC_REVIEW", "APPROVE_BOARD_SYNC", "Board state is stale relative to Mala; sync after confirming the matched row."
    if action_type in {"repair_run_summary", "inspect_terminal"}:
        return "MARK_STALE_OR_REPAIR", "MARK_STALE", "Evidence is incomplete; mark stale if the artifact is not needed, otherwise repair before use."
    if action_type == "run_m1":
        return "RUN_M1_REVIEW", "SKIP", "Pending hypothesis needs human/agent thesis review before first execution."

    if hypothesis is None:
        return "INSPECT", "SKIP", "Action key does not map to a local hypothesis; inspect before execution."
    combined = f"{hypothesis.state} {hypothesis.decision} {summary_text}".lower()
    if hypothesis.state == "kill":
        return "NO_ACTION", "SKIP", "Hypothesis is already kill; do not spend more research cycles unless a new thesis is written."
    if hypothesis.state == "completed":
        return "PUBLISH_REVIEW", "APPROVE_PUBLISH", "Hypothesis is completed; use publish review only if a selected catalog row is missing."

    if action_type == "retune_plan":
        if "m1 fail: no positive configs" in combined:
            return "KILL_OR_SKIP", "SKIP", "Latest retune found no positive configs; archive or kill unless the thesis changes materially."
        if "m1 fail" in combined and ("signals=" in combined or "windows=" in combined or "pct_pos=" in combined):
            return "SURFACE_EXPANSION_REVIEW", "APPROVE_SURFACE_EXPANSION", "M1 failed on sample/stability; inspect whether the search surface is too narrow before another retune."
        if m2_rows:
            pass_all = any(_truthy(row.get("passes_all_gates")) for row in m2_rows)
            pass_exp = any(_truthy(row.get("passes_exp_gate")) for row in m2_rows)
            pass_stability = any(_truthy(row.get("passes_stability_gate")) for row in m2_rows)
            if pass_all:
                return "APPROVE_CONTINUATION_REVIEW", "SKIP", "M2 has passing candidates; inspect why the hypothesis remains retune before rerunning."
            if pass_exp and not pass_stability:
                return "APPROVE_RETUNE", "APPROVE_RETUNE", "Expectancy exists but stability did not survive M2; a bounded retune is reasonable."
            if not pass_exp:
                return "KILL_OR_SURFACE_RETHINK", "SKIP", "M2 expectancy did not survive cost convergence; avoid another simple retune."
        if m1_rows:
            positive = [row for row in m1_rows if _to_float(row.get("avg_test_exp_r")) > 0]
            if positive:
                return "APPROVE_RETUNE", "APPROVE_RETUNE", "M1 still has positive candidates; run the bounded retune plan."
        return "INSPECT_BEFORE_RETUNE", "SKIP", "Retune is queued, but artifact evidence is thin."

    return "INSPECT", "SKIP", "No specific recommendation rule matched this action."


def build_action_brief(
    *,
    ledger: ResearchLedger,
    key: str,
    action_type: str = "",
) -> ActionBrief:
    requested_action_type, clean_key = _split_action_key(key)
    action_type = action_type or requested_action_type
    actions = build_next_actions(ledger)
    action = _find_action_for_brief(actions=actions, key=clean_key, action_type=action_type)
    if action is None and action_type:
        action = NextAction(
            rank=0,
            priority="medium",
            action_type=action_type,
            key=clean_key,
            reason="Ad hoc action brief request.",
            suggested_command="",
            requires_approval="yes",
            mutates_external_state="no",
        )

    hypothesis = _find_hypothesis(ledger, clean_key)
    latest_run = _latest_run(ledger, clean_key)
    latest_dir = _artifact_path(hypothesis.latest_artifact_dir) if hypothesis else None
    summary_path = latest_dir / "RUN_SUMMARY.md" if latest_dir else None
    summary_text = _read_text(summary_path) if summary_path else ""
    m1_rows = _read_csv_dicts(latest_dir / "M1_top.csv") if latest_dir else []
    m2_rows = _read_csv_dicts(latest_dir / "M2_gate_report.csv") if latest_dir else []
    recommendation, operator_action, summary = _brief_recommendation(
        hypothesis=hypothesis,
        action=action,
        m1_rows=m1_rows,
        m2_rows=m2_rows,
        summary_text=summary_text,
    )
    evidence: list[str] = []
    if action is not None:
        evidence.append(
            (
                f"Queued action rank={action.rank}, priority={action.priority}, "
                f"type={action.action_type}, reason={action.reason}"
            )
        )
    if hypothesis is not None:
        evidence.append(
            (
                f"Hypothesis state={hypothesis.state}, decision={hypothesis.decision or '<empty>'}, "
                f"strategy={hypothesis.strategy}, symbols={hypothesis.symbol_scope}, latest_stage={hypothesis.latest_stage}."
            )
        )
    if latest_run is not None:
        evidence.append(
            (
                f"Latest run {latest_run.run_ts} terminal_stage={latest_run.terminal_stage}, "
                f"decision={latest_run.decision or '<empty>'}, artifacts={latest_run.artifact_files}."
            )
        )
    if hypothesis is not None or (action is not None and action.action_type in {"retune_plan", "run_m1"}):
        evidence.extend(_m2_evidence_lines(m2_rows))
        evidence.extend(_m1_evidence_lines(m1_rows))
    sources = []
    if hypothesis is not None:
        sources.append(hypothesis.file_path)
    if latest_run is not None:
        sources.append(latest_run.artifact_dir)
    if summary_path and summary_path.exists():
        sources.append(_relative(summary_path))

    brief_action_id = action_id(action) if action else f"{action_type}:{clean_key}".strip(":")
    return ActionBrief(
        generated_at=ledger.generated_at,
        action_id=brief_action_id,
        action_type=action.action_type if action else action_type,
        key=clean_key,
        hypothesis_id=hypothesis.hypothesis_id if hypothesis else "",
        recommendation=recommendation,
        suggested_operator_action=operator_action,
        summary=summary,
        suggested_command=action.suggested_command if action else "",
        report_path="",
        evidence=evidence,
        surface_proposal=_surface_proposal(
            hypothesis=hypothesis,
            action_type=action.action_type if action else action_type,
            m1_rows=m1_rows,
            m2_rows=m2_rows,
        ),
        sources=sources,
    )


def write_action_brief(brief: ActionBrief, out_dir: Path) -> ActionBrief:
    out_dir.mkdir(parents=True, exist_ok=True)
    safe_key = re.sub(r"[^a-zA-Z0-9_.-]+", "-", brief.action_id).strip("-") or "action"
    stamp = brief.generated_at.replace(":", "").replace("-", "").replace("+", "Z")
    path = out_dir / "action_briefs" / f"{stamp}__{safe_key}.md"
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Mala Research Action Brief",
        "",
        f"- generated_at: `{brief.generated_at}`",
        f"- action_id: `{brief.action_id}`",
        f"- key: `{brief.key}`",
        f"- recommendation: `{brief.recommendation}`",
        f"- suggested_operator_action: `{brief.suggested_operator_action}`",
        f"- suggested_command: `{brief.suggested_command}`",
        "",
        "## Summary",
        "",
        brief.summary,
        "",
        "## Evidence",
        "",
    ]
    lines.extend(f"- {line}" for line in brief.evidence)
    lines.extend(["", "## Retune And Surface Proposal", ""])
    lines.extend(f"- {line}" for line in brief.surface_proposal)
    lines.extend(["", "## Sources", ""])
    lines.extend(f"- `{source}`" for source in brief.sources)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return ActionBrief(
        generated_at=brief.generated_at,
        action_id=brief.action_id,
        action_type=brief.action_type,
        key=brief.key,
        hypothesis_id=brief.hypothesis_id,
        recommendation=brief.recommendation,
        suggested_operator_action=brief.suggested_operator_action,
        summary=brief.summary,
        suggested_command=brief.suggested_command,
        report_path=str(path),
        evidence=brief.evidence,
        surface_proposal=brief.surface_proposal,
        sources=brief.sources,
    )


def update_control_row_with_brief(
    *,
    client: GoogleSheetTableClient,
    brief: ActionBrief,
) -> bool:
    client.ensure_sheet_exists()
    client.ensure_columns(CONTROL_SHEET_HEADERS)
    rows = client.read_rows(range_suffix="A1:ZZ5000")
    for row in rows:
        if str(row.get("action_id", "")).strip() != brief.action_id:
            continue
        next_row = dict(row)
        next_row["brief_recommendation"] = brief.recommendation
        next_row["brief_summary"] = _brief_cell(brief.summary)
        next_row["brief_path"] = brief.report_path
        next_row["updated_at"] = sheet_timestamp()
        client.batch_update_rows(
            rows=[next_row],
            columns=["brief_recommendation", "brief_summary", "brief_path", "updated_at"],
        )
        return True
    return False


def _numeric_range(rows: list[dict[str, str]], field: str) -> str:
    values = [_to_float(row.get(field), default=float("nan")) for row in rows if str(row.get(field, "")).strip()]
    values = [value for value in values if value == value]
    if not values:
        return ""
    low = min(values)
    high = max(values)
    if low == high:
        return _format_number(low, digits=2)
    return f"{_format_number(low, digits=2)}..{_format_number(high, digits=2)}"


def _categorical_values(rows: list[dict[str, str]], field: str, *, limit: int = 6) -> str:
    counts = Counter(str(row.get(field, "")).strip() for row in rows if str(row.get(field, "")).strip())
    if not counts:
        return ""
    return ", ".join(value for value, _count in counts.most_common(limit))


def _candidate_rows_for_surface(latest_dir: Path | None) -> list[dict[str, str]]:
    if latest_dir is None:
        return []
    rows: list[dict[str, str]] = []
    for filename in ("M1_top.csv", "M1_aggregate.csv", "M2_gate_report.csv"):
        rows.extend(_read_csv_dicts(latest_dir / filename))
    return rows


def _generic_surface_bounds(rows: list[dict[str, str]]) -> list[str]:
    ignored = {
        "ticker",
        "strategy",
        "direction",
        "decision",
        "passes_all_gates",
        "passes_exp_gate",
        "passes_signal_gate",
        "passes_stability_gate",
        "passes_window_gate",
        "has_all_cost_points",
    }
    metric_tokens = (
        "score",
        "signals",
        "windows",
        "exp_r",
        "confidence",
        "mfe",
        "mae",
        "pct_positive",
        "cost",
    )
    fields = sorted({key for row in rows for key in row if key not in ignored and not any(token in key for token in metric_tokens)})
    bounds: list[str] = []
    for field in fields[:12]:
        numeric = _numeric_range(rows, field)
        categorical = _categorical_values(rows, field)
        if numeric:
            bounds.append(f"{field}: center around observed range {numeric}.")
        elif categorical:
            bounds.append(f"{field}: restrict to observed values {categorical}.")
    return bounds


def _strategy_surface_template(strategy: str) -> list[str]:
    lowered = strategy.lower()
    if "opening drive" in lowered:
        return [
            "opening_window_minutes: test adjacent windows around the failing sample, not a broad sweep.",
            "entry_start_offset_minutes / entry_end_offset_minutes: widen entry timing only enough to recover signal count.",
            "breakout_buffer_pct and min_drive_return_pct: loosen one notch for sample recovery, then re-tighten if M1 passes.",
            "volume_multiplier, use_volume_filter, use_directional_mass, use_jerk_confirmation: compare filter-on vs filter-off slices.",
            "regime_timeframe / use_regime_filter: add one slower timeframe candidate if the current window is too sparse.",
        ]
    if "market impulse" in lowered:
        return [
            "regime_timeframe: test only adjacent timeframes around the best observed row.",
            "vwma_periods: center around the best observed tuple; do not introduce a large tuple grid.",
            "entry_buffer_minutes and entry_window_minutes: widen gradually only if signal count is the failure mode.",
            "direction: split long/short or combined lanes if the latest report mixes unstable directions.",
        ]
    if "jerk" in lowered:
        return [
            "kinematic_periods_back: test adjacent lookbacks around the current pivot window.",
            "jerk confirmation threshold/toggle: compare strict vs relaxed confirmation to recover sparse samples.",
            "directional mass and regime filters: isolate whether filters are suppressing valid setups.",
        ]
    if "elastic band" in lowered:
        return [
            "zscore_threshold and zscore_window: center around the nearest passing/stretch candidates.",
            "directional mass and jerk confirmation: relax one filter at a time to recover signal count.",
            "kinematic lookback: test adjacent values only; keep this config-only.",
        ]
    return ["Use the strategy search_spec or parameter_space; expand one bounded parameter family at a time."]


def _surface_plan_decision(
    *,
    hypothesis: HypothesisLedgerRow | None,
    summary_text: str,
    rows: list[dict[str, str]],
) -> tuple[str, str, str, str]:
    if hypothesis is None:
        return "inspect", "INSPECT", "SKIP", "No matching hypothesis row; do not modify search surfaces yet."
    combined = f"{hypothesis.state} {hypothesis.decision} {summary_text}".lower()
    if hypothesis.state == "kill":
        return "config-only", "NO_ACTION", "SKIP", "Hypothesis is already killed; write a new thesis before changing surface."
    if "m1 fail" in combined and ("signals=" in combined or "windows=" in combined or "pct_pos=" in combined):
        return (
            "config-only",
            "CONFIG_ONLY_SURFACE_EXPANSION",
            "APPROVE_RETUNE",
            "Search surface likely needs bounded widening before another M1 retune.",
        )
    if rows:
        pass_exp = any(_truthy(row.get("passes_exp_gate")) for row in rows)
        pass_all = any(_truthy(row.get("passes_all_gates")) for row in rows)
        if pass_all:
            return "config-only", "CONTINUATION_REVIEW", "SKIP", "Existing rows include a passing gate candidate; inspect state before expansion."
        if pass_exp:
            return (
                "config-only",
                "CONFIG_ONLY_STABILITY_RETUNE",
                "APPROVE_RETUNE",
                "Expectancy exists, but stability needs a narrower parameter surface.",
            )
        return (
            "config-only",
            "RETHINK_BEFORE_EXPANSION",
            "SKIP",
            "Candidate evidence did not preserve positive expectancy; surface expansion needs a stronger thesis.",
        )
    return "config-only", "EVIDENCE_THIN", "SKIP", "No candidate CSV evidence was found; inspect or rerun before expanding."


def build_surface_expansion_plan(
    *,
    ledger: ResearchLedger,
    key: str,
) -> SurfaceExpansionPlan:
    action_type, clean_key = _split_action_key(key)
    action_id_value = f"{action_type or 'retune_plan'}:{clean_key}"
    hypothesis = _find_hypothesis(ledger, clean_key)
    latest_run = _latest_run(ledger, clean_key)
    latest_dir = _artifact_path(hypothesis.latest_artifact_dir) if hypothesis else None
    summary_path = latest_dir / "RUN_SUMMARY.md" if latest_dir else None
    summary_text = _read_text(summary_path) if summary_path else ""
    rows = _candidate_rows_for_surface(latest_dir)
    feasibility, recommendation, next_action, summary = _surface_plan_decision(
        hypothesis=hypothesis,
        summary_text=summary_text,
        rows=rows,
    )
    proposed_bounds = _generic_surface_bounds(rows)
    proposed_bounds.extend(_strategy_surface_template(hypothesis.strategy if hypothesis else ""))
    rationale = []
    if hypothesis is not None:
        rationale.append(
            (
                f"{hypothesis.hypothesis_id} is state={hypothesis.state}, decision={hypothesis.decision}, "
                f"latest_stage={hypothesis.latest_stage}, strategy={hypothesis.strategy}, symbols={hypothesis.symbol_scope}."
            )
        )
    if latest_run is not None:
        rationale.append(
            (
                f"Latest run {latest_run.run_ts} reached {latest_run.terminal_stage} with decision={latest_run.decision or '<empty>'}."
            )
        )
    rationale.extend(_m2_evidence_lines(_read_csv_dicts(latest_dir / "M2_gate_report.csv") if latest_dir else []))
    rationale.extend(_m1_evidence_lines(_read_csv_dicts(latest_dir / "M1_top.csv") if latest_dir else []))
    validation_steps = [
        "Treat this as config-only unless the plan explicitly says a new strategy feature is required.",
        "Update the strategy search_spec / parameter_space, not hypothesis_agent.py.",
        "Run research_runner dry-run first to verify config count and data availability.",
        "Run M1 only; do not continue to M2 until M1 meets the normal gate thresholds.",
        "If M1 fails again on no positive expectancy, mark the hypothesis kill instead of widening again.",
    ]
    sources: list[str] = []
    if hypothesis is not None:
        sources.append(hypothesis.file_path)
    if latest_run is not None:
        sources.append(latest_run.artifact_dir)
    if summary_path and summary_path.exists():
        sources.append(_relative(summary_path))
    return SurfaceExpansionPlan(
        generated_at=ledger.generated_at,
        action_id=action_id_value,
        key=clean_key,
        hypothesis_id=hypothesis.hypothesis_id if hypothesis else "",
        strategy=hypothesis.strategy if hypothesis else "",
        symbol_scope=hypothesis.symbol_scope if hypothesis else "",
        feasibility_tag=feasibility,
        recommendation=recommendation,
        next_operator_action=next_action,
        summary=summary,
        proposed_bounds=proposed_bounds,
        rationale=rationale,
        validation_steps=validation_steps,
        sources=sources,
        report_path="",
        json_path="",
    )


def write_surface_expansion_plan(plan: SurfaceExpansionPlan, out_dir: Path) -> SurfaceExpansionPlan:
    out_dir.mkdir(parents=True, exist_ok=True)
    safe_key = re.sub(r"[^a-zA-Z0-9_.-]+", "-", plan.action_id).strip("-") or "surface"
    stamp = plan.generated_at.replace(":", "").replace("-", "").replace("+", "Z")
    md_path = out_dir / "surface_expansion" / f"{stamp}__{safe_key}.md"
    json_path = out_dir / "surface_expansion" / f"{stamp}__{safe_key}.json"
    md_path.parent.mkdir(parents=True, exist_ok=True)
    complete = SurfaceExpansionPlan(
        generated_at=plan.generated_at,
        action_id=plan.action_id,
        key=plan.key,
        hypothesis_id=plan.hypothesis_id,
        strategy=plan.strategy,
        symbol_scope=plan.symbol_scope,
        feasibility_tag=plan.feasibility_tag,
        recommendation=plan.recommendation,
        next_operator_action=plan.next_operator_action,
        summary=plan.summary,
        proposed_bounds=plan.proposed_bounds,
        rationale=plan.rationale,
        validation_steps=plan.validation_steps,
        sources=plan.sources,
        report_path=str(md_path),
        json_path=str(json_path),
    )
    lines = [
        "# Mala Surface Expansion Plan",
        "",
        f"- generated_at: `{complete.generated_at}`",
        f"- action_id: `{complete.action_id}`",
        f"- hypothesis_id: `{complete.hypothesis_id}`",
        f"- strategy: `{complete.strategy}`",
        f"- symbol_scope: `{complete.symbol_scope}`",
        f"- feasibility_tag: `{complete.feasibility_tag}`",
        f"- recommendation: `{complete.recommendation}`",
        f"- next_operator_action: `{complete.next_operator_action}`",
        "",
        "## Summary",
        "",
        complete.summary,
        "",
        "## Proposed Bounds",
        "",
    ]
    lines.extend(f"- {line}" for line in complete.proposed_bounds)
    lines.extend(["", "## Rationale", ""])
    lines.extend(f"- {line}" for line in complete.rationale)
    lines.extend(["", "## Validation Steps", ""])
    lines.extend(f"- {line}" for line in complete.validation_steps)
    lines.extend(["", "## Sources", ""])
    lines.extend(f"- `{source}`" for source in complete.sources)
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    json_path.write_text(json.dumps(asdict(complete), indent=2), encoding="utf-8")
    return complete


def update_control_row_with_surface_plan(
    *,
    client: GoogleSheetTableClient,
    plan: SurfaceExpansionPlan,
) -> bool:
    client.ensure_sheet_exists()
    client.ensure_columns(CONTROL_SHEET_HEADERS)
    rows = client.read_rows(range_suffix="A1:ZZ5000")
    for row in rows:
        if str(row.get("action_id", "")).strip() != plan.action_id:
            continue
        next_row = dict(row)
        next_row["brief_recommendation"] = plan.recommendation
        next_row["brief_summary"] = _brief_cell(plan.summary)
        next_row["brief_path"] = plan.report_path
        next_row["status"] = "surface_plan_ready"
        next_row["updated_at"] = sheet_timestamp()
        client.batch_update_rows(
            rows=[next_row],
            columns=["brief_recommendation", "brief_summary", "brief_path", "status", "updated_at"],
        )
        return True
    return False


def evaluate_hypothesis_intake(row: dict[str, Any]) -> HypothesisIntakeEvaluation:
    title = str(row.get("title", "")).strip()
    hypothesis_id = _slug(str(row.get("hypothesis_id", "")).strip() or title)
    strategy = str(row.get("strategy", "")).strip()
    symbol_scope = str(row.get("symbol_scope", "")).strip()
    max_stage = str(row.get("max_stage", "")).strip() or "M5"
    intake_id = str(row.get("intake_id", "")).strip() or hypothesis_id

    if not title:
        return HypothesisIntakeEvaluation(
            intake_id=intake_id,
            title=title,
            hypothesis_id=hypothesis_id,
            strategy=strategy,
            symbol_scope=symbol_scope,
            max_stage=max_stage,
            feasibility_tag="needs-human",
            feasibility_summary="Missing title; fill the intake row before evaluation.",
            search_param_keys="",
            discovery_config_count=0,
            retune_config_count=0,
        )
    if not symbol_scope:
        return HypothesisIntakeEvaluation(
            intake_id=intake_id,
            title=title,
            hypothesis_id=hypothesis_id,
            strategy=strategy,
            symbol_scope=symbol_scope,
            max_stage=max_stage,
            feasibility_tag="needs-human",
            feasibility_summary="Missing symbol_scope; specify comma-separated tickers before evaluation.",
            search_param_keys="",
            discovery_config_count=0,
            retune_config_count=0,
        )
    if strategy not in available_strategy_names():
        return HypothesisIntakeEvaluation(
            intake_id=intake_id,
            title=title,
            hypothesis_id=hypothesis_id,
            strategy=strategy,
            symbol_scope=symbol_scope,
            max_stage=max_stage,
            feasibility_tag="new-class",
            feasibility_summary=(
                "Strategy is not in the current factory registry; route to strategy/code development before creating a runnable hypothesis."
            ),
            search_param_keys="",
            discovery_config_count=0,
            retune_config_count=0,
        )

    try:
        keys = search_param_keys(strategy)
        discovery_configs = build_search_configs(strategy, mode="discovery", max_configs=32)
        retune_configs = build_search_configs(strategy, mode="retune", max_configs=32)
    except Exception as exc:  # pragma: no cover - defensive; surfaced in sheet summary
        return HypothesisIntakeEvaluation(
            intake_id=intake_id,
            title=title,
            hypothesis_id=hypothesis_id,
            strategy=strategy,
            symbol_scope=symbol_scope,
            max_stage=max_stage,
            feasibility_tag="new-feature",
            feasibility_summary=f"Current strategy exists, but search-surface construction failed: {exc}",
            search_param_keys="",
            discovery_config_count=0,
            retune_config_count=0,
        )

    tag = "config-only" if discovery_configs else "new-feature"
    summary = (
        f"Runnable with current codebase: strategy exists with {len(keys)} search parameters, "
        f"{len(discovery_configs)} discovery configs, and {len(retune_configs)} retune configs."
    )
    if not keys:
        summary = (
            "Strategy is runnable but has no declared search parameters; this can be tested as fixed-config, "
            "but surface expansion would need search_spec/parameter_space work."
        )
    return HypothesisIntakeEvaluation(
        intake_id=intake_id,
        title=title,
        hypothesis_id=hypothesis_id,
        strategy=strategy,
        symbol_scope=symbol_scope,
        max_stage=max_stage,
        feasibility_tag=tag,
        feasibility_summary=summary,
        search_param_keys=", ".join(keys),
        discovery_config_count=len(discovery_configs),
        retune_config_count=len(retune_configs),
    )


def _intake_status_from_evaluation(evaluation: HypothesisIntakeEvaluation) -> str:
    if evaluation.feasibility_tag == "config-only":
        return "evaluated_ready_for_approval"
    return f"blocked_{evaluation.feasibility_tag}"


def _write_intake_report(
    *,
    evaluation: HypothesisIntakeEvaluation,
    row: dict[str, Any],
    out_dir: Path,
) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).replace(microsecond=0).isoformat().replace(":", "").replace("-", "").replace("+", "Z")
    path = out_dir / "intake" / f"{stamp}__{evaluation.hypothesis_id}.md"
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Mala Hypothesis Intake Evaluation",
        "",
        f"- intake_id: `{evaluation.intake_id}`",
        f"- hypothesis_id: `{evaluation.hypothesis_id}`",
        f"- title: `{evaluation.title}`",
        f"- strategy: `{evaluation.strategy}`",
        f"- symbol_scope: `{evaluation.symbol_scope}`",
        f"- max_stage: `{evaluation.max_stage}`",
        f"- feasibility_tag: `{evaluation.feasibility_tag}`",
        f"- discovery_config_count: `{evaluation.discovery_config_count}`",
        f"- retune_config_count: `{evaluation.retune_config_count}`",
        "",
        "## Summary",
        "",
        evaluation.feasibility_summary,
        "",
        "## Thesis",
        "",
        str(row.get("thesis", "")).strip() or "<empty>",
        "",
        "## Search Parameters",
        "",
        evaluation.search_param_keys or "<none>",
        "",
        "## Next Step",
        "",
        (
            "Set `operator_action=APPROVE_CREATE_HYPOTHESIS` to create a pending hypothesis file."
            if evaluation.feasibility_tag == "config-only"
            else "Route this to human/agent development before creating a runnable hypothesis."
        ),
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def _merge_intake_update(
    *,
    row: dict[str, Any],
    evaluation: HypothesisIntakeEvaluation,
    status: str,
    report_path: str,
    hypothesis_path: str = "",
    clear_operator_action: bool,
) -> dict[str, Any]:
    next_row = dict(row)
    next_row["intake_id"] = evaluation.intake_id
    next_row["hypothesis_id"] = evaluation.hypothesis_id
    next_row["max_stage"] = evaluation.max_stage
    next_row["status"] = status
    next_row["feasibility_tag"] = evaluation.feasibility_tag
    next_row["feasibility_summary"] = evaluation.feasibility_summary
    next_row["search_param_keys"] = evaluation.search_param_keys
    next_row["discovery_config_count"] = evaluation.discovery_config_count
    next_row["retune_config_count"] = evaluation.retune_config_count
    next_row["report_path"] = report_path or evaluation.report_path
    next_row["hypothesis_path"] = hypothesis_path or evaluation.hypothesis_path
    next_row["updated_at"] = sheet_timestamp()
    if not str(next_row.get("created_at", "")).strip():
        next_row["created_at"] = next_row["updated_at"]
    if clear_operator_action:
        next_row["operator_action"] = ""
    return next_row


def process_intake_rows(
    *,
    rows: list[dict[str, Any]],
    hypotheses_dir: Path,
    out_dir: Path,
    apply: bool,
    limit: int = 1,
    force: bool = False,
) -> list[dict[str, Any]]:
    updates: list[dict[str, Any]] = []
    for row in rows:
        if limit and len(updates) >= limit:
            break
        operator_action = str(row.get("operator_action", "")).strip().upper()
        if operator_action not in INTAKE_OPERATOR_ACTIONS:
            continue
        if not operator_action:
            continue
        evaluation = evaluate_hypothesis_intake(row)
        report_path = _write_intake_report(evaluation=evaluation, row=row, out_dir=out_dir)
        hypothesis_path = ""
        status = _intake_status_from_evaluation(evaluation)
        clear_operator_action = apply
        if operator_action == "SKIP":
            status = "skipped"
        elif operator_action == "APPROVE_CREATE_HYPOTHESIS":
            if evaluation.feasibility_tag != "config-only":
                status = f"blocked_{evaluation.feasibility_tag}"
            elif apply:
                target_path = hypotheses_dir / f"{evaluation.hypothesis_id}.md"
                if target_path.exists() and not force:
                    hypothesis_path = _relative(target_path)
                    status = "existing_hypothesis"
                else:
                    created = create_hypothesis_file(
                        hypothesis_id=evaluation.hypothesis_id,
                        title=evaluation.title,
                        strategy=evaluation.strategy,
                        symbol_scope=evaluation.symbol_scope,
                        max_stage=evaluation.max_stage,
                        thesis=str(row.get("thesis", "")),
                        rules=_split_multiline_cell(row.get("rules", "")),
                        notes=[
                            f"Feasibility tag: {evaluation.feasibility_tag}.",
                            evaluation.feasibility_summary,
                            *_split_multiline_cell(row.get("notes", "")),
                        ],
                        hypotheses_dir=hypotheses_dir,
                        force=force,
                    )
                    hypothesis_path = _relative(created)
                    status = "created_pending"
            else:
                status = "would_create_pending"
        elif operator_action == "EVALUATE":
            status = _intake_status_from_evaluation(evaluation)
        updates.append(
            _merge_intake_update(
                row=row,
                evaluation=evaluation,
                status=status,
                report_path=str(report_path),
                hypothesis_path=hypothesis_path,
                clear_operator_action=clear_operator_action,
            )
        )
    return updates


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


def _parse_run_ts(value: str) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%dT%H%M%S", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S"):
        try:
            parsed = datetime.strptime(text, fmt)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=UTC)
            return parsed.astimezone(UTC)
        except ValueError:
            continue
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)
    except ValueError:
        return None


def _nonempty_operator_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [row for row in rows if str(row.get("operator_action", "")).strip()]


def write_digest_report(
    *,
    ledger: ResearchLedger,
    actions: list[NextAction],
    control_rows: list[dict[str, Any]],
    intake_rows: list[dict[str, Any]],
    path: Path,
    days: int,
) -> ResearchDigest:
    path.parent.mkdir(parents=True, exist_ok=True)
    now = datetime.now(UTC).replace(microsecond=0)
    cutoff_seconds = max(1, days) * 24 * 60 * 60
    recent_runs = [
        row
        for row in ledger.runs
        if (parsed := _parse_run_ts(row.run_ts)) is not None
        and (now - parsed).total_seconds() <= cutoff_seconds
    ]
    hypotheses_by_state = dict(Counter(row.state or "<empty>" for row in ledger.hypotheses))
    actions_by_type = dict(Counter(row.action_type for row in actions))
    findings_by_category = dict(Counter(row.category for row in ledger.findings))
    pending_control = _nonempty_operator_rows(control_rows)
    pending_intake = _nonempty_operator_rows(intake_rows)
    blocked_intake = [
        row for row in intake_rows if str(row.get("status", "")).strip().startswith("blocked_")
    ]
    ready_intake = [
        row for row in intake_rows if str(row.get("status", "")).strip() == "evaluated_ready_for_approval"
    ]

    lines = [
        "# Mala Research Digest",
        "",
        f"- generated_at: `{now.isoformat()}`",
        f"- window_days: `{days}`",
        f"- hypotheses: `{len(ledger.hypotheses)}`",
        f"- runs: `{len(ledger.runs)}`",
        f"- recent_runs: `{len(recent_runs)}`",
        f"- promoted_candidates: `{len(ledger.promoted)}`",
        f"- findings: `{len(ledger.findings)}`",
        f"- next_actions: `{len(actions)}`",
        f"- pending_control_actions: `{len(pending_control)}`",
        f"- pending_intake_actions: `{len(pending_intake)}`",
        "",
        "## State Counts",
        "",
    ]
    for key, count in sorted(hypotheses_by_state.items()):
        lines.append(f"- hypotheses `{key}`: `{count}`")
    lines.append("")
    lines.append("## Next Action Counts")
    lines.append("")
    if not actions_by_type:
        lines.append("- No queued actions.")
    for key, count in sorted(actions_by_type.items()):
        lines.append(f"- `{key}`: `{count}`")
    lines.extend(["", "## Top Queue", ""])
    if not actions:
        lines.append("- Empty.")
    for action in actions[:10]:
        lines.append(
            f"- #{action.rank} `{action.action_type}` `{action.key}`: {action.reason}"
        )
    lines.extend(["", "## Recent Runs", ""])
    if not recent_runs:
        lines.append("- No recent runs in this window.")
    for run in sorted(recent_runs, key=lambda item: item.run_ts, reverse=True)[:12]:
        lines.append(
            f"- `{run.run_ts}` `{run.hypothesis_id}` stage={run.terminal_stage} decision={run.decision or '<empty>'}"
        )
    lines.extend(["", "## Findings", ""])
    if not findings_by_category:
        lines.append("- No hot-start findings.")
    for key, count in sorted(findings_by_category.items()):
        lines.append(f"- `{key}`: `{count}`")
    lines.extend(["", "## Control Sheet", ""])
    if not pending_control:
        lines.append("- No pending operator actions.")
    for row in pending_control[:10]:
        lines.append(
            f"- `{row.get('operator_action', '')}` `{row.get('action_id', '')}` status={row.get('status', '')}"
        )
    lines.extend(["", "## Intake Sheet", ""])
    if not pending_intake and not ready_intake and not blocked_intake:
        lines.append("- No active intake rows.")
    for row in pending_intake[:10]:
        lines.append(
            f"- pending `{row.get('operator_action', '')}` `{row.get('hypothesis_id', '') or row.get('intake_id', '')}`"
        )
    for row in ready_intake[:10]:
        lines.append(
            f"- ready `{row.get('hypothesis_id', '')}` strategy={row.get('strategy', '')}"
        )
    for row in blocked_intake[:10]:
        lines.append(
            f"- blocked `{row.get('hypothesis_id', '')}` tag={row.get('feasibility_tag', '')}: {row.get('feasibility_summary', '')}"
        )
    lines.extend(["", "## Suggested Routine", ""])
    lines.append("- Review pending `Research_Control.operator_action` rows first; these are executable decisions.")
    lines.append("- Review `Research_Intake` ready/blocked rows next; ready rows can become pending hypotheses, blocked rows need agent or human development.")
    lines.append("- Let agents propose changes as sheet rows or reports; keep Mala artifacts as the source of truth.")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return ResearchDigest(
        generated_at=now.isoformat(),
        days=days,
        report_path=str(path),
        hypotheses_by_state=hypotheses_by_state,
        next_actions_by_type=actions_by_type,
        findings_by_category=findings_by_category,
        recent_runs=len(recent_runs),
        pending_control_actions=len(pending_control),
        pending_intake_actions=len(pending_intake),
    )


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


def _strategy_catalog_client(args: argparse.Namespace) -> GoogleSheetTableClient:
    credentials = args.catalog_google_credentials or args.google_credentials
    if not args.catalog_sheet_id:
        raise SystemExit("--catalog-sheet-id or STRATEGY_CATALOG_SHEET_ID is required")
    if not credentials:
        raise SystemExit("--google-credentials or --catalog-google-credentials is required")
    return GoogleSheetTableClient(
        spreadsheet_id=args.catalog_sheet_id,
        sheet_name=args.catalog_sheet_name,
        credentials_path=Path(credentials),
    )


def _board_client(args: argparse.Namespace) -> GoogleSheetTableClient:
    credentials = args.board_google_credentials or args.google_credentials
    if not args.board_sheet_id:
        raise SystemExit("--board-sheet-id is required")
    if not credentials:
        raise SystemExit("--google-credentials or --board-google-credentials is required")
    return GoogleSheetTableClient(
        spreadsheet_id=args.board_sheet_id,
        sheet_name=args.board_scout_sheet,
        credentials_path=Path(credentials),
    )


def _control_client(args: argparse.Namespace) -> GoogleSheetTableClient:
    credentials = args.control_google_credentials or args.google_credentials
    sheet_id = args.control_sheet_id or args.board_sheet_id
    if not sheet_id:
        raise SystemExit("--control-sheet-id or --board-sheet-id is required")
    if not credentials:
        raise SystemExit("--google-credentials or --control-google-credentials is required")
    return GoogleSheetTableClient(
        spreadsheet_id=sheet_id,
        sheet_name=args.control_sheet_name,
        credentials_path=Path(credentials),
    )


def _intake_client(args: argparse.Namespace) -> GoogleSheetTableClient:
    credentials = args.intake_google_credentials or args.google_credentials
    sheet_id = args.intake_sheet_id or args.control_sheet_id or args.board_sheet_id
    if not sheet_id:
        raise SystemExit("--intake-sheet-id, --control-sheet-id, or --board-sheet-id is required")
    if not credentials:
        raise SystemExit("--google-credentials or --intake-google-credentials is required")
    return GoogleSheetTableClient(
        spreadsheet_id=sheet_id,
        sheet_name=args.intake_sheet_name,
        credentials_path=Path(credentials),
    )


def _build_with_optional_sheets(args: argparse.Namespace) -> ResearchLedger:
    catalog_rows = _read_strategy_catalog_rows(args) if args.with_catalog else []
    dispositions = read_dispositions(Path(args.dispositions_path))
    ledger = build_ledger(
        hypotheses_dir=Path(args.hypotheses_dir),
        runs_dir=Path(args.runs_dir),
        strategy_catalog_rows=catalog_rows,
        dispositions=dispositions,
    )
    if args.with_board:
        board_rows = _read_board_rows(args)
        ledger.findings = build_hot_start_findings(
            hypotheses=ledger.hypotheses,
            runs=ledger.runs,
            promoted=ledger.promoted,
            board_rows=board_rows,
            dispositions=dispositions,
        )
    return ledger


def _read_control_rows(args: argparse.Namespace) -> list[dict[str, Any]]:
    if not getattr(args, "with_control", False):
        return []
    try:
        client = _control_client(args)
    except SystemExit:
        return []
    return client.read_rows(range_suffix="A1:ZZ5000")


def _read_intake_rows(args: argparse.Namespace) -> list[dict[str, Any]]:
    if not getattr(args, "with_intake", False):
        return []
    try:
        client = _intake_client(args)
    except SystemExit:
        return []
    return client.read_rows(range_suffix="A1:ZZ5000")


def _selected_matches_m5(selected: dict[str, str], row: dict[str, str]) -> bool:
    ignored = {
        "catalog_key",
        "recommendation_tier",
        "exit_reliability",
        "exit_trade_count",
        "selected_exit_policy",
        "mc_prob_positive_exp",
        "mc_exp_r_p50",
        "base_exp_r",
        "holdout_trades",
        "holdout_win_rate",
    }
    for key, value in selected.items():
        if key in ignored or value in ("", None):
            continue
        if key not in row:
            continue
        if str(row.get(key, "")).strip() != str(value).strip():
            return False
    return True


def _matching_m5_row(run_dir: Path, selected: dict[str, str]) -> dict[str, str]:
    rows = _read_csv_dicts(run_dir / "M5_execution.csv")
    for row in rows:
        if _selected_matches_m5(selected, row):
            return row
    catalog_key = selected.get("catalog_key", "<unknown>")
    raise RuntimeError(f"No M5_execution.csv row matched {catalog_key} in {run_dir}")


def _exit_opt_matches_selected(item: dict[str, Any], selected: dict[str, str]) -> bool:
    key = item.get("candidate_key", {})
    if not isinstance(key, dict):
        return False
    match_keys = [
        "ticker",
        "direction",
        "strategy",
        "entry_buffer_minutes",
        "entry_window_minutes",
        "regime_timeframe",
        "vwma_periods",
    ]
    for field in match_keys:
        selected_value = selected.get(field, "")
        if not selected_value:
            continue
        if str(key.get(field, "")).strip() != str(selected_value).strip():
            return False
    return True


def _exit_opt_for_selected(run_dir: Path, selected: dict[str, str]) -> dict[str, Any] | None:
    summary_path = run_dir / "m5_exit_optimizations.json"
    if not summary_path.exists():
        return None
    try:
        items = json.loads(summary_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    if not isinstance(items, list):
        return None

    for item in items:
        if not isinstance(item, dict) or not _exit_opt_matches_selected(item, selected):
            continue
        artifact = run_dir / str(item.get("artifact", ""))
        if artifact.exists():
            try:
                return json.loads(artifact.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                return None
        selected_policy = str(item.get("selected_policy_name", "") or selected.get("selected_exit_policy", ""))
        if not selected_policy:
            return None
        return {
            "selected_policy_name": selected_policy,
            "thesis_exit_policy": selected_policy.split(":", 1)[0],
            "selected_metrics": item.get("selected_metrics", {}),
        }
    return None


def _latest_promoted_by_catalog_key(ledger: ResearchLedger) -> dict[str, PromotedLedgerRow]:
    latest: dict[str, PromotedLedgerRow] = {}
    for row in ledger.promoted:
        if row.catalog_key:
            latest[row.catalog_key] = row
    return latest


def _catalog_publish_plan(
    *,
    ledger: ResearchLedger,
    catalog_keys: set[str],
    only_catalog_key: str = "",
) -> list[PromotedLedgerRow]:
    rows: list[PromotedLedgerRow] = []
    for row in _latest_promoted_by_catalog_key(ledger).values():
        if only_catalog_key and row.catalog_key != only_catalog_key:
            continue
        if row.catalog_key in catalog_keys:
            continue
        if row.recommendation_tier not in {"promote", "shadow"}:
            continue
        rows.append(row)
    return sorted(rows, key=lambda item: (item.recommendation_tier != "promote", item.catalog_key))


def _publish_catalog_rows(
    *,
    rows: list[PromotedLedgerRow],
    args: argparse.Namespace,
    dry_run: bool,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    credentials = args.catalog_google_credentials or args.google_credentials
    if not dry_run and not credentials:
        raise SystemExit("--google-credentials or --catalog-google-credentials is required for --apply")
    for row in rows:
        run_dir = REPO_ROOT / row.artifact_dir
        selected_rows = [
            selected
            for selected in _read_csv_dicts(run_dir / "CATALOG_SELECTED.csv")
            if selected.get("catalog_key") == row.catalog_key
        ]
        if not selected_rows:
            raise RuntimeError(f"CATALOG_SELECTED.csv row missing for {row.catalog_key}")
        selected = selected_rows[-1]
        m5_best = _matching_m5_row(run_dir, selected)
        exit_opt = _exit_opt_for_selected(run_dir, selected)
        result = {
            "catalog_key": row.catalog_key,
            "ticker": row.ticker,
            "direction": row.direction,
            "strategy": row.strategy,
            "recommendation_tier": row.recommendation_tier,
            "artifact_dir": row.artifact_dir,
            "action": "would_publish" if dry_run else "published",
        }
        if not dry_run:
            upsert_strategy_catalog(
                catalog_key=row.catalog_key,
                symbol=row.ticker,
                strategy=row.strategy,
                m5_best=m5_best,
                spreadsheet_id=args.catalog_sheet_id,
                credentials_path=Path(credentials),
                sheet_name=args.catalog_sheet_name,
                exit_opt=exit_opt,
            )
        results.append(result)
    return results


def _board_status_for(row: HypothesisLedgerRow) -> dict[str, str]:
    stage = row.latest_stage if row.latest_stage != "none" else "FEASIBILITY"
    if row.state == "completed" and row.decision == "promote":
        return {
            "Operator_Action": "",
            "Agent_State": "PROMOTED",
            "Current_Stage": "M5",
            "Recommendation": "PROMOTE",
        }
    if row.state == "kill":
        return {
            "Operator_Action": "",
            "Agent_State": "KILLED",
            "Current_Stage": stage,
            "Recommendation": "KILL",
        }
    if row.state == "retune":
        return {
            "Operator_Action": "",
            "Agent_State": "ASSESSED",
            "Current_Stage": stage,
            "Recommendation": "RETUNE_M1",
        }
    if row.state == "running":
        return {
            "Operator_Action": "",
            "Agent_State": "RUNNING_PIPELINE",
            "Current_Stage": stage,
            "Recommendation": "CONTINUE_PIPELINE",
        }
    return {}


def _board_sync_plan(
    *,
    ledger: ResearchLedger,
    board_rows: list[dict[str, Any]],
    only_task_id: str = "",
) -> list[dict[str, Any]]:
    latest_by_hyp = {row.hypothesis_id: row for row in ledger.hypotheses}
    updates: list[dict[str, Any]] = []
    for board_row in board_rows:
        task_id = str(board_row.get("Task_ID", "")).strip()
        if not task_id or (only_task_id and task_id != only_task_id):
            continue
        matched = _match_board_row_to_hypothesis(board_row, latest_by_hyp)
        if matched is None:
            continue
        status = _board_status_for(matched)
        if not status:
            continue
        needs_update = any(str(board_row.get(key, "")) != value for key, value in status.items())
        if not needs_update:
            continue
        next_row = dict(board_row)
        next_row.update(status)
        next_row["_matched_hypothesis_id"] = matched.hypothesis_id
        updates.append(next_row)
    return updates


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


def cmd_digest(args: argparse.Namespace) -> int:
    ledger = _build_with_optional_sheets(args)
    actions = build_next_actions(ledger)
    if args.limit:
        actions = actions[: args.limit]
    out_dir = Path(args.out_dir)
    stamp = datetime.now(UTC).replace(microsecond=0).isoformat().replace(":", "").replace("-", "").replace("+", "Z")
    path = Path(args.output) if args.output else out_dir / "digests" / f"digest-{stamp}.md"
    digest = write_digest_report(
        ledger=ledger,
        actions=actions,
        control_rows=_read_control_rows(args),
        intake_rows=_read_intake_rows(args),
        path=path,
        days=args.days,
    )
    print(f"DIGEST_REPORT={digest.report_path}")
    print(f"DIGEST_DAYS={digest.days}")
    print(f"DIGEST_RECENT_RUNS={digest.recent_runs}")
    print(f"DIGEST_PENDING_CONTROL={digest.pending_control_actions}")
    print(f"DIGEST_PENDING_INTAKE={digest.pending_intake_actions}")
    return 0


def cmd_next_actions(args: argparse.Namespace) -> int:
    ledger = _build_with_optional_sheets(args)
    actions = build_next_actions(ledger)
    if args.limit:
        actions = actions[: args.limit]
    rows = [asdict(row) for row in actions]
    out_dir = Path(args.out_dir)
    if args.format == "json":
        print(json.dumps(rows, indent=2))
    elif args.format == "csv":
        path = Path(args.output) if args.output else out_dir / "next_actions.csv"
        _write_csv(path, rows)
        print(f"NEXT_ACTIONS_CSV={path}")
    else:
        path = Path(args.output) if args.output else out_dir / "next_actions.md"
        path.parent.mkdir(parents=True, exist_ok=True)
        lines = [
            "# Mala Research Next Actions",
            "",
            f"- generated_at: `{ledger.generated_at}`",
            f"- actions: `{len(actions)}`",
            "",
            "| Rank | Priority | Action | Key | Approval | External | Suggested Command |",
            "|---:|---|---|---|---|---|---|",
        ]
        if not actions:
            lines.append("|  |  | No actions |  |  |  |  |")
        for action in actions:
            lines.append(
                "| {rank} | {priority} | `{action_type}` | `{key}` | {approval} | {external} | `{command}` |".format(
                    rank=action.rank,
                    priority=action.priority,
                    action_type=action.action_type,
                    key=action.key,
                    approval=action.requires_approval,
                    external=action.mutates_external_state,
                    command=action.suggested_command.replace("|", "/"),
                )
            )
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        print(f"NEXT_ACTIONS_REPORT={path}")
    print(f"NEXT_ACTIONS={len(actions)}")
    return 0


def cmd_push_control(args: argparse.Namespace) -> int:
    ledger = _build_with_optional_sheets(args)
    actions = build_next_actions(ledger)
    if args.limit:
        actions = actions[: args.limit]
    client = _control_client(args)
    client.ensure_sheet_exists()
    existing_rows = client.read_rows(range_suffix="A1:ZZ5000")
    rows = build_control_rows(
        actions=actions,
        generated_at=sheet_timestamp(),
        existing_rows=existing_rows,
    )
    client.overwrite_table(headers=CONTROL_SHEET_HEADERS, rows=rows)
    print(f"CONTROL_SHEET_ID={args.control_sheet_id or args.board_sheet_id}")
    print(f"CONTROL_SHEET_NAME={args.control_sheet_name}")
    print(f"CONTROL_ROWS={len(rows)}")
    return 0


def cmd_action_brief(args: argparse.Namespace) -> int:
    ledger = _build_with_optional_sheets(args)
    brief = build_action_brief(
        ledger=ledger,
        key=args.key,
        action_type=args.action_type,
    )
    brief = write_action_brief(brief, Path(args.out_dir))
    pushed = False
    if args.push_control:
        client = _control_client(args)
        pushed = update_control_row_with_brief(client=client, brief=brief)
    print(f"ACTION_BRIEF_REPORT={brief.report_path}")
    print(f"ACTION_BRIEF_ID={brief.action_id}")
    print(f"ACTION_BRIEF_RECOMMENDATION={brief.recommendation}")
    print(f"ACTION_BRIEF_OPERATOR_ACTION={brief.suggested_operator_action}")
    print(f"ACTION_BRIEF_CONTROL_UPDATED={'yes' if pushed else 'no'}")
    return 0


def cmd_surface_expansion_plan(args: argparse.Namespace) -> int:
    ledger = _build_with_optional_sheets(args)
    plan = build_surface_expansion_plan(ledger=ledger, key=args.key)
    plan = write_surface_expansion_plan(plan, Path(args.out_dir))
    pushed = False
    if args.push_control:
        client = _control_client(args)
        pushed = update_control_row_with_surface_plan(client=client, plan=plan)
    print(f"SURFACE_EXPANSION_PLAN_REPORT={plan.report_path}")
    print(f"SURFACE_EXPANSION_PLAN_JSON={plan.json_path}")
    print(f"SURFACE_EXPANSION_PLAN_ID={plan.action_id}")
    print(f"SURFACE_EXPANSION_PLAN_RECOMMENDATION={plan.recommendation}")
    print(f"SURFACE_EXPANSION_PLAN_NEXT_ACTION={plan.next_operator_action}")
    print(f"SURFACE_EXPANSION_PLAN_CONTROL_UPDATED={'yes' if pushed else 'no'}")
    return 0


def cmd_push_intake_template(args: argparse.Namespace) -> int:
    client = _intake_client(args)
    client.ensure_sheet_exists()
    existing_rows = client.read_rows(range_suffix="A1:ZZ5000")
    if existing_rows and not args.force:
        client.ensure_columns(INTAKE_SHEET_HEADERS)
        print(f"INTAKE_SHEET_ID={args.intake_sheet_id or args.control_sheet_id or args.board_sheet_id}")
        print(f"INTAKE_SHEET_NAME={args.intake_sheet_name}")
        print("INTAKE_TEMPLATE_UPDATED=headers_only")
        return 0
    client.overwrite_table(headers=INTAKE_SHEET_HEADERS, rows=[])
    print(f"INTAKE_SHEET_ID={args.intake_sheet_id or args.control_sheet_id or args.board_sheet_id}")
    print(f"INTAKE_SHEET_NAME={args.intake_sheet_name}")
    print("INTAKE_TEMPLATE_UPDATED=table")
    return 0


def cmd_process_intake(args: argparse.Namespace) -> int:
    client = _intake_client(args)
    client.ensure_sheet_exists()
    client.ensure_columns(INTAKE_SHEET_HEADERS)
    rows = client.read_rows(range_suffix="A1:ZZ5000")
    updates = process_intake_rows(
        rows=rows,
        hypotheses_dir=Path(args.hypotheses_dir),
        out_dir=Path(args.out_dir),
        apply=args.apply,
        limit=args.limit,
        force=args.force,
    )
    if args.apply and updates:
        client.batch_update_rows(
            rows=updates,
            columns=[
                "operator_action",
                "status",
                "feasibility_tag",
                "feasibility_summary",
                "search_param_keys",
                "discovery_config_count",
                "retune_config_count",
                "hypothesis_id",
                "hypothesis_path",
                "report_path",
                "updated_at",
                "created_at",
            ],
        )
    if args.output:
        _write_csv(Path(args.output), updates)
    else:
        print(json.dumps(updates, indent=2))
    print(f"INTAKE_ACTIONS={len(updates)}")
    print(f"INTAKE_APPLIED={len(updates) if args.apply else 0}")
    print(f"DRY_RUN={'false' if args.apply else 'true'}")
    return 0


def cmd_publish_pending(args: argparse.Namespace) -> int:
    catalog_client = _strategy_catalog_client(args)
    catalog_rows = catalog_client.read_rows(range_suffix="A1:ZZ5000")
    catalog_keys = {
        str(row.get("catalog_key", "")).strip()
        for row in catalog_rows
        if str(row.get("catalog_key", "")).strip()
    }
    ledger = build_ledger(
        hypotheses_dir=Path(args.hypotheses_dir),
        runs_dir=Path(args.runs_dir),
        strategy_catalog_rows=catalog_rows,
    )
    rows = _catalog_publish_plan(
        ledger=ledger,
        catalog_keys=catalog_keys,
        only_catalog_key=args.catalog_key,
    )
    results = _publish_catalog_rows(rows=rows, args=args, dry_run=not args.apply)
    if args.output:
        _write_csv(Path(args.output), results)
    else:
        print(json.dumps(results, indent=2))
    print(f"CATALOG_PENDING={len(rows)}")
    print(f"CATALOG_PUBLISHED={0 if not args.apply else len(results)}")
    print(f"DRY_RUN={'false' if args.apply else 'true'}")
    return 0


def cmd_sync_board(args: argparse.Namespace) -> int:
    board_client = _board_client(args)
    board_rows = board_client.read_rows(range_suffix="A1:ZZ5000")
    catalog_rows = _read_strategy_catalog_rows(args) if args.with_catalog else []
    ledger = build_ledger(
        hypotheses_dir=Path(args.hypotheses_dir),
        runs_dir=Path(args.runs_dir),
        strategy_catalog_rows=catalog_rows,
    )
    updates = _board_sync_plan(
        ledger=ledger,
        board_rows=board_rows,
        only_task_id=args.task_id,
    )
    public_rows = [
        {
            "Task_ID": row.get("Task_ID", ""),
            "matched_hypothesis_id": row.get("_matched_hypothesis_id", ""),
            "Operator_Action": row.get("Operator_Action", ""),
            "Agent_State": row.get("Agent_State", ""),
            "Current_Stage": row.get("Current_Stage", ""),
            "Recommendation": row.get("Recommendation", ""),
        }
        for row in updates
    ]
    if args.output:
        _write_csv(Path(args.output), public_rows)
    else:
        print(json.dumps(public_rows, indent=2))
    if args.apply and updates:
        clean_updates = []
        for row in updates:
            clean = dict(row)
            clean.pop("_matched_hypothesis_id", None)
            clean_updates.append(clean)
        board_client.batch_update_rows(
            rows=clean_updates,
            columns=["Operator_Action", "Agent_State", "Current_Stage", "Recommendation"],
        )
    print(f"BOARD_UPDATES={len(updates)}")
    print(f"BOARD_APPLIED={len(updates) if args.apply else 0}")
    print(f"DRY_RUN={'false' if args.apply else 'true'}")
    return 0


def cmd_mark_stale(args: argparse.Namespace) -> int:
    disposition = append_disposition(
        path=Path(args.dispositions_path),
        key=args.key,
        category=args.category,
        status="stale",
        reason=args.reason,
        operator=args.operator,
    )
    print(f"DISPOSITION_PATH={Path(args.dispositions_path)}")
    print(f"DISPOSITION_STATUS={disposition.status}")
    print(f"DISPOSITION_CATEGORY={disposition.category}")
    print(f"DISPOSITION_KEY={disposition.key}")
    return 0


def cmd_clear_disposition(args: argparse.Namespace) -> int:
    disposition = append_disposition(
        path=Path(args.dispositions_path),
        key=args.key,
        category=args.category,
        status="cleared",
        reason=args.reason,
        operator=args.operator,
    )
    print(f"DISPOSITION_PATH={Path(args.dispositions_path)}")
    print(f"DISPOSITION_STATUS={disposition.status}")
    print(f"DISPOSITION_CATEGORY={disposition.category}")
    print(f"DISPOSITION_KEY={disposition.key}")
    return 0


def cmd_dispositions(args: argparse.Namespace) -> int:
    rows = [asdict(row) for row in read_dispositions(Path(args.dispositions_path))]
    if args.format == "json":
        print(json.dumps(rows, indent=2))
    else:
        _write_csv(Path(args.output), rows)
        print(f"DISPOSITIONS_CSV={Path(args.output)}")
    print(f"DISPOSITIONS={len(rows)}")
    return 0


def _add_common_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--hypotheses-dir", default=str(DEFAULT_HYPOTHESES_DIR))
    parser.add_argument("--runs-dir", default=str(DEFAULT_RUNS_DIR))
    parser.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR))
    parser.add_argument("--dispositions-path", default=str(DEFAULT_DISPOSITIONS_PATH))
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
    parser.add_argument("--intake-google-credentials", default="")
    parser.add_argument("--intake-sheet-id", default="")
    parser.add_argument("--intake-sheet-name", default=DEFAULT_INTAKE_SHEET_NAME)
    parser.add_argument("--with-catalog", action="store_true", help="Read Strategy_Catalog and mark promoted rows present/absent.")
    parser.add_argument("--with-board", action="store_true", help="Read Scout_Queue and include stale-board findings.")
    parser.add_argument("--with-control", action="store_true", help="Read Research_Control rows for digest/reporting.")
    parser.add_argument("--with-intake", action="store_true", help="Read Research_Intake rows for digest/reporting.")


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

    digest = subparsers.add_parser("digest", help="Write a daily/weekly research operations digest.")
    _add_common_args(digest)
    digest.add_argument("--days", type=int, default=1)
    digest.add_argument("--limit", type=int, default=25)
    digest.add_argument("--output", default="")
    digest.set_defaults(func=cmd_digest)

    next_actions = subparsers.add_parser("next-actions", help="Write or print the ranked operator action queue.")
    _add_common_args(next_actions)
    next_actions.add_argument("--format", choices=["md", "csv", "json"], default="md")
    next_actions.add_argument("--output", default="")
    next_actions.add_argument("--limit", type=int, default=0)
    next_actions.set_defaults(func=cmd_next_actions)

    push_control = subparsers.add_parser("push-control", help="Mirror next-actions into a Google Sheet control tab.")
    _add_common_args(push_control)
    push_control.add_argument("--limit", type=int, default=25)
    push_control.set_defaults(func=cmd_push_control)

    action_brief = subparsers.add_parser("action-brief", help="Write an evidence brief for a queued Research_Control action.")
    _add_common_args(action_brief)
    action_brief.add_argument("--key", required=True, help="Hypothesis key or action_id, e.g. retune_plan:my-hypothesis.")
    action_brief.add_argument("--action-type", default="", help="Optional action type when --key is only the hypothesis id.")
    action_brief.add_argument("--push-control", action="store_true", help="Mirror recommendation/summary/path to Research_Control.")
    action_brief.set_defaults(func=cmd_action_brief)

    surface_plan = subparsers.add_parser(
        "surface-expansion-plan",
        help="Write a bounded config-surface expansion plan for an approved queued retune.",
    )
    _add_common_args(surface_plan)
    surface_plan.add_argument("--key", required=True, help="Hypothesis key or action_id, e.g. retune_plan:my-hypothesis.")
    surface_plan.add_argument("--push-control", action="store_true", help="Mirror plan recommendation/summary/path to Research_Control.")
    surface_plan.set_defaults(func=cmd_surface_expansion_plan)

    intake_template = subparsers.add_parser("push-intake-template", help="Create or update the Research_Intake sheet headers.")
    _add_common_args(intake_template)
    intake_template.add_argument("--force", action="store_true", help="Overwrite the intake table even when rows already exist.")
    intake_template.set_defaults(func=cmd_push_intake_template)

    process_intake = subparsers.add_parser("process-intake", help="Evaluate or create approved Research_Intake rows.")
    _add_common_args(process_intake)
    process_intake.add_argument("--limit", type=int, default=1)
    process_intake.add_argument("--apply", action="store_true", help="Update the sheet and create approved hypothesis files.")
    process_intake.add_argument("--dry-run", action="store_true", help="Explicit no-op alias; dry-run is the default.")
    process_intake.add_argument("--force", action="store_true", help="Allow overwriting an existing hypothesis file.")
    process_intake.add_argument("--output", default="", help="Optional CSV output path for processed rows.")
    process_intake.set_defaults(func=cmd_process_intake)

    publish = subparsers.add_parser("publish-pending", help="Dry-run or publish promoted Strategy_Catalog rows missing from the sheet.")
    _add_common_args(publish)
    publish.add_argument("--catalog-key", default="", help="Limit to one catalog_key.")
    publish.add_argument("--apply", action="store_true", help="Actually upsert rows into Strategy_Catalog. Omit for dry-run.")
    publish.add_argument("--dry-run", action="store_true", help="Explicit no-op alias; dry-run is the default.")
    publish.add_argument("--output", default="", help="Optional CSV output path for the publish plan.")
    publish.set_defaults(func=cmd_publish_pending)

    sync_board = subparsers.add_parser("sync-board", help="Dry-run or apply Scout_Queue status updates from Mala ledger state.")
    _add_common_args(sync_board)
    sync_board.add_argument("--task-id", default="", help="Limit to one Scout_Queue Task_ID.")
    sync_board.add_argument("--apply", action="store_true", help="Actually update Scout_Queue. Omit for dry-run.")
    sync_board.add_argument("--dry-run", action="store_true", help="Explicit no-op alias; dry-run is the default.")
    sync_board.add_argument("--output", default="", help="Optional CSV output path for the sync plan.")
    sync_board.set_defaults(func=cmd_sync_board)

    mark_stale = subparsers.add_parser("mark-stale", help="Mark a finding key as stale without moving or deleting artifacts.")
    _add_common_args(mark_stale)
    mark_stale.add_argument("--key", required=True, help="Finding key, e.g. hypothesis/run_ts.")
    mark_stale.add_argument("--category", default="", help="Optional exact finding category. Blank suppresses this key across categories.")
    mark_stale.add_argument("--reason", required=True)
    mark_stale.add_argument("--operator", default="")
    mark_stale.set_defaults(func=cmd_mark_stale)

    clear = subparsers.add_parser("clear-disposition", help="Clear a prior stale/archive disposition for a finding key.")
    _add_common_args(clear)
    clear.add_argument("--key", required=True)
    clear.add_argument("--category", default="")
    clear.add_argument("--reason", required=True)
    clear.add_argument("--operator", default="")
    clear.set_defaults(func=cmd_clear_disposition)

    disp = subparsers.add_parser("dispositions", help="List finding dispositions.")
    _add_common_args(disp)
    disp.add_argument("--format", choices=["json", "csv"], default="json")
    disp.add_argument("--output", default=str(DEFAULT_OUT_DIR / "dispositions.csv"))
    disp.set_defaults(func=cmd_dispositions)

    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
