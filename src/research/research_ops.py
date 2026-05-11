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
from src.research.bhiksha_plumbing_triage import build_bhiksha_plumbing_triage
from src.research.bhiksha_signal_ev import build_bhiksha_signal_ev_report
from src.research.google_sheets import GoogleSheetTableClient
from src.research.provider_validation_m6 import (
    build_m6_provider_validation,
    discover_latest_m5_run_dirs,
)
from src.research.provider_volume_parity import build_provider_volume_parity_report
from src.research.research_runner import create_hypothesis_file
from src.research.search_space import build_search_configs, search_param_keys
from src.research.shadow_campaign import (
    DEFAULT_ACTIVE_STRATEGY_SHEET_NAME,
    DEFAULT_EVIDENCE_SHEET_NAME,
    DEFAULT_OPERATOR_DEFAULTS_SHEET_NAME,
    ShadowActivationConfig,
    apply_active_strategy_rows,
    apply_operator_defaults_patch,
    build_shadow_activation_packet,
    build_shadow_daily_report,
    read_sheet_rows,
)
from src.research.time_utils import SHEET_TIMEZONE, sheet_timestamp
from src.strategy.factory import available_strategy_names


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_HYPOTHESES_DIR = REPO_ROOT / "research" / "hypotheses"
DEFAULT_RUNS_DIR = REPO_ROOT / "data" / "results" / "hypothesis_runs"
DEFAULT_OUT_DIR = REPO_ROOT / "data" / "results" / "research_ops"
DEFAULT_DISPOSITIONS_PATH = REPO_ROOT / "research" / "reports" / "research_ops" / "finding_dispositions.jsonl"
DEFAULT_CONTROL_SHEET_NAME = "Research_Control"
DEFAULT_INTAKE_SHEET_NAME = "Research_Intake"
DEFAULT_OPTIONS_SHEET_NAME = "op_options"
DEFAULT_SHADOW_CAMPAIGN_DIR = REPO_ROOT / "data" / "results" / "shadow_campaign"
DEFAULT_LIVE_FEEDBACK_DIR = REPO_ROOT / "data" / "live_feedback"

CONTROL_SHEET_HEADERS = [
    "rank",
    "status",
    "recommendation",
    "recommended_operator_action",
    "operator_action",
    "decision_needed",
    "symbol_scope",
    "strategy",
    "latest_stage",
    "reason",
    "evidence_summary",
    "next_step",
    "artifact_path",
    "action_id",
    "action_type",
    "key",
    "priority",
    "brief_recommendation",
    "brief_summary",
    "brief_path",
    "last_report_path",
    "suggested_command",
    "requires_approval",
    "mutates_external_state",
    "updated_at",
    "generated_at",
]

CONTROL_OPERATOR_ACTIONS = {
    "",
    "APPROVE_CONTINUE_M2",
    "APPROVE_CONTINUE_M3",
    "APPROVE_CONTINUE_M4",
    "APPROVE_CONTINUE_M5",
    "APPROVE_KILL",
    "APPROVE_RUN_M1",
    "APPROVE_RETUNE",
    "APPROVE_PUBLISH",
    "APPROVE_BOARD_SYNC",
    "APPROVE_SURFACE_EXPANSION",
    "MARK_STALE",
    "SKIP",
}

CONTROL_OPERATOR_ACTION_DROPDOWN = [
    "APPROVE_CONTINUE_M2",
    "APPROVE_CONTINUE_M3",
    "APPROVE_CONTINUE_M4",
    "APPROVE_CONTINUE_M5",
    "APPROVE_RUN_M1",
    "APPROVE_RETUNE",
    "APPROVE_SURFACE_EXPANSION",
    "APPROVE_KILL",
    "SKIP",
    "MARK_STALE",
    "APPROVE_PUBLISH",
    "APPROVE_BOARD_SYNC",
]

INTAKE_SHEET_HEADERS = [
    "status",
    "recommendation",
    "recommended_operator_action",
    "operator_action",
    "decision_needed",
    "title",
    "symbol_scope",
    "strategy",
    "thesis",
    "reason_to_try",
    "risk_or_overlap",
    "suggested_config",
    "source",
    "max_stage",
    "intake_id",
    "hypothesis_id",
    "rules",
    "notes",
    "feasibility_tag",
    "feasibility_summary",
    "search_param_keys",
    "discovery_config_count",
    "retune_config_count",
    "hypothesis_path",
    "report_path",
    "updated_at",
    "created_at",
    "research_ops_notes",
    "proposed_by",
    "proposed_at",
]

INTAKE_OPERATOR_ACTIONS = {
    "",
    "EVALUATE",
    "APPROVE_CREATE_HYPOTHESIS",
    "SKIP",
}

INTAKE_OPERATOR_ACTION_DROPDOWN = [
    "EVALUATE",
    "APPROVE_CREATE_HYPOTHESIS",
    "SKIP",
]

OP_OPTIONS_HEADERS = [
    "Operator_Action in Research Control",
    "",
    "Operator_Action in Research_Intake",
]

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
    dispositions: list[FindingDisposition]


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


def _action_is_disposed(
    *,
    action_type: str,
    key: str,
    dispositions: list[FindingDisposition],
) -> bool:
    latest = _latest_disposition_by_target(dispositions)
    action_key = f"{action_type}:{key}"
    disposition = (
        latest.get(("control_skip", action_key))
        or latest.get(("control_skip", key))
        or latest.get((action_type, key))
        or latest.get(("", action_key))
    )
    if disposition is None:
        return False
    return disposition.status in {"stale", "archived", "ignore", "skipped"}


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
        dispositions=list(dispositions or []),
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
        if _action_is_disposed(action_type=action_type, key=key, dispositions=ledger.dispositions):
            return
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


def _operator_decision_text(sheet_name: str, action: str) -> str:
    if action:
        return f"Choose {action} in {sheet_name}.operator_action to apply; leave blank to defer."
    return "Review evidence before choosing an operator_action."


def _control_next_step(action: str, recommendation: str = "") -> str:
    if action == "SKIP":
        return "Use SKIP to clean this from the queue unless you want a new thesis."
    if action == "APPROVE_RUN_M1":
        return "Use APPROVE_RUN_M1 to run the first M1 feasibility pass for this pending hypothesis."
    if action.startswith("APPROVE_CONTINUE_M"):
        stage = action.rsplit("_", 1)[-1]
        return f"Use {action} to continue a passing hypothesis through {stage} only."
    if action == "APPROVE_RETUNE":
        return "Use APPROVE_RETUNE only if the bounded retune still has a credible edge thesis."
    if action == "APPROVE_SURFACE_EXPANSION":
        return "Use APPROVE_SURFACE_EXPANSION to request a config-surface plan before another run."
    if action == "MARK_STALE":
        return "Use MARK_STALE if this artifact no longer needs repair."
    if action:
        return "Use the dropdown action only if the linked evidence matches the intended mutation."
    if recommendation.startswith("CONFIG_ONLY_"):
        return "Ask Codex/research agent to apply the config-only search-surface patch before approving a retune."
    return "Leave blank until the evidence is clear."


def _surface_plan_operator_action(recommendation: str) -> str:
    if recommendation in {"CONTINUATION_REVIEW", "EVIDENCE_THIN", "NO_ACTION", "RETHINK_BEFORE_EXPANSION"}:
        return "SKIP"
    return ""


def _control_decision_fields(
    *,
    ledger: ResearchLedger | None,
    item: NextAction,
    existing: dict[str, Any],
) -> dict[str, Any]:
    hypothesis = _find_hypothesis(ledger, item.key) if ledger is not None else None
    latest_run = _latest_run(ledger, item.key) if ledger is not None else None
    status = str(existing.get("status", "")).strip()
    brief_path = str(
        existing.get("brief_path", "") or existing.get("last_report_path", "") or existing.get("artifact_path", "")
    ).strip()
    surface_plan_ready = status == "surface_plan_ready" or "surface_expansion/" in brief_path
    if surface_plan_ready:
        recommendation = str(existing.get("brief_recommendation", "") or existing.get("recommendation", ""))
    else:
        recommendation = str(existing.get("recommendation", "") or existing.get("brief_recommendation", ""))
    recommended_action = str(existing.get("recommended_operator_action", ""))
    evidence_summary = str(existing.get("evidence_summary", "") or existing.get("brief_summary", ""))
    artifact_path = str(existing.get("artifact_path", "") or existing.get("brief_path", "") or existing.get("last_report_path", ""))
    if surface_plan_ready and recommendation.startswith("CONFIG_ONLY_"):
        recommended_action = ""
    elif surface_plan_ready and (not recommended_action or recommended_action == "APPROVE_SURFACE_EXPANSION"):
        recommended_action = _surface_plan_operator_action(recommendation)
    if surface_plan_ready and brief_path:
        artifact_path = brief_path
    if latest_run is not None and not artifact_path:
        artifact_path = latest_run.artifact_dir

    if ledger is not None and not surface_plan_ready:
        try:
            brief = build_action_brief(ledger=ledger, key=item.key, action_type=item.action_type)
            recommendation = brief.recommendation
            recommended_action = brief.suggested_operator_action
            evidence_summary = _brief_cell(brief.summary, max_chars=650)
            artifact_path = artifact_path or "; ".join(brief.sources[:2])
        except Exception as exc:  # pragma: no cover - defensive; control sync should not fail on one brief
            recommendation = recommendation or "INSPECT"
            evidence_summary = evidence_summary or f"Could not build evidence brief: {exc}"

    return {
        "recommendation": recommendation,
        "recommended_operator_action": recommended_action,
        "decision_needed": _operator_decision_text(DEFAULT_CONTROL_SHEET_NAME, recommended_action),
        "symbol_scope": hypothesis.symbol_scope if hypothesis is not None else str(existing.get("symbol_scope", "")),
        "strategy": hypothesis.strategy if hypothesis is not None else str(existing.get("strategy", "")),
        "latest_stage": hypothesis.latest_stage if hypothesis is not None else str(existing.get("latest_stage", "")),
        "evidence_summary": evidence_summary,
        "next_step": _control_next_step(recommended_action, recommendation),
        "artifact_path": artifact_path,
    }


def build_control_rows(
    *,
    actions: list[NextAction],
    generated_at: str,
    existing_rows: list[dict[str, Any]] | None = None,
    ledger: ResearchLedger | None = None,
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
        existing_status = str(existing.get("status", "") or "queued")
        if operator_action not in CONTROL_OPERATOR_ACTIONS:
            status = f"invalid_operator_action:{operator_action}"
        else:
            status = existing_status
        decision_fields = _control_decision_fields(ledger=ledger, item=item, existing=existing)
        rows.append(
            {
                "rank": item.rank,
                "status": status,
                **decision_fields,
                "operator_action": operator_action,
                "reason": item.reason,
                "action_id": action_id(item),
                "action_type": item.action_type,
                "key": item.key,
                "priority": item.priority,
                "brief_recommendation": decision_fields["recommendation"],
                "brief_summary": decision_fields["evidence_summary"],
                "brief_path": str(existing.get("brief_path", "")),
                "last_report_path": str(existing.get("last_report_path", "")),
                "suggested_command": item.suggested_command,
                "requires_approval": item.requires_approval,
                "mutates_external_state": item.mutates_external_state,
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
    researcher_verdict: dict[str, Any] | None = None,
) -> tuple[str, str, str]:
    action_type = action.action_type if action else ""
    if action_type == "publish_pending":
        return "PUBLISH_REVIEW", "APPROVE_PUBLISH", "Catalog write is external state; dedupe and review execution fields before applying."
    if action_type == "sync_board":
        return "BOARD_SYNC_REVIEW", "APPROVE_BOARD_SYNC", "Board state is stale relative to Mala; sync after confirming the matched row."
    if action_type in {"repair_run_summary", "inspect_terminal"}:
        return "MARK_STALE_OR_REPAIR", "MARK_STALE", "Evidence is incomplete; mark stale if the artifact is not needed, otherwise repair before use."
    if action_type == "run_m1":
        return "RUN_M1_REVIEW", "APPROVE_RUN_M1", "Pending hypothesis is config-only; run M1 only after thesis review."

    if hypothesis is None:
        return "INSPECT", "SKIP", "Action key does not map to a local hypothesis; inspect before execution."
    combined = f"{hypothesis.state} {hypothesis.decision} {summary_text}".lower()
    if action_type == "resume_or_normalize":
        for stage in ("M2", "M3", "M4", "M5"):
            if hypothesis.state == "running" and f"promote_to_{stage.lower()}" in combined:
                return (
                    f"CONTINUE_{stage}_REVIEW",
                    f"APPROVE_CONTINUE_{stage}",
                    f"{hypothesis.latest_stage} passed; continue through {stage} only before any catalog/publish work.",
                )
        return "RESUME_OR_NORMALIZE_REVIEW", "SKIP", "Running hypothesis needs inspection before resume or normalization."
    if hypothesis.state == "kill":
        return "NO_ACTION", "SKIP", "Hypothesis is already kill; do not spend more research cycles unless a new thesis is written."
    if hypothesis.state == "completed":
        return "PUBLISH_REVIEW", "APPROVE_PUBLISH", "Hypothesis is completed; use publish review only if a selected catalog row is missing."

    if action_type == "retune_plan":
        verdict = researcher_verdict or {}
        verdict_recommendation = str(verdict.get("recommendation") or "")
        verdict_rationale = str(verdict.get("rationale") or "")
        if verdict_recommendation == "close_as_smoke_test":
            return "CLOSE_SMOKE_TEST", "SKIP", verdict_rationale or "Researcher verdict classifies this as smoke/plumbing evidence, not an alpha retune."
        if "m1 fail: no positive configs" in combined:
            return "KILL_OR_SKIP", "SKIP", "Latest retune found no positive configs; archive or kill unless the thesis changes materially."
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
        if verdict_recommendation == "approve_surface_expansion":
            return "SURFACE_EXPANSION_REVIEW", "APPROVE_SURFACE_EXPANSION", verdict_rationale or "Researcher verdict supports a targeted surface expansion."
        if verdict_recommendation == "approve_bounded_retune":
            return "APPROVE_RETUNE", "APPROVE_RETUNE", verdict_rationale or "Researcher verdict supports only a bounded diagnostic retune."
        if verdict_recommendation == "defer_for_better_evidence":
            return "DEFER_FOR_BETTER_EVIDENCE", "", verdict_rationale or "Researcher verdict says the evidence is too thin to recommend more compute now."
        if verdict_recommendation == "reject_or_kill":
            return "KILL_OR_SKIP", "SKIP", verdict_rationale or "Researcher verdict does not support another research cycle."
        if "m1 fail" in combined and ("signals=" in combined or "windows=" in combined or "pct_pos=" in combined):
            return "SURFACE_EXPANSION_REVIEW", "APPROVE_SURFACE_EXPANSION", "M1 failed on sample/stability; inspect whether the search surface is too narrow before another retune."
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
    m1_aggregate_rows = _read_csv_dicts(latest_dir / "M1_aggregate.csv") if latest_dir else []
    m1_detail_rows = _read_csv_dicts(latest_dir / "M1_detail.csv") if latest_dir else []
    m2_rows = _read_csv_dicts(latest_dir / "M2_gate_report.csv") if latest_dir else []
    story = _hypothesis_story(hypothesis.file_path if hypothesis else "")
    researcher_verdict = _build_researcher_verdict(
        hypothesis_metadata={
            "title": story.get("title", ""),
            "thesis": story.get("thesis", ""),
            "strategy": hypothesis.strategy if hypothesis else "",
            "symbol_scope": hypothesis.symbol_scope if hypothesis else "",
        },
        m1_rows=m1_rows,
        aggregate_rows=m1_aggregate_rows,
        detail_rows=m1_detail_rows,
        m2_rows=m2_rows,
        run_summary_text=summary_text,
    )
    recommendation, operator_action, summary = _brief_recommendation(
        hypothesis=hypothesis,
        action=action,
        m1_rows=m1_rows,
        m2_rows=m2_rows,
        summary_text=summary_text,
        researcher_verdict=researcher_verdict,
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
            "",
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
                "",
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
        next_row["recommendation"] = plan.recommendation
        next_row["recommended_operator_action"] = plan.next_operator_action
        next_row["decision_needed"] = _operator_decision_text(DEFAULT_CONTROL_SHEET_NAME, plan.next_operator_action)
        next_row["evidence_summary"] = _brief_cell(plan.summary, max_chars=650)
        next_row["next_step"] = _control_next_step(plan.next_operator_action, plan.recommendation)
        next_row["artifact_path"] = plan.report_path
        next_row["brief_recommendation"] = plan.recommendation
        next_row["brief_summary"] = _brief_cell(plan.summary)
        next_row["brief_path"] = plan.report_path
        next_row["last_report_path"] = plan.report_path
        next_row["status"] = "surface_plan_ready"
        next_row["updated_at"] = sheet_timestamp()
        client.batch_update_rows(
            rows=[next_row],
            columns=[
                "recommendation",
                "recommended_operator_action",
                "decision_needed",
                "evidence_summary",
                "next_step",
                "artifact_path",
                "brief_recommendation",
                "brief_summary",
                "brief_path",
                "last_report_path",
                "status",
                "updated_at",
            ],
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


def _intake_decision_fields(row: dict[str, Any]) -> dict[str, str]:
    status = str(row.get("status", "")).strip()
    operator_action = str(row.get("operator_action", "")).strip().upper()
    feasibility_tag = str(row.get("feasibility_tag", "")).strip()
    if operator_action:
        recommendation = "PENDING_OPERATOR_ACTION"
        recommended_action = operator_action
        decision = f"{operator_action} is pending; leave it only if this is intentional."
    elif status == "proposed_by_research_ops":
        recommendation = "EVALUATE"
        recommended_action = "EVALUATE"
        decision = "Choose EVALUATE to feasibility-check this proposal; leave blank to defer."
    elif status == "evaluated_ready_for_approval":
        recommendation = "CREATE_HYPOTHESIS"
        recommended_action = "APPROVE_CREATE_HYPOTHESIS"
        decision = "Choose APPROVE_CREATE_HYPOTHESIS to create a pending hypothesis."
    elif status.startswith("blocked_"):
        recommendation = feasibility_tag or status
        recommended_action = ""
        decision = "Blocked; route to code/human research before choosing an action."
    elif status in {"created_pending", "existing_hypothesis"}:
        recommendation = "NO_ACTION"
        recommended_action = ""
        decision = "Already converted to a hypothesis; no intake action needed."
    elif status == "skipped":
        recommendation = "NO_ACTION"
        recommended_action = ""
        decision = "Skipped; no intake action needed."
    else:
        recommendation = feasibility_tag or "REVIEW"
        recommended_action = ""
        decision = "Review this row before choosing an operator_action."
    return {
        "recommendation": recommendation,
        "recommended_operator_action": recommended_action,
        "decision_needed": decision,
    }


def _enrich_intake_row(row: dict[str, Any]) -> dict[str, Any]:
    next_row = dict(row)
    if not str(next_row.get("reason_to_try", "")).strip():
        next_row["reason_to_try"] = str(next_row.get("thesis", "")).strip()
    if not str(next_row.get("suggested_config", "")).strip():
        next_row["suggested_config"] = str(next_row.get("rules", "") or next_row.get("notes", "")).strip()
    if not str(next_row.get("risk_or_overlap", "")).strip() and str(next_row.get("rules", "")).strip():
        next_row["risk_or_overlap"] = str(next_row.get("notes", "")).strip()
    next_row.update(_intake_decision_fields(next_row))
    return next_row


def build_intake_proposal_row(
    *,
    intake_id: str,
    title: str,
    strategy: str,
    symbol_scope: str,
    thesis: str,
    hypothesis_id: str = "",
    rules: str = "",
    notes: str = "",
    suggested_config: str = "",
    reason_to_try: str = "",
    risk_or_overlap: str = "",
    max_stage: str = "M2",
    feasibility_tag: str = "",
    feasibility_summary: str = "",
    source: str = "",
    research_ops_notes: str = "",
    proposed_by: str = "research_ops",
    proposed_at: str = "",
    existing_row: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Create a review-only Research_Intake proposal row."""
    now = proposed_at or sheet_timestamp()
    clean_intake_id = _slug(intake_id or title)
    clean_hypothesis_id = _slug(hypothesis_id or clean_intake_id)
    proposal_notes = "\n".join(part for part in [suggested_config.strip(), notes.strip()] if part)
    base = {
        "intake_id": clean_intake_id,
        "title": title.strip(),
        "hypothesis_id": clean_hypothesis_id,
        "strategy": strategy.strip(),
        "symbol_scope": symbol_scope.strip(),
        "thesis": thesis.strip(),
        "rules": rules.strip(),
        "notes": proposal_notes,
        "max_stage": max_stage.strip() or "M2",
    }
    evaluation = evaluate_hypothesis_intake(base)
    existing = existing_row or {}
    created_at = str(existing.get("created_at", "")).strip() or now
    row = {
        **base,
        "operator_action": "",
        "status": "proposed_by_research_ops",
        "reason_to_try": reason_to_try.strip() or thesis.strip(),
        "risk_or_overlap": risk_or_overlap.strip(),
        "suggested_config": suggested_config.strip(),
        "feasibility_tag": feasibility_tag.strip() or evaluation.feasibility_tag,
        "feasibility_summary": feasibility_summary.strip() or evaluation.feasibility_summary,
        "search_param_keys": evaluation.search_param_keys,
        "discovery_config_count": evaluation.discovery_config_count,
        "retune_config_count": evaluation.retune_config_count,
        "hypothesis_path": str(existing.get("hypothesis_path", "")),
        "report_path": str(existing.get("report_path", "")),
        "updated_at": now,
        "created_at": created_at,
        "source": source.strip(),
        "research_ops_notes": research_ops_notes.strip(),
        "proposed_by": proposed_by.strip() or "research_ops",
        "proposed_at": str(existing.get("proposed_at", "")).strip() or now,
    }
    return _enrich_intake_row(row)


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
    return _enrich_intake_row(next_row)


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



PROGRAM_STATUS_TAGS = {"auto_continue", "needs_suman", "blocked", "running", "done"}
DEFAULT_OBSIDIAN_VAULT = Path("/Users/sunny/Library/Mobile Documents/iCloud~md~obsidian/Documents/northstar")
DECISION_CARD_DIR = Path("Projects/Trading/Mala/Research/Decision Cards")
COMMENTS_START = "<!-- mala-card-comments:start -->"
COMMENTS_END = "<!-- mala-card-comments:end -->"
RECEIPT_START = "<!-- mala-card-receipt:start -->"
RECEIPT_END = "<!-- mala-card-receipt:end -->"


def _safe_id(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "-", str(value).strip()).strip("-").lower() or "item"


def _latest_file(root: Path, pattern: str) -> Path | None:
    if not root.exists():
        return None
    matches = [path for path in root.glob(pattern) if path.is_file()]
    if not matches:
        return None
    return sorted(matches, key=lambda path: (path.stat().st_mtime, str(path)), reverse=True)[0]


def _latest_shadow_brief(vault: Path | None = None) -> dict[str, str]:
    candidates: list[Path] = []
    if vault is not None:
        root = vault / "Projects" / "Trading" / "Mala" / "Shadow"
        if root.exists():
            candidates.extend(root.glob("*.md"))
    if DEFAULT_SHADOW_CAMPAIGN_DIR.exists():
        candidates.extend(DEFAULT_SHADOW_CAMPAIGN_DIR.rglob("*.md"))
    candidates = [path for path in candidates if path.is_file() and not path.name.startswith(".")]
    if not candidates:
        return {}
    note = sorted(candidates, key=lambda path: (path.stat().st_mtime, str(path)), reverse=True)[0]
    text = note.read_text(encoding="utf-8", errors="replace")[:5000]
    title_match = re.search(r"^#\s+(.+)$", text, re.MULTILINE)
    verdict_match = re.search(r"decision_verdict:\s*(?:\*\*)?`?([^`*\n]+)", text, re.IGNORECASE)
    owner_match = re.search(r"^-\s*Owner:\s*(.+)$", text, re.MULTILINE | re.IGNORECASE)
    return {
        "title": title_match.group(1).strip() if title_match else note.stem,
        "verdict": verdict_match.group(1).strip() if verdict_match else "",
        "owner": (owner_match.group(1).split(".")[0].strip() if owner_match else "Research Ops"),
        "path": str(note),
    }


def _read_sheet_rows_for_status(args: argparse.Namespace, kind: str) -> tuple[list[dict[str, Any]], list[str]]:
    if not getattr(args, f"with_{kind}", False):
        return [], [f"{kind} sheet not requested; run with --with-{kind} and credentials to include live sheet state."]
    try:
        rows = _read_control_rows(args) if kind == "control" else _read_intake_rows(args)
        if not rows:
            return [], [f"{kind} sheet returned no rows or sheet access was unavailable."]
        return rows, []
    except Exception as exc:  # pragma: no cover - defensive degradation path
        return [], [f"{kind} sheet unavailable: {exc}"]


def _classify_action(action: NextAction) -> str:
    if action.action_type in {"repair_run_summary", "inspect_terminal"}:
        return "blocked"
    if action.action_type == "resume_or_normalize":
        return "running"
    if action.requires_approval == "yes" or action.mutates_external_state == "yes":
        return "needs_suman"
    return "auto_continue"


def _status_item_from_action(action: NextAction, ledger: ResearchLedger) -> dict[str, Any]:
    tag = _classify_action(action)
    hyp = _find_hypothesis(ledger, action.key)
    latest_run = _latest_run(ledger, action.key)
    source = hyp.file_path if hyp else (latest_run.artifact_dir if latest_run else action.key)
    item = {
        "id": action_id(action),
        "tag": tag,
        "title": f"{action.action_type}: {action.key}",
        "why": action.reason,
        "next": action.suggested_command,
        "owner": "Suman" if tag == "needs_suman" else ("Codex/Jarvis" if tag == "blocked" else "Research Ops"),
        "source": source,
        "priority": action.priority,
        "action_type": action.action_type,
        "key": action.key,
        "requires_approval": action.requires_approval,
        "mutates_external_state": action.mutates_external_state,
    }
    if action.action_type == "retune_plan":
        item["brief"] = _research_brief_for_action(action, ledger)
    return item


def build_program_status(args: argparse.Namespace) -> dict[str, Any]:
    ledger = _build_with_optional_sheets(args)
    actions = build_next_actions(ledger)
    if getattr(args, "limit", 0):
        actions = actions[: args.limit]
    control_rows, control_warnings = _read_sheet_rows_for_status(args, "control")
    intake_rows, intake_warnings = _read_sheet_rows_for_status(args, "intake")
    out_dir = Path(args.out_dir)
    latest_digest = _latest_file(out_dir / "digests", "digest-*.md")
    next_actions_report = out_dir / "next_actions.md"
    shadow = _latest_shadow_brief(Path(args.vault).expanduser() if getattr(args, "vault", "") else DEFAULT_OBSIDIAN_VAULT)

    items = [_status_item_from_action(action, ledger) for action in actions]
    for row in ledger.hypotheses:
        if row.state == "running" and not any(item["key"] == row.hypothesis_id for item in items):
            items.append({
                "id": f"running:{row.hypothesis_id}",
                "tag": "running",
                "title": f"Running hypothesis: {row.hypothesis_id}",
                "why": f"Hypothesis file state is running; latest_stage={row.latest_stage} decision={row.decision or '<empty>'}.",
                "next": "Resume through the bounded runner or normalize state after inspection.",
                "owner": "Research Ops",
                "source": row.file_path,
                "priority": "high",
                "action_type": "running_hypothesis",
                "key": row.hypothesis_id,
            })
    if shadow:
        verdict = shadow.get("verdict", "")
        tag = "blocked" if "red" in verdict.lower() else "running"
        items.append({
            "id": f"shadow-brief:{_safe_id(Path(shadow['path']).stem)}",
            "tag": tag,
            "title": f"Shadow brief: {shadow.get('title')}",
            "why": f"Latest Mala/Bhiksha shadow brief verdict: {verdict or 'not stated'}.",
            "next": "Continue daily evidence capture; do not change active_strategy without explicit approval.",
            "owner": shadow.get("owner") or "Research Ops",
            "source": shadow.get("path", ""),
            "priority": "medium",
            "action_type": "shadow_brief",
            "key": Path(shadow.get("path", "shadow")).stem,
        })
    for row in sorted([r for r in ledger.hypotheses if r.state in {"completed", "kill"}], key=lambda r: r.hypothesis_id)[:10]:
        items.append({
            "id": f"done:{row.hypothesis_id}",
            "tag": "done",
            "title": f"{row.hypothesis_id} is {row.state}",
            "why": f"decision={row.decision or '<empty>'}; latest_stage={row.latest_stage}.",
            "next": "No operator action unless new evidence reopens it.",
            "owner": "Research Ops",
            "source": row.file_path,
            "priority": "low",
            "action_type": "done_hypothesis",
            "key": row.hypothesis_id,
        })

    by_tag = {tag: [item for item in items if item.get("tag") == tag] for tag in sorted(PROGRAM_STATUS_TAGS)}
    warnings = control_warnings + intake_warnings
    if not latest_digest:
        warnings.append("No digest artifact found under data/results/research_ops/digests/.")
    if not next_actions_report.exists():
        warnings.append("No next_actions.md artifact found yet; program-status rebuilt next actions in memory.")
    return {
        "generated_at": datetime.now(UTC).replace(microsecond=0).isoformat(),
        "program": {"id": "mala_next_gen_research_ops_flow", "title": "Mala Next-Gen Research Ops Flow"},
        "summary": {
            "hypotheses": len(ledger.hypotheses),
            "runs": len(ledger.runs),
            "promoted_candidates": len(ledger.promoted),
            "findings": len(ledger.findings),
            "next_actions": len(actions),
            "control_rows": len(control_rows),
            "intake_rows": len(intake_rows),
            "tags": {tag: len(by_tag[tag]) for tag in sorted(PROGRAM_STATUS_TAGS)},
        },
        "latest": {"digest": str(latest_digest) if latest_digest else "", "next_actions": str(next_actions_report) if next_actions_report.exists() else "", "shadow_brief": shadow},
        "state": {
            "control": {"available": bool(control_rows), "active_rows": [row for row in control_rows if str(row.get("operator_action", "")).strip() or str(row.get("status", "")).strip() not in {"", "queued"}][:10]},
            "intake": {"available": bool(intake_rows), "active_rows": [row for row in intake_rows if str(row.get("operator_action", "")).strip() or str(row.get("status", "")).strip()][:10]},
        },
        "items": items,
        "by_tag": by_tag,
        "artifacts": {"hypotheses_dir": str(Path(args.hypotheses_dir)), "runs_dir": str(Path(args.runs_dir)), "out_dir": str(Path(args.out_dir))},
        "warnings": warnings,
    }


def write_program_status(status: dict[str, Any], out_dir: Path) -> tuple[Path, Path]:
    status_dir = out_dir / "program_status"
    status_dir.mkdir(parents=True, exist_ok=True)
    json_path = status_dir / "program_status.json"
    md_path = status_dir / "program_status.md"
    json_path.write_text(json.dumps(status, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    lines = ["# Mala Program Status", "", f"- generated_at: `{status['generated_at']}`", f"- program_id: `{status['program']['id']}`", "", "## Summary", ""]
    for key, value in status["summary"].items():
        lines.append(f"- {key}: `{value}`")
    if status.get("warnings"):
        lines.extend(["", "## Warnings / Blockers", ""])
        for warning in status["warnings"]:
            lines.append(f"- {warning}")
    latest = status.get("latest", {})
    lines.extend(["", "## Latest Sources", ""])
    for key in ("digest", "next_actions"):
        lines.append(f"- {key}: `{latest.get(key) or 'not found'}`")
    shadow = latest.get("shadow_brief") or {}
    if shadow:
        lines.append(f"- shadow_brief: `{shadow.get('path', '')}` — {shadow.get('verdict', '')}")
    for tag in ("needs_suman", "blocked", "running", "auto_continue", "done"):
        lines.extend(["", f"## {tag}", ""])
        bucket = status.get("by_tag", {}).get(tag, [])
        if not bucket:
            lines.append("- None.")
            continue
        for item in bucket[:20]:
            lines.append(f"- `{item['id']}` **{item['title']}**")
            lines.append(f"  - why: {item.get('why', '')}")
            lines.append(f"  - next: {item.get('next', '')}")
            lines.append(f"  - owner: {item.get('owner', '')}")
            lines.append(f"  - source: `{item.get('source', '')}`")
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return json_path, md_path


def cmd_program_status(args: argparse.Namespace) -> int:
    status = build_program_status(args)
    json_path, md_path = write_program_status(status, Path(args.out_dir))
    print(f"PROGRAM_STATUS_JSON={json_path}")
    print(f"PROGRAM_STATUS_MD={md_path}")
    print(f"NEEDS_SUMAN={status['summary']['tags'].get('needs_suman', 0)}")
    print(f"BLOCKED={status['summary']['tags'].get('blocked', 0)}")
    print(f"RUNNING={status['summary']['tags'].get('running', 0)}")
    return 0


def _load_or_build_program_status(args: argparse.Namespace) -> dict[str, Any]:
    status_path = Path(args.out_dir) / "program_status" / "program_status.json"
    if status_path.exists() and not getattr(args, "refresh", False):
        try:
            return json.loads(status_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            pass
    status = build_program_status(args)
    write_program_status(status, Path(args.out_dir))
    return status


def _extract_preserved_block(text: str, start: str, end: str, default_body: str) -> str:
    if start in text and end in text:
        return text.split(start, 1)[1].split(end, 1)[0].strip("\n")
    return default_body.strip("\n")


def _section_text(markdown: str, heading: str, *, max_chars: int = 320) -> str:
    pattern = rf"^##\s+{re.escape(heading)}\s*$\n(?P<body>.*?)(?=^##\s+|\Z)"
    match = re.search(pattern, markdown, re.MULTILINE | re.DOTALL)
    if not match:
        return ""
    body = re.sub(r"\s+", " ", match.group("body")).strip(" -\t\n")
    return body[: max_chars - 3] + "..." if len(body) > max_chars else body


def _hypothesis_story(path_value: str) -> dict[str, str]:
    path = _artifact_path(path_value)
    if path is None or not path.exists():
        return {"title": Path(path_value or "").stem, "thesis": ""}
    text = _read_text(path, max_chars=6000)
    title_match = re.search(r"^#\s+Hypothesis:\s*(.+)$", text, re.MULTILINE) or re.search(r"^#\s+(.+)$", text, re.MULTILINE)
    return {
        "title": title_match.group(1).strip() if title_match else path.stem,
        "thesis": _section_text(text, "Thesis"),
    }


def _summary_note(summary_text: str) -> str:
    notes = _section_text(summary_text, "Notes", max_chars=220)
    if notes:
        return notes
    match = re.search(r"^-\s*decision:\s*`?([^`\n]+)`?", summary_text, re.MULTILINE)
    return f"decision={match.group(1).strip()}" if match else ""


def _m1_metric_summary(m1_rows: list[dict[str, str]]) -> str:
    if not m1_rows:
        return "M1 evidence not found"
    best_rows = _sorted_metric_rows(m1_rows, primary="m1_score", limit=1)
    best = best_rows[0] if best_rows else m1_rows[0]
    return (
        f"M1 rows={len(m1_rows)}; best {best.get('ticker', '')} {best.get('direction', '')} "
        f"exp_r={_format_number(best.get('avg_test_exp_r'))} "
        f"pct_pos={_format_number(best.get('pct_positive_oos_windows'), digits=2)} "
        f"signals={best.get('oos_signals', '')}"
    ).strip()


def _m2_metric_summary(m2_rows: list[dict[str, str]]) -> str:
    if not m2_rows:
        return ""
    pass_all = sum(1 for row in m2_rows if _truthy(row.get("passes_all_gates")))
    best_rows = _sorted_metric_rows(m2_rows, primary="score", secondary="min_avg_test_exp_r", limit=1)
    best = best_rows[0] if best_rows else m2_rows[0]
    return (
        f"M2 rows={len(m2_rows)} pass_all={pass_all}; best min_exp_r="
        f"{_format_number(best.get('min_avg_test_exp_r'))} "
        f"min_pct_pos={_format_number(best.get('min_pct_positive_oos_windows'), digits=2)}"
    ).strip()



def _metric_row_fingerprint(row: dict[str, str]) -> tuple[str, str, str]:
    return (str(row.get("ticker", "")), str(row.get("strategy", "")), str(row.get("direction", "")))


def _best_m1_row(m1_rows: list[dict[str, str]]) -> dict[str, str]:
    if not m1_rows:
        return {}
    positive = [row for row in m1_rows if _to_float(row.get("avg_test_exp_r")) > 0]
    rows = positive or m1_rows
    return _sorted_metric_rows(rows, primary="m1_score", secondary="avg_test_exp_r", limit=1)[0]


def _matching_effective_cost(best: dict[str, str], detail_rows: list[dict[str, str]]) -> str:
    if not best:
        return ""
    ignored = {"oos_windows", "oos_signals", "avg_test_exp_r", "pct_positive_oos_windows", "avg_test_confidence", "avg_test_mfe_mae_ratio", "m1_score"}
    keys = [key for key, value in best.items() if key not in ignored and str(value).strip()]
    matches = []
    for row in detail_rows:
        if all(str(row.get(key, "")).strip() == str(best.get(key, "")).strip() for key in keys if key in row):
            matches.append(row)
    costs = [_to_float(row.get("effective_cost_r"), default=float("nan")) for row in matches]
    costs = [value for value in costs if value == value]
    if not costs:
        return ""
    return _format_number(sum(costs) / len(costs), digits=4)


def _best_config_sketch(best: dict[str, str], detail_rows: list[dict[str, str]]) -> dict[str, Any]:
    if not best:
        return {}
    sketch = {
        "ticker": best.get("ticker", ""),
        "direction": best.get("direction", ""),
        "avg_test_exp_r": _format_number(best.get("avg_test_exp_r")),
        "pct_positive_oos_windows": _format_number(best.get("pct_positive_oos_windows"), digits=2),
        "oos_signals": best.get("oos_signals", ""),
        "avg_test_confidence": _format_number(best.get("avg_test_confidence"), digits=4),
        "avg_test_mfe_mae_ratio": _format_number(best.get("avg_test_mfe_mae_ratio"), digits=4),
        "effective_cost_r_if_available": _matching_effective_cost(best, detail_rows),
        "key_params": _compact_config(best),
    }
    return {key: value for key, value in sketch.items() if str(value).strip()}


def _direction_stability_flag(rows: list[dict[str, str]]) -> bool:
    direction_exp: dict[str, list[float]] = {}
    for row in rows:
        direction = str(row.get("direction", "")).strip() or "unknown"
        direction_exp.setdefault(direction, []).append(_to_float(row.get("avg_test_exp_r")))
    positives = {direction for direction, values in direction_exp.items() if any(value > 0 for value in values)}
    negatives = {direction for direction, values in direction_exp.items() if any(value < 0 for value in values)}
    directional = {d for d in positives | negatives if d not in {"combined", "unknown"}}
    return bool(directional and positives and negatives and len(direction_exp) > 1)


def _stability_read(m1_rows: list[dict[str, str]], aggregate_rows: list[dict[str, str]], best: dict[str, str]) -> dict[str, Any]:
    all_rows = aggregate_rows or m1_rows
    positive_count = sum(1 for row in all_rows if _to_float(row.get("avg_test_exp_r")) > 0)
    robust_rows = [
        row for row in all_rows
        if _to_float(row.get("avg_test_exp_r")) > 0
        and _to_float(row.get("oos_windows")) >= 3
        and _to_float(row.get("oos_signals")) >= 50
    ]
    broader_rows = [
        row for row in all_rows
        if _to_float(row.get("oos_windows")) >= max(2, _to_float(best.get("oos_windows"), 0))
        and _to_float(row.get("oos_signals")) >= max(30, _to_float(best.get("oos_signals"), 0))
    ]
    broader_negative = any(_to_float(row.get("avg_test_exp_r")) < 0 for row in broader_rows)
    notes: list[str] = []
    if robust_rows:
        sample = _sorted_metric_rows(robust_rows, primary="avg_test_exp_r", limit=2)
        notes.append("robust-ish positives: " + ", ".join(f"{r.get('ticker','')} {r.get('direction','')} exp_r={_format_number(r.get('avg_test_exp_r'))} signals={r.get('oos_signals','')}" for r in sample))
    else:
        notes.append("no positive configs clear both 3-window and 50-signal rough stability bars")
    if broader_negative:
        notes.append("broader-sample rows include negative expectancy")
    if _direction_stability_flag(all_rows):
        notes.append("direction rows disagree across the sample")
    return {
        "positive_config_count": positive_count,
        "robust_config_count": len(robust_rows),
        "broader_sample_disagreement": broader_negative,
        "notes": "; ".join(notes[:3]),
    }


def _metadata_text(metadata: dict[str, Any] | None, run_summary_text: str = "") -> str:
    metadata = metadata or {}
    allowed_keys = {
        "title",
        "strategy",
        "symbol_scope",
        "thesis",
        "purpose",
        "tags",
        "notes",
        "run_purpose",
        "description",
    }
    parts = [str(value) for key, value in metadata.items() if key in allowed_keys for value in ([value] if not isinstance(value, list) else value)]
    if run_summary_text:
        parts.append(run_summary_text)
    return "\n".join(parts).lower()


def _is_smoke_test_metadata(metadata: dict[str, Any] | None, run_summary_text: str = "") -> bool:
    text = _metadata_text(metadata, run_summary_text)
    return bool(re.search(r"\b(smoke|plumbing|pipeline\s+test|dry[- ]run)\b", text))


def _fragility_flags(*, metadata: dict[str, Any] | None, best: dict[str, str], stability: dict[str, Any], rows: list[dict[str, str]], run_summary_text: str = "") -> list[str]:
    flags: list[str] = []
    if not best:
        flags.append("missing_artifact")
    if _is_smoke_test_metadata(metadata, run_summary_text):
        flags.append("smoke_test")
    if best and _to_float(best.get("oos_signals")) < 50:
        flags.append("thin_signals")
    if best and _to_float(best.get("oos_windows")) <= 1:
        flags.append("one_oos_window")
    if bool(stability.get("broader_sample_disagreement")):
        flags.append("aggregate_disagrees")
    if best and _to_float(best.get("avg_test_exp_r")) < 0.10:
        flags.append("weak_expectancy")
    if _direction_stability_flag(rows):
        flags.append("direction_unstable")
    return list(dict.fromkeys(flags))


def _evidence_quality(*, flags: list[str], robust_count: int, positive_count: int, exp_r: float) -> str:
    if "smoke_test" in flags:
        return "smoke_test"
    if "missing_artifact" in flags:
        return "blocked"
    if exp_r <= 0:
        return "weak"
    if robust_count >= 2 and positive_count >= 3 and "aggregate_disagrees" not in flags:
        return "strong"
    if robust_count >= 1 and positive_count >= 2:
        return "medium"
    return "weak"


def _researcher_recommendation(*, best: dict[str, str], stability: dict[str, Any], flags: list[str]) -> tuple[str, str, str, str]:
    exp_r = _to_float(best.get("avg_test_exp_r"), default=0.0) if best else 0.0
    pct_positive = _to_float(best.get("pct_positive_oos_windows"), default=0.0) if best else 0.0
    robust_count = int(stability.get("robust_config_count") or 0)
    positive_count = int(stability.get("positive_config_count") or 0)
    quality = _evidence_quality(flags=flags, robust_count=robust_count, positive_count=positive_count, exp_r=exp_r)

    if "smoke_test" in flags:
        return "close_as_smoke_test", "low", "smoke_test", "Artifacts read like a smoke/plumbing test; close as pipeline evidence unless Suman intentionally converts it into a real hypothesis."
    if "missing_artifact" in flags:
        return "defer_for_better_evidence", "low", "blocked", "Required M1 evidence artifacts are missing, so Research Ops cannot justify another compute cycle."
    if exp_r <= 0 or positive_count == 0:
        return "reject_or_kill", "low", quality, "No positive after-cost expectancy is visible in the available evidence."
    if robust_count >= 2 and positive_count >= 3 and "aggregate_disagrees" not in flags:
        return "approve_surface_expansion", "high", quality, "Multiple positive configs clear rough window/signal stability bars; approve targeted surface expansion, not alpha promotion."
    if robust_count >= 1 and positive_count >= 3:
        return "approve_surface_expansion", "medium", quality, "At least one robust-ish positive config exists with broader positive support; expand the surface cautiously."
    if "weak_expectancy" in flags and positive_count <= 1:
        return "defer_for_better_evidence", "low", quality, "Positive evidence is too weak and sparse to justify a retune right now."
    if "aggregate_disagrees" in flags and robust_count == 0:
        return "defer_for_better_evidence", "low", quality, "The best row is not backed by broader-sample evidence; defer until the thesis or surface is clearer."
    if pct_positive <= 0.50 and robust_count == 0:
        return "defer_for_better_evidence", "low", "weak", "The best positive row is not stable across OOS windows; clarify the failure mode before retuning."
    if "one_oos_window" in flags or "thin_signals" in flags:
        return "approve_bounded_retune", "medium", quality, "Positive evidence exists but is sample-fragile; only a bounded diagnostic retune is justified."
    return "defer_for_better_evidence", "low", quality, "Evidence is mixed or under-supported, so defer rather than spending a broad research cycle."


def score_researcher_verdict(
    *,
    hypothesis_metadata: dict[str, Any] | None = None,
    m1_top_rows: list[dict[str, str]] | None = None,
    m1_aggregate_rows: list[dict[str, str]] | None = None,
    m1_detail_rows: list[dict[str, str]] | None = None,
    m2_rows: list[dict[str, str]] | None = None,
    run_summary_text: str = "",
) -> dict[str, Any]:
    del m2_rows  # Reserved for later-stage gates; M1 evidence drives the current retune verdict.
    m1_rows = m1_top_rows or []
    aggregate_rows = m1_aggregate_rows or []
    detail_rows = m1_detail_rows or []
    best = _best_m1_row(m1_rows)
    stability = _stability_read(m1_rows, aggregate_rows, best)
    flags = _fragility_flags(
        metadata=hypothesis_metadata,
        best=best,
        stability=stability,
        rows=(aggregate_rows or m1_rows),
        run_summary_text=run_summary_text,
    )
    recommendation, priority, evidence_quality, rationale = _researcher_recommendation(
        best=best,
        stability=stability,
        flags=flags,
    )
    return {
        "recommendation": recommendation,
        "priority": priority,
        "rationale": rationale,
        "evidence_quality": evidence_quality,
        "fragility_flags": flags,
        "best_config": _best_config_sketch(best, detail_rows),
        "stability_read": stability,
    }


def _build_researcher_verdict(*, hypothesis_metadata: dict[str, Any] | None, m1_rows: list[dict[str, str]], aggregate_rows: list[dict[str, str]], detail_rows: list[dict[str, str]], m2_rows: list[dict[str, str]], run_summary_text: str) -> dict[str, Any]:
    return score_researcher_verdict(
        hypothesis_metadata=hypothesis_metadata,
        m1_top_rows=m1_rows,
        m1_aggregate_rows=aggregate_rows,
        m1_detail_rows=detail_rows,
        m2_rows=m2_rows,
        run_summary_text=run_summary_text,
    )

def _verdict_line(verdict: dict[str, Any]) -> str:
    flags = verdict.get("fragility_flags") or []
    flag_text = ", ".join(flags[:5]) if flags else "none"
    best = verdict.get("best_config") or {}
    best_text = ""
    if best:
        best_text = (
            f" best={best.get('ticker','')} {best.get('direction','')} exp_r={best.get('avg_test_exp_r','')} "
            f"pct_pos={best.get('pct_positive_oos_windows','')} signals={best.get('oos_signals','')}"
        ).strip()
    return (
        f"{verdict.get('recommendation', 'defer_for_better_evidence')} / {verdict.get('priority', 'low')} "
        f"/ evidence={verdict.get('evidence_quality', 'weak')} — {verdict.get('rationale', '')} "
        f"Flags: {flag_text}.{(' ' + best_text) if best_text else ''}"
    ).strip()


def _batch_recommended_path(entries: list[dict[str, Any]]) -> list[str]:
    by_rec: dict[str, list[str]] = {}
    for entry in entries:
        brief = entry.get("brief") if isinstance(entry.get("brief"), dict) else {}
        verdict = brief.get("researcher_verdict") if isinstance(brief.get("researcher_verdict"), dict) else {}
        rec = str(verdict.get("recommendation") or "defer_for_better_evidence")
        name = str(brief.get("title") or brief.get("hypothesis_id") or entry.get("key") or entry.get("id"))
        by_rec.setdefault(rec, []).append(name)
    lines = ["Batch recommended path:" if len(entries) > 1 else "Recommended path:"]
    if by_rec.get("approve_surface_expansion"):
        lines.append("- Prioritize targeted surface expansion: " + "; ".join(by_rec["approve_surface_expansion"]))
    if by_rec.get("approve_bounded_retune"):
        lines.append("- Allow bounded diagnostic retune only: " + "; ".join(by_rec["approve_bounded_retune"]))
    parked = by_rec.get("defer_for_better_evidence", []) + by_rec.get("reject_or_kill", [])
    if parked:
        lines.append("- Defer/reject unless strategic diversification matters: " + "; ".join(parked))
    if by_rec.get("close_as_smoke_test"):
        lines.append("- Close as smoke test or intentionally convert to a real hypothesis: " + "; ".join(by_rec["close_as_smoke_test"]))
    return lines


def _entry_brief(entry: dict[str, Any]) -> dict[str, Any]:
    brief = entry.get("brief") if isinstance(entry.get("brief"), dict) else {}
    return brief if isinstance(brief, dict) else {}


def _entry_verdict(entry: dict[str, Any]) -> dict[str, Any]:
    verdict = _entry_brief(entry).get("researcher_verdict")
    return verdict if isinstance(verdict, dict) else {}


def _entry_title(entry: dict[str, Any]) -> str:
    brief = _entry_brief(entry)
    return str(brief.get("title") or entry.get("title") or entry.get("key") or entry.get("id") or "research item")


def _entry_recommendation(entry: dict[str, Any]) -> str:
    brief = _entry_brief(entry)
    verdict = _entry_verdict(entry)
    return str(verdict.get("recommendation") or brief.get("suggested_operator_action") or brief.get("recommendation") or "defer_for_better_evidence")


def _plain_recommendation(recommendation: str) -> str:
    return {
        "approve_surface_expansion": "approve targeted surface expansion",
        "approve_bounded_retune": "approve bounded diagnostic retune",
        "defer_for_better_evidence": "defer",
        "reject_or_kill": "reject/kill",
        "close_as_smoke_test": "close as smoke test",
    }.get(recommendation, recommendation.replace("_", " "))


def _executive_recommendation(entries: list[dict[str, Any]]) -> str:
    by_rec: dict[str, list[str]] = {}
    for entry in entries:
        by_rec.setdefault(_entry_recommendation(entry), []).append(_entry_title(entry))
    if len(entries) == 1:
        rec, names = next(iter(by_rec.items()))
        return f"Recommendation: {_plain_recommendation(rec).capitalize()} for {names[0]}."

    labels = {
        "approve_surface_expansion": "approve targeted expansion for",
        "approve_bounded_retune": "approve bounded retune for",
        "defer_for_better_evidence": "defer",
        "reject_or_kill": "reject/kill",
        "close_as_smoke_test": "close as smoke test",
    }
    ordered = list(labels)
    parts: list[str] = []
    for rec in ordered:
        names = by_rec.get(rec, [])
        if not names:
            continue
        noun = "candidate" if len(names) == 1 else "candidates"
        parts.append(f"{labels[rec]} {len(names)} {noun}")
    return "Recommendation: " + "; ".join(parts) + "."


def _executive_confidence(entries: list[dict[str, Any]]) -> str:
    qualities = {str(_entry_verdict(entry).get("evidence_quality") or "").lower() for entry in entries}
    if "strong" in qualities and len(qualities - {"strong", "medium"}) == 0:
        return "High"
    if "medium" in qualities or "strong" in qualities:
        return "Medium"
    if "smoke_test" in qualities and len(qualities) == 1:
        return "Low - smoke-test evidence only"
    return "Low"


def _executive_decision_needed(item: dict[str, Any], entries: list[dict[str, Any]]) -> str:
    if item.get("action_type") == "retune_plan_batch":
        return "Decision needed: accept the defer/close recommendation, or name an override subset in Comments."
    return f"Decision needed: {_decision_next_text(item)}"


def _executive_data_line(entry: dict[str, Any]) -> str:
    verdict = _entry_verdict(entry)
    stability = verdict.get("stability_read") if isinstance(verdict.get("stability_read"), dict) else {}
    best = verdict.get("best_config") if isinstance(verdict.get("best_config"), dict) else {}
    flags = verdict.get("fragility_flags") if isinstance(verdict.get("fragility_flags"), list) else []
    best_bits: list[str] = []
    if best:
        if best.get("ticker") or best.get("direction"):
            best_bits.append(f"best={best.get('ticker', '')} {best.get('direction', '')}".strip())
        if best.get("avg_test_exp_r"):
            best_bits.append(f"exp_r={best.get('avg_test_exp_r')}")
        if best.get("oos_signals"):
            best_bits.append(f"signals={best.get('oos_signals')}")
    stability_text = ""
    if stability:
        stability_text = (
            f"robustish={stability.get('robust_config_count', 0)} / "
            f"{stability.get('positive_config_count', 0)} positive configs"
        )
    caveats: list[str] = []
    if stability:
        if stability.get("broader_sample_disagreement"):
            caveats.append("broader sample disagrees")
    caveats.extend(str(flag) for flag in flags[:3] if str(flag).strip())
    evidence_parts = ["; ".join(best_bits) if best_bits else "", stability_text]
    evidence = "; ".join(part for part in evidence_parts if part)
    caveat_text = f"; caveats={', '.join(caveats)}" if caveats else ""
    return f"- **{_entry_title(entry)}**: `{_entry_recommendation(entry)}`; {evidence or 'evidence unavailable'}{caveat_text}."


def _render_executive_summary(item: dict[str, Any], entries: list[dict[str, Any]]) -> list[str]:
    return [
        "## Executive Summary",
        "",
        f"- {_executive_recommendation(entries)}",
        f"- {_executive_decision_needed(item, entries)}",
        f"- Confidence: {_executive_confidence(entries)}.",
        "- Permission boundary: any approval authorizes only the named bounded research action; it does **not** mutate Google Sheets, Strategy_Catalog, active_strategy, broker state, or live risk.",
        "",
        "## Decision Data That Matters",
        "",
        *[_executive_data_line(entry) for entry in entries],
        "",
    ]


def _research_brief_for_action(action: NextAction, ledger: ResearchLedger) -> dict[str, Any]:
    hyp = _find_hypothesis(ledger, action.key)
    latest_run = _latest_run(ledger, action.key)
    latest_dir = _artifact_path(hyp.latest_artifact_dir) if hyp else (_artifact_path(latest_run.artifact_dir) if latest_run else None)
    summary_path = latest_dir / "RUN_SUMMARY.md" if latest_dir else None
    summary_text = _read_text(summary_path, max_chars=5000) if summary_path else ""
    m1_rows = _read_csv_dicts(latest_dir / "M1_top.csv") if latest_dir else []
    m1_aggregate_rows = _read_csv_dicts(latest_dir / "M1_aggregate.csv") if latest_dir else []
    m1_detail_rows = _read_csv_dicts(latest_dir / "M1_detail.csv") if latest_dir else []
    m2_rows = _read_csv_dicts(latest_dir / "M2_gate_report.csv") if latest_dir else []
    story = _hypothesis_story(hyp.file_path if hyp else "")
    hypothesis_metadata = {
        "title": story.get("title", ""),
        "thesis": story.get("thesis", ""),
        "strategy": hyp.strategy if hyp else "",
        "symbol_scope": hyp.symbol_scope if hyp else "",
    }
    researcher_verdict = _build_researcher_verdict(
        hypothesis_metadata=hypothesis_metadata,
        m1_rows=m1_rows,
        aggregate_rows=m1_aggregate_rows,
        detail_rows=m1_detail_rows,
        m2_rows=m2_rows,
        run_summary_text=summary_text,
    )
    recommendation, operator_action, recommendation_reason = _brief_recommendation(
        hypothesis=hyp,
        action=action,
        m1_rows=m1_rows,
        m2_rows=m2_rows,
        summary_text=summary_text,
        researcher_verdict=researcher_verdict,
    )
    metrics = [value for value in (_m2_metric_summary(m2_rows), _m1_metric_summary(m1_rows), _summary_note(summary_text)) if value]
    sources: list[str] = []
    if hyp is not None:
        sources.append(hyp.file_path)
    if latest_run is not None:
        sources.append(latest_run.artifact_dir)
    if summary_path and summary_path.exists():
        sources.append(_relative(summary_path))
    confidence = "medium" if (m1_rows or m2_rows) else "low"
    if recommendation.startswith(("INSPECT", "KILL_OR_SURFACE_RETHINK")):
        confidence = "low" if not (m1_rows or m2_rows) else "medium-low"
    return {
        "title": story.get("title") or (hyp.hypothesis_id if hyp else action.key),
        "hypothesis_id": hyp.hypothesis_id if hyp else action.key,
        "strategy": hyp.strategy if hyp else "",
        "symbol_scope": hyp.symbol_scope if hyp else "",
        "stage": hyp.latest_stage if hyp else (latest_run.terminal_stage if latest_run else "none"),
        "decision": hyp.decision if hyp else (latest_run.decision if latest_run else ""),
        "thesis": story.get("thesis", ""),
        "latest_artifact": latest_run.artifact_dir if latest_run else (hyp.latest_artifact_dir if hyp else ""),
        "metrics": metrics[:3],
        "recommendation": recommendation,
        "suggested_operator_action": operator_action,
        "recommendation_reason": recommendation_reason,
        "confidence": confidence,
        "researcher_verdict": researcher_verdict,
        "sources": sources[:4],
    }


def _brief_source_text(sources: list[str]) -> str:
    if not sources:
        return "none found"
    return "; ".join(f"`{source}`" for source in sources[:3])


def _render_research_ops_brief(item: dict[str, Any]) -> list[str]:
    if item.get("action_type") not in {"retune_plan", "retune_plan_batch"}:
        return []
    if item.get("action_type") == "retune_plan_batch":
        entries = [child for child in item.get("batched_item_details", []) if isinstance(child, dict)]
    else:
        entries = [item]
    if not entries:
        return []

    if item.get("action_type") == "retune_plan_batch":
        plain_english = "Plain English: this is an attention-managed retune summary. The items are grouped because they are lower-priority or similar, not because Research Ops should ignore per-item differences."
        approval_text = "Approving a subset must be named in comments. Approval authorizes only the recommended bounded action for the named items; it does **not** mutate Google Sheets, Strategy_Catalog, active_strategy, broker state, or live risk by itself."
    else:
        plain_english = "Plain English: this is an individual retune decision card because this candidate has a differentiated Research Ops verdict or enough unblock value to deserve standalone judgment."
        approval_text = "Approving this card authorizes only the named bounded research action. It does **not** mutate Google Sheets, Strategy_Catalog, active_strategy, broker state, or live risk by itself."

    lines = _render_executive_summary(item, entries)
    lines.extend([
        "## Research Detail",
        "",
        plain_english,
        "",
        approval_text,
        "",
        "### Evidence Snapshot",
        "",
    ])
    for idx, entry in enumerate(entries, start=1):
        brief = entry.get("brief") if isinstance(entry.get("brief"), dict) else {}
        title = brief.get("title") or entry.get("title") or entry.get("key") or entry.get("id")
        hyp_id = brief.get("hypothesis_id") or entry.get("key") or entry.get("id")
        strategy = brief.get("strategy") or "unknown strategy"
        symbols = brief.get("symbol_scope") or "unknown symbols"
        stage = brief.get("stage") or "none"
        decision = brief.get("decision") or "unknown"
        thesis = brief.get("thesis") or "Thesis was not extractable from the hypothesis file."
        metrics = brief.get("metrics") or []
        metrics_text = "; ".join(str(metric) for metric in metrics[:2]) if metrics else "metrics not found"
        verdict = brief.get("researcher_verdict") if isinstance(brief.get("researcher_verdict"), dict) else {}
        recommendation = verdict.get("recommendation") or brief.get("suggested_operator_action") or brief.get("recommendation") or "defer_for_better_evidence"
        reason = verdict.get("rationale") or brief.get("recommendation_reason") or entry.get("why") or "No recommendation reason found."
        artifact = brief.get("latest_artifact") or entry.get("source") or "not found"
        stability = verdict.get("stability_read") if isinstance(verdict.get("stability_read"), dict) else {}
        best = verdict.get("best_config") if isinstance(verdict.get("best_config"), dict) else {}
        best_params = f"; params=({best.get('key_params')})" if best.get("key_params") else ""
        stability_text = (
            f"positive_configs={stability.get('positive_config_count', '')}; "
            f"robustish={stability.get('robust_config_count', '')}; "
            f"broader_disagrees={stability.get('broader_sample_disagreement', '')}"
        )
        lines.extend([
            f"{idx}. **{title}** (`{hyp_id}`) — {strategy}; symbols: {symbols}",
            f"   - Thesis: {thesis}",
            f"   - State/evidence: stage={stage}, decision={decision}; {metrics_text}",
            f"   - Researcher Verdict: `{recommendation}` — {_verdict_line(verdict) if verdict else reason}",
            f"   - Stability read: {stability_text}; {stability.get('notes', '')}",
            f"   - Best config sketch: {best.get('ticker', '')} {best.get('direction', '')} exp_r={best.get('avg_test_exp_r', '')} pct_pos={best.get('pct_positive_oos_windows', '')} signals={best.get('oos_signals', '')} confidence={best.get('avg_test_confidence', '')} MFE/MAE={best.get('avg_test_mfe_mae_ratio', '')}{best_params}",
            f"   - Latest artifact: `{artifact}`",
        ])
    recommendation_counts = Counter(
        str(((entry.get("brief") or {}).get("researcher_verdict") or {}).get("recommendation") or (entry.get("brief") or {}).get("suggested_operator_action") or (entry.get("brief") or {}).get("recommendation") or "defer")
        for entry in entries
    )
    lines.extend([
        "",
        "## Recommendation",
        "",
    ])
    if recommendation_counts:
        summary = ", ".join(f"{count}× {name}" for name, count in recommendation_counts.most_common())
        lines.append(f"Research Ops recommendation mix: {summary}.")
    lines.extend(_batch_recommended_path(entries))
    low_conf = [entry for entry in entries if "low" in str((entry.get("brief") or {}).get("confidence", "")).lower()]
    if low_conf:
        lines.append("Confidence is mixed; if any item feels stale or off-thesis, defer that subset with comments rather than approving the whole batch.")
    else:
        lines.append("Confidence is medium because local M1/M2 summaries are present, but retune approval is still a judgment call about spending more research cycles.")
    lines.extend([
        "",
        "Choices and consequences:",
        "- **approve**: approve the card as written; for summary cards, name any approved subset in Comments.",
        "- **reject**: stop spending cycles on this item/group unless a new thesis is written.",
        "- **defer**: leave the item/group parked for more evidence or a cleaner recommendation.",
        "- **approve subset with comments**: for grouped summaries only, write exceptions in Comments; only named approved items should move next.",
        "",
        "## Appendix: Source Artifacts",
        "",
    ])
    seen: set[str] = set()
    for entry in entries:
        brief = entry.get("brief") if isinstance(entry.get("brief"), dict) else {}
        hyp_id = brief.get("hypothesis_id") or entry.get("key") or entry.get("id")
        sources = [str(source) for source in brief.get("sources", []) if str(source).strip()]
        if not sources and entry.get("source"):
            sources = [str(entry.get("source"))]
        for source in sources[:3]:
            key = f"{hyp_id}:{source}"
            if key in seen:
                continue
            seen.add(key)
            lines.append(f"- `{hyp_id}`: `{source}`")
    if not seen:
        lines.append("- No drill-down artifacts found in the local status read model.")
    lines.append("")
    return lines



def _local_operator_timestamp(value: datetime | None = None) -> str:
    """Return the local Central timestamp shown on review-facing artifacts."""
    stamp = value or datetime.now(UTC)
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=UTC)
    return stamp.astimezone(SHEET_TIMEZONE).strftime("%Y-%m-%d %H:%M %Z (local)")


def _existing_card_generated_at(existing: str) -> str:
    """Preserve a card's original generated timestamp across idempotent rewrites."""
    patterns = (
        r"^generated_at:\s*[\"']?([^\"'`\n]+)[\"']?\s*$",
        r"^-\s*generated_at:\s*`?([^`\n]+)`?\s*$",
    )
    for pattern in patterns:
        match = re.search(pattern, existing, re.MULTILINE)
        if match:
            return match.group(1).strip()
    return ""


def _decision_next_text(item: dict[str, Any]) -> str:
    brief = item.get("brief") if isinstance(item.get("brief"), dict) else {}
    verdict = brief.get("researcher_verdict") if isinstance(brief.get("researcher_verdict"), dict) else {}
    title = str(brief.get("title") or item.get("title") or item.get("key") or "this research item")
    recommendation = str(verdict.get("recommendation") or "")
    if recommendation == "approve_surface_expansion":
        return f"Approve targeted surface expansion for {title}."
    if recommendation == "approve_bounded_retune":
        return f"Approve bounded diagnostic retune for {title}."
    if recommendation == "defer_for_better_evidence":
        return f"Defer {title} until Research Ops has better evidence or a clearer thesis."
    if recommendation == "reject_or_kill":
        return f"Reject or kill {title} unless a materially new thesis is written."
    if recommendation == "close_as_smoke_test":
        return f"Close {title} as smoke-test/plumbing evidence unless intentionally converted into a real hypothesis."
    next_text = str(item.get("next", "")).strip()
    if next_text.startswith("python ") or next_text.startswith("python3 ") or " -m " in next_text:
        return "Make the research decision described below; implementation command is source detail, not the approval request."
    return next_text


def _decision_card_display_path(card_path: Path | None) -> str:
    """Return a concise Obsidian-relative card path for human-visible metadata."""
    if card_path is None:
        return ""
    parts = card_path.parts
    marker = DECISION_CARD_DIR.parts
    for index in range(0, len(parts) - len(marker) + 1):
        if parts[index : index + len(marker)] == marker:
            return str(Path(*parts[index:]))
    return card_path.name


def render_decision_card(item: dict[str, Any], existing: str = "", card_path: Path | None = None) -> str:
    comments = _extract_preserved_block(existing, COMMENTS_START, COMMENTS_END, "-")
    receipt = _extract_preserved_block(existing, RECEIPT_START, RECEIPT_END, "- pending")
    card_id = _safe_id(str(item.get("id", item.get("key", "item"))))
    generated_at = _existing_card_generated_at(existing) or _local_operator_timestamp()
    canonical_card_path = str(card_path.resolve()) if card_path is not None else ""
    display_card_path = _decision_card_display_path(card_path)
    lines = [
        "---",
        f"card_id: {card_id}",
        "program_id: mala_next_gen_research_ops_flow",
        f"status_tag: {item.get('tag', 'needs_suman')}",
        f"owner: {item.get('owner', 'Suman')}",
        "generated_by: research_ops publish-review-cards",
        f"generated_at: {generated_at!r}",
        f"canonical_card_path: {canonical_card_path!r}",
        "edit_this_file: yes",
        "---",
        "",
        f"# Mala Decision Card: {item.get('title', card_id)}", "",
        f"- generated_at: `{generated_at}`",
        "- edit_this_file: yes — Review decisions are read from this card.",
        f"- card: `{display_card_path}`",
        f"- stable_id: `{card_id}`", f"- why: {item.get('why', '')}", f"- next: {_decision_next_text(item)}", f"- owner: {item.get('owner', 'Suman')}", f"- source: `{item.get('source', '')}`",
        "- why_not_auto_continuing: This item requires human judgment, approval, external mutation, or risk-sensitive confirmation before Research Ops can proceed.", "",
    ]
    lines.extend(_render_research_ops_brief(item))
    lines.extend([
        "## Decision", "- [ ] approve", "- [ ] reject", "- [ ] defer", "", "## Comments", COMMENTS_START, comments, COMMENTS_END, "", "## Receipt", RECEIPT_START, receipt, RECEIPT_END, "",
    ])
    return "\n".join(lines)



def _retune_verdict(item: dict[str, Any]) -> dict[str, Any]:
    brief = item.get("brief") if isinstance(item.get("brief"), dict) else {}
    verdict = brief.get("researcher_verdict") if isinstance(brief.get("researcher_verdict"), dict) else {}
    return verdict if isinstance(verdict, dict) else {}


def _retune_sort_key(item: dict[str, Any]) -> tuple[int, int, str]:
    priority_rank = {"high": 0, "medium": 1, "low": 2}
    rec_rank = {
        "approve_surface_expansion": 0,
        "approve_bounded_retune": 1,
        "defer_for_better_evidence": 2,
        "close_as_smoke_test": 3,
        "reject_or_kill": 4,
    }
    verdict = _retune_verdict(item)
    priority = str(verdict.get("priority") or item.get("priority") or "medium").lower()
    recommendation = str(verdict.get("recommendation") or "defer_for_better_evidence")
    return (priority_rank.get(priority, 1), rec_rank.get(recommendation, 9), str(item.get("id") or item.get("key") or ""))


def _retune_material_signature(item: dict[str, Any]) -> tuple[str, str]:
    verdict = _retune_verdict(item)
    return (str(verdict.get("recommendation") or ""), str(verdict.get("priority") or ""))


def _retune_needs_individual_card(item: dict[str, Any]) -> bool:
    verdict = _retune_verdict(item)
    recommendation = str(verdict.get("recommendation") or "")
    priority = str(verdict.get("priority") or "").lower()
    return priority in {"high", "medium"} and recommendation in {"approve_surface_expansion", "approve_bounded_retune"}


def _retune_summary_card(items: list[dict[str, Any]], *, summary_kind: str = "defer") -> dict[str, Any]:
    title = f"Retune {summary_kind} summary ({len(items)} candidates)"
    why = "Low-priority retune candidates have weak/defer/smoke-test verdicts; grouped as attention management, not approval batching."
    next_text = "Review only if you want to override the defer/reject/close recommendations; otherwise leave parked."
    return {
        "id": f"retune_plan:{summary_kind}-summary-needs-suman",
        "tag": "needs_suman",
        "title": title,
        "why": why,
        "next": next_text,
        "owner": "Suman",
        "source": "per-candidate artifacts listed in card",
        "action_type": "retune_plan_batch",
        "key": f"{summary_kind}-summary-needs-suman",
        "batched_items": [item.get("id") for item in items],
        "batched_item_details": items,
    }


def _retune_card_candidates(retunes: list[dict[str, Any]], slots: int) -> list[dict[str, Any]]:
    if slots <= 0 or not retunes:
        return []
    signatures = {_retune_material_signature(item) for item in retunes}
    has_verdicts = any(_retune_verdict(item) for item in retunes)
    # Legacy/no-verdict path and genuinely similar decisions can remain batched.
    if len(retunes) > slots and (not has_verdicts or len(signatures) <= 1):
        batch = sorted(retunes, key=_retune_sort_key)
        return [{
            "id": "retune_plan:batch-needs-suman",
            "tag": "needs_suman",
            "title": f"Batch retune review ({len(batch)} candidates)",
            "why": "Similar retune candidates need the same human judgment; batched to avoid flooding Review Inbox while preserving subset comments.",
            "next": "Review the listed retune candidates and approve/reject/defer the batch or comment with exceptions.",
            "owner": "Suman",
            "source": "per-candidate artifacts listed in card",
            "action_type": "retune_plan_batch",
            "key": "batch-needs-suman",
            "batched_items": [item.get("id") for item in batch],
            "batched_item_details": batch,
        }]

    ordered = sorted(retunes, key=_retune_sort_key)
    individual = [item for item in ordered if _retune_needs_individual_card(item)]
    remainder = [item for item in ordered if item not in individual]
    cards: list[dict[str, Any]] = []
    for item in individual[:slots]:
        cards.append(item)
    remaining_slots = slots - len(cards)
    # Preserve nuance: if there is room, group low-quality remainder as a defer/close summary rather than hiding it inside approval batch.
    if remainder and remaining_slots > 0:
        cards.append(_retune_summary_card(remainder, summary_kind="defer"))
    elif not cards:
        cards.extend(ordered[:slots])
    return cards[:slots]

def _decision_card_candidates(status: dict[str, Any], limit: int = 3) -> list[dict[str, Any]]:
    needs = [item for item in status.get("items", []) if item.get("tag") == "needs_suman"]
    retunes = [item for item in needs if item.get("action_type") == "retune_plan"]
    others = [item for item in needs if item.get("action_type") != "retune_plan"]
    candidates: list[dict[str, Any]] = []
    candidates.extend(others[:limit])
    remaining_slots = max(0, limit - len(candidates))
    candidates.extend(_retune_card_candidates(retunes, remaining_slots))
    return candidates[:limit]


def write_decision_cards(status: dict[str, Any], vault: Path, *, dry_run: bool = False, limit: int = 3) -> list[dict[str, str]]:
    candidates = _decision_card_candidates(status, limit=limit)
    card_dir = vault / DECISION_CARD_DIR
    results: list[dict[str, str]] = []
    for item in candidates:
        card_id = _safe_id(str(item.get("id", item.get("key", "item"))))
        path = card_dir / f"{card_id}.md"
        existing = path.read_text(encoding="utf-8") if path.exists() else ""
        rendered = render_decision_card(item, existing, card_path=path)
        action = "unchanged" if existing == rendered else ("would_update" if dry_run and path.exists() else "would_create" if dry_run else "updated" if path.exists() else "created")
        if not dry_run and existing != rendered:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(rendered, encoding="utf-8")
        results.append({"id": card_id, "path": str(path), "action": action})
    return results


def cmd_publish_review_cards(args: argparse.Namespace) -> int:
    vault = Path(args.vault).expanduser() if args.vault else DEFAULT_OBSIDIAN_VAULT
    status = _load_or_build_program_status(args)
    results = write_decision_cards(status, vault, dry_run=args.dry_run, limit=args.limit)
    for result in results:
        print(f"{result['action'].upper()}={result['path']}")
    print(f"DECISION_CARDS={len(results)}")
    print(f"DRY_RUN={'yes' if args.dry_run else 'no'}")
    return 0


def _parse_card_frontmatter(text: str) -> dict[str, str]:
    if not text.startswith("---\n"):
        return {}
    try:
        block = text.split("---\n", 2)[1]
    except IndexError:
        return {}
    data: dict[str, str] = {}
    for line in block.splitlines():
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        data[key.strip()] = value.strip().strip("'\"")
    return data


def _card_checked_decision(text: str) -> tuple[str, list[str]]:
    section = re.search(r"^##\s+Decision\s*$\n(?P<body>.*?)(?=^##\s+|\Z)", text, re.MULTILINE | re.DOTALL)
    if not section:
        return "", ["missing Decision section"]
    checked = re.findall(r"^\s*-\s*\[[xX]\]\s*([A-Za-z_-]+)\s*$", section.group("body"), re.MULTILINE)
    if not checked:
        return "", []
    normalized = [value.strip().lower().replace("-", "_") for value in checked]
    if len(normalized) > 1:
        return "", [f"multiple checked decisions: {', '.join(normalized)}"]
    if normalized[0] not in {"approve", "reject", "defer"}:
        return "", [f"unsupported checked decision: {normalized[0]}"]
    return normalized[0], []


def _card_comments(text: str) -> str:
    return _extract_preserved_block(text, COMMENTS_START, COMMENTS_END, "").strip()


def _card_action_id(text: str, frontmatter: dict[str, str]) -> str:
    title = re.search(r"^#\s+Mala Decision Card:\s*(.+?)\s*$", text, re.MULTILINE)
    if title:
        value = title.group(1).strip()
        if ":" in value and not value.lower().startswith("retune defer summary"):
            action_type, key = value.split(":", 1)
            return f"{action_type.strip()}:{key.strip()}"
    card_id = frontmatter.get("card_id", "")
    if card_id.startswith("retune_plan-") and not card_id.endswith("-summary-needs-suman"):
        return "retune_plan:" + card_id.removeprefix("retune_plan-")
    return ""


def _decision_operator_action(decision: str, control_row: dict[str, Any]) -> tuple[str, str]:
    if decision == "approve":
        action = str(control_row.get("recommended_operator_action", "")).strip().upper()
        if not action:
            return "", "approve has no recommended operator action; leave for Jarvis/Codex review"
        return action, "approve maps to the current recommended operator action"
    if decision == "reject":
        return "SKIP", "reject maps to SKIP for the current queued research action"
    if decision == "defer":
        return "", "defer records no Sheet mutation; leave operator_action blank"
    return "", "no checked decision"


def build_review_decision_records(
    *,
    vault: Path,
    ledger: ResearchLedger,
    existing_control_rows: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    card_dir = vault / DECISION_CARD_DIR
    actions = build_next_actions(ledger)
    control_rows = build_control_rows(
        actions=actions,
        generated_at=sheet_timestamp(),
        existing_rows=existing_control_rows or [],
        ledger=ledger,
    )
    control_by_action_id = {str(row.get("action_id", "")).strip(): row for row in control_rows}
    records: list[dict[str, Any]] = []
    for path in sorted(card_dir.glob("*.md")) if card_dir.exists() else []:
        text = path.read_text(encoding="utf-8", errors="replace")
        frontmatter = _parse_card_frontmatter(text)
        decision, warnings = _card_checked_decision(text)
        if not decision and not warnings:
            continue
        action_id = _card_action_id(text, frontmatter)
        control_row = control_by_action_id.get(action_id, {})
        operator_action, mapping_reason = _decision_operator_action(decision, control_row) if decision else ("", "no valid decision")
        status = "ready"
        if warnings:
            status = "blocked"
        elif not action_id:
            status = "blocked"
            warnings.append("card does not map to one Research_Control action; summary cards require Jarvis/manual subset handling")
        elif not control_row:
            status = "blocked"
            warnings.append(f"no current Research_Control row for {action_id}")
        elif decision == "defer":
            status = "no_update"
        elif not operator_action:
            status = "blocked"
            warnings.append(mapping_reason)
        records.append(
            {
                "card_id": frontmatter.get("card_id", path.stem),
                "path": str(path),
                "decision": decision,
                "comments": _card_comments(text),
                "action_id": action_id,
                "operator_action": operator_action,
                "status": status,
                "mapping_reason": mapping_reason,
                "recommendation": str(control_row.get("recommendation", "")),
                "recommended_operator_action": str(control_row.get("recommended_operator_action", "")),
                "decision_needed": str(control_row.get("decision_needed", "")),
                "warnings": warnings,
            }
        )
    return records


def write_review_decision_report(records: list[dict[str, Any]], out_dir: Path, *, applied: bool) -> tuple[Path, Path]:
    stamp = datetime.now(UTC).replace(microsecond=0).isoformat().replace(":", "").replace("-", "").replace("+", "Z")
    report_dir = out_dir / "review_decisions"
    report_dir.mkdir(parents=True, exist_ok=True)
    json_path = report_dir / f"ingest-review-decisions-{stamp}.json"
    md_path = report_dir / f"ingest-review-decisions-{stamp}.md"
    payload = {"generated_at": stamp, "applied": applied, "records": records}
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    lines = ["# Ingest Review Decisions", "", f"- generated_at: `{stamp}`", f"- applied: `{applied}`", f"- decisions: `{len(records)}`", ""]
    if not records:
        lines.append("- No checked decision cards found.")
    for record in records:
        lines.append(f"- `{record['status']}` `{record.get('card_id', '')}` decision={record.get('decision', '')} action_id=`{record.get('action_id', '')}` operator_action=`{record.get('operator_action', '')}`")
        if record.get("mapping_reason"):
            lines.append(f"  - reason: {record['mapping_reason']}")
        for warning in record.get("warnings", []):
            lines.append(f"  - warning: {warning}")
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return json_path, md_path


def apply_review_decision_records(client: GoogleSheetTableClient, records: list[dict[str, Any]]) -> int:
    rows = client.read_rows(range_suffix="A1:ZZ5000")
    by_action_id = {str(row.get("action_id", "")).strip(): row for row in rows}
    updates: list[dict[str, Any]] = []
    for record in records:
        if record.get("status") != "ready" or not record.get("operator_action"):
            continue
        row = by_action_id.get(str(record.get("action_id", "")))
        if not row:
            record.setdefault("warnings", []).append("apply skipped; sheet row disappeared")
            record["status"] = "blocked"
            continue
        next_row = dict(row)
        next_row["operator_action"] = str(record["operator_action"])
        next_row["recommendation"] = str(record.get("recommendation", ""))
        next_row["recommended_operator_action"] = str(record.get("recommended_operator_action", ""))
        next_row["decision_needed"] = str(record.get("decision_needed", ""))
        next_row["updated_at"] = sheet_timestamp()
        updates.append(next_row)
        record["status"] = "applied"
    if updates:
        client.batch_update_rows(rows=updates, columns=["recommendation", "recommended_operator_action", "operator_action", "decision_needed", "updated_at"])
    return len(updates)


def cmd_ingest_review_decisions(args: argparse.Namespace) -> int:
    vault = Path(args.vault).expanduser() if args.vault else DEFAULT_OBSIDIAN_VAULT
    ledger = _build_with_optional_sheets(args)
    existing_rows = _read_control_rows(args) if args.apply else []
    records = build_review_decision_records(vault=vault, ledger=ledger, existing_control_rows=existing_rows)
    applied_count = 0
    if args.apply:
        applied_count = apply_review_decision_records(_control_client(args), records)
    json_path, md_path = write_review_decision_report(records, Path(args.out_dir), applied=bool(args.apply))
    print(f"REVIEW_DECISIONS_JSON={json_path}")
    print(f"REVIEW_DECISIONS_MD={md_path}")
    print(f"DECISIONS={len(records)}")
    print(f"READY={sum(1 for record in records if record.get('status') == 'ready')}")
    print(f"APPLIED={applied_count}")
    print(f"DRY_RUN={'no' if args.apply else 'yes'}")
    return 0

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


def _options_client(args: argparse.Namespace) -> GoogleSheetTableClient:
    credentials = args.options_google_credentials or args.control_google_credentials or args.google_credentials
    sheet_id = args.options_sheet_id or args.control_sheet_id or args.board_sheet_id
    if not sheet_id:
        raise SystemExit("--options-sheet-id, --control-sheet-id, or --board-sheet-id is required")
    if not credentials:
        raise SystemExit("--google-credentials, --control-google-credentials, or --options-google-credentials is required")
    return GoogleSheetTableClient(
        spreadsheet_id=sheet_id,
        sheet_name=args.options_sheet_name,
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


def build_operator_options_table() -> list[list[str]]:
    rows = [OP_OPTIONS_HEADERS]
    length = max(len(CONTROL_OPERATOR_ACTION_DROPDOWN), len(INTAKE_OPERATOR_ACTION_DROPDOWN))
    for index in range(length):
        rows.append(
            [
                CONTROL_OPERATOR_ACTION_DROPDOWN[index] if index < len(CONTROL_OPERATOR_ACTION_DROPDOWN) else "",
                "",
                INTAKE_OPERATOR_ACTION_DROPDOWN[index] if index < len(INTAKE_OPERATOR_ACTION_DROPDOWN) else "",
            ]
        )
    return rows


def _quote_sheet_for_formula(sheet_name: str) -> str:
    return "'" + sheet_name.replace("'", "''") + "'"


def _operator_option_range(sheet_name: str, column_letter: str, option_count: int) -> str:
    return f"={_quote_sheet_for_formula(sheet_name)}!${column_letter}$2:${column_letter}${option_count + 1}"


def _sheet_id_for(client: GoogleSheetTableClient) -> int:
    metadata = client.service.spreadsheets().get(spreadsheetId=client.spreadsheet_id).execute()
    for sheet in metadata.get("sheets", []):
        properties = sheet.get("properties", {})
        if str(properties.get("title", "")).strip() == client.sheet_name:
            return int(properties["sheetId"])
    raise RuntimeError(f"Sheet not found: {client.sheet_name}")


def _operator_action_column_index(client: GoogleSheetTableClient) -> int:
    headers = client._header_row()
    for index, header in enumerate(headers):
        if str(header).strip() == "operator_action":
            return index
    raise RuntimeError(f"operator_action header not found in {client.sheet_name}")


def _apply_operator_action_dropdown(
    *,
    client: GoogleSheetTableClient,
    options_range: str,
    start_row: int = 2,
    end_row: int = 5000,
) -> dict[str, Any]:
    request = {
        "setDataValidation": {
            "range": {
                "sheetId": _sheet_id_for(client),
                "startRowIndex": start_row - 1,
                "endRowIndex": end_row,
                "startColumnIndex": _operator_action_column_index(client),
                "endColumnIndex": _operator_action_column_index(client) + 1,
            },
            "rule": {
                "condition": {
                    "type": "ONE_OF_RANGE",
                    "values": [{"userEnteredValue": options_range}],
                },
                "strict": True,
                "showCustomUi": True,
            },
        }
    }
    return (
        client.service.spreadsheets()
        .batchUpdate(spreadsheetId=client.spreadsheet_id, body={"requests": [request]})
        .execute()
    )


def _clear_sheet_validations(
    *,
    client: GoogleSheetTableClient,
    start_row: int = 2,
    end_row: int = 5000,
) -> dict[str, Any]:
    headers = client._header_row()
    if not headers:
        return {}
    request = {
        "setDataValidation": {
            "range": {
                "sheetId": _sheet_id_for(client),
                "startRowIndex": start_row - 1,
                "endRowIndex": end_row,
                "startColumnIndex": 0,
                "endColumnIndex": len(headers),
            },
        }
    }
    return (
        client.service.spreadsheets()
        .batchUpdate(spreadsheetId=client.spreadsheet_id, body={"requests": [request]})
        .execute()
    )


def push_operator_options(
    *,
    options_client: GoogleSheetTableClient,
    control_client: GoogleSheetTableClient,
    intake_client: GoogleSheetTableClient,
) -> dict[str, Any]:
    options_client.ensure_sheet_exists()
    values = build_operator_options_table()
    options_client.service.spreadsheets().values().clear(
        spreadsheetId=options_client.spreadsheet_id,
        range=f"{options_client.sheet_name}!A1:ZZ100",
        body={},
    ).execute()
    (
        options_client.service.spreadsheets()
        .values()
        .update(
            spreadsheetId=options_client.spreadsheet_id,
            range=f"{options_client.sheet_name}!A1:C{len(values)}",
            valueInputOption="USER_ENTERED",
            body={"values": values},
        )
        .execute()
    )

    control_client.ensure_sheet_exists()
    control_client.ensure_columns(CONTROL_SHEET_HEADERS)
    intake_client.ensure_sheet_exists()
    intake_client.ensure_columns(INTAKE_SHEET_HEADERS)
    control_range = _operator_option_range(
        options_client.sheet_name,
        "A",
        len(CONTROL_OPERATOR_ACTION_DROPDOWN),
    )
    intake_range = _operator_option_range(
        options_client.sheet_name,
        "C",
        len(INTAKE_OPERATOR_ACTION_DROPDOWN),
    )
    _clear_sheet_validations(client=control_client)
    _clear_sheet_validations(client=intake_client)
    _apply_operator_action_dropdown(client=control_client, options_range=control_range)
    _apply_operator_action_dropdown(client=intake_client, options_range=intake_range)
    return {
        "options_rows": len(values) - 1,
        "control_options_range": control_range,
        "intake_options_range": intake_range,
        "dropdowns_applied": 2,
    }


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
        if not exit_opt:
            result["action"] = "blocked_missing_thesis_exit"
            result["block_reason"] = "Run exit optimization/backfill before Strategy_Catalog publish."
            results.append(result)
            continue
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
        ledger=ledger,
    )
    client.overwrite_table(headers=CONTROL_SHEET_HEADERS, rows=rows)
    print(f"CONTROL_SHEET_ID={args.control_sheet_id or args.board_sheet_id}")
    print(f"CONTROL_SHEET_NAME={args.control_sheet_name}")
    print(f"CONTROL_ROWS={len(rows)}")
    return 0


def cmd_push_operator_options(args: argparse.Namespace) -> int:
    result = push_operator_options(
        options_client=_options_client(args),
        control_client=_control_client(args),
        intake_client=_intake_client(args),
    )
    print(f"OP_OPTIONS_SHEET_ID={args.options_sheet_id or args.control_sheet_id or args.board_sheet_id}")
    print(f"OP_OPTIONS_SHEET_NAME={args.options_sheet_name}")
    print(f"OP_OPTIONS_ROWS={result['options_rows']}")
    print(f"CONTROL_OPERATOR_ACTION_RANGE={result['control_options_range']}")
    print(f"INTAKE_OPERATOR_ACTION_RANGE={result['intake_options_range']}")
    print(f"DROPDOWNS_APPLIED={result['dropdowns_applied']}")
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


def cmd_push_intake_view(args: argparse.Namespace) -> int:
    client = _intake_client(args)
    client.ensure_sheet_exists()
    rows = [_enrich_intake_row(row) for row in client.read_rows(range_suffix="A1:ZZ5000")]
    client.overwrite_table(headers=INTAKE_SHEET_HEADERS, rows=rows)
    print(f"INTAKE_SHEET_ID={args.intake_sheet_id or args.control_sheet_id or args.board_sheet_id}")
    print(f"INTAKE_SHEET_NAME={args.intake_sheet_name}")
    print(f"INTAKE_ROWS={len(rows)}")
    return 0


def cmd_propose_intake(args: argparse.Namespace) -> int:
    dry_row = build_intake_proposal_row(
        intake_id=args.intake_id,
        title=args.title,
        hypothesis_id=args.hypothesis_id,
        strategy=args.strategy,
        symbol_scope=args.symbol_scope,
        thesis=args.thesis,
        rules=args.rules,
        notes=args.notes,
        suggested_config=args.suggested_config,
        reason_to_try=args.reason_to_try,
        risk_or_overlap=args.risk_or_overlap,
        max_stage=args.max_stage,
        feasibility_tag=args.feasibility_tag,
        feasibility_summary=args.feasibility_summary,
        source=args.source,
        research_ops_notes=args.research_ops_notes,
        proposed_by=args.proposed_by,
    )
    if not args.apply:
        print(json.dumps(dry_row, indent=2))
        print("INTAKE_PROPOSAL_STATUS=dry_run")
        print("DRY_RUN=true")
        return 0

    client = _intake_client(args)
    client.ensure_sheet_exists()
    client.ensure_columns(INTAKE_SHEET_HEADERS)
    rows = client.read_rows(range_suffix="A1:ZZ5000")
    matching: dict[str, Any] | None = None
    for row in rows:
        if str(row.get("intake_id", "")).strip() == dry_row["intake_id"]:
            matching = row
            break
        if str(row.get("hypothesis_id", "")).strip() == dry_row["hypothesis_id"]:
            matching = row
            break
    status = str((matching or {}).get("status", "")).strip()
    if matching and status not in {"", "proposed_by_research_ops"} and not args.force:
        print(f"INTAKE_PROPOSAL_STATUS=exists")
        print(f"INTAKE_ROW_INDEX={matching.get('row_index', '')}")
        print(f"INTAKE_STATUS={status}")
        print("DRY_RUN=false")
        return 0

    row = build_intake_proposal_row(
        intake_id=args.intake_id,
        title=args.title,
        hypothesis_id=args.hypothesis_id,
        strategy=args.strategy,
        symbol_scope=args.symbol_scope,
        thesis=args.thesis,
        rules=args.rules,
        notes=args.notes,
        suggested_config=args.suggested_config,
        reason_to_try=args.reason_to_try,
        risk_or_overlap=args.risk_or_overlap,
        max_stage=args.max_stage,
        feasibility_tag=args.feasibility_tag,
        feasibility_summary=args.feasibility_summary,
        source=args.source,
        research_ops_notes=args.research_ops_notes,
        proposed_by=args.proposed_by,
        existing_row=matching,
    )
    row_index = str((matching or {}).get("row_index", "")).strip()
    row["row_index"] = int(row_index) if row_index.isdigit() else len(rows) + 2
    client.batch_update_rows(rows=[row], columns=INTAKE_SHEET_HEADERS)
    print(f"INTAKE_PROPOSAL_STATUS={'updated' if matching else 'created'}")
    print(f"INTAKE_ROW_INDEX={row['row_index']}")
    print(f"INTAKE_ID={row['intake_id']}")
    print(f"HYPOTHESIS_ID={row['hypothesis_id']}")
    print("DRY_RUN=false")
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
            columns=INTAKE_SHEET_HEADERS,
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
    published_count = sum(1 for item in results if item.get("action") == "published")
    print(f"CATALOG_PUBLISHED={published_count}")
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


def cmd_shadow_activation_packet(args: argparse.Namespace) -> int:
    evidence_rows, active_rows, defaults_rows = read_sheet_rows(
        spreadsheet_id=args.sheet_id or args.catalog_sheet_id,
        credentials_path=args.google_credentials,
        evidence_sheet_name=args.evidence_sheet_name,
        active_strategy_sheet_name=args.active_strategy_sheet_name,
        operator_defaults_sheet_name=args.operator_defaults_sheet_name,
    )
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    out_dir = Path(args.output_dir) if args.output_dir else Path(args.out_dir) / "shadow_campaign" / "activation" / stamp
    artifacts = build_shadow_activation_packet(
        evidence_rows=evidence_rows,
        active_strategy_rows=active_rows,
        out_dir=out_dir,
        config=ShadowActivationConfig(
            min_execution_robustness=args.min_execution_robustness,
            experiment_min_execution_robustness=args.experiment_min_execution_robustness,
            min_signal_count=args.min_signal_count,
            max_trade_premium_usd=args.max_trade_premium_usd,
            dte_min=args.dte_min,
            dte_max=args.dte_max,
            delta_min=args.delta_min,
            delta_max=args.delta_max,
            max_bid_ask_spread_pct=args.max_bid_ask_spread_pct,
            min_open_interest=args.min_open_interest,
        ),
        include_experiments=args.include_experiments,
    )
    applied_active = 0
    applied_defaults = 0
    if args.apply_active_strategy:
        merged = apply_active_strategy_rows(
            spreadsheet_id=args.sheet_id or args.catalog_sheet_id,
            credentials_path=args.google_credentials,
            active_strategy_sheet_name=args.active_strategy_sheet_name,
            existing_rows=active_rows,
            recommended_rows=artifacts.active_strategy_rows,
            disable_non_recommended=args.disable_non_recommended,
        )
        applied_active = len(merged)
    if args.apply_operator_defaults:
        updates = apply_operator_defaults_patch(
            spreadsheet_id=args.sheet_id or args.catalog_sheet_id,
            credentials_path=args.google_credentials,
            operator_defaults_sheet_name=args.operator_defaults_sheet_name,
            defaults_rows=defaults_rows,
            patch_rows=artifacts.defaults_patch_rows,
        )
        applied_defaults = len(updates)
    print(f"SHADOW_ACTIVATION_REPORT={artifacts.packet_md}")
    print(f"SHADOW_ACTIVATION_CSV={artifacts.packet_csv}")
    print(f"ACTIVE_STRATEGY_ROWS_CSV={artifacts.active_strategy_csv}")
    print(f"OPERATOR_DEFAULTS_PATCH_CSV={artifacts.defaults_patch_csv}")
    print(f"SHADOW_RECOMMENDED={len(artifacts.recommended_rows)}")
    print(f"ACTIVE_STRATEGY_APPLIED_ROWS={applied_active}")
    print(f"OPERATOR_DEFAULTS_APPLIED_ROWS={applied_defaults}")
    print(f"DRY_RUN={'false' if args.apply_active_strategy or args.apply_operator_defaults else 'true'}")
    return 0


def cmd_shadow_daily_report(args: argparse.Namespace) -> int:
    evidence_rows: list[dict[str, Any]] = []
    if args.with_evidence:
        if not (args.sheet_id or args.catalog_sheet_id):
            raise SystemExit("--sheet-id or --catalog-sheet-id is required with --with-evidence")
        evidence_rows, _, _ = read_sheet_rows(
            spreadsheet_id=args.sheet_id or args.catalog_sheet_id,
            credentials_path=args.google_credentials,
            evidence_sheet_name=args.evidence_sheet_name,
            active_strategy_sheet_name=args.active_strategy_sheet_name,
            operator_defaults_sheet_name=args.operator_defaults_sheet_name,
        )
    out_dir = Path(args.output_dir) if args.output_dir else Path(args.out_dir) / "shadow_campaign" / "daily"
    artifacts = build_shadow_daily_report(
        feedback_root=Path(args.feedback_root),
        evidence_rows=evidence_rows,
        out_dir=out_dir,
        active_plan_id=args.active_plan_id or None,
    )
    print(f"SHADOW_DAILY_REPORT={artifacts.report_md}")
    print(f"SHADOW_DAILY_SCORECARD={artifacts.scorecard_csv}")
    print(f"FEEDBACK_BUNDLES={artifacts.bundle_count}")
    print(f"OBSERVATIONS={artifacts.observation_count}")
    print(f"ISSUES={artifacts.issue_count}")
    return 0


def cmd_bhiksha_plumbing_triage(args: argparse.Namespace) -> int:
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    out_dir = Path(args.output_dir) if args.output_dir else Path(args.out_dir) / "bhiksha_plumbing_triage" / stamp
    artifacts = build_bhiksha_plumbing_triage(
        db_path=Path(args.db_path),
        logs_dir=Path(args.logs_dir),
        out_dir=out_dir,
        lookback_days=args.lookback_days,
    )
    print(f"BHIKSHA_PLUMBING_TRIAGE_REPORT={artifacts.report_md}")
    print(f"BHIKSHA_PLUMBING_ISSUE_CSV={artifacts.issue_csv}")
    print(f"BHIKSHA_PLUMBING_DAY_CSV={artifacts.day_csv}")
    print(f"BHIKSHA_PLUMBING_TRADE_BLOCK_CSV={artifacts.trade_block_csv}")
    return 0


def cmd_bhiksha_signal_ev(args: argparse.Namespace) -> int:
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    out_dir = Path(args.output_dir) if args.output_dir else Path(args.out_dir) / "bhiksha_signal_ev" / stamp
    artifacts = build_bhiksha_signal_ev_report(
        db_path=Path(args.db_path),
        out_dir=out_dir,
        lookback_days=args.lookback_days,
        max_signal_lag_minutes=args.max_signal_lag_minutes,
        same_bar_replay=args.same_bar_replay,
        counterfactual_replay=args.counterfactual_replay,
        data_dir=Path(args.data_dir) if args.data_dir else None,
        replay_warmup_days=args.replay_warmup_days,
    )
    print(f"BHIKSHA_SIGNAL_EV_REPORT={artifacts.report_md}")
    print(f"BHIKSHA_SIGNAL_EV_TRADES_CSV={artifacts.trade_csv}")
    print(f"BHIKSHA_SIGNAL_EV_DEPLOYMENTS_CSV={artifacts.deployment_csv}")
    print(f"BHIKSHA_SIGNAL_EV_SIGNALS_CSV={artifacts.signal_csv}")
    print(f"BHIKSHA_SIGNAL_COUNTERFACTUAL_CSV={artifacts.counterfactual_csv}")
    print(f"BHIKSHA_SIGNAL_COUNTERFACTUAL_SUMMARY_CSV={artifacts.counterfactual_summary_csv}")
    return 0


def cmd_provider_volume_parity(args: argparse.Namespace) -> int:
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    out_dir = Path(args.output_dir) if args.output_dir else Path(args.out_dir) / "provider_volume_parity" / stamp
    artifacts = build_provider_volume_parity_report(
        divergence_dir=Path(args.divergence_dir),
        out_dir=out_dir,
        session=args.session,
        relative_volume_window=args.relative_volume_window,
    )
    print(f"PROVIDER_VOLUME_PARITY_REPORT={artifacts.report_md}")
    print(f"PROVIDER_VOLUME_WINDOW_CSV={artifacts.volume_window_csv}")
    print(f"PROVIDER_RELATIVE_VOLUME_CSV={artifacts.relative_volume_csv}")
    print(f"PROVIDER_FEATURE_PARITY_CSV={artifacts.feature_csv}")
    return 0


def cmd_provider_validate_m6(args: argparse.Namespace) -> int:
    run_dirs = [Path(args.run_dir)] if args.run_dir else discover_latest_m5_run_dirs(args.runs_dir)
    provider_relative_volume_csv = args.provider_relative_volume_csv or _latest_provider_artifact(
        args.out_dir,
        "provider_relative_volume_parity.csv",
    )
    provider_feature_parity_csv = args.provider_feature_parity_csv or _latest_provider_artifact(
        args.out_dir,
        "provider_feature_parity.csv",
    )
    artifacts = build_m6_provider_validation(
        run_dirs=run_dirs,
        provider_relative_volume_csv=provider_relative_volume_csv,
        provider_feature_parity_csv=provider_feature_parity_csv,
        provider_replay_csv=args.provider_replay_csv or None,
    )
    print(f"M6_RUN_DIRS={len(artifacts.run_dirs)}")
    for path in artifacts.provider_validation_csvs:
        print(f"M6_PROVIDER_VALIDATION_CSV={path}")
    for path in artifacts.feature_parity_csvs:
        print(f"M6_FEATURE_PARITY_CSV={path}")
    for path in artifacts.review_markdowns:
        print(f"M6_PROVIDER_REVIEW={path}")
    print(f"PROVIDER_RELATIVE_VOLUME_CSV={provider_relative_volume_csv or ''}")
    print(f"PROVIDER_FEATURE_PARITY_CSV={provider_feature_parity_csv or ''}")
    print(f"PROVIDER_REPLAY_CSV={args.provider_replay_csv or ''}")
    return 0


def _latest_provider_artifact(out_dir: str | Path, filename: str) -> str:
    roots = [
        Path(out_dir) / "provider_volume_parity",
        REPO_ROOT / "data" / "results" / "provider_volume_parity",
    ]
    paths = sorted(path for root in roots for path in root.glob(f"*/{filename}"))
    return str(paths[-1]) if paths else ""


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
    parser.add_argument("--options-google-credentials", default="")
    parser.add_argument("--options-sheet-id", default="")
    parser.add_argument("--options-sheet-name", default=DEFAULT_OPTIONS_SHEET_NAME)
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

    program_status = subparsers.add_parser("program-status", help="Write a deterministic Mala program status JSON/Markdown read model.")
    _add_common_args(program_status)
    program_status.add_argument("--limit", type=int, default=50, help="Maximum next-action items to classify before adding running/done summaries.")
    program_status.add_argument("--vault", default=str(DEFAULT_OBSIDIAN_VAULT), help="Northstar vault path for local shadow brief discovery.")
    program_status.set_defaults(func=cmd_program_status)

    publish_cards = subparsers.add_parser("publish-review-cards", help="Create/update Obsidian decision cards for Needs Suman program-status items.")
    _add_common_args(publish_cards)
    publish_cards.add_argument("--vault", default=str(DEFAULT_OBSIDIAN_VAULT), help="Northstar vault path.")
    publish_cards.add_argument("--limit", type=int, default=3, help="Maximum Needs Suman cards to publish.")
    publish_cards.add_argument("--dry-run", action="store_true", help="Preview card creates/updates without writing Obsidian files.")
    publish_cards.add_argument("--refresh", action="store_true", help="Rebuild program-status before publishing instead of using the latest JSON.")
    publish_cards.set_defaults(func=cmd_publish_review_cards)

    ingest_cards = subparsers.add_parser("ingest-review-decisions", help="Read checked Obsidian decision cards and map them to safe sheet actions.")
    _add_common_args(ingest_cards)
    ingest_cards.add_argument("--vault", default=str(DEFAULT_OBSIDIAN_VAULT), help="Northstar vault path.")
    ingest_cards.add_argument("--apply", action="store_true", help="Write ready decisions to Research_Control.operator_action. Omit for dry-run.")
    ingest_cards.set_defaults(func=cmd_ingest_review_decisions)

    push_control = subparsers.add_parser("push-control", help="Mirror next-actions into a Google Sheet control tab.")
    _add_common_args(push_control)
    push_control.add_argument("--limit", type=int, default=25)
    push_control.set_defaults(func=cmd_push_control)

    push_options = subparsers.add_parser("push-operator-options", help="Refresh op_options and dropdowns for operator_action columns.")
    _add_common_args(push_options)
    push_options.set_defaults(func=cmd_push_operator_options)

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

    intake_view = subparsers.add_parser("push-intake-view", help="Rewrite Research_Intake with operator-first columns while preserving rows.")
    _add_common_args(intake_view)
    intake_view.set_defaults(func=cmd_push_intake_view)

    propose_intake = subparsers.add_parser("propose-intake", help="Write a review-only Research_Intake proposal row.")
    _add_common_args(propose_intake)
    propose_intake.add_argument("--idea-id", "--intake-id", dest="intake_id", required=True)
    propose_intake.add_argument("--title", required=True)
    propose_intake.add_argument("--hypothesis-id", default="")
    propose_intake.add_argument("--strategy", "--candidate-strategy", dest="strategy", required=True)
    propose_intake.add_argument("--symbol-scope", required=True)
    propose_intake.add_argument("--thesis", "--hypothesis", dest="thesis", required=True)
    propose_intake.add_argument("--rules", default="")
    propose_intake.add_argument("--notes", default="")
    propose_intake.add_argument("--suggested-config", default="")
    propose_intake.add_argument("--reason-to-try", default="")
    propose_intake.add_argument("--risk-or-overlap", default="")
    propose_intake.add_argument("--max-stage", default="M2")
    propose_intake.add_argument("--feasibility", "--feasibility-tag", dest="feasibility_tag", default="")
    propose_intake.add_argument("--feasibility-reason", "--feasibility-summary", dest="feasibility_summary", default="")
    propose_intake.add_argument("--source", default="")
    propose_intake.add_argument("--research-ops-notes", default="")
    propose_intake.add_argument("--proposed-by", default="research_ops")
    propose_intake.add_argument("--apply", action="store_true", help="Actually upsert the proposal row. Omit for JSON preview.")
    propose_intake.add_argument("--dry-run", action="store_true", help="Explicit no-op alias; dry-run is the default.")
    propose_intake.add_argument("--force", action="store_true", help="Update existing non-proposal intake rows.")
    propose_intake.set_defaults(func=cmd_propose_intake)

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

    shadow_activation = subparsers.add_parser(
        "shadow-activation-packet",
        help="Build a reviewable active_strategy shadow activation packet from Mala_Evidence_v1.",
    )
    _add_common_args(shadow_activation)
    shadow_activation.add_argument("--sheet-id", default="", help="Spreadsheet URL or ID; defaults to --catalog-sheet-id / STRATEGY_CATALOG_SHEET_ID.")
    shadow_activation.add_argument("--evidence-sheet-name", default=DEFAULT_EVIDENCE_SHEET_NAME)
    shadow_activation.add_argument("--active-strategy-sheet-name", default=DEFAULT_ACTIVE_STRATEGY_SHEET_NAME)
    shadow_activation.add_argument("--operator-defaults-sheet-name", default=DEFAULT_OPERATOR_DEFAULTS_SHEET_NAME)
    shadow_activation.add_argument("--output-dir", default="", help="Explicit artifact output directory.")
    shadow_activation.add_argument("--min-execution-robustness", type=float, default=0.75)
    shadow_activation.add_argument("--experiment-min-execution-robustness", type=float, default=0.65)
    shadow_activation.add_argument("--min-signal-count", type=int, default=20)
    shadow_activation.add_argument("--max-trade-premium-usd", type=float, default=2000.0)
    shadow_activation.add_argument("--dte-min", type=int, default=7)
    shadow_activation.add_argument("--dte-max", type=int, default=21)
    shadow_activation.add_argument("--delta-min", type=float, default=0.15)
    shadow_activation.add_argument("--delta-max", type=float, default=0.35)
    shadow_activation.add_argument("--max-bid-ask-spread-pct", type=float, default=0.08)
    shadow_activation.add_argument("--min-open-interest", type=int, default=100)
    shadow_activation.add_argument("--include-experiments", action="store_true", help="Allow 0.65-0.75 MC robustness rows as explicit experiments when other gates pass.")
    shadow_activation.add_argument("--apply-active-strategy", action="store_true", help="Overwrite active_strategy with merged shadow rows. Omit for dry-run packet only.")
    shadow_activation.add_argument("--apply-operator-defaults", action="store_true", help="Patch Operator_Defaults_v1 option constraints. Omit for dry-run packet only.")
    shadow_activation.add_argument("--disable-non-recommended", action="store_true", help="When applying active_strategy, disable currently enabled rows that are not in the current shadow packet.")
    shadow_activation.set_defaults(func=cmd_shadow_activation_packet)

    shadow_daily = subparsers.add_parser(
        "shadow-daily-report",
        help="Summarize Bhiksha session feedback bundles for the shadow campaign.",
    )
    _add_common_args(shadow_daily)
    shadow_daily.add_argument("--feedback-root", default=str(DEFAULT_LIVE_FEEDBACK_DIR))
    shadow_daily.add_argument("--active-plan-id", default="")
    shadow_daily.add_argument("--output-dir", default="", help="Explicit artifact output directory.")
    shadow_daily.add_argument("--with-evidence", action="store_true", help="Join Mala_Evidence_v1 metrics into the scorecard.")
    shadow_daily.add_argument("--sheet-id", default="", help="Spreadsheet URL or ID; defaults to --catalog-sheet-id / STRATEGY_CATALOG_SHEET_ID.")
    shadow_daily.add_argument("--evidence-sheet-name", default=DEFAULT_EVIDENCE_SHEET_NAME)
    shadow_daily.add_argument("--active-strategy-sheet-name", default=DEFAULT_ACTIVE_STRATEGY_SHEET_NAME)
    shadow_daily.add_argument("--operator-defaults-sheet-name", default=DEFAULT_OPERATOR_DEFAULTS_SHEET_NAME)
    shadow_daily.set_defaults(func=cmd_shadow_daily_report)

    plumbing = subparsers.add_parser(
        "bhiksha-plumbing-triage",
        help="Build a historical Bhiksha plumbing report from events.db and runtime logs.",
    )
    _add_common_args(plumbing)
    plumbing.add_argument("--db-path", default="../bhiksha/bhiksha.db")
    plumbing.add_argument("--logs-dir", default="../bhiksha/artifacts/playbook/runtime")
    plumbing.add_argument("--lookback-days", type=int, default=21)
    plumbing.add_argument("--output-dir", default="", help="Explicit artifact output directory.")
    plumbing.set_defaults(func=cmd_bhiksha_plumbing_triage)

    signal_ev = subparsers.add_parser(
        "bhiksha-signal-ev",
        help="Join Bhiksha signals/trades to Mala active-plan evidence and realized option EV.",
    )
    _add_common_args(signal_ev)
    signal_ev.add_argument("--db-path", default="../bhiksha/bhiksha.db")
    signal_ev.add_argument("--lookback-days", type=int, default=21)
    signal_ev.add_argument("--max-signal-lag-minutes", type=int, default=5)
    signal_ev.add_argument("--same-bar-replay", action="store_true", help="Independently rerun Mala strategy params on cached bars for each Bhiksha signal bar.")
    signal_ev.add_argument("--counterfactual-replay", action="store_true", help="Replay each active-plan deployment/day across its Mala signal window and compare expected Mala signals with actual Bhiksha signals.")
    signal_ev.add_argument("--data-dir", default="", help="Mala cached bar directory; defaults to repo data/.")
    signal_ev.add_argument(
        "--replay-warmup-days",
        type=int,
        default=0,
        help="Override replay warmup trading days; default 0 uses Bhiksha startup warmup contract.",
    )
    signal_ev.add_argument("--output-dir", default="", help="Explicit artifact output directory.")
    signal_ev.set_defaults(func=cmd_bhiksha_signal_ev)

    volume_parity = subparsers.add_parser(
        "provider-volume-parity",
        help="Summarize Schwab-vs-Polygon volume parity from Bhiksha divergence CSVs.",
    )
    _add_common_args(volume_parity)
    volume_parity.add_argument("--divergence-dir", default="../bhiksha/artifacts/provider_divergence")
    volume_parity.add_argument("--session", choices=["all", "regular", "extended"], default="regular")
    volume_parity.add_argument("--relative-volume-window", type=int, default=20)
    volume_parity.add_argument("--output-dir", default="", help="Explicit artifact output directory.")
    volume_parity.set_defaults(func=cmd_provider_volume_parity)

    provider_validate = subparsers.add_parser(
        "provider-validate-m6",
        help="Write advisory M6 provider-validation artifacts into M5 run dirs.",
    )
    _add_common_args(provider_validate)
    provider_validate.add_argument("--run-dir", default="", help="One M5 run dir. Omit to process latest M5 run dirs.")
    provider_validate.add_argument(
        "--provider-relative-volume-csv",
        default="",
        help="provider_relative_volume_parity.csv. Defaults to latest data/results/research_ops/provider_volume_parity run.",
    )
    provider_validate.add_argument(
        "--provider-feature-parity-csv",
        default="",
        help="provider_feature_parity.csv. Defaults to latest data/results/research_ops/provider_volume_parity run.",
    )
    provider_validate.add_argument(
        "--provider-replay-csv",
        default="",
        help="Optional volume_mismatch_replay_by_row.csv or catalog_volume_sensitivity_by_row.csv for signal overlap.",
    )
    provider_validate.set_defaults(func=cmd_provider_validate_m6)

    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
