from __future__ import annotations

import csv
from pathlib import Path
from types import SimpleNamespace

import pytest

from src.research.research_ops import (
    _board_sync_plan,
    _catalog_publish_plan,
    _publish_catalog_rows,
    parse_args,
    FindingDisposition,
    append_disposition,
    apply_review_decision_records,
    build_action_brief,
    build_control_rows,
    build_hot_start_findings,
    INTAKE_SHEET_HEADERS,
    build_intake_proposal_row,
    build_operator_options_table,
    build_ledger,
    build_next_actions,
    build_review_decision_records,
    build_surface_expansion_plan,
    score_researcher_verdict,
    evaluate_hypothesis_intake,
    process_intake_rows,
    read_dispositions,
    update_control_row_with_brief,
    update_control_row_with_surface_plan,
    write_action_brief,
    write_csv_tables,
    write_digest_report,
    write_hot_start_report,
    write_surface_expansion_plan,
    write_workbook,
)


def _write_hypothesis(
    root: Path,
    *,
    hypothesis_id: str,
    state: str,
    decision: str,
    strategy: str = "Market Impulse (Cross & Reclaim)",
    symbol_scope: str = "AMD",
) -> Path:
    path = root / f"{hypothesis_id}.md"
    path.write_text(
        "\n".join(
            [
                "# Hypothesis",
                "## Config",
                f"- id: `{hypothesis_id}`",
                f"- state: `{state}`",
                f"- decision: `{decision}`",
                f"- symbol_scope: `{symbol_scope}`",
                f"- strategy: `{strategy}`",
                "- max_stage: `M5`",
                "- last_run: `2026-04-24T13:39:00+0000`",
            ]
        ),
        encoding="utf-8",
    )
    return path


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def test_build_ledger_backfills_promoted_candidate_and_catalog_presence(tmp_path: Path) -> None:
    hypotheses = tmp_path / "research" / "hypotheses"
    runs = tmp_path / "research" / "results" / "hypothesis_runs"
    hypotheses.mkdir(parents=True)
    _write_hypothesis(hypotheses, hypothesis_id="idea", state="completed", decision="promote")
    run_dir = runs / "idea" / "2026-04-24T083939"
    run_dir.mkdir(parents=True)
    (run_dir / "RUN_SUMMARY.md").write_text("- decision: `promote`\n", encoding="utf-8")
    _write_csv(
        run_dir / "M5_execution.csv",
        [
            {
                "ticker": "AMD",
                "direction": "short",
                "execution_profile": "single_option",
            }
        ],
    )
    _write_csv(
        run_dir / "CATALOG_SELECTED.csv",
        [
            {
                "catalog_key": "idea__amd_short",
                "ticker": "AMD",
                "direction": "short",
                "strategy": "Market Impulse",
                "execution_profile": "single_option",
                "recommendation_tier": "promote",
                "exit_reliability": "none",
                "selected_exit_policy": "",
                "mc_prob_positive_exp": "0.99",
                "mc_exp_r_p50": "0.4",
                "base_exp_r": "0.5",
                "holdout_trades": "110",
                "holdout_win_rate": "0.55",
            }
        ],
    )

    ledger = build_ledger(
        hypotheses_dir=hypotheses,
        runs_dir=runs,
        strategy_catalog_rows=[{"catalog_key": "idea__amd_short"}],
    )

    assert len(ledger.hypotheses) == 1
    assert ledger.hypotheses[0].latest_stage == "M5"
    assert ledger.runs[0].decision == "promote"
    assert ledger.promoted[0].in_strategy_catalog == "yes"
    assert not [f for f in ledger.findings if f.category == "catalog_publish_pending"]


def test_hot_start_flags_missing_catalog_and_stale_board_state(tmp_path: Path) -> None:
    hypotheses = tmp_path / "research" / "hypotheses"
    runs = tmp_path / "research" / "results" / "hypothesis_runs"
    hypotheses.mkdir(parents=True)
    _write_hypothesis(hypotheses, hypothesis_id="expand30-amd-mi-01", state="completed", decision="promote")
    run_dir = runs / "expand30-amd-mi-01" / "2026-04-24T083939"
    run_dir.mkdir(parents=True)
    (run_dir / "RUN_SUMMARY.md").write_text("- decision: `promote`\n", encoding="utf-8")
    _write_csv(
        run_dir / "CATALOG_SELECTED.csv",
        [
            {
                "catalog_key": "expand30-amd-mi-01__amd_short",
                "ticker": "AMD",
                "direction": "short",
                "strategy": "Market Impulse",
                "execution_profile": "single_option",
                "recommendation_tier": "promote",
                "exit_reliability": "none",
                "selected_exit_policy": "",
                "mc_prob_positive_exp": "0.99",
                "mc_exp_r_p50": "0.4",
                "base_exp_r": "0.5",
                "holdout_trades": "110",
                "holdout_win_rate": "0.55",
            }
        ],
    )
    ledger = build_ledger(hypotheses_dir=hypotheses, runs_dir=runs, strategy_catalog_rows=[])

    findings = build_hot_start_findings(
        hypotheses=ledger.hypotheses,
        runs=ledger.runs,
        promoted=ledger.promoted,
        board_rows=[
            {
                "Task_ID": "SC-20260422-expand30-amd-mi-01",
                "Operator_Action": "APPROVED_RUN",
                "Agent_State": "APPROVED_M1",
            }
        ],
    )

    categories = {finding.category for finding in findings}
    assert "board_state_stale" in categories


def test_writers_emit_csv_workbook_and_hot_start_report(tmp_path: Path) -> None:
    hypotheses = tmp_path / "research" / "hypotheses"
    runs = tmp_path / "runs"
    hypotheses.mkdir(parents=True)
    _write_hypothesis(hypotheses, hypothesis_id="idea", state="retune", decision="retune")

    ledger = build_ledger(hypotheses_dir=hypotheses, runs_dir=runs)
    csv_paths = write_csv_tables(ledger, tmp_path / "csv")
    workbook = write_workbook(ledger, tmp_path / "research_ledger.xlsx")
    report = write_hot_start_report(ledger=ledger, path=tmp_path / "hot_start.md")

    assert csv_paths["hypotheses"].exists()
    assert workbook.exists()
    assert report.read_text(encoding="utf-8").startswith("# Mala Research Hot Start")


def test_digest_report_summarizes_queue_and_intake(tmp_path: Path) -> None:
    hypotheses = tmp_path / "research" / "hypotheses"
    runs = tmp_path / "runs"
    hypotheses.mkdir(parents=True)
    _write_hypothesis(hypotheses, hypothesis_id="idea", state="pending", decision="")
    ledger = build_ledger(hypotheses_dir=hypotheses, runs_dir=runs)
    actions = build_next_actions(ledger)

    digest = write_digest_report(
        ledger=ledger,
        actions=actions,
        control_rows=[{"operator_action": "APPROVE_RETUNE", "action_id": "retune_plan:old", "status": "queued"}],
        intake_rows=[
            {
                "status": "evaluated_ready_for_approval",
                "hypothesis_id": "new-idea",
                "strategy": "Opening Drive Classifier",
                "operator_action": "",
            }
        ],
        path=tmp_path / "digest.md",
        days=1,
    )

    text = Path(digest.report_path).read_text(encoding="utf-8")
    assert digest.pending_control_actions == 1
    assert "Mala Research Digest" in text
    assert "`run_m1`" in text
    assert "new-idea" in text


def test_next_actions_rank_publish_and_retune_work(tmp_path: Path) -> None:
    hypotheses = tmp_path / "research" / "hypotheses"
    runs = tmp_path / "research" / "results" / "hypothesis_runs"
    hypotheses.mkdir(parents=True)
    _write_hypothesis(hypotheses, hypothesis_id="pending-idea", state="pending", decision="")
    _write_hypothesis(hypotheses, hypothesis_id="retune-idea", state="retune", decision="retune")
    _write_hypothesis(hypotheses, hypothesis_id="promoted-idea", state="completed", decision="promote")
    run_dir = runs / "promoted-idea" / "2026-04-24T083939"
    run_dir.mkdir(parents=True)
    (run_dir / "RUN_SUMMARY.md").write_text("- decision: `promote`\n", encoding="utf-8")
    _write_csv(
        run_dir / "CATALOG_SELECTED.csv",
        [
            {
                "catalog_key": "promoted-idea__amd_short",
                "ticker": "AMD",
                "direction": "short",
                "strategy": "Market Impulse",
                "execution_profile": "single_option",
                "recommendation_tier": "promote",
                "exit_reliability": "none",
                "selected_exit_policy": "",
                "mc_prob_positive_exp": "0.99",
                "mc_exp_r_p50": "0.4",
                "base_exp_r": "0.5",
                "holdout_trades": "110",
                "holdout_win_rate": "0.55",
            }
        ],
    )

    ledger = build_ledger(hypotheses_dir=hypotheses, runs_dir=runs, strategy_catalog_rows=[{"catalog_key": "different"}])
    actions = build_next_actions(ledger)

    assert [action.action_type for action in actions[:3]] == ["publish_pending", "retune_plan", "run_m1"]
    assert actions[0].mutates_external_state == "yes"


def test_control_skip_disposition_suppresses_matching_next_action(tmp_path: Path) -> None:
    hypotheses = tmp_path / "research" / "hypotheses"
    runs = tmp_path / "research" / "results" / "hypothesis_runs"
    hypotheses.mkdir(parents=True)
    _write_hypothesis(hypotheses, hypothesis_id="retune-idea", state="retune", decision="retune")
    ledger = build_ledger(
        hypotheses_dir=hypotheses,
        runs_dir=runs,
        dispositions=[
            FindingDisposition(
                created_at="2026-05-08T00:00:00+00:00",
                status="ignore",
                key="retune_plan:retune-idea",
                category="control_skip",
                reason="operator skip",
            )
        ],
    )

    actions = build_next_actions(ledger)

    assert "retune_plan:retune-idea" not in [f"{action.action_type}:{action.key}" for action in actions]


def test_pending_run_m1_brief_recommends_approve_run_m1(tmp_path: Path) -> None:
    hypotheses = tmp_path / "research" / "hypotheses"
    runs = tmp_path / "research" / "results" / "hypothesis_runs"
    hypotheses.mkdir(parents=True)
    _write_hypothesis(hypotheses, hypothesis_id="pending-idea", state="pending", decision="")
    ledger = build_ledger(hypotheses_dir=hypotheses, runs_dir=runs)

    brief = build_action_brief(ledger=ledger, key="run_m1:pending-idea")

    assert brief.recommendation == "RUN_M1_REVIEW"
    assert brief.suggested_operator_action == "APPROVE_RUN_M1"


def test_running_m1_promote_brief_recommends_continue_m2(tmp_path: Path) -> None:
    hypotheses = tmp_path / "research" / "hypotheses"
    runs = tmp_path / "research" / "results" / "hypothesis_runs"
    hypotheses.mkdir(parents=True)
    _write_hypothesis(hypotheses, hypothesis_id="running-idea", state="running", decision="promote_to_m2")
    run_dir = runs / "running-idea" / "2026-05-08T080000"
    run_dir.mkdir(parents=True)
    (run_dir / "RUN_SUMMARY.md").write_text("- decision: `promote_to_m2`\n", encoding="utf-8")
    _write_csv(run_dir / "M1_top.csv", [{"ticker": "AMD", "avg_test_exp_r": "0.1", "m1_score": "1.2"}])
    ledger = build_ledger(hypotheses_dir=hypotheses, runs_dir=runs)

    brief = build_action_brief(ledger=ledger, key="resume_or_normalize:running-idea")

    assert brief.recommendation == "CONTINUE_M2_REVIEW"
    assert brief.suggested_operator_action == "APPROVE_CONTINUE_M2"


def test_build_control_rows_preserves_operator_action() -> None:
    from src.research.research_ops import NextAction

    actions = [
        NextAction(
            rank=1,
            priority="medium",
            action_type="retune_plan",
            key="idea",
            reason="needs retune",
            suggested_command="cmd",
            requires_approval="yes",
            mutates_external_state="no",
        )
    ]

    rows = build_control_rows(
        actions=actions,
        generated_at="2026-04-24T00:00:00+00:00",
        existing_rows=[
            {
                "action_id": "retune_plan:idea",
                "operator_action": "APPROVE_RETUNE",
                "status": "reviewed",
                "last_report_path": "old.md",
            }
        ],
    )

    assert rows[0]["action_id"] == "retune_plan:idea"
    assert rows[0]["operator_action"] == "APPROVE_RETUNE"
    assert rows[0]["status"] == "reviewed"
    assert rows[0]["brief_recommendation"] == ""
    assert rows[0]["last_report_path"] == "old.md"


def test_build_control_rows_preserves_invalid_operator_action_with_status() -> None:
    from src.research.research_ops import NextAction

    actions = [
        NextAction(
            rank=1,
            priority="medium",
            action_type="retune_plan",
            key="idea",
            reason="needs decision",
            suggested_command="cmd",
            requires_approval="yes",
            mutates_external_state="no",
        )
    ]

    rows = build_control_rows(
        actions=actions,
        generated_at="2026-04-24 18:00:00 CDT",
        existing_rows=[
            {
                "action_id": "retune_plan:idea",
                "operator_action": "KILL",
                "status": "queued",
            }
        ],
    )

    assert rows[0]["operator_action"] == "KILL"
    assert rows[0]["status"] == "invalid_operator_action:KILL"


def test_action_brief_recommends_retune_after_m2_exp_positive_instability(tmp_path: Path) -> None:
    hypotheses = tmp_path / "research" / "hypotheses"
    runs = tmp_path / "research" / "results" / "hypothesis_runs"
    hypotheses.mkdir(parents=True)
    _write_hypothesis(hypotheses, hypothesis_id="idea", state="retune", decision="retune")
    run_dir = runs / "idea" / "2026-04-24T083939"
    run_dir.mkdir(parents=True)
    (run_dir / "RUN_SUMMARY.md").write_text("- decision: `retune`\n- M2: 0 candidates promoted\n", encoding="utf-8")
    _write_csv(
        run_dir / "M2_gate_report.csv",
        [
            {
                "ticker": "AMD",
                "strategy": "Market Impulse",
                "direction": "short",
                "min_avg_test_exp_r": "0.04",
                "min_pct_positive_oos_windows": "50.0",
                "passes_exp_gate": "true",
                "passes_stability_gate": "false",
                "passes_all_gates": "false",
                "score": "0.40",
                "regime_timeframe": "15m",
                "fast_vwma": "8",
                "slow_vwma": "21",
            }
        ],
    )
    ledger = build_ledger(hypotheses_dir=hypotheses, runs_dir=runs)

    brief = build_action_brief(ledger=ledger, key="idea", action_type="retune_plan")
    brief = write_action_brief(brief, tmp_path / "ops")

    assert brief.recommendation == "APPROVE_RETUNE"
    assert brief.suggested_operator_action == "APPROVE_RETUNE"
    assert "Expectancy exists" in brief.summary
    assert Path(brief.report_path).exists()
    assert "Market Impulse tuning center" in "\n".join(brief.surface_proposal)


def test_action_brief_recommends_skip_after_no_positive_m1_retune(tmp_path: Path) -> None:
    hypotheses = tmp_path / "research" / "hypotheses"
    runs = tmp_path / "research" / "results" / "hypothesis_runs"
    hypotheses.mkdir(parents=True)
    _write_hypothesis(hypotheses, hypothesis_id="dead-idea", state="retune", decision="retune")
    run_dir = runs / "dead-idea" / "2026-04-24T083939"
    run_dir.mkdir(parents=True)
    (run_dir / "RUN_SUMMARY.md").write_text(
        "- decision: `retune`\n\n## Notes\n\n- M1 FAIL: no positive configs found\n",
        encoding="utf-8",
    )
    ledger = build_ledger(hypotheses_dir=hypotheses, runs_dir=runs)

    brief = build_action_brief(ledger=ledger, key="retune_plan:dead-idea")

    assert brief.recommendation == "KILL_OR_SKIP"
    assert brief.suggested_operator_action == "SKIP"


def test_control_rows_surface_operator_recommendation_for_failed_retune(tmp_path: Path) -> None:
    hypotheses = tmp_path / "research" / "hypotheses"
    runs = tmp_path / "research" / "results" / "hypothesis_runs"
    hypotheses.mkdir(parents=True)
    _write_hypothesis(hypotheses, hypothesis_id="dead-idea", state="retune", decision="retune")
    run_dir = runs / "dead-idea" / "2026-04-24T083939"
    run_dir.mkdir(parents=True)
    (run_dir / "RUN_SUMMARY.md").write_text(
        "- decision: `retune`\n\n## Notes\n\n- M1 FAIL: no positive configs found\n",
        encoding="utf-8",
    )
    ledger = build_ledger(hypotheses_dir=hypotheses, runs_dir=runs)
    actions = build_next_actions(ledger)

    rows = build_control_rows(
        actions=actions,
        existing_rows=[{"action_id": "retune_plan:dead-idea", "status": "queued"}],
        generated_at="2026-05-07 06:10:00 CDT",
        ledger=ledger,
    )

    action_row = next(row for row in rows if row["action_id"] == "retune_plan:dead-idea")
    assert action_row["recommended_operator_action"] == "SKIP"
    assert action_row["recommendation"] == "KILL_OR_SKIP"
    assert "Choose SKIP" in action_row["decision_needed"]
    assert action_row["symbol_scope"] == "AMD"


class _FakeControlClient:
    def __init__(self) -> None:
        self.rows = [{"row_index": 2, "action_id": "retune_plan:idea", "updated_at": ""}]
        self.columns: list[str] = []

    def ensure_sheet_exists(self) -> None:
        return None

    def ensure_columns(self, columns: list[str]) -> list[str]:
        self.columns = columns
        return []

    def read_rows(self, *, range_suffix: str = "A1:ZZ5000") -> list[dict[str, object]]:
        return self.rows

    def batch_update_rows(self, *, rows: list[dict[str, object]], columns: list[str]) -> dict[str, object]:
        self.rows = rows
        self.columns = columns
        return {}


def test_update_control_row_with_brief_writes_brief_columns() -> None:
    from src.research.research_ops import ActionBrief

    client = _FakeControlClient()
    brief = ActionBrief(
        generated_at="2026-04-24T00:00:00+00:00",
        action_id="retune_plan:idea",
        action_type="retune_plan",
        key="idea",
        hypothesis_id="idea",
        recommendation="APPROVE_RETUNE",
        suggested_operator_action="APPROVE_RETUNE",
        summary="Bounded retune is reasonable.",
        suggested_command="cmd",
        report_path="brief.md",
        evidence=[],
        surface_proposal=[],
        sources=[],
    )

    assert update_control_row_with_brief(client=client, brief=brief)
    assert client.rows[0]["brief_recommendation"] == "APPROVE_RETUNE"
    assert client.rows[0]["brief_summary"] == "Bounded retune is reasonable."
    assert client.rows[0]["brief_path"] == "brief.md"


def test_surface_expansion_plan_recommends_config_only_for_m1_sample_failure(tmp_path: Path) -> None:
    hypotheses = tmp_path / "research" / "hypotheses"
    runs = tmp_path / "research" / "results" / "hypothesis_runs"
    hypotheses.mkdir(parents=True)
    _write_hypothesis(
        hypotheses,
        hypothesis_id="opening-idea",
        state="retune",
        decision="retune",
        strategy="Opening Drive Classifier",
        symbol_scope="MSFT",
    )
    run_dir = runs / "opening-idea" / "2026-04-24T083939"
    run_dir.mkdir(parents=True)
    (run_dir / "RUN_SUMMARY.md").write_text(
        "- decision: `retune`\n\n## Notes\n\n- M1 FAIL: signals=15<50; windows=1<3\n",
        encoding="utf-8",
    )
    ledger = build_ledger(hypotheses_dir=hypotheses, runs_dir=runs)

    plan = build_surface_expansion_plan(ledger=ledger, key="retune_plan:opening-idea")
    plan = write_surface_expansion_plan(plan, tmp_path / "ops")

    assert plan.feasibility_tag == "config-only"
    assert plan.recommendation == "CONFIG_ONLY_SURFACE_EXPANSION"
    assert plan.next_operator_action == ""
    assert any("opening_window_minutes" in line for line in plan.proposed_bounds)
    assert Path(plan.report_path).exists()
    assert Path(plan.json_path).exists()


def test_update_control_row_with_surface_plan_writes_plan_columns() -> None:
    from src.research.research_ops import SurfaceExpansionPlan

    client = _FakeControlClient()
    plan = SurfaceExpansionPlan(
        generated_at="2026-04-24T00:00:00+00:00",
        action_id="retune_plan:idea",
        key="idea",
        hypothesis_id="idea",
        strategy="Opening Drive Classifier",
        symbol_scope="MSFT",
        feasibility_tag="config-only",
        recommendation="CONFIG_ONLY_SURFACE_EXPANSION",
        next_operator_action="",
        summary="Expand one bounded parameter family.",
        proposed_bounds=[],
        rationale=[],
        validation_steps=[],
        sources=[],
        report_path="surface.md",
        json_path="surface.json",
    )

    assert update_control_row_with_surface_plan(client=client, plan=plan)
    assert client.rows[0]["recommendation"] == "CONFIG_ONLY_SURFACE_EXPANSION"
    assert client.rows[0]["recommended_operator_action"] == ""
    assert "Review evidence" in client.rows[0]["decision_needed"]
    assert client.rows[0]["evidence_summary"] == "Expand one bounded parameter family."
    assert "config-only search-surface patch" in client.rows[0]["next_step"]
    assert client.rows[0]["artifact_path"] == "surface.md"
    assert client.rows[0]["brief_recommendation"] == "CONFIG_ONLY_SURFACE_EXPANSION"
    assert client.rows[0]["brief_summary"] == "Expand one bounded parameter family."
    assert client.rows[0]["brief_path"] == "surface.md"
    assert client.rows[0]["last_report_path"] == "surface.md"
    assert client.rows[0]["status"] == "surface_plan_ready"


def test_control_rows_preserve_surface_plan_recommendation_on_refresh(tmp_path: Path) -> None:
    from src.research.research_ops import NextAction

    hypotheses = tmp_path / "research" / "hypotheses"
    runs = tmp_path / "research" / "results" / "hypothesis_runs"
    hypotheses.mkdir(parents=True)
    _write_hypothesis(
        hypotheses,
        hypothesis_id="opening-idea",
        state="retune",
        decision="retune",
        strategy="Opening Drive Classifier",
        symbol_scope="MSFT",
    )
    run_dir = runs / "opening-idea" / "2026-04-24T083939"
    run_dir.mkdir(parents=True)
    (run_dir / "RUN_SUMMARY.md").write_text(
        "- decision: `retune`\n\n## Notes\n\n- M1 FAIL: signals=15<50; windows=1<3\n",
        encoding="utf-8",
    )
    ledger = build_ledger(hypotheses_dir=hypotheses, runs_dir=runs)
    action = NextAction(
        rank=1,
        priority="medium",
        action_type="retune_plan",
        key="opening-idea",
        reason="needs retune",
        suggested_command="cmd",
        requires_approval="yes",
        mutates_external_state="no",
    )

    rows = build_control_rows(
        actions=[action],
        generated_at="2026-05-08 08:00:00 CDT",
        existing_rows=[
            {
                "row_index": 2,
                "action_id": "retune_plan:opening-idea",
                "status": "surface_plan_ready",
                "recommendation": "SURFACE_EXPANSION_REVIEW",
                "recommended_operator_action": "APPROVE_RETUNE",
                "brief_recommendation": "CONFIG_ONLY_SURFACE_EXPANSION",
                "brief_summary": "Plan says bounded retune next.",
                "brief_path": "research/results/research_ops/surface_expansion/plan.md",
            }
        ],
        ledger=ledger,
    )

    assert rows[0]["recommendation"] == "CONFIG_ONLY_SURFACE_EXPANSION"
    assert rows[0]["recommended_operator_action"] == ""
    assert rows[0]["evidence_summary"] == "Plan says bounded retune next."
    assert rows[0]["artifact_path"] == "research/results/research_ops/surface_expansion/plan.md"


def test_evaluate_hypothesis_intake_classifies_config_only() -> None:
    evaluation = evaluate_hypothesis_intake(
        {
            "title": "MSFT opening drive extension",
            "strategy": "Opening Drive Classifier",
            "symbol_scope": "MSFT",
            "max_stage": "M5",
        }
    )

    assert evaluation.feasibility_tag == "config-only"
    assert evaluation.hypothesis_id == "msft-opening-drive-extension"
    assert evaluation.discovery_config_count > 0
    assert "opening_window_minutes" in evaluation.search_param_keys


def test_evaluate_hypothesis_intake_blocks_unknown_strategy() -> None:
    evaluation = evaluate_hypothesis_intake(
        {
            "title": "Needs new idea",
            "strategy": "Totally New Strategy",
            "symbol_scope": "SPY",
        }
    )

    assert evaluation.feasibility_tag == "new-class"
    assert "not in the current factory registry" in evaluation.feasibility_summary


def test_build_intake_proposal_row_is_review_only() -> None:
    row = build_intake_proposal_row(
        intake_id="xle-market-impulse-cross-reclaim-01",
        title="XLE Market Impulse Cross & Reclaim",
        hypothesis_id="xle-market-impulse-cross-reclaim-01",
        strategy="Market Impulse (Cross & Reclaim)",
        symbol_scope="XLE",
        thesis="Energy-sector relative strength after pullback/reclaim may produce intraday continuation.",
        suggested_config="Bounded M1 first; continue only if the M1 gate passes.",
        max_stage="M2",
        source="StockCharts May 5 sector note; FXEmpire Apr 28 XLE note.",
        research_ops_notes="No existing XLE hypothesis found.",
        proposed_at="2026-05-07 06:10:00 CDT",
    )

    assert row["operator_action"] == ""
    assert row["status"] == "proposed_by_research_ops"
    assert row["recommendation"] == "EVALUATE"
    assert row["recommended_operator_action"] == "EVALUATE"
    assert "Choose EVALUATE" in row["decision_needed"]
    assert row["feasibility_tag"] == "config-only"
    assert row["max_stage"] == "M2"
    assert row["suggested_config"] == "Bounded M1 first; continue only if the M1 gate passes."
    assert "Bounded M1 first" in row["notes"]
    assert row["source"].startswith("StockCharts")


def test_build_operator_options_table_lists_sheet_dropdown_values() -> None:
    table = build_operator_options_table()

    assert table[0] == [
        "Operator_Action in Research Control",
        "",
        "Operator_Action in Research_Intake",
    ]
    assert ["APPROVE_CONTINUE_M2", "", "EVALUATE"] in table
    assert any(row[0] == "APPROVE_RUN_M1" for row in table)
    assert any(row[0] == "APPROVE_RETUNE" for row in table)
    assert ["SKIP", "", ""] in table
    assert any(row[2] == "APPROVE_CREATE_HYPOTHESIS" for row in table)


def test_intake_sheet_headers_keep_human_reading_columns_first() -> None:
    assert INTAKE_SHEET_HEADERS[:8] == [
        "symbol_scope",
        "strategy",
        "thesis",
        "status",
        "recommendation",
        "recommended_operator_action",
        "operator_action",
        "decision_needed",
    ]
    assert len(INTAKE_SHEET_HEADERS) == len(set(INTAKE_SHEET_HEADERS))


def test_process_intake_rows_creates_pending_hypothesis_when_approved(tmp_path: Path) -> None:
    updates = process_intake_rows(
        rows=[
            {
                "row_index": 2,
                "title": "MSFT opening drive extension",
                "strategy": "Opening Drive Classifier",
                "symbol_scope": "MSFT",
                "thesis": "Opening drive sample recovery.",
                "rules": "Use existing Opening Drive rules; widen sample window one notch",
                "operator_action": "APPROVE_CREATE_HYPOTHESIS",
            }
        ],
        hypotheses_dir=tmp_path / "hypotheses",
        out_dir=tmp_path / "ops",
        apply=True,
    )

    assert updates[0]["status"] == "created_pending"
    created = tmp_path / "hypotheses" / "msft-opening-drive-extension.md"
    assert created.exists()
    text = created.read_text(encoding="utf-8")
    assert "- state: `pending`" in text
    assert "Feasibility tag: config-only." in text


def test_process_intake_rows_evaluate_does_not_create_hypothesis(tmp_path: Path) -> None:
    updates = process_intake_rows(
        rows=[
            {
                "row_index": 2,
                "title": "Unknown class idea",
                "strategy": "New Strategy",
                "symbol_scope": "SPY",
                "operator_action": "EVALUATE",
            }
        ],
        hypotheses_dir=tmp_path / "hypotheses",
        out_dir=tmp_path / "ops",
        apply=True,
    )

    assert updates[0]["status"] == "blocked_new-class"
    assert not list((tmp_path / "hypotheses").glob("*.md"))


def test_catalog_publish_plan_uses_latest_missing_promoted_rows(tmp_path: Path) -> None:
    hypotheses = tmp_path / "research" / "hypotheses"
    runs = tmp_path / "research" / "results" / "hypothesis_runs"
    hypotheses.mkdir(parents=True)
    _write_hypothesis(hypotheses, hypothesis_id="idea", state="completed", decision="promote")
    run_dir = runs / "idea" / "2026-04-24T083939"
    run_dir.mkdir(parents=True)
    (run_dir / "RUN_SUMMARY.md").write_text("- decision: `promote`\n", encoding="utf-8")
    _write_csv(
        run_dir / "CATALOG_SELECTED.csv",
        [
            {
                "catalog_key": "idea__amd_short",
                "ticker": "AMD",
                "direction": "short",
                "strategy": "Market Impulse",
                "execution_profile": "single_option",
                "recommendation_tier": "promote",
                "exit_reliability": "none",
                "selected_exit_policy": "",
                "mc_prob_positive_exp": "0.99",
                "mc_exp_r_p50": "0.4",
                "base_exp_r": "0.5",
                "holdout_trades": "110",
                "holdout_win_rate": "0.55",
            }
        ],
    )

    ledger = build_ledger(hypotheses_dir=hypotheses, runs_dir=runs, strategy_catalog_rows=[{"catalog_key": "different"}])

    assert [row.catalog_key for row in _catalog_publish_plan(ledger=ledger, catalog_keys={"different"})] == ["idea__amd_short"]
    assert _catalog_publish_plan(ledger=ledger, catalog_keys={"idea__amd_short"}) == []


def test_publish_catalog_rows_blocks_rows_without_exit_artifact(tmp_path: Path) -> None:
    hypotheses = tmp_path / "research" / "hypotheses"
    runs = tmp_path / "research" / "results" / "hypothesis_runs"
    hypotheses.mkdir(parents=True)
    _write_hypothesis(hypotheses, hypothesis_id="idea", state="completed", decision="promote")
    run_dir = runs / "idea" / "2026-04-24T083939"
    run_dir.mkdir(parents=True)
    (run_dir / "RUN_SUMMARY.md").write_text("- decision: `promote`\n", encoding="utf-8")
    selected = {
        "catalog_key": "idea__amd_short",
        "ticker": "AMD",
        "direction": "short",
        "strategy": "Market Impulse (Cross & Reclaim)",
        "execution_profile": "single_option",
        "recommendation_tier": "promote",
        "exit_reliability": "none",
        "selected_exit_policy": "",
        "mc_prob_positive_exp": "0.99",
        "mc_exp_r_p50": "0.4",
        "base_exp_r": "0.5",
        "holdout_trades": "110",
        "holdout_win_rate": "0.55",
        "entry_buffer_minutes": "5",
        "entry_window_minutes": "45",
    }
    _write_csv(run_dir / "CATALOG_SELECTED.csv", [selected])
    _write_csv(
        run_dir / "M5_execution.csv",
        [
            {
                **selected,
                "stress_profile": "single_option",
                "structure": "long_put",
                "dte": "7-21",
                "delta_plan": "0.35-0.55",
                "entry_window_et": "09:45-14:30",
                "profit_take": "50-90% premium",
                "risk_rule": "hard stop at -35% premium",
            }
        ],
    )
    ledger = build_ledger(hypotheses_dir=hypotheses, runs_dir=runs, strategy_catalog_rows=[])
    rows = _catalog_publish_plan(ledger=ledger, catalog_keys=set())

    results = _publish_catalog_rows(
        rows=rows,
        args=SimpleNamespace(
            catalog_google_credentials="",
            google_credentials="",
            catalog_sheet_id="sheet-id",
            catalog_sheet_name="Mala_Evidence_v1",
        ),
        dry_run=True,
    )

    assert results[0]["action"] == "blocked_legacy_catalog_removed"
    assert "Mala_Evidence_v1" in results[0]["block_reason"]




def test_publication_guardrail_doc_names_mala_evidence_as_canonical() -> None:
    text = Path("docs/MALA_PUBLICATION_GUARDRAILS.md").read_text(encoding="utf-8")

    assert "Mala_Evidence_v1` is the only Mala-owned evidence publication tab" in text
    assert "Legacy `Strategy_Catalog` is historical/read-only" in text
    assert "--no-write" in text


def test_publish_pending_help_deprecates_strategy_catalog_and_points_to_mala_evidence(capsys) -> None:
    with pytest.raises(SystemExit):
        parse_args(["publish-pending", "--help"])

    help_text = capsys.readouterr().out
    assert "Mala_Evidence_v1" in help_text
    assert "Deprecated" in help_text
    assert "Actually upsert rows into Strategy_Catalog" not in help_text


def test_board_sync_plan_maps_terminal_states_to_operator_columns(tmp_path: Path) -> None:
    hypotheses = tmp_path / "research" / "hypotheses"
    runs = tmp_path / "runs"
    hypotheses.mkdir(parents=True)
    _write_hypothesis(hypotheses, hypothesis_id="expand30-amd-mi-01", state="completed", decision="promote")
    ledger = build_ledger(hypotheses_dir=hypotheses, runs_dir=runs)

    updates = _board_sync_plan(
        ledger=ledger,
        board_rows=[
            {
                "row_index": 2,
                "Task_ID": "SC-20260422-expand30-amd-mi-01",
                "Operator_Action": "APPROVED_RUN",
                "Agent_State": "APPROVED_M1",
                "Current_Stage": "M1",
                "Recommendation": "RUN_M1",
            }
        ],
    )

    assert len(updates) == 1
    assert updates[0]["Operator_Action"] == ""
    assert updates[0]["Agent_State"] == "PROMOTED"
    assert updates[0]["Current_Stage"] == "M5"
    assert updates[0]["Recommendation"] == "PROMOTE"


def test_stale_disposition_suppresses_matching_hot_start_finding(tmp_path: Path) -> None:
    hypotheses = tmp_path / "research" / "hypotheses"
    runs = tmp_path / "research" / "results" / "hypothesis_runs"
    hypotheses.mkdir(parents=True)
    _write_hypothesis(hypotheses, hypothesis_id="idea", state="running", decision="")
    ledger = build_ledger(
        hypotheses_dir=hypotheses,
        runs_dir=runs,
        dispositions=[
            FindingDisposition(
                created_at="2026-04-24T00:00:00+00:00",
                status="stale",
                key="idea",
                category="running_hypothesis",
                reason="intentional test disposition",
            )
        ],
    )

    assert not [finding for finding in ledger.findings if finding.key == "idea"]


def test_disposition_clear_restores_matching_finding(tmp_path: Path) -> None:
    hypotheses = tmp_path / "research" / "hypotheses"
    runs = tmp_path / "research" / "results" / "hypothesis_runs"
    hypotheses.mkdir(parents=True)
    _write_hypothesis(hypotheses, hypothesis_id="idea", state="running", decision="")
    dispositions = [
        FindingDisposition(
            created_at="2026-04-24T00:00:00+00:00",
            status="stale",
            key="idea",
            category="running_hypothesis",
            reason="stale",
        ),
        FindingDisposition(
            created_at="2026-04-24T01:00:00+00:00",
            status="cleared",
            key="idea",
            category="running_hypothesis",
            reason="bring it back",
        ),
    ]

    ledger = build_ledger(hypotheses_dir=hypotheses, runs_dir=runs, dispositions=dispositions)

    assert [finding.key for finding in ledger.findings] == ["idea"]


def test_append_and_read_dispositions_jsonl(tmp_path: Path) -> None:
    path = tmp_path / "dispositions.jsonl"

    append_disposition(
        path=path,
        key="idea/run",
        category="run_missing_summary",
        status="stale",
        reason="old run",
        operator="test",
    )

    rows = read_dispositions(path)
    assert len(rows) == 1
    assert rows[0].key == "idea/run"
    assert rows[0].category == "run_missing_summary"
    assert rows[0].status == "stale"


def test_program_status_writes_json_markdown_and_degrades_without_sheets(tmp_path: Path) -> None:
    hypotheses = tmp_path / "research" / "hypotheses"
    runs = tmp_path / "runs"
    out_dir = tmp_path / "out"
    vault = tmp_path / "vault"
    hypotheses.mkdir(parents=True)
    _write_hypothesis(hypotheses, hypothesis_id="pending-idea", state="pending", decision="")

    from src.research.research_ops import build_program_status, write_program_status

    args = SimpleNamespace(
        hypotheses_dir=str(hypotheses),
        runs_dir=str(runs),
        dispositions_path=str(tmp_path / "dispositions.jsonl"),
        out_dir=str(out_dir),
        with_catalog=False,
        with_board=False,
        with_control=False,
        with_intake=False,
        limit=50,
        vault=str(vault),
    )
    status = build_program_status(args)
    json_path, md_path = write_program_status(status, out_dir)

    assert json_path.exists()
    assert md_path.exists()
    assert status["summary"]["tags"]["needs_suman"] == 1
    assert any("control sheet not requested" in warning for warning in status["warnings"])
    assert "needs_suman" in md_path.read_text(encoding="utf-8")


def test_program_status_treats_empty_requested_sheets_as_available(tmp_path: Path, monkeypatch) -> None:
    hypotheses = tmp_path / "research" / "hypotheses"
    runs = tmp_path / "runs"
    out_dir = tmp_path / "out"
    vault = tmp_path / "vault"
    hypotheses.mkdir(parents=True)
    _write_hypothesis(hypotheses, hypothesis_id="pending-idea", state="pending", decision="")

    import src.research.research_ops as research_ops

    class EmptySheetClient:
        def read_rows(self, range_suffix: str) -> list[dict[str, str]]:
            assert range_suffix == "A1:ZZ5000"
            return []

    monkeypatch.setattr(research_ops, "_control_client", lambda args: EmptySheetClient())
    monkeypatch.setattr(research_ops, "_intake_client", lambda args: EmptySheetClient())

    args = SimpleNamespace(
        hypotheses_dir=str(hypotheses),
        runs_dir=str(runs),
        dispositions_path=str(tmp_path / "dispositions.jsonl"),
        out_dir=str(out_dir),
        with_catalog=False,
        with_board=False,
        with_control=True,
        with_intake=True,
        limit=50,
        vault=str(vault),
    )
    status = research_ops.build_program_status(args)

    assert status["summary"]["control_rows"] == 0
    assert status["summary"]["intake_rows"] == 0
    assert status["state"]["control"]["available"] is True
    assert status["state"]["intake"]["available"] is True
    assert not any("returned no rows" in warning for warning in status["warnings"])


def test_latest_shadow_brief_finds_current_numbered_vault_path(tmp_path: Path) -> None:
    import src.research.research_ops as research_ops

    vault = tmp_path / "vault"
    current = vault / "03 Agent Org" / "research_lab" / "Mala" / "Shadow" / "2026-05-18.md"
    legacy = vault / "areas" / "trading" / "mala-shadow" / "2026-05-17.md"
    current.parent.mkdir(parents=True)
    legacy.parent.mkdir(parents=True)
    legacy.write_text(
        "# Mala Shadow Decision Brief - 2026-05-17\n"
        "- decision_verdict: **YELLOW - stale legacy path**\n"
        "\n## Recommendation\n"
        "- Owner: legacy. Ignore this if a current note exists.\n",
        encoding="utf-8",
    )
    current.write_text(
        "# Mala Shadow Decision Brief - 2026-05-18\n"
        "- decision_verdict: **YELLOW - signal/provider concordance not clean**\n"
        "\n## Recommendation\n"
        "- Owner: data/adoption. Investigate mismatched same-bar replay.\n",
        encoding="utf-8",
    )

    shadow = research_ops._latest_shadow_brief(vault)

    assert shadow["path"] == str(current)
    assert shadow["verdict"] == "YELLOW - signal/provider concordance not clean"
    assert shadow["owner"] == "data/adoption"


def test_decision_cards_preserve_comments_and_limit_to_needs_suman(tmp_path: Path) -> None:
    from src.research.research_ops import COMMENTS_END, COMMENTS_START, write_decision_cards

    vault = tmp_path / "vault"
    status = {
        "items": [
            {"id": "run_m1:idea-a", "tag": "needs_suman", "title": "Run M1 A", "why": "Pending", "next": "Approve M1", "owner": "Suman", "source": "a.md"},
            {"id": "retune_plan:idea-b", "tag": "needs_suman", "title": "Retune B", "why": "Retune", "next": "Approve retune", "owner": "Suman", "source": "b.md"},
            {"id": "running:idea-c", "tag": "running", "title": "Running C", "why": "Running", "next": "Watch", "owner": "Research Ops", "source": "c.md"},
        ]
    }
    first = write_decision_cards(status, vault, limit=1)
    assert len(first) == 1
    path = Path(first[0]["path"])
    text = path.read_text(encoding="utf-8")
    relative_card_path = "03 Agent Org/research_lab/Mala/Research/Decision Cards/run_m1-idea-a.md"
    body = text.split("---\n", 2)[2]
    assert "generated_at:" in text
    assert "- generated_at: `" in body
    assert f"canonical_card_path: '{path.resolve()}'" in text
    assert str(path.resolve()) not in body
    assert "/Users/sunny/Library/" not in body
    assert "- canonical_card_path:" not in body
    assert "edit_this_file: yes" in text
    assert "Review decisions are read from this card" in body
    assert f"- card: `{relative_card_path}`" in body
    path.write_text(text.replace(f"{COMMENTS_START}\n-\n{COMMENTS_END}", f"{COMMENTS_START}\n- human note\n{COMMENTS_END}"), encoding="utf-8")

    second = write_decision_cards(status, vault, limit=1)
    updated_text = path.read_text(encoding="utf-8")
    updated_body = updated_text.split("---\n", 2)[2]
    assert second[0]["action"] == "unchanged"
    assert "- human note" in updated_text
    assert "- generated_at: `" in updated_body
    assert str(path.resolve()) not in updated_body
    assert "/Users/sunny/Library/" not in updated_body
    assert "- canonical_card_path:" not in updated_body
    assert f"- card: `{relative_card_path}`" in updated_body
    assert len(list((vault / "03 Agent Org" / "research_lab" / "Mala" / "Research" / "Decision Cards").glob("*.md"))) == 1

def test_retune_batch_decision_card_includes_brief_story_and_evidence(tmp_path: Path) -> None:
    from src.research.research_ops import COMMENTS_END, COMMENTS_START, RECEIPT_END, RECEIPT_START, write_decision_cards

    vault = tmp_path / "vault"
    status = {
        "items": [
            {
                "id": "retune_plan:idea-a",
                "tag": "needs_suman",
                "title": "retune_plan: idea-a",
                "why": "Retune needs judgment",
                "next": "Approve bounded retune",
                "owner": "Suman",
                "source": "research/hypotheses/idea-a.md",
                "action_type": "retune_plan",
                "key": "idea-a",
                "brief": {
                    "title": "Idea A Opening Drive",
                    "hypothesis_id": "idea-a",
                    "strategy": "Opening Drive Classifier",
                    "symbol_scope": "SPY",
                    "stage": "M1",
                    "decision": "retune",
                    "thesis": "SPY may continue after a clean opening drive.",
                    "latest_artifact": "research/results/hypothesis_runs/idea-a/2026-05-01T000000",
                    "metrics": ["M1 rows=4; best SPY combined exp_r=0.2232 pct_pos=1.00 signals=16"],
                    "suggested_operator_action": "APPROVE_RETUNE",
                    "recommendation_reason": "M1 still has positive candidates; run the bounded retune plan.",
                    "confidence": "medium",
                    "sources": ["research/hypotheses/idea-a.md", "research/results/hypothesis_runs/idea-a/2026-05-01T000000"],
                },
            },
            {
                "id": "retune_plan:idea-b",
                "tag": "needs_suman",
                "title": "retune_plan: idea-b",
                "why": "Retune needs judgment",
                "next": "Approve bounded retune",
                "owner": "Suman",
                "source": "research/hypotheses/idea-b.md",
                "action_type": "retune_plan",
                "key": "idea-b",
                "brief": {
                    "title": "Idea B Market Impulse",
                    "hypothesis_id": "idea-b",
                    "strategy": "Market Impulse (Cross & Reclaim)",
                    "symbol_scope": "QQQ",
                    "stage": "M1",
                    "decision": "retune",
                    "thesis": "Velocity and acceleration may identify continuation pockets.",
                    "latest_artifact": "research/results/hypothesis_runs/idea-b/2026-05-01T000000",
                    "metrics": ["M1 rows=2; best QQQ short exp_r=0.1568 pct_pos=0.50 signals=33"],
                    "suggested_operator_action": "APPROVE_RETUNE",
                    "recommendation_reason": "M1 still has positive candidates; run the bounded retune plan.",
                    "confidence": "medium",
                    "sources": ["research/hypotheses/idea-b.md"],
                },
            },
        ]
    }
    first = write_decision_cards(status, vault, limit=1)
    path = Path(first[0]["path"])
    text = path.read_text(encoding="utf-8")
    assert "## Executive Summary" in text
    assert "## Decision Data That Matters" in text
    assert "## Research Detail" in text
    assert "### Evidence Snapshot" in text
    assert "## Recommendation" in text
    assert "## Appendix: Source Artifacts" in text
    assert "Recommendation:" in text
    assert "Decision needed:" in text
    assert "Permission boundary:" in text
    assert "SPY may continue after a clean opening drive" in text
    assert "M1 rows=4" in text
    assert "APPROVE_RETUNE" in text
    assert "approve subset with comments" in text

    path.write_text(
        text.replace(f"{COMMENTS_START}\n-\n{COMMENTS_END}", f"{COMMENTS_START}\n- keep idea-a only\n{COMMENTS_END}")
        .replace(f"{RECEIPT_START}\n- pending\n{RECEIPT_END}", f"{RECEIPT_START}\n- prior receipt\n{RECEIPT_END}"),
        encoding="utf-8",
    )
    second = write_decision_cards(status, vault, limit=1)
    updated = path.read_text(encoding="utf-8")
    assert second[0]["action"] == "unchanged"
    assert "- keep idea-a only" in updated
    assert "- prior receipt" in updated



def _m1_row(
    *,
    ticker: str = "ABC",
    direction: str = "combined",
    exp_r: str = "0.20",
    windows: str = "3",
    signals: str = "60",
    pct_pos: str = "0.75",
    score: str = "10",
) -> dict[str, str]:
    return {
        "ticker": ticker,
        "strategy": "Synthetic Strategy",
        "direction": direction,
        "oos_windows": windows,
        "oos_signals": signals,
        "avg_test_exp_r": exp_r,
        "pct_positive_oos_windows": pct_pos,
        "avg_test_confidence": "0.55",
        "avg_test_mfe_mae_ratio": "1.80",
        "m1_score": score,
        "lookback": "20",
    }


def test_score_researcher_verdict_approves_surface_for_robust_synthetic_evidence() -> None:
    rows = [
        _m1_row(ticker="AAA", exp_r="0.22", windows="4", signals="90", score="12"),
        _m1_row(ticker="BBB", exp_r="0.16", windows="3", signals="75", score="10"),
        _m1_row(ticker="CCC", exp_r="0.11", windows="3", signals="55", score="9"),
    ]

    verdict = score_researcher_verdict(
        hypothesis_metadata={"title": "Synthetic continuation basket", "thesis": "Continuation after impulse."},
        m1_top_rows=rows,
        m1_aggregate_rows=rows,
        m1_detail_rows=rows,
    )

    assert verdict["recommendation"] == "approve_surface_expansion"
    assert verdict["priority"] == "high"
    assert verdict["evidence_quality"] == "strong"
    assert verdict["stability_read"]["robust_config_count"] == 3


def test_score_researcher_verdict_fragile_positive_gets_bounded_retune() -> None:
    verdict = score_researcher_verdict(
        hypothesis_metadata={"title": "Synthetic open auction continuation"},
        m1_top_rows=[_m1_row(exp_r="0.31", windows="1", signals="18")],
        m1_aggregate_rows=[_m1_row(exp_r="0.31", windows="1", signals="18")],
    )

    assert verdict["recommendation"] == "approve_bounded_retune"
    assert {"thin_signals", "one_oos_window"}.issubset(verdict["fragility_flags"])


def test_score_researcher_verdict_defers_weak_sparse_expectancy() -> None:
    verdict = score_researcher_verdict(
        hypothesis_metadata={"title": "Synthetic weak edge"},
        m1_top_rows=[_m1_row(exp_r="0.04", windows="3", signals="80")],
        m1_aggregate_rows=[_m1_row(exp_r="0.04", windows="3", signals="80")],
    )

    assert verdict["recommendation"] == "defer_for_better_evidence"
    assert "weak_expectancy" in verdict["fragility_flags"]


def test_score_researcher_verdict_rejects_zero_or_negative_evidence() -> None:
    verdict = score_researcher_verdict(
        hypothesis_metadata={"title": "Synthetic dead end"},
        m1_top_rows=[_m1_row(exp_r="-0.03", windows="3", signals="100")],
        m1_aggregate_rows=[_m1_row(exp_r="-0.03", windows="3", signals="100")],
    )

    assert verdict["recommendation"] == "reject_or_kill"


def test_score_researcher_verdict_closes_explicit_smoke_metadata() -> None:
    verdict = score_researcher_verdict(
        hypothesis_metadata={"title": "Pipeline smoke validation", "purpose": "plumbing smoke test"},
        m1_top_rows=[_m1_row(exp_r="0.40", windows="1", signals="12")],
    )

    assert verdict["recommendation"] == "close_as_smoke_test"
    assert verdict["evidence_quality"] == "smoke_test"
    assert "smoke_test" in verdict["fragility_flags"]


def test_score_researcher_verdict_flags_aggregate_disagreement_from_rows() -> None:
    verdict = score_researcher_verdict(
        hypothesis_metadata={"title": "Synthetic disagreement"},
        m1_top_rows=[_m1_row(exp_r="0.28", windows="1", signals="20")],
        m1_aggregate_rows=[_m1_row(exp_r="-0.06", windows="3", signals="75")],
    )

    assert "aggregate_disagrees" in verdict["fragility_flags"]
    assert verdict["stability_read"]["broader_sample_disagreement"] is True


def test_researcher_scoring_path_has_no_current_batch_identity_branches() -> None:
    import inspect
    from src.research import research_ops

    source = "\n".join(
        inspect.getsource(obj)
        for obj in [
            research_ops.score_researcher_verdict,
            research_ops._researcher_recommendation,
            research_ops._fragility_flags,
        ]
    ).lower()
    forbidden = [
        "market-impulse-current-basket-discovery",
        "expand30-w1-b2-p2-msft-opening-drive",
        "opening-drive-v2",
        "msft",
        "xlf",
        "tlt",
        "kinematic",
    ]
    assert not any(token in source for token in forbidden)


def test_researcher_verdicts_split_high_value_retunes_and_summary_defer(tmp_path: Path) -> None:
    from src.research.research_ops import write_decision_cards

    vault = tmp_path / "vault"

    def item(key: str, title: str, recommendation: str, priority: str, quality: str = "weak", strategy: str = "Synthetic Strategy") -> dict[str, object]:
        return {
            "id": f"retune_plan:{key}",
            "tag": "needs_suman",
            "title": f"retune_plan: {key}",
            "why": "Retune needs judgment",
            "next": "Review researcher verdict",
            "owner": "Suman",
            "source": f"research/hypotheses/{key}.md",
            "action_type": "retune_plan",
            "key": key,
            "brief": {
                "title": title,
                "hypothesis_id": key,
                "strategy": strategy,
                "symbol_scope": "SPY",
                "stage": "M1",
                "decision": "retune",
                "thesis": "A differentiated thesis exists.",
                "latest_artifact": f"research/results/hypothesis_runs/{key}/2026-05-01T000000",
                "metrics": ["M1 rows=4; best SPY combined exp_r=0.2232 pct_pos=1.00 signals=16"],
                "researcher_verdict": {
                    "recommendation": recommendation,
                    "priority": priority,
                    "evidence_quality": quality,
                    "rationale": f"{title} rationale.",
                    "fragility_flags": ["thin_signals"] if quality != "smoke_test" else ["smoke_test", "thin_signals"],
                    "best_config": {"ticker": "SPY", "direction": "combined", "avg_test_exp_r": "0.2232", "pct_positive_oos_windows": "1.00", "oos_signals": "16"},
                    "stability_read": {"positive_config_count": 2, "robust_config_count": 0, "broader_sample_disagreement": True, "notes": "broader rows disagree"},
                },
                "sources": [f"research/hypotheses/{key}.md"],
            },
        }

    status = {
        "items": [
            item("robust-surface-candidate", "Robust Surface Candidate", "approve_surface_expansion", "high", "medium"),
            item("fragile-diagnostic-candidate", "Fragile Diagnostic Candidate", "approve_bounded_retune", "medium"),
            item("weak-defer-candidate", "Weak Defer Candidate", "defer_for_better_evidence", "low"),
            item("explicit-smoke-candidate", "Explicit Smoke Candidate", "close_as_smoke_test", "low", "smoke_test"),
        ]
    }

    cards = write_decision_cards(status, vault, limit=3)
    ids = [card["id"] for card in cards]
    assert "retune_plan-robust-surface-candidate" in ids
    assert "retune_plan-fragile-diagnostic-candidate" in ids
    assert "retune_plan-defer-summary-needs-suman" in ids

    summary = Path(next(card["path"] for card in cards if card["id"] == "retune_plan-defer-summary-needs-suman")).read_text(encoding="utf-8")
    assert "## Executive Summary" in summary
    assert "Decision needed: accept the defer/close recommendation" in summary
    assert "## Decision Data That Matters" in summary
    assert "attention-managed retune summary" in summary
    assert "Researcher Verdict: `defer_for_better_evidence`" in summary
    assert "Researcher Verdict: `close_as_smoke_test`" in summary
    assert "approve subset with comments" in summary


def test_program_status_researcher_verdict_flags_fragile_smoke_and_one_window(tmp_path: Path) -> None:
    from src.research.research_ops import build_program_status

    hypotheses = tmp_path / "research" / "hypotheses"
    runs = tmp_path / "research" / "results" / "hypothesis_runs"
    hypotheses.mkdir(parents=True)
    hyp_path = _write_hypothesis(
        hypotheses,
        hypothesis_id="spy-opening-drive-m1-smoke",
        state="retune",
        decision="retune",
        strategy="Opening Drive Classifier",
        symbol_scope="SPY",
    )
    hyp_path.write_text(
        hyp_path.read_text(encoding="utf-8") + "\n\n## Thesis\nPipeline smoke test for research plumbing.\n",
        encoding="utf-8",
    )
    run_dir = runs / "spy-opening-drive-m1-smoke" / "2026-05-01T000000"
    run_dir.mkdir(parents=True)
    (run_dir / "RUN_SUMMARY.md").write_text("- decision: `retune`\n\n## Notes\n\n- M1 FAIL: signals=16<50; windows=1<3\n", encoding="utf-8")
    _write_csv(run_dir / "M1_top.csv", [{"ticker": "SPY", "strategy": "Opening Drive Classifier", "direction": "combined", "oos_windows": "1", "oos_signals": "16", "avg_test_exp_r": "0.2232", "pct_positive_oos_windows": "1.0", "avg_test_confidence": "0.4375", "avg_test_mfe_mae_ratio": "14.1082", "m1_score": "10", "opening_window_minutes": "20"}])
    _write_csv(run_dir / "M1_aggregate.csv", [{"ticker": "SPY", "strategy": "Opening Drive Classifier", "direction": "combined", "oos_windows": "3", "oos_signals": "54", "avg_test_exp_r": "-0.161", "pct_positive_oos_windows": "0.0"}])
    _write_csv(run_dir / "M1_detail.csv", [{"ticker": "SPY", "strategy": "Opening Drive Classifier", "direction": "combined", "window_idx": "1", "test_signals": "16", "test_exp_r": "0.2232", "effective_cost_r": "0.01", "opening_window_minutes": "20"}])
    args = SimpleNamespace(
        hypotheses_dir=str(hypotheses),
        runs_dir=str(runs),
        dispositions_path=str(tmp_path / "dispositions.jsonl"),
        out_dir=str(tmp_path / "out"),
        with_catalog=False,
        with_board=False,
        with_control=False,
        with_intake=False,
        limit=50,
        vault=str(tmp_path / "vault"),
    )

    status = build_program_status(args)
    item = next(item for item in status["items"] if item["key"] == "spy-opening-drive-m1-smoke")
    verdict = item["brief"]["researcher_verdict"]

    assert verdict["recommendation"] == "close_as_smoke_test"
    assert verdict["evidence_quality"] == "smoke_test"
    assert {"smoke_test", "thin_signals", "one_oos_window", "aggregate_disagrees"}.issubset(verdict["fragility_flags"])
    assert verdict["best_config"]["ticker"] == "SPY"


def test_program_status_reads_kinematic_m1_schema_without_missing_artifact(tmp_path: Path) -> None:
    from src.research.research_ops import build_program_status

    hypotheses = tmp_path / "research" / "hypotheses"
    runs = tmp_path / "research" / "results" / "hypothesis_runs"
    hypotheses.mkdir(parents=True)
    hyp_path = _write_hypothesis(
        hypotheses,
        hypothesis_id="market-impulse-current-basket-discovery",
        state="retune",
        decision="retune",
        strategy="Market Impulse (Cross & Reclaim)",
        symbol_scope="SPY, QQQ, IWM, AAPL, AMD, META, NVDA, PLTR, TSLA",
    )
    hyp_path.write_text(
        hyp_path.read_text(encoding="utf-8")
        + "\n\n## Thesis\nIntraday continuation should improve when market impulse reclaims hold after VWMA pierces.\n",
        encoding="utf-8",
    )
    run_dir = runs / "market-impulse-current-basket-discovery" / "2026-05-10T070455"
    run_dir.mkdir(parents=True)
    (run_dir / "RUN_SUMMARY.md").write_text(
        "- decision: `retune`\n\n## Notes\n\n- M1 FAIL: top candidate signals=17<50; windows=1<3\n",
        encoding="utf-8",
    )
    rows = [
        {
            "ticker": "AMD",
            "strategy": "Market Impulse (Cross & Reclaim)",
            "direction": "combined",
            "oos_windows": "4",
            "oos_signals": "96",
            "avg_test_exp_r": "0.3169",
            "pct_positive_oos_windows": "0.8",
            "avg_test_confidence": "0.61",
            "avg_test_mfe_mae_ratio": "6.2",
            "regime_window": "45",
            "accel_window": "10",
            "kinematic_periods_back": "3",
            "use_volume_filter": "true",
            "volume_multiplier": "1.0",
            "m1_score": "516.9",
        },
        {
            "ticker": "META",
            "strategy": "Market Impulse (Cross & Reclaim)",
            "direction": "combined",
            "oos_windows": "4",
            "oos_signals": "89",
            "avg_test_exp_r": "0.1895",
            "pct_positive_oos_windows": "0.8",
            "avg_test_confidence": "0.58",
            "avg_test_mfe_mae_ratio": "5.1",
            "regime_window": "30",
            "accel_window": "20",
            "kinematic_periods_back": "3",
            "use_volume_filter": "true",
            "volume_multiplier": "1.1",
            "m1_score": "389.5",
        },
        {
            "ticker": "NVDA",
            "strategy": "Market Impulse (Cross & Reclaim)",
            "direction": "combined",
            "oos_windows": "3",
            "oos_signals": "72",
            "avg_test_exp_r": "0.1412",
            "pct_positive_oos_windows": "0.67",
            "avg_test_confidence": "0.55",
            "avg_test_mfe_mae_ratio": "4.8",
            "regime_window": "45",
            "accel_window": "15",
            "kinematic_periods_back": "2",
            "use_volume_filter": "true",
            "volume_multiplier": "1.0",
            "m1_score": "341.2",
        },
        {
            "ticker": "SPY",
            "strategy": "Market Impulse (Cross & Reclaim)",
            "direction": "combined",
            "oos_windows": "4",
            "oos_signals": "63",
            "avg_test_exp_r": "-0.3369",
            "pct_positive_oos_windows": "0.0",
            "avg_test_confidence": "0.35",
            "avg_test_mfe_mae_ratio": "9.2",
            "regime_window": "20",
            "accel_window": "10",
            "kinematic_periods_back": "1",
            "use_volume_filter": "true",
            "volume_multiplier": "1.0",
            "m1_score": "-236.9",
        },
    ]
    _write_csv(run_dir / "M1_top.csv", rows)
    _write_csv(run_dir / "M1_aggregate.csv", rows)
    _write_csv(
        run_dir / "M1_detail.csv",
        [
            {
                "ticker": "AMD",
                "strategy": "Market Impulse (Cross & Reclaim)",
                "direction": "combined",
                "window_idx": "1",
                "test_signals": "24",
                "test_confidence": "0.61",
                "test_exp_r": "0.3169",
                "test_avg_mfe_mae_ratio": "6.2",
                "effective_cost_r": "0.05",
                "regime_window": "45",
                "accel_window": "10",
                "kinematic_periods_back": "3",
                "use_volume_filter": "true",
                "volume_multiplier": "1.0",
            }
        ],
    )
    args = SimpleNamespace(
        hypotheses_dir=str(hypotheses),
        runs_dir=str(runs),
        dispositions_path=str(tmp_path / "dispositions.jsonl"),
        out_dir=str(tmp_path / "out"),
        with_catalog=False,
        with_board=False,
        with_control=False,
        with_intake=False,
        limit=50,
        vault=str(tmp_path / "vault"),
    )

    status = build_program_status(args)
    item = next(item for item in status["items"] if item["key"] == "market-impulse-current-basket-discovery")
    brief = item["brief"]
    verdict = brief["researcher_verdict"]

    assert brief["latest_artifact"].endswith("2026-05-10T070455")
    assert "M1 evidence not found" not in brief["metrics"]
    assert verdict["recommendation"] == "approve_surface_expansion"
    assert verdict["evidence_quality"] == "strong"
    assert "missing_artifact" not in verdict["fragility_flags"]
    assert verdict["best_config"]["ticker"] == "AMD"
    assert verdict["stability_read"]["robust_config_count"] == 3


def test_control_rows_use_researcher_verdict_for_smoke_retune(tmp_path: Path) -> None:
    hypotheses = tmp_path / "research" / "hypotheses"
    runs = tmp_path / "research" / "results" / "hypothesis_runs"
    hypotheses.mkdir(parents=True)
    hyp_path = _write_hypothesis(
        hypotheses,
        hypothesis_id="spy-opening-drive-m1-smoke",
        state="retune",
        decision="retune",
        strategy="Opening Drive Classifier",
        symbol_scope="SPY",
    )
    hyp_path.write_text(
        hyp_path.read_text(encoding="utf-8") + "\n\n## Thesis\nPipeline smoke test for research plumbing.\n",
        encoding="utf-8",
    )
    run_dir = runs / "spy-opening-drive-m1-smoke" / "2026-05-01T000000"
    run_dir.mkdir(parents=True)
    (run_dir / "RUN_SUMMARY.md").write_text(
        "- decision: `retune`\n\n## Notes\n\n- M1 FAIL: signals=16<50; windows=1<3\n",
        encoding="utf-8",
    )
    _write_csv(
        run_dir / "M1_top.csv",
        [
            {
                "ticker": "SPY",
                "strategy": "Opening Drive Classifier",
                "direction": "combined",
                "oos_windows": "1",
                "oos_signals": "16",
                "avg_test_exp_r": "0.2232",
                "pct_positive_oos_windows": "1.0",
                "avg_test_confidence": "0.4375",
                "avg_test_mfe_mae_ratio": "14.1082",
                "m1_score": "10",
                "opening_window_minutes": "20",
            }
        ],
    )
    _write_csv(
        run_dir / "M1_aggregate.csv",
        [
            {
                "ticker": "SPY",
                "strategy": "Opening Drive Classifier",
                "direction": "combined",
                "oos_windows": "3",
                "oos_signals": "54",
                "avg_test_exp_r": "-0.161",
                "pct_positive_oos_windows": "0.0",
            }
        ],
    )
    ledger = build_ledger(hypotheses_dir=hypotheses, runs_dir=runs)
    brief = build_action_brief(ledger=ledger, key="retune_plan:spy-opening-drive-m1-smoke")
    rows = build_control_rows(
        actions=build_next_actions(ledger),
        generated_at="2026-05-10T00:00:00+00:00",
        existing_rows=[{"action_id": "retune_plan:spy-opening-drive-m1-smoke", "status": "queued"}],
        ledger=ledger,
    )
    row = next(item for item in rows if item["action_id"] == "retune_plan:spy-opening-drive-m1-smoke")

    assert brief.recommendation == "CLOSE_SMOKE_TEST"
    assert brief.suggested_operator_action == "SKIP"
    assert row["recommendation"] == "CLOSE_SMOKE_TEST"
    assert row["recommended_operator_action"] == "SKIP"
    assert row["brief_recommendation"] == "CLOSE_SMOKE_TEST"


def test_ingest_review_decisions_maps_checked_individual_card_to_control_action(tmp_path: Path) -> None:
    hypotheses = tmp_path / "research" / "hypotheses"
    runs = tmp_path / "research" / "results" / "hypothesis_runs"
    vault = tmp_path / "vault"
    card_dir = vault / "03 Agent Org" / "research_lab" / "Mala" / "Research" / "Decision Cards"
    hypotheses.mkdir(parents=True)
    card_dir.mkdir(parents=True)
    _write_hypothesis(
        hypotheses,
        hypothesis_id="market-impulse-current-basket-discovery",
        state="retune",
        decision="retune",
        strategy="Market Impulse (Cross & Reclaim)",
        symbol_scope="SPY",
    )
    run_dir = runs / "market-impulse-current-basket-discovery" / "2026-05-01T000000"
    run_dir.mkdir(parents=True)
    (run_dir / "RUN_SUMMARY.md").write_text("- decision: `retune`\n", encoding="utf-8")
    rows = [
        {
            "ticker": "SPY",
            "strategy": "Market Impulse (Cross & Reclaim)",
            "direction": "combined",
            "oos_windows": "4",
            "oos_signals": "90",
            "avg_test_exp_r": "0.22",
            "pct_positive_oos_windows": "0.75",
            "m1_score": "12",
        },
        {
            "ticker": "QQQ",
            "strategy": "Market Impulse (Cross & Reclaim)",
            "direction": "combined",
            "oos_windows": "3",
            "oos_signals": "80",
            "avg_test_exp_r": "0.16",
            "pct_positive_oos_windows": "0.67",
            "m1_score": "10",
        },
        {
            "ticker": "IWM",
            "strategy": "Market Impulse (Cross & Reclaim)",
            "direction": "combined",
            "oos_windows": "3",
            "oos_signals": "65",
            "avg_test_exp_r": "0.11",
            "pct_positive_oos_windows": "0.67",
            "m1_score": "9",
        },
    ]
    _write_csv(run_dir / "M1_top.csv", rows)
    _write_csv(run_dir / "M1_aggregate.csv", rows)
    card = card_dir / "retune_plan-market-impulse-current-basket-discovery.md"
    card.write_text(
        "\n".join(
            [
                "---",
                "card_id: retune_plan-market-impulse-current-basket-discovery",
                "program_id: mala_next_gen_research_ops_flow",
                "---",
                "",
                "# Mala Decision Card: retune_plan: market-impulse-current-basket-discovery",
                "",
                "## Decision",
                "- [x] approve",
                "- [ ] reject",
                "- [ ] defer",
                "",
                "## Comments",
                "<!-- mala-card-comments:start -->",
                "- approve this one",
                "<!-- mala-card-comments:end -->",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    ledger = build_ledger(hypotheses_dir=hypotheses, runs_dir=runs)

    records = build_review_decision_records(vault=vault, ledger=ledger)

    assert len(records) == 1
    assert records[0]["status"] == "ready"
    assert records[0]["action_id"] == "retune_plan:market-impulse-current-basket-discovery"
    assert records[0]["operator_action"] == "APPROVE_SURFACE_EXPANSION"
    assert records[0]["comments"] == "- approve this one"


def test_apply_review_decision_records_updates_operator_and_recommendation_columns() -> None:
    client = _FakeControlClient()
    client.rows = [
        {
            "row_index": 2,
            "action_id": "retune_plan:spy-opening-drive-m1-smoke",
            "recommendation": "SURFACE_EXPANSION_REVIEW",
            "recommended_operator_action": "APPROVE_SURFACE_EXPANSION",
            "operator_action": "",
            "decision_needed": "",
        }
    ]
    records = [
        {
            "status": "ready",
            "action_id": "retune_plan:spy-opening-drive-m1-smoke",
            "operator_action": "SKIP",
            "recommendation": "CLOSE_SMOKE_TEST",
            "recommended_operator_action": "SKIP",
            "decision_needed": "Choose SKIP in Research_Control.operator_action to apply; leave blank to defer.",
        }
    ]

    applied = apply_review_decision_records(client, records)

    assert applied == 1
    assert records[0]["status"] == "applied"
    assert client.rows[0]["operator_action"] == "SKIP"
    assert client.rows[0]["recommendation"] == "CLOSE_SMOKE_TEST"
    assert client.rows[0]["recommended_operator_action"] == "SKIP"
    assert "Choose SKIP" in client.rows[0]["decision_needed"]
    assert client.columns == ["recommendation", "recommended_operator_action", "operator_action", "decision_needed", "updated_at"]
