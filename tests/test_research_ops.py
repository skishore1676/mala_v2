from __future__ import annotations

import csv
from pathlib import Path

from src.research.research_ops import (
    _board_sync_plan,
    _catalog_publish_plan,
    FindingDisposition,
    append_disposition,
    build_action_brief,
    build_control_rows,
    build_hot_start_findings,
    build_ledger,
    build_next_actions,
    build_surface_expansion_plan,
    read_dispositions,
    update_control_row_with_brief,
    update_control_row_with_surface_plan,
    write_action_brief,
    write_csv_tables,
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
    runs = tmp_path / "data" / "results" / "hypothesis_runs"
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
    runs = tmp_path / "data" / "results" / "hypothesis_runs"
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


def test_next_actions_rank_publish_and_retune_work(tmp_path: Path) -> None:
    hypotheses = tmp_path / "research" / "hypotheses"
    runs = tmp_path / "data" / "results" / "hypothesis_runs"
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


def test_action_brief_recommends_retune_after_m2_exp_positive_instability(tmp_path: Path) -> None:
    hypotheses = tmp_path / "research" / "hypotheses"
    runs = tmp_path / "data" / "results" / "hypothesis_runs"
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
    runs = tmp_path / "data" / "results" / "hypothesis_runs"
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
    runs = tmp_path / "data" / "results" / "hypothesis_runs"
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
    assert plan.next_operator_action == "APPROVE_RETUNE"
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
        next_operator_action="APPROVE_RETUNE",
        summary="Expand one bounded parameter family.",
        proposed_bounds=[],
        rationale=[],
        validation_steps=[],
        sources=[],
        report_path="surface.md",
        json_path="surface.json",
    )

    assert update_control_row_with_surface_plan(client=client, plan=plan)
    assert client.rows[0]["brief_recommendation"] == "CONFIG_ONLY_SURFACE_EXPANSION"
    assert client.rows[0]["brief_summary"] == "Expand one bounded parameter family."
    assert client.rows[0]["brief_path"] == "surface.md"
    assert client.rows[0]["status"] == "surface_plan_ready"


def test_catalog_publish_plan_uses_latest_missing_promoted_rows(tmp_path: Path) -> None:
    hypotheses = tmp_path / "research" / "hypotheses"
    runs = tmp_path / "data" / "results" / "hypothesis_runs"
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
    runs = tmp_path / "data" / "results" / "hypothesis_runs"
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
    runs = tmp_path / "data" / "results" / "hypothesis_runs"
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
