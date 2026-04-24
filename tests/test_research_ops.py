from __future__ import annotations

import csv
from pathlib import Path

from src.research.research_ops import (
    _board_sync_plan,
    _catalog_publish_plan,
    build_hot_start_findings,
    build_ledger,
    build_next_actions,
    write_csv_tables,
    write_hot_start_report,
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
