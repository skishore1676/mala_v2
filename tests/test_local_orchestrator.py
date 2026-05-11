from __future__ import annotations

from pathlib import Path

from src.research import local_orchestrator
from src.research.local_orchestrator import _command_for_control_row, _record_control_skip, parse_args, run_once
from src.research.research_ops import read_dispositions


def _write_hypothesis(root: Path, hypothesis_id: str, state: str = "retune") -> None:
    path = root / f"{hypothesis_id}.md"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(
            [
                "# Hypothesis",
                "## Config",
                f"- id: `{hypothesis_id}`",
                f"- state: `{state}`",
                f"- decision: `{state}`",
                "- symbol_scope: `AMD`",
                "- strategy: `Market Impulse (Cross & Reclaim)`",
                "- max_stage: `M5`",
                "- last_run: ``",
                "",
                "## Agent Report",
                "Pending.",
            ]
        ),
        encoding="utf-8",
    )


def test_orchestrator_once_dry_run_writes_reasoning_brief(tmp_path: Path) -> None:
    hypotheses = tmp_path / "hypotheses"
    runs = tmp_path / "runs"
    out_dir = tmp_path / "orch"
    _write_hypothesis(hypotheses, "retune-me")

    args = parse_args(
        [
            "once",
            "--hypotheses-dir",
            str(hypotheses),
            "--runs-dir",
            str(runs),
            "--orchestrator-out-dir",
            str(out_dir),
        ]
    )
    result = run_once(args)

    assert result.executed == "planned"
    assert "retune-plan" in result.command
    assert "Agent reasoning checkpoint" in result.reasoning_brief
    assert Path(result.report_path).exists()
    assert Path(result.json_path).exists()


def test_orchestrator_apply_safe_runs_retune_plan(tmp_path: Path) -> None:
    hypotheses = tmp_path / "hypotheses"
    runs = tmp_path / "runs"
    out_dir = tmp_path / "orch"
    _write_hypothesis(hypotheses, "retune-me")

    args = parse_args(
        [
            "once",
            "--mode",
            "apply-safe",
            "--hypotheses-dir",
            str(hypotheses),
            "--runs-dir",
            str(runs),
            "--orchestrator-out-dir",
            str(out_dir),
        ]
    )
    result = run_once(args)

    assert result.executed == "ran_safe_command"
    assert result.returncode == 0
    assert "RETUNE_PLAN_FOR=" in result.stdout_tail


def test_orchestrator_with_control_sheet_does_not_autorun_without_operator_action(
    tmp_path: Path,
    monkeypatch,
) -> None:
    hypotheses = tmp_path / "hypotheses"
    runs = tmp_path / "runs"
    out_dir = tmp_path / "orch"
    _write_hypothesis(hypotheses, "retune-me")

    class FakeControlClient:
        def __init__(self) -> None:
            self.rows: list[dict[str, object]] = []

        def ensure_sheet_exists(self) -> None:
            return None

        def read_rows(self, *, range_suffix: str = "A1:ZZ5000") -> list[dict[str, object]]:
            return self.rows

        def overwrite_table(self, *, headers: list[str], rows: list[dict[str, object]]) -> None:
            self.rows = rows

    monkeypatch.setattr(local_orchestrator, "_control_client", lambda args: FakeControlClient())
    args = parse_args(
        [
            "once",
            "--mode",
            "apply-safe",
            "--with-control-sheet",
            "--hypotheses-dir",
            str(hypotheses),
            "--runs-dir",
            str(runs),
            "--orchestrator-out-dir",
            str(out_dir),
        ]
    )

    result = run_once(args)

    assert result.executed == "blocked_for_approval"
    assert "RETUNE_PLAN_FOR=" not in result.stdout_tail


def test_control_row_approval_maps_to_retune_command(tmp_path: Path) -> None:
    hypotheses = tmp_path / "hypotheses"
    _write_hypothesis(hypotheses, "retune-me")
    args = parse_args(["once", "--hypotheses-dir", str(hypotheses)])

    command = _command_for_control_row(
        {
            "operator_action": "APPROVE_RETUNE",
            "rank": "1",
            "priority": "medium",
            "action_type": "retune_plan",
            "key": "retune-me",
            "reason": "test",
            "suggested_command": "",
        },
        args,
    )

    assert command is not None
    assert command[2:] == [
        "src.research.research_runner",
        "retune-approved",
        "--hypothesis",
        str(hypotheses / "retune-me.md"),
    ]


def test_control_row_approval_maps_to_run_m1_command(tmp_path: Path) -> None:
    hypotheses = tmp_path / "hypotheses"
    _write_hypothesis(hypotheses, "pending-me", state="pending")
    args = parse_args(["once", "--hypotheses-dir", str(hypotheses)])

    command = _command_for_control_row(
        {
            "operator_action": "APPROVE_RUN_M1",
            "rank": "1",
            "priority": "medium",
            "action_type": "run_m1",
            "key": "pending-me",
            "reason": "operator approved first run",
            "suggested_command": "",
        },
        args,
    )

    assert command is not None
    assert command[2:] == [
        "src.research.research_runner",
        "run-m1",
        "--hypothesis",
        str(hypotheses / "pending-me.md"),
    ]


def test_control_row_approval_maps_to_continue_m2_command(tmp_path: Path) -> None:
    hypotheses = tmp_path / "hypotheses"
    _write_hypothesis(hypotheses, "running-me", state="running")
    args = parse_args(["once", "--hypotheses-dir", str(hypotheses)])

    command = _command_for_control_row(
        {
            "operator_action": "APPROVE_CONTINUE_M2",
            "rank": "1",
            "priority": "high",
            "action_type": "resume_or_normalize",
            "key": "running-me",
            "reason": "operator approved M2 continuation",
            "suggested_command": "",
        },
        args,
    )

    assert command is not None
    assert command[2:] == [
        "src.research.research_runner",
        "continue-approved",
        "--hypothesis",
        str(hypotheses / "running-me.md"),
        "--max-stage",
        "M2",
    ]


def test_control_row_approval_maps_to_kill_command(tmp_path: Path) -> None:
    hypotheses = tmp_path / "hypotheses"
    _write_hypothesis(hypotheses, "retune-me")
    args = parse_args(["once", "--hypotheses-dir", str(hypotheses)])

    command = _command_for_control_row(
        {
            "operator_action": "APPROVE_KILL",
            "rank": "1",
            "priority": "medium",
            "action_type": "retune_plan",
            "key": "retune-me",
            "reason": "operator chose to kill",
            "suggested_command": "",
        },
        args,
    )

    assert command is not None
    assert command[2:] == [
        "src.research.research_runner",
        "kill-approved",
        "--hypothesis",
        str(hypotheses / "retune-me.md"),
    ]


def test_control_row_surface_expansion_maps_to_plan_command(tmp_path: Path) -> None:
    args = parse_args(
        [
            "once",
            "--hypotheses-dir",
            str(tmp_path / "hypotheses"),
            "--runs-dir",
            str(tmp_path / "runs"),
            "--out-dir",
            str(tmp_path / "ops"),
            "--control-sheet-id",
            "sheet-id",
            "--control-google-credentials",
            str(tmp_path / "credentials.json"),
        ]
    )

    command = _command_for_control_row(
        {
            "operator_action": "APPROVE_SURFACE_EXPANSION",
            "rank": "1",
            "priority": "medium",
            "action_type": "retune_plan",
            "key": "retune-me",
            "reason": "needs search-space review",
            "suggested_command": "",
        },
        args,
    )

    assert command is not None
    assert command[2:7] == [
        "src.research.research_ops",
        "surface-expansion-plan",
        "--key",
        "retune_plan:retune-me",
        "--push-control",
    ]


def test_control_skip_records_action_disposition(tmp_path: Path) -> None:
    path = tmp_path / "dispositions.jsonl"
    args = parse_args(["once", "--dispositions-path", str(path)])

    payload = _record_control_skip(
        {
            "operator_action": "SKIP",
            "rank": "1",
            "priority": "medium",
            "action_type": "retune_plan",
            "key": "retune-me",
            "reason": "operator chose to skip",
            "suggested_command": "",
        },
        args,
    )

    rows = read_dispositions(path)
    assert payload["category"] == "control_skip"
    assert len(rows) == 1
    assert rows[0].key == "retune_plan:retune-me"
    assert rows[0].status == "ignore"
