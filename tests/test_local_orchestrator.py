from __future__ import annotations

from pathlib import Path

from src.research.local_orchestrator import _command_for_control_row, parse_args, run_once


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


def test_control_row_surface_expansion_is_reasoning_only(tmp_path: Path) -> None:
    args = parse_args(["once", "--hypotheses-dir", str(tmp_path / "hypotheses")])

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

    assert command is None
