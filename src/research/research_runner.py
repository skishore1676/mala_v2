"""Guardrail wrapper around hypothesis_agent.py.

This module does not implement a second research engine. It gives agents and
humans a smaller command surface for the common Mala research actions while
delegating execution to the canonical hypothesis runner.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

from src.strategy.factory import available_strategy_names


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_HYPOTHESES_DIR = REPO_ROOT / "research" / "hypotheses"


def _slug(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.strip().lower()).strip("-")
    return slug or "untitled-hypothesis"


def _resolve_hypothesis(value: str) -> Path:
    path = Path(value)
    if not path.suffix:
        path = DEFAULT_HYPOTHESES_DIR / f"{value}.md"
    elif not path.is_absolute():
        path = REPO_ROOT / path
    if not path.exists():
        raise SystemExit(f"Hypothesis file not found: {path}")
    return path


def _run_hypothesis_agent(
    hypothesis: Path,
    *,
    max_stage: str | None = None,
    dry_run: bool = False,
    force_rerun: bool = False,
    allow_catalog_write: bool = False,
    extra_args: list[str] | None = None,
) -> int:
    command = [
        sys.executable,
        str(REPO_ROOT / "hypothesis_agent.py"),
        "--hypothesis",
        str(hypothesis),
    ]
    if max_stage:
        command.extend(["--max-stage", max_stage])
    if dry_run:
        command.append("--dry-run")
    if force_rerun:
        command.append("--force-rerun")
    if not allow_catalog_write:
        command.append("--no-catalog-write")
    if extra_args:
        command.extend(extra_args)
    print("RUNNER_COMMAND=" + " ".join(command))
    return subprocess.run(command, cwd=REPO_ROOT, check=False).returncode


def create_hypothesis_file(
    *,
    hypothesis_id: str,
    title: str,
    strategy: str,
    symbol_scope: str,
    max_stage: str,
    thesis: str,
    rules: list[str],
    notes: list[str],
    hypotheses_dir: Path = DEFAULT_HYPOTHESES_DIR,
    force: bool = False,
) -> Path:
    if strategy not in available_strategy_names():
        names = ", ".join(available_strategy_names())
        raise SystemExit(f"Unknown strategy: {strategy}. Available: {names}")
    hypothesis_id = _slug(hypothesis_id)
    title = title.strip() or hypothesis_id.replace("-", " ").title()
    path = hypotheses_dir / f"{hypothesis_id}.md"
    if path.exists() and not force:
        raise SystemExit(f"Refusing to overwrite existing hypothesis: {path}")

    rules = rules or ["Use the existing strategy rules and declared search surface."]
    notes = notes or ["Feasibility tag: config-only."]
    lines = [
        f"# Hypothesis: {title}",
        "",
        "## Config",
        f"- id: `{hypothesis_id}`",
        "- state: `pending`",
        "- decision: ``",
        f"- symbol_scope: `{symbol_scope}`",
        f"- strategy: `{strategy}`",
        f"- max_stage: `{max_stage}`",
        "- last_run: ``",
        "",
        "## Thesis",
        thesis.strip() or "TODO: describe the expected edge before running M1.",
        "",
        "## Rules",
    ]
    lines.extend(f"- {rule.strip()}" for rule in rules if rule.strip())
    lines.extend(["", "## Notes"])
    lines.extend(f"- {note.strip()}" for note in notes if note.strip())
    lines.extend(["", "## Agent Report", "Pending."])
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def cmd_create_hypothesis(args: argparse.Namespace) -> int:
    path = create_hypothesis_file(
        hypothesis_id=args.id or _slug(args.title),
        title=args.title,
        strategy=args.strategy,
        symbol_scope=args.symbol_scope,
        max_stage=args.max_stage,
        thesis=args.thesis,
        rules=args.rule,
        notes=args.note,
        hypotheses_dir=Path(args.hypotheses_dir),
        force=args.force,
    )
    print(f"HYPOTHESIS_FILE={path}")
    return 0


def cmd_dry_run(args: argparse.Namespace) -> int:
    return _run_hypothesis_agent(
        _resolve_hypothesis(args.hypothesis),
        max_stage=args.max_stage,
        dry_run=True,
        allow_catalog_write=False,
        extra_args=args.extra,
    )


def cmd_run_m1(args: argparse.Namespace) -> int:
    return _run_hypothesis_agent(
        _resolve_hypothesis(args.hypothesis),
        max_stage="M1",
        dry_run=False,
        force_rerun=args.force_rerun,
        allow_catalog_write=False,
        extra_args=args.extra,
    )


def cmd_continue_approved(args: argparse.Namespace) -> int:
    return _run_hypothesis_agent(
        _resolve_hypothesis(args.hypothesis),
        max_stage=args.max_stage,
        dry_run=False,
        force_rerun=args.force_rerun,
        allow_catalog_write=args.allow_catalog_write,
        extra_args=args.extra,
    )


def cmd_retune_plan(args: argparse.Namespace) -> int:
    hypothesis = _resolve_hypothesis(args.hypothesis)
    print(f"RETUNE_PLAN_FOR={hypothesis}")
    print("NEXT_STEP=Review latest M1/M2 artifacts, adjust hypothesis notes/search bounds if needed, then run retune-approved.")
    print(f"APPROVED_COMMAND=python -m src.research.research_runner retune-approved --hypothesis {hypothesis}")
    return 0


def cmd_retune_approved(args: argparse.Namespace) -> int:
    return _run_hypothesis_agent(
        _resolve_hypothesis(args.hypothesis),
        max_stage="M1",
        dry_run=False,
        force_rerun=args.force_rerun,
        allow_catalog_write=False,
        extra_args=args.extra,
    )


def kill_hypothesis_file(hypothesis: Path, *, reason: str = "") -> Path:
    """Mark a hypothesis as killed without deleting its evidence."""
    text = hypothesis.read_text(encoding="utf-8")
    now = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S+0000")

    def _replace_field(body: str, field: str, value: str) -> str:
        pattern = rf"(- {field}:\s*)`[^`]*`"
        replacement = rf"\1`{value}`"
        if re.search(pattern, body):
            return re.sub(pattern, replacement, body)
        return body

    text = _replace_field(text, "state", "kill")
    text = _replace_field(text, "decision", "kill")
    text = _replace_field(text, "last_run", now)
    report_lines = [
        "## Agent Report",
        f"- decision: `kill`",
        f"- updated_at: `{now}`",
        "- source: `research_runner kill-approved`",
    ]
    if reason.strip():
        report_lines.append(f"- reason: {reason.strip()}")
    report = "\n".join(report_lines)
    if "## Agent Report" in text:
        text = re.sub(r"## Agent Report.*", report, text, flags=re.DOTALL)
    else:
        text = text.rstrip() + "\n\n" + report
    hypothesis.write_text(text.rstrip() + "\n", encoding="utf-8")
    return hypothesis


def cmd_kill_approved(args: argparse.Namespace) -> int:
    hypothesis = _resolve_hypothesis(args.hypothesis)
    path = kill_hypothesis_file(hypothesis, reason=args.reason)
    print(f"KILLED_HYPOTHESIS={path}")
    print("STATE=kill")
    print("DECISION=kill")
    return 0


def _add_hypothesis_arg(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--hypothesis", required=True, help="Hypothesis path or id.")
    parser.add_argument("extra", nargs=argparse.REMAINDER, help="Extra args passed after '--' to hypothesis_agent.py.")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    create = subparsers.add_parser("create-hypothesis", help="Create a hypothesis markdown file.")
    create.add_argument("--id", default="")
    create.add_argument("--title", required=True)
    create.add_argument("--strategy", required=True, choices=available_strategy_names())
    create.add_argument("--symbol-scope", required=True)
    create.add_argument("--max-stage", choices=["M1", "M2", "M3", "M4", "M5"], default="M5")
    create.add_argument("--thesis", default="")
    create.add_argument("--rule", action="append", default=[])
    create.add_argument("--note", action="append", default=[])
    create.add_argument("--hypotheses-dir", default=str(DEFAULT_HYPOTHESES_DIR))
    create.add_argument("--force", action="store_true")
    create.set_defaults(func=cmd_create_hypothesis)

    dry_run = subparsers.add_parser("dry-run", help="Run hypothesis_agent.py --dry-run.")
    _add_hypothesis_arg(dry_run)
    dry_run.add_argument("--max-stage", choices=["M1", "M2", "M3", "M4", "M5"], default=None)
    dry_run.set_defaults(func=cmd_dry_run)

    run_m1 = subparsers.add_parser("run-m1", help="Run only M1 for an approved hypothesis.")
    _add_hypothesis_arg(run_m1)
    run_m1.add_argument("--force-rerun", action="store_true")
    run_m1.set_defaults(func=cmd_run_m1)

    cont = subparsers.add_parser("continue-approved", help="Continue an approved hypothesis through remaining gates.")
    _add_hypothesis_arg(cont)
    cont.add_argument("--max-stage", choices=["M2", "M3", "M4", "M5"], default=None)
    cont.add_argument("--force-rerun", action="store_true")
    cont.add_argument("--allow-catalog-write", action="store_true")
    cont.set_defaults(func=cmd_continue_approved)

    retune_plan = subparsers.add_parser("retune-plan", help="Print the bounded retune handoff.")
    retune_plan.add_argument("--hypothesis", required=True, help="Hypothesis path or id.")
    retune_plan.set_defaults(func=cmd_retune_plan)

    retune = subparsers.add_parser("retune-approved", help="Run approved M1 retune.")
    _add_hypothesis_arg(retune)
    retune.add_argument("--force-rerun", action="store_true")
    retune.set_defaults(func=cmd_retune_approved)

    kill = subparsers.add_parser("kill-approved", help="Mark a hypothesis killed after explicit operator approval.")
    kill.add_argument("--hypothesis", required=True, help="Hypothesis path or id.")
    kill.add_argument("--reason", default="operator approved kill from Research_Control")
    kill.set_defaults(func=cmd_kill_approved)

    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
