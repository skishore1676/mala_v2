#!/usr/bin/env python3
"""Nightly brain steward (RFC phase 2, Q1 auto-commit + Q2-REVISED dev-Mac home).

Deterministic runner around a text-only agent-broker hire:

  gather evidence -> hire actors.steward (claude opus -> sonnet -> codex)
  -> parse + validate file blocks fail-closed -> write docs/brain/STATE.md
  (+ optional candidates/) -> mechanical INDEX row stamp -> git commit
  -> freshness lint -> Friday: weekly advisory curation digest via Lathi bus.

Safety posture (RFC §5.6): the hired model has NO tools; this runner reads
oldmac strictly over read-only ssh; it writes ONLY under docs/brain/ and
commits ONLY docs/brain paths; on any parse/validation failure it touches
nothing (stale beats wrong). No Sheet writes, no deploys, no order path.

Usage:
  steward.py            nightly run (hire + auto-commit)
  steward.py --dry-run  gather evidence + write the task bundle, no hire
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path

REPO = Path("/Users/suman/code/mala_v2")
BRAIN = REPO / "docs" / "brain"
BROKER = "/Users/suman/code/agent-broker/.venv/bin/agent-broker"
POLICY = REPO / "scripts" / "brain" / "steward_policy.yaml"
PROMPT = REPO / "scripts" / "brain" / "steward_prompt.md"
LINT = REPO / "scripts" / "brain" / "freshness_lint.py"
BHIKSHA_DEV = Path("/Users/suman/code/bhiksha")
LATHI_BUS = Path("/Users/suman/code/lathi-bus")
RUN_ROOT = Path.home() / "Library" / "Logs" / "mala-brain-steward"

SSH = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "oldmac"]
FILE_BLOCK_RE = re.compile(r"===FILE: (\S+)===\n(.*?)\n===END FILE===", re.S)
ALLOWED_PATH_RE = re.compile(r"^docs/brain/(STATE\.md|candidates/[A-Za-z0-9._-]+\.md)$")


def log(msg: str) -> None:
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {msg}", flush=True)


def run(cmd: list[str], cwd: Path | None = None, timeout: int = 60) -> tuple[int, str]:
    try:
        p = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, timeout=timeout)
        return p.returncode, (p.stdout + p.stderr).strip()
    except (subprocess.TimeoutExpired, OSError) as exc:
        return 1, f"<<command failed: {exc}>>"


def evidence_section(title: str, tier: str, body: str, limit: int = 12000) -> str:
    body = body.strip() or "<<empty>>"
    if len(body) > limit:
        body = body[-limit:]
        body = f"<<truncated to last {limit} chars>>\n" + body
    return f"\n\n## EVIDENCE: {title}  [tier: {tier}]\n\n{body}"


def gather_evidence(now: datetime) -> str:
    parts = [
        f"# Evidence bundle — brain steward nightly run\n\n"
        f"Tonight's timestamp for `as_of`: **{now:%Y-%m-%dT%H:%M}** (local, CT).\n"
        f"Current STATE.md and INDEX.md are included for continuity and style; "
        f"everything else is tonight's raw evidence, labeled by trust tier."
    ]

    parts.append(evidence_section(
        "current docs/brain/STATE.md (the file you are replacing)", "brain summary",
        (BRAIN / "STATE.md").read_text(encoding="utf-8")))
    parts.append(evidence_section(
        "current docs/brain/INDEX.md (context only — you never edit it)", "brain summary",
        (BRAIN / "INDEX.md").read_text(encoding="utf-8")))

    _, mala_log = run(["git", "log", "--since=60 hours ago", "--stat",
                       "--pretty=format:%h %ad %s", "--date=format:%m-%d %H:%M"], cwd=REPO)
    parts.append(evidence_section("mala_v2 git log (last 60h, dev Mac = canonical)", "diary", mala_log))

    workplan = (REPO / "docs" / "LIVE_LOOP_WORKPLAN.md").read_text(encoding="utf-8")
    parts.append(evidence_section(
        "diary tail — docs/LIVE_LOOP_WORKPLAN.md (last ~250 lines)", "diary",
        "\n".join(workplan.splitlines()[-250:])))

    sup = REPO / ".supervisor-lane" / "STATE.md"
    if sup.exists():
        parts.append(evidence_section(
            ".supervisor-lane/STATE.md tail (verdict log)", "diary",
            "\n".join(sup.read_text(encoding="utf-8").splitlines()[-80:])))

    _, bh_log = run(["git", "log", "--oneline", "-8"], cwd=BHIKSHA_DEV)
    parts.append(evidence_section("bhiksha git log (dev Mac checkout)", "diary", bh_log))

    # --- oldmac: READ-ONLY ssh, failure-tolerant (RFC §5.6.4: canon just ages) ---
    rc, oldmac_git = run(SSH + ["cd ~/Documents/bhiksha && git log --oneline -5 && git status -sb | head -2"], timeout=25)
    parts.append(evidence_section(
        "oldmac runtime checkout ~/Documents/bhiksha (git)", "runtime",
        oldmac_git if rc == 0 else f"UNAVAILABLE tonight — {oldmac_git}"))

    rc, jobs = run(SSH + ["launchctl list | grep com.bhiksha || true"], timeout=25)
    parts.append(evidence_section(
        "oldmac launchd jobs (launchctl list | grep com.bhiksha)", "runtime",
        jobs if rc == 0 else f"UNAVAILABLE tonight — {jobs}"))

    rc, status = run(SSH + ["cat ~/Documents/bhiksha/artifacts/playbook/launchd/latest_status.json 2>/dev/null; "
                            "ls -t ~/Documents/bhiksha/artifacts/playbook/launchd/*.out.log 2>/dev/null | head -5"], timeout=25)
    parts.append(evidence_section(
        "oldmac launchd latest_status.json + newest job logs", "runtime",
        status if rc == 0 else f"UNAVAILABLE tonight — {status}"))

    parts.append(
        "\n\n## OUTPUT REMINDER\n\nEmit file blocks + `OUTCOME: updated`, or just "
        "`OUTCOME: no_change`. Allowed paths: `docs/brain/STATE.md`, "
        "`docs/brain/candidates/*.md`. Nothing else.")
    return "".join(parts)


def build_task(run_dir: Path, now: datetime) -> Path:
    payload = {
        "spec": {
            "lane_id": "brain_steward",
            "actor": "actors.steward",
            "role": "nightly brain steward — draft replacement STATE.md from evidence",
            "risk_class": "internal",
        },
        "task": {
            "task_id": f"brain-steward-{now:%Y-%m-%d}",
            "objective": "Draft the nightly replacement of docs/brain/STATE.md from tonight's evidence bundle.",
            "context": {"system": PROMPT.read_text(encoding="utf-8")},
            "raw_prompt": gather_evidence(now),
            "allowed_outcomes": ["updated", "no_change"],
            "timeout_seconds": 1200,
        },
    }
    task_path = run_dir / "task.json"
    task_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return task_path


def parse_and_validate(output_text: str, now: datetime) -> dict[str, str]:
    files: dict[str, str] = {}
    for path, body in FILE_BLOCK_RE.findall(output_text):
        if not ALLOWED_PATH_RE.match(path) or ".." in path:
            raise ValueError(f"disallowed path in steward output: {path}")
        files[path] = body.rstrip() + "\n"
    state = files.get("docs/brain/STATE.md")
    if state is None:
        raise ValueError("outcome=updated but no docs/brain/STATE.md block")
    lines = state.count("\n")
    if not (40 <= lines <= 250):
        raise ValueError(f"STATE.md draft suspicious length: {lines} lines")
    if not state.startswith("---") or f"as_of: {now:%Y-%m-%d}" not in state.split("---")[1]:
        raise ValueError("STATE.md draft missing frontmatter or tonight's as_of")
    if "Trust order" not in state:
        raise ValueError("STATE.md draft dropped the trust-order banner")
    return files


def stamp_index(now: datetime) -> None:
    """Mechanically refresh the STATE.md row's as_of in the INDEX table."""
    index = BRAIN / "INDEX.md"
    text = index.read_text(encoding="utf-8")
    new = re.sub(r"(\|\s*`STATE\.md`\s*\|[^|]*\|\s*)\d{4}-\d{2}-\d{2}(\s*\|)",
                 rf"\g<1>{now:%Y-%m-%d}\g<2>", text)
    if new != text:
        index.write_text(new, encoding="utf-8")


def git_guard() -> None:
    rc, out = run(["git", "status", "--porcelain", "--", "docs/brain"], cwd=REPO)
    if rc != 0:
        raise RuntimeError(f"git status failed: {out}")
    if out.strip():
        raise RuntimeError(f"docs/brain has uncommitted local changes — refusing to overwrite:\n{out}")


def commit(files: dict[str, str], receipt: dict, now: datetime) -> str:
    provider = receipt.get("provider_id", "?")
    degraded = " degraded" if receipt.get("degraded") else ""
    msg = (f"brain: nightly steward update {now:%Y-%m-%d}\n\n"
           f"provider={provider}{degraded} receipt={receipt.get('receipt_id', '?')}\n"
           f"auto-committed per RFC 9a Q1 (advisory curation via weekly digest)")
    rc, out = run(["git", "add", "--", "docs/brain"], cwd=REPO)
    if rc != 0:
        raise RuntimeError(f"git add failed: {out}")
    rc, out = run(["git", "commit", "-m", msg, "--", "docs/brain"], cwd=REPO)
    if rc != 0:
        raise RuntimeError(f"git commit failed: {out}")
    _, sha = run(["git", "rev-parse", "--short", "HEAD"], cwd=REPO)
    return sha


def weekly_digest(now: datetime, run_dir: Path, lint_output: str) -> None:
    """Friday advisory curation digest (RFC 9a Q1) — published via Lathi bus."""
    if now.weekday() != 4:
        return
    _, week_log = run(["git", "log", "--since=7 days ago", "--stat",
                       "--pretty=format:%h %ad %s", "--date=format:%m-%d",
                       "--", "docs/brain"], cwd=REPO)
    candidates = sorted(p.name for p in (BRAIN / "candidates").glob("*.md"))
    digest = run_dir / f"brain-curation-digest-{now:%Y-%m-%d}.md"
    digest.write_text(
        f"# Brain curation digest — week ending {now:%Y-%m-%d}\n\n"
        "Advisory review (RFC 9a Q1): the steward auto-commits; your `<pointy-bracket>`\n"
        "comments here prune/correct canon after the fact. Diffs: `git log -p -- docs/brain`.\n\n"
        f"## Brain commits this week\n\n```\n{week_log or '(none)'}\n```\n\n"
        f"## Candidates awaiting curation\n\n"
        + ("\n".join(f"- `docs/brain/candidates/{c}`" for c in candidates) or "(none)")
        + f"\n\n## Freshness lint tonight\n\n```\n{lint_output}\n```\n",
        encoding="utf-8")
    rc, out = run(["python3", "-m", "lathi_bus.cli", "publish",
                   "--profile", "coding-agent-northstar",
                   "--source", str(digest),
                   "--title", f"Brain curation digest {now:%Y-%m-%d}",
                   "--workspace-root", str(REPO),
                   "--artifact-id", f"brain-digest/{digest.name}",
                   "--owner-consumer", "brain-steward"],
                  cwd=LATHI_BUS, timeout=120)
    log(f"weekly digest publish rc={rc}: {out[:300]}")


def main() -> int:
    dry_run = "--dry-run" in sys.argv
    now = datetime.now()
    run_dir = RUN_ROOT / f"{now:%Y-%m-%d_%H%M}"
    run_dir.mkdir(parents=True, exist_ok=True)
    log(f"steward run start (dry_run={dry_run}) run_dir={run_dir}")

    git_guard()
    task_path = build_task(run_dir, now)
    log(f"task bundle written: {task_path} ({task_path.stat().st_size} bytes)")
    if dry_run:
        return 0

    p = subprocess.run([BROKER, "run", str(task_path), "--policy", str(POLICY)],
                       capture_output=True, text=True, timeout=1500)
    (run_dir / "receipt.json").write_text(p.stdout, encoding="utf-8")
    if p.returncode != 0:
        log(f"broker run FAILED (rc={p.returncode}): {p.stderr[-500:]}\n{p.stdout[-500:]}")
        return 1
    receipt = json.loads(p.stdout)
    outcome = receipt.get("outcome")
    log(f"hire ok: provider={receipt.get('provider_id')} degraded={receipt.get('degraded')} outcome={outcome}")

    lint_out = ""
    if outcome == "no_change":
        log("steward verdict: no_change — canon untouched")
    elif outcome == "updated":
        files = parse_and_validate(receipt.get("output_text", ""), now)
        git_guard()  # re-check: nothing slipped in while the model ran
        for rel, body in files.items():
            dest = REPO / rel
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_text(body, encoding="utf-8")
            log(f"wrote {rel} ({body.count(chr(10))} lines)")
        stamp_index(now)
        sha = commit(files, receipt, now)
        log(f"committed {sha}")
    else:
        log(f"unexpected outcome {outcome!r} — failing closed, canon untouched")
        return 1

    rc, lint_out = run(["python3", str(LINT)], cwd=REPO)
    log(f"freshness lint (rc={rc}):\n{lint_out}")
    weekly_digest(now, run_dir, lint_out)
    log("steward run done")
    return 0


def push_tower_status() -> None:
    """Project status to the oldmac Control Tower drop (non-fatal, always attempted)."""
    rc, out = run(["python3", str(REPO / "scripts" / "brain" / "tower_status.py"), "--push"],
                  cwd=REPO, timeout=90)
    log(f"tower status push rc={rc}" + (f": {out[-200:]}" if rc != 0 else ""))


if __name__ == "__main__":
    try:
        code = main()
    except Exception as exc:  # fail closed, loudly, canon untouched
        log(f"STEWARD FAILED CLOSED: {exc}")
        code = 1
    try:
        push_tower_status()  # even after failure — the tower should see failed, not silence
    except Exception as exc:
        log(f"tower status push failed: {exc}")
    sys.exit(code)
