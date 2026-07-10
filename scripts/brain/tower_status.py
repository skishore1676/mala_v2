#!/usr/bin/env python3
"""Compose (and optionally push) the mala Control Tower status snapshot.

mala owns its launchd job meaning (the bhiksha pattern: registry + status
snapshot; Lathi only projects). This script derives truth from artifacts —
launchctl state, the latest steward run dir + receipt, the last steward commit
on docs/brain, freshness lint — and emits one `mala.launchd.status.v1` payload.

  tower_status.py --json    print the payload (local inspection)
  tower_status.py --push    also scp snapshot + reader to the oldmac drop:
                            oldmac:~/Documents/mala_v2/artifacts/brain/

The oldmac tower reads the drop via tower_status_reader.py, which recomputes
staleness at READ time so a dead dev Mac shows as stuck, never as healthy.
The push is the ONE sanctioned steward write to oldmac: a non-secret status
projection into a mala-owned artifacts dir — never the runtime, never bhiksha.
"""

from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path

REPO = Path("/Users/suman/code/mala_v2")
RUN_ROOT = Path.home() / "Library" / "Logs" / "mala-brain-steward"
LABEL = "com.mala.brain-steward"
DROP = "oldmac:/Users/sunny/Documents/mala_v2/artifacts/brain/"
READER = REPO / "scripts" / "brain" / "tower_status_reader.py"


def sh(cmd: list[str], cwd: Path | None = None, timeout: int = 30) -> tuple[int, str]:
    try:
        p = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, timeout=timeout)
        return p.returncode, (p.stdout + p.stderr).strip()
    except (subprocess.TimeoutExpired, OSError) as exc:
        return 1, str(exc)


def latest_run() -> dict:
    """Derive last-run truth from the newest steward run dir + its receipt."""
    dirs = sorted(RUN_ROOT.glob("2*"), reverse=True) if RUN_ROOT.exists() else []
    for d in dirs:
        receipt_path = d / "receipt.json"
        stamp = d.name  # YYYY-MM-DD_HHMM
        try:
            at = datetime.strptime(stamp, "%Y-%m-%d_%H%M").isoformat()
        except ValueError:
            continue
        if receipt_path.exists():
            try:
                r = json.loads(receipt_path.read_text(encoding="utf-8"))
                return {"at": at, "status": "ok" if r.get("status") == "succeeded" else "failed",
                        "outcome": r.get("outcome"), "provider": r.get("provider_id"),
                        "degraded": bool(r.get("degraded"))}
            except (json.JSONDecodeError, OSError):
                return {"at": at, "status": "failed", "outcome": None,
                        "provider": None, "degraded": False}
        if (d / "task.json").exists():  # run started; no receipt => dry-run or died pre-hire
            continue
    return {"at": None, "status": "never_ran", "outcome": None, "provider": None, "degraded": False}


def build_payload() -> dict:
    now = datetime.now().astimezone()
    rc, out = sh(["launchctl", "list"])
    loaded = LABEL in out if rc == 0 else False

    run = latest_run()
    _, last_commit = sh(["git", "log", "-1", "--pretty=format:%h %ad %s",
                         "--date=format:%m-%d %H:%M", "--", "docs/brain"], cwd=REPO)
    lint_rc, lint_out = sh(["python3", str(REPO / "scripts" / "brain" / "freshness_lint.py")], cwd=REPO)

    findings: list[str] = []
    if not loaded:
        findings.append("launchd job not loaded on dev Mac")
    if run["status"] == "failed":
        findings.append("last steward run failed (canon untouched — stale beats wrong)")
    if run.get("degraded"):
        findings.append(f"last run degraded to fallback provider {run['provider']}")
    if lint_rc != 0:
        findings.extend(f"lint: {line}" for line in lint_out.splitlines()
                        if line.startswith(("STALE", "MISSING", "NO-AS-OF")))

    job = {
        "label": LABEL,
        "unit_id": LABEL,
        "kind": "external_launchd_job",
        "schedule": "nightly 21:45 (dev Mac launchd; fires on wake if slept)",
        "declared_enabled": True,
        "effective_enabled": loaded,
        "loaded": loaded,
        "risk_class": "read_only_reflection",
        "last_run_status": run["status"],
        "last_run_at": run["at"],
        "findings": findings,
        "available_actions": [],  # no remote actions: canon commits happen only on the dev Mac
        "detail": {
            "outcome": run["outcome"],
            "provider": run["provider"],
            "last_brain_commit": last_commit,
            "purpose": "nightly replacement of mala_v2 docs/brain/STATE.md (RFC phase 2)",
        },
    }
    return {
        "schema": "mala.launchd.status.v1",
        "source": "mala",
        "generated_at": now.isoformat(),
        "host": "dev-mac (Air)",
        "repo_root": str(REPO),
        "jobs": [job],
    }


def push(payload: dict) -> int:
    drop_dir = DROP.split(":", 1)[1]
    rc, out = sh(["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "oldmac",
                  f"mkdir -p {drop_dir}"])
    if rc != 0:
        print(f"push failed (mkdir): {out}", file=sys.stderr)
        return rc
    tmp = RUN_ROOT / "latest_status.json"
    tmp.parent.mkdir(parents=True, exist_ok=True)
    tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    rc, out = sh(["scp", "-q", "-o", "BatchMode=yes", str(tmp), DROP + "latest_status.json"], timeout=30)
    if rc != 0:
        print(f"push failed (snapshot): {out}", file=sys.stderr)
        return rc
    rc, out = sh(["scp", "-q", "-o", "BatchMode=yes", str(READER), DROP + "tower_status_reader.py"], timeout=30)
    if rc != 0:
        print(f"push failed (reader): {out}", file=sys.stderr)
    return rc


def main() -> int:
    payload = build_payload()
    print(json.dumps(payload, indent=2))
    if "--push" in sys.argv:
        return push(payload)
    return 0


if __name__ == "__main__":
    sys.exit(main())
