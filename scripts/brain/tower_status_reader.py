#!/usr/bin/env python3
"""oldmac-side reader for the mala Control Tower source (stdlib only).

The Lathi tower runs this locally on oldmac; it loads the snapshot the dev-Mac
steward pushed and RECOMPUTES staleness at read time, so a dead/asleep dev Mac
renders as a stuck unit — never as frozen "healthy". Read-only: it executes
nothing, touches nothing, prints one JSON object.

Deployed (alongside the snapshot it reads) by scripts/brain/tower_status.py
--push in mala_v2 on the dev Mac — edit THERE, this copy is overwritten nightly.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

SNAPSHOT = Path(__file__).resolve().parent / "latest_status.json"
# nightly 21:45 + a generous wake-late allowance; past this the brain is stale
EXPECTED_WITHIN = timedelta(hours=27)


def error_payload(msg: str) -> dict:
    return {
        "schema": "mala.launchd.status.v1",
        "source": "mala",
        "generated_at": datetime.now().astimezone().isoformat(),
        "host": "oldmac (reader)",
        "jobs": [{
            "label": "com.mala.brain-steward",
            "unit_id": "com.mala.brain-steward",
            "kind": "external_launchd_job",
            "schedule": "nightly 21:45 (dev Mac)",
            "declared_enabled": True,
            "effective_enabled": False,
            "lifecycle": "stuck",
            "last_run_status": "unknown",
            "last_run_at": None,
            "findings": [msg],
            "available_actions": [],
        }],
    }


def main() -> int:
    try:
        payload = json.loads(SNAPSHOT.read_text(encoding="utf-8"))
    except FileNotFoundError:
        print(json.dumps(error_payload("no snapshot — dev Mac steward has never pushed status")))
        return 0
    except (json.JSONDecodeError, OSError) as exc:
        print(json.dumps(error_payload(f"snapshot unreadable: {exc}")))
        return 0

    now = datetime.now().astimezone()
    generated_raw = payload.get("generated_at") or ""
    try:
        generated = datetime.fromisoformat(generated_raw)
        age = now - generated
    except ValueError:
        age = None

    for job in payload.get("jobs", []):
        findings = list(job.get("findings") or [])
        if age is None:
            job["lifecycle"] = "stuck"
            findings.append("snapshot generated_at unparseable")
        elif age > EXPECTED_WITHIN:
            job["lifecycle"] = "stuck"
            findings.append("stale_last_run")
            findings.append(
                f"last={generated_raw} expected_after={(generated + EXPECTED_WITHIN).isoformat()}"
                " — dev-Mac steward has not pushed; brain may be aging"
            )
        elif job.get("last_run_status") in ("ok",):
            job.setdefault("lifecycle", "armed")
        job["findings"] = findings
    payload["observed_at"] = now.isoformat()
    payload["observed_on"] = "oldmac"
    print(json.dumps(payload))
    return 0


if __name__ == "__main__":
    sys.exit(main())
