#!/usr/bin/env python3
"""Brain freshness lint (RFC §5.6.4: stale beats wrong — but staleness must be VISIBLE).

Parses the `as_of` stamp of each docs/brain/ file and flags anything older than
its threshold. STATE.md is the nightly-replaced episodic tier (48h budget, the
phase-2 success criterion); the slow layers get weeks.

Exit 0 = all fresh, 1 = something stale, 2 = a file is missing/unparseable.
"""

from __future__ import annotations

import re
import sys
from datetime import datetime, timedelta
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
BRAIN = REPO / "docs" / "brain"

# file -> staleness threshold
THRESHOLDS = {
    "STATE.md": timedelta(hours=48),
    "INDEX.md": timedelta(days=14),
    "OPERATIONS.md": timedelta(days=30),
    "ARCHITECTURE.md": timedelta(days=30),
    "DECISIONS.md": timedelta(days=45),
}

AS_OF_RE = re.compile(r"^as_of:\s*(\d{4}-\d{2}-\d{2})(?:T(\d{2}):(\d{2}))?", re.M)


def as_of(path: Path) -> datetime | None:
    m = AS_OF_RE.search(path.read_text(encoding="utf-8")[:2000])
    if not m:
        return None
    stamp = datetime.strptime(m.group(1), "%Y-%m-%d")
    if m.group(2):
        stamp = stamp.replace(hour=int(m.group(2)), minute=int(m.group(3)))
    return stamp


def main() -> int:
    now = datetime.now()
    worst = 0
    for name, budget in THRESHOLDS.items():
        path = BRAIN / name
        if not path.exists():
            print(f"MISSING  {name}")
            worst = max(worst, 2)
            continue
        stamp = as_of(path)
        if stamp is None:
            print(f"NO-AS-OF {name}")
            worst = max(worst, 2)
            continue
        age = now - stamp
        state = "STALE" if age > budget else "fresh"
        if state == "STALE":
            worst = max(worst, 1)
        print(f"{state:8s} {name:16s} as_of={stamp:%Y-%m-%d %H:%M} age={age.days}d{age.seconds // 3600}h budget={budget}")
    return worst


if __name__ == "__main__":
    sys.exit(main())
