"""Time formatting helpers for research operations."""

from __future__ import annotations

from datetime import UTC, datetime
from zoneinfo import ZoneInfo


SHEET_TIMEZONE = ZoneInfo("America/Chicago")


def sheet_timestamp(value: datetime | None = None) -> str:
    """Return a human-readable timestamp for operator-facing sheets."""
    stamp = value or datetime.now(UTC)
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=UTC)
    return stamp.astimezone(SHEET_TIMEZONE).strftime("%Y-%m-%d %H:%M:%S %Z")

