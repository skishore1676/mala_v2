from __future__ import annotations

from datetime import UTC, datetime

from src.research.time_utils import sheet_timestamp


def test_sheet_timestamp_formats_central_daylight_time() -> None:
    stamp = sheet_timestamp(datetime(2026, 4, 24, 23, 19, tzinfo=UTC))

    assert stamp == "2026-04-24 18:19:00 CDT"


def test_sheet_timestamp_formats_central_standard_time() -> None:
    stamp = sheet_timestamp(datetime(2026, 1, 24, 23, 19, tzinfo=UTC))

    assert stamp == "2026-01-24 17:19:00 CST"

