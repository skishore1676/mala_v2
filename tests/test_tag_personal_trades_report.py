"""Regression tests for the fingerprint-report row builder in
scripts/tag_personal_trades.py.

Locks in the P1 bug where the per-tag distribution row was one tangled
f-string: `... if pnls else "| | |"` guarded the WHOLE concatenation, so
`holds[len(holds)//2]` still ran (unguarded) when a tag bucket had episodes
with PnL but no holding_minutes -> IndexError; and a bucket with holds but no
PnL collapsed to a meaningless "| | |" row, dropping the tag name / counts /
hold / DTE columns. Each stat cell is now guarded independently.
"""

from __future__ import annotations

import importlib.util
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

REPO = Path(__file__).resolve().parents[1]
_SPEC = importlib.util.spec_from_file_location(
    "tag_personal_trades", REPO / "scripts" / "tag_personal_trades.py"
)
tpt = importlib.util.module_from_spec(_SPEC)
sys.modules.setdefault("tag_personal_trades", tpt)
_SPEC.loader.exec_module(tpt)  # type: ignore[union-attr]

from src.research.playbook_tagging import Episode  # noqa: E402

ET = ZoneInfo("America/New_York")


def _ep(hold, pnl, *, dte=3, conf="HIGH") -> Episode:
    e = Episode(
        episode_id="x", underlying="IWM", thesis_dir=1,
        entry_dt=datetime(2026, 1, 5, 10, 0, tzinfo=ET), n_fills=1,
        dte=dte, holding_minutes=hold, pnl=pnl,
    )
    e.confidence = conf
    return e


def test_median_empty_returns_none_and_preserves_upper_middle():
    assert tpt._median([]) is None
    # upper-middle element (preserves the original holds[len//2] semantics)
    assert tpt._median([30.0, 50.0]) == 50.0
    assert tpt._median([10.0, 20.0, 30.0]) == 20.0


def test_tag_row_pnl_present_but_no_holding_minutes_does_not_crash():
    # Old code: holds[len(holds)//2] on an empty list -> IndexError.
    row = tpt._tag_row("FLASH_REVERSAL", [_ep(None, 100.0), _ep(None, -50.0)])
    assert row.count("|") == 9  # 8 data columns => 9 pipe delimiters
    assert row.startswith("| FLASH_REVERSAL | 2 |")
    assert "| +50 |" in row  # summed PnL still rendered
    # hold cell blank (no holding_minutes), other cells intact
    assert "| FLASH_REVERSAL | 2 | 2 | 0 |  | 3 | 50% | +50 |" == row


def test_tag_row_holds_present_but_no_pnl_keeps_full_row():
    # Old code collapsed the entire row to "| | |" when pnls was empty,
    # losing the tag name, counts, hold and DTE columns.
    row = tpt._tag_row("TREND_CONTINUATION", [_ep(30.0, None), _ep(50.0, None)])
    assert row.count("|") == 9
    assert "TREND_CONTINUATION" in row
    assert "| 50 |" in row  # median hold preserved
    assert row != "| | |"


def test_tag_row_full_stats_render():
    row = tpt._tag_row(
        "RANGE_EXPANSION",
        [_ep(20.0, 10.0, dte=5), _ep(40.0, -5.0, dte=7, conf="MEDIUM")],
    )
    assert "| 40 |" in row   # median hold (upper-middle of 20,40)
    assert "| 7 |" in row    # median DTE (upper-middle of 5,7)
    assert "50%" in row      # 1 of 2 pnls > 0
