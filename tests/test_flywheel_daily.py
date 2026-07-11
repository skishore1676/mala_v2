"""Tests for the Flywheel daily feed card view (scripts/flywheel_daily.py)."""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from scripts.flywheel_daily import card_view

ET = ZoneInfo("America/New_York")


def _fire(sym, det, profile, d, hh, mm, strength):
    return {"sym": sym, "det": det, "profile": profile, "dir": d,
            "dt": datetime(2026, 6, 15, hh, mm, tzinfo=ET), "strength": strength,
            "tape": 0.0, "stretch": strength}


def test_exhaustion_below_p85_hidden_from_card():
    fires = [_fire("IWM", "E-C", "EXHAUSTION_REVERSAL", -1, 10, 10, 53.0),
             _fire("IWM", "E-C", "EXHAUSTION_REVERSAL", -1, 10, 35, 88.0)]
    shown = card_view(fires)
    assert len(shown) == 1 and shown[0]["strength"] == 88.0


def test_weak_flash_hidden_from_card():
    fires = [_fire("SPY", "F-C", "FLASH_REVERSAL", 1, 9, 40, 0.10),
             _fire("SPY", "F-A", "FLASH_REVERSAL", 1, 9, 45, 0.27)]
    shown = card_view(fires)
    assert [f["strength"] for f in shown] == [0.27]


def test_opposite_direction_same_bar_pair_dropped():
    fires = [_fire("IWM", "F-C", "FLASH_REVERSAL", 1, 9, 40, 0.30),
             _fire("IWM", "F-C", "FLASH_REVERSAL", -1, 9, 40, 0.30)]
    assert card_view(fires) == []  # whipsaw bar, both suppressed


def test_trend_always_shown():
    fires = [_fire("IWM", "T-C", "TREND_CONTINUATION", 1, 11, 35, 1.0)]
    assert len(card_view(fires)) == 1
