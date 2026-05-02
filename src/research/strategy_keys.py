"""Canonical strategy-key helpers shared by research outputs."""

from __future__ import annotations

import re


_DISPLAY_STRATEGY_KEYS: dict[str, str] = {
    "Elastic Band Reversion": "elastic_band_reversion",
    "Market Impulse (Cross & Reclaim)": "market_impulse",
    "Market Impulse Descendants": "market_impulse",
    "MI Shallow Spring": "market_impulse",
    "MI Second Touch": "market_impulse",
    "MI High Close Reclaim": "market_impulse",
    "MI Push Through": "market_impulse",
    "Opening Drive Classifier": "opening_drive_classifier",
    "Opening Drive v2 (Short Continue)": "opening_drive_classifier",
    "Jerk-Pivot Momentum (tight)": "jerk_pivot_momentum",
    "Kinematic Ladder": "kinematic_ladder",
    "Compression Expansion Breakout": "compression_expansion_breakout",
    "Regime Router (Kinematic + Compression)": "regime_router",
}


def to_strategy_key(strategy_display_name: str) -> str:
    """Map display/parametric strategy names to catalog/research keys."""
    if strategy_display_name in _DISPLAY_STRATEGY_KEYS:
        return _DISPLAY_STRATEGY_KEYS[strategy_display_name]
    if strategy_display_name.startswith("Market Impulse"):
        return "market_impulse"
    if strategy_display_name.startswith("Jerk-Pivot Momentum"):
        return "jerk_pivot_momentum"
    if strategy_display_name.startswith("Elastic Band"):
        return "elastic_band_reversion"
    if strategy_display_name.startswith("Opening Drive"):
        return "opening_drive_classifier"
    return re.sub(r"[^a-z0-9]+", "_", strategy_display_name.lower()).strip("_")


__all__ = ["to_strategy_key"]
