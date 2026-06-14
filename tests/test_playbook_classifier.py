"""Tests for the Phase-3 deterministic playbook classifier."""

import pytest

from src.research.exit_profiles import PROFILE_BY_STRATEGY
from src.research.playbook_classifier import (
    EXHAUSTION_REVERSAL,
    FLASH_REVERSAL,
    RANGE_EXPANSION,
    TREND_CONTINUATION,
    classify_strategy,
)
from src.research.strategy_keys import to_strategy_key


# ── Explicit reference table is the high-confidence backstop ──────────────────

def test_every_mapped_strategy_classifies_to_its_table_profile_at_high_confidence():
    for strategy_key, profile in PROFILE_BY_STRATEGY.items():
        result = classify_strategy(strategy_key, strategy_key=strategy_key)
        assert result.profile == profile
        assert result.confidence == "high"
        assert result.source == "explicit_override"
        assert result.needs_operator_review is False


def test_display_name_resolves_through_strategy_key_to_table():
    # "Elastic Band Reversion" -> elastic_band_reversion -> EXHAUSTION_REVERSAL.
    result = classify_strategy("Elastic Band Reversion")
    assert result.strategy_key == "elastic_band_reversion"
    assert result.profile == EXHAUSTION_REVERSAL
    assert result.confidence == "high"


def test_parametric_variant_name_resolves_same_as_canonical():
    # to_strategy_key collapses "Elastic Band z=2.0 w=360" to the same key.
    a = classify_strategy("Elastic Band z=2.0 w=360")
    assert a.strategy_key == to_strategy_key("Elastic Band z=2.0 w=360")
    assert a.profile == EXHAUSTION_REVERSAL


def test_market_impulse_descendants_map_to_trend_continuation():
    for name in ("MI Shallow Spring", "MI High Close Reclaim", "Market Impulse (Cross & Reclaim)"):
        result = classify_strategy(name)
        assert result.strategy_key == "market_impulse"
        assert result.profile == TREND_CONTINUATION


# ── Heuristic family scan for unmapped strategies ─────────────────────────────

def test_unmapped_momentum_name_classifies_heuristically_medium():
    result = classify_strategy("Velocity Momentum Burst", strategy_key="velocity_momentum_burst")
    assert result.profile == TREND_CONTINUATION
    assert result.confidence == "medium"
    assert result.source == "heuristic"
    assert result.needs_operator_review is False


def test_unmapped_breakout_name_classifies_to_range_expansion():
    result = classify_strategy("Volatility Squeeze Breakout", strategy_key="volatility_squeeze_breakout")
    assert result.profile == RANGE_EXPANSION
    assert result.confidence == "medium"
    assert result.source == "heuristic"


def test_unmapped_fast_reversion_name_classifies_to_flash_reversal():
    result = classify_strategy("Intraday Extreme Fade", strategy_key="intraday_extreme_fade")
    assert result.profile == FLASH_REVERSAL
    assert result.confidence == "medium"


# ── Low-confidence / ambiguous / unclassified are flagged, never forced ───────

def test_unknown_strategy_is_unclassified_and_flagged():
    result = classify_strategy("Totally Novel Alpha XYZ", strategy_key="totally_novel_alpha_xyz")
    assert result.profile is None
    assert result.confidence == "low"
    assert result.source == "unclassified"
    assert result.needs_operator_review is True
    assert result.is_low_confidence is True


def test_ambiguous_multi_family_match_is_low_confidence_and_flagged():
    # Both a breakout (Range Expansion) and momentum (Trend Continuation) cue.
    result = classify_strategy("Momentum Breakout Continuation", strategy_key="momentum_breakout_continuation")
    assert result.needs_operator_review is True
    assert result.confidence == "low"
    assert len(result.candidates) >= 2
    # Highest-priority rule among the matches wins as the default (still flagged).
    assert result.profile in {TREND_CONTINUATION, RANGE_EXPANSION}


# ── Explicit override beats both heuristic and the table ──────────────────────

def test_operator_override_wins_and_is_high_confidence():
    # Override a table strategy with a different profile.
    result = classify_strategy(
        "Elastic Band Reversion", override_profile="flash_reversal"
    )
    assert result.profile == FLASH_REVERSAL
    assert result.confidence == "high"
    assert result.source == "explicit_override"
    assert result.needs_operator_review is False


def test_invalid_override_raises():
    with pytest.raises(ValueError):
        classify_strategy("Elastic Band Reversion", override_profile="NOT_A_PROFILE")


def test_to_dict_is_json_serializable_shape():
    result = classify_strategy("Compression Expansion Breakout")
    d = result.to_dict()
    assert set(d) == {
        "strategy_key", "strategy_name", "profile", "confidence",
        "rationale", "source", "needs_operator_review", "candidates",
    }
    assert d["profile"] == RANGE_EXPANSION
