from __future__ import annotations

from src.research.recommendation_tier import RecommendationThresholds, classify_recommendation_tier


def test_recommendation_promote_requires_exit_sample_depth() -> None:
    decision = classify_recommendation_tier(
        mc_prob_positive_exp=0.99,
        holdout_trades=100,
        base_exp_r=0.5,
        thesis_exit_tested=True,
        exit_trade_count=39,
        thresholds=RecommendationThresholds(),
    )

    assert decision.tier == "shadow"
    assert "exit_trade_count 39 < 40" in decision.reason


def test_recommendation_shadow_requires_tested_thesis_exit() -> None:
    decision = classify_recommendation_tier(
        mc_prob_positive_exp=0.90,
        holdout_trades=100,
        base_exp_r=0.5,
        thesis_exit_tested=False,
        exit_trade_count=None,
        thresholds=RecommendationThresholds(),
    )

    assert decision.tier == "watch_only"
    assert decision.reason == "watch_only: missing tested thesis exit"


def test_recommendation_promote_when_all_hardened_checks_pass() -> None:
    decision = classify_recommendation_tier(
        mc_prob_positive_exp=0.99,
        holdout_trades=100,
        base_exp_r=0.5,
        thesis_exit_tested=True,
        exit_trade_count=41,
        thresholds=RecommendationThresholds(),
    )

    assert decision.tier == "promote"
    assert decision.checks["min_exit_trades_for_promote"] == 40
