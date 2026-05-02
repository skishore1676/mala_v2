"""Catalog recommendation tier rules for Mala evidence handoff."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class RecommendationThresholds:
    min_mc_prob_for_catalog: float = 0.70
    min_mc_prob_for_promote: float = 0.95
    min_holdout_trades_for_promote: int = 80
    min_holdout_trades_for_shadow: int = 15
    min_exit_trades_for_promote: int = 40


@dataclass(frozen=True)
class RecommendationDecision:
    tier: str
    reason: str
    checks: dict[str, Any]


def classify_recommendation_tier(
    *,
    mc_prob_positive_exp: Any,
    holdout_trades: Any,
    base_exp_r: Any,
    thesis_exit_tested: bool,
    exit_trade_count: Any,
    thresholds: RecommendationThresholds | None = None,
) -> RecommendationDecision:
    """Classify a candidate for operator review.

    ``promote`` means the research evidence is strong enough for a human to
    consider live authorization. ``shadow`` means it is catalog-worthy but
    needs paper/shadow evidence or more exit samples. ``watch_only`` means it
    should remain evidence only.
    """
    t = thresholds or RecommendationThresholds()
    mc_prob = _float_or_zero(mc_prob_positive_exp)
    trades = _int_or_zero(holdout_trades)
    exp_r = _float_or_zero(base_exp_r)
    exit_trades = _int_or_zero(exit_trade_count)

    checks = {
        "mc_prob_positive_exp": round(mc_prob, 6),
        "holdout_trades": trades,
        "base_exp_r": round(exp_r, 6),
        "thesis_exit_tested": bool(thesis_exit_tested),
        "exit_trade_count": exit_trades,
        "min_mc_prob_for_catalog": t.min_mc_prob_for_catalog,
        "min_mc_prob_for_promote": t.min_mc_prob_for_promote,
        "min_holdout_trades_for_promote": t.min_holdout_trades_for_promote,
        "min_holdout_trades_for_shadow": t.min_holdout_trades_for_shadow,
        "min_exit_trades_for_promote": t.min_exit_trades_for_promote,
    }

    if not thesis_exit_tested:
        return RecommendationDecision(
            tier="watch_only",
            reason="watch_only: missing tested thesis exit",
            checks=checks,
        )
    if exp_r <= 0:
        return RecommendationDecision(
            tier="watch_only",
            reason=f"watch_only: non-positive expectancy {exp_r:.2f}",
            checks=checks,
        )
    if mc_prob < t.min_mc_prob_for_catalog:
        return RecommendationDecision(
            tier="watch_only",
            reason=f"watch_only: mc_prob {mc_prob:.2f} < {t.min_mc_prob_for_catalog:.2f}",
            checks=checks,
        )
    if trades < t.min_holdout_trades_for_shadow:
        return RecommendationDecision(
            tier="watch_only",
            reason=f"watch_only: holdout_trades {trades} < {t.min_holdout_trades_for_shadow}",
            checks=checks,
        )

    promote_failures: list[str] = []
    if mc_prob < t.min_mc_prob_for_promote:
        promote_failures.append(f"mc_prob {mc_prob:.2f} < {t.min_mc_prob_for_promote:.2f}")
    if trades < t.min_holdout_trades_for_promote:
        promote_failures.append(f"holdout_trades {trades} < {t.min_holdout_trades_for_promote}")
    if exit_trades < t.min_exit_trades_for_promote:
        promote_failures.append(f"exit_trade_count {exit_trades} < {t.min_exit_trades_for_promote}")

    if not promote_failures:
        return RecommendationDecision(
            tier="promote",
            reason=(
                f"promote: mc_prob {mc_prob:.2f}, holdout_trades {trades}, "
                f"exit_trade_count {exit_trades}"
            ),
            checks=checks,
        )

    return RecommendationDecision(
        tier="shadow",
        reason="shadow: " + "; ".join(promote_failures),
        checks=checks,
    )


def _float_or_zero(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _int_or_zero(value: Any) -> int:
    try:
        return int(float(value or 0))
    except (TypeError, ValueError):
        return 0


__all__ = [
    "RecommendationDecision",
    "RecommendationThresholds",
    "classify_recommendation_tier",
]
