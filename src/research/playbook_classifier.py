"""Deterministic per-strategy -> playbook classifier (Phase 3: classify).

The operator trades four playbooks, each mapped 1:1 to a named option exit
profile in :mod:`src.research.exit_profiles` / :mod:`src.research.option_translation`:

    Flash Reversal        <- fast intraday reversion at extremes (scalper DNA)
    Exhaustion Reversal   <- slower reversion / band fade (give the snapback room)
    Trend Continuation    <- momentum / continuation / drive entries
    Range Expansion       <- compression -> breakout / impulse expansion

This module classifies which playbook a promoted strategy/evidence row
represents. It is intentionally:

* DETERMINISTIC  - same input -> same {profile, confidence, rationale}; no model,
  no randomness, fully reviewable.
* BACKSTOPPED    - the operator's explicit ``PROFILE_BY_STRATEGY`` mapping in
  exit_profiles.py is the source of truth. When a strategy is in that table the
  classifier returns it at HIGH confidence and records that it was an explicit
  override. Heuristics only fill the gap for *unmapped* strategies.
* SAFE BY DEFAULT - a strategy the heuristics cannot place lands as LOW
  confidence with ``profile=None`` and is flagged for operator review. A
  low-confidence row is NEVER force-assigned a profile downstream.

The taxonomy is keyed off the canonical strategy key (via
:func:`src.research.strategy_keys.to_strategy_key`) plus a token scan of the
display name, so both ``"Elastic Band Reversion"`` and a parametric variant such
as ``"Elastic Band z=2.0 w=360"`` resolve identically.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Literal

from src.research.exit_profiles import EXIT_PROFILES, PROFILE_BY_STRATEGY
from src.research.strategy_keys import to_strategy_key

Confidence = Literal["high", "medium", "low"]

# The four playbook profiles, in canonical (exit_profiles.EXIT_PROFILES) form.
FLASH_REVERSAL = "FLASH_REVERSAL"
EXHAUSTION_REVERSAL = "EXHAUSTION_REVERSAL"
TREND_CONTINUATION = "TREND_CONTINUATION"
RANGE_EXPANSION = "RANGE_EXPANSION"


@dataclass(frozen=True)
class PlaybookClassification:
    """Result of classifying one strategy into a playbook profile.

    ``profile`` is None only when the row is unclassifiable -> it is flagged for
    operator review and must not be force-assigned a profile downstream.
    """

    strategy_key: str
    strategy_name: str
    profile: str | None
    confidence: Confidence
    rationale: str
    source: Literal["explicit_override", "heuristic", "unclassified"]
    needs_operator_review: bool
    candidates: dict[str, str] = field(default_factory=dict)

    @property
    def is_low_confidence(self) -> bool:
        return self.confidence == "low"

    def to_dict(self) -> dict:
        return {
            "strategy_key": self.strategy_key,
            "strategy_name": self.strategy_name,
            "profile": self.profile,
            "confidence": self.confidence,
            "rationale": self.rationale,
            "source": self.source,
            "needs_operator_review": self.needs_operator_review,
            "candidates": self.candidates,
        }


# ── Heuristic taxonomy ────────────────────────────────────────────────────────
#
# Each rule is (profile, token-regex, human reason). Rules are scanned in order;
# the FIRST family whose regex matches the (key + name) text wins. The regexes
# encode the operator's families:
#
#   reversion families         -> Flash / Exhaustion Reversal
#   momentum / continuation    -> Trend Continuation
#   breakout / impulse / range -> Range Expansion
#
# Flash vs Exhaustion within the reversion family is split on speed cues:
#   "extreme"/"mean_reversion"/"intraday" reads as the fast Flash scalp; a
#   slower band/elastic fade reads as Exhaustion (give the snapback room).
@dataclass(frozen=True)
class _Rule:
    profile: str
    pattern: re.Pattern[str]
    reason: str


def _rx(*words: str) -> re.Pattern[str]:
    return re.compile("|".join(words))


_RULES: tuple[_Rule, ...] = (
    # --- Reversion family -> Flash Reversal (fast intraday extreme fade) ---
    _Rule(
        FLASH_REVERSAL,
        _rx(r"mean[_ ]?reversion", r"\bextreme", r"\bfade\b", r"snap[_ ]?back"),
        "fast intraday mean-reversion / extreme-fade cue -> Flash Reversal scalp",
    ),
    # --- Reversion family -> Exhaustion Reversal (slower band / elastic fade) ---
    _Rule(
        EXHAUSTION_REVERSAL,
        _rx(r"elastic[_ ]?band", r"\bband[_ ]?reversion", r"exhaustion", r"\bz[_ ]?score"),
        "elastic-band / exhaustion reversion cue -> Exhaustion Reversal (give the snapback room)",
    ),
    # --- Momentum / continuation -> Trend Continuation ---
    _Rule(
        TREND_CONTINUATION,
        _rx(
            r"momentum", r"\btrend", r"continuation", r"jerk", r"pivot",
            r"\bdrive\b", r"opening[_ ]?drive", r"\bimpulse", r"market[_ ]?impulse",
            r"reclaim", r"cross", r"\bspring\b", r"push[_ ]?through",
        ),
        "momentum / continuation / drive / impulse cue -> Trend Continuation",
    ),
    # --- Breakout / range expansion -> Range Expansion ---
    _Rule(
        RANGE_EXPANSION,
        _rx(r"compression", r"\bbreakout", r"expansion", r"range[_ ]?expansion", r"squeeze"),
        "compression -> breakout / range-expansion cue -> Range Expansion",
    ),
)


def _normalize_text(strategy_key: str, strategy_name: str) -> str:
    """Lower-cased token soup of the key + display name for regex scanning."""
    return f"{strategy_key} {strategy_name}".lower()


def _heuristic_candidates(text: str) -> dict[str, str]:
    """All profiles whose family regex matches, profile -> matched reason.

    A single match -> unambiguous (medium confidence). Multiple distinct
    profiles matching -> ambiguous (downgraded), which is exactly the signal an
    operator should review.
    """
    hits: dict[str, str] = {}
    for rule in _RULES:
        if rule.profile in hits:
            continue
        if rule.pattern.search(text):
            hits[rule.profile] = rule.reason
    return hits


def classify_strategy(
    strategy_name: str,
    *,
    strategy_key: str | None = None,
    override_profile: str | None = None,
) -> PlaybookClassification:
    """Classify one strategy into a playbook profile.

    Resolution order (deterministic):

    1. ``override_profile`` (operator-supplied, e.g. a Sheet cell) -> HIGH,
       source=explicit_override.
    2. ``PROFILE_BY_STRATEGY`` (the operator's explicit reference table) -> HIGH,
       source=explicit_override.
    3. Heuristic family scan:
         * exactly one family matches -> MEDIUM, source=heuristic.
         * several families match     -> the first (highest-priority) family at
           LOW confidence, flagged for review (ambiguous).
    4. Nothing matches -> profile=None, LOW, source=unclassified, flagged.
    """
    key = (strategy_key or to_strategy_key(strategy_name)).strip()
    text = _normalize_text(key, strategy_name)

    # (1) explicit operator override wins outright.
    if override_profile:
        norm = override_profile.strip().upper()
        if norm not in EXIT_PROFILES:
            raise ValueError(
                f"override_profile {override_profile!r} is not a known profile "
                f"(known: {sorted(EXIT_PROFILES)})"
            )
        return PlaybookClassification(
            strategy_key=key,
            strategy_name=strategy_name,
            profile=norm,
            confidence="high",
            rationale=f"operator override -> {norm}",
            source="explicit_override",
            needs_operator_review=False,
            candidates={norm: "operator override"},
        )

    # (2) explicit reference table (PROFILE_BY_STRATEGY) is the backstop/override.
    backstop = PROFILE_BY_STRATEGY.get(key)
    candidates = _heuristic_candidates(text)
    if backstop:
        agrees = backstop in candidates
        note = "agrees with heuristic" if agrees else (
            "heuristic disagreed (table wins)" if candidates else "no heuristic signal"
        )
        return PlaybookClassification(
            strategy_key=key,
            strategy_name=strategy_name,
            profile=backstop,
            confidence="high",
            rationale=f"explicit PROFILE_BY_STRATEGY['{key}'] = {backstop} ({note})",
            source="explicit_override",
            needs_operator_review=False,
            candidates={backstop: "explicit reference table", **candidates},
        )

    # (3) heuristic family scan.
    if len(candidates) == 1:
        profile, reason = next(iter(candidates.items()))
        return PlaybookClassification(
            strategy_key=key,
            strategy_name=strategy_name,
            profile=profile,
            confidence="medium",
            rationale=f"heuristic: {reason}",
            source="heuristic",
            needs_operator_review=False,
            candidates=candidates,
        )
    if len(candidates) > 1:
        # Ambiguous: take the highest-priority rule's profile but flag it LOW so
        # the operator decides. (Rules are scanned in priority order.)
        ordered = [r.profile for r in _RULES if r.profile in candidates]
        profile = ordered[0]
        return PlaybookClassification(
            strategy_key=key,
            strategy_name=strategy_name,
            profile=profile,
            confidence="low",
            rationale=(
                f"ambiguous: matched {sorted(candidates)} -> defaulting to "
                f"highest-priority {profile}; operator review required"
            ),
            source="heuristic",
            needs_operator_review=True,
            candidates=candidates,
        )

    # (4) nothing matched -> flag for review, do NOT force-assign.
    return PlaybookClassification(
        strategy_key=key,
        strategy_name=strategy_name,
        profile=None,
        confidence="low",
        rationale="no family rule matched and no explicit mapping; flagged for operator review",
        source="unclassified",
        needs_operator_review=True,
        candidates={},
    )


__all__ = [
    "PlaybookClassification",
    "classify_strategy",
    "FLASH_REVERSAL",
    "EXHAUSTION_REVERSAL",
    "TREND_CONTINUATION",
    "RANGE_EXPANSION",
]
