"""Operator exit profiles for the underlying exit optimizer (Wave 1).

The operator trades four playbooks, each mapped 1:1 to a named option exit
profile (live in ``public_api_trading_v3/.../profiles.py``). This module is the
canonical code home for those profiles as they apply to the UNDERLYING exit
optimizer: the R-multiples, partial scale-out, giveback, and time bounds carry
over directly (R and time are unit-free); ``stop_loss_pct`` is re-scaled to
underlying terms (the live values are option-premium %, ~10x larger).

Option-only dials (theta budget, GDS sensitivity, premium disaster stop,
DTE/delta vehicle selection, capital sizing) do NOT belong here — they are S4 /
Bhiksha concerns. See docs/EXIT_PROFILE_PLAYBOOKS.md.

The stop_loss_pct defaults are deliberate first-pass values to be tuned from
evidence (mala_v2 is the calibration bench). Tune, don't trust.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from src.oracle.trade_simulator import ProfileExitPolicy

# Giveback tiers carried verbatim from the live engine (arm_r, retrace_frac);
# R is unit-free so these transfer to the underlying path unchanged.
GIVEBACK_TIERS: dict[str, tuple[float | None, float]] = {
    "STRICT": (0.5, 0.5),
    "MODERATE": (0.75, 0.6),
    "LOOSE": (1.0, 0.75),
    "OFF": (None, 0.5),
}

NO_PROGRESS_FLOOR_R = 0.25  # "never got going" threshold (live engine default)


@dataclass(frozen=True)
class UnderlyingExitProfile:
    """A named exit profile expressed in underlying-path terms."""

    name: str
    stop_loss_pct: float          # underlying-scaled (NOT the option-premium %)
    target_1_r: float
    target_2_r: float
    target_1_quantity: float       # fraction banked at target 1
    giveback: str                  # key into GIVEBACK_TIERS
    no_progress_bars: int | None   # 1-minute bars ~ minutes
    max_hold_bars: int | None      # None = ride to the session EOD boundary
    breakeven_after_t1: bool = True

    def build_policy(self) -> ProfileExitPolicy:
        arm_r, retrace = GIVEBACK_TIERS[self.giveback]
        return ProfileExitPolicy(
            profile_name=self.name,
            stop_loss_pct=self.stop_loss_pct,
            target_1_r=self.target_1_r,
            target_2_r=self.target_2_r,
            target_1_quantity=self.target_1_quantity,
            giveback_arm_r=arm_r,
            giveback_retrace_frac=retrace,
            no_progress_bars=self.no_progress_bars,
            no_progress_floor_r=NO_PROGRESS_FLOOR_R,
            max_hold_bars=self.max_hold_bars,
            breakeven_after_t1=self.breakeven_after_t1,
            policy_name=f"profile:{self.name.lower()}",
        )

    def thesis_exit_params(self) -> dict[str, Any]:
        arm_r, retrace = GIVEBACK_TIERS[self.giveback]
        return {
            "profile": self.name,
            "stop_loss_underlying_pct": self.stop_loss_pct,
            "target_1_r": self.target_1_r,
            "target_2_r": self.target_2_r,
            "target_1_quantity": self.target_1_quantity,
            "giveback_policy": self.giveback,
            "giveback_arm_r": arm_r,
            "giveback_retrace_frac": retrace,
            "no_progress_bars": self.no_progress_bars,
            "max_hold_bars": self.max_hold_bars,
            "breakeven_after_t1": self.breakeven_after_t1,
        }


EXIT_PROFILES: dict[str, UnderlyingExitProfile] = {
    "FLASH_REVERSAL": UnderlyingExitProfile(
        name="FLASH_REVERSAL", stop_loss_pct=0.0035,
        target_1_r=1.0, target_2_r=2.0, target_1_quantity=0.75,
        giveback="STRICT", no_progress_bars=15, max_hold_bars=90,
    ),
    "EXHAUSTION_REVERSAL": UnderlyingExitProfile(
        name="EXHAUSTION_REVERSAL", stop_loss_pct=0.006,
        target_1_r=1.0, target_2_r=2.5, target_1_quantity=0.5,
        giveback="MODERATE", no_progress_bars=75, max_hold_bars=240,
    ),
    "TREND_CONTINUATION": UnderlyingExitProfile(
        name="TREND_CONTINUATION", stop_loss_pct=0.005,
        target_1_r=1.0, target_2_r=2.0, target_1_quantity=0.6,
        giveback="MODERATE", no_progress_bars=45, max_hold_bars=180,
    ),
    "RANGE_EXPANSION": UnderlyingExitProfile(
        name="RANGE_EXPANSION", stop_loss_pct=0.006,
        target_1_r=1.0, target_2_r=2.0, target_1_quantity=0.4,
        giveback="LOOSE", no_progress_bars=120, max_hold_bars=None,
    ),
}


# Operator's reference assignment of each existing strategy to a profile
# (Wave 1.2 first pass; the optimizer still tests all profiles and reports the
# best-scoring one, so this records intent and flags mismatches).
PROFILE_BY_STRATEGY: dict[str, str] = {
    "intraday_mean_reversion_extremes": "FLASH_REVERSAL",
    "elastic_band_reversion": "EXHAUSTION_REVERSAL",
    "opening_drive_classifier": "TREND_CONTINUATION",
    "jerk_pivot_momentum": "TREND_CONTINUATION",
    "market_impulse": "TREND_CONTINUATION",
    "compression_expansion_breakout": "RANGE_EXPANSION",
}


def build_profile_exit_policy(name: str) -> ProfileExitPolicy:
    """Build a ProfileExitPolicy for a named profile (case-insensitive)."""
    key = name.strip().upper()
    if key not in EXIT_PROFILES:
        raise KeyError(f"Unknown exit profile '{name}'. Known: {sorted(EXIT_PROFILES)}")
    return EXIT_PROFILES[key].build_policy()


def assigned_profile(strategy_key: str) -> str | None:
    """The operator's reference profile for a strategy key, if assigned."""
    return PROFILE_BY_STRATEGY.get(strategy_key)
