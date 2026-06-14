"""Phase 3: propose the best exit for a classified row as a kernel contract.

Given a row's classified playbook profile plus the option-path scores for the
profile exit and the legacy exit, this module:

1. SELECTS the best exit (``select_exit``) by option-path expectancy, with the
   operator profile *favored* when it is comparable to legacy -- the operator
   profile is live-validated and tighter, so a tie or near-tie goes to it.
2. SERIALIZES the proposed exit as a kernel ``ManagementPolicySpec`` JSON
   (``propose_management_policy_spec``) -- the exact contract the bhiksha
   active_plan compiler reads from the ``management_policy_spec`` Sheet cell.

The spec is built from the operator's ``OPTION_PROFILES`` exit DNA (premium-%
stop, staged R targets, partial bank, giveback tier, time bounds, disaster stop,
EOD flatten) via the same kernel field mapping used by
``playbook_packet_registry``, so it round-trips identically. We do NOT set
``profile_exit_drives_live`` -- it is not part of the spec, and live remains the
operator's manual flip.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from src.research.option_translation import OPTION_PROFILES
from src.research.playbook_packet_registry import (
    _BARS_TO_SECONDS,
    _GIVEBACK_TO_POLICY,
)
from src.research.shared_kernel import ensure_kernel_on_path

ensure_kernel_on_path()

from mala_bhiksha_kernel import ManagementPolicySpec  # noqa: E402


# When the operator profile is within this expectancy margin (percentage points
# of premium per trade) of the best legacy exit, prefer the profile: it is the
# operator's live-validated, tighter exit DNA. Only a clearly better legacy
# number (beyond this margin) overrides it.
PROFILE_FAVOR_MARGIN_PCT = 0.50

# Each profile's stop_family / exit_family for the kernel spec. Reversion fades
# anchor on the reversal extreme; momentum/breakout anchor on the entry-bar
# failure. exit_family carries the staged-R target model (target_1_r/target_2_r
# live in the additive spec fields).
_PROFILE_FAMILIES: dict[str, tuple[str, str]] = {
    "FLASH_REVERSAL": ("reversal_extreme", "profile_staged_r"),
    "EXHAUSTION_REVERSAL": ("reversal_extreme", "profile_staged_r"),
    "TREND_CONTINUATION": ("entry_bar_failure", "profile_staged_r"),
    "RANGE_EXPANSION": ("entry_bar_failure", "profile_staged_r"),
}


@dataclass(frozen=True)
class ExitSelection:
    """Which exit won, and why."""

    chosen: str  # "profile" | "legacy"
    profile_name: str
    profile_expectancy_pct: float | None
    legacy_policy: str | None
    legacy_expectancy_pct: float | None
    margin_pct: float | None
    rule: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "chosen": self.chosen,
            "profile_name": self.profile_name,
            "profile_expectancy_pct": self.profile_expectancy_pct,
            "legacy_policy": self.legacy_policy,
            "legacy_expectancy_pct": self.legacy_expectancy_pct,
            "margin_pct": self.margin_pct,
            "rule": self.rule,
        }


def select_exit(
    *,
    profile_name: str,
    profile_expectancy_pct: float | None,
    legacy_policy: str | None = None,
    legacy_expectancy_pct: float | None = None,
    favor_margin_pct: float = PROFILE_FAVOR_MARGIN_PCT,
) -> ExitSelection:
    """Pick profile vs legacy on option-path expectancy, profile-favored on ties.

    Decision rule (deterministic):
      * No legacy number available -> profile wins (only candidate).
      * No profile number available -> legacy wins.
      * Otherwise legacy wins only if it beats the profile by MORE than
        ``favor_margin_pct``; any tie / near-tie / profile-ahead -> profile,
        because the operator profile is live-validated and tighter.
    """
    p = profile_expectancy_pct
    legacy = legacy_expectancy_pct
    margin = None if (p is None or legacy is None) else round(legacy - p, 6)

    if legacy is None:
        return ExitSelection(
            chosen="profile", profile_name=profile_name,
            profile_expectancy_pct=p, legacy_policy=legacy_policy,
            legacy_expectancy_pct=legacy, margin_pct=margin,
            rule="no legacy score -> operator profile is the only candidate",
        )
    if p is None:
        return ExitSelection(
            chosen="legacy", profile_name=profile_name,
            profile_expectancy_pct=p, legacy_policy=legacy_policy,
            legacy_expectancy_pct=legacy, margin_pct=margin,
            rule="no profile score -> fall back to legacy exit",
        )
    if legacy - p > favor_margin_pct:
        return ExitSelection(
            chosen="legacy", profile_name=profile_name,
            profile_expectancy_pct=p, legacy_policy=legacy_policy,
            legacy_expectancy_pct=legacy, margin_pct=margin,
            rule=(
                f"legacy beats profile by {legacy - p:+.3f} pct > "
                f"favor margin {favor_margin_pct:.2f} -> legacy"
            ),
        )
    if p >= legacy:
        reason = (
            f"profile expectancy ahead of legacy by {p - legacy:+.3f} pct "
            "-> operator profile wins outright"
        )
    else:
        reason = (
            f"legacy ahead by only {legacy - p:+.3f} pct (<= favor margin "
            f"{favor_margin_pct:.2f}) -> prefer live-validated, tighter operator profile"
        )
    return ExitSelection(
        chosen="profile", profile_name=profile_name,
        profile_expectancy_pct=p, legacy_policy=legacy_policy,
        legacy_expectancy_pct=legacy, margin_pct=margin,
        rule=reason,
    )


def _profile_spec_fields(profile_name: str) -> dict[str, Any]:
    """Map a profile's OPTION_PROFILES exit DNA onto the additive v2 spec fields.

    Mirrors ``playbook_packet_registry._exit_profile_spec_fields`` but keyed on
    an explicit profile name (the classifier's pick) rather than going through
    ``PROFILE_BY_STRATEGY`` -- so a heuristically classified strategy that is not
    in that table still gets the full exit contract.
    """
    profile = OPTION_PROFILES[profile_name]
    no_progress_bars = profile.get("no_progress_bars")
    max_hold_bars = profile.get("max_hold_bars")
    return {
        "initial_stop_pct": profile["stop_loss_pct"],
        "premium_disaster_stop_pct": profile["disaster_stop_pct"],
        "target_1_r": profile["target_1_r"],
        "target_2_r": profile["target_2_r"],
        "target_1_quantity": profile["target_1_quantity"],
        "no_progress_seconds": (
            None if no_progress_bars is None else int(no_progress_bars) * _BARS_TO_SECONDS
        ),
        "max_hold_seconds": (
            None if max_hold_bars is None else int(max_hold_bars) * _BARS_TO_SECONDS
        ),
        "high_water_giveback_policy": _GIVEBACK_TO_POLICY[profile["giveback"]],
        "breakeven_after_t1": True,
        "eod_flat": profile["eod_flat"],
    }


def build_profile_management_policy_spec(
    profile_name: str,
    *,
    source_config_id: str | None = None,
    parameters: dict[str, Any] | None = None,
) -> ManagementPolicySpec:
    """Build the kernel ManagementPolicySpec for a profile (option-premium DNA).

    The non-profile fields (target_model, target_r, hard_flat_time_et,
    option_stop_fallback_pct, target_order_mode) reproduce the
    ``playbook_packet_registry._management_policy_spec`` defaults; the additive
    v2 fields carry the operator's profile exit DNA.
    """
    key = profile_name.strip().upper()
    if key not in OPTION_PROFILES:
        raise KeyError(f"Unknown profile {profile_name!r}; known: {sorted(OPTION_PROFILES)}")
    stop_family, exit_family = _PROFILE_FAMILIES[key]
    profile = OPTION_PROFILES[key]
    return ManagementPolicySpec(
        policy_id=f"profile__{key.lower()}",
        stop_family=stop_family,
        stop_anchor=f"underlying_{stop_family}",
        exit_family=exit_family,
        # staged-R profile: the single-shot target_r mirrors the profile's runner
        # target (target_2_r); staged targets live in the additive fields.
        target_model="profile_staged_r",
        target_r=float(profile["target_2_r"]),
        hard_flat_time_et="15:55",
        option_stop_fallback_pct=0.45,
        target_order_mode="virtual_or_broker",
        source_config_id=source_config_id,
        parameters=dict(parameters or {}),
        **_profile_spec_fields(key),
    )


def propose_management_policy_spec(
    selection: ExitSelection,
    *,
    source_config_id: str | None = None,
    parameters: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Serialize the chosen exit as ``management_policy_spec`` JSON for bhiksha.

    Only the operator profile is serialized as a full exit-DNA spec (that is the
    contract bhiksha consumes). When ``select_exit`` chose legacy, we still emit
    the profile spec as the *proposal* but stamp ``selected_exit`` so the operator
    sees the profile lost the comparison -- the legacy underlying exit is not a
    kernel ManagementPolicySpec and stays Mala/operator-owned.
    """
    base_params = dict(parameters or {})
    base_params["selected_exit"] = selection.to_dict()
    spec = build_profile_management_policy_spec(
        selection.profile_name,
        source_config_id=source_config_id,
        parameters=base_params,
    )
    return spec.model_dump(mode="json")


__all__ = [
    "ExitSelection",
    "select_exit",
    "build_profile_management_policy_spec",
    "propose_management_policy_spec",
    "PROFILE_FAVOR_MARGIN_PCT",
]
