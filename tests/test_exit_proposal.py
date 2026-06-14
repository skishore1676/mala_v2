"""Tests for the Phase-3 explore->propose selection + ManagementPolicySpec build."""

import pytest

from src.research.exit_proposal import (
    PROFILE_FAVOR_MARGIN_PCT,
    build_profile_management_policy_spec,
    propose_management_policy_spec,
    select_exit,
)
from src.research.option_translation import OPTION_PROFILES
from src.research.playbook_packet_registry import _BARS_TO_SECONDS
from src.research.shared_kernel import ensure_kernel_on_path

ensure_kernel_on_path()
from mala_bhiksha_kernel import ManagementPolicySpec  # noqa: E402


# ── select_exit: operator profile favored on ties / near-ties ─────────────────

def test_profile_wins_when_no_legacy_score():
    sel = select_exit(profile_name="FLASH_REVERSAL", profile_expectancy_pct=2.0)
    assert sel.chosen == "profile"
    assert "only candidate" in sel.rule


def test_legacy_wins_when_no_profile_score():
    sel = select_exit(
        profile_name="FLASH_REVERSAL", profile_expectancy_pct=None,
        legacy_policy="atr_trail", legacy_expectancy_pct=1.0,
    )
    assert sel.chosen == "legacy"


def test_profile_favored_on_near_tie():
    # Legacy slightly ahead but within the favor margin -> profile wins.
    sel = select_exit(
        profile_name="FLASH_REVERSAL", profile_expectancy_pct=3.0,
        legacy_policy="atr_trail", legacy_expectancy_pct=3.0 + PROFILE_FAVOR_MARGIN_PCT - 0.01,
    )
    assert sel.chosen == "profile"
    assert "live-validated" in sel.rule


def test_profile_favored_when_profile_ahead():
    sel = select_exit(
        profile_name="TREND_CONTINUATION", profile_expectancy_pct=5.0,
        legacy_policy="atr_trail", legacy_expectancy_pct=1.0,
    )
    assert sel.chosen == "profile"


def test_legacy_wins_only_when_clearly_better():
    sel = select_exit(
        profile_name="FLASH_REVERSAL", profile_expectancy_pct=2.0,
        legacy_policy="atr_trail", legacy_expectancy_pct=2.0 + PROFILE_FAVOR_MARGIN_PCT + 0.5,
    )
    assert sel.chosen == "legacy"
    assert sel.margin_pct == pytest.approx(PROFILE_FAVOR_MARGIN_PCT + 0.5)


# ── build_profile_management_policy_spec: valid kernel spec + exit DNA ─────────

@pytest.mark.parametrize("profile_name", sorted(OPTION_PROFILES))
def test_spec_builds_and_validates_for_every_profile(profile_name):
    spec = build_profile_management_policy_spec(profile_name)
    assert isinstance(spec, ManagementPolicySpec)
    profile = OPTION_PROFILES[profile_name]
    # Exit DNA carried verbatim from OPTION_PROFILES.
    assert spec.initial_stop_pct == profile["stop_loss_pct"]
    assert spec.premium_disaster_stop_pct == profile["disaster_stop_pct"]
    assert spec.target_1_r == profile["target_1_r"]
    assert spec.target_2_r == profile["target_2_r"]
    assert spec.target_1_quantity == profile["target_1_quantity"]
    assert spec.high_water_giveback_policy == profile["giveback"]
    assert spec.eod_flat == profile["eod_flat"]
    # Time bounds convert 1-min bars -> wall-clock seconds (None stays None).
    npb = profile["no_progress_bars"]
    assert spec.no_progress_seconds == (None if npb is None else npb * _BARS_TO_SECONDS)
    mhb = profile["max_hold_bars"]
    assert spec.max_hold_seconds == (None if mhb is None else mhb * _BARS_TO_SECONDS)


def test_range_expansion_rides_past_close():
    # RANGE_EXPANSION has max_hold_bars=None and eod_flat=False (multi-day ride).
    spec = build_profile_management_policy_spec("RANGE_EXPANSION")
    assert spec.max_hold_seconds is None
    assert spec.eod_flat is False
    assert spec.high_water_giveback_policy == "LOOSE"


def test_unknown_profile_raises():
    with pytest.raises(KeyError):
        build_profile_management_policy_spec("NOT_A_PROFILE")


def test_spec_does_not_carry_profile_exit_drives_live():
    # profile_exit_drives_live is not part of the spec and must never be set.
    spec = build_profile_management_policy_spec("FLASH_REVERSAL")
    dumped = spec.model_dump(mode="json")
    assert "profile_exit_drives_live" not in dumped


# ── propose_management_policy_spec: serialized contract ───────────────────────

def test_propose_emits_kernel_json_round_trip():
    sel = select_exit(
        profile_name="EXHAUSTION_REVERSAL", profile_expectancy_pct=4.0,
        legacy_policy="atr_trail", legacy_expectancy_pct=3.9,
    )
    spec_json = propose_management_policy_spec(sel, source_config_id="cfg-7")
    assert isinstance(spec_json, dict)
    assert spec_json["policy_id"] == "profile__exhaustion_reversal"
    assert spec_json["source_config_id"] == "cfg-7"
    # The selection record is stamped into parameters for operator visibility.
    assert spec_json["parameters"]["selected_exit"]["chosen"] == "profile"
    # Round-trips back into the kernel model (proves it is a valid contract).
    rebuilt = ManagementPolicySpec.model_validate(spec_json)
    assert rebuilt.policy_id == "profile__exhaustion_reversal"
    assert rebuilt.target_2_r == OPTION_PROFILES["EXHAUSTION_REVERSAL"]["target_2_r"]


def test_propose_when_legacy_chosen_still_serializes_profile_spec_with_record():
    sel = select_exit(
        profile_name="FLASH_REVERSAL", profile_expectancy_pct=1.0,
        legacy_policy="atr_trail", legacy_expectancy_pct=5.0,
    )
    assert sel.chosen == "legacy"
    spec_json = propose_management_policy_spec(sel)
    # Still a valid spec; the selection record shows legacy won the comparison.
    ManagementPolicySpec.model_validate(spec_json)
    assert spec_json["parameters"]["selected_exit"]["chosen"] == "legacy"
