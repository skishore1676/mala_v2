from __future__ import annotations

from src.research.search_space import build_search_configs, search_param_keys


def test_opening_drive_search_space_uses_strategy_constraints() -> None:
    configs = build_search_configs(
        "Opening Drive Classifier",
        mode="discovery",
        max_configs=64,
    )

    assert configs
    assert len(configs) <= 64
    for config in configs:
        assert config["opening_window_minutes"] < config["entry_start_offset_minutes"]
        assert config["entry_start_offset_minutes"] < config["entry_end_offset_minutes"]
        if not config.get("use_volume_filter", True):
            assert "volume_multiplier" not in config
        if not config.get("use_regime_filter", False):
            assert "regime_timeframe" not in config


def test_retune_search_space_is_narrower_than_discovery() -> None:
    discovery = build_search_configs("Elastic Band Reversion", mode="discovery", max_configs=256)
    retune = build_search_configs("Elastic Band Reversion", mode="retune", max_configs=256)

    assert retune
    assert len(retune) < len(discovery)


def test_search_param_keys_come_from_strategy_surface() -> None:
    assert "entry_window_minutes" in search_param_keys("Market Impulse (Cross & Reclaim)")
    keys = search_param_keys("Market Impulse Descendants")
    assert "entry_mode" in keys
    assert "max_vma_excursion_pct" in keys
    assert "confirmation_window_bars" in keys


def test_search_space_can_sample_single_config() -> None:
    configs = build_search_configs(
        "Opening Drive Classifier",
        mode="discovery",
        max_configs=1,
    )

    assert len(configs) == 1


def test_fixed_search_mode_replays_parametric_strategy() -> None:
    configs = build_search_configs(
        "Elastic Band z=3.0/w=120+dm",
        mode="fixed",
    )

    assert configs == [
        {
            "z_score_threshold": 3.0,
            "z_score_window": 120,
            "kinematic_periods_back": 1,
            "use_directional_mass": True,
            "use_jerk_confirmation": True,
        }
    ]


def test_market_impulse_descendant_search_space_is_bounded_and_mode_valid() -> None:
    configs = build_search_configs(
        "Market Impulse Descendants",
        mode="discovery",
        max_configs=32,
    )

    assert configs
    assert len(configs) <= 32
    modes = {config["entry_mode"] for config in configs}
    assert modes <= {
        "same_bar_shallow_reclaim",
        "delayed_reclaim",
        "close_location_reclaim",
        "continuation_confirmation",
    }
    assert modes
    for config in configs:
        assert config["entry_buffer_minutes"] < config["entry_window_minutes"]
        if config["entry_mode"] != "delayed_reclaim":
            assert "reclaim_window_bars" not in config
        if config["entry_mode"] != "continuation_confirmation":
            assert "confirmation_type" not in config
        if not config.get("use_volume_filter", False):
            assert "min_relative_volume" not in config
