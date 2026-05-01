from __future__ import annotations

from datetime import datetime, timezone

import polars as pl

from src.oracle.trade_simulator import FixedPercentRewardRiskExitPolicy
from src.research.catalog_volume_sensitivity import (
    CatalogRow,
    _build_exit_policy,
    _build_strategy_for_row,
    _normalized_params,
    _shuffle_volume_intraday,
)
from src.strategy.market_impulse import MarketImpulseStrategy


def test_normalized_params_coerces_catalog_csv_values() -> None:
    params = _normalized_params(
        {
            "entry_buffer_minutes": "5",
            "vwma_periods": "5,13,21",
            "use_volume_filter": "FALSE",
            "z_score_threshold": "1.25",
        }
    )

    assert params == {
        "entry_buffer_minutes": 5,
        "vwma_periods": (5, 13, 21),
        "use_volume_filter": False,
        "z_score_threshold": 1.25,
    }


def test_build_strategy_drops_catalog_direction_metadata() -> None:
    row = CatalogRow(
        catalog_key="mi_iwm_long",
        symbol="IWM",
        strategy_key="market_impulse",
        direction="long",
        thesis_exit_policy="fixed_rr_underlying",
        thesis_exit_params={},
        playbook_summary={"entry_params": {"direction": "long", "entry_buffer_minutes": "5"}},
        raw_row={},
    )

    strategy = _build_strategy_for_row(row, scenario="polygon_baseline")

    assert isinstance(strategy, MarketImpulseStrategy)
    assert strategy.entry_buffer_minutes == 5


def test_build_exit_policy_uses_catalog_fixed_rr_params() -> None:
    row = CatalogRow(
        catalog_key="mi_iwm_long",
        symbol="IWM",
        strategy_key="market_impulse",
        direction="long",
        thesis_exit_policy="fixed_rr_underlying",
        thesis_exit_params={
            "stop_loss_underlying_pct": 0.005,
            "take_profit_underlying_r_multiple": 2.0,
        },
        playbook_summary={},
        raw_row={},
    )

    policy = _build_exit_policy(row, MarketImpulseStrategy())

    assert isinstance(policy, FixedPercentRewardRiskExitPolicy)
    assert policy.stop_loss_pct == 0.005
    assert policy.reward_multiple == 2.0


def test_shuffle_volume_intraday_preserves_daily_volume_set() -> None:
    frame = pl.DataFrame(
        {
            "timestamp": [
                datetime(2026, 1, 2, 14, 30, tzinfo=timezone.utc),
                datetime(2026, 1, 2, 14, 31, tzinfo=timezone.utc),
                datetime(2026, 1, 5, 14, 30, tzinfo=timezone.utc),
                datetime(2026, 1, 5, 14, 31, tzinfo=timezone.utc),
            ],
            "open": [1.0] * 4,
            "high": [1.0] * 4,
            "low": [1.0] * 4,
            "close": [1.0] * 4,
            "volume": [10.0, 20.0, 30.0, 40.0],
        }
    )

    shuffled = _shuffle_volume_intraday(frame, symbol="TST", seed=7)

    assert sorted(shuffled["volume"][:2].to_list()) == [10.0, 20.0]
    assert sorted(shuffled["volume"][2:].to_list()) == [30.0, 40.0]
