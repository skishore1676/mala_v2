from __future__ import annotations

from datetime import date, datetime, timezone

import polars as pl

from src.research.classical_patterns.exit_experiment_v2 import (
    _assign_analysis_periods,
    _edge_verdict,
    _profile_daily_bars,
    _verify_trade_population,
)


def _trade_row(
    signal_id: str,
    variant_id: str,
    *,
    breakout_date: str = "2024-06-01",
    exit_date: str | None = "2024-06-10",
    status: str = "closed",
) -> dict[str, object]:
    return {
        "signal_id": signal_id,
        "variant_id": variant_id,
        "breakout_date": breakout_date,
        "exit_date": exit_date,
        "status": status,
    }


def test_analysis_period_purges_every_variant_when_one_crosses_boundary() -> None:
    rows = [
        _trade_row("signal:a", "v1", exit_date="2024-12-30"),
        _trade_row("signal:a", "v2", exit_date="2025-01-03"),
        _trade_row(
            "signal:b",
            "v1",
            breakout_date="2025-01-02",
            exit_date="2025-01-08",
        ),
    ]

    _assign_analysis_periods(rows)

    assert [row["analysis_period"] for row in rows] == [
        "purged_boundary",
        "purged_boundary",
        "out_of_sample",
    ]


def test_trade_population_requires_all_four_variants_per_signal() -> None:
    variants = (
        "rectangle_height_lfd_buffer_0p00atr",
        "rectangle_height_lfd_buffer_0p10atr",
        "range_expansion_lfd_buffer_0p00atr",
        "range_expansion_lfd_buffer_0p10atr",
    )
    rows = [_trade_row("signal:a", variant) for variant in variants]

    _verify_trade_population(rows, [{"signal_id": "signal:a"}])


def test_daily_quality_profile_checks_grain_and_ohlc() -> None:
    frame = pl.DataFrame(
        {
            "session_date": [date(2024, 1, 2), date(2024, 1, 3)],
            "visible_at": [
                datetime(2024, 1, 2, 21, tzinfo=timezone.utc),
                datetime(2024, 1, 3, 21, tzinfo=timezone.utc),
            ],
            "symbol": ["TEST", "TEST"],
            "open": [100.0, 101.0],
            "high": [102.0, 103.0],
            "low": [99.0, 100.0],
            "close": [101.0, 102.0],
            "volume": [1_000.0, 1_100.0],
        }
    )

    quality = _profile_daily_bars(frame)

    assert quality["status"] == "ready"
    assert quality["duplicate_symbol_dates"] == 0
    assert quality["invalid_ohlc_rows"] == 0


def test_edge_verdict_rejects_negative_oos_expectancy() -> None:
    verdict = _edge_verdict(
        {
            "signal_count": 33,
            "mean_net_r_per_signal": -0.05,
            "profit_factor": 0.90,
            "symbol_cluster_ci95_lower": -0.25,
        }
    )

    assert verdict["code"] == "no_out_of_sample_edge"
