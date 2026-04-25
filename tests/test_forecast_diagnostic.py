from __future__ import annotations

from datetime import datetime, timedelta, timezone

import polars as pl

from src.research.forecast_diagnostic import (
    build_forecast_feature_frame,
    summarize_forecast_edges,
)


def _frame(closes: list[float]) -> pl.DataFrame:
    start = datetime(2024, 1, 2, 14, 30, tzinfo=timezone.utc)
    return pl.DataFrame(
        {
            "timestamp": [start + timedelta(minutes=i) for i in range(len(closes))],
            "ticker": ["SPY"] * len(closes),
            "close": closes,
        }
    )


def test_forecast_rows_are_lag_safe_for_lag_return_model() -> None:
    df = _frame([100.0, 101.0, 103.0, 106.0, 110.0])

    rows = build_forecast_feature_frame(
        df,
        ticker="SPY",
        horizons=[2],
        models=["lag_return"],
    ).sort("timestamp")

    second_valid = rows.filter(pl.col("timestamp") == df["timestamp"][2]).row(0, named=True)
    assert round(second_valid["forecast_return"], 6) == 0.03
    assert round(second_valid["actual_return"], 6) == round((110.0 / 103.0) - 1.0, 6)


def test_forecast_summary_marks_strong_synthetic_momentum_as_pass() -> None:
    closes = [100.0 + i for i in range(800)]
    feature_rows = build_forecast_feature_frame(
        _frame(closes),
        ticker="SPY",
        horizons=[5],
        models=["momentum_30"],
    )

    summary = summarize_forecast_edges(
        feature_rows,
        min_rows=100,
        min_top_quartile_signed_return_bps=0.1,
    )

    row = summary.row(0, named=True)
    assert row["verdict"] == "pass"
    assert row["direction_hit_rate"] == 1.0
    assert row["top_quartile_avg_signed_return_bps"] > 0
