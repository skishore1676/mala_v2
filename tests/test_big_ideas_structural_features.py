from __future__ import annotations

from datetime import datetime, timezone

import polars as pl

from src.newton.auction_proxy import auction_proxy_features
from src.newton.vpoc_daily import add_vpoc_drift, daily_vpoc
from src.research.structural_breaks import latest_structural_break
from src.research.structural_breaks_feed import build_feed_payload


def _dt(day: int, hour: int = 14, minute: int = 0) -> datetime:
    return datetime(2026, 1, day, hour, minute, tzinfo=timezone.utc)


def test_structural_break_scores_weekly_reclaim_range_and_acceptance() -> None:
    daily = pl.DataFrame({
        "trade_date": [datetime(2026, 1, day).date() for day in range(1, 24)],
        "open": [100.0] * 23,
        "high": [101.0] * 22 + [112.0],
        "low": [99.0] * 22 + [101.0],
        "close": [100.0 + day * 0.1 for day in range(22)] + [111.0],
        "volume": [1_000_000] * 23,
    })
    minutes = pl.DataFrame({
        "timestamp": [_dt(23, 14), _dt(23, 19), _dt(23, 20)],
        "open": [102.0, 104.0, 108.0],
        "high": [104.0, 108.0, 112.0],
        "low": [101.0, 103.0, 107.0],
        "close": [103.0, 107.0, 111.0],
        "volume": [100, 10_000, 5_000],
    })

    row = latest_structural_break(daily, symbol="NVDA", minute_bars=minutes, as_of="2026-01-23")

    assert row is not None
    assert row.weekly_reclaim_long is True
    assert row.daily_range_expansion is True
    assert row.intraday_volume_acceptance_long is True
    assert row.confluence_score_long == 3


def test_auction_proxy_uses_last_minute_volume_share() -> None:
    frame = pl.DataFrame({
        "timestamp": [
            datetime(2026, 7, 1, 19, 59, tzinfo=timezone.utc),
            datetime(2026, 7, 1, 20, 0, tzinfo=timezone.utc),
            datetime(2026, 7, 2, 19, 59, tzinfo=timezone.utc),
            datetime(2026, 7, 2, 20, 0, tzinfo=timezone.utc),
        ],
        "open": [10.0, 10.1, 10.2, 10.4],
        "high": [10.2, 10.3, 10.5, 10.8],
        "low": [9.9, 10.0, 10.1, 10.3],
        "close": [10.1, 10.2, 10.4, 10.7],
        "volume": [900, 100, 500, 500],
    })

    features = auction_proxy_features(frame, lookback_days=2)

    assert features.height == 2
    assert features.tail(1).to_dicts()[0]["last_minute_share"] == 0.5


def test_auction_proxy_handles_standard_time_close() -> None:
    frame = pl.DataFrame({
        "timestamp": [
            datetime(2026, 1, 2, 20, 59, tzinfo=timezone.utc),
            datetime(2026, 1, 2, 21, 0, tzinfo=timezone.utc),
        ],
        "open": [10.0, 10.1],
        "high": [10.2, 10.3],
        "low": [9.9, 10.0],
        "close": [10.1, 10.2],
        "volume": [900, 100],
    })

    features = auction_proxy_features(frame)

    assert features.height == 1
    assert features.to_dicts()[0]["last_minute_share"] == 0.1


def test_daily_vpoc_groups_by_et_trade_date() -> None:
    frame = pl.DataFrame({
        "timestamp": [
            datetime(2026, 1, 2, 14, 30, tzinfo=timezone.utc),
            datetime(2026, 1, 2, 21, 0, tzinfo=timezone.utc),
            datetime(2026, 1, 3, 0, 30, tzinfo=timezone.utc),
        ],
        "close": [100.0, 101.0, 102.0],
        "volume": [10, 100, 1_000],
    })

    vpoc = daily_vpoc(frame, price_round=1.0)

    assert vpoc.height == 1
    assert vpoc.to_dicts()[0]["trade_date"].isoformat() == "2026-01-02"
    assert vpoc.to_dicts()[0]["vpoc"] == 102.0


def test_vpoc_daily_and_drift_detect_direction() -> None:
    frame = pl.DataFrame({
        "timestamp": [_dt(1), _dt(1, 15), _dt(2), _dt(2, 15), _dt(3), _dt(3, 15)],
        "close": [100.0, 101.0, 101.0, 102.0, 102.0, 103.0],
        "volume": [10, 100, 10, 100, 10, 100],
    })

    vpoc = daily_vpoc(frame, price_round=1.0)
    drift = add_vpoc_drift(vpoc, window=2, min_sessions=2)

    assert vpoc["vpoc"].to_list() == [101.0, 102.0, 103.0]
    assert drift.tail(1).to_dicts()[0]["vpoc_drift"] == 2.0


def test_structural_break_feed_does_not_emit_stale_signal(tmp_path) -> None:
    ticker_dir = tmp_path / "SPY"
    ticker_dir.mkdir()
    timestamps = [
        datetime(2026, 2, day, 14, 30, tzinfo=timezone.utc)
        for day in range(15, 29)
    ] + [
        datetime(2026, 3, day, 14, 30, tzinfo=timezone.utc)
        for day in range(1, 10)
    ]
    pl.DataFrame({
        "timestamp": timestamps,
        "ticker": ["SPY"] * len(timestamps),
        "open": [100.0] * len(timestamps),
        "high": [101.0] * len(timestamps),
        "low": [99.0] * len(timestamps),
        "close": [100.0 + index * 0.1 for index in range(len(timestamps))],
        "volume": [1_000.0] * len(timestamps),
    }).write_parquet(ticker_dir / "2026-03-09.parquet")

    payload = build_feed_payload(
        symbols=["SPY"],
        trade_date=datetime(2026, 5, 13).date(),
        data_dir=tmp_path,
    )

    row = payload["symbols"][0]
    assert row["date"] == "2026-05-13"
    assert row["confluence_score"] == 0
    assert row["notes"] == "latest cached bar is 2026-03-09"
