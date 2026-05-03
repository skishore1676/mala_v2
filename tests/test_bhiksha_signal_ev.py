from __future__ import annotations

import csv
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import polars as pl

from src.research.bhiksha_signal_ev import build_bhiksha_signal_ev_report


def test_bhiksha_signal_ev_report_joins_runtime_to_mala_metadata(tmp_path: Path) -> None:
    db_path = tmp_path / "bhiksha.db"
    _create_db(db_path)

    deployment_id = "strategy_market_impulse_amd_long_shadow_row_1"
    startup_payload = {
        "active_plan": {
            "active_plan_id": "active_plan_test",
            "trading_date": "2026-05-01",
            "deployments": [
                {
                    "deployment_id": deployment_id,
                    "symbol": "AMD",
                    "strategy": {
                        "key": "market_impulse",
                        "params": {"direction": "long"},
                    },
                    "risk": {"stop_loss_pct": 0.35},
                    "exit": {"stop_loss_pct": 0.35},
                    "source": {
                        "metadata": {
                            "authorization_mode": "shadow",
                            "catalog_key": "market-impulse-all-basket-discovery__amd_long",
                            "direction": "long",
                            "expectancy": 0.5,
                            "playbook_summary": {
                                "mala_evidence": {
                                    "strategy_name": "Market Impulse (Cross & Reclaim)",
                                    "signal_window_et": "09:35-11:00",
                                    "thesis_exit_metrics": {
                                        "expectancy": 0.9,
                                        "trade_count": 50,
                                        "win_rate": 0.58,
                                    },
                                }
                            },
                        }
                    },
                }
            ],
        }
    }
    signal_payload = {
        "deployment_id": deployment_id,
        "symbol": "AMD",
        "timestamp": "2026-05-01T13:36:00+00:00",
        "signal": True,
        "direction": "long",
        "reason": ["time_window_ok", "regime_bullish"],
        "features": {"close": 100.0},
    }
    trade_payload = {
        "trade_id": "trade-1",
        "deployment_id": deployment_id,
        "symbol": "AMD",
        "direction": "long",
        "option_symbol": "AMD260515C00100000",
        "quantity": 1,
        "estimated_entry_price": 4.0,
        "entry_timestamp": "2026-05-01T13:37:00+00:00",
        "risk_reasons": ["approved"],
    }
    _insert_event(db_path, "2026-05-01T13:30:00+00:00", "startup_config", startup_payload)
    _insert_event(db_path, "2026-05-01T13:36:05+00:00", "signal_decision", signal_payload)
    _insert_event(db_path, "2026-05-01T13:37:02+00:00", "trade_plan", trade_payload)
    _insert_trade_session(db_path, deployment_id)

    artifacts = build_bhiksha_signal_ev_report(db_path=db_path, out_dir=tmp_path / "out", lookback_days=7)

    assert artifacts.report_md.exists()
    trades = list(csv.DictReader(artifacts.trade_csv.open()))
    assert len(trades) == 1
    row = trades[0]
    assert row["catalog_key"] == "market-impulse-all-basket-discovery__amd_long"
    assert row["signal_match_status"] == "matched"
    assert row["concordance_status"] == "ok"
    assert row["entry_inside_signal_window"] == "yes"
    assert row["realized_pnl_usd"] == "100.0"
    assert row["realized_stop_r"] == "0.7143"
    assert row["mala_expected_r_used"] == "0.9"
    assert row["ev_alignment"] == "positive_trade"

    deployments = list(csv.DictReader(artifacts.deployment_csv.open()))
    assert deployments[0]["operator_verdict"] == "small_sample_positive"


def test_same_bar_replay_reports_missing_cached_bars(tmp_path: Path) -> None:
    db_path = tmp_path / "bhiksha.db"
    _create_db(db_path)
    deployment_id = "strategy_market_impulse_amd_long_shadow_row_1"
    _insert_event(
        db_path,
        "2026-05-01T13:30:00+00:00",
        "startup_config",
        {
            "active_plan": {
                "deployments": [
                    {
                        "deployment_id": deployment_id,
                        "symbol": "AMD",
                        "strategy": {
                            "key": "market_impulse",
                            "params": {"direction": "long"},
                        },
                        "source": {
                            "metadata": {
                                "direction": "long",
                                "playbook_summary": {
                                    "mala_evidence": {"signal_window_et": "09:35-11:00"}
                                },
                            }
                        },
                    }
                ]
            }
        },
    )
    _insert_event(
        db_path,
        "2026-05-01T13:36:05+00:00",
        "signal_decision",
        {
            "deployment_id": deployment_id,
            "symbol": "AMD",
            "timestamp": "2026-05-01T13:36:00+00:00",
            "signal": True,
            "direction": "long",
        },
    )

    artifacts = build_bhiksha_signal_ev_report(
        db_path=db_path,
        out_dir=tmp_path / "out",
        same_bar_replay=True,
        data_dir=tmp_path / "cache",
    )

    signals = list(csv.DictReader(artifacts.signal_csv.open()))
    assert signals[0]["mala_same_bar_replay_status"] == "missing_bars"


def test_same_bar_replay_compares_runtime_features_to_mala_bar(tmp_path: Path) -> None:
    db_path = tmp_path / "bhiksha.db"
    _create_db(db_path)
    deployment_id = "strategy_market_impulse_amd_long_shadow_row_1"
    _insert_event(
        db_path,
        "2026-05-01T13:30:00+00:00",
        "startup_config",
        {
            "active_plan": {
                "deployments": [
                    {
                        "deployment_id": deployment_id,
                        "symbol": "AMD",
                        "strategy": {
                            "key": "market_impulse",
                            "params": {
                                "direction": "long",
                                "entry_buffer_minutes": 3,
                                "entry_window_minutes": 60,
                                "regime_timeframe": "5m",
                            },
                        },
                        "source": {
                            "metadata": {
                                "direction": "long",
                                "playbook_summary": {
                                    "mala_evidence": {"signal_window_et": "09:33-10:30"}
                                },
                            }
                        },
                    }
                ]
            }
        },
    )
    _insert_event(
        db_path,
        "2026-05-01T13:36:05+00:00",
        "signal_decision",
        {
            "deployment_id": deployment_id,
            "symbol": "AMD",
            "timestamp": "2026-05-01T13:36:00+00:00",
            "signal": True,
            "direction": "long",
            "features": {"close": 100.0, "volume": 900.0},
        },
    )
    _write_cached_bars(tmp_path / "cache", symbol="AMD")

    artifacts = build_bhiksha_signal_ev_report(
        db_path=db_path,
        out_dir=tmp_path / "out",
        same_bar_replay=True,
        data_dir=tmp_path / "cache",
    )

    signals = list(csv.DictReader(artifacts.signal_csv.open()))
    assert signals[0]["mala_same_bar_replay_status"] != "missing_bars"
    assert signals[0]["mala_same_bar_feature_compared"] == "2"
    assert signals[0]["mala_same_bar_feature_mismatch_count"] == "1"
    assert signals[0]["mala_same_bar_feature_worst"] == "volume"


def _create_db(db_path: Path) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                event_type TEXT NOT NULL,
                payload TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE trade_sessions (
                trade_id TEXT PRIMARY KEY,
                deployment_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                option_symbol TEXT,
                quantity INTEGER NOT NULL,
                entry_price REAL,
                underlying_entry_price REAL,
                entry_timestamp TEXT,
                status TEXT NOT NULL,
                entry_order_id TEXT,
                stop_order_id TEXT,
                stop_price REAL,
                target_order_id TEXT,
                target_price REAL,
                exit_order_id TEXT,
                exit_limit_price REAL,
                exit_submitted_at TEXT,
                exit_mode TEXT,
                updated_at TEXT NOT NULL,
                exit_price REAL,
                exit_filled_quantity INTEGER,
                exit_filled_at TEXT,
                exit_order_status TEXT,
                exit_order_type TEXT,
                exit_broker_payload TEXT
            )
            """
        )


def _insert_event(db_path: Path, created_at: str, event_type: str, payload: dict) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO events(created_at, event_type, payload) VALUES (?, ?, ?)",
            (created_at, event_type, json.dumps(payload)),
        )


def _insert_trade_session(db_path: Path, deployment_id: str) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO trade_sessions(
                trade_id, deployment_id, symbol, option_symbol, quantity, entry_price,
                underlying_entry_price, entry_timestamp, status, entry_order_id,
                stop_order_id, stop_price, target_order_id, target_price, exit_order_id,
                exit_limit_price, exit_submitted_at, exit_mode, updated_at, exit_price,
                exit_filled_quantity, exit_filled_at, exit_order_status, exit_order_type,
                exit_broker_payload
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "trade-1",
                deployment_id,
                "AMD",
                "AMD260515C00100000",
                1,
                4.0,
                100.0,
                "2026-05-01T13:37:00+00:00",
                "closed",
                "entry-1",
                "stop-1",
                2.6,
                None,
                None,
                "exit-1",
                None,
                None,
                "strategy",
                "2026-05-01T14:00:00+00:00",
                5.0,
                1,
                "2026-05-01T14:00:00+00:00",
                "FILLED",
                "MARKET",
                "{}",
            ),
        )


def _write_cached_bars(base_dir: Path, *, symbol: str) -> None:
    start = datetime(2026, 5, 1, 13, 30, tzinfo=timezone.utc)
    timestamps = [start + timedelta(minutes=idx) for idx in range(40)]
    closes = [100.0 + idx * 0.01 for idx in range(40)]
    closes[6] = 100.0
    frame = pl.DataFrame(
        {
            "timestamp": timestamps,
            "ticker": [symbol] * len(timestamps),
            "open": closes,
            "high": [close + 0.1 for close in closes],
            "low": [close - 0.1 for close in closes],
            "close": closes,
            "volume": [1000.0] * len(timestamps),
        }
    )
    path = base_dir / symbol / "2026-05-01.parquet"
    path.parent.mkdir(parents=True)
    frame.write_parquet(path)
