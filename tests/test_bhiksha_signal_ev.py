from __future__ import annotations

import csv
import json
import sqlite3
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import polars as pl

import src.research.bhiksha_signal_ev as signal_ev
from src.research.bhiksha_signal_ev import build_bhiksha_signal_ev_report


def test_replay_warmup_uses_startup_contract_before_legacy_fallback() -> None:
    deployment = {"deployment_id": "dep_1", "symbol": "MU", "strategy": {"key": "market_impulse"}}

    assert (
        signal_ev._resolve_replay_warmup_trading_days(
            snapshot={"warmup": {"by_deployment": {"dep_1": 9}}, "app": {"warmup_trading_days": 2}},
            deployment_id="dep_1",
            deployment=deployment,
            override_days=0,
        )
        == 9
    )
    assert (
        signal_ev._resolve_replay_warmup_trading_days(
            snapshot={"app": {"warmup_trading_days": 2}},
            deployment_id="dep_1",
            deployment=deployment,
            override_days=0,
        )
        == 5
    )


def test_replay_start_date_counts_trading_days_like_bhiksha_runtime() -> None:
    assert signal_ev._replay_start_date(date(2026, 5, 8), 5) == date(2026, 5, 4)
    assert signal_ev._replay_start_date(date(2026, 5, 8), 9) == date(2026, 4, 28)


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
    report = artifacts.report_md.read_text(encoding="utf-8")
    assert "Signal Expected Value" in report
    assert "## Metric Glossary" in report
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


def test_counterfactual_replay_finds_matched_missed_and_extra_signals(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "bhiksha.db"
    _create_db(db_path)
    deployment_id = "strategy_market_impulse_amd_long_shadow_row_1"
    _insert_event(
        db_path,
        "2026-05-01T13:30:00+00:00",
        "startup_config",
        {
            "active_plan": {
                "active_plan_id": "active_plan_test",
                "trading_date": "2026-05-01",
                "deployments": [
                    {
                        "deployment_id": deployment_id,
                        "symbol": "AMD",
                        "strategy": {"key": "market_impulse", "params": {"direction": "long"}},
                        "source": {
                            "metadata": {
                                "authorization_mode": "shadow",
                                "catalog_key": "market-impulse-amd-long",
                                "direction": "long",
                                "playbook_summary": {
                                    "mala_evidence": {
                                        "strategy_name": "Market Impulse (Cross & Reclaim)",
                                        "signal_window_et": "09:35-09:40",
                                        "thesis_exit_metrics": {
                                            "expectancy": 0.5,
                                            "trade_count": 20,
                                            "win_rate": 0.55,
                                        },
                                    }
                                },
                            }
                        },
                    }
                ],
            }
        },
    )
    for timestamp in ("2026-05-01T13:36:00+00:00", "2026-05-01T13:38:00+00:00"):
        _insert_event(
            db_path,
            timestamp,
            "signal_decision",
            {
                "deployment_id": deployment_id,
                "symbol": "AMD",
                "timestamp": timestamp,
                "signal": True,
                "direction": "long",
                "reason": ["runtime_signal"],
            },
        )
    _insert_event(
        db_path,
        "2026-05-01T13:37:01+00:00",
        "signal_evaluation",
        {
            "deployment_id": deployment_id,
            "symbol": "AMD",
            "timestamp": "2026-05-01T13:37:00+00:00",
            "signal": False,
            "direction": None,
            "reason": ["volume_gate_blocked"],
        },
    )

    class FakeReplayCache:
        def __init__(self, data_dir: Path) -> None:
            pass

        def signal_frame(self, **kwargs) -> pl.DataFrame:
            return pl.DataFrame(
                {
                    "timestamp": [
                        datetime(2026, 5, 1, 13, 36, tzinfo=timezone.utc),
                        datetime(2026, 5, 1, 13, 37, tzinfo=timezone.utc),
                        datetime(2026, 5, 1, 13, 38, tzinfo=timezone.utc),
                    ],
                    "close": [100.0, 100.0, 101.0],
                    "high": [100.1, 100.5, 103.0],
                    "low": [99.9, 99.0, 100.5],
                    "signal": [True, True, False],
                    "signal_direction": ["long", "long", None],
                }
            )

    monkeypatch.setattr(signal_ev, "_ReplayCache", FakeReplayCache)

    artifacts = build_bhiksha_signal_ev_report(
        db_path=db_path,
        out_dir=tmp_path / "out",
        lookback_days=7,
        counterfactual_replay=True,
        data_dir=tmp_path / "cache",
    )

    rows = list(csv.DictReader(artifacts.counterfactual_csv.open()))
    assert [row["counterfactual_status"] for row in rows] == [
        "matched_actual",
        "missed_by_bhiksha",
        "extra_bhiksha_signal",
    ]
    assert rows[1]["bhiksha_evaluation_reason"] == "volume_gate_blocked"
    assert rows[1]["root_cause"] == "runtime_volume_gate"
    summary = list(csv.DictReader(artifacts.counterfactual_summary_csv.open()))
    assert summary[0]["mala_expected_signals"] == "2"
    assert summary[0]["matched_actual_signals"] == "1"
    assert summary[0]["missed_mala_signals"] == "1"
    assert summary[0]["extra_bhiksha_signals"] == "1"
    assert summary[0]["mala_counterfactual_win_rate"] == "1.0"
    assert summary[0]["mala_avg_underlying_mfe_points"] == "3.0"
    assert summary[0]["mala_avg_underlying_mae_points"] == "0.5"
    assert summary[0]["mala_avg_thesis_expectancy_r"] == "0.5"
    assert "runtime_volume_gate:1" in summary[0]["top_root_causes"]


def test_deployment_lookup_does_not_attach_future_snapshot(tmp_path: Path) -> None:
    db_path = tmp_path / "bhiksha.db"
    _create_db(db_path)
    old_deployment_id = "strategy_market_impulse_amd_long_old"
    new_deployment_id = "strategy_market_impulse_amd_long_new"
    _insert_event(
        db_path,
        "2026-05-01T13:30:00+00:00",
        "startup_config",
        {
            "active_plan": {
                "active_plan_id": "active_plan_2026-05-01",
                "trading_date": "2026-05-01",
                "deployments": [
                    {
                        "deployment_id": old_deployment_id,
                        "symbol": "AMD",
                        "strategy": {"key": "market_impulse", "params": {"direction": "long"}},
                        "source": {
                            "metadata": {
                                "catalog_key": "old-row",
                                "direction": "long",
                            }
                        },
                    }
                ],
            }
        },
    )
    _insert_event(
        db_path,
        "2026-05-01T13:36:05+00:00",
        "signal_decision",
        {
            "deployment_id": old_deployment_id,
            "symbol": "AMD",
            "timestamp": "2026-05-01T13:36:00+00:00",
            "signal": True,
            "direction": "long",
        },
    )
    _insert_event(
        db_path,
        "2026-05-01T13:40:00+00:00",
        "startup_config",
        {
            "active_plan": {
                "active_plan_id": "active_plan_2026-05-02",
                "trading_date": "2026-05-02",
                "deployments": [
                    {
                        "deployment_id": old_deployment_id,
                        "symbol": "AMD",
                        "strategy": {"key": "market_impulse", "params": {"direction": "long"}},
                        "source": {
                            "metadata": {
                                "catalog_key": "future-row",
                                "direction": "long",
                            }
                        },
                    },
                    {
                        "deployment_id": new_deployment_id,
                        "symbol": "AMD",
                        "strategy": {"key": "market_impulse", "params": {"direction": "long"}},
                    },
                ],
            }
        },
    )

    artifacts = build_bhiksha_signal_ev_report(db_path=db_path, out_dir=tmp_path / "out", lookback_days=7)

    signals = list(csv.DictReader(artifacts.signal_csv.open()))
    assert signals[0]["active_plan_id"] == "active_plan_2026-05-01"
    assert signals[0]["catalog_key"] == "old-row"


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
