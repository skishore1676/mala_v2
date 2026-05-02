from __future__ import annotations

import csv
import json
from pathlib import Path

from src.research.mala_handoff import (
    build_handoff_packets,
    derive_signal_window,
    review_thesis_exit_metrics,
    write_handoff_outputs,
)


def test_market_impulse_signal_window_is_authoritative_from_params() -> None:
    start, end, derivation = derive_signal_window(
        "market_impulse",
        {"entry_buffer_minutes": 5, "entry_window_minutes": 45},
    )

    assert (start, end) == ("09:35", "10:15")
    assert "entry_buffer_minutes" in derivation


def test_opening_drive_signal_window_is_authoritative_from_offsets() -> None:
    start, end, derivation = derive_signal_window(
        "opening_drive_classifier",
        {"entry_start_offset_minutes": 25, "entry_end_offset_minutes": 120},
    )

    assert (start, end) == ("09:55", "11:30")
    assert "entry_start_offset_minutes" in derivation


def test_mala_handoff_packet_keeps_runtime_fields_out_of_evidence(tmp_path: Path) -> None:
    run_dir = _write_market_impulse_run(tmp_path)

    packets = build_handoff_packets(runs_root=tmp_path)

    assert len(packets) == 1
    packet = packets[0]
    assert packet.catalog_key == "market-impulse-test__iwm_long"
    assert packet.strategy.strategy_key == "market_impulse"
    assert packet.strategy.signal_window_start_et == "09:35"
    assert packet.strategy.signal_window_end_et == "10:15"
    assert packet.thesis_exit.tested is True
    assert packet.thesis_exit.policy == "fixed_rr_underlying"
    assert "runtime_requirements" not in packet.to_dict()
    assert not any(warning.startswith("legacy_m5_execution_mapping_ignored") for warning in packet.warnings)
    assert str(run_dir) == packet.provenance.run_dir


def test_handoff_outputs_do_not_publish_vehicle_mapping_as_truth(tmp_path: Path) -> None:
    _write_market_impulse_run(tmp_path)
    packets = build_handoff_packets(runs_root=tmp_path)
    out_dir = tmp_path / "out"

    paths = write_handoff_outputs(packets, out_dir)

    csv_text = paths["csv"].read_text(encoding="utf-8")
    jsonl_text = paths["jsonl"].read_text(encoding="utf-8")
    assert "50-90% premium" not in csv_text
    assert "long_call" not in csv_text
    payload = json.loads(jsonl_text.strip())
    assert "runtime_requirements" not in payload
    assert "option_vehicle" not in csv_text
    assert "max_premium_usd" not in csv_text


def test_exit_artifact_match_treats_bracketed_and_comma_lists_as_same(tmp_path: Path) -> None:
    run_dir = _write_market_impulse_run(tmp_path)
    summary_path = run_dir / "m5_exit_optimizations.json"
    items = json.loads(summary_path.read_text(encoding="utf-8"))
    items[0]["candidate_key"]["vwma_periods"] = "[5, 13, 21]"
    summary_path.write_text(json.dumps(items), encoding="utf-8")

    packet = build_handoff_packets(runs_root=tmp_path)[0]

    assert packet.thesis_exit.tested is True
    assert packet.thesis_exit.policy == "fixed_rr_underlying"


def test_review_thesis_exit_metrics_are_ordered_and_rounded() -> None:
    metrics = review_thesis_exit_metrics(
        {
            "win_rate": 0.5737704918032787,
            "expectancy": 0.7771573770491802,
            "profit_factor": 1.8217287528622161,
            "trade_count": 61,
            "avg_winner": 3.0027971428571427,
            "avg_loser": -2.2188961538461545,
            "total_pnl": 47.40659999999999,
        }
    )

    assert list(metrics) == [
        "expectancy",
        "profit_factor",
        "trade_count",
        "win_rate",
        "avg_winner",
        "avg_loser",
        "total_pnl",
    ]
    assert metrics == {
        "expectancy": 0.78,
        "profit_factor": 1.82,
        "trade_count": 61,
        "win_rate": 0.57,
        "avg_winner": 3.0,
        "avg_loser": -2.22,
        "total_pnl": 47.41,
    }


def _write_market_impulse_run(root: Path) -> Path:
    run_dir = root / "market-impulse-test" / "2026-04-15T000000"
    run_dir.mkdir(parents=True)
    _write_csv(
        run_dir / "CATALOG_SELECTED.csv",
        [
            {
                "catalog_key": "market-impulse-test__iwm_long",
                "ticker": "IWM",
                "direction": "long",
                "strategy": "Market Impulse (Cross & Reclaim)",
                "execution_profile": "single_option",
                "recommendation_tier": "shadow",
                "exit_reliability": "thin",
                "exit_trade_count": "24",
                "selected_exit_policy": "fixed_rr_underlying:0.0050x2.00",
                "mc_prob_positive_exp": "0.97825",
                "mc_exp_r_p50": "0.429951",
                "base_exp_r": "0.5731",
                "holdout_trades": "49",
                "holdout_win_rate": "0.551",
                "entry_buffer_minutes": "5",
                "entry_window_minutes": "45",
                "regime_timeframe": "15m",
                "vwma_periods": "5,13,21",
            }
        ],
    )
    _write_csv(
        run_dir / "M5_execution.csv",
        [
            {
                "ticker": "IWM",
                "strategy": "Market Impulse (Cross & Reclaim)",
                "direction": "long",
                "entry_buffer_minutes": "5",
                "entry_window_minutes": "45",
                "regime_timeframe": "15m",
                "vwma_periods": "5,13,21",
                "execution_profile": "single_option",
                "stress_profile": "single_option",
                "holdout_trades": "49",
                "holdout_win_rate": "0.551",
                "base_exp_r": "0.5731",
                "mc_prob_positive_exp": "0.97825",
                "mc_exp_r_p50": "0.429951",
                "structure": "long_call",
                "dte": "7-21",
                "delta_plan": "0.35-0.55",
                "entry_window_et": "09:45-14:30",
                "profit_take": "50-90% premium",
                "risk_rule": "hard stop at -35% premium",
            }
        ],
    )
    (run_dir / "m5_exit_optimizations.json").write_text(
        json.dumps(
            [
                {
                    "candidate_key": {
                        "ticker": "IWM",
                        "strategy": "Market Impulse (Cross & Reclaim)",
                        "direction": "long",
                        "entry_buffer_minutes": "5",
                        "entry_window_minutes": "45",
                        "regime_timeframe": "15m",
                        "vwma_periods": "5,13,21",
                    },
                    "artifact": "m5_exit_optimization_iwm_long_abc.json",
                    "selected_policy_name": "fixed_rr_underlying:0.0050x2.00",
                    "selected_metrics": {"trade_count": 24},
                }
            ]
        ),
        encoding="utf-8",
    )
    (run_dir / "m5_exit_optimization_iwm_long_abc.json").write_text(
        json.dumps(
            {
                "selected_policy_name": "fixed_rr_underlying:0.0050x2.00",
                "thesis_exit_policy": "fixed_rr_underlying",
                "thesis_exit_params": {
                    "stop_loss_underlying_pct": 0.005,
                    "take_profit_underlying_r_multiple": 2.0,
                },
                "selected_metrics": {"trade_count": 24, "expectancy": 0.42},
                "catastrophe_exit_params": {"hard_flat_time_et": "15:55", "stop_loss_pct": 0.35},
            }
        ),
        encoding="utf-8",
    )
    return run_dir


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
