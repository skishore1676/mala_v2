from __future__ import annotations

from src.research.evidence_rework import ManifestInputs, build_manifest


def test_build_manifest_prioritizes_active_rows_and_flags_short_option_rework() -> None:
    rows = build_manifest(
        ManifestInputs(
            local_rows=[
                {
                    "catalog_key": "mi__qqq_short",
                    "hypothesis_id": "mi",
                    "symbol": "QQQ",
                    "direction": "short",
                    "strategy_key": "market_impulse",
                    "recommendation_tier": "shadow",
                    "bhiksha_capability_status": "supported",
                    "provider_validation_status": "provider_watch",
                    "provider_feature_risk": "yellow",
                    "thesis_exit_policy": "hold_to_eod_underlying",
                    "thesis_exit_params_json": "{}",
                    "exit_trade_count": "16",
                    "signal_count": "53",
                    "execution_robustness": "0.82",
                    "strategy_params_json": '{"entry_buffer_minutes": 3, "entry_window_minutes": 60}',
                    "run_dir": "research/results/hypothesis_runs/mi/run",
                }
            ],
            live_rows=[{"catalog_key": "mi__qqq_short", "recommendation_tier": "shadow"}],
            active_rows=[{"strategy_id": "mi__qqq_short", "enabled": "TRUE"}],
        )
    )

    assert len(rows) == 1
    row = rows[0]
    assert row["priority"] == "P0_active_rework"
    assert row["active_enabled"] == "TRUE"
    assert "short_option_exit_reopt" in row["exit_rework_flags"]
    assert "hold_to_eod_not_short_option_native" in row["exit_rework_flags"]
    assert "early_entry_buffer_whipsaw_risk" in row["whipsaw_flags"]


def test_build_manifest_includes_stale_active_rows_missing_from_handoff() -> None:
    rows = build_manifest(
        ManifestInputs(
            local_rows=[],
            live_rows=[],
            active_rows=[{"strategy_id": "old_runtime_id", "enabled": "FALSE"}],
        )
    )

    assert rows[0]["catalog_key"] == "old_runtime_id"
    assert rows[0]["priority"] == "P1_contract_rework"
    assert "active_strategy_missing_from_regenerated_handoff" in rows[0]["rework_reasons"]
