from __future__ import annotations

import json
from pathlib import Path

from src.research.shadow_campaign import (
    ShadowActivationConfig,
    build_shadow_activation_packet,
    build_shadow_daily_report,
    classify_shadow_activation,
    merge_active_strategy_rows,
)


def test_shadow_activation_classifies_supported_shadow_row() -> None:
    decision, reasons = classify_shadow_activation(
        {
            "bhiksha_ready": "TRUE",
            "bhiksha_capability_status": "supported",
            "recommendation_tier": "shadow",
            "expectancy": "0.25",
            "execution_robustness": "0.80",
            "signal_count": "25",
        },
        config=ShadowActivationConfig(),
    )

    assert decision == "shadow"
    assert reasons == ["eligible"]


def test_shadow_activation_blocks_unsupported_runtime() -> None:
    decision, reasons = classify_shadow_activation(
        {
            "bhiksha_ready": "FALSE",
            "bhiksha_capability_status": "unsupported",
            "recommendation_tier": "shadow",
            "expectancy": "0.5",
            "execution_robustness": "0.99",
            "signal_count": "100",
        },
        config=ShadowActivationConfig(),
    )

    assert decision == "blocked"
    assert "runtime_unsupported" in reasons


def test_shadow_activation_blocks_non_ready_option_exit() -> None:
    decision, reasons = classify_shadow_activation(
        {
            "bhiksha_ready": "TRUE",
            "bhiksha_capability_status": "supported",
            "recommendation_tier": "shadow",
            "expectancy": "0.5",
            "execution_robustness": "0.99",
            "signal_count": "100",
            "option_trade_ready": "false",
            "option_adjusted_expectancy_pct": "-0.01",
            "recommended_dte_max": "21",
        },
        config=ShadowActivationConfig(),
    )

    assert decision == "blocked"
    assert "option_trade_not_ready" in reasons
    assert "non_positive_option_adjusted_expectancy" in reasons
    assert "option_dte_outside_short_packet" in reasons


def test_shadow_activation_packet_writes_review_artifacts(tmp_path: Path) -> None:
    evidence_rows = [
        {
            "catalog_key": "idea__amd_short",
            "symbol": "AMD",
            "direction": "short",
            "strategy_key": "market_impulse",
            "strategy_name": "Market Impulse (Cross & Reclaim)",
            "recommendation_tier": "shadow",
            "bhiksha_ready": "TRUE",
            "bhiksha_capability_status": "supported",
            "expectancy": "0.5",
            "confidence": "0.55",
            "signal_count": "100",
            "execution_robustness": "0.99",
            "thesis_exit_policy": "time_stop_underlying",
            "exit_reliability": "thin",
            "exit_trade_count": "25",
            "signal_window_et": "09:35-11:00",
            "option_trade_ready": "true",
            "option_adjusted_expectancy_pct": "0.21",
            "option_exit_quality": "fast_intraday",
            "recommended_dte_min": "3",
            "recommended_dte_max": "7",
            "median_minutes_held": "55",
            "pnl_pct_per_minute": "0.004",
        }
    ]

    artifacts = build_shadow_activation_packet(
        evidence_rows=evidence_rows,
        active_strategy_rows=[],
        out_dir=tmp_path,
    )

    assert artifacts.packet_md.exists()
    assert artifacts.packet_csv.exists()
    assert artifacts.active_strategy_rows[0]["strategy_id"] == "idea__amd_short"
    assert artifacts.active_strategy_rows[0]["entry_window_start_et"] == "09:35"
    overrides = json.loads(artifacts.active_strategy_rows[0]["execution_overrides"])
    assert overrides["dte_min"] == 3
    assert overrides["dte_max"] == 7
    assert overrides["target_abs_delta_min"] == 0.15
    assert "Shadow Activation Packet" in artifacts.packet_md.read_text(encoding="utf-8")


def test_merge_active_strategy_rows_preserves_existing_and_updates_matches() -> None:
    merged = merge_active_strategy_rows(
        existing_rows=[
            {
                "enabled": "FALSE",
                "authorization_mode": "shadow",
                "strategy_id": "old",
                "entry_window_start_et": "09:30",
                "max_trade_premium_usd": "1000",
                "execution_overrides": "{}",
                "notes": "old",
            }
        ],
        recommended_rows=[
            {
                "enabled": "TRUE",
                "authorization_mode": "shadow",
                "strategy_id": "old",
                "entry_window_start_et": "09:35",
                "max_trade_premium_usd": "500",
                "execution_overrides": "{}",
                "notes": "new",
            },
            {
                "enabled": "TRUE",
                "authorization_mode": "shadow",
                "strategy_id": "new",
                "entry_window_start_et": "10:00",
                "max_trade_premium_usd": "500",
                "execution_overrides": "{}",
                "notes": "new row",
            },
        ],
    )

    assert [row["strategy_id"] for row in merged] == ["old", "new"]
    assert merged[0]["enabled"] == "TRUE"
    assert merged[0]["entry_window_start_et"] == "09:35"


def test_merge_active_strategy_rows_can_disable_non_recommended() -> None:
    merged = merge_active_strategy_rows(
        existing_rows=[
            {
                "enabled": "TRUE",
                "authorization_mode": "shadow",
                "strategy_id": "old",
                "entry_window_start_et": "09:30",
                "max_trade_premium_usd": "1000",
                "execution_overrides": "{}",
                "notes": "old",
            }
        ],
        recommended_rows=[],
        disable_non_recommended=True,
    )

    assert merged[0]["enabled"] == "FALSE"
    assert "not in current shadow packet" in merged[0]["notes"]


def test_shadow_daily_report_reads_feedback_bundle(tmp_path: Path) -> None:
    feedback = tmp_path / "feedback" / "active_plan_2026-05-04"
    feedback.mkdir(parents=True)
    (feedback / "active_plan.json").write_text(
        json.dumps({"active_plan_id": "active_plan_2026-05-04"}),
        encoding="utf-8",
    )
    (feedback / "session_summary.json").write_text(
        json.dumps({"runtime_issue_counts": {}}),
        encoding="utf-8",
    )
    (feedback / "observation_index.json").write_text(
        json.dumps(
            {
                "reports": [
                    {
                        "deployment_id": "idea__amd_short",
                        "symbol": "AMD",
                        "strategy_key": "market_impulse",
                        "authorization_mode": "shadow",
                        "shadow_only": True,
                        "signal_decisions_total": 2,
                        "signal_true_count": 1,
                        "trade_plan_count": 1,
                        "exit_true_count": 0,
                        "pending_exit_count": 0,
                        "blocked_entry_reasons": {},
                        "runtime_issue_counts": {},
                        "signal_reason_counts": {"cross_reclaim": 1},
                        "exit_reason_counts": {},
                        "latest_lifecycle_state": "open_protected",
                        "replay": {"status": "ok", "trade_count": 1},
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    artifacts = build_shadow_daily_report(
        feedback_root=tmp_path / "feedback",
        evidence_rows=[
            {
                "catalog_key": "idea__amd_short",
                "recommendation_tier": "shadow",
                "expectancy": "0.5",
                "execution_robustness": "0.99",
            }
        ],
        out_dir=tmp_path / "reports",
    )

    assert artifacts.bundle_count == 1
    assert artifacts.observation_count == 1
    report = artifacts.report_md.read_text(encoding="utf-8")
    assert "idea__amd_short" in report
    assert "Signal Expected Value" in report
    assert "## Metric Glossary" in report


def test_shadow_daily_report_prefers_session_counts_when_replay_packet_failed(tmp_path: Path) -> None:
    feedback = tmp_path / "feedback" / "active_plan_2026-05-20"
    feedback.mkdir(parents=True)
    (feedback / "active_plan.json").write_text(
        json.dumps({"active_plan_id": "active_plan_2026-05-20"}),
        encoding="utf-8",
    )
    deployment_id = "strategy_idea_amd_short_row_2"
    (feedback / "session_summary.json").write_text(
        json.dumps(
            {
                "signal_true_counts": {deployment_id: 3},
                "exit_true_counts": {deployment_id: 1},
                "blocked_entry_reasons_by_deployment": {
                    deployment_id: {
                        "approved": 2,
                        "insufficient_budget_for_single_contract": 1,
                        "lifecycle_state:open_protected": 4,
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    (feedback / "observation_index.json").write_text(
        json.dumps(
            {
                "reports": [
                    {
                        "deployment_id": deployment_id,
                        "symbol": "AMD",
                        "strategy_key": "market_impulse",
                        "authorization_mode": "shadow",
                        "shadow_only": True,
                        "signal_decisions_total": 0,
                        "signal_true_count": 0,
                        "trade_plan_count": 0,
                        "exit_true_count": 0,
                        "pending_exit_count": 0,
                        "blocked_entry_reasons": {},
                        "runtime_issue_counts": {},
                        "signal_reason_counts": {},
                        "exit_reason_counts": {},
                        "latest_lifecycle_state": "closed",
                        "replay": {"status": "error"},
                        "startup_deployment": {
                            "source": {
                                "metadata": {
                                    "catalog_key": "idea__amd_short",
                                }
                            }
                        },
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    artifacts = build_shadow_daily_report(
        feedback_root=tmp_path / "feedback",
        evidence_rows=[
            {
                "catalog_key": "idea__amd_short",
                "recommendation_tier": "shadow",
                "expectancy": "0.5",
                "execution_robustness": "0.99",
            }
        ],
        out_dir=tmp_path / "reports",
    )

    report = artifacts.report_md.read_text(encoding="utf-8")
    scorecard = artifacts.scorecard_csv.read_text(encoding="utf-8")
    assert artifacts.issue_count == 1
    assert "signal_true_count: `3`" in report
    assert "trade_plan_count: `3`" in report
    assert "replay_error" in scorecard
    assert "insufficient_budget_for_single_contract" in report
    assert "mala_tier" in scorecard
    assert "shadow" in scorecard
