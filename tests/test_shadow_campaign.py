from __future__ import annotations

import json
from pathlib import Path

from src.research.shadow_campaign import (
    ShadowActivationConfig,
    build_shadow_activation_packet,
    build_shadow_daily_report,
    classify_shadow_activation,
    merge_active_strategy_rows,
    publish_schwab_adoption_columns,
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
            "provider_validation_status": "provider_pass",
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
            "provider_validation_status": "provider_pass",
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
            "provider_validation_status": "provider_pass",
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
            "provider_validation_status": "provider_pass",
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


def test_shadow_activation_packet_demotes_duplicate_shadow_signature(tmp_path: Path) -> None:
    base = {
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
        "provider_validation_status": "provider_pass",
        "option_trade_ready": "true",
        "option_adjusted_expectancy_pct": "0.21",
        "option_exit_quality": "fast_intraday",
        "recommended_dte_min": "3",
        "recommended_dte_max": "7",
        "median_minutes_held": "55",
        "pnl_pct_per_minute": "0.004",
    }

    artifacts = build_shadow_activation_packet(
        evidence_rows=[
            {"catalog_key": "active__amd_short", **base},
            {"catalog_key": "duplicate__amd_short", **base},
        ],
        active_strategy_rows=[{"strategy_id": "active__amd_short", "enabled": "TRUE"}],
        out_dir=tmp_path,
    )

    assert [row["catalog_key"] for row in artifacts.recommended_rows] == ["active__amd_short"]
    packet = artifacts.packet_md.read_text(encoding="utf-8")
    assert "`observe_only` `duplicate__amd_short`: eligible,duplicate_shadow_signature" in packet


def test_shadow_activation_blocks_provider_unknown_by_default() -> None:
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

    assert decision == "blocked"
    assert "provider_unknown" in reasons


def test_shadow_activation_maps_schwab_adoption_pass_to_provider_pass() -> None:
    decision, reasons = classify_shadow_activation(
        {
            "bhiksha_ready": "TRUE",
            "bhiksha_capability_status": "supported",
            "recommendation_tier": "shadow",
            "expectancy": "0.25",
            "execution_robustness": "0.80",
            "signal_count": "25",
            "schwab_adoption_status": "adoption_pass",
        },
        config=ShadowActivationConfig(),
    )

    assert decision == "shadow"
    assert reasons == ["eligible"]


def test_shadow_activation_keeps_provider_watch_out_of_shadow() -> None:
    decision, reasons = classify_shadow_activation(
        {
            "bhiksha_ready": "TRUE",
            "bhiksha_capability_status": "supported",
            "recommendation_tier": "shadow",
            "expectancy": "0.25",
            "execution_robustness": "0.80",
            "signal_count": "25",
            "provider_validation_status": "provider_watch",
        },
        config=ShadowActivationConfig(),
    )

    assert decision == "observe_only"
    assert "provider_watch" in reasons


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


def test_publish_schwab_adoption_columns_updates_provider_gate() -> None:
    client = _FakeEvidenceClient(
        [
            {
                "row_index": 2,
                "catalog_key": "idea__amd_short",
                "provider_validation_status": "",
            }
        ]
    )

    result = publish_schwab_adoption_columns(
        [
            {
                "catalog_key": "idea__amd_short",
                "adoption_status": "adoption_pass",
                "adoption_reason": "positive_schwab_replay",
                "materialization_status": "materialized",
                "materialization_reason": "temporary_runtime_contract_written",
                "schwab_trade_count": "12",
                "schwab_win_rate": "0.58",
                "schwab_avg_signed_move_pct": "0.07",
                "schwab_median_minutes_held": "21",
                "public_option_smoke_status": "pass",
                "public_option_smoke_reason": "public_chain_and_option_bars_available",
                "public_option_symbol": "AMD260529P00100000",
                "public_option_day_1m_bars": "78",
            }
        ],
        spreadsheet_id="sheet",
        credentials_path="creds.json",
        report_path="data/results/research_ops/schwab_adoption/report.md",
        evidence_client=client,  # type: ignore[arg-type]
    )

    assert result["updated_rows"] == 1
    assert client.ensured_columns[0][0] == "provider_validation_status"
    assert client.updated_rows[0]["provider_validation_status"] == "provider_pass"
    assert client.updated_rows[0]["provider_feature_risk"] == "green"
    assert client.updated_rows[0]["schwab_adoption_status"] == "adoption_pass"
    assert client.updated_columns == [
        "provider_validation_status",
        "provider_feature_risk",
        "provider_signal_overlap",
        "provider_validation_report",
        "schwab_adoption_status",
        "schwab_adoption_reason",
        "schwab_materialization_status",
        "schwab_materialization_reason",
        "schwab_trade_count",
        "schwab_win_rate",
        "schwab_avg_signed_move_pct",
        "schwab_median_minutes_held",
        "public_option_smoke_status",
        "public_option_smoke_reason",
        "public_option_symbol",
        "public_option_day_1m_bars",
        "schwab_adoption_report",
        "schwab_adoption_updated_at",
    ]


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


class _FakeEvidenceClient:
    def __init__(self, rows: list[dict[str, str | int]]) -> None:
        self.rows = rows
        self.ensured_columns: list[list[str]] = []
        self.updated_rows: list[dict[str, str | int]] = []
        self.updated_columns: list[str] = []

    def require_sheet_exists(self) -> None:
        return None

    def ensure_columns(self, columns: list[str]) -> list[str]:
        self.ensured_columns.append(columns)
        return columns

    def read_rows(self, *, range_suffix: str = "A1:ZZ5000") -> list[dict[str, str | int]]:
        return self.rows

    def batch_update_rows(
        self,
        *,
        rows: list[dict[str, str | int]],
        columns: list[str],
    ) -> dict[str, int]:
        self.updated_rows = rows
        self.updated_columns = columns
        return {"updated": len(rows)}
