from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

from src.research.mala_handoff import (
    MALA_EVIDENCE_ACTIVATION_FIELDNAMES,
    build_handoff_packets,
    derive_signal_window,
    handoff_csv_fieldnames,
    packet_to_csv_row,
    publish_provider_validation_columns,
    publish_review_tabs,
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
    manifest_path = _write_capability_manifest(tmp_path)

    packets = build_handoff_packets(runs_root=tmp_path, bhiksha_capabilities_path=manifest_path)

    assert len(packets) == 1
    packet = packets[0]
    assert packet.catalog_key == "market-impulse-test__iwm_long"
    assert packet.strategy.strategy_key == "market_impulse"
    assert packet.strategy.strategy_variant == "cross_reclaim"
    assert packet.bhiksha_capability.status == "supported"
    assert packet.bhiksha_capability.bhiksha_ready is True
    assert packet.strategy.signal_window_start_et == "09:35"
    assert packet.strategy.signal_window_end_et == "10:15"
    assert packet.thesis_exit.tested is True
    assert packet.thesis_exit.policy == "fixed_rr_underlying"
    assert packet.thesis_exit.metrics["option_trade_ready"] is True
    assert "runtime_requirements" not in packet.to_dict()
    assert not any(warning.startswith("legacy_m5_execution_mapping_ignored") for warning in packet.warnings)
    assert str(run_dir) == packet.provenance.run_dir


def test_mala_handoff_latest_run_without_catalog_suppresses_stale_selected_rows(tmp_path: Path) -> None:
    _write_market_impulse_run(tmp_path)
    latest_run = tmp_path / "market-impulse-test" / "2026-05-17T010000"
    latest_run.mkdir(parents=True)
    (latest_run / "RUN_SUMMARY.md").write_text("decision: retune\n", encoding="utf-8")

    packets = build_handoff_packets(runs_root=tmp_path)

    assert packets == []


def test_mala_handoff_marks_market_impulse_descendant_unsupported(tmp_path: Path) -> None:
    _write_market_impulse_run(
        tmp_path,
        catalog_key="mi-desc-high-close-semiconductors-m1__amd_short",
        ticker="AMD",
        direction="short",
        strategy="MI High Close Reclaim",
        extra_params={
            "entry_mode": "close_location_reclaim",
            "min_close_location": "0.7",
        },
    )
    manifest_path = _write_capability_manifest(tmp_path)

    packet = build_handoff_packets(runs_root=tmp_path, bhiksha_capabilities_path=manifest_path)[0]
    row = packet_to_csv_row(packet)

    assert row["strategy_variant"] == "close_location_reclaim"
    assert row["bhiksha_capability_status"] == "unsupported"
    assert row["bhiksha_capability_reason"] == "runtime_adapter_not_implemented"
    assert row["bhiksha_ready"] == "false"
    assert "bhiksha_unsupported_variant:close_location_reclaim:runtime_adapter_not_implemented" in row["warnings"]


def test_mala_handoff_blocks_market_impulse_volume_filter_runtime_gap(tmp_path: Path) -> None:
    _write_market_impulse_run(
        tmp_path,
        extra_params={
            "use_volume_filter": "true",
            "min_relative_volume": "1.2",
        },
    )
    manifest_path = _write_capability_manifest(tmp_path)

    packet = build_handoff_packets(runs_root=tmp_path, bhiksha_capabilities_path=manifest_path)[0]
    row = packet_to_csv_row(packet)

    assert row["strategy_variant"] == "cross_reclaim"
    assert row["bhiksha_capability_status"] == "unsupported"
    assert row["bhiksha_capability_reason"] == "unsupported_runtime_param:use_volume_filter"
    assert row["bhiksha_ready"] == "false"


def test_mala_handoff_missing_capability_manifest_fails_closed(tmp_path: Path) -> None:
    _write_market_impulse_run(tmp_path)

    packet = build_handoff_packets(
        runs_root=tmp_path,
        bhiksha_capabilities_path=tmp_path / "missing-capabilities.yaml",
    )[0]
    row = packet_to_csv_row(packet)

    assert row["strategy_variant"] == "cross_reclaim"
    assert row["bhiksha_capability_status"] == "unknown_manifest"
    assert row["bhiksha_capability_reason"] == "bhiksha_capability_manifest_missing"
    assert row["bhiksha_ready"] == "false"


def test_mala_handoff_accepts_verified_runtime_capability_manifest(tmp_path: Path) -> None:
    _write_market_impulse_run(tmp_path)
    manifest_path = tmp_path / "bhiksha_runtime_capabilities_v2.json"
    manifest_path.write_text(
        json.dumps(
            {
                "version": 2,
                "generated_at": "2026-05-03T13:25:13Z",
                "supported_thesis_exit_policies": ["fixed_rr_underlying"],
                "strategies": {
                    "market_impulse": {
                        "default_variant": "cross_reclaim",
                        "variants": {
                            "cross_reclaim": {
                                "status": "supported",
                                "reason": "runtime_verified",
                                "verification_status": "verified",
                            }
                        },
                    }
                },
                "verification": {"status": "passed"},
            }
        ),
        encoding="utf-8",
    )

    packet = build_handoff_packets(runs_root=tmp_path, bhiksha_capabilities_path=manifest_path)[0]
    row = packet_to_csv_row(packet)

    assert row["bhiksha_capability_status"] == "supported"
    assert row["bhiksha_capability_reason"] == "runtime_verified"
    assert row["bhiksha_ready"] == "true"


def test_mala_handoff_joins_m6_provider_fields_when_present(tmp_path: Path) -> None:
    run_dir = _write_market_impulse_run(tmp_path)
    _write_csv(
        run_dir / "M6_provider_validation.csv",
        [
            {
                "catalog_key": "market-impulse-test__iwm_long",
                "provider_validation_status": "provider_watch",
                "provider_feature_risk": "yellow",
                "provider_signal_overlap": "0.91",
                "provider_trade_count_ratio": "0.87",
                "provider_expectancy_ratio": "0.8",
                "provider_validation_report": "research/results/hypothesis_runs/market-impulse-test/2026-04-15T000000/M6_PROVIDER_REVIEW.md",
            }
        ],
    )
    (run_dir / "M6_PROVIDER_REVIEW.md").write_text("# M6 Provider Review\n", encoding="utf-8")

    row = packet_to_csv_row(build_handoff_packets(runs_root=tmp_path)[0])

    assert row["provider_validation_status"] == "provider_watch"
    assert row["provider_feature_risk"] == "yellow"
    assert row["provider_signal_overlap"] == "0.91"
    assert row["provider_validation_report"].endswith("M6_PROVIDER_REVIEW.md")
    assert row["m7_status"] == "provider_watch"
    assert row["m7_feature_risk"] == "yellow"
    assert row["m7_signal_overlap"] == "0.91"
    assert row["activation_candidate"] == "true"
    assert row["triage_verdict"] == "CLEAN"
    assert row["triage_blocking_checks"] == "none"
    assert "provider_trade_count_ratio" not in handoff_csv_fieldnames()
    assert "provider_expectancy_ratio" not in handoff_csv_fieldnames()


def test_mala_handoff_missing_m6_file_is_advisory_unknown(tmp_path: Path) -> None:
    _write_market_impulse_run(tmp_path)

    row = packet_to_csv_row(build_handoff_packets(runs_root=tmp_path)[0])

    assert row["provider_validation_status"] == "provider_unknown"
    assert row["provider_feature_risk"] == ""
    assert row["provider_signal_overlap"] == ""
    assert row["provider_validation_report"] == ""
    assert row["m7_status"] == "provider_unknown"
    assert row["activation_candidate"] == "false"
    assert row["triage_verdict"] == "REPAIR"
    assert row["bhiksha_ready"] in {"true", "false"}


def test_mala_handoff_exports_option_exit_fields(tmp_path: Path) -> None:
    _write_market_impulse_run(tmp_path)

    row = packet_to_csv_row(build_handoff_packets(runs_root=tmp_path)[0])

    assert "option_trade_ready" in handoff_csv_fieldnames()
    assert row["option_trade_ready"] == "true"
    assert row["option_adjusted_expectancy_pct"] == "0.36"
    assert row["recommended_dte_min"] == "0"
    assert row["recommended_dte_max"] == "3"
    assert row["pnl_pct_per_minute"] == "0.018"
    assert "bhiksha_runtime_supported" in handoff_csv_fieldnames()
    assert "activation_candidate" in handoff_csv_fieldnames()
    assert row["bhiksha_runtime_supported"] == "true"
    assert row["mala_evidence_ready"] == "true"


def test_mala_handoff_marks_clean_activation_when_all_gates_pass(tmp_path: Path) -> None:
    run_dir = _write_market_impulse_run(tmp_path)
    _write_csv(
        run_dir / "M7_provider_translation.csv",
        [
            {
                "catalog_key": "market-impulse-test__iwm_long",
                "provider_validation_status": "provider_pass",
                "provider_feature_risk": "green",
                "provider_signal_overlap": "0.98",
                "provider_validation_report": "research/results/hypothesis_runs/market-impulse-test/2026-04-15T000000/M7_PROVIDER_TRANSLATION_REVIEW.md",
            }
        ],
    )
    manifest_path = _write_capability_manifest(tmp_path)

    row = packet_to_csv_row(build_handoff_packets(runs_root=tmp_path, bhiksha_capabilities_path=manifest_path)[0])

    assert row["bhiksha_runtime_supported"] == "true"
    assert row["mala_evidence_ready"] == "true"
    assert row["activation_candidate"] == "true"
    assert row["triage_verdict"] == "CLEAN"
    assert row["triage_blocking_checks"] == "none"



def test_publish_review_tabs_requires_canonical_mala_evidence_tab_before_write(tmp_path: Path) -> None:
    packets = build_handoff_packets(runs_root=_write_market_impulse_run(tmp_path).parents[1])
    client = _FakeEvidenceClient([], sheet_exists=False)

    with pytest.raises(RuntimeError, match="required tab"):
        publish_review_tabs(
            packets,
            spreadsheet_id="sheet",
            credentials_path=tmp_path / "creds.json",
            evidence_client=client,
        )

    assert client.overwritten_rows == []


def test_publish_review_tabs_rejects_legacy_strategy_catalog_target(tmp_path: Path) -> None:
    packets = build_handoff_packets(runs_root=_write_market_impulse_run(tmp_path).parents[1])
    client = _FakeEvidenceClient([], sheet_exists=True)

    with pytest.raises(RuntimeError, match="canonical publish surface"):
        publish_review_tabs(
            packets,
            spreadsheet_id="sheet",
            credentials_path=tmp_path / "creds.json",
            evidence_sheet_name="Strategy_Catalog",
            evidence_client=client,
        )

    assert client.overwritten_rows == []


def test_provider_validation_publish_updates_only_m6_columns(tmp_path: Path) -> None:
    run_dir = _write_market_impulse_run(tmp_path)
    _write_csv(
        run_dir / "M6_provider_validation.csv",
        [
            {
                "catalog_key": "market-impulse-test__iwm_long",
                "provider_validation_status": "provider_watch",
                "provider_feature_risk": "yellow",
                "provider_signal_overlap": "0.91",
                "provider_validation_report": "research/results/hypothesis_runs/market-impulse-test/2026-04-15T000000/M6_PROVIDER_REVIEW.md",
            }
        ],
    )
    packets = build_handoff_packets(runs_root=tmp_path)
    client = _FakeEvidenceClient(
        [
            {
                "row_index": 2,
                "catalog_key": "market-impulse-test__iwm_long",
                "bhiksha_ready": "true",
                "thesis_exit_tested": "true",
                "thesis_exit_policy": "fixed_rr_underlying",
                "provider_validation_status": "",
                "provider_feature_risk": "",
                "provider_signal_overlap": "",
                "provider_validation_report": "",
            }
        ]
    )

    result = publish_provider_validation_columns(
        packets,
        spreadsheet_id="sheet",
        credentials_path=tmp_path / "creds.json",
        evidence_client=client,
    )

    assert result == {
        "provider_rows": 1,
        "updated_rows": 1,
        "missing_catalog_keys": [],
        "added_columns": [],
    }
    assert client.updated_columns == [
        "provider_validation_status",
        "provider_feature_risk",
        "provider_signal_overlap",
        "provider_validation_report",
        *MALA_EVIDENCE_ACTIVATION_FIELDNAMES,
    ]
    assert client.updated_rows == [
        {
            "row_index": 2,
            "provider_validation_status": "provider_watch",
            "provider_feature_risk": "yellow",
            "provider_signal_overlap": "0.91",
            "provider_validation_report": "research/results/hypothesis_runs/market-impulse-test/2026-04-15T000000/M6_PROVIDER_REVIEW.md",
            "bhiksha_runtime_supported": "true",
            "bhiksha_runtime_reason": "runtime_verified",
            "mala_evidence_ready": "true",
            "mala_evidence_blocking_checks": "none",
            "activation_candidate": "true",
            "activation_blocking_checks": "none",
            "m7_status": "provider_watch",
            "m7_feature_risk": "yellow",
            "m7_signal_overlap": "0.91",
            "triage_verdict": "CLEAN",
            "triage_verdict_reason": "runtime_supported_mala_ready_option_ready_m7_overlap_clean",
            "triage_blocking_checks": "none",
            "triage_advisory_notes": (
                "holdout_trades=49; promote_requires>=80; exit_trade_count=24; promote_requires>=40; "
                "m7_feature_risk=yellow; provider-sensitive features present, monitor even if overlap is acceptable"
            ),
            "triage_artifact": "research/results/hypothesis_runs/market-impulse-test/2026-04-15T000000/M6_PROVIDER_REVIEW.md",
        }
    ]
    assert "bhiksha_ready" not in client.updated_rows[0]
    assert "thesis_exit_tested" not in client.updated_rows[0]
    assert "thesis_exit_policy" not in client.updated_rows[0]


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


def _write_market_impulse_run(
    root: Path,
    *,
    catalog_key: str = "market-impulse-test__iwm_long",
    ticker: str = "IWM",
    direction: str = "long",
    strategy: str = "Market Impulse (Cross & Reclaim)",
    extra_params: dict[str, str] | None = None,
) -> Path:
    run_dir = root / "market-impulse-test" / "2026-04-15T000000"
    run_dir.mkdir(parents=True)
    params = extra_params or {}
    _write_csv(
        run_dir / "CATALOG_SELECTED.csv",
        [
            {
                "catalog_key": catalog_key,
                "ticker": ticker,
                "direction": direction,
                "strategy": strategy,
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
                **params,
            }
        ],
    )
    _write_csv(
        run_dir / "M5_execution.csv",
        [
            {
                "ticker": ticker,
                "strategy": strategy,
                "direction": direction,
                "entry_buffer_minutes": "5",
                "entry_window_minutes": "45",
                "regime_timeframe": "15m",
                "vwma_periods": "5,13,21",
                **params,
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
                        "ticker": ticker,
                        "strategy": strategy,
                        "direction": direction,
                        "entry_buffer_minutes": "5",
                        "entry_window_minutes": "45",
                        "regime_timeframe": "15m",
                        "vwma_periods": "5,13,21",
                        **params,
                    },
                    "artifact": f"m5_exit_optimization_{ticker.lower()}_{direction}_abc.json",
                    "selected_policy_name": "fixed_rr_underlying:0.0050x2.00",
                    "selected_metrics": {"trade_count": 24},
                }
            ]
        ),
        encoding="utf-8",
    )
    (run_dir / f"m5_exit_optimization_{ticker.lower()}_{direction}_abc.json").write_text(
        json.dumps(
            {
                "selected_policy_name": "fixed_rr_underlying:0.0050x2.00",
                "thesis_exit_policy": "fixed_rr_underlying",
                "thesis_exit_params": {
                    "stop_loss_underlying_pct": 0.005,
                    "take_profit_underlying_r_multiple": 2.0,
                },
                "selected_metrics": {
                    "trade_count": 24,
                    "expectancy": 0.42,
                    "option_trade_ready": True,
                    "option_adjusted_expectancy_pct": 0.36,
                    "option_exit_quality": "fast_scalp",
                    "recommended_dte_min": 0,
                    "recommended_dte_max": 3,
                    "theta_penalty_pct": 0.092308,
                    "expectancy_pct": 0.452308,
                    "avg_win_pct": 0.7,
                    "avg_loss_pct_abs": 0.35,
                    "pnl_pct_per_minute": 0.018,
                    "pnl_pct_per_bar": 0.09,
                    "median_minutes_held": 25.0,
                    "avg_minutes_held": 27.5,
                    "target_hit_rate": 0.58,
                    "stop_loss_rate": 0.42,
                    "target_hit_within_15_minutes": 0.25,
                    "target_hit_within_30_minutes": 0.5,
                    "stop_loss_within_15_minutes": 0.1,
                },
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


def _write_capability_manifest(root: Path) -> Path:
    path = root / "bhiksha_capabilities_v1.yaml"
    path.write_text(
        """
version: 1
supported_thesis_exit_policies:
  - fixed_rr_underlying
strategies:
  market_impulse:
    default_variant: cross_reclaim
    variants:
      cross_reclaim:
        status: supported
      close_location_reclaim:
        status: unsupported
        reason: runtime_adapter_not_implemented
""".strip()
        + "\n",
        encoding="utf-8",
    )
    return path


class _FakeEvidenceClient:
    def __init__(self, rows: list[dict[str, str | int]], sheet_exists: bool = True) -> None:
        self.rows = rows
        self.sheet_exists = sheet_exists
        self.updated_rows: list[dict[str, str | int]] = []
        self.updated_columns: list[str] = []
        self.overwritten_rows: list[dict[str, str | int]] = []

    def ensure_sheet_exists(self) -> None:
        return None

    def require_sheet_exists(self) -> None:
        if not self.sheet_exists:
            raise RuntimeError("required tab 'Mala_Evidence_v1' is missing")

    def ensure_columns(self, columns: list[str]) -> list[str]:
        return []

    def read_rows(self, *, range_suffix: str = "A1:Z1000") -> list[dict[str, str | int]]:
        assert range_suffix == "A1:ZZ5000"
        return self.rows

    def overwrite_table(
        self,
        *,
        headers: list[str],
        rows: list[dict[str, str | int]],
        clear_range_suffix: str = "A1:ZZ5000",
    ) -> dict[str, object]:
        self.overwritten_rows = rows
        return {"updated": len(rows)}

    def batch_update_rows(
        self,
        *,
        rows: list[dict[str, str | int]],
        columns: list[str],
    ) -> dict[str, object]:
        self.updated_rows = rows
        self.updated_columns = columns
        return {"updated": len(rows)}
