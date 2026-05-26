from __future__ import annotations

import csv
from pathlib import Path

from src.research.provider_validation_m6 import (
    M6_FEATURE_PARITY_CSV,
    M6_PROVIDER_REVIEW_MD,
    M6_PROVIDER_VALIDATION_CSV,
    M7_PROVIDER_REPLAY_CSV,
    M7_PROVIDER_REVIEW_MD,
    M7_PROVIDER_TRANSLATION_JSON,
    M7_PROVIDER_VALIDATION_CSV,
    M7GatePolicy,
    build_m6_provider_validation,
    build_m7_provider_validation,
    classify_feature_parity,
    classify_provider_validation_status,
    discover_latest_m5_run_dirs,
    max_feature_risk,
)


def test_feature_registry_classifies_green_yellow_and_red() -> None:
    green = classify_feature_parity(
        strategy_key="compression_expansion_breakout",
        strategy_name="Compression Expansion Breakout",
        strategy_params={"use_volume_filter": False},
        symbol="AMD",
    )
    assert max_feature_risk(green) == "green"

    yellow = classify_feature_parity(
        strategy_key="elastic_band_reversion",
        strategy_name="Elastic Band Reversion",
        strategy_params={"use_directional_mass": True},
        symbol="AMD",
    )
    assert max_feature_risk(yellow) == "yellow"
    assert {row["feature"] for row in yellow} >= {"vpoc_4h", "directional_mass_sign"}

    red = classify_feature_parity(
        strategy_key="jerk_pivot_momentum",
        strategy_name="Jerk-Pivot Momentum (tight)",
        strategy_params={"use_volume_filter": True, "volume_multiplier": 1.2},
        symbol="AMD",
    )
    assert max_feature_risk(red) == "red"
    assert any(row["feature"] == "raw_1m_volume_gate" for row in red)


def test_provider_validation_status_assignments() -> None:
    assert classify_provider_validation_status(
        feature_risk="green",
        provider_signal_overlap=0.92,
        has_provider_evidence=True,
    ) == "provider_pass"
    assert classify_provider_validation_status(
        feature_risk="green",
        provider_signal_overlap=0.96,
        has_provider_evidence=True,
    ) == "provider_pass"
    assert classify_provider_validation_status(
        feature_risk="yellow",
        provider_signal_overlap=0.92,
        has_provider_evidence=True,
    ) == "provider_watch"
    assert classify_provider_validation_status(
        feature_risk="green",
        provider_signal_overlap=0.42,
        has_provider_evidence=True,
    ) == "provider_blocked"
    assert classify_provider_validation_status(
        feature_risk="red",
        provider_signal_overlap=None,
        has_provider_evidence=False,
    ) == "provider_blocked"
    assert classify_provider_validation_status(
        feature_risk="green",
        provider_signal_overlap=None,
        has_provider_evidence=False,
    ) == "provider_unknown"


def test_provider_validation_status_accepts_explicit_gate_policy() -> None:
    strict = M7GatePolicy(signal_overlap_block_below=0.80, signal_overlap_activation_min=0.95)

    assert classify_provider_validation_status(
        feature_risk="green",
        provider_signal_overlap=0.92,
        has_provider_evidence=True,
        gate_policy=strict,
    ) == "provider_watch"


def test_build_m6_provider_validation_writes_artifacts(tmp_path: Path) -> None:
    run_dir = tmp_path / "runs" / "jerk-idea" / "2026-05-04T120000"
    run_dir.mkdir(parents=True)
    _write_csv(
        run_dir / "CATALOG_SELECTED.csv",
        [
            {
                "catalog_key": "jerk-idea__amd_long",
                "ticker": "AMD",
                "direction": "long",
                "strategy": "Jerk-Pivot Momentum (tight)",
                "recommendation_tier": "shadow",
                "use_volume_filter": "true",
                "volume_multiplier": "1.2",
                "base_exp_r": "0.4",
                "holdout_trades": "30",
            }
        ],
    )
    _write_csv(
        run_dir / "M5_execution.csv",
        [
            {
                "ticker": "AMD",
                "direction": "long",
                "strategy": "Jerk-Pivot Momentum (tight)",
                "use_volume_filter": "true",
                "volume_multiplier": "1.2",
                "base_exp_r": "0.4",
                "holdout_trades": "30",
            }
        ],
    )
    relative_csv = tmp_path / "provider_relative_volume_parity.csv"
    _write_csv(
        relative_csv,
        [
            {
                "symbol": "AMD",
                "aggregate_minutes": "1",
                "relative_volume_window": "20",
                "comparisons": "100",
                "gate_flip_rate_ge_1_2": "0.04",
            }
        ],
    )
    replay_csv = tmp_path / "volume_mismatch_replay_by_row.csv"
    _write_csv(
        replay_csv,
        [
            {
                "catalog_key": "jerk-idea__amd_long",
                "scenario": "volume_provider_like",
                "entry_overlap_rate_vs_baseline": "0.91",
                "trade_count_ratio_vs_baseline": "0.87",
                "expectancy": "0.32",
                "baseline_expectancy": "0.4",
            }
        ],
    )

    artifacts = build_m6_provider_validation(
        run_dirs=[run_dir],
        provider_relative_volume_csv=relative_csv,
        provider_replay_csv=replay_csv,
    )

    assert artifacts.run_dirs == [run_dir]
    validation_rows = list(csv.DictReader((run_dir / M6_PROVIDER_VALIDATION_CSV).open()))
    assert validation_rows[0]["provider_validation_status"] == "provider_blocked"
    assert validation_rows[0]["provider_feature_risk"] == "red"
    assert validation_rows[0]["provider_signal_overlap"] == "0.91"
    assert validation_rows[0]["provider_trade_count_ratio"] == "0.87"
    assert validation_rows[0]["provider_expectancy_ratio"] == "0.8"
    feature_rows = list(csv.DictReader((run_dir / M6_FEATURE_PARITY_CSV).open()))
    assert any(row["feature"] == "raw_1m_volume_gate" for row in feature_rows)
    assert (run_dir / M6_PROVIDER_REVIEW_MD).read_text(encoding="utf-8").startswith("# M6 Provider Review")


def test_build_m7_provider_validation_writes_gate_artifacts(tmp_path: Path) -> None:
    run_dir = _write_minimal_run(tmp_path, "idea", "2026-05-05T120000", "idea__amd_long")
    replay_csv = tmp_path / "provider_replay.csv"
    _write_csv(
        replay_csv,
        [
            {
                "catalog_key": "idea__amd_long",
                "scenario": "provider_like",
                "entry_overlap_rate_vs_baseline": "0.96",
            }
        ],
    )

    artifacts = build_m7_provider_validation(
        run_dirs=[run_dir],
        provider_replay_csv=replay_csv,
    )

    assert artifacts.provider_validation_csvs == [run_dir / M7_PROVIDER_VALIDATION_CSV]
    rows = list(csv.DictReader((run_dir / M7_PROVIDER_VALIDATION_CSV).open()))
    assert rows[0]["provider_validation_status"] == "provider_pass"
    assert rows[0]["provider_signal_overlap"] == "0.96"
    assert (run_dir / M7_PROVIDER_REVIEW_MD).read_text(encoding="utf-8").startswith(
        "# M7 Provider Translation Review"
    )
    assert (run_dir / M7_PROVIDER_TRANSLATION_JSON).exists()
    assert "provider_pass" in (run_dir / M7_PROVIDER_TRANSLATION_JSON).read_text(encoding="utf-8")


def test_m7_provider_validation_treats_zero_baseline_replay_as_unknown(tmp_path: Path) -> None:
    run_dir = _write_minimal_run(tmp_path, "idea", "2026-05-06T120000", "idea__amd_long")
    replay_csv = tmp_path / "provider_replay.csv"
    _write_csv(
        replay_csv,
        [
            {
                "catalog_key": "idea__amd_long",
                "scenario": "provider_like",
                "baseline_signal_count": "0",
                "candidate_signal_count": "0",
                "signal_evidence_status": "no_baseline_signals",
                "entry_overlap_rate_vs_baseline": "",
            }
        ],
    )

    build_m7_provider_validation(run_dirs=[run_dir], provider_replay_csv=replay_csv)

    rows = list(csv.DictReader((run_dir / M7_PROVIDER_VALIDATION_CSV).open()))
    assert rows[0]["provider_validation_status"] == "provider_unknown"
    assert rows[0]["provider_signal_overlap"] == ""


def test_m7_provider_validation_uses_each_run_local_replay_csv(tmp_path: Path) -> None:
    first = _write_minimal_run(tmp_path, "first", "2026-05-07T120000", "first__amd_long")
    second = _write_minimal_run(tmp_path, "second", "2026-05-07T120000", "second__amd_long")
    _write_csv(
        first / M7_PROVIDER_REPLAY_CSV,
        [
            {
                "catalog_key": "first__amd_long",
                "scenario": "provider_like",
                "baseline_signal_count": "10",
                "entry_overlap_rate_vs_baseline": "0.96",
            }
        ],
    )
    _write_csv(
        second / M7_PROVIDER_REPLAY_CSV,
        [
            {
                "catalog_key": "second__amd_long",
                "scenario": "provider_like",
                "baseline_signal_count": "10",
                "entry_overlap_rate_vs_baseline": "0.72",
            }
        ],
    )

    build_m7_provider_validation(run_dirs=[first, second])

    first_rows = list(csv.DictReader((first / M7_PROVIDER_VALIDATION_CSV).open()))
    second_rows = list(csv.DictReader((second / M7_PROVIDER_VALIDATION_CSV).open()))
    assert first_rows[0]["provider_signal_overlap"] == "0.96"
    assert first_rows[0]["provider_validation_status"] == "provider_pass"
    assert second_rows[0]["provider_signal_overlap"] == "0.72"
    assert second_rows[0]["provider_validation_status"] == "provider_blocked"


def test_discover_latest_m5_run_dirs_uses_latest_catalog_key(tmp_path: Path) -> None:
    old_run = _write_minimal_run(tmp_path, "idea", "2026-05-03T120000", "idea__amd_long")
    new_run = _write_minimal_run(tmp_path, "idea", "2026-05-04T120000", "idea__amd_long")

    assert discover_latest_m5_run_dirs(tmp_path) == [new_run]
    assert old_run != new_run


def _write_minimal_run(root: Path, hypothesis_id: str, run_ts: str, catalog_key: str) -> Path:
    run_dir = root / hypothesis_id / run_ts
    run_dir.mkdir(parents=True)
    row = {
        "catalog_key": catalog_key,
        "ticker": "AMD",
        "direction": "long",
        "strategy": "Compression Expansion Breakout",
    }
    _write_csv(run_dir / "CATALOG_SELECTED.csv", [row])
    _write_csv(run_dir / "M5_execution.csv", [{k: v for k, v in row.items() if k != "catalog_key"}])
    return run_dir


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
