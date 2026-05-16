"""Write shared-kernel packets for Mala playbook surfaces."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any

from src.research.shared_kernel import ensure_kernel_on_path
from src.strategy.intraday_mean_reversion import PLAYBOOK_ID, STRATEGY_NAME

ensure_kernel_on_path()

from mala_bhiksha_kernel import (  # noqa: E402
    FeatureContract,
    FeatureSpec,
    ManagementPolicy,
    PacketLineage,
    PacketStatus,
    PlaybookPacket,
    SourceArtifact,
    write_packet,
    write_registry_index,
)


DEFAULT_PACKET_ID = "playbook.mean_reversion_at_extremes.iwm_qqq"
DEFAULT_PACKET_VERSION = 1


def write_mean_reversion_playbook_packet(
    run_dir: Path,
    *,
    packet_root: Path,
    packet_id: str = DEFAULT_PACKET_ID,
    version: int = DEFAULT_PACKET_VERSION,
    status: PacketStatus = PacketStatus.REVIEW,
) -> Path:
    config_path = run_dir / "config.json"
    surface_path = run_dir / "conditional_surface_by_symbol.csv"
    if not config_path.exists():
        raise FileNotFoundError(f"config.json not found under {run_dir}")
    if not surface_path.exists():
        raise FileNotFoundError(f"conditional_surface_by_symbol.csv not found under {run_dir}")

    run_config = json.loads(config_path.read_text(encoding="utf-8"))
    playbook_id = str(run_config.get("playbook_id", ""))
    if playbook_id != PLAYBOOK_ID:
        raise ValueError(f"unsupported playbook {playbook_id!r}; expected {PLAYBOOK_ID!r}")

    symbols = [str(symbol).upper() for symbol in run_config.get("symbols", [])]
    packet = PlaybookPacket(
        packet_id=packet_id,
        version=version,
        status=status,
        title="IWM/QQQ Mean Reversion At Extremes",
        symbol_scope=symbols,
        intended_horizon="intraday-short-horizon",
        feature_contract=_feature_contract(run_config),
        lineage=_lineage(run_dir),
        playbook_id=PLAYBOOK_ID,
        entry_model={
            "strategy": STRATEGY_NAME,
            "config_generation": run_config.get("config_generation"),
            "calibration_holdout_split": run_config.get("calibration_holdout_split"),
            "match_grade_thresholds": run_config.get("match_grade_thresholds", {}),
        },
        management_policies=_management_policies(surface_path),
        consultation_state="ready_for_parity",
        metadata={
            "source_run_dir": str(run_dir),
            "surface_rows": _row_count(surface_path),
            "config_count": run_config.get("config_count"),
            "feature_families_tested": run_config.get("feature_families_tested", {}),
            "date_range": {
                "start": run_config.get("start"),
                "end": run_config.get("end"),
            },
            "generated_from": "src.research.playbook_packet_registry",
        },
    )
    packet_path = write_packet(packet_root, packet)
    write_registry_index(packet_root)
    return packet_path


def _feature_contract(run_config: dict[str, Any]) -> FeatureContract:
    features = [
        FeatureSpec(
            name=name,
            provider_sensitive=True,
        )
        for name in [
            "opening_vwap_rth",
            "prior_rth_close_atr",
            "vpoc_4h",
            "market_pulse_stage",
            "gap_state_rth_open",
            "velocity",
            "jerk",
            "relative_volume_rth",
        ]
    ]
    return FeatureContract(
        contract_id="mean_reversion_at_extremes_intraday_v1",
        bar_interval="1m",
        session="rth",
        provider="polygon",
        warmup_bars=60,
        features=features,
    )


def _lineage(run_dir: Path) -> PacketLineage:
    artifacts = [
        SourceArtifact(label="run_config", uri=str(run_dir / "config.json")),
        SourceArtifact(
            label="conditional_surface",
            uri=str(run_dir / "conditional_surface_by_symbol.csv"),
        ),
        SourceArtifact(label="sample_events", uri=str(run_dir / "sample_events.csv")),
        SourceArtifact(label="receipt", uri=str(run_dir / "RECEIPT.md")),
    ]
    review = run_dir / "surface_review" / "SURFACE_REVIEW.md"
    if review.exists():
        artifacts.append(SourceArtifact(label="surface_review", uri=str(review)))
    return PacketLineage(source_system="mala_v2", source_artifacts=artifacts)


def _management_policies(surface_path: Path) -> list[ManagementPolicy]:
    rows = _surface_rows(surface_path)
    ranked = sorted(
        rows,
        key=lambda row: (
            _grade_rank(str(row.get("match_grade", ""))),
            _rank_float(row.get("holdout_expectancy_r")),
            _rank_float(row.get("calibration_expectancy_r")),
            _rank_float(row.get("holdout_win_rate")),
        ),
        reverse=True,
    )
    policies: list[ManagementPolicy] = []
    seen: set[tuple[str, str]] = set()
    for row in ranked:
        key = (str(row.get("stop_family", "")), str(row.get("exit_family", "")))
        if key in seen:
            continue
        seen.add(key)
        policies.append(
            ManagementPolicy(
                policy_id=f"{key[0]}__{key[1]}",
                name=f"{key[0]} / {key[1]}",
                rank=len(policies) + 1,
                parameters={
                    "source_config_id": row.get("config_id"),
                    "match_grade": row.get("match_grade"),
                    "sample_count": _safe_int(row.get("sample_count")),
                    "holdout_expectancy_r": _optional_float(row.get("holdout_expectancy_r")),
                    "holdout_win_rate": _optional_float(row.get("holdout_win_rate")),
                },
            )
        )
        if len(policies) == 2:
            break
    return policies


def _surface_rows(surface_path: Path) -> list[dict[str, str]]:
    with surface_path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _row_count(surface_path: Path) -> int:
    return len(_surface_rows(surface_path))


def _rank_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float("-inf")


def _optional_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any) -> int | None:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _grade_rank(grade: str) -> int:
    return {
        "favorable": 3,
        "near_favorable": 2,
        "partial": 1,
    }.get(grade, 0)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--packet-root", type=Path, default=Path.cwd())
    parser.add_argument("--packet-id", default=DEFAULT_PACKET_ID)
    parser.add_argument("--version", type=int, default=DEFAULT_PACKET_VERSION)
    parser.add_argument(
        "--status",
        choices=[status.value for status in PacketStatus],
        default=PacketStatus.REVIEW.value,
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    packet_path = write_mean_reversion_playbook_packet(
        args.run_dir,
        packet_root=args.packet_root,
        packet_id=args.packet_id,
        version=args.version,
        status=PacketStatus(args.status),
    )
    print(packet_path)


if __name__ == "__main__":
    main()
