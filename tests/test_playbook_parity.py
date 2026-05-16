from __future__ import annotations

import csv
import json
from pathlib import Path

from src.research.playbook_parity import write_playbook_parity_report
from src.research.shared_kernel import ensure_kernel_on_path

ensure_kernel_on_path()

from mala_bhiksha_kernel import (  # noqa: E402
    FeatureContract,
    FeatureSpec,
    PacketLineage,
    PacketStatus,
    PlaybookPacket,
    SourceArtifact,
    write_packet,
)


def test_playbook_parity_writes_blocked_report_without_runtime_adapter(tmp_path: Path) -> None:
    packet_path = _write_packet(tmp_path)
    run_dir = _write_sample_events(tmp_path)

    report_path = write_playbook_parity_report(
        packet_path=packet_path,
        run_dir=run_dir,
        out_root=tmp_path / "parity",
    )

    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["status"] == "blocked"
    assert report["missing_count"] == 1
    assert report["drift_categories"] == ["runtime_adapter_missing"]
    assert Path(report["artifacts"]["signal_diff"]).exists()


def test_playbook_parity_passes_when_runtime_events_match(tmp_path: Path) -> None:
    packet_path = _write_packet(tmp_path)
    run_dir = _write_sample_events(tmp_path)
    runtime_events = tmp_path / "runtime_events.csv"
    _write_events(runtime_events)

    report_path = write_playbook_parity_report(
        packet_path=packet_path,
        run_dir=run_dir,
        out_root=tmp_path / "parity",
        runtime_events_path=runtime_events,
    )

    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["status"] == "passed"
    assert report["missing_count"] == 0
    assert report["extra_count"] == 0


def test_playbook_parity_accepts_explicit_research_events(tmp_path: Path) -> None:
    packet_path = _write_packet(tmp_path)
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    research_events = tmp_path / "research_events.csv"
    runtime_events = tmp_path / "runtime_events.csv"
    _write_events(research_events)
    _write_events(runtime_events)

    report_path = write_playbook_parity_report(
        packet_path=packet_path,
        run_dir=run_dir,
        out_root=tmp_path / "parity",
        research_events_path=research_events,
        runtime_events_path=runtime_events,
    )

    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["status"] == "passed"


def _write_packet(tmp_path: Path) -> Path:
    packet = PlaybookPacket(
        packet_id="playbook.mean_reversion_at_extremes.iwm_qqq",
        version=1,
        status=PacketStatus.REVIEW,
        title="IWM/QQQ Mean Reversion At Extremes",
        symbol_scope=["IWM", "QQQ"],
        intended_horizon="intraday-short-horizon",
        feature_contract=FeatureContract(
            contract_id="mean_reversion_at_extremes_intraday_v1",
            bar_interval="1m",
            session="rth",
            provider="polygon",
            warmup_bars=60,
            features=[FeatureSpec(name="opening_vwap_rth", provider_sensitive=True)],
        ),
        lineage=PacketLineage(
            source_system="mala_v2",
            source_artifacts=[SourceArtifact(label="test", uri="sample_events.csv")],
        ),
        playbook_id="mean-reversion-at-extremes-intraday",
    )
    return write_packet(tmp_path, packet)


def _write_sample_events(tmp_path: Path) -> Path:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    _write_events(run_dir / "sample_events.csv")
    return run_dir


def _write_events(path: Path) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["symbol", "direction", "event_timestamp", "exit_family"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "symbol": "IWM",
                "direction": "short",
                "event_timestamp": "2026-05-15T15:01:00+00:00",
                "exit_family": "fixed_1_5r",
            }
        )
