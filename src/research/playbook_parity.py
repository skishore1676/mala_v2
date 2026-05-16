"""Write signal-level parity artifacts for playbook packets."""

from __future__ import annotations

import argparse
import csv
import json
from datetime import UTC, datetime
from pathlib import Path

from src.research.shared_kernel import ensure_kernel_on_path

ensure_kernel_on_path()

from mala_bhiksha_kernel import (  # noqa: E402
    ParityReport,
    ParityStatus,
    SignalDiff,
    SignalEvent,
    compare_signal_events,
    read_packet_file,
)


def write_playbook_parity_report(
    *,
    packet_path: Path,
    run_dir: Path,
    out_root: Path,
    research_events_path: Path | None = None,
    runtime_events_path: Path | None = None,
) -> Path:
    packet = read_packet_file(packet_path)
    research_events = _load_signal_events(research_events_path or run_dir / "sample_events.csv")
    runtime_events = _load_signal_events(runtime_events_path) if runtime_events_path else []
    generated_at = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    report_dir = out_root / packet.packet_id / generated_at
    report_dir.mkdir(parents=True, exist_ok=True)

    if runtime_events_path is None:
        diff = SignalDiff(
            missing_in_runtime=research_events,
            extra_in_runtime=[],
            matched=[],
            drift_category="runtime_adapter_missing",
            notes="Bhiksha runtime adapter has not produced comparable events yet.",
        )
        report = ParityReport(
            report_id=f"parity.{packet.packet_id}.{generated_at}",
            packet_ref=packet.ref,
            status=ParityStatus.BLOCKED,
            compared_event_count=len(research_events),
            missing_count=len(research_events),
            extra_count=0,
            drift_categories=["runtime_adapter_missing"],
            artifacts={},
            notes=(
                "Blocked, not failed: the playbook packet is structurally valid, "
                "but Bhiksha has no runtime event file for this adapter yet."
            ),
        )
    else:
        diff = compare_signal_events(
            research_events,
            runtime_events,
            drift_category="strategy_semantic_drift",
        )
        report = ParityReport.from_signal_diff(
            report_id=f"parity.{packet.packet_id}.{generated_at}",
            packet_ref=packet.ref,
            diff=diff,
            drift_categories=[] if diff.passed else ["strategy_semantic_drift"],
        )

    diff_path = report_dir / "signal_diff.csv"
    _write_signal_diff(diff_path, diff)
    report_json = report_dir / "PARITY_REPORT.json"
    report_md = report_dir / "PARITY_REPORT.md"
    report = report.model_copy(
        update={
            "artifacts": {
                "signal_diff": str(diff_path),
                "report_json": str(report_json),
                "report_md": str(report_md),
            }
        }
    )
    report_json.write_text(report.model_dump_json(indent=2) + "\n", encoding="utf-8")
    report_md.write_text(_report_markdown(report), encoding="utf-8")
    return report_json


def _load_signal_events(path: Path | None) -> list[SignalEvent]:
    if path is None:
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    events: list[SignalEvent] = []
    for row in rows:
        timestamp = row.get("event_timestamp") or row.get("timestamp") or row.get("ts")
        if not timestamp:
            continue
        events.append(
            SignalEvent(
                ts=datetime.fromisoformat(timestamp).astimezone(UTC),
                symbol=str(row.get("symbol", "")).strip().upper(),
                direction=_direction(str(row.get("direction", ""))),
                event_type=str(row.get("event_type") or "entry"),
                policy_id=row.get("policy_id") or row.get("config_id") or row.get("exit_family") or None,
                metadata={
                    key: value
                    for key, value in row.items()
                    if key
                    in {
                        "config_id",
                        "outcome_label",
                        "pnl_r",
                        "trigger_summary",
                    }
                },
            )
        )
    return events


def _direction(value: str) -> str:
    normalized = value.strip().lower()
    if normalized not in {"long", "short"}:
        raise ValueError(f"Unsupported signal direction: {value!r}")
    return normalized


def _write_signal_diff(path: Path, diff: SignalDiff) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["side", "symbol", "direction", "ts", "event_type", "policy_id"],
        )
        writer.writeheader()
        for side, events in (
            ("matched", diff.matched),
            ("missing_in_runtime", diff.missing_in_runtime),
            ("extra_in_runtime", diff.extra_in_runtime),
        ):
            for event in events:
                writer.writerow(
                    {
                        "side": side,
                        "symbol": event.symbol,
                        "direction": event.direction,
                        "ts": event.ts.isoformat(),
                        "event_type": event.event_type,
                        "policy_id": event.policy_id or "",
                    }
                )


def _report_markdown(report: ParityReport) -> str:
    categories = ", ".join(report.drift_categories) or "none"
    return "\n".join(
        [
            f"# Parity Report: {report.packet_ref.packet_id} v{report.packet_ref.version}",
            "",
            f"- status: `{report.status.value}`",
            f"- compared_event_count: `{report.compared_event_count}`",
            f"- missing_count: `{report.missing_count}`",
            f"- extra_count: `{report.extra_count}`",
            f"- drift_categories: `{categories}`",
            f"- notes: {report.notes or ''}",
            "",
        ]
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--packet", type=Path, required=True)
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--out-root", type=Path, default=Path("artifacts/parity"))
    parser.add_argument("--research-events", type=Path)
    parser.add_argument("--runtime-events", type=Path)
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    report_path = write_playbook_parity_report(
        packet_path=args.packet,
        run_dir=args.run_dir,
        out_root=args.out_root,
        research_events_path=args.research_events,
        runtime_events_path=args.runtime_events,
    )
    print(report_path)


if __name__ == "__main__":
    main()
