"""Excel review workbook for M7 provider translation artifacts."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import xlsxwriter


@dataclass(slots=True, frozen=True)
class M7ReviewWorkbook:
    workbook_path: Path


def build_m7_review_workbook(
    *,
    artifact_root: str | Path,
    output_path: str | Path | None = None,
) -> M7ReviewWorkbook:
    """Write an operator-readable workbook over M7 CSV artifacts."""

    root = Path(artifact_root)
    workbook_path = Path(output_path) if output_path else root / "M7_PIPELINE_REVIEW.xlsx"
    workbook_path.parent.mkdir(parents=True, exist_ok=True)

    verdicts = _read_all(root, "M7_provider_translation.csv")
    feature_rows = _read_all(root, "M7_feature_parity.csv")
    replay_rows = _read_all(root, "M7_provider_replay.csv")
    replay_pair_rows = _read_all(root, "M7_provider_replay_by_pair.csv")
    panel_bar_rows = _read_csv(root / "provider_panel" / "provider_pair_bar_parity.csv")
    panel_feature_rows = _read_csv(root / "provider_panel" / "provider_feature_parity.csv")
    panel_relative_rows = _read_csv(root / "provider_panel" / "provider_relative_volume_parity.csv")

    workbook = xlsxwriter.Workbook(str(workbook_path))
    formats = {
        "header": workbook.add_format({"bold": True, "bg_color": "#D9EAF7", "border": 1}),
        "title": workbook.add_format({"bold": True, "font_size": 14}),
        "bad": workbook.add_format({"bg_color": "#F4CCCC"}),
        "watch": workbook.add_format({"bg_color": "#FFF2CC"}),
        "good": workbook.add_format({"bg_color": "#D9EAD3"}),
        "note": workbook.add_format({"text_wrap": True}),
    }
    _write_readme(workbook, formats, root)
    _write_sheet(workbook, formats, "M7 Verdicts", verdicts)
    _write_sheet(workbook, formats, "Replay Summary", replay_rows)
    _write_sheet(workbook, formats, "Replay By Pair", replay_pair_rows)
    _write_sheet(workbook, formats, "Row Feature Findings", feature_rows)
    _write_sheet(workbook, formats, "Panel Bar Parity", panel_bar_rows)
    _write_sheet(workbook, formats, "Panel Feature Parity", panel_feature_rows)
    _write_sheet(workbook, formats, "Panel Rel Volume", panel_relative_rows)
    workbook.close()
    return M7ReviewWorkbook(workbook_path=workbook_path)


def _write_readme(workbook: xlsxwriter.Workbook, formats: dict[str, Any], root: Path) -> None:
    sheet = workbook.add_worksheet("Read Me")
    sheet.write(0, 0, "M7 Pipeline Review", formats["title"])
    notes = [
        ("Artifact root", str(root)),
        ("Read order", "M7 Verdicts -> Replay Summary -> Replay By Pair -> Row Feature Findings -> Panel Bar Parity -> Panel Feature Parity"),
        ("Gate pair", "Replay Summary gates on research_provider_vs_runtime_provider when runtime provider is configured."),
        ("Diagnostic pair", "Replay By Pair keeps every pair; diagnostic_worst_pair records the worst non-gating pair for investigation."),
        ("Zero-signal rule", "If baseline_signal_count is 0, signal overlap is blank and signal_evidence_status is no_baseline_signals."),
        ("price_time_range_core", "Static row-family finding; pairwise OHLC evidence lives in Panel Bar Parity."),
    ]
    for row_idx, (key, value) in enumerate(notes, start=2):
        sheet.write(row_idx, 0, key, formats["header"])
        sheet.write(row_idx, 1, value, formats["note"])
    sheet.set_column(0, 0, 24)
    sheet.set_column(1, 1, 120)


def _write_sheet(
    workbook: xlsxwriter.Workbook,
    formats: dict[str, Any],
    name: str,
    rows: list[dict[str, str]],
) -> None:
    sheet = workbook.add_worksheet(name[:31])
    if not rows:
        sheet.write(0, 0, "No rows")
        return
    headers = list(rows[0].keys())
    for col_idx, header in enumerate(headers):
        sheet.write(0, col_idx, header, formats["header"])
    for row_idx, row in enumerate(rows, start=1):
        for col_idx, header in enumerate(headers):
            value = row.get(header, "")
            sheet.write(row_idx, col_idx, value, _format_for_value(formats, header, value))
    sheet.freeze_panes(1, 0)
    sheet.autofilter(0, 0, len(rows), len(headers) - 1)
    for col_idx, header in enumerate(headers):
        width = min(
            max([len(header), *[len(str(row.get(header, ""))) for row in rows[:200]]]) + 2,
            52,
        )
        sheet.set_column(col_idx, col_idx, width)


def _format_for_value(formats: dict[str, Any], header: str, value: str) -> Any:
    if header not in {
        "provider_validation_status",
        "feature_risk",
        "provider_feature_risk",
        "signal_evidence_status",
        "gate_pair_selection_reason",
    }:
        return None
    if "blocked" in value or value == "red":
        return formats["bad"]
    if "watch" in value or value == "yellow" or "unknown" in value or "missing" in value:
        return formats["watch"]
    if "pass" in value or value == "green" or value == "present" or value == "runtime_provider_pair":
        return formats["good"]
    return None


def _read_all(root: Path, filename: str) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for path in sorted(root.rglob(filename)):
        for row in _read_csv(path):
            rows.append({"artifact_path": str(path), **row})
    return rows


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


__all__ = [
    "M7ReviewWorkbook",
    "build_m7_review_workbook",
]
