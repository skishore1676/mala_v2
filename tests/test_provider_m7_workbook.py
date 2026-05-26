from __future__ import annotations

import csv
import zipfile
from pathlib import Path

from src.research.provider_m7_workbook import build_m7_review_workbook
from src.research.research_ops import main as research_ops_main


def test_build_m7_review_workbook_writes_excel_tabs(tmp_path: Path) -> None:
    root = _write_m7_artifacts(tmp_path)

    artifacts = build_m7_review_workbook(artifact_root=root)

    assert artifacts.workbook_path == root / "M7_PIPELINE_REVIEW.xlsx"
    assert artifacts.workbook_path.stat().st_size > 0
    with zipfile.ZipFile(artifacts.workbook_path) as workbook_zip:
        workbook_xml = workbook_zip.read("xl/workbook.xml").decode("utf-8")
    assert "M7 Verdicts" in workbook_xml
    assert "Replay Summary" in workbook_xml
    assert "Panel Bar Parity" in workbook_xml


def test_provider_review_m7_cli_writes_requested_workbook(tmp_path: Path) -> None:
    root = _write_m7_artifacts(tmp_path)
    output = root / "review.xlsx"

    exit_code = research_ops_main(
        [
            "provider-review-m7",
            "--artifact-root",
            str(root),
            "--output",
            str(output),
        ]
    )

    assert exit_code == 0
    assert output.exists()


def _write_m7_artifacts(root: Path) -> Path:
    panel_dir = root / "provider_panel"
    run_dir = root / "pilot_runs" / "amd" / "2026-05-26T120000"
    _write_csv(
        run_dir / "M7_provider_translation.csv",
        [
            {
                "catalog_key": "amd__long",
                "provider_validation_status": "provider_watch",
                "provider_signal_overlap": "0.91",
                "provider_feature_risk": "yellow",
            }
        ],
    )
    _write_csv(
        run_dir / "M7_provider_replay.csv",
        [
            {
                "catalog_key": "amd__long",
                "gate_provider_pair": "polygon_vs_schwab",
                "gate_pair_selection_reason": "runtime_provider_pair",
                "baseline_signal_count": "10",
                "entry_overlap_rate_vs_baseline": "0.91",
            }
        ],
    )
    _write_csv(
        run_dir / "M7_provider_replay_by_pair.csv",
        [
            {
                "catalog_key": "amd__long",
                "provider_pair": "polygon_vs_public",
                "entry_overlap_rate_vs_baseline": "0.81",
            }
        ],
    )
    _write_csv(
        run_dir / "M7_feature_parity.csv",
        [
            {
                "catalog_key": "amd__long",
                "feature": "vpoc_4h",
                "feature_risk": "yellow",
            }
        ],
    )
    _write_csv(
        panel_dir / "provider_pair_bar_parity.csv",
        [
            {
                "symbol": "AMD",
                "provider_pair": "polygon_vs_schwab",
                "p95_abs_close_pct": "0.00001",
            }
        ],
    )
    _write_csv(
        panel_dir / "provider_feature_parity.csv",
        [
            {
                "symbol": "AMD",
                "feature": "relative_volume",
                "feature_risk": "yellow",
            }
        ],
    )
    _write_csv(
        panel_dir / "provider_relative_volume_parity.csv",
        [
            {
                "symbol": "AMD",
                "provider_pair": "polygon_vs_schwab",
                "gate_flip_rate_ge_1_2": "0.05",
            }
        ],
    )
    return root


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
