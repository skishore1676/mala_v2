from __future__ import annotations

import hashlib
import json
from pathlib import Path

import polars as pl

from src.research.classical_patterns.public_validation_analysis import (
    analyze_public_validation,
)


def _hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_public_validation_analysis_rejects_nonreplicating_directional_effect(
    tmp_path: Path,
) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    rows = []
    for split, direction, values in (
        ("validation", "long", [-1.0, 0.5, 0.5]),
        ("holdout", "long", [1.0, 0.5, 0.5]),
        ("validation", "short", [1.0, 0.5, 0.5]),
        ("holdout", "short", [-1.0, -0.5, -0.5]),
    ):
        for index, value in enumerate(values):
            rows.append(
                {
                    "split": split,
                    "direction": direction,
                    "variant_id": "lfd_buffer_0p00atr",
                    "symbol": f"S{index}",
                    "status": "closed",
                    "net_r": value,
                }
            )
    trades_path = run_dir / "trades.csv"
    scorecard_path = run_dir / "economic_scorecard.csv"
    pl.DataFrame(rows).write_csv(trades_path)
    pl.DataFrame({"placeholder": [1]}).write_csv(scorecard_path)
    receipt = {
        "run_id": "test-public-run",
        "mode": "public_daily_research",
        "status": "complete",
        "executable": False,
        "git": {"commit": "run-commit"},
        "config": {"hash": "config-hash"},
        "data": {
            "dataset_manifest_hash": "dataset-hash",
            "semantic_freeze_hash": "freeze-hash",
        },
        "population": {"representative_signals": 12},
        "variants": {"count": 1},
        "artifacts": {
            "trades.csv": {"path": "trades.csv", "content_hash": _hash(trades_path)},
            "economic_scorecard.csv": {
                "path": "economic_scorecard.csv",
                "content_hash": _hash(scorecard_path),
            },
        },
    }
    (run_dir / "receipt.json").write_text(json.dumps(receipt), encoding="utf-8")

    analysis = analyze_public_validation(run_dir=run_dir)

    assert analysis["verdict"] == "no_replicated_alpha"
    assert analysis["counts"]["replicated_positive_cells"] == 0
    replication = pl.read_csv(run_dir / "replication_scorecard.csv")
    assert set(replication.get_column("replication_status")) == {"not_replicated"}
    assert "Do not retune from the holdout" in (run_dir / "OBSIDIAN_REVIEW.md").read_text()


def test_public_validation_analysis_verifies_source_artifact_hashes(
    tmp_path: Path,
) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "trades.csv").write_text("tampered\n", encoding="utf-8")
    (run_dir / "economic_scorecard.csv").write_text("placeholder\n", encoding="utf-8")
    receipt = {
        "run_id": "test-public-run",
        "mode": "public_daily_research",
        "status": "complete",
        "executable": False,
        "artifacts": {
            "trades.csv": {"path": "trades.csv", "content_hash": "wrong"},
            "economic_scorecard.csv": {
                "path": "economic_scorecard.csv",
                "content_hash": _hash(run_dir / "economic_scorecard.csv"),
            },
        },
    }
    (run_dir / "receipt.json").write_text(json.dumps(receipt), encoding="utf-8")

    try:
        analyze_public_validation(run_dir=run_dir)
    except ValueError as exc:
        assert str(exc) == "Public run artifact hash mismatch: trades.csv"
    else:
        raise AssertionError("tampered trades must be rejected")
