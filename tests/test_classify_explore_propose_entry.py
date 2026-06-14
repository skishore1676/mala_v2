"""Orchestration test for the classify->explore->propose entry point.

Locks in the hard boundary: a low-confidence / unclassified row is flagged for
operator review and gets NO exit proposal (no management_policy_spec) -- it is
never force-assigned a profile. Uses a synthetic run dir so it needs no bar
cache and stays fully local / read-only.
"""

import importlib.util
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
_SPEC = importlib.util.spec_from_file_location(
    "classify_explore_propose", REPO_ROOT / "scripts" / "classify_explore_propose.py"
)
cep = importlib.util.module_from_spec(_SPEC)
sys.modules.setdefault("classify_explore_propose", cep)
_SPEC.loader.exec_module(cep)  # type: ignore[union-attr]


def _write_catalog(tmp_path: Path, *, strategy: str, ticker: str, direction: str) -> Path:
    run_dir = tmp_path / "pilot" / "ts"
    run_dir.mkdir(parents=True)
    header = "catalog_key,ticker,direction,strategy\n"
    body = f"synthetic__{ticker.lower()}_{direction},{ticker},{direction},{strategy}\n"
    (run_dir / "CATALOG_SELECTED.csv").write_text(header + body, encoding="utf-8")
    return run_dir


def test_unclassified_row_is_flagged_with_no_proposal(tmp_path):
    run_dir = _write_catalog(
        tmp_path, strategy="Totally Novel Alpha XYZ", ticker="ZZZ", direction="long"
    )
    # storage is never hit for a flagged row (no explore), so a bare LocalStorage
    # pointed at an empty dir is fine.
    from src.chronos.storage import LocalStorage

    rec = cep.process_row(run_dir, storage=LocalStorage(base_dir=tmp_path / "empty_data"))

    assert rec["status"] == "flagged_for_operator_review"
    assert rec["classification"]["profile"] is None
    assert rec["classification"]["needs_operator_review"] is True
    # The hard boundary: no exit proposed, no contract emitted.
    assert "proposal" not in rec
    assert "explore" not in rec


def test_missing_catalog_returns_error(tmp_path):
    from src.chronos.storage import LocalStorage

    rec = cep.process_row(tmp_path / "nope", storage=LocalStorage(base_dir=tmp_path / "d"))
    assert rec["error"] == "no CATALOG_SELECTED.csv"


def test_main_checkout_root_finds_populated_data_dir():
    # The resolver must point at a checkout whose data/ dir is populated (the
    # real cache lives beside the real checkout, not inside a worktree).
    root = cep._main_checkout_root()
    assert (root / "data").is_dir()
