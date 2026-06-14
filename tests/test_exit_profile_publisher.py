"""Tests for the Phase-3 exit-profile Google-Sheet publisher.

MOCK-ONLY: an in-memory sheet client (``_FakeEvidenceClient``, duck-typing
``GoogleSheetTableClient``) records every read/write. No real Google API is
touched. These prove:

  * dry-run computes the correct diff and writes NOTHING;
  * ``--commit`` (``publish_exit_profile_specs``) issues exactly the right cell
    updates for matched profile rows;
  * legacy rows are skipped (left unchanged), even though their artifact still
    carries a management_policy_spec;
  * unmatched catalog_keys are reported, not written;
  * the written cell value round-trips back through the kernel
    ``ManagementPolicySpec`` (contract intact);
  * the canonical-tab guard rejects a non-Mala_Evidence_v1 target.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from src.research.exit_profile_publisher import (
    MANAGEMENT_POLICY_SPEC_COLUMN,
    build_exit_profile_plan,
    latest_results_dir,
    load_exit_profile_proposals,
    parse_spec_cell,
    publish_exit_profile_specs,
    render_dry_run_report,
    spec_round_trips,
)
from src.research.exit_proposal import propose_management_policy_spec, select_exit
from src.research.shared_kernel import ensure_kernel_on_path

ensure_kernel_on_path()
from mala_bhiksha_kernel import ManagementPolicySpec  # noqa: E402


# ── in-memory mock sheet client (duck-types GoogleSheetTableClient) ───────────


class _FakeEvidenceClient:
    """Records calls; mirrors the subset of GoogleSheetTableClient we use."""

    def __init__(self, rows: list[dict[str, Any]], *, sheet_exists: bool = True) -> None:
        self.rows = rows
        self.sheet_exists = sheet_exists
        self.ensured_columns: list[str] = []
        self.read_count = 0
        self.updated_rows: list[dict[str, Any]] = []
        self.updated_columns: list[str] = []
        self.batch_update_calls = 0

    def require_sheet_exists(self) -> None:
        if not self.sheet_exists:
            raise RuntimeError("required tab 'Mala_Evidence_v1' is missing")

    def ensure_columns(self, columns: list[str]) -> list[str]:
        self.ensured_columns = list(columns)
        return []

    def read_rows(self, *, range_suffix: str = "A1:Z1000") -> list[dict[str, Any]]:
        assert range_suffix == "A1:ZZ5000"
        self.read_count += 1
        return self.rows

    def batch_update_rows(
        self, *, rows: list[dict[str, Any]], columns: list[str]
    ) -> dict[str, Any]:
        self.batch_update_calls += 1
        self.updated_rows = rows
        self.updated_columns = columns
        return {"updated": len(rows)}


# ── artifact fixtures (built from the REAL kernel spec via exit_proposal) ─────


def _proposal_spec(profile_name: str, *, catalog_key: str, symbol: str, chosen: str) -> dict[str, Any]:
    """A real ``.proposal`` block, exactly like classify_explore_propose emits."""
    # Force the chosen branch deterministically: huge legacy edge -> legacy,
    # otherwise no legacy score -> profile.
    if chosen == "legacy":
        selection = select_exit(
            profile_name=profile_name,
            profile_expectancy_pct=-1.0,
            legacy_policy="ma_crossover_underlying:ema_12_exit>ema_50_exit",
            legacy_expectancy_pct=5.0,
        )
    else:
        selection = select_exit(profile_name=profile_name, profile_expectancy_pct=3.0)
    assert selection.chosen == chosen
    spec = propose_management_policy_spec(
        selection,
        parameters={"catalog_key": catalog_key, "symbol": symbol},
    )
    return {"selection": selection.to_dict(), "management_policy_spec": spec}


def _write_artifact(
    results_dir: Path,
    *,
    catalog_key: str,
    symbol: str,
    profile_name: str,
    chosen: str,
) -> str:
    artifact_name = f"{catalog_key}.json"
    payload = {
        "catalog_key": catalog_key,
        "symbol": symbol,
        "classification": {"profile": profile_name, "confidence": "high"},
        "proposal": _proposal_spec(
            profile_name, catalog_key=catalog_key, symbol=symbol, chosen=chosen
        ),
        "status": "proposed",
    }
    (results_dir / artifact_name).write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return artifact_name


@pytest.fixture
def results_dir(tmp_path: Path) -> Path:
    """A Phase-3 stamp dir: 2 profile rows + 1 legacy (META) row + INDEX.json."""
    out = tmp_path / "20260614T154615Z"
    out.mkdir()
    rows = [
        _write_artifact(
            out,
            catalog_key="opening-drive__nvda_short",
            symbol="NVDA",
            profile_name="TREND_CONTINUATION",
            chosen="profile",
        ),
        _write_artifact(
            out,
            catalog_key="elastic-band__meta_short",
            symbol="META",
            profile_name="EXHAUSTION_REVERSAL",
            chosen="legacy",
        ),
        _write_artifact(
            out,
            catalog_key="compression-breakout__amd_short",
            symbol="AMD",
            profile_name="RANGE_EXPANSION",
            chosen="profile",
        ),
    ]
    index = {
        "generated_at": "2026-06-14T15:46:30Z",
        "rows": [{"artifact": name} for name in rows],
    }
    (out / "INDEX.json").write_text(json.dumps(index, indent=2), encoding="utf-8")
    return out


def _sheet_rows() -> list[dict[str, Any]]:
    """Existing Sheet rows: NVDA + META present (empty spec cells); AMD absent."""
    return [
        {
            "row_index": 2,
            "catalog_key": "opening-drive__nvda_short",
            "symbol": "NVDA",
            "management_policy_spec": "",
        },
        {
            "row_index": 3,
            "catalog_key": "elastic-band__meta_short",
            "symbol": "META",
            "management_policy_spec": "",
        },
        # AMD intentionally missing -> reported as missing, never written.
    ]


# ── artifact loading ──────────────────────────────────────────────────────────


def test_load_proposals_reads_index_and_classifies_chosen(results_dir: Path) -> None:
    proposals = load_exit_profile_proposals(results_dir)
    by_key = {p.catalog_key: p for p in proposals}
    assert set(by_key) == {
        "opening-drive__nvda_short",
        "elastic-band__meta_short",
        "compression-breakout__amd_short",
    }
    assert by_key["opening-drive__nvda_short"].is_profile is True
    assert by_key["opening-drive__nvda_short"].playbook == "TREND_CONTINUATION"
    assert by_key["elastic-band__meta_short"].is_profile is False  # META = legacy
    assert by_key["elastic-band__meta_short"].chosen == "legacy"


def test_load_proposals_falls_back_to_glob_without_index(results_dir: Path) -> None:
    (results_dir / "INDEX.json").unlink()
    proposals = load_exit_profile_proposals(results_dir)
    assert len(proposals) == 3


def test_latest_results_dir_picks_newest_stamp(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import src.research.exit_profile_publisher as mod

    root = tmp_path / "research" / "results" / "exit_profile_classify_propose"
    older = root / "20260101T000000Z"
    newer = root / "20260601T000000Z"
    for stamp in (older, newer):
        stamp.mkdir(parents=True)
        (stamp / "INDEX.json").write_text("{}", encoding="utf-8")
    # An empty dir without INDEX.json must be ignored.
    (root / "20260701T000000Z").mkdir()

    monkeypatch.setattr(mod, "REPO_ROOT", tmp_path)
    found = latest_results_dir()
    assert found == newer


# ── dry-run: correct diff, writes NOTHING ─────────────────────────────────────


def test_dry_run_plan_computes_diff_and_writes_nothing(results_dir: Path) -> None:
    proposals = load_exit_profile_proposals(results_dir)
    existing = _sheet_rows()
    plan = build_exit_profile_plan(proposals, existing)

    # NVDA matched (present, profile) -> one diff, empty current -> changed.
    assert [d.catalog_key for d in plan.diffs] == ["opening-drive__nvda_short"]
    diff = plan.diffs[0]
    assert diff.row_index == 2
    assert diff.current == ""
    assert diff.changed is True
    assert diff.proposed  # non-empty compact JSON

    # META profile-lost -> kept legacy (NOT a diff, NOT written).
    assert [k.catalog_key for k in plan.kept_legacy] == ["elastic-band__meta_short"]

    # AMD is a profile row but absent from the Sheet -> missing.
    assert plan.missing_catalog_keys == ["compression-breakout__amd_short"]

    summary = plan.summary()
    assert summary["matched_profile_rows"] == 1
    assert summary["rows_changed"] == 1
    assert summary["kept_legacy"] == ["elastic-band__meta_short"]
    assert summary["missing_catalog_keys"] == ["compression-breakout__amd_short"]
    assert summary["column"] == MANAGEMENT_POLICY_SPEC_COLUMN

    # The dry-run report mentions all three buckets.
    report = render_dry_run_report(plan)
    assert "DRY RUN: nothing written." in report
    assert "opening-drive__nvda_short" in report
    assert "elastic-band__meta_short" in report  # kept-legacy section
    assert "compression-breakout__amd_short" in report  # missing section


def test_dry_run_marks_unchanged_when_cell_already_matches(results_dir: Path) -> None:
    proposals = load_exit_profile_proposals(results_dir)
    nvda = next(p for p in proposals if p.catalog_key == "opening-drive__nvda_short")
    existing = [
        {
            "row_index": 2,
            "catalog_key": "opening-drive__nvda_short",
            "symbol": "NVDA",
            "management_policy_spec": nvda.spec_cell_value,
        }
    ]
    plan = build_exit_profile_plan(proposals, existing)
    assert plan.diffs[0].changed is False
    assert plan.summary()["rows_changed"] == 0
    assert plan.summary()["rows_unchanged"] == 1


# ── commit: exactly the right cell updates ────────────────────────────────────


def test_commit_writes_only_matched_profile_rows(results_dir: Path) -> None:
    proposals = load_exit_profile_proposals(results_dir)
    client = _FakeEvidenceClient(_sheet_rows())

    result = publish_exit_profile_specs(proposals, evidence_client=client)

    # Exactly one batch_update_rows call, exactly one row (NVDA), one column.
    assert client.batch_update_calls == 1
    assert client.updated_columns == [MANAGEMENT_POLICY_SPEC_COLUMN]
    assert len(client.updated_rows) == 1
    written_row = client.updated_rows[0]
    assert written_row["row_index"] == 2
    assert written_row[MANAGEMENT_POLICY_SPEC_COLUMN]  # the compact spec JSON
    assert client.ensured_columns == [MANAGEMENT_POLICY_SPEC_COLUMN]

    # Reported result mirrors the plan.
    assert result["matched_profile_rows"] == 1
    assert result["kept_legacy"] == ["elastic-band__meta_short"]
    assert result["missing_catalog_keys"] == ["compression-breakout__amd_short"]
    assert [w["catalog_key"] for w in result["written"]] == ["opening-drive__nvda_short"]
    assert result["written"][0]["symbol"] == "NVDA"
    assert result["written"][0]["playbook"] == "TREND_CONTINUATION"


def test_commit_skips_legacy_rows_even_when_present_in_sheet(results_dir: Path) -> None:
    proposals = load_exit_profile_proposals(results_dir)
    # META present in the sheet AND has a (lost) spec in its artifact, but it is
    # chosen=legacy -> must be left unchanged.
    client = _FakeEvidenceClient(_sheet_rows())
    publish_exit_profile_specs(proposals, evidence_client=client)
    written_keys = {row["row_index"] for row in client.updated_rows}
    assert 3 not in written_keys  # META row_index=3 never written


def test_commit_reports_missing_catalog_keys_without_writing_them(results_dir: Path) -> None:
    proposals = load_exit_profile_proposals(results_dir)
    # Only META in the sheet -> both profile rows (NVDA, AMD) are missing.
    client = _FakeEvidenceClient(
        [
            {
                "row_index": 5,
                "catalog_key": "elastic-band__meta_short",
                "symbol": "META",
                "management_policy_spec": "",
            }
        ]
    )
    result = publish_exit_profile_specs(proposals, evidence_client=client)
    assert client.updated_rows == []  # nothing matched
    assert sorted(result["missing_catalog_keys"]) == [
        "compression-breakout__amd_short",
        "opening-drive__nvda_short",
    ]


# ── contract: written cell round-trips through the kernel ─────────────────────


def test_written_cell_round_trips_through_kernel_spec(results_dir: Path) -> None:
    proposals = load_exit_profile_proposals(results_dir)
    client = _FakeEvidenceClient(_sheet_rows())
    publish_exit_profile_specs(proposals, evidence_client=client)

    cell_value = client.updated_rows[0][MANAGEMENT_POLICY_SPEC_COLUMN]
    assert spec_round_trips(cell_value) is True

    rehydrated = parse_spec_cell(cell_value)
    assert isinstance(rehydrated, ManagementPolicySpec)
    assert rehydrated.policy_id == "profile__trend_continuation"
    assert rehydrated.exit_family == "profile_staged_r"
    # And the parameters carry the selection record Phase 3 stamped.
    assert rehydrated.parameters["selected_exit"]["chosen"] == "profile"
    assert rehydrated.parameters["catalog_key"] == "opening-drive__nvda_short"


def test_spec_round_trips_rejects_garbage() -> None:
    assert spec_round_trips("not json") is False
    assert spec_round_trips(json.dumps({"missing": "required fields"})) is False


# ── safety guards ─────────────────────────────────────────────────────────────


def test_publish_rejects_non_canonical_tab(results_dir: Path) -> None:
    proposals = load_exit_profile_proposals(results_dir)
    client = _FakeEvidenceClient(_sheet_rows())
    with pytest.raises(RuntimeError, match="canonical publish surface"):
        publish_exit_profile_specs(
            proposals, evidence_client=client, evidence_sheet_name="Strategy_Catalog"
        )
    assert client.batch_update_calls == 0


def test_publish_requires_existing_tab(results_dir: Path) -> None:
    proposals = load_exit_profile_proposals(results_dir)
    client = _FakeEvidenceClient(_sheet_rows(), sheet_exists=False)
    with pytest.raises(RuntimeError, match="required tab"):
        publish_exit_profile_specs(proposals, evidence_client=client)
    assert client.batch_update_calls == 0


def test_publish_requires_catalog_key_column(results_dir: Path) -> None:
    proposals = load_exit_profile_proposals(results_dir)
    client = _FakeEvidenceClient([{"row_index": 2, "some_other_col": "x"}])
    with pytest.raises(RuntimeError, match="catalog_key column"):
        publish_exit_profile_specs(proposals, evidence_client=client)
    assert client.batch_update_calls == 0
