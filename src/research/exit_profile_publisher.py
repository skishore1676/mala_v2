"""Publish Phase-3 exit-profile proposals into the ``Mala_Evidence_v1`` Sheet.

Pure, sheet-client-agnostic core for ``scripts/publish_exit_profiles.py``. It
loads the Phase-3 per-row artifacts, builds a dry-run plan/diff, and writes the
``management_policy_spec`` column for the rows whose proposal chose the operator
profile -- matching rows by ``catalog_key`` exactly as
``mala_handoff.publish_provider_validation_columns`` does, and reusing
``GoogleSheetTableClient.batch_update_rows``.

The cell value is the proposal's kernel ``ManagementPolicySpec`` dump serialized
as compact JSON. It round-trips back through ``ManagementPolicySpec`` so the
bhiksha active_plan compiler reads exactly what Phase 3 proposed.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import json
from pathlib import Path
from typing import Any, Protocol

from src.research.mala_handoff import DEFAULT_EVIDENCE_SHEET_NAME, REPO_ROOT
from src.research.shared_kernel import ensure_kernel_on_path

ensure_kernel_on_path()

from mala_bhiksha_kernel import ManagementPolicySpec  # noqa: E402


# The Sheet column the bhiksha active_plan compiler reads the kernel
# ManagementPolicySpec from. This is THE contract surface.
MANAGEMENT_POLICY_SPEC_COLUMN = "management_policy_spec"

# Phase-3 producer (scripts/classify_explore_propose.py) output root.
DEFAULT_EXIT_PROFILE_RESULTS_REL = Path("research/results/exit_profile_classify_propose")

# Selection values from ``.proposal.selection.chosen``.
_CHOSEN_PROFILE = "profile"
_CHOSEN_LEGACY = "legacy"


class SheetClient(Protocol):
    """The subset of ``GoogleSheetTableClient`` this module uses.

    Declared as a Protocol so tests can inject an in-memory/mock client without
    touching any real Google API.
    """

    def require_sheet_exists(self) -> None: ...

    def ensure_columns(self, columns: list[str]) -> list[str]: ...

    def read_rows(self, *, range_suffix: str = ...) -> list[dict[str, Any]]: ...

    def batch_update_rows(
        self, *, rows: list[dict[str, Any]], columns: list[str]
    ) -> dict[str, Any]: ...


@dataclass(frozen=True)
class ExitProfileProposal:
    """One Phase-3 per-row proposal, normalized to what the publisher needs."""

    catalog_key: str
    symbol: str
    playbook: str  # .classification.profile
    chosen: str  # "profile" | "legacy"
    management_policy_spec: dict[str, Any]
    artifact_path: str

    @property
    def is_profile(self) -> bool:
        return self.chosen == _CHOSEN_PROFILE

    @property
    def spec_cell_value(self) -> str:
        """Compact JSON the publisher writes into the ``management_policy_spec`` cell."""
        return _compact_spec_json(self.management_policy_spec)


@dataclass(frozen=True)
class RowDiff:
    """A single matched profile row: what the Sheet has now vs. what we'd write."""

    catalog_key: str
    symbol: str
    playbook: str
    row_index: int
    current: str
    proposed: str

    @property
    def changed(self) -> bool:
        return self.current.strip() != self.proposed.strip()


@dataclass(frozen=True)
class KeptLegacy:
    catalog_key: str
    symbol: str
    playbook: str


@dataclass
class ExitProfilePlan:
    """The fully-computed publish plan (dry-run and commit share this)."""

    diffs: list[RowDiff] = field(default_factory=list)
    kept_legacy: list[KeptLegacy] = field(default_factory=list)
    missing_catalog_keys: list[str] = field(default_factory=list)

    def update_rows(self) -> list[dict[str, Any]]:
        """Row payloads for ``batch_update_rows`` (only matched profile rows)."""
        return [
            {"row_index": diff.row_index, MANAGEMENT_POLICY_SPEC_COLUMN: diff.proposed}
            for diff in self.diffs
        ]

    def summary(self) -> dict[str, Any]:
        return {
            "matched_profile_rows": len(self.diffs),
            "rows_changed": sum(1 for diff in self.diffs if diff.changed),
            "rows_unchanged": sum(1 for diff in self.diffs if not diff.changed),
            "kept_legacy": [item.catalog_key for item in self.kept_legacy],
            "missing_catalog_keys": list(self.missing_catalog_keys),
            "column": MANAGEMENT_POLICY_SPEC_COLUMN,
        }


def latest_results_dir(
    *, results_root_rel: Path = DEFAULT_EXIT_PROFILE_RESULTS_REL
) -> Path | None:
    """Latest stamped Phase-3 results dir, worktree-aware.

    Mirrors the producer's checkout resolution: when Mala runs from a git
    worktree the artifacts may live beside the *real* checkout. Scan the
    worktree root and every ancestor's ``mala_v2`` checkout, returning the
    lexicographically-latest stamp dir that contains an ``INDEX.json``.
    """
    candidate_roots: list[Path] = [REPO_ROOT / results_root_rel]
    for ancestor in REPO_ROOT.parents:
        candidate = ancestor / results_root_rel
        if candidate not in candidate_roots:
            candidate_roots.append(candidate)

    stamp_dirs: list[Path] = []
    for root in candidate_roots:
        if not root.is_dir():
            continue
        for child in root.iterdir():
            if child.is_dir() and (child / "INDEX.json").is_file():
                stamp_dirs.append(child)
    if not stamp_dirs:
        return None
    # Stamp dir names are UTC timestamps (e.g. 20260614T154615Z); lexical sort
    # on the name is chronological. Resolve so duplicates across roots dedupe.
    return max(stamp_dirs, key=lambda path: (path.name, str(path.resolve())))


def load_exit_profile_proposals(results_dir: str | Path) -> list[ExitProfileProposal]:
    """Load every per-row proposal under ``results_dir``.

    Prefers ``INDEX.json``'s row ordering/artifact pointers; falls back to
    globbing per-row JSON files if INDEX.json is absent.
    """
    root = Path(results_dir)
    index_path = root / "INDEX.json"
    proposals: list[ExitProfileProposal] = []
    seen: set[str] = set()

    artifact_names: list[str] = []
    if index_path.is_file():
        index = json.loads(index_path.read_text(encoding="utf-8"))
        for row in index.get("rows", []):
            name = str(row.get("artifact") or "")
            if name:
                artifact_names.append(name)
    if not artifact_names:
        artifact_names = sorted(
            path.name for path in root.glob("*.json") if path.name != "INDEX.json"
        )

    for name in artifact_names:
        path = root / name
        if not path.is_file():
            continue
        proposal = _proposal_from_artifact(path)
        if proposal is None or proposal.catalog_key in seen:
            continue
        seen.add(proposal.catalog_key)
        proposals.append(proposal)
    return proposals


def _proposal_from_artifact(path: Path) -> ExitProfileProposal | None:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        return None
    catalog_key = str(payload.get("catalog_key") or "").strip()
    proposal = payload.get("proposal")
    if not catalog_key or not isinstance(proposal, dict):
        return None
    selection = proposal.get("selection") or {}
    spec = proposal.get("management_policy_spec")
    classification = payload.get("classification") or {}
    return ExitProfileProposal(
        catalog_key=catalog_key,
        symbol=str(payload.get("symbol") or "").strip(),
        playbook=str(classification.get("profile") or payload.get("classified_profile") or "").strip(),
        chosen=str(selection.get("chosen") or "").strip(),
        management_policy_spec=spec if isinstance(spec, dict) else {},
        artifact_path=str(path),
    )


def build_exit_profile_plan(
    proposals: list[ExitProfileProposal],
    existing_rows: list[dict[str, Any]],
) -> ExitProfilePlan:
    """Compute the diff/plan: matched profile rows, kept-legacy, missing keys."""
    rows_by_key: dict[str, dict[str, Any]] = {}
    for row in existing_rows:
        key = str(row.get("catalog_key") or "").strip()
        if key:
            rows_by_key[key] = row

    plan = ExitProfilePlan()
    for proposal in proposals:
        if not proposal.is_profile:
            plan.kept_legacy.append(
                KeptLegacy(
                    catalog_key=proposal.catalog_key,
                    symbol=proposal.symbol,
                    playbook=proposal.playbook,
                )
            )
            continue
        existing = rows_by_key.get(proposal.catalog_key)
        if existing is None:
            plan.missing_catalog_keys.append(proposal.catalog_key)
            continue
        current = str(existing.get(MANAGEMENT_POLICY_SPEC_COLUMN) or "")
        plan.diffs.append(
            RowDiff(
                catalog_key=proposal.catalog_key,
                symbol=proposal.symbol,
                playbook=proposal.playbook,
                row_index=int(existing["row_index"]),
                current=current,
                proposed=proposal.spec_cell_value,
            )
        )
    return plan


def publish_exit_profile_specs(
    proposals: list[ExitProfileProposal],
    *,
    spreadsheet_id: str = "",
    credentials_path: str | Path = "",
    evidence_sheet_name: str = DEFAULT_EVIDENCE_SHEET_NAME,
    evidence_client: SheetClient | None = None,
) -> dict[str, Any]:
    """Write the ``management_policy_spec`` column for matched profile rows.

    Follows ``mala_handoff.publish_provider_validation_columns`` exactly:
    require the canonical tab, ensure the column exists, read rows, match by
    ``catalog_key``, and batch-update only the matched profile rows. Legacy rows
    are skipped; unmatched catalog_keys are reported, never written.

    ``evidence_client`` is injectable so tests use an in-memory mock and no live
    Google credentials are required to exercise this path.
    """
    _require_canonical_evidence_sheet(evidence_sheet_name)
    if evidence_client is None:
        from src.research.google_sheets import GoogleSheetTableClient

        evidence_client = GoogleSheetTableClient(
            spreadsheet_id=spreadsheet_id,
            sheet_name=evidence_sheet_name,
            credentials_path=Path(credentials_path),
        )
    evidence_client.require_sheet_exists()
    added_columns = evidence_client.ensure_columns([MANAGEMENT_POLICY_SPEC_COLUMN])
    existing_rows = evidence_client.read_rows(range_suffix="A1:ZZ5000")
    if existing_rows and "catalog_key" not in existing_rows[0]:
        raise RuntimeError(
            f"{evidence_sheet_name} must contain a catalog_key column for exit-profile publish"
        )

    plan = build_exit_profile_plan(proposals, existing_rows)
    update_rows = plan.update_rows()
    evidence_client.batch_update_rows(
        rows=update_rows, columns=[MANAGEMENT_POLICY_SPEC_COLUMN]
    )

    written = [
        {
            "catalog_key": diff.catalog_key,
            "symbol": diff.symbol,
            "playbook": diff.playbook,
            "row_index": diff.row_index,
            "management_policy_spec": diff.proposed,
        }
        for diff in plan.diffs
    ]
    summary = plan.summary()
    summary["added_columns"] = added_columns
    summary["written"] = written
    return summary


def render_dry_run_report(plan: ExitProfilePlan) -> str:
    """Human-readable per-row diff for the dry-run, plus the matched/kept/missing lists."""
    lines: list[str] = []
    lines.append("=== EXIT PROFILE PUBLISH (DRY RUN) ===")
    lines.append(
        f"matched profile rows: {len(plan.diffs)} "
        f"| kept legacy: {len(plan.kept_legacy)} "
        f"| missing: {len(plan.missing_catalog_keys)}"
    )
    lines.append("")
    lines.append(
        "catalog_key | symbol | playbook | current management_policy_spec -> proposed"
    )
    lines.append("-" * 100)
    if not plan.diffs:
        lines.append("(no matched profile rows)")
    for diff in plan.diffs:
        current = diff.current.strip() or "<empty>"
        marker = "" if diff.changed else "  [unchanged]"
        lines.append(f"{diff.catalog_key} | {diff.symbol} | {diff.playbook}{marker}")
        lines.append(f"    current : {_preview(current)}")
        lines.append(f"    proposed: {_preview(diff.proposed)}")
    lines.append("")
    lines.append("KEPT LEGACY (left unchanged):")
    if not plan.kept_legacy:
        lines.append("  (none)")
    for item in plan.kept_legacy:
        lines.append(f"  {item.catalog_key} | {item.symbol} | {item.playbook}")
    lines.append("")
    lines.append("MISSING (in proposals, not in Sheet -> not written):")
    if not plan.missing_catalog_keys:
        lines.append("  (none)")
    for key in plan.missing_catalog_keys:
        lines.append(f"  {key}")
    lines.append("")
    lines.append("DRY RUN: nothing written.")
    return "\n".join(lines)


def spec_round_trips(cell_value: str) -> bool:
    """True if ``cell_value`` parses back into a kernel ``ManagementPolicySpec``.

    The contract guarantee: what we write is exactly what bhiksha can rehydrate.
    """
    try:
        parse_spec_cell(cell_value)
    except Exception:
        return False
    return True


def parse_spec_cell(cell_value: str) -> ManagementPolicySpec:
    """Rehydrate a written cell back into the kernel spec (raises on mismatch)."""
    payload = json.loads(cell_value)
    return ManagementPolicySpec.model_validate(payload)


def _compact_spec_json(spec: dict[str, Any]) -> str:
    if not spec:
        return ""
    # Validate-then-dump so the written cell is exactly the kernel's canonical
    # JSON shape (and we fail fast if Phase 3 emitted a spec the kernel rejects).
    model = ManagementPolicySpec.model_validate(spec)
    return json.dumps(model.model_dump(mode="json"), separators=(",", ":"), sort_keys=True)


def _require_canonical_evidence_sheet(evidence_sheet_name: str) -> None:
    if evidence_sheet_name != DEFAULT_EVIDENCE_SHEET_NAME:
        raise RuntimeError(
            f"Refusing exit-profile publish to {evidence_sheet_name!r}; "
            f"canonical publish surface is {DEFAULT_EVIDENCE_SHEET_NAME!r}."
        )


def _preview(value: str, *, limit: int = 240) -> str:
    if len(value) <= limit:
        return value
    return value[:limit] + f"... (+{len(value) - limit} chars)"


__all__ = [
    "MANAGEMENT_POLICY_SPEC_COLUMN",
    "DEFAULT_EXIT_PROFILE_RESULTS_REL",
    "ExitProfileProposal",
    "RowDiff",
    "KeptLegacy",
    "ExitProfilePlan",
    "SheetClient",
    "latest_results_dir",
    "load_exit_profile_proposals",
    "build_exit_profile_plan",
    "publish_exit_profile_specs",
    "render_dry_run_report",
    "spec_round_trips",
    "parse_spec_cell",
]
