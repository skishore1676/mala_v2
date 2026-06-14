#!/usr/bin/env python3
"""Publish Phase-3 proposed exit profiles into the ``Mala_Evidence_v1`` Sheet.

Phase 3 (``scripts/classify_explore_propose.py``) emits, per promoted strategy
row, a local artifact under
``research/results/exit_profile_classify_propose/<stamp>/`` carrying a kernel
``ManagementPolicySpec`` proposal. The bhiksha active_plan compiler reads that
spec from a Sheet cell named ``management_policy_spec`` (round-tripping it
through the kernel ``ManagementPolicySpec``). This CLI bridges the two: it
writes the proposal's ``management_policy_spec`` (as compact JSON) into the
``management_policy_spec`` column on each ``Mala_Evidence_v1`` row whose
``catalog_key`` matches.

Selection contract (the operator's exit DNA gate already ran in Phase 3):

  * Rows with ``.proposal.selection.chosen == "profile"`` get their spec
    written -- the operator profile beat (or tied) the legacy exit.
  * Rows with ``chosen == "legacy"`` (e.g. META) are LEFT UNCHANGED and
    reported as "kept legacy". The legacy underlying exit is not a kernel
    ManagementPolicySpec and stays Mala/operator-owned.
  * ``catalog_key`` values present in the proposals but absent from the Sheet
    are reported as "missing", never written.

Safety:

  * ``--dry-run`` is the DEFAULT. It reads the current rows, prints a per-row
    diff (current cell -> proposed) plus a JSON summary, and writes NOTHING.
  * ``--commit`` performs the column write via the google_sheets client. It
    requires real credentials + sheet id and errors loudly if they are missing
    -- it never silently no-ops.

This mirrors ``src/research/mala_handoff.py::publish_provider_validation_columns``
(open ``Mala_Evidence_v1``, match rows by ``catalog_key``, update only the one
column, report missing rows) and reuses ``GoogleSheetTableClient``.

    # dry-run (default) against the latest P3 stamp:
    python scripts/publish_exit_profiles.py --sheet-id <id> --google-credentials <path>

    # actually write:
    python scripts/publish_exit_profiles.py --commit --sheet-id <id> --google-credentials <path>

    # pin a specific results dir:
    python scripts/publish_exit_profiles.py --results-dir research/results/exit_profile_classify_propose/<stamp>
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.config import settings
from src.research.exit_profile_publisher import (
    DEFAULT_EXIT_PROFILE_RESULTS_REL,
    MANAGEMENT_POLICY_SPEC_COLUMN,
    ExitProfileProposal,
    build_exit_profile_plan,
    latest_results_dir,
    load_exit_profile_proposals,
    publish_exit_profile_specs,
    render_dry_run_report,
)
from src.research.mala_handoff import DEFAULT_EVIDENCE_SHEET_NAME


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Publish Phase-3 proposed exit profiles into the Mala_Evidence_v1 "
            "Google Sheet (management_policy_spec column). Dry-run is the default."
        )
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--dry-run",
        dest="dry_run",
        action="store_true",
        default=True,
        help="Default. Print the per-row diff + JSON summary, write nothing.",
    )
    mode.add_argument(
        "--commit",
        dest="dry_run",
        action="store_false",
        help="Write the management_policy_spec column for matched profile rows.",
    )
    parser.add_argument(
        "--results-dir",
        default="",
        help=(
            "Phase-3 artifact dir (contains INDEX.json + per-row JSON). "
            f"Default: latest stamp under {DEFAULT_EXIT_PROFILE_RESULTS_REL}."
        ),
    )
    parser.add_argument(
        "--sheet-id",
        default="",
        help=(
            "Google spreadsheet ID or URL for Mala_Evidence_v1. "
            "Defaults to STRATEGY_CATALOG_SHEET_ID (compat env for the Mala evidence workbook)."
        ),
    )
    parser.add_argument(
        "--google-credentials",
        default="",
        help="Google service-account JSON path. Defaults to GOOGLE_API_CREDENTIALS_PATH.",
    )
    parser.add_argument(
        "--evidence-sheet-name",
        default=DEFAULT_EVIDENCE_SHEET_NAME,
        help="Canonical Mala evidence tab name (must be Mala_Evidence_v1).",
    )
    parser.add_argument(
        "--fixture-rows",
        default="",
        help=(
            "Optional JSON file of Sheet rows (list of {catalog_key, "
            "management_policy_spec, row_index, ...}) used for an offline dry-run "
            "when no live credentials are available. Ignored by --commit."
        ),
    )
    args = parser.parse_args(argv)

    results_dir = _resolve_results_dir(args.results_dir)
    proposals = load_exit_profile_proposals(results_dir)
    print(f"RESULTS_DIR={results_dir}")
    print(f"PROPOSALS={len(proposals)}")

    if args.dry_run:
        return _run_dry_run(args, proposals)
    return _run_commit(args, proposals)


def _resolve_results_dir(explicit: str) -> Path:
    if explicit:
        path = Path(explicit).expanduser()
        if not path.is_dir():
            raise SystemExit(f"--results-dir {path} does not exist or is not a directory")
        return path
    found = latest_results_dir()
    if found is None:
        raise SystemExit(
            "No Phase-3 results dir found under "
            f"{DEFAULT_EXIT_PROFILE_RESULTS_REL!r}. Run scripts/classify_explore_propose.py "
            "first, or pass --results-dir."
        )
    return found


def _run_dry_run(args: argparse.Namespace, proposals: list[ExitProfileProposal]) -> int:
    existing_rows = _dry_run_existing_rows(args)
    plan = build_exit_profile_plan(proposals, existing_rows)
    print(render_dry_run_report(plan))
    print(f"EXIT_PROFILE_PUBLISH_SUMMARY={json.dumps(plan.summary(), sort_keys=True)}")
    return 0


def _dry_run_existing_rows(args: argparse.Namespace) -> list[dict[str, Any]]:
    """Current Sheet rows for the dry-run diff.

    Prefer a live read (read-only) when credentials are configured. When they
    are not, fall back to an explicit ``--fixture-rows`` file so dry-run still
    works offline; otherwise treat the Sheet as having no pre-existing rows so
    the diff still renders (every proposed cell shows ``<empty> -> proposed``)
    rather than erroring.
    """
    if args.fixture_rows:
        path = Path(args.fixture_rows).expanduser()
        if not path.is_file():
            raise SystemExit(f"--fixture-rows {path} not found")
        loaded = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(loaded, list):
            raise SystemExit(f"--fixture-rows {path} must be a JSON list of row objects")
        return [dict(row) for row in loaded]

    sheet_id = args.sheet_id or settings.strategy_catalog_sheet_id
    credentials = args.google_credentials or settings.google_api_credentials_path
    if sheet_id and credentials and Path(credentials).expanduser().is_file():
        from src.research.google_sheets import GoogleSheetTableClient

        client = GoogleSheetTableClient(
            spreadsheet_id=sheet_id,
            sheet_name=args.evidence_sheet_name,
            credentials_path=Path(credentials),
        )
        return client.read_rows(range_suffix="A1:ZZ5000")

    print(
        "DRY_RUN_NOTE=no live credentials/fixture; rendering diff against an "
        "empty Sheet (current cells shown as <empty>). Pass --fixture-rows for "
        "an offline diff against real row state.",
    )
    return []


def _run_commit(args: argparse.Namespace, proposals: list[ExitProfileProposal]) -> int:
    sheet_id = args.sheet_id or settings.strategy_catalog_sheet_id
    credentials = args.google_credentials or settings.google_api_credentials_path
    if not sheet_id:
        raise SystemExit(
            "--sheet-id or STRATEGY_CATALOG_SHEET_ID (compat env for the Mala "
            "evidence workbook) is required for --commit"
        )
    if not credentials:
        raise SystemExit(
            "--google-credentials or GOOGLE_API_CREDENTIALS_PATH is required for --commit"
        )
    if not Path(credentials).expanduser().is_file():
        raise SystemExit(
            f"--commit requires a real Google service-account JSON; {credentials!r} "
            "does not exist. Refusing to write."
        )

    result = publish_exit_profile_specs(
        proposals,
        spreadsheet_id=sheet_id,
        credentials_path=credentials,
        evidence_sheet_name=args.evidence_sheet_name,
    )
    print(f"EVIDENCE_SHEET={args.evidence_sheet_name}")
    print(f"WRITTEN_COLUMN={MANAGEMENT_POLICY_SPEC_COLUMN}")
    print(f"EXIT_PROFILE_PUBLISH={json.dumps(result, sort_keys=True)}")
    for row in result.get("written", []):
        print(f"WROTE catalog_key={row['catalog_key']} symbol={row['symbol']} playbook={row['playbook']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
