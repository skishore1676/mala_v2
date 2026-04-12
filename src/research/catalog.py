"""
Strategy_Catalog writer.

Upserts a single row into the Strategy_Catalog Google Sheet tab.
Only called when a hypothesis reaches `promote` (M5 pass).
The row lands with lifecycle_status=candidate; a human flips it to approved.

Column schema matches the existing sheet (mala_v1 PlaybookRecord layout):
    catalog_key, playbook_id, symbol, bias_template, strategy_key,
    strategy_family, direction, lifecycle_status, operator_status_override,
    operator_notes, bionic_ready, first_validated_date, last_validated_date,
    validation_count, expectancy, confidence, signal_count,
    execution_robustness, thesis_exit_policy, playbook_summary_json
"""

from __future__ import annotations

import re
from datetime import date
from pathlib import Path
from typing import Any

from src.research.google_sheets import GoogleSheetTableClient

# Must match the column order in the live Google Sheet exactly.
STRATEGY_CATALOG_HEADERS = [
    "catalog_key",
    "playbook_id",
    "symbol",
    "bias_template",
    "strategy_key",
    "strategy_family",
    "direction",
    "lifecycle_status",
    "operator_status_override",
    "operator_notes",
    "bionic_ready",
    "first_validated_date",
    "last_validated_date",
    "validation_count",
    "expectancy",
    "confidence",
    "signal_count",
    "execution_robustness",
    "thesis_exit_policy",
    "playbook_summary_json",
]


def _to_strategy_key(strategy_display_name: str) -> str:
    """'Opening Drive Classifier' → 'opening_drive_classifier'"""
    s = strategy_display_name.lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_")


def upsert_strategy_catalog(
    *,
    catalog_key: str,
    symbol: str,
    strategy: str,
    direction: str,
    m1_best: dict[str, Any],
    artifact_path: str,
    notes: str,
    spreadsheet_id: str,
    credentials_path: str | Path,
    sheet_name: str = "Strategy_Catalog",
) -> None:
    """Write or update one row in Strategy_Catalog.

    Matches on `catalog_key`. If not found, appends a new row.
    Credentials must be a service-account JSON path.
    """
    client = GoogleSheetTableClient(
        spreadsheet_id=spreadsheet_id,
        sheet_name=sheet_name,
        credentials_path=Path(credentials_path),
    )
    client.ensure_sheet_exists()

    today = date.today().isoformat()
    strategy_key = _to_strategy_key(strategy)
    operator_notes = f"{notes} | artifact: {artifact_path}"[:300]

    row: dict[str, Any] = {
        "catalog_key":              catalog_key,
        "playbook_id":              catalog_key,
        "symbol":                   symbol,
        "bias_template":            "",
        "strategy_key":             strategy_key,
        "strategy_family":          strategy_key,
        "direction":                direction,
        "lifecycle_status":         "candidate",
        "operator_status_override": "",
        "operator_notes":           operator_notes,
        "bionic_ready":             "false",
        "first_validated_date":     today,
        "last_validated_date":      today,
        "validation_count":         1,
        "expectancy":               round(float(m1_best.get("avg_test_exp_r") or 0), 4),
        "confidence":               round(float(m1_best.get("pct_positive_oos_windows") or 0), 4),
        "signal_count":             int(m1_best.get("oos_signals") or 0),
        "execution_robustness":     "",
        "thesis_exit_policy":       "",
        "playbook_summary_json":    "",
    }

    existing = client.read_rows()

    # Update if row already exists
    for ex in existing:
        if ex.get("catalog_key") == catalog_key:
            ex.update(row)
            client.batch_update_rows(rows=[ex], columns=list(row.keys()))
            return

    # First write ever — write headers + row
    if not existing:
        client.overwrite_table(headers=STRATEGY_CATALOG_HEADERS, rows=[row])
        return

    # Append below existing data
    values = [[row.get(h, "") for h in STRATEGY_CATALOG_HEADERS]]
    client.service.spreadsheets().values().append(
        spreadsheetId=client.spreadsheet_id,
        range=f"{sheet_name}!A1",
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body={"values": values},
    ).execute()
