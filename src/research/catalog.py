"""
Strategy_Catalog writer.

Upserts a single row into the Strategy_Catalog Google Sheet tab.
Only called when a hypothesis reaches `promote` (M5 pass).
The row lands with lifecycle_status=candidate; a human flips it to approved.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

from src.research.google_sheets import GoogleSheetTableClient

STRATEGY_CATALOG_HEADERS = [
    "catalog_key",
    "symbol",
    "strategy",
    "direction",
    "lifecycle_status",
    "first_validated_date",
    "m1_exp_r",
    "m1_pct_positive",
    "m1_signals",
    "artifact_path",
    "notes",
]


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

    row: dict[str, Any] = {
        "catalog_key":          catalog_key,
        "symbol":               symbol,
        "strategy":             strategy,
        "direction":            direction,
        "lifecycle_status":     "candidate",
        "first_validated_date": date.today().isoformat(),
        "m1_exp_r":             round(float(m1_best.get("avg_test_exp_r") or 0), 4),
        "m1_pct_positive":      round(float(m1_best.get("pct_positive_oos_windows") or 0), 4),
        "m1_signals":           int(m1_best.get("oos_signals") or 0),
        "artifact_path":        artifact_path,
        "notes":                notes[:300],
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
