"""Validate a published Mala_Evidence_v1 table against a local CSV artifact."""

from __future__ import annotations

import argparse
import csv
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.config import settings
from src.research.google_sheets import GoogleSheetTableClient


DEFAULT_RANGE_SUFFIX = ""


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", required=True, help="Published local CSV artifact to compare.")
    parser.add_argument("--sheet-id", default="", help="Google spreadsheet ID or URL.")
    parser.add_argument("--sheet-name", default="Mala_Evidence_v1")
    parser.add_argument("--credentials-path", default="", help="Google service-account JSON path.")
    parser.add_argument("--range-suffix", default=DEFAULT_RANGE_SUFFIX, help="Optional explicit A1 range suffix. Defaults to CSV-sized range.")
    parser.add_argument("--out-dir", default="research/results/mala_sheet_readback_validation")
    args = parser.parse_args(argv)

    spreadsheet_id = args.sheet_id or settings.strategy_catalog_sheet_id
    credentials_path = args.credentials_path or settings.google_api_credentials_path
    if not spreadsheet_id:
        parser.error("--sheet-id or STRATEGY_CATALOG_SHEET_ID is required")
    if not credentials_path:
        parser.error("--credentials-path or GOOGLE_API_CREDENTIALS_PATH is required")

    result = validate_sheet_readback(
        csv_path=Path(args.csv),
        spreadsheet_id=spreadsheet_id,
        sheet_name=args.sheet_name,
        credentials_path=Path(credentials_path),
        range_suffix=args.range_suffix,
    )
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    out_dir = Path(args.out_dir) / stamp
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "sheet_readback_validation.json"
    md_path = out_dir / "SHEET_READBACK_VALIDATION.md"
    json_path.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_path.write_text(render_markdown(result), encoding="utf-8")

    print(f"SHEET_READBACK_VALIDATION={md_path}")
    print(f"SHEET_READBACK_JSON={json_path}")
    print(f"EXPECTED_ROWS={result['expected_rows']}")
    print(f"SHEET_ROWS={result['sheet_rows']}")
    print(f"EXPECTED_COLUMNS={result['expected_columns']}")
    print(f"SHEET_COLUMNS={result['sheet_columns']}")
    print(f"CELL_MISMATCHES={len(result['cell_mismatches'])}")
    print(f"HEADER_MISMATCHES={len(result['header_mismatches'])}")
    print(f"VALIDATION_STATUS={result['status']}")
    return 0 if result["status"] == "passed" else 2


def validate_sheet_readback(
    *,
    csv_path: Path,
    spreadsheet_id: str,
    sheet_name: str,
    credentials_path: Path,
    range_suffix: str = DEFAULT_RANGE_SUFFIX,
    service: Any | None = None,
) -> dict[str, Any]:
    expected = _read_csv_table(csv_path)
    client = GoogleSheetTableClient(
        spreadsheet_id=spreadsheet_id,
        sheet_name=sheet_name,
        credentials_path=credentials_path,
        service=service,
    )
    client.require_sheet_exists()
    resolved_range_suffix = range_suffix or _range_suffix_for_table(expected)
    sheet_values = _read_sheet_values(client, range_suffix=resolved_range_suffix)
    if not sheet_values:
        sheet_headers: list[str] = []
        sheet_rows: list[list[Any]] = []
    else:
        sheet_headers = [str(value).strip() for value in sheet_values[0]]
        sheet_rows = sheet_values[1:]

    expected_headers = expected[0] if expected else []
    expected_rows = expected[1:] if len(expected) > 1 else []
    header_mismatches = _compare_headers(expected_headers, sheet_headers)
    cell_mismatches = _compare_rows(expected_rows, sheet_rows, expected_headers)
    status = "passed" if not header_mismatches and not cell_mismatches else "failed"
    return {
        "status": status,
        "checked_at": datetime.now(UTC).isoformat(),
        "csv_path": str(csv_path),
        "spreadsheet_id": client.spreadsheet_id,
        "sheet_name": sheet_name,
        "range_suffix": resolved_range_suffix,
        "expected_columns": len(expected_headers),
        "sheet_columns": len(sheet_headers),
        "expected_rows": len(expected_rows),
        "sheet_rows": len(sheet_rows),
        "header_mismatches": header_mismatches,
        "cell_mismatches": cell_mismatches,
    }


def render_markdown(result: dict[str, Any]) -> str:
    lines = [
        "# Sheet Readback Validation",
        "",
        f"- status: `{result['status']}`",
        f"- sheet: `{result['sheet_name']}`",
        f"- range: `{result['range_suffix']}`",
        f"- expected rows: `{result['expected_rows']}`",
        f"- sheet rows: `{result['sheet_rows']}`",
        f"- expected columns: `{result['expected_columns']}`",
        f"- sheet columns: `{result['sheet_columns']}`",
        f"- header mismatches: `{len(result['header_mismatches'])}`",
        f"- cell mismatches: `{len(result['cell_mismatches'])}`",
    ]
    if result["header_mismatches"]:
        lines.extend(["", "## Header Mismatches", ""])
        for mismatch in result["header_mismatches"][:25]:
            lines.append(f"- col `{mismatch['column']}` expected `{mismatch['expected']}` got `{mismatch['actual']}`")
    if result["cell_mismatches"]:
        lines.extend(["", "## Cell Mismatches", ""])
        for mismatch in result["cell_mismatches"][:50]:
            lines.append(
                f"- {mismatch['cell']} `{mismatch['column']}` expected `{mismatch['expected']}` got `{mismatch['actual']}`"
            )
    return "\n".join(lines) + "\n"


def _read_csv_table(path: Path) -> list[list[str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return [list(row) for row in csv.reader(handle)]


def _read_sheet_values(client: GoogleSheetTableClient, *, range_suffix: str) -> list[list[Any]]:
    result = (
        client.service.spreadsheets()
        .values()
        .get(spreadsheetId=client.spreadsheet_id, range=f"{client.sheet_name}!{range_suffix}")
        .execute()
    )
    return result.get("values", [])


def _range_suffix_for_table(table: list[list[str]]) -> str:
    if not table:
        return "A1:A1"
    width = max((len(row) for row in table), default=1)
    height = max(len(table), 1)
    last_column = _column_label(max(width, 1) - 1)
    return f"A1:{last_column}{height}"


def _compare_headers(expected: list[str], actual: list[str]) -> list[dict[str, Any]]:
    mismatches: list[dict[str, Any]] = []
    width = max(len(expected), len(actual))
    for index in range(width):
        expected_value = expected[index] if index < len(expected) else ""
        actual_value = actual[index] if index < len(actual) else ""
        if _normalize_cell(expected_value) != _normalize_cell(actual_value):
            mismatches.append(
                {
                    "column": _column_label(index),
                    "expected": expected_value,
                    "actual": actual_value,
                }
            )
    return mismatches


def _compare_rows(expected: list[list[str]], actual: list[list[Any]], headers: list[str]) -> list[dict[str, Any]]:
    mismatches: list[dict[str, Any]] = []
    height = max(len(expected), len(actual))
    width = len(headers)
    for row_index in range(height):
        expected_row = expected[row_index] if row_index < len(expected) else []
        actual_row = actual[row_index] if row_index < len(actual) else []
        for column_index in range(width):
            expected_value = expected_row[column_index] if column_index < len(expected_row) else ""
            actual_value = actual_row[column_index] if column_index < len(actual_row) else ""
            if _normalize_cell(expected_value) != _normalize_cell(actual_value):
                mismatches.append(
                    {
                        "cell": f"{_column_label(column_index)}{row_index + 2}",
                        "row": row_index + 2,
                        "column": headers[column_index] if column_index < len(headers) else _column_label(column_index),
                        "expected": str(expected_value),
                        "actual": str(actual_value),
                    }
                )
    return mismatches


def _normalize_cell(value: Any) -> str:
    text = str(value if value is not None else "").strip()
    if text.upper() in {"TRUE", "FALSE"}:
        return text.lower()
    return text


def _column_label(index: int) -> str:
    label = ""
    current = index + 1
    while current:
        current, remainder = divmod(current - 1, 26)
        label = chr(65 + remainder) + label
    return label


if __name__ == "__main__":
    raise SystemExit(main())
