from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from src.research.google_sheets import GoogleSheetTableClient


def test_google_sheet_client_appends_missing_headers(tmp_path: Path) -> None:
    service = _FakeService(headers=["catalog_key", "operator_notes"])
    client = GoogleSheetTableClient(
        spreadsheet_id="sheet-id",
        sheet_name="Strategy_Catalog",
        credentials_path=tmp_path / "credentials.json",
        service=service,
    )

    missing = client.ensure_columns(["operator_notes", "steward_recommendation", "steward_notes"])

    assert missing == ["steward_recommendation", "steward_notes"]
    assert service.updated_range == "Strategy_Catalog!C1:D1"
    assert service.updated_body == {"values": [["steward_recommendation", "steward_notes"]]}


def test_google_sheet_client_expands_grid_before_appending_headers(tmp_path: Path) -> None:
    service = _FakeService(
        headers=["catalog_key", "operator_notes"],
        grid_column_count=2,
    )
    client = GoogleSheetTableClient(
        spreadsheet_id="sheet-id",
        sheet_name="Strategy_Catalog",
        credentials_path=tmp_path / "credentials.json",
        service=service,
    )

    missing = client.ensure_columns(["management_policy_spec"])

    assert missing == ["management_policy_spec"]
    assert service.batch_updated_body == {
        "requests": [
            {
                "updateSheetProperties": {
                    "properties": {
                        "sheetId": 123,
                        "gridProperties": {"columnCount": 3},
                    },
                    "fields": "gridProperties.columnCount",
                }
            }
        ]
    }
    assert service.updated_range == "Strategy_Catalog!C1:C1"
    assert service.updated_body == {"values": [["management_policy_spec"]]}


def test_google_sheet_client_clears_blank_batch_update_values(tmp_path: Path) -> None:
    service = _FakeService(headers=["operator_action", "status"])
    client = GoogleSheetTableClient(
        spreadsheet_id="sheet-id",
        sheet_name="Research_Intake",
        credentials_path=tmp_path / "credentials.json",
        service=service,
    )

    result = client.batch_update_rows(
        rows=[{"row_index": 4, "operator_action": "", "status": "created_pending"}],
        columns=["operator_action", "status"],
    )

    assert service.cleared_ranges == ["Research_Intake!A4"]
    assert service.batch_updated_body == {
        "valueInputOption": "USER_ENTERED",
        "data": [{"range": "Research_Intake!B4", "values": [["created_pending"]]}],
    }
    assert result == {
        "clear": {"clearedRanges": ["Research_Intake!A4"]},
        "update": {"updatedData": service.batch_updated_body},
    }


def test_google_sheet_client_requires_exact_existing_tab_before_mutation(tmp_path: Path) -> None:
    service = _FakeService(headers=["catalog_key"], sheet_titles=["Mala_Evidence_v1"])
    client = GoogleSheetTableClient(
        spreadsheet_id="sheet-id",
        sheet_name="Strategy_Catalog",
        credentials_path=tmp_path / "credentials.json",
        service=service,
    )

    with pytest.raises(RuntimeError, match="required tab 'Strategy_Catalog' is missing"):
        client.require_sheet_exists()

    assert not service.batch_updated_body


class _FakeService:
    def __init__(
        self,
        headers: list[str],
        sheet_titles: list[str] | None = None,
        grid_column_count: int = 26,
    ) -> None:
        self.headers = headers
        self.sheet_titles = sheet_titles or ["Strategy_Catalog", "Research_Intake"]
        self.grid_column_count = grid_column_count
        self.updated_range = ""
        self.updated_body: dict[str, Any] = {}
        self.cleared_ranges: list[str] = []
        self.batch_updated_body: dict[str, Any] = {}
        self._last_action = ""

    def spreadsheets(self) -> "_FakeService":
        return self

    def values(self) -> "_FakeService":
        return self

    def get(self, *, spreadsheetId: str, range: str | None = None) -> "_FakeService":
        self._last_action = "metadata" if range is None else "values_get"
        return self

    def update(
        self,
        *,
        spreadsheetId: str,
        range: str,
        valueInputOption: str,
        body: dict[str, Any],
    ) -> "_FakeService":
        self.updated_range = range
        self.updated_body = body
        return self

    def batchClear(self, *, spreadsheetId: str, body: dict[str, Any]) -> "_FakeService":
        self.cleared_ranges = list(body["ranges"])
        self._last_action = "batchClear"
        return self

    def batchUpdate(
        self,
        *,
        spreadsheetId: str,
        body: dict[str, Any],
    ) -> "_FakeService":
        self.batch_updated_body = body
        self._last_action = "batchUpdate"
        return self

    def execute(self) -> dict[str, Any]:
        if self._last_action == "batchClear":
            self._last_action = ""
            return {"clearedRanges": self.cleared_ranges}
        if self._last_action == "batchUpdate":
            self._last_action = ""
            return {"updatedData": self.batch_updated_body}
        if self._last_action == "metadata":
            self._last_action = ""
            return {
                "sheets": [
                    {
                        "properties": {
                            "title": title,
                            "sheetId": 123 + index,
                            "gridProperties": {"columnCount": self.grid_column_count},
                        }
                    }
                    for index, title in enumerate(self.sheet_titles)
                ]
            }
        self._last_action = ""
        return {"values": [self.headers]}
