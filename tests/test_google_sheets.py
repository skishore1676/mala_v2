from __future__ import annotations

from pathlib import Path
from typing import Any

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


class _FakeService:
    def __init__(self, headers: list[str]) -> None:
        self.headers = headers
        self.updated_range = ""
        self.updated_body: dict[str, Any] = {}

    def spreadsheets(self) -> "_FakeService":
        return self

    def values(self) -> "_FakeService":
        return self

    def get(self, *, spreadsheetId: str, range: str) -> "_FakeService":
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

    def execute(self) -> dict[str, Any]:
        return {"values": [self.headers]}
