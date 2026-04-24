from __future__ import annotations

from pathlib import Path
from typing import Any

from src.research import catalog
from src.research.catalog import upsert_strategy_catalog


class FakeSheetClient:
    instances: list["FakeSheetClient"] = []

    def __init__(
        self,
        *,
        spreadsheet_id: str,
        sheet_name: str,
        credentials_path: Path,
    ) -> None:
        self.spreadsheet_id = spreadsheet_id
        self.sheet_name = sheet_name
        self.credentials_path = credentials_path
        self.rows: list[dict[str, Any]] = []
        self.overwritten: tuple[list[str], list[dict[str, Any]]] | None = None
        self.updated: tuple[list[dict[str, Any]], list[str]] | None = None
        self.service = _FakeAppendService(self)
        FakeSheetClient.instances.append(self)

    def ensure_sheet_exists(self) -> None:
        return None

    def read_rows(self) -> list[dict[str, Any]]:
        return [dict(row) for row in self.rows]

    def batch_update_rows(self, *, rows: list[dict[str, Any]], columns: list[str]) -> dict[str, Any]:
        self.updated = (rows, columns)
        return {}

    def overwrite_table(self, *, headers: list[str], rows: list[dict[str, Any]]) -> dict[str, Any]:
        self.overwritten = (headers, rows)
        return {}


class _FakeAppendService:
    def __init__(self, client: FakeSheetClient) -> None:
        self.client = client
        self.append_body: dict[str, Any] | None = None

    def spreadsheets(self) -> "_FakeAppendService":
        return self

    def values(self) -> "_FakeAppendService":
        return self

    def append(self, **kwargs: Any) -> "_FakeAppendService":
        self.append_body = kwargs
        return self

    def execute(self) -> dict[str, Any]:
        return {}


def test_catalog_strategy_keys_match_canonical_surface_names() -> None:
    assert catalog._to_strategy_key("Market Impulse (Cross & Reclaim)") == "market_impulse"
    assert catalog._to_strategy_key("Jerk-Pivot Momentum (tight)") == "jerk_pivot_momentum"
    assert catalog._to_strategy_key("Opening Drive v2 (Short Continue)") == "opening_drive_classifier"
    assert catalog._to_strategy_key("Regime Router (Kinematic + Compression)") == "regime_router"


def test_upsert_strategy_catalog_writes_canonical_market_impulse_row(monkeypatch) -> None:
    FakeSheetClient.instances.clear()
    monkeypatch.setattr(catalog, "GoogleSheetTableClient", FakeSheetClient)

    upsert_strategy_catalog(
        catalog_key="spy-mi-short",
        symbol="SPY",
        strategy="Market Impulse (Cross & Reclaim)",
        m5_best={
            "ticker": "SPY",
            "direction": "short",
            "base_exp_r": 0.12345,
            "holdout_win_rate": 0.61,
            "holdout_trades": 42,
            "mc_prob_positive_exp": 0.7321,
            "execution_profile": "debit_spread_default",
        },
        spreadsheet_id="sheet-id",
        credentials_path=Path("/tmp/service-account.json"),
    )

    client = FakeSheetClient.instances[-1]
    assert client.overwritten is not None
    _, rows = client.overwritten
    row = rows[0]
    assert row["strategy_key"] == "market_impulse"
    assert row["strategy_family"] == "market_impulse"
    assert row["bias_template"] == "bearish_trend_intraday"
    assert row["lifecycle_status"] == "candidate"
    assert row["bhiksha_ready"] == "false"


def test_upsert_strategy_catalog_marks_supported_strategy_and_exit_ready(monkeypatch) -> None:
    FakeSheetClient.instances.clear()
    monkeypatch.setattr(catalog, "GoogleSheetTableClient", FakeSheetClient)

    upsert_strategy_catalog(
        catalog_key="spy-mi-short",
        symbol="SPY",
        strategy="Market Impulse (Cross & Reclaim)",
        m5_best={
            "ticker": "SPY",
            "direction": "short",
            "base_exp_r": 0.12345,
            "holdout_win_rate": 0.61,
            "holdout_trades": 42,
            "mc_prob_positive_exp": 0.7321,
            "execution_profile": "single_option",
        },
        spreadsheet_id="sheet-id",
        credentials_path=Path("/tmp/service-account.json"),
        exit_opt={
            "thesis_exit_policy": "fixed_rr_underlying",
            "thesis_exit_params": {
                "stop_loss_underlying_pct": 0.0035,
                "take_profit_underlying_r_multiple": 1.5,
            },
            "catastrophe_exit_params": {"hard_flat_time_et": "15:55", "stop_loss_pct": 0.35},
        },
    )

    client = FakeSheetClient.instances[-1]
    assert client.overwritten is not None
    _, rows = client.overwritten
    row = rows[0]
    assert row["strategy_key"] == "market_impulse"
    assert row["thesis_exit_policy"] == "fixed_rr_underlying"
    assert row["bhiksha_ready"] == "true"
