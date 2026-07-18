from __future__ import annotations

from datetime import date
import hashlib
import json
from pathlib import Path

import pytest

from src.research.classical_patterns.contracts import load_rectangle_config
from src.research.classical_patterns.public_daily import (
    acquire_public_daily_dataset,
    load_public_daily_dataset,
    load_public_validation_universe,
    verify_public_daily_dataset,
    verify_semantic_freeze_for_public_run,
)
from src.trading_calendar import trading_dates


CONFIG_PATH = Path("config/classical_patterns/rectangle_daily_v1.yaml")
PUBLIC_UNIVERSE_PATH = Path(
    "config/classical_patterns/public_validation_universe_v1.json"
)


class StubPublicClient:
    def __init__(self, symbols: list[str], start: date, end: date) -> None:
        self.symbols = symbols
        self.start = start
        self.end = end
        self.requested: list[str] = []

    def fetch_instruments(self, instrument_type: str) -> dict:
        assert instrument_type == "EQUITY"
        return {
            "instruments": [
                {
                    "instrument": {"symbol": symbol, "type": "EQUITY"},
                    "trading": "ENABLED",
                }
                for symbol in self.symbols
            ]
        }

    def fetch_bars_payload(self, symbol: str) -> dict:
        self.requested.append(symbol)
        bars = []
        for index, session_date in enumerate(trading_dates(self.start, self.end)):
            close = 100.0 + index
            bars.append(
                {
                    "timestamp": f"{session_date.isoformat()}T20:00:00Z",
                    "open": close - 0.5,
                    "high": close + 1.0,
                    "low": close - 1.0,
                    "close": close,
                    "volume": 1_000_000,
                }
            )
        return {"regularMarket": {"bars": bars}}


def _hash_json(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _write_universe(path: Path, *, config_hash: str) -> None:
    payload = {
        "schema_version": "PublicValidationUniverseV1",
        "universe_id": "test-public-cohort-v1",
        "config_hash": config_hash,
        "selection_date": "2026-07-17",
        "selection_basis": "Frozen before outcomes.",
        "requested_start": "2024-01-02",
        "requested_end": "2024-01-10",
        "symbols": ["AAA", "BBB"],
        "known_split_continuity_checks": [],
        "limitations": ["Test cohort only."],
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_tracked_public_universe_matches_frozen_rectangle_config() -> None:
    config = load_rectangle_config(CONFIG_PATH)
    universe = load_public_validation_universe(PUBLIC_UNIVERSE_PATH)

    assert universe["config_hash"] == config.source_hash
    assert len(universe["symbols"]) == 43
    assert universe["symbols"] == sorted(set(universe["symbols"]))


def test_public_daily_acquisition_is_hash_bound_and_loadable(tmp_path: Path) -> None:
    config_hash = load_rectangle_config(CONFIG_PATH).source_hash
    universe_path = tmp_path / "universe.json"
    _write_universe(universe_path, config_hash=config_hash)
    output_dir = tmp_path / "dataset"
    client = StubPublicClient(
        ["AAA", "BBB"],
        date(2024, 1, 2),
        date(2024, 1, 10),
    )

    result = acquire_public_daily_dataset(
        universe_path=universe_path,
        output_dir=output_dir,
        client=client,
        request_delay_seconds=0,
    )
    frames, manifest = load_public_daily_dataset(output_dir)

    assert result.quality_status == "ready_for_frozen_cohort_validation"
    assert manifest["quality_checks"] == {
        "all_symbols_cover_at_least_98_percent": True,
        "all_symbols_returned": True,
        "catalog_identity_unique": True,
        "daily_rows_are_unique_and_valid": True,
        "known_split_checks_pass": True,
    }
    assert set(frames) == {"AAA", "BBB"}
    assert client.requested == ["AAA", "BBB"]
    assert "accessToken" not in (output_dir / "dataset_manifest.json").read_text()
    assert (output_dir / "DATA_QUALITY.md").exists()


def test_public_daily_verifier_rejects_raw_payload_tampering(tmp_path: Path) -> None:
    config_hash = load_rectangle_config(CONFIG_PATH).source_hash
    universe_path = tmp_path / "universe.json"
    _write_universe(universe_path, config_hash=config_hash)
    output_dir = tmp_path / "dataset"
    acquire_public_daily_dataset(
        universe_path=universe_path,
        output_dir=output_dir,
        client=StubPublicClient(
            ["AAA", "BBB"],
            date(2024, 1, 2),
            date(2024, 1, 10),
        ),
        request_delay_seconds=0,
    )
    raw_path = output_dir / "raw" / "bars" / "AAA.json"
    raw_path.write_text(raw_path.read_text() + " ", encoding="utf-8")

    with pytest.raises(ValueError, match="raw payload hash mismatch: AAA"):
        verify_public_daily_dataset(output_dir)


def test_semantic_freeze_verification_is_hash_and_policy_bound(tmp_path: Path) -> None:
    config_hash = load_rectangle_config(CONFIG_PATH).source_hash
    payload = {
        "schema_version": "MalaRectangleSemanticSpecFreezeV1",
        "status": "frozen",
        "config_hash": config_hash,
        "economic_filtering_allowed": False,
        "trade_worthiness_fields_present": False,
    }
    payload["canonical_hash"] = _hash_json(payload)
    freeze_path = tmp_path / "freeze.json"
    freeze_path.write_text(json.dumps(payload), encoding="utf-8")

    verified = verify_semantic_freeze_for_public_run(
        freeze_path=freeze_path,
        config_hash=config_hash,
    )
    assert verified["status"] == "frozen"

    payload["economic_filtering_allowed"] = True
    payload.pop("canonical_hash")
    payload["canonical_hash"] = _hash_json(payload)
    freeze_path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="does not authorize"):
        verify_semantic_freeze_for_public_run(
            freeze_path=freeze_path,
            config_hash=config_hash,
        )
