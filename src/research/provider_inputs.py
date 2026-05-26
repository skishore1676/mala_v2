"""Normalize provider OHLCV input artifacts for Mala M7 panels."""

from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


DEFAULT_WIDE_PROVIDERS = ("polygon", "schwab", "public")


@dataclass(slots=True, frozen=True)
class ProviderInputArtifacts:
    provider_csvs: dict[str, Path]


def build_provider_input_csvs(
    *,
    wide_csvs: Iterable[str | Path],
    out_dir: str | Path,
    providers: Iterable[str] = DEFAULT_WIDE_PROVIDERS,
) -> ProviderInputArtifacts:
    """Convert wide provider parity CSVs into one normalized CSV per provider.

    Expected wide columns look like `polygon_open`, `schwab_volume`, etc. This
    keeps provider fetching/export details outside the provider-panel gate.
    """

    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    provider_names = tuple(provider.strip().lower() for provider in providers if provider.strip())
    output_rows: dict[str, list[dict[str, Any]]] = {provider: [] for provider in provider_names}

    for raw_path in wide_csvs:
        path = Path(raw_path)
        symbol_from_name = _symbol_from_path(path)
        with path.open("r", encoding="utf-8", newline="") as handle:
            for row in csv.DictReader(handle):
                symbol = str(row.get("symbol") or row.get("ticker") or symbol_from_name).upper()
                timestamp = str(row.get("timestamp") or row.get("datetime") or row.get("time") or "")
                if not symbol or not timestamp:
                    continue
                for provider in provider_names:
                    normalized = _provider_row(row, provider=provider, symbol=symbol, timestamp=timestamp)
                    if normalized:
                        output_rows[provider].append(normalized)

    paths: dict[str, Path] = {}
    for provider, rows in output_rows.items():
        if not rows:
            continue
        path = out / f"{provider}_provider_bars.csv"
        _write_csv(path, rows)
        paths[provider] = path
    return ProviderInputArtifacts(provider_csvs=paths)


def _provider_row(
    row: dict[str, str],
    *,
    provider: str,
    symbol: str,
    timestamp: str,
) -> dict[str, Any] | None:
    values = {
        field: row.get(f"{provider}_{field}", "")
        for field in ("open", "high", "low", "close", "volume")
    }
    if any(value in (None, "") for value in values.values()):
        return None
    return {
        "provider": provider,
        "symbol": symbol,
        "timestamp": timestamp,
        "open": values["open"],
        "high": values["high"],
        "low": values["low"],
        "close": values["close"],
        "volume": values["volume"],
    }


def _symbol_from_path(path: Path) -> str:
    stem = path.stem.upper()
    match = re.match(r"([A-Z0-9]+)_(?:THREE_WAY|PROVIDER|OHLCV)", stem)
    if match:
        return match.group(1)
    return stem.split("_", 1)[0]


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["provider", "symbol", "timestamp", "open", "high", "low", "close", "volume"]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


__all__ = [
    "ProviderInputArtifacts",
    "build_provider_input_csvs",
]
