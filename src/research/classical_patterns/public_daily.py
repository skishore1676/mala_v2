"""Auditable Public.com daily-bar acquisition for rectangle validation.

This module deliberately keeps provider evidence separate from the existing
minute cache. It stores canonical raw responses, normalized daily bars, a
catalogue snapshot, and a hash-bound quality manifest without persisting an
access token.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
import hashlib
import json
from pathlib import Path
import subprocess
import time
from typing import Any

import polars as pl

from src.chronos.client import PublicMarketDataClient
from src.trading_calendar import trading_dates

from .daily_bars import hash_daily_bars, normalize_daily_input


PUBLIC_PERIOD = "FIVE_YEARS"
PUBLIC_AGGREGATION = "ONE_DAY"
MINIMUM_COVERAGE_FRACTION = 0.98
FROZEN_DETECTOR_PATHS = (
    "config/classical_patterns/rectangle_daily_v1.yaml",
    "src/oracle/rectangle_trade_simulator.py",
    "src/research/classical_patterns/contracts.py",
    "src/research/classical_patterns/daily_bars.py",
    "src/research/classical_patterns/lifecycle.py",
    "src/research/classical_patterns/rectangle.py",
)


@dataclass(frozen=True, slots=True)
class PublicDailyDatasetResult:
    dataset_id: str
    output_dir: Path
    manifest_path: Path
    report_path: Path
    canonical_hash: str
    quality_status: str


def acquire_public_daily_dataset(
    *,
    universe_path: Path,
    output_dir: Path,
    client: PublicMarketDataClient | None = None,
    request_delay_seconds: float = 0.12,
) -> PublicDailyDatasetResult:
    """Acquire or resume one frozen-universe Public daily dataset."""

    if request_delay_seconds < 0:
        raise ValueError("request_delay_seconds must be non-negative")
    universe_path = universe_path.expanduser().resolve()
    universe = load_public_validation_universe(universe_path)
    output_dir = output_dir.expanduser().resolve()
    manifest_path = output_dir / "dataset_manifest.json"
    if manifest_path.exists():
        raise ValueError("Completed Public dataset already has a manifest; use a new output root.")
    raw_bars_dir = output_dir / "raw" / "bars"
    daily_dir = output_dir / "daily"
    raw_bars_dir.mkdir(parents=True, exist_ok=True)
    daily_dir.mkdir(parents=True, exist_ok=True)

    provider = client or PublicMarketDataClient(
        period=PUBLIC_PERIOD,
        aggregation=PUBLIC_AGGREGATION,
        instrument_type="EQUITY",
    )
    catalog_path = output_dir / "raw" / "instruments_equity.json"
    if catalog_path.exists():
        catalog_payload = json.loads(catalog_path.read_text(encoding="utf-8"))
    else:
        catalog_payload = provider.fetch_instruments("EQUITY")
        _write_canonical_json(catalog_path, catalog_payload)
    catalog_rows = catalog_payload.get("instruments") or []
    catalog_by_symbol: dict[str, list[dict[str, Any]]] = {}
    for row in catalog_rows:
        instrument = row.get("instrument") or {}
        if instrument.get("type") != "EQUITY" or not instrument.get("symbol"):
            continue
        catalog_by_symbol.setdefault(str(instrument["symbol"]).upper(), []).append(row)

    requested_start = date.fromisoformat(universe["requested_start"])
    requested_end = date.fromisoformat(universe["requested_end"])
    expected_sessions = set(trading_dates(requested_start, requested_end))
    symbol_rows: list[dict[str, Any]] = []
    daily_by_symbol: dict[str, pl.DataFrame] = {}
    for index, symbol in enumerate(universe["symbols"]):
        raw_path = raw_bars_dir / f"{symbol}.json"
        if raw_path.exists():
            payload = json.loads(raw_path.read_text(encoding="utf-8"))
        else:
            payload = provider.fetch_bars_payload(symbol)
            _write_canonical_json(raw_path, payload)
            if request_delay_seconds and index + 1 < len(universe["symbols"]):
                time.sleep(request_delay_seconds)
        daily = public_payload_to_daily(
            symbol=symbol,
            payload=payload,
            start=requested_start,
            end=requested_end,
        )
        daily_path = daily_dir / f"{symbol}.parquet"
        daily.write_parquet(daily_path)
        actual_sessions = set(daily.get_column("session_date").to_list())
        missing = sorted(expected_sessions - actual_sessions)
        unexpected = sorted(actual_sessions - expected_sessions)
        coverage_fraction = (
            len(actual_sessions & expected_sessions) / len(expected_sessions)
            if expected_sessions
            else 0.0
        )
        catalog_matches = catalog_by_symbol.get(symbol, [])
        coverage_ready = (
            coverage_fraction >= MINIMUM_COVERAGE_FRACTION and not unexpected
        )
        symbol_rows.append(
            {
                "symbol": symbol,
                "raw_path": raw_path.relative_to(output_dir).as_posix(),
                "raw_hash": _sha256_path(raw_path),
                "daily_path": daily_path.relative_to(output_dir).as_posix(),
                "daily_file_hash": _sha256_path(daily_path),
                "daily_hash": hash_daily_bars(daily),
                "row_count": len(daily),
                "coverage_start": daily.get_column("session_date").min().isoformat() if len(daily) else None,
                "coverage_end": daily.get_column("session_date").max().isoformat() if len(daily) else None,
                "missing_expected_session_count": len(missing),
                "unexpected_session_count": len(unexpected),
                "coverage_fraction": coverage_fraction,
                "catalog_match_count": len(catalog_matches),
                "catalog_trading_statuses": sorted(
                    {str(row.get("trading")) for row in catalog_matches}
                ),
                "coverage_ready": coverage_ready,
                "ready": coverage_ready and len(catalog_matches) == 1,
            }
        )
        daily_by_symbol[symbol] = daily

    split_checks = [
        _split_continuity_check(daily_by_symbol, check)
        for check in universe["known_split_continuity_checks"]
    ]
    maximum_abs_return = _maximum_absolute_close_return(daily_by_symbol)
    quality_checks = {
        "all_symbols_returned": len(symbol_rows) == len(universe["symbols"])
        and all(row["row_count"] > 0 for row in symbol_rows),
        "all_symbols_cover_at_least_98_percent": all(
            row["coverage_ready"] for row in symbol_rows
        ),
        "catalog_identity_unique": all(row["catalog_match_count"] == 1 for row in symbol_rows),
        "known_split_checks_pass": all(row["passed"] for row in split_checks),
        "daily_rows_are_unique_and_valid": True,
    }
    quality_status = (
        "ready_for_frozen_cohort_validation"
        if all(quality_checks.values())
        else "blocked_data_quality"
    )
    payload = {
        "schema_version": "PublicDailyDatasetV1",
        "dataset_id": universe["universe_id"] + "-daily-v1",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "provider": "public",
        "endpoint_template": "/userapigateway/historicdata/EQUITY/{symbol}/FIVE_YEARS/ONE_DAY",
        "period": PUBLIC_PERIOD,
        "aggregation": PUBLIC_AGGREGATION,
        "config_hash": universe["config_hash"],
        "universe_path": universe_path.name,
        "universe_hash": _sha256_path(universe_path),
        "universe_status": "frozen_current_symbol_cohort_not_point_in_time",
        "requested_start": universe["requested_start"],
        "requested_end": universe["requested_end"],
        "catalog": {
            "raw_path": catalog_path.relative_to(output_dir).as_posix(),
            "raw_hash": _sha256_path(catalog_path),
            "equity_row_count": len(catalog_rows),
        },
        "symbols": symbol_rows,
        "split_continuity_checks": split_checks,
        "maximum_absolute_close_return": maximum_abs_return,
        "quality_checks": quality_checks,
        "quality_status": quality_status,
        "adjustment_provenance": "empirically_split_continuous_provider_policy_undocumented",
        "economic_research_grade": "frozen_cohort_validation_not_population_alpha",
        "git": _git_state(),
        "limitations": universe["limitations"],
    }
    payload["canonical_hash"] = _hash_json(payload)
    _write_json(manifest_path, payload)
    report_path = output_dir / "DATA_QUALITY.md"
    report_path.write_text(_render_data_quality(payload), encoding="utf-8")
    verify_public_daily_dataset(output_dir)
    return PublicDailyDatasetResult(
        dataset_id=str(payload["dataset_id"]),
        output_dir=output_dir,
        manifest_path=manifest_path,
        report_path=report_path,
        canonical_hash=str(payload["canonical_hash"]),
        quality_status=quality_status,
    )


def load_public_validation_universe(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    expected = {
        "schema_version", "universe_id", "config_hash", "selection_date",
        "selection_basis", "requested_start", "requested_end", "symbols",
        "known_split_continuity_checks", "limitations",
    }
    if set(payload) != expected or payload["schema_version"] != "PublicValidationUniverseV1":
        raise ValueError("Public validation universe fields mismatch.")
    symbols = payload["symbols"]
    if not symbols or symbols != sorted(set(symbols)):
        raise ValueError("Public validation universe symbols must be sorted and unique.")
    start = date.fromisoformat(payload["requested_start"])
    end = date.fromisoformat(payload["requested_end"])
    if start >= end:
        raise ValueError("Public validation universe date range is invalid.")
    return payload


def public_payload_to_daily(
    *, symbol: str, payload: dict[str, Any], start: date, end: date
) -> pl.DataFrame:
    """Normalize Public ONE_DAY regular-market bars to the frozen daily contract."""

    rows: list[dict[str, Any]] = []
    for raw in ((payload.get("regularMarket") or {}).get("bars") or []):
        try:
            parsed = datetime.fromisoformat(str(raw["timestamp"]).replace("Z", "+00:00"))
            session_date = parsed.date()
            if not start <= session_date <= end:
                continue
            rows.append(
                {
                    "session_date": session_date,
                    "open": float(raw["open"]),
                    "high": float(raw["high"]),
                    "low": float(raw["low"]),
                    "close": float(raw["close"]),
                    "volume": float(raw.get("volume") or 0),
                }
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(f"Malformed Public daily bar for {symbol}.") from exc
    if not rows:
        raise ValueError(f"Public returned no daily bars for {symbol} in the requested range.")
    return normalize_daily_input(pl.DataFrame(rows), symbol=symbol)


def verify_public_daily_dataset(output_dir: Path) -> dict[str, Any]:
    output_dir = output_dir.expanduser().resolve()
    manifest = json.loads((output_dir / "dataset_manifest.json").read_text(encoding="utf-8"))
    canonical = manifest.pop("canonical_hash", None)
    if canonical != _hash_json(manifest):
        raise ValueError("Public daily dataset canonical hash mismatch.")
    manifest["canonical_hash"] = canonical
    if manifest.get("schema_version") != "PublicDailyDatasetV1":
        raise ValueError("Unsupported Public daily dataset schema.")
    catalog = manifest["catalog"]
    if _sha256_path(output_dir / catalog["raw_path"]) != catalog["raw_hash"]:
        raise ValueError("Public instrument catalog hash mismatch.")
    for row in manifest["symbols"]:
        raw_path = output_dir / row["raw_path"]
        daily_path = output_dir / row["daily_path"]
        if _sha256_path(raw_path) != row["raw_hash"]:
            raise ValueError(f"Public raw payload hash mismatch: {row['symbol']}")
        if _sha256_path(daily_path) != row["daily_file_hash"]:
            raise ValueError(f"Public daily file hash mismatch: {row['symbol']}")
        daily = pl.read_parquet(daily_path)
        if hash_daily_bars(daily) != row["daily_hash"] or len(daily) != row["row_count"]:
            raise ValueError(f"Public normalized daily identity mismatch: {row['symbol']}")
    return manifest


def load_public_daily_dataset(output_dir: Path) -> tuple[dict[str, pl.DataFrame], dict[str, Any]]:
    manifest = verify_public_daily_dataset(output_dir)
    if manifest["quality_status"] != "ready_for_frozen_cohort_validation":
        raise ValueError("Public daily dataset is not ready for frozen-cohort validation.")
    root = output_dir.expanduser().resolve()
    frames = {
        row["symbol"]: pl.read_parquet(root / row["daily_path"])
        for row in manifest["symbols"]
    }
    return frames, manifest


def verify_public_daily_dataset_against_universe(
    *, output_dir: Path, universe_path: Path, config_hash: str
) -> dict[str, Any]:
    """Re-bind a retained dataset to the exact tracked universe contract."""

    manifest = verify_public_daily_dataset(output_dir)
    universe_path = universe_path.expanduser().resolve()
    universe = load_public_validation_universe(universe_path)
    manifest_symbols = [row["symbol"] for row in manifest["symbols"]]
    if (
        universe["config_hash"] != config_hash
        or manifest["config_hash"] != config_hash
        or manifest["universe_hash"] != _sha256_path(universe_path)
        or manifest["universe_path"] != universe_path.name
        or manifest["requested_start"] != universe["requested_start"]
        or manifest["requested_end"] != universe["requested_end"]
        or manifest_symbols != universe["symbols"]
    ):
        raise ValueError("Public dataset does not match the tracked frozen universe.")
    return manifest


def verify_semantic_freeze_for_public_run(
    *, freeze_path: Path, config_hash: str, repo_root: Path | None = None
) -> dict[str, Any]:
    payload = json.loads(freeze_path.read_text(encoding="utf-8"))
    canonical = payload.pop("canonical_hash", None)
    if canonical != _hash_json(payload):
        raise ValueError("Semantic freeze canonical hash mismatch.")
    payload["canonical_hash"] = canonical
    if (
        payload.get("schema_version") != "MalaRectangleSemanticSpecFreezeV1"
        or payload.get("status") != "frozen"
        or payload.get("config_hash") != config_hash
        or payload.get("economic_filtering_allowed") is not False
        or payload.get("trade_worthiness_fields_present") is not False
    ):
        raise ValueError("Semantic freeze does not authorize a complete-population Public run.")
    detector_commit = payload.get("detector_git_commit")
    if not isinstance(detector_commit, str) or not detector_commit:
        raise ValueError("Semantic freeze does not declare its detector commit.")
    _verify_frozen_detector_paths(
        detector_commit=detector_commit,
        repo_root=(repo_root or Path.cwd()).expanduser().resolve(),
    )
    return payload


def _verify_frozen_detector_paths(*, detector_commit: str, repo_root: Path) -> None:
    exists = subprocess.run(
        ["git", "cat-file", "-e", f"{detector_commit}^{{commit}}"],
        cwd=repo_root,
        check=False,
        capture_output=True,
        text=True,
    )
    if exists.returncode != 0:
        raise ValueError("Semantic freeze detector commit is not available in this repository.")
    comparison = subprocess.run(
        ["git", "diff", "--quiet", detector_commit, "HEAD", "--", *FROZEN_DETECTOR_PATHS],
        cwd=repo_root,
        check=False,
        capture_output=True,
        text=True,
    )
    if comparison.returncode == 1:
        raise ValueError("Frozen detector paths changed after the semantic freeze commit.")
    if comparison.returncode != 0:
        raise ValueError("Unable to compare frozen detector paths to the semantic freeze commit.")


def _split_continuity_check(
    daily_by_symbol: dict[str, pl.DataFrame], check: dict[str, Any]
) -> dict[str, Any]:
    symbol = str(check["symbol"])
    effective = date.fromisoformat(str(check["effective_date"]))
    after = daily_by_symbol[symbol].filter(pl.col("session_date") >= effective).head(1)
    before = daily_by_symbol[symbol].filter(pl.col("session_date") < effective).tail(1)
    if before.is_empty() or after.is_empty():
        return {**check, "before_close": None, "after_close": None, "absolute_close_return": None, "passed": False}
    before_close = float(before.get_column("close")[0])
    after_close = float(after.get_column("close")[0])
    absolute_return = abs(after_close / before_close - 1.0)
    return {
        **check,
        "before_close": before_close,
        "after_close": after_close,
        "absolute_close_return": absolute_return,
        "passed": absolute_return <= float(check["maximum_absolute_close_return"]),
    }


def _maximum_absolute_close_return(
    daily_by_symbol: dict[str, pl.DataFrame]
) -> dict[str, Any]:
    maximum = {"symbol": None, "session_date": None, "absolute_close_return": 0.0}
    for symbol, daily in daily_by_symbol.items():
        returns = daily.with_columns(
            (pl.col("close") / pl.col("close").shift(1) - 1.0).abs().alias("absolute_close_return")
        ).drop_nulls("absolute_close_return")
        if returns.is_empty():
            continue
        row = returns.sort("absolute_close_return", descending=True).row(0, named=True)
        if float(row["absolute_close_return"]) > float(maximum["absolute_close_return"]):
            maximum = {
                "symbol": symbol,
                "session_date": row["session_date"].isoformat(),
                "absolute_close_return": float(row["absolute_close_return"]),
            }
    return maximum


def _render_data_quality(manifest: dict[str, Any]) -> str:
    lines = [
        f"# Public Daily Dataset — {manifest['dataset_id']}",
        "",
        f"- Quality status: `{manifest['quality_status']}`",
        f"- Economic grade: `{manifest['economic_research_grade']}`",
        f"- Adjustment evidence: `{manifest['adjustment_provenance']}`",
        f"- Universe: `{manifest['universe_status']}`",
        f"- Symbols: `{len(manifest['symbols'])}`",
        f"- Period: `{manifest['requested_start']}` through `{manifest['requested_end']}`",
        f"- Canonical hash: `{manifest['canonical_hash']}`",
        "",
        "## Checks",
        "",
        *[f"- {key}: `{str(value).lower()}`" for key, value in manifest["quality_checks"].items()],
        "",
        "## Coverage",
        "",
        "| Symbol | Rows | Coverage | Missing | Unexpected | Catalog matches | Ready |",
        "|---|---:|---:|---:|---:|---:|---|",
    ]
    for row in manifest["symbols"]:
        lines.append(
            f"| {row['symbol']} | {row['row_count']} | {row['coverage_fraction']:.2%} "
            f"| {row['missing_expected_session_count']} | {row['unexpected_session_count']} "
            f"| {row['catalog_match_count']} | {str(row['ready']).lower()} |"
        )
    lines.extend(["", "## Known Split Continuity", ""])
    for row in manifest["split_continuity_checks"]:
        observed = row["absolute_close_return"]
        observed_text = f"{observed:.3%}" if observed is not None else "unavailable"
        lines.append(
            f"- {row['symbol']} {row['effective_date']} {row['ratio']}: "
            f"absolute close return `{observed_text}`; passed `{str(row['passed']).lower()}`"
        )
    lines.extend(["", "## Limitations", "", *[f"- {item}" for item in manifest["limitations"]], ""])
    return "\n".join(lines)


def _git_state() -> dict[str, Any]:
    def run(*args: str) -> str:
        return subprocess.run(["git", *args], check=False, capture_output=True, text=True).stdout.strip()
    dirty = [line for line in run("status", "--short").splitlines() if line]
    return {"commit": run("rev-parse", "HEAD"), "branch": run("branch", "--show-current"), "dirty": bool(dirty), "dirty_paths": dirty}


def _write_canonical_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _sha256_path(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _hash_json(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
