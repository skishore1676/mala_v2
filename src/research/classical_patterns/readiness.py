"""Local cache readiness audit for classical-pattern semantic review."""

from __future__ import annotations

from dataclasses import asdict, dataclass, fields
from datetime import date, datetime, timezone
import hashlib
import json
from pathlib import Path
from typing import Sequence

import polars as pl

from src.chronos.storage import LocalStorage
from src.trading_calendar import trading_dates

from .contracts import RectangleResearchConfig
from .daily_bars import build_rth_daily_bars, hash_daily_bars


@dataclass(frozen=True, slots=True)
class SymbolReadiness:
    symbol: str
    cache_file_count: int
    source_row_count: int
    source_start: str | None
    source_end: str | None
    duplicate_timestamp_count: int
    invalid_source_row_count: int
    complete_session_count: int
    incomplete_session_count: int
    missing_expected_session_count: int
    unexpected_session_count: int
    coverage_start: str | None
    coverage_end: str | None
    complete_daily_hash: str | None
    semantic_pilot_ready: bool
    readiness_reasons: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class DataReadinessReport:
    schema_version: str
    generated_at: str
    config_hash: str
    requested_start: str | None
    requested_end: str | None
    adjustment_provenance: str
    semantic_review_status: str
    economic_research_status: str
    symbols: tuple[SymbolReadiness, ...]
    report_hash: str


def audit_local_cache(
    *,
    symbols: Sequence[str],
    config: RectangleResearchConfig,
    data_dir: Path,
    start: date | None = None,
    end: date | None = None,
    minimum_complete_sessions: int = 252,
    maximum_missing_fraction: float = 0.02,
) -> DataReadinessReport:
    """Audit retained bars without fetching, repairing, or mutating cache data."""

    if minimum_complete_sessions <= 0:
        raise ValueError("minimum_complete_sessions must be positive")
    if not 0 <= maximum_missing_fraction <= 1:
        raise ValueError("maximum_missing_fraction must be in [0, 1]")
    storage = LocalStorage(base_dir=data_dir)
    rows: list[SymbolReadiness] = []
    for requested_symbol in sorted({symbol.upper() for symbol in symbols if symbol.strip()}):
        rows.append(
            _audit_symbol(
                storage=storage,
                data_dir=data_dir,
                symbol=requested_symbol,
                config=config,
                start=start,
                end=end,
                minimum_complete_sessions=minimum_complete_sessions,
                maximum_missing_fraction=maximum_missing_fraction,
            )
        )
    if not rows:
        raise ValueError("At least one symbol is required for readiness audit.")

    ready_count = sum(row.semantic_pilot_ready for row in rows)
    semantic_status = "ready" if ready_count >= min(4, len(rows)) else "insufficient"
    payload = {
        "schema_version": "ClassicalPatternDataReadinessV1",
        "config_hash": config.source_hash,
        "requested_start": start.isoformat() if start else None,
        "requested_end": end.isoformat() if end else None,
        "adjustment_provenance": "unverified_provider_adjusted",
        "semantic_review_status": semantic_status,
        "economic_research_status": "blocked_unverified_adjustment_and_point_in_time_universe",
        "symbols": [asdict(row) for row in rows],
    }
    report_hash = _readiness_payload_hash(payload)
    return DataReadinessReport(
        generated_at=datetime.now(timezone.utc).isoformat(),
        report_hash=report_hash,
        symbols=tuple(rows),
        **{key: value for key, value in payload.items() if key != "symbols"},
    )


def write_readiness_report(report: DataReadinessReport, output_dir: Path) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "data_readiness.json"
    markdown_path = output_dir / "DATA_READINESS.md"
    payload = asdict(report)
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    lines = [
        "# Classical Pattern Data Readiness",
        "",
        f"- Semantic review: `{report.semantic_review_status}`",
        f"- Economic research: `{report.economic_research_status}`",
        f"- Adjustment provenance: `{report.adjustment_provenance}`",
        f"- Config hash: `{report.config_hash}`",
        f"- Report hash: `{report.report_hash}`",
        "",
        "| Symbol | Complete | Incomplete | Missing | Unexpected | Duplicate timestamps | Semantic pilot |",
        "|---|---:|---:|---:|---:|---:|---|",
    ]
    for row in report.symbols:
        lines.append(
            f"| {row.symbol} | {row.complete_session_count} | {row.incomplete_session_count} "
            f"| {row.missing_expected_session_count} | {row.unexpected_session_count} "
            f"| {row.duplicate_timestamp_count} | {str(row.semantic_pilot_ready).lower()} |"
        )
    lines.extend(
        [
            "",
            "## Boundary",
            "",
            "This audit may authorize a local, outcome-hidden semantic review pilot only.",
            "It does not prove corporate-action adjustment provenance, point-in-time universe",
            "membership, delisting coverage, or fitness for economic claims.",
            "",
        ]
    )
    markdown_path.write_text("\n".join(lines), encoding="utf-8")
    return {"json": json_path, "markdown": markdown_path}


def load_readiness_report(path: Path) -> DataReadinessReport:
    payload = json.loads(path.read_text(encoding="utf-8"))
    expected_keys = {field.name for field in fields(DataReadinessReport)}
    if set(payload) != expected_keys:
        raise ValueError("Data-readiness report fields do not match V1 contract.")
    symbol_keys = {field.name for field in fields(SymbolReadiness)}
    if any(set(row) != symbol_keys for row in payload["symbols"]):
        raise ValueError("Data-readiness symbol fields do not match V1 contract.")
    symbols = tuple(
        SymbolReadiness(
            **{
                **row,
                "readiness_reasons": tuple(row["readiness_reasons"]),
            }
        )
        for row in payload["symbols"]
    )
    report = DataReadinessReport(
        symbols=symbols,
        **{key: value for key, value in payload.items() if key != "symbols"},
    )
    validate_readiness_report(report)
    return report


def validate_readiness_report(report: DataReadinessReport) -> None:
    if report.schema_version != "ClassicalPatternDataReadinessV1":
        raise ValueError("Unsupported data-readiness report version.")
    payload = {
        "schema_version": report.schema_version,
        "config_hash": report.config_hash,
        "requested_start": report.requested_start,
        "requested_end": report.requested_end,
        "adjustment_provenance": report.adjustment_provenance,
        "semantic_review_status": report.semantic_review_status,
        "economic_research_status": report.economic_research_status,
        "symbols": [asdict(row) for row in report.symbols],
    }
    if report.report_hash != _readiness_payload_hash(payload):
        raise ValueError("Data-readiness report hash mismatch.")


def _readiness_payload_hash(payload: dict[str, object]) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _audit_symbol(
    *,
    storage: LocalStorage,
    data_dir: Path,
    symbol: str,
    config: RectangleResearchConfig,
    start: date | None,
    end: date | None,
    minimum_complete_sessions: int,
    maximum_missing_fraction: float,
) -> SymbolReadiness:
    files = sorted((data_dir / symbol).glob("*.parquet"))
    source = storage.load_bars(symbol, start=start, end=end)
    if source.is_empty():
        return SymbolReadiness(
            symbol=symbol,
            cache_file_count=len(files),
            source_row_count=0,
            source_start=None,
            source_end=None,
            duplicate_timestamp_count=0,
            invalid_source_row_count=0,
            complete_session_count=0,
            incomplete_session_count=0,
            missing_expected_session_count=0,
            unexpected_session_count=0,
            coverage_start=None,
            coverage_end=None,
            complete_daily_hash=None,
            semantic_pilot_ready=False,
            readiness_reasons=("no_source_rows",),
        )

    source = source.sort("timestamp")
    duplicate_count = len(source) - source.get_column("timestamp").n_unique()
    invalid_count = _invalid_source_count(source)
    reasons: list[str] = []
    if duplicate_count:
        reasons.append("duplicate_timestamps")
    if invalid_count:
        reasons.append("invalid_ohlcv")

    daily = pl.DataFrame()
    try:
        daily = build_rth_daily_bars(
            source,
            symbol=symbol,
            session=config.session,
            require_complete=False,
        )
    except ValueError as exc:
        reasons.append(f"daily_build_failed:{type(exc).__name__}")

    complete = daily.filter(pl.col("complete_session")) if not daily.is_empty() else daily
    incomplete_count = len(daily) - len(complete)
    coverage_start = complete.get_column("session_date").min() if not complete.is_empty() else None
    coverage_end = complete.get_column("session_date").max() if not complete.is_empty() else None
    missing_count = 0
    unexpected_count = 0
    if coverage_start and coverage_end:
        expected = set(trading_dates(coverage_start, coverage_end))
        actual = set(complete.get_column("session_date").to_list())
        missing_count = len(expected - actual)
        unexpected_count = len(actual - expected)
        if expected and missing_count / len(expected) > maximum_missing_fraction:
            reasons.append("excess_missing_sessions")
        if unexpected_count:
            reasons.append("unexpected_session_dates")
    if len(complete) < minimum_complete_sessions:
        reasons.append("insufficient_complete_sessions")

    ready = not reasons
    source_start = source.get_column("timestamp").min()
    source_end = source.get_column("timestamp").max()
    return SymbolReadiness(
        symbol=symbol,
        cache_file_count=len(files),
        source_row_count=len(source),
        source_start=source_start.isoformat() if source_start else None,
        source_end=source_end.isoformat() if source_end else None,
        duplicate_timestamp_count=duplicate_count,
        invalid_source_row_count=invalid_count,
        complete_session_count=len(complete),
        incomplete_session_count=incomplete_count,
        missing_expected_session_count=missing_count,
        unexpected_session_count=unexpected_count,
        coverage_start=coverage_start.isoformat() if coverage_start else None,
        coverage_end=coverage_end.isoformat() if coverage_end else None,
        complete_daily_hash=hash_daily_bars(complete) if not complete.is_empty() else None,
        semantic_pilot_ready=ready,
        readiness_reasons=tuple(reasons),
    )


def _invalid_source_count(source: pl.DataFrame) -> int:
    required = ("open", "high", "low", "close", "volume")
    if set(required) - set(source.columns):
        return len(source)
    return len(
        source.filter(
            pl.any_horizontal([pl.col(column).is_null() for column in required])
            | pl.any_horizontal(
                [~pl.col(column).cast(pl.Float64).is_finite() for column in required]
            )
            | (pl.col("high") < pl.max_horizontal("open", "close", "low"))
            | (pl.col("low") > pl.min_horizontal("open", "close", "high"))
            | (pl.col("volume") < 0)
        )
    )
