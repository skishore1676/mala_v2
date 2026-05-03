"""Provider volume parity reports from Bhiksha divergence CSVs."""

from __future__ import annotations

import csv
import math
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable


DEFAULT_VOLUME_WINDOWS = (1, 3, 5, 10, 20)
DEFAULT_AGGREGATE_MINUTES = (1, 3, 5)
DEFAULT_GATE_THRESHOLDS = (1.0, 1.2, 1.4)
DEFAULT_FEATURE_COLUMNS = (
    "close_pct",
    "volume_ma_20_pct",
    "directional_mass_pct",
    "directional_mass_ma_20_pct",
    "vpoc_4h_pct",
)


@dataclass(slots=True, frozen=True)
class ProviderVolumeParityArtifacts:
    report_md: Path
    volume_window_csv: Path
    relative_volume_csv: Path
    feature_csv: Path


@dataclass(slots=True, frozen=True)
class ProviderVolumeRow:
    symbol: str
    timestamp: str
    trade_date: str
    session: str
    volume_schwab: float
    volume_polygon: float
    raw: dict[str, str]


def build_provider_volume_parity_report(
    *,
    divergence_dir: str | Path,
    out_dir: str | Path,
    session: str = "regular",
    volume_windows: Iterable[int] = DEFAULT_VOLUME_WINDOWS,
    aggregate_minutes: Iterable[int] = DEFAULT_AGGREGATE_MINUTES,
    relative_volume_window: int = 20,
    gate_thresholds: Iterable[float] = DEFAULT_GATE_THRESHOLDS,
) -> ProviderVolumeParityArtifacts:
    """Summarize whether provider volume mismatch survives smoothing.

    Bhiksha writes divergence CSVs with one row per aligned bar and columns such
    as `volume_schwab`, `volume_polygon`, and derived feature pct diffs. This
    report intentionally consumes those artifacts instead of fetching either
    provider again.
    """

    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    rows_by_symbol = _load_divergence_rows(Path(divergence_dir), session=session)

    volume_windows = tuple(sorted({max(1, int(window)) for window in volume_windows}))
    aggregate_minutes = tuple(sorted({max(1, int(window)) for window in aggregate_minutes}))
    gate_thresholds = tuple(sorted(float(threshold) for threshold in gate_thresholds))
    relative_volume_window = max(2, int(relative_volume_window))

    volume_rows = _volume_window_rows(rows_by_symbol, volume_windows)
    relative_rows = _relative_volume_rows(
        rows_by_symbol,
        aggregate_minutes=aggregate_minutes,
        relative_volume_window=relative_volume_window,
        gate_thresholds=gate_thresholds,
    )
    feature_rows = _feature_rows(rows_by_symbol)

    volume_window_csv = out / "provider_volume_window_parity.csv"
    relative_volume_csv = out / "provider_relative_volume_parity.csv"
    feature_csv = out / "provider_feature_parity.csv"
    report_md = out / "PROVIDER_VOLUME_PARITY_REPORT.md"

    _write_csv(volume_window_csv, volume_rows)
    _write_csv(relative_volume_csv, relative_rows)
    _write_csv(feature_csv, feature_rows)
    report_md.write_text(
        _render_report(
            divergence_dir=Path(divergence_dir),
            session=session,
            rows_by_symbol=rows_by_symbol,
            volume_rows=volume_rows,
            relative_rows=relative_rows,
            feature_rows=feature_rows,
            relative_volume_window=relative_volume_window,
            volume_window_csv=volume_window_csv,
            relative_volume_csv=relative_volume_csv,
            feature_csv=feature_csv,
        ),
        encoding="utf-8",
    )
    return ProviderVolumeParityArtifacts(
        report_md=report_md,
        volume_window_csv=volume_window_csv,
        relative_volume_csv=relative_volume_csv,
        feature_csv=feature_csv,
    )


def _load_divergence_rows(root: Path, *, session: str) -> dict[str, list[ProviderVolumeRow]]:
    paths = _divergence_csv_paths(root)
    rows_by_symbol: dict[str, list[ProviderVolumeRow]] = defaultdict(list)
    for path in paths:
        symbol = _symbol_from_path(path)
        with path.open("r", encoding="utf-8", newline="") as handle:
            for raw in csv.DictReader(handle):
                if not _session_allowed(raw, session):
                    continue
                try:
                    schwab_volume = float(raw.get("volume_schwab") or "")
                    polygon_volume = float(raw.get("volume_polygon") or "")
                except ValueError:
                    continue
                timestamp = str(raw.get("timestamp") or "")
                if not timestamp:
                    continue
                rows_by_symbol[symbol].append(
                    ProviderVolumeRow(
                        symbol=symbol,
                        timestamp=timestamp,
                        trade_date=_trade_date(raw, timestamp),
                        session=str(raw.get("session") or ""),
                        volume_schwab=schwab_volume,
                        volume_polygon=polygon_volume,
                        raw=raw,
                    )
                )
    return {
        symbol: sorted(rows, key=lambda row: row.timestamp)
        for symbol, rows in sorted(rows_by_symbol.items())
        if rows
    }


def _divergence_csv_paths(root: Path) -> list[Path]:
    if root.is_file():
        return [root]
    if not root.exists():
        return []
    direct = sorted(path for path in root.glob("*.csv") if _looks_like_divergence_csv(path))
    if direct:
        return direct
    return sorted(path for path in root.rglob("*.csv") if _looks_like_divergence_csv(path))


def _looks_like_divergence_csv(path: Path) -> bool:
    name = path.name.lower()
    return "divergence" in name and name.endswith(".csv")


def _symbol_from_path(path: Path) -> str:
    stem = path.stem
    if stem.endswith("_provider_divergence"):
        return stem.removesuffix("_provider_divergence").upper()
    if stem.startswith("divergence_"):
        parts = stem.split("_")
        if len(parts) >= 2:
            return parts[1].upper()
    return stem.split("_", 1)[0].upper()


def _session_allowed(row: dict[str, str], wanted: str) -> bool:
    normalized = wanted.strip().lower()
    if normalized in {"", "all"}:
        return True
    row_session = str(row.get("session") or "").strip().lower()
    if row_session:
        return row_session == normalized
    # Older divergence artifacts did not include a session column. Keep them
    # usable rather than silently producing an empty report.
    return True


def _trade_date(row: dict[str, str], timestamp: str) -> str:
    if row.get("date_ct"):
        return str(row["date_ct"])
    try:
        return datetime.fromisoformat(timestamp.replace("Z", "+00:00")).date().isoformat()
    except ValueError:
        return timestamp[:10]


def _volume_window_rows(
    rows_by_symbol: dict[str, list[ProviderVolumeRow]],
    windows: tuple[int, ...],
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for symbol, rows in rows_by_symbol.items():
        for window in windows:
            ratios: list[float] = []
            pct_diffs: list[float] = []
            for schwab_sum, polygon_sum in _rolling_volume_pairs(rows, window):
                if polygon_sum > 0:
                    ratios.append(schwab_sum / polygon_sum)
                pct_diffs.append(_pct_diff(schwab_sum, polygon_sum))
            output.append(
                {
                    "symbol": symbol,
                    "window_minutes": window,
                    "bars": len(pct_diffs),
                    "median_schwab_over_polygon": _round(_median(ratios)),
                    "median_pct_diff": _round(_median(pct_diffs)),
                    "p90_pct_diff": _round(_percentile(pct_diffs, 0.90)),
                    "p95_pct_diff": _round(_percentile(pct_diffs, 0.95)),
                    "over_10pct_rate": _round(_rate(value > 0.10 for value in pct_diffs)),
                    "over_20pct_rate": _round(_rate(value > 0.20 for value in pct_diffs)),
                }
            )
    return output


def _relative_volume_rows(
    rows_by_symbol: dict[str, list[ProviderVolumeRow]],
    *,
    aggregate_minutes: tuple[int, ...],
    relative_volume_window: int,
    gate_thresholds: tuple[float, ...],
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for symbol, rows in rows_by_symbol.items():
        for aggregate_minute in aggregate_minutes:
            bars = _aggregate_rows(rows, aggregate_minute)
            schwab_volumes = [bar[1] for bar in bars]
            polygon_volumes = [bar[2] for bar in bars]
            aggregate_pct_diffs = [
                _pct_diff(schwab, polygon)
                for schwab, polygon in zip(schwab_volumes, polygon_volumes, strict=True)
            ]
            rel_pct_diffs: list[float] = []
            flip_counts = {threshold: 0 for threshold in gate_thresholds}
            comparisons = 0
            for idx in range(relative_volume_window - 1, len(bars)):
                schwab_ma = sum(schwab_volumes[idx - relative_volume_window + 1 : idx + 1]) / relative_volume_window
                polygon_ma = sum(polygon_volumes[idx - relative_volume_window + 1 : idx + 1]) / relative_volume_window
                if schwab_ma <= 0 or polygon_ma <= 0:
                    continue
                schwab_rel = schwab_volumes[idx] / schwab_ma
                polygon_rel = polygon_volumes[idx] / polygon_ma
                rel_pct_diffs.append(_pct_diff(schwab_rel, polygon_rel))
                comparisons += 1
                for threshold in gate_thresholds:
                    if (schwab_rel >= threshold) != (polygon_rel >= threshold):
                        flip_counts[threshold] += 1
            row: dict[str, Any] = {
                "symbol": symbol,
                "aggregate_minutes": aggregate_minute,
                "relative_volume_window": relative_volume_window,
                "bars": len(bars),
                "comparisons": comparisons,
                "aggregate_volume_median_pct_diff": _round(_median(aggregate_pct_diffs)),
                "aggregate_volume_p90_pct_diff": _round(_percentile(aggregate_pct_diffs, 0.90)),
                "relative_volume_median_pct_diff": _round(_median(rel_pct_diffs)),
                "relative_volume_p90_pct_diff": _round(_percentile(rel_pct_diffs, 0.90)),
            }
            for threshold in gate_thresholds:
                row[f"gate_flip_rate_ge_{_threshold_label(threshold)}"] = _round(
                    flip_counts[threshold] / comparisons if comparisons else None
                )
            output.append(row)
    return output


def _feature_rows(rows_by_symbol: dict[str, list[ProviderVolumeRow]]) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for symbol, rows in rows_by_symbol.items():
        columns = [
            column
            for column in DEFAULT_FEATURE_COLUMNS
            if any(row.raw.get(column) not in (None, "") for row in rows)
        ]
        for column in columns:
            values = [_float_or_none(row.raw.get(column)) for row in rows]
            values = [value for value in values if value is not None]
            output.append(
                {
                    "symbol": symbol,
                    "feature_pct_column": column,
                    "bars": len(values),
                    "median_pct_diff": _round(_median(values)),
                    "p90_pct_diff": _round(_percentile(values, 0.90)),
                    "p95_pct_diff": _round(_percentile(values, 0.95)),
                    "over_10pct_rate": _round(_rate(value > 0.10 for value in values)),
                }
            )
    return output


def _rolling_volume_pairs(rows: list[ProviderVolumeRow], window: int) -> list[tuple[float, float]]:
    pairs: list[tuple[float, float]] = []
    for day_rows in _rows_by_day(rows).values():
        for idx in range(window - 1, len(day_rows)):
            chunk = day_rows[idx - window + 1 : idx + 1]
            pairs.append(
                (
                    sum(row.volume_schwab for row in chunk),
                    sum(row.volume_polygon for row in chunk),
                )
            )
    return pairs


def _aggregate_rows(rows: list[ProviderVolumeRow], minutes: int) -> list[tuple[str, float, float]]:
    aggregated: list[tuple[str, float, float]] = []
    for day_rows in _rows_by_day(rows).values():
        for start in range(0, len(day_rows) - minutes + 1, minutes):
            chunk = day_rows[start : start + minutes]
            aggregated.append(
                (
                    chunk[-1].timestamp,
                    sum(row.volume_schwab for row in chunk),
                    sum(row.volume_polygon for row in chunk),
                )
            )
    return aggregated


def _rows_by_day(rows: list[ProviderVolumeRow]) -> dict[str, list[ProviderVolumeRow]]:
    grouped: dict[str, list[ProviderVolumeRow]] = defaultdict(list)
    for row in rows:
        grouped[row.trade_date].append(row)
    return {
        day: sorted(day_rows, key=lambda row: row.timestamp)
        for day, day_rows in sorted(grouped.items())
    }


def _render_report(
    *,
    divergence_dir: Path,
    session: str,
    rows_by_symbol: dict[str, list[ProviderVolumeRow]],
    volume_rows: list[dict[str, Any]],
    relative_rows: list[dict[str, Any]],
    feature_rows: list[dict[str, Any]],
    relative_volume_window: int,
    volume_window_csv: Path,
    relative_volume_csv: Path,
    feature_csv: Path,
) -> str:
    one_minute = [row for row in volume_rows if row["window_minutes"] == 1]
    aggregate_3 = [row for row in relative_rows if row["aggregate_minutes"] == 3]
    aggregate_5 = [row for row in relative_rows if row["aggregate_minutes"] == 5]
    raw_median = _mean(row.get("median_pct_diff") for row in one_minute)
    rel3_median = _mean(row.get("relative_volume_median_pct_diff") for row in aggregate_3)
    rel5_median = _mean(row.get("relative_volume_median_pct_diff") for row in aggregate_5)
    flip3 = _mean(row.get("gate_flip_rate_ge_1_2") for row in aggregate_3)
    flip5 = _mean(row.get("gate_flip_rate_ge_1_2") for row in aggregate_5)
    feature_watch = sorted(
        feature_rows,
        key=lambda row: float(row.get("p90_pct_diff") or 0.0),
        reverse=True,
    )[:8]
    lines = [
        "# Provider Volume Parity",
        "",
        f"- divergence_dir: `{divergence_dir}`",
        f"- session_filter: `{session}`",
        f"- symbols: `{len(rows_by_symbol)}`",
        f"- rows: `{sum(len(rows) for rows in rows_by_symbol.values())}`",
        f"- relative_volume_window: `{relative_volume_window}`",
        f"- volume_window_csv: `{volume_window_csv}`",
        f"- relative_volume_csv: `{relative_volume_csv}`",
        f"- feature_csv: `{feature_csv}`",
        "",
        "## Operator Read",
        "",
        f"- Median absolute 1-minute volume disagreement across symbols: `{_round(raw_median)}`.",
        f"- Median 3-minute aggregated relative-volume disagreement: `{_round(rel3_median)}`.",
        f"- Median 5-minute aggregated relative-volume disagreement: `{_round(rel5_median)}`.",
        f"- Average 1.2x volume-gate flip rate on 3-minute aggregates: `{_round(flip3)}`.",
        f"- Average 1.2x volume-gate flip rate on 5-minute aggregates: `{_round(flip5)}`.",
        "- Treat absolute-volume features as provider-sensitive. Treat normalized volume gates as usable only after checking symbol-level flip rates.",
        "",
        "## Relative Volume By Symbol",
        "",
        "| Symbol | Agg Min | RelVol Median Diff | RelVol P90 Diff | Flip >= 1.2 |",
        "|---|---:|---:|---:|---:|",
    ]
    for row in relative_rows:
        if int(row["aggregate_minutes"]) not in {1, 3, 5}:
            continue
        lines.append(
            "| {symbol} | {aggregate_minutes} | {relative_volume_median_pct_diff} | "
            "{relative_volume_p90_pct_diff} | {gate_flip_rate_ge_1_2} |".format(**row)
        )
    lines.extend(
        [
            "",
            "## Feature Watchlist",
            "",
            "| Symbol | Feature | Median Diff | P90 Diff | Over 10pct |",
            "|---|---|---:|---:|---:|",
        ]
    )
    for row in feature_watch:
        lines.append(
            "| {symbol} | {feature_pct_column} | {median_pct_diff} | {p90_pct_diff} | {over_10pct_rate} |".format(
                **row
            )
        )
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "- If raw volume disagreement remains large after rolling sums, the providers are using different share-volume definitions or adjustments, not just minute allocation noise.",
            "- If relative-volume disagreement is small but gate flips are high, the threshold is too close to provider noise and should be widened, disabled, or made provider-invariant before shadowing.",
            "- VPOC percentage drift can be small even when absolute volume differs, because VPOC is sensitive to relative volume by price bucket rather than total volume scale.",
        ]
    )
    return "\n".join(lines) + "\n"


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames: list[str] = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    with path.open("w", encoding="utf-8", newline="") as handle:
        if not fieldnames:
            return
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _pct_diff(a: float, b: float) -> float:
    if a == 0 and b == 0:
        return 0.0
    return abs(a - b) / max(abs(a), abs(b), 1e-12)


def _float_or_none(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _median(values: Iterable[float]) -> float | None:
    return _percentile(values, 0.50)


def _percentile(values: Iterable[float], q: float) -> float | None:
    finite = sorted(value for value in values if math.isfinite(value))
    if not finite:
        return None
    index = min(len(finite) - 1, max(0, math.ceil(q * len(finite)) - 1))
    return finite[index]


def _mean(values: Iterable[Any]) -> float | None:
    finite = [_float_or_none(value) for value in values]
    finite = [value for value in finite if value is not None]
    return sum(finite) / len(finite) if finite else None


def _rate(values: Iterable[bool]) -> float | None:
    materialized = list(values)
    if not materialized:
        return None
    return sum(1 for value in materialized if value) / len(materialized)


def _round(value: Any) -> str:
    number = _float_or_none(value)
    if number is None:
        return ""
    return str(round(number, 6))


def _threshold_label(threshold: float) -> str:
    return str(threshold).replace(".", "_")


__all__ = [
    "ProviderVolumeParityArtifacts",
    "build_provider_volume_parity_report",
]
