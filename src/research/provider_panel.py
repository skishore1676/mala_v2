"""Mala-owned provider parity panel for M7 provider translation.

The panel is intentionally source-agnostic: provider fetchers can write CSVs
from Polygon, Schwab, Public, or another reader, and Mala owns the normalization,
Newton feature enrichment, alignment, and parity summaries.
"""

from __future__ import annotations

import csv
import math
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, time
from pathlib import Path
from typing import Any, Iterable
from zoneinfo import ZoneInfo

import polars as pl

from src.newton.engine import PhysicsEngine


DEFAULT_PANEL_FEATURE_COLUMNS = (
    "close",
    "volume_ma_20",
    "directional_mass",
    "directional_mass_ma_20",
    "vpoc_4h",
)
DEFAULT_PANEL_GATE_THRESHOLDS = (1.0, 1.2, 1.4)
DEFAULT_PANEL_AGGREGATE_MINUTES = (1, 3, 5)
CENTRAL = ZoneInfo("America/Chicago")
EASTERN = ZoneInfo("America/New_York")


@dataclass(slots=True, frozen=True)
class ProviderPanelArtifacts:
    panel_csv: Path
    bar_parity_csv: Path
    relative_volume_csv: Path
    feature_parity_csv: Path
    report_md: Path


@dataclass(slots=True, frozen=True)
class ProviderBar:
    provider: str
    symbol: str
    timestamp: str
    trade_date: str
    session: str
    open: float
    high: float
    low: float
    close: float
    volume: float


def parse_provider_bar_spec(spec: str) -> tuple[str, Path]:
    """Parse `provider=/path/to/bars.csv` CLI specs."""

    if "=" not in spec:
        raise ValueError(f"Provider bar spec must be provider=path, got {spec!r}")
    provider, raw_path = spec.split("=", 1)
    provider = provider.strip().lower()
    if not provider:
        raise ValueError(f"Provider bar spec has empty provider: {spec!r}")
    return provider, Path(raw_path).expanduser()


def build_provider_panel_report(
    *,
    provider_bar_csvs: dict[str, str | Path],
    out_dir: str | Path,
    baseline_provider: str = "polygon",
    session: str = "regular",
    relative_volume_window: int = 20,
    aggregate_minutes: Iterable[int] = DEFAULT_PANEL_AGGREGATE_MINUTES,
    gate_thresholds: Iterable[float] = DEFAULT_PANEL_GATE_THRESHOLDS,
    feature_columns: Iterable[str] = DEFAULT_PANEL_FEATURE_COLUMNS,
) -> ProviderPanelArtifacts:
    """Build provider parity artifacts from normalized OHLCV CSV inputs."""

    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    baseline = baseline_provider.strip().lower()
    bars = _load_provider_bars(provider_bar_csvs, session=session)
    if not bars:
        raise ValueError("No provider bars were loaded for the requested session")
    enriched_rows = _enriched_panel_rows(bars)
    provider_symbols = _provider_symbols(enriched_rows)
    if len(provider_symbols) < 2:
        raise ValueError("M7 provider panel requires at least two providers")

    provider_pairs = [
        (baseline, provider)
        for provider in sorted(provider_symbols)
        if provider != baseline
    ]
    if baseline not in provider_symbols and len(provider_symbols) >= 2:
        providers = sorted(provider_symbols)
        provider_pairs = [(providers[0], provider) for provider in providers[1:]]

    aggregate_minutes = tuple(sorted({max(1, int(value)) for value in aggregate_minutes}))
    gate_thresholds = tuple(sorted(float(value) for value in gate_thresholds))
    feature_columns = tuple(feature_columns)
    relative_volume_window = max(2, int(relative_volume_window))

    aligned = _aligned_provider_pairs(enriched_rows, provider_pairs)
    bar_rows = _bar_parity_rows(enriched_rows, aligned, provider_pairs)
    relative_rows = _relative_volume_rows(
        aligned,
        aggregate_minutes=aggregate_minutes,
        relative_volume_window=relative_volume_window,
        gate_thresholds=gate_thresholds,
    )
    feature_rows = _feature_parity_rows(aligned, feature_columns=feature_columns)

    panel_csv = out / "provider_bar_panel.csv"
    bar_parity_csv = out / "provider_pair_bar_parity.csv"
    relative_volume_csv = out / "provider_relative_volume_parity.csv"
    feature_parity_csv = out / "provider_feature_parity.csv"
    report_md = out / "M7_PROVIDER_PANEL_REPORT.md"

    _write_csv(panel_csv, enriched_rows)
    _write_csv(bar_parity_csv, bar_rows)
    _write_csv(relative_volume_csv, relative_rows)
    _write_csv(feature_parity_csv, feature_rows)
    report_md.write_text(
        _render_report(
            provider_bar_csvs=provider_bar_csvs,
            baseline_provider=baseline,
            session=session,
            enriched_rows=enriched_rows,
            bar_rows=bar_rows,
            relative_rows=relative_rows,
            feature_rows=feature_rows,
            panel_csv=panel_csv,
            bar_parity_csv=bar_parity_csv,
            relative_volume_csv=relative_volume_csv,
            feature_parity_csv=feature_parity_csv,
        ),
        encoding="utf-8",
    )

    return ProviderPanelArtifacts(
        panel_csv=panel_csv,
        bar_parity_csv=bar_parity_csv,
        relative_volume_csv=relative_volume_csv,
        feature_parity_csv=feature_parity_csv,
        report_md=report_md,
    )


def _load_provider_bars(provider_bar_csvs: dict[str, str | Path], *, session: str) -> list[ProviderBar]:
    rows: list[ProviderBar] = []
    for raw_provider, raw_path in provider_bar_csvs.items():
        provider = raw_provider.strip().lower()
        path = Path(raw_path)
        if not path.exists():
            raise FileNotFoundError(path)
        with path.open("r", encoding="utf-8", newline="") as handle:
            for raw in csv.DictReader(handle):
                row_provider = str(raw.get("provider") or provider).strip().lower()
                symbol = str(raw.get("symbol") or raw.get("ticker") or "").strip().upper()
                timestamp = _normalize_timestamp(raw.get("timestamp") or raw.get("datetime") or raw.get("time"))
                if not symbol or not timestamp:
                    continue
                session_name = str(raw.get("session") or _session_for_timestamp(timestamp)).strip().lower()
                if not _session_allowed(session_name, session):
                    continue
                try:
                    bar = ProviderBar(
                        provider=row_provider,
                        symbol=symbol,
                        timestamp=timestamp,
                        trade_date=str(raw.get("trade_date") or _trade_date(timestamp)),
                        session=session_name,
                        open=_float_required(raw.get("open") or raw.get("o")),
                        high=_float_required(raw.get("high") or raw.get("h")),
                        low=_float_required(raw.get("low") or raw.get("l")),
                        close=_float_required(raw.get("close") or raw.get("c")),
                        volume=_float_required(raw.get("volume") or raw.get("v")),
                    )
                except ValueError:
                    continue
                rows.append(bar)
    return sorted(rows, key=lambda row: (row.provider, row.symbol, row.timestamp))


def _enriched_panel_rows(bars: list[ProviderBar]) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    engine = PhysicsEngine()
    grouped: dict[tuple[str, str], list[ProviderBar]] = defaultdict(list)
    for bar in bars:
        grouped[(bar.provider, bar.symbol)].append(bar)

    for (provider, symbol), group in sorted(grouped.items()):
        frame = pl.DataFrame(
            [
                {
                    "timestamp": _parse_datetime(bar.timestamp),
                    "open": bar.open,
                    "high": bar.high,
                    "low": bar.low,
                    "close": bar.close,
                    "volume": bar.volume,
                }
                for bar in group
            ]
        ).sort("timestamp")
        enriched = engine.enrich(frame)
        enriched_dicts = enriched.to_dicts()
        for bar, enriched_row in zip(group, enriched_dicts, strict=True):
            row = {
                "provider": provider,
                "symbol": symbol,
                "timestamp": bar.timestamp,
                "trade_date": bar.trade_date,
                "session": bar.session,
                "open": _round(bar.open),
                "high": _round(bar.high),
                "low": _round(bar.low),
                "close": _round(bar.close),
                "volume": _round(bar.volume),
            }
            for key, value in enriched_row.items():
                if key in row or key == "timestamp":
                    continue
                row[key] = _round(value)
            output.append(row)
    return sorted(output, key=lambda row: (row["provider"], row["symbol"], row["timestamp"]))


def _provider_symbols(rows: list[dict[str, Any]]) -> dict[str, set[str]]:
    output: dict[str, set[str]] = defaultdict(set)
    for row in rows:
        output[str(row["provider"])].add(str(row["symbol"]))
    return output


def _aligned_provider_pairs(
    rows: list[dict[str, Any]],
    provider_pairs: list[tuple[str, str]],
) -> dict[tuple[str, str, str], list[tuple[dict[str, Any], dict[str, Any]]]]:
    by_key = {
        (str(row["provider"]), str(row["symbol"]), str(row["timestamp"])): row
        for row in rows
    }
    symbols = sorted({str(row["symbol"]) for row in rows})
    timestamps_by_symbol: dict[str, set[str]] = defaultdict(set)
    for row in rows:
        timestamps_by_symbol[str(row["symbol"])].add(str(row["timestamp"]))

    output: dict[tuple[str, str, str], list[tuple[dict[str, Any], dict[str, Any]]]] = {}
    for baseline, candidate in provider_pairs:
        for symbol in symbols:
            pairs: list[tuple[dict[str, Any], dict[str, Any]]] = []
            for timestamp in sorted(timestamps_by_symbol[symbol]):
                left = by_key.get((baseline, symbol, timestamp))
                right = by_key.get((candidate, symbol, timestamp))
                if left and right:
                    pairs.append((left, right))
            output[(symbol, baseline, candidate)] = pairs
    return output


def _bar_parity_rows(
    panel_rows: list[dict[str, Any]],
    aligned: dict[tuple[str, str, str], list[tuple[dict[str, Any], dict[str, Any]]]],
    provider_pairs: list[tuple[str, str]],
) -> list[dict[str, Any]]:
    counts: dict[tuple[str, str], int] = defaultdict(int)
    for row in panel_rows:
        counts[(str(row["provider"]), str(row["symbol"]))] += 1

    output: list[dict[str, Any]] = []
    symbols = sorted({str(row["symbol"]) for row in panel_rows})
    for baseline, candidate in provider_pairs:
        for symbol in symbols:
            pairs = aligned.get((symbol, baseline, candidate), [])
            price_diffs: list[float] = []
            volume_diffs: list[float] = []
            for left, right in pairs:
                price_diffs.extend(
                    _pct_diff(_to_float(left.get(column)), _to_float(right.get(column)))
                    for column in ("open", "high", "low", "close")
                )
                volume_diffs.append(_pct_diff(_to_float(left.get("volume")), _to_float(right.get("volume"))))
            output.append(
                {
                    "symbol": symbol,
                    "provider_pair": f"{baseline}_vs_{candidate}",
                    "baseline_provider": baseline,
                    "candidate_provider": candidate,
                    "baseline_bars": counts.get((baseline, symbol), 0),
                    "candidate_bars": counts.get((candidate, symbol), 0),
                    "aligned_bars": len(pairs),
                    "coverage_ratio": _round(
                        len(pairs) / max(counts.get((baseline, symbol), 0), 1)
                    ),
                    "price_median_pct_diff": _round(_median(price_diffs)),
                    "price_p95_pct_diff": _round(_percentile(price_diffs, 0.95)),
                    "price_over_10bp_rate": _round(_rate(value > 0.001 for value in price_diffs)),
                    "volume_median_pct_diff": _round(_median(volume_diffs)),
                    "volume_p95_pct_diff": _round(_percentile(volume_diffs, 0.95)),
                    "volume_over_10pct_rate": _round(_rate(value > 0.10 for value in volume_diffs)),
                }
            )
    return output


def _relative_volume_rows(
    aligned: dict[tuple[str, str, str], list[tuple[dict[str, Any], dict[str, Any]]]],
    *,
    aggregate_minutes: tuple[int, ...],
    relative_volume_window: int,
    gate_thresholds: tuple[float, ...],
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for (symbol, baseline, candidate), pairs in sorted(aligned.items()):
        for aggregate_minute in aggregate_minutes:
            aggregates = _aggregate_pairs(pairs, aggregate_minute)
            baseline_volumes = [item[1] for item in aggregates]
            candidate_volumes = [item[2] for item in aggregates]
            aggregate_pct_diffs = [
                _pct_diff(left, right)
                for left, right in zip(baseline_volumes, candidate_volumes, strict=True)
            ]
            rel_pct_diffs: list[float] = []
            flip_counts = {threshold: 0 for threshold in gate_thresholds}
            comparisons = 0
            for idx in range(relative_volume_window - 1, len(aggregates)):
                left_ma = sum(baseline_volumes[idx - relative_volume_window + 1 : idx + 1]) / relative_volume_window
                right_ma = sum(candidate_volumes[idx - relative_volume_window + 1 : idx + 1]) / relative_volume_window
                if left_ma <= 0 or right_ma <= 0:
                    continue
                left_rel = baseline_volumes[idx] / left_ma
                right_rel = candidate_volumes[idx] / right_ma
                rel_pct_diffs.append(_pct_diff(left_rel, right_rel))
                comparisons += 1
                for threshold in gate_thresholds:
                    if (left_rel >= threshold) != (right_rel >= threshold):
                        flip_counts[threshold] += 1
            row: dict[str, Any] = {
                "symbol": symbol,
                "provider_pair": f"{baseline}_vs_{candidate}",
                "baseline_provider": baseline,
                "candidate_provider": candidate,
                "aggregate_minutes": aggregate_minute,
                "relative_volume_window": relative_volume_window,
                "bars": len(aggregates),
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


def _feature_parity_rows(
    aligned: dict[tuple[str, str, str], list[tuple[dict[str, Any], dict[str, Any]]]],
    *,
    feature_columns: tuple[str, ...],
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for (symbol, baseline, candidate), pairs in sorted(aligned.items()):
        for column in feature_columns:
            values: list[float] = []
            for left, right in pairs:
                left_value = _float_or_none(left.get(column))
                right_value = _float_or_none(right.get(column))
                if left_value is None or right_value is None:
                    continue
                values.append(_pct_diff(left_value, right_value))
            if not values:
                continue
            output.append(
                {
                    "symbol": symbol,
                    "provider_pair": f"{baseline}_vs_{candidate}",
                    "baseline_provider": baseline,
                    "candidate_provider": candidate,
                    "feature_pct_column": f"{column}_pct" if not column.endswith("_pct") else column,
                    "feature_column": column,
                    "bars": len(values),
                    "median_pct_diff": _round(_median(values)),
                    "p90_pct_diff": _round(_percentile(values, 0.90)),
                    "p95_pct_diff": _round(_percentile(values, 0.95)),
                    "over_10pct_rate": _round(_rate(value > 0.10 for value in values)),
                }
            )
    return output


def _aggregate_pairs(
    pairs: list[tuple[dict[str, Any], dict[str, Any]]],
    minutes: int,
) -> list[tuple[str, float, float]]:
    output: list[tuple[str, float, float]] = []
    by_day: dict[str, list[tuple[dict[str, Any], dict[str, Any]]]] = defaultdict(list)
    for pair in pairs:
        by_day[str(pair[0].get("trade_date") or "")].append(pair)
    for day_pairs in by_day.values():
        ordered = sorted(day_pairs, key=lambda item: str(item[0]["timestamp"]))
        for start in range(0, len(ordered) - minutes + 1, minutes):
            chunk = ordered[start : start + minutes]
            output.append(
                (
                    str(chunk[-1][0]["timestamp"]),
                    sum(_to_float(item[0].get("volume")) for item in chunk),
                    sum(_to_float(item[1].get("volume")) for item in chunk),
                )
            )
    return output


def _render_report(
    *,
    provider_bar_csvs: dict[str, str | Path],
    baseline_provider: str,
    session: str,
    enriched_rows: list[dict[str, Any]],
    bar_rows: list[dict[str, Any]],
    relative_rows: list[dict[str, Any]],
    feature_rows: list[dict[str, Any]],
    panel_csv: Path,
    bar_parity_csv: Path,
    relative_volume_csv: Path,
    feature_parity_csv: Path,
) -> str:
    price_p95 = _mean(row.get("price_p95_pct_diff") for row in bar_rows)
    volume_p95 = _mean(row.get("volume_p95_pct_diff") for row in bar_rows)
    flip_1_2 = _mean(row.get("gate_flip_rate_ge_1_2") for row in relative_rows)
    feature_watch = sorted(
        feature_rows,
        key=lambda row: _to_float(row.get("p90_pct_diff")),
        reverse=True,
    )[:10]
    lines = [
        "# M7 Provider Panel",
        "",
        f"- generated_at: `{datetime.now(UTC).isoformat()}`",
        f"- baseline_provider: `{baseline_provider}`",
        f"- session_filter: `{session}`",
        f"- providers: `{', '.join(sorted(provider_bar_csvs))}`",
        f"- rows: `{len(enriched_rows)}`",
        f"- panel_csv: `{panel_csv}`",
        f"- bar_parity_csv: `{bar_parity_csv}`",
        f"- relative_volume_csv: `{relative_volume_csv}`",
        f"- feature_parity_csv: `{feature_parity_csv}`",
        "",
        "## Operator Read",
        "",
        f"- Average p95 OHLC disagreement: `{_round(price_p95)}`.",
        f"- Average p95 volume disagreement: `{_round(volume_p95)}`.",
        f"- Average 1.2x relative-volume gate flip rate: `{_round(flip_1_2)}`.",
        "- Use this panel as M7 input. It does not tune strategies and it does not authorize Bhiksha execution.",
        "",
        "## Bar Coverage",
        "",
        "| Symbol | Pair | Baseline Bars | Candidate Bars | Aligned | Coverage | Price P95 | Volume P95 |",
        "|---|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in bar_rows:
        lines.append(
            "| {symbol} | {provider_pair} | {baseline_bars} | {candidate_bars} | {aligned_bars} | "
            "{coverage_ratio} | {price_p95_pct_diff} | {volume_p95_pct_diff} |".format(**row)
        )
    lines.extend(
        [
            "",
            "## Feature Watchlist",
            "",
            "| Symbol | Pair | Feature | P90 Diff | Over 10pct |",
            "|---|---|---|---:|---:|",
        ]
    )
    for row in feature_watch:
        lines.append(
            "| {symbol} | {provider_pair} | {feature_pct_column} | {p90_pct_diff} | {over_10pct_rate} |".format(
                **row
            )
        )
    lines.append("")
    return "\n".join(lines)


def _session_allowed(row_session: str, wanted: str) -> bool:
    normalized = wanted.strip().lower()
    if normalized in {"", "all"}:
        return True
    if row_session:
        return row_session == normalized
    return True


def _session_for_timestamp(timestamp: str) -> str:
    parsed = _parse_datetime(timestamp).astimezone(EASTERN)
    current = parsed.time()
    if time(9, 30) <= current < time(16, 0):
        return "regular"
    return "extended"


def _trade_date(timestamp: str) -> str:
    return _parse_datetime(timestamp).astimezone(CENTRAL).date().isoformat()


def _normalize_timestamp(value: Any) -> str:
    if value in (None, ""):
        return ""
    parsed = _parse_datetime(str(value))
    return parsed.astimezone(UTC).isoformat()


def _parse_datetime(value: str) -> datetime:
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


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


def _float_required(value: Any) -> float:
    parsed = _float_or_none(value)
    if parsed is None:
        raise ValueError(f"missing numeric value {value!r}")
    return parsed


def _float_or_none(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _to_float(value: Any) -> float:
    return _float_or_none(value) or 0.0


def _pct_diff(a: float, b: float) -> float:
    if a == 0 and b == 0:
        return 0.0
    return abs(a - b) / max(abs(a), abs(b), 1e-12)


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
    parsed = _float_or_none(value)
    if parsed is None:
        return ""
    return str(round(parsed, 6))


def _threshold_label(threshold: float) -> str:
    return str(float(threshold)).rstrip("0").rstrip(".").replace(".", "_")


__all__ = [
    "ProviderPanelArtifacts",
    "build_provider_panel_report",
    "parse_provider_bar_spec",
]
