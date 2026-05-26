"""Exact-row M7 provider replay over a provider panel."""

from __future__ import annotations

import csv
import inspect
import math
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable

import polars as pl
import yaml

from src.newton.engine import PhysicsEngine
from src.research.strategy_keys import to_strategy_key
from src.strategy.factory import build_strategy


M7_PROVIDER_REPLAY_CSV = "M7_provider_replay.csv"
M7_PROVIDER_REPLAY_BY_PAIR_CSV = "M7_provider_replay_by_pair.csv"


@dataclass(slots=True, frozen=True)
class M7ProviderReplayArtifacts:
    run_dirs: list[Path]
    replay_csvs: list[Path]
    replay_by_pair_csvs: list[Path]


def build_m7_provider_replay(
    *,
    run_dirs: Iterable[str | Path],
    provider_panel_csv: str | Path,
    baseline_provider: str = "polygon",
    runtime_provider: str = "",
    runtime_provider_source: str = "",
) -> M7ProviderReplayArtifacts:
    """Replay exact selected rows across providers and write signal-overlap CSVs."""

    panel_rows = _read_csv(Path(provider_panel_csv))
    panel_by_symbol_provider = _panel_by_symbol_provider(panel_rows)
    runtime_provider = runtime_provider.strip().lower()
    runtime_provider_source = runtime_provider_source or ("explicit" if runtime_provider else "not_configured")
    replay_paths: list[Path] = []
    replay_pair_paths: list[Path] = []
    processed: list[Path] = []

    for raw_run_dir in run_dirs:
        run_dir = Path(raw_run_dir)
        selected_rows = _read_csv(run_dir / "CATALOG_SELECTED.csv")
        m5_rows = _read_csv(run_dir / "M5_execution.csv")
        if not selected_rows or not m5_rows:
            continue
        pair_rows: list[dict[str, Any]] = []
        summary_rows: list[dict[str, Any]] = []
        for selected in selected_rows:
            catalog_key = str(selected.get("catalog_key") or "").strip()
            if not catalog_key:
                continue
            m5_row = _matching_m5_row(selected, m5_rows) or {}
            strategy_name = str(m5_row.get("strategy") or selected.get("strategy") or "")
            symbol = str(m5_row.get("ticker") or selected.get("ticker") or "").upper()
            direction = str(m5_row.get("direction") or selected.get("direction") or "").lower()
            params = _strategy_params_from_rows(selected=selected, m5_row=m5_row)
            row_pairs = _replay_selected_row(
                catalog_key=catalog_key,
                strategy_name=strategy_name,
                symbol=symbol,
                direction=direction,
                params=params,
                panel_by_symbol_provider=panel_by_symbol_provider,
                baseline_provider=baseline_provider.strip().lower(),
            )
            pair_rows.extend(row_pairs)
            if row_pairs:
                summary_rows.append(
                    _gate_pair_summary(
                        row_pairs,
                        runtime_provider=runtime_provider,
                        runtime_provider_source=runtime_provider_source,
                    )
                )
            else:
                summary_rows.append(
                    {
                        "catalog_key": catalog_key,
                        "symbol": symbol,
                        "direction": direction,
                        "strategy": strategy_name,
                        "strategy_key": to_strategy_key(strategy_name),
                        "scenario": "provider_like",
                        "error": "missing_provider_panel_or_replay_error",
                    }
                )

        replay_csv = run_dir / M7_PROVIDER_REPLAY_CSV
        replay_by_pair_csv = run_dir / M7_PROVIDER_REPLAY_BY_PAIR_CSV
        _write_csv(replay_csv, summary_rows)
        _write_csv(replay_by_pair_csv, pair_rows)
        processed.append(run_dir)
        replay_paths.append(replay_csv)
        replay_pair_paths.append(replay_by_pair_csv)

    return M7ProviderReplayArtifacts(
        run_dirs=processed,
        replay_csvs=replay_paths,
        replay_by_pair_csvs=replay_pair_paths,
    )


def runtime_provider_from_bhiksha_config(path: str | Path) -> tuple[str, str]:
    """Return Bhiksha's current signal provider from providers.yaml."""

    config_path = Path(path)
    if not config_path.exists():
        return "", "missing_bhiksha_provider_config"
    data = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    provider = str(data.get("underlying_live_primary") or "").strip().lower()
    return provider, f"bhiksha_providers_config:{config_path}"


def _replay_selected_row(
    *,
    catalog_key: str,
    strategy_name: str,
    symbol: str,
    direction: str,
    params: dict[str, Any],
    panel_by_symbol_provider: dict[tuple[str, str], list[dict[str, str]]],
    baseline_provider: str,
) -> list[dict[str, Any]]:
    providers = sorted(
        provider
        for row_symbol, provider in panel_by_symbol_provider
        if row_symbol == symbol
    )
    if baseline_provider not in providers or len(providers) < 2:
        return []

    strategy = _build_strategy_for_replay(strategy_name, params)
    required_features = set(strategy.required_features) | set(strategy.feature_requests)
    baseline_rows = panel_by_symbol_provider[(symbol, baseline_provider)]
    baseline_signals = _signal_events_for_provider(
        rows=baseline_rows,
        strategy=strategy,
        required_features=required_features,
        direction=direction,
    )

    output: list[dict[str, Any]] = []
    for candidate_provider in providers:
        if candidate_provider == baseline_provider:
            continue
        candidate_strategy = _build_strategy_for_replay(strategy_name, params)
        candidate_signals = _signal_events_for_provider(
            rows=panel_by_symbol_provider[(symbol, candidate_provider)],
            strategy=candidate_strategy,
            required_features=required_features,
            direction=direction,
        )
        matched = sorted(baseline_signals & candidate_signals)
        missing = sorted(baseline_signals - candidate_signals)
        extra = sorted(candidate_signals - baseline_signals)
        baseline_count = len(baseline_signals)
        candidate_count = len(candidate_signals)
        overlap = _overlap_rate(matched=len(matched), baseline_count=baseline_count, candidate_count=candidate_count)
        signal_evidence_status = "present" if baseline_count else "no_baseline_signals"
        output.append(
            {
                "catalog_key": catalog_key,
                "symbol": symbol,
                "direction": direction,
                "strategy": strategy_name,
                "strategy_key": to_strategy_key(strategy_name),
                "scenario": "provider_like",
                "provider_pair": f"{baseline_provider}_vs_{candidate_provider}",
                "baseline_provider": baseline_provider,
                "candidate_provider": candidate_provider,
                "baseline_signal_count": baseline_count,
                "candidate_signal_count": candidate_count,
                "matched_signal_count": len(matched),
                "missing_signal_count": len(missing),
                "extra_signal_count": len(extra),
                "signal_evidence_status": signal_evidence_status,
                "entry_overlap_rate_vs_baseline": _round(overlap),
                "trade_overlap_rate_vs_baseline": _round(overlap),
                "trade_count_ratio_vs_baseline": _round(
                    candidate_count / baseline_count if baseline_count else (1.0 if candidate_count == 0 else None)
                ),
                "first_missing_signal": missing[0] if missing else "",
                "first_extra_signal": extra[0] if extra else "",
            }
        )
    return output


def _signal_events_for_provider(
    *,
    rows: list[dict[str, str]],
    strategy: Any,
    required_features: set[str],
    direction: str,
) -> set[str]:
    frame = _provider_rows_to_frame(rows)
    if required_features:
        frame = PhysicsEngine().enrich_for_features(frame, required_features)
    signals = strategy.generate_signals(frame)
    filtered = signals.filter(pl.col("signal").fill_null(False))
    if "signal_direction" in filtered.columns and direction in {"long", "short"}:
        filtered = filtered.filter(pl.col("signal_direction") == direction)
    elif direction in {"long", "short"}:
        filtered = filtered.with_columns(pl.lit(direction).alias("signal_direction"))
    else:
        filtered = filtered.with_columns(pl.lit("").alias("signal_direction"))
    return {
        f"{_timestamp_key(row['timestamp'])}|{row.get('signal_direction') or ''}"
        for row in filtered.select(["timestamp", "signal_direction"]).to_dicts()
    }


def _provider_rows_to_frame(rows: list[dict[str, str]]) -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "timestamp": _parse_datetime(row["timestamp"]),
                "open": _float_required(row.get("open")),
                "high": _float_required(row.get("high")),
                "low": _float_required(row.get("low")),
                "close": _float_required(row.get("close")),
                "volume": _float_required(row.get("volume")),
            }
            for row in sorted(rows, key=lambda item: item["timestamp"])
        ]
    ).sort("timestamp")


def _build_strategy_for_replay(strategy_name: str, params: dict[str, Any]) -> Any:
    default = build_strategy(strategy_name)
    allowed = set(inspect.signature(type(default).__init__).parameters) - {"self"}
    filtered = {key: value for key, value in params.items() if key in allowed}
    return build_strategy(strategy_name, filtered)


def _gate_pair_summary(
    rows: list[dict[str, Any]],
    *,
    runtime_provider: str,
    runtime_provider_source: str,
) -> dict[str, Any]:
    worst = _worst_pair(rows)
    selected = None
    reason = "diagnostic_worst_pair_fallback"
    if runtime_provider:
        selected = next(
            (
                row for row in rows
                if str(row.get("candidate_provider") or "").lower() == runtime_provider
            ),
            None,
        )
        reason = "runtime_provider_pair" if selected else "runtime_provider_pair_missing"
    if selected is None:
        selected = worst
    return dict(selected) | {
        "gate_provider_pair": selected.get("provider_pair", ""),
        "gate_pair_selection_reason": reason,
        "runtime_provider": runtime_provider,
        "runtime_provider_source": runtime_provider_source,
        "diagnostic_worst_pair": worst.get("provider_pair", ""),
        "diagnostic_worst_overlap": worst.get("entry_overlap_rate_vs_baseline", ""),
        "diagnostic_worst_missing_signal_count": worst.get("missing_signal_count", ""),
        "diagnostic_worst_extra_signal_count": worst.get("extra_signal_count", ""),
    }


def _worst_pair(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return min(
        rows,
        key=lambda row: (
            _float_or_none(row.get("entry_overlap_rate_vs_baseline")) or -1.0,
            -int(row.get("missing_signal_count") or 0),
            -int(row.get("extra_signal_count") or 0),
        ),
    )


def _overlap_rate(*, matched: int, baseline_count: int, candidate_count: int) -> float | None:
    if baseline_count == 0:
        return None
    return matched / baseline_count


def _panel_by_symbol_provider(rows: list[dict[str, str]]) -> dict[tuple[str, str], list[dict[str, str]]]:
    output: dict[tuple[str, str], list[dict[str, str]]] = {}
    for row in rows:
        symbol = str(row.get("symbol") or "").upper()
        provider = str(row.get("provider") or "").lower()
        if symbol and provider:
            output.setdefault((symbol, provider), []).append(row)
    return output


def _matching_m5_row(selected: dict[str, str], m5_rows: list[dict[str, str]]) -> dict[str, str] | None:
    for row in m5_rows:
        if _selected_matches_m5(selected, row):
            return row
    return None


def _selected_matches_m5(selected: dict[str, str], row: dict[str, str]) -> bool:
    for key in ("ticker", "direction", "strategy"):
        selected_value = str(selected.get(key) or "").strip()
        if selected_value and str(row.get(key) or "").strip() != selected_value:
            return False
    return True


def _strategy_params_from_rows(*, selected: dict[str, str], m5_row: dict[str, str]) -> dict[str, Any]:
    excluded = {
        "catalog_key",
        "ticker",
        "strategy",
        "direction",
        "execution_profile",
        "stress_profile",
        "recommendation_tier",
        "recommendation_tier_reason",
        "recommendation_checks_json",
        "exit_reliability",
        "exit_trade_count",
        "selected_exit_policy",
        "mc_prob_positive_exp",
        "mc_exp_r_p50",
        "base_exp_r",
        "holdout_trades",
        "holdout_win_rate",
    }
    params: dict[str, Any] = {}
    for source in (m5_row, selected):
        for key, value in source.items():
            if key in excluded or value in ("", None):
                continue
            params[key] = _parse_scalar(value)
    return params


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


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


def _parse_scalar(value: Any) -> Any:
    if value is None:
        return None
    text = str(value).strip()
    if text == "":
        return ""
    lowered = text.lower()
    if lowered in {"true", "false"}:
        return lowered == "true"
    if "," in text:
        parts = [part.strip() for part in text.split(",") if part.strip()]
        if parts and all(re.fullmatch(r"-?\d+", part) for part in parts):
            return [int(part) for part in parts]
        return text
    if re.fullmatch(r"-?\d+", text):
        return int(text)
    if re.fullmatch(r"-?\d+\.\d+", text):
        return float(text)
    return text


def _timestamp_key(value: Any) -> str:
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return _parse_datetime(str(value)).isoformat()


def _parse_datetime(value: str) -> datetime:
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed


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


def _round(value: Any) -> str:
    parsed = _float_or_none(value)
    if parsed is None:
        return ""
    return str(round(parsed, 6))


__all__ = [
    "M7_PROVIDER_REPLAY_BY_PAIR_CSV",
    "M7_PROVIDER_REPLAY_CSV",
    "M7ProviderReplayArtifacts",
    "build_m7_provider_replay",
    "runtime_provider_from_bhiksha_config",
]
