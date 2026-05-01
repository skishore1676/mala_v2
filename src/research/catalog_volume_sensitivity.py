"""Replay Strategy_Catalog rows under alternate volume assumptions.

This is an artifact-only research tool. It does not publish to
Strategy_Catalog and does not touch live execution state.
"""

from __future__ import annotations

import argparse
import csv
import inspect
import json
import math
import os
import random
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import polars as pl

from src.chronos.storage import LocalStorage
from src.config import DATA_DIR
from src.newton.engine import PhysicsEngine
from src.oracle.trade_simulator import (
    AtrTrailingExitPolicy,
    FixedPercentRewardRiskExitPolicy,
    HoldToEodExitPolicy,
    MovingAverageCrossoverExitPolicy,
    MovingAverageTrailingExitPolicy,
    SimulationResult,
    TimeStopExitPolicy,
    TradeSimulator,
    VmaTrailingExitPolicy,
)
from src.research.exit_optimizer import _with_exit_policy_features
from src.research.google_sheets import GoogleSheetTableClient
from src.strategy.base import BaseStrategy, required_feature_union
from src.strategy.factory import build_strategy


ET = ZoneInfo("America/New_York")

STRATEGY_NAME_BY_KEY = {
    "elastic_band_reversion": "Elastic Band Reversion",
    "jerk_pivot_momentum": "Jerk-Pivot Momentum (tight)",
    "market_impulse": "Market Impulse (Cross & Reclaim)",
    "opening_drive_classifier": "Opening Drive Classifier",
}

DEFAULT_SCENARIOS = (
    "polygon_baseline",
    "volume_filter_off",
    "volume_neutral",
    "volume_shuffled_intraday",
    "volume_provider_like",
)


@dataclass(frozen=True, slots=True)
class CatalogRow:
    catalog_key: str
    symbol: str
    strategy_key: str
    direction: str
    thesis_exit_policy: str
    thesis_exit_params: dict[str, Any]
    playbook_summary: dict[str, Any]
    raw_row: dict[str, Any]


@dataclass(frozen=True, slots=True)
class ScenarioResult:
    row: CatalogRow
    scenario: str
    metrics: dict[str, Any]
    signals: set[tuple[str, str]]
    trades: SimulationResult
    diagnostics: dict[str, Any]
    error: str = ""


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    _load_dotenv(args.env_file)

    out_dir = _resolve_out_dir(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    rows = _load_catalog_rows(args)
    rows = _filter_rows(rows, catalog_keys=args.catalog_key, symbols=args.symbol, limit=args.limit)
    scenarios = tuple(args.scenario or DEFAULT_SCENARIOS)
    provider_model = _load_provider_volume_model(args.provider_divergence_dir)

    storage = LocalStorage(base_dir=Path(args.data_dir) if args.data_dir else DATA_DIR)
    started_at = datetime.now(UTC)
    all_metrics: list[dict[str, Any]] = []
    detail_rows: list[dict[str, Any]] = []
    diagnostics_rows: list[dict[str, Any]] = []

    print(f"OUTPUT_DIR={out_dir}")
    print(f"CATALOG_ROWS={len(rows)} SCENARIOS={','.join(scenarios)}")

    for index, row in enumerate(rows, start=1):
        print(f"ROW {index}/{len(rows)} {row.catalog_key} {row.symbol} {row.strategy_key} {row.direction}")
        raw = storage.load_bars(row.symbol, args.start, args.end)
        if raw.is_empty():
            message = f"no cached bars for {row.symbol}"
            all_metrics.append(_error_metrics(row, "all", message, args.start, args.end))
            print(f"  SKIP {message}")
            continue

        baseline: ScenarioResult | None = None
        for scenario in scenarios:
            artifact_path = out_dir / "per_row" / row.catalog_key / f"{scenario}.json"
            if args.resume and artifact_path.exists():
                cached = json.loads(artifact_path.read_text(encoding="utf-8"))
                all_metrics.append(cached["metrics"])
                diagnostics_rows.append(cached.get("diagnostics", {}))
                continue

            result = _run_row_scenario(
                row=row,
                raw=raw,
                scenario=scenario,
                provider_model=provider_model,
                seed=args.seed,
                start=args.start,
                end=args.end,
                baseline=baseline,
            )
            if scenario == "polygon_baseline":
                baseline = result
            all_metrics.append(result.metrics)
            diagnostics_rows.append({"catalog_key": row.catalog_key, "scenario": scenario, **result.diagnostics})
            detail_rows.extend(_trade_detail_rows(result))
            _write_json(
                artifact_path,
                {
                    "metrics": result.metrics,
                    "diagnostics": result.diagnostics,
                    "error": result.error,
                    "trades": _trades_payload(result.trades),
                },
            )
            print(
                "  "
                f"{scenario}: trades={result.metrics.get('trade_count')} "
                f"signals={result.metrics.get('signal_count')} "
                f"expectancy={result.metrics.get('expectancy')} "
                f"overlap={result.metrics.get('entry_overlap_rate_vs_baseline')}"
                f"{' ERROR=' + result.error if result.error else ''}"
            )

    _write_csv(out_dir / "catalog_volume_sensitivity_by_row.csv", all_metrics)
    _write_csv(out_dir / "catalog_volume_sensitivity_trades.csv", detail_rows)
    _write_csv(out_dir / "volume_feature_flip_report.csv", diagnostics_rows)
    summary_rows = _summarize_metrics(all_metrics)
    _write_csv(out_dir / "catalog_volume_sensitivity_summary.csv", summary_rows)
    _write_report(
        out_dir / "catalog_volume_sensitivity_report.md",
        started_at=started_at,
        args=args,
        metrics=all_metrics,
        summary=summary_rows,
    )
    print(f"SUMMARY_CSV={out_dir / 'catalog_volume_sensitivity_summary.csv'}")
    print(f"ROW_CSV={out_dir / 'catalog_volume_sensitivity_by_row.csv'}")
    print(f"REPORT={out_dir / 'catalog_volume_sensitivity_report.md'}")
    return 0


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog-csv", type=Path, default=None, help="Read Strategy_Catalog rows from a local CSV")
    parser.add_argument("--catalog-sheet-id", default=None, help="Google Strategy_Catalog spreadsheet ID/URL")
    parser.add_argument("--catalog-sheet-name", default=None, help="Strategy_Catalog tab name")
    parser.add_argument("--credentials-path", type=Path, default=None, help="Google service account JSON path")
    parser.add_argument("--env-file", type=Path, default=Path(".env"), help="Optional .env file with catalog credentials")
    parser.add_argument("--data-dir", default=None, help="Local Polygon parquet cache root")
    parser.add_argument("--out-dir", type=Path, default=None, help="Output directory")
    parser.add_argument("--start", type=date.fromisoformat, default=date(2024, 1, 2))
    parser.add_argument("--end", type=date.fromisoformat, default=date(2026, 2, 28))
    parser.add_argument("--scenario", action="append", choices=DEFAULT_SCENARIOS, help="Scenario to run; repeatable")
    parser.add_argument("--catalog-key", action="append", help="Catalog key to include; repeatable")
    parser.add_argument("--symbol", action="append", help="Symbol to include; repeatable")
    parser.add_argument("--limit", type=int, default=None, help="Limit rows for smoke runs")
    parser.add_argument("--seed", type=int, default=1729)
    parser.add_argument("--provider-divergence-dir", type=Path, default=None, help="Directory of Bhiksha provider divergence CSVs")
    parser.add_argument("--resume", action="store_true", help="Reuse completed per-row scenario JSON artifacts")
    return parser.parse_args(argv)


def _load_dotenv(path: Path | None) -> None:
    if path is None or not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def _resolve_out_dir(out_dir: Path | None) -> Path:
    if out_dir is not None:
        return out_dir
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    return Path("data/results/volume_sensitivity") / stamp


def _load_catalog_rows(args: argparse.Namespace) -> list[CatalogRow]:
    if args.catalog_csv is not None:
        raw_rows = _read_csv(args.catalog_csv)
    else:
        sheet_id = args.catalog_sheet_id or os.getenv("STRATEGY_CATALOG_SHEET_ID")
        sheet_name = args.catalog_sheet_name or os.getenv("STRATEGY_CATALOG_SHEET_NAME", "Strategy_Catalog")
        credentials_path = args.credentials_path or Path(os.getenv("GOOGLE_API_CREDENTIALS_PATH", ""))
        if not sheet_id or not str(credentials_path):
            raise SystemExit("--catalog-csv or Strategy_Catalog Google credentials are required")
        client = GoogleSheetTableClient(
            spreadsheet_id=sheet_id,
            sheet_name=sheet_name,
            credentials_path=credentials_path,
        )
        raw_rows = client.read_rows()
    parsed: list[CatalogRow] = []
    for raw in raw_rows:
        row = _parse_catalog_row(raw)
        if row is not None:
            parsed.append(row)
    return parsed


def _parse_catalog_row(raw: dict[str, Any]) -> CatalogRow | None:
    catalog_key = str(raw.get("catalog_key") or "").strip()
    symbol = str(raw.get("symbol") or "").strip().upper()
    strategy_key = str(raw.get("strategy_key") or "").strip().lower()
    direction = str(raw.get("direction") or "").strip().lower()
    thesis_exit_policy = str(raw.get("thesis_exit_policy") or "").strip()
    if not catalog_key or not symbol or strategy_key not in STRATEGY_NAME_BY_KEY:
        return None
    summary = _parse_json_obj(raw.get("playbook_summary_json"))
    thesis_exit_params = dict(summary.get("thesis_exit_params") or {})
    return CatalogRow(
        catalog_key=catalog_key,
        symbol=symbol,
        strategy_key=strategy_key,
        direction=direction or "long",
        thesis_exit_policy=thesis_exit_policy,
        thesis_exit_params=thesis_exit_params,
        playbook_summary=summary,
        raw_row=raw,
    )


def _filter_rows(
    rows: list[CatalogRow],
    *,
    catalog_keys: list[str] | None,
    symbols: list[str] | None,
    limit: int | None,
) -> list[CatalogRow]:
    selected = rows
    if catalog_keys:
        wanted = set(catalog_keys)
        selected = [row for row in selected if row.catalog_key in wanted]
    if symbols:
        wanted_symbols = {symbol.upper() for symbol in symbols}
        selected = [row for row in selected if row.symbol in wanted_symbols]
    if limit is not None:
        selected = selected[: max(0, limit)]
    return selected


def _run_row_scenario(
    *,
    row: CatalogRow,
    raw: pl.DataFrame,
    scenario: str,
    provider_model: dict[str, dict[int, float]],
    seed: int,
    start: date,
    end: date,
    baseline: ScenarioResult | None,
) -> ScenarioResult:
    try:
        strategy = _build_strategy_for_row(row, scenario=scenario)
        scenario_raw = _apply_volume_scenario(
            raw,
            symbol=row.symbol,
            scenario=scenario,
            provider_model=provider_model,
            seed=seed,
        )
        needed = required_feature_union([strategy])
        enriched = PhysicsEngine().enrich_for_features(scenario_raw, needed)
        signal_frame = strategy.generate_signals(enriched.clone())
        signal_frame = _with_exit_policy_features(_directional_signal_frame(signal_frame, row.direction))
        policy = _build_exit_policy(row, strategy)
        simulator = TradeSimulator(
            entry_delay_bars=1,
            min_hold_bars=2,
            cooldown_bars_after_signal=5,
            exit_policy=policy,
        )
        trades = simulator.simulate(signal_frame)
        signals = _signal_keys(signal_frame)
        diagnostics = _diagnostics(
            baseline_frame=baseline.trades.to_dataframe() if baseline else None,
            baseline_signals=baseline.signals if baseline else None,
            signal_frame=signal_frame,
            baseline_metrics=baseline.metrics if baseline else None,
        )
        metrics = _metrics(row, scenario, signal_frame, signals, trades, start, end, baseline)
        return ScenarioResult(row=row, scenario=scenario, metrics=metrics, signals=signals, trades=trades, diagnostics=diagnostics)
    except Exception as exc:
        metrics = _error_metrics(row, scenario, str(exc), start, end)
        return ScenarioResult(
            row=row,
            scenario=scenario,
            metrics=metrics,
            signals=set(),
            trades=SimulationResult(),
            diagnostics={},
            error=str(exc),
        )


def _build_strategy_for_row(row: CatalogRow, *, scenario: str) -> BaseStrategy:
    params = _normalized_params(row.playbook_summary.get("entry_params") or {})
    if scenario == "volume_filter_off" and "use_volume_filter" in _strategy_parameter_names(row.strategy_key):
        params["use_volume_filter"] = False
    params = _constructor_params(STRATEGY_NAME_BY_KEY[row.strategy_key], params)
    strategy_name = STRATEGY_NAME_BY_KEY[row.strategy_key]
    return build_strategy(strategy_name, params)


def _constructor_params(strategy_name: str, params: dict[str, Any]) -> dict[str, Any]:
    strategy = build_strategy(strategy_name, {})
    signature = inspect.signature(type(strategy).__init__)
    allowed = {
        name
        for name, parameter in signature.parameters.items()
        if name != "self"
        and parameter.kind in {inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY}
    }
    return {key: value for key, value in params.items() if key in allowed}


def _strategy_parameter_names(strategy_key: str) -> set[str]:
    if strategy_key in {"opening_drive_classifier", "jerk_pivot_momentum"}:
        return {"use_volume_filter"}
    return set()


def _normalized_params(params: dict[str, Any]) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for key, value in params.items():
        if value in (None, ""):
            continue
        normalized[key] = _coerce_value(key, value)
    # Catalog rows may preserve comma-delimited tuples from CSV serialization.
    if isinstance(normalized.get("vwma_periods"), str):
        normalized["vwma_periods"] = tuple(
            int(part.strip()) for part in normalized["vwma_periods"].split(",") if part.strip()
        )
    return normalized


def _coerce_value(key: str, value: Any) -> Any:
    if isinstance(value, bool | int | float | list | tuple | dict):
        return value
    text = str(value).strip()
    lowered = text.lower()
    if lowered in {"true", "false"}:
        return lowered == "true"
    if key.endswith("_minutes") or key.endswith("_window") or key in {
        "entry_buffer_minutes",
        "entry_window_minutes",
        "opening_window_minutes",
        "entry_start_offset_minutes",
        "entry_end_offset_minutes",
        "kinematic_periods_back",
        "jerk_lookback",
    }:
        try:
            return int(text)
        except ValueError:
            return value
    try:
        if any(char in text for char in ".eE"):
            return float(text)
        return int(text)
    except ValueError:
        return value


def _apply_volume_scenario(
    raw: pl.DataFrame,
    *,
    symbol: str,
    scenario: str,
    provider_model: dict[str, dict[int, float]],
    seed: int,
) -> pl.DataFrame:
    if scenario in {"polygon_baseline", "volume_filter_off"}:
        return raw
    if scenario == "volume_neutral":
        return raw.with_columns(pl.lit(1.0).alias("volume"))
    if scenario == "volume_shuffled_intraday":
        return _shuffle_volume_intraday(raw, symbol=symbol, seed=seed)
    if scenario == "volume_provider_like":
        return _provider_like_volume(raw, symbol=symbol, provider_model=provider_model, seed=seed)
    raise ValueError(f"Unknown volume scenario: {scenario}")


def _shuffle_volume_intraday(raw: pl.DataFrame, *, symbol: str, seed: int) -> pl.DataFrame:
    frame = raw.with_row_index("_row_idx").with_columns(pl.col("timestamp").dt.date().alias("_trade_date"))
    replacements: dict[int, float] = {}
    for (trade_date,), group in frame.group_by("_trade_date", maintain_order=True):
        volumes = [float(v) for v in group["volume"].to_list()]
        random.Random(f"{seed}:{symbol}:{trade_date}").shuffle(volumes)
        for row_idx, volume in zip(group["_row_idx"].to_list(), volumes, strict=True):
            replacements[int(row_idx)] = volume
    ordered = [replacements[i] for i in range(frame.height)]
    return frame.drop(["_trade_date"]).with_columns(pl.Series("volume", ordered)).drop("_row_idx")


def _provider_like_volume(
    raw: pl.DataFrame,
    *,
    symbol: str,
    provider_model: dict[str, dict[int, float]],
    seed: int,
) -> pl.DataFrame:
    ratios = provider_model.get(symbol.upper()) or provider_model.get("*") or {}
    timestamps = raw["timestamp"].to_list()
    volumes = raw["volume"].to_list()
    adjusted = []
    for idx, (timestamp, volume) in enumerate(zip(timestamps, volumes, strict=True)):
        minute = _minute_of_regular_session(timestamp)
        ratio = ratios.get(minute)
        if ratio is None:
            ratio = _default_provider_like_ratio(minute)
        jitter = random.Random(f"{seed}:{symbol}:{timestamp}:{idx}").uniform(-0.03, 0.03)
        adjusted.append(max(0.0, float(volume) * max(0.05, ratio + jitter)))
    return raw.with_columns(pl.Series("volume", adjusted))


def _default_provider_like_ratio(minute: int) -> float:
    if minute < 0:
        return 0.75
    if minute <= 30:
        return 0.85
    if minute >= 360:
        return 0.9
    if 120 <= minute <= 240:
        return 1.1
    return 1.0


def _minute_of_regular_session(timestamp: Any) -> int:
    ts = timestamp
    if getattr(ts, "tzinfo", None) is None:
        ts = ts.replace(tzinfo=UTC)
    et = ts.astimezone(ET)
    return (et.hour - 9) * 60 + (et.minute - 30)


def _load_provider_volume_model(path: Path | None) -> dict[str, dict[int, float]]:
    if path is None or not path.exists():
        return {}
    ratios_by_symbol_minute: dict[str, dict[int, list[float]]] = defaultdict(lambda: defaultdict(list))
    for csv_path in sorted(path.glob("*.csv")):
        symbol = _symbol_from_provider_csv(csv_path)
        for row in _read_csv(csv_path):
            try:
                pvol = float(row.get("volume_polygon") or 0)
                svol = float(row.get("volume_schwab") or 0)
            except ValueError:
                continue
            if pvol <= 0:
                continue
            try:
                ts = datetime.fromisoformat(str(row.get("timestamp")).replace("Z", "+00:00"))
            except ValueError:
                continue
            ratios_by_symbol_minute[symbol][_minute_of_regular_session(ts)].append(svol / pvol)
    model: dict[str, dict[int, float]] = {}
    for symbol, by_minute in ratios_by_symbol_minute.items():
        model[symbol] = {
            minute: _median([ratio for ratio in ratios if math.isfinite(ratio) and ratio > 0])
            for minute, ratios in by_minute.items()
            if ratios
        }
    return model


def _symbol_from_provider_csv(path: Path) -> str:
    first = path.stem.split("_", 1)[0]
    return first.upper()


def _directional_signal_frame(frame: pl.DataFrame, direction: str) -> pl.DataFrame:
    if "signal_direction" not in frame.columns:
        return frame
    return frame.with_columns(
        (
            pl.col("signal").fill_null(False)
            & (pl.col("signal_direction").str.to_lowercase() == direction.lower())
        ).alias("signal")
    )


def _build_exit_policy(row: CatalogRow, strategy: BaseStrategy):
    params = row.thesis_exit_params
    policy = row.thesis_exit_policy
    if policy == "fixed_rr_underlying":
        return FixedPercentRewardRiskExitPolicy(
            stop_loss_pct=float(params.get("stop_loss_underlying_pct", 0.005)),
            reward_multiple=float(params.get("take_profit_underlying_r_multiple", 2.0)),
        )
    if policy == "time_stop_underlying":
        return TimeStopExitPolicy(exit_time=time.fromisoformat(str(params.get("exit_time_et", "15:55"))))
    if policy == "hold_to_eod_underlying":
        return HoldToEodExitPolicy()
    if policy == "trailing_vma_underlying":
        return VmaTrailingExitPolicy(vma_col=str(params.get("vma_col") or getattr(strategy, "vma_col", "vma_10")))
    if policy == "ma_trailing_underlying":
        return MovingAverageTrailingExitPolicy(ma_col=str(params.get("ma_col", "ema_20_exit")))
    if policy == "ma_crossover_underlying":
        return MovingAverageCrossoverExitPolicy(
            fast_ma_col=str(params.get("fast_ma_col", "ema_8_exit")),
            slow_ma_col=str(params.get("slow_ma_col", "ema_20_exit")),
        )
    if policy == "atr_trailing_underlying":
        return AtrTrailingExitPolicy(
            atr_col=str(params.get("atr_col", "atr_14_exit")),
            atr_multiple=float(params.get("atr_multiple", 2.0)),
        )
    raise ValueError(f"Unsupported thesis_exit_policy for replay: {policy}")


def _signal_keys(frame: pl.DataFrame) -> set[tuple[str, str]]:
    if frame.is_empty() or "signal" not in frame.columns:
        return set()
    rows = frame.filter(pl.col("signal").fill_null(False)).select(["timestamp", "signal_direction"]).iter_rows()
    return {(str(ts), str(direction)) for ts, direction in rows}


def _metrics(
    row: CatalogRow,
    scenario: str,
    signal_frame: pl.DataFrame,
    signals: set[tuple[str, str]],
    trades: SimulationResult,
    start: date,
    end: date,
    baseline: ScenarioResult | None,
) -> dict[str, Any]:
    pnl = [trade.pnl for trade in trades.trades]
    baseline_signals = baseline.signals if baseline is not None else set()
    overlap = len(signals & baseline_signals) / len(baseline_signals) if baseline_signals else None
    trade_entry_keys = {(str(trade.entry_time), trade.direction) for trade in trades.trades}
    baseline_trade_keys = (
        {(str(trade.entry_time), trade.direction) for trade in baseline.trades.trades}
        if baseline is not None
        else set()
    )
    trade_overlap = len(trade_entry_keys & baseline_trade_keys) / len(baseline_trade_keys) if baseline_trade_keys else None
    return {
        "catalog_key": row.catalog_key,
        "symbol": row.symbol,
        "strategy_key": row.strategy_key,
        "direction": row.direction,
        "thesis_exit_policy": row.thesis_exit_policy,
        "scenario": scenario,
        "period_start": start.isoformat(),
        "period_end": end.isoformat(),
        "signal_count": len(signals),
        "trade_count": trades.total_trades,
        "win_rate": round(trades.win_rate, 6),
        "expectancy": round(trades.expectancy, 6),
        "profit_factor": _round_float(trades.profit_factor),
        "total_pnl": round(trades.total_pnl, 6),
        "avg_winner": round(trades.avg_winner, 6),
        "avg_loser": round(trades.avg_loser, 6),
        "max_drawdown": round(_max_drawdown(pnl), 6),
        "avg_bars_held": round(sum(t.bars_held for t in trades.trades) / trades.total_trades, 6) if trades.total_trades else 0.0,
        "entry_overlap_rate_vs_baseline": _round_float(overlap),
        "trade_overlap_rate_vs_baseline": _round_float(trade_overlap),
        "baseline_expectancy": _round_float(baseline.metrics.get("expectancy")) if baseline else None,
        "delta_expectancy_vs_baseline": _round_float(trades.expectancy - float(baseline.metrics.get("expectancy", 0))) if baseline else None,
        "baseline_trade_count": baseline.trades.total_trades if baseline else None,
        "error": "",
    }


def _error_metrics(row: CatalogRow, scenario: str, error: str, start: date, end: date) -> dict[str, Any]:
    return {
        "catalog_key": row.catalog_key,
        "symbol": row.symbol,
        "strategy_key": row.strategy_key,
        "direction": row.direction,
        "thesis_exit_policy": row.thesis_exit_policy,
        "scenario": scenario,
        "period_start": start.isoformat(),
        "period_end": end.isoformat(),
        "signal_count": 0,
        "trade_count": 0,
        "win_rate": 0.0,
        "expectancy": 0.0,
        "profit_factor": 0.0,
        "total_pnl": 0.0,
        "avg_winner": 0.0,
        "avg_loser": 0.0,
        "max_drawdown": 0.0,
        "avg_bars_held": 0.0,
        "entry_overlap_rate_vs_baseline": None,
        "trade_overlap_rate_vs_baseline": None,
        "baseline_expectancy": None,
        "delta_expectancy_vs_baseline": None,
        "baseline_trade_count": None,
        "error": error,
    }


def _diagnostics(
    *,
    baseline_frame: pl.DataFrame | None,
    baseline_signals: set[tuple[str, str]] | None,
    signal_frame: pl.DataFrame,
    baseline_metrics: dict[str, Any] | None,
) -> dict[str, Any]:
    del baseline_frame, baseline_metrics
    diagnostics: dict[str, Any] = {}
    if baseline_signals is not None:
        signals = _signal_keys(signal_frame)
        diagnostics["signal_overlap_count"] = len(signals & baseline_signals)
        diagnostics["baseline_signal_count"] = len(baseline_signals)
        diagnostics["scenario_signal_count"] = len(signals)
    for column in [c for c in ("volume", "vpoc_4h", "directional_mass") if c in signal_frame.columns]:
        values = [float(v) for v in signal_frame[column].drop_nulls().to_list()]
        if values:
            diagnostics[f"{column}_mean"] = round(sum(values) / len(values), 6)
            diagnostics[f"{column}_max"] = round(max(values), 6)
            diagnostics[f"{column}_min"] = round(min(values), 6)
    for col in [c for c in signal_frame.columns if c.startswith("impulse_regime")]:
        counts = signal_frame.group_by(col).len().to_dict(as_series=False)
        diagnostics[f"{col}_distribution"] = json.dumps(dict(zip(counts[col], counts["len"], strict=False)), sort_keys=True)
    return diagnostics


def _max_drawdown(pnls: list[float]) -> float:
    peak = 0.0
    equity = 0.0
    max_dd = 0.0
    for pnl in pnls:
        equity += pnl
        peak = max(peak, equity)
        max_dd = max(max_dd, peak - equity)
    return max_dd


def _trade_detail_rows(result: ScenarioResult) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for trade in result.trades.trades:
        rows.append(
            {
                "catalog_key": result.row.catalog_key,
                "symbol": result.row.symbol,
                "strategy_key": result.row.strategy_key,
                "scenario": result.scenario,
                "entry_time": trade.entry_time,
                "exit_time": trade.exit_time,
                "direction": trade.direction,
                "entry_price": trade.entry_price,
                "exit_price": trade.exit_price,
                "exit_reason": trade.exit_reason,
                "pnl": trade.pnl,
                "bars_held": trade.bars_held,
            }
        )
    return rows


def _trades_payload(trades: SimulationResult) -> list[dict[str, Any]]:
    return [
        {
            "entry_time": str(trade.entry_time),
            "exit_time": str(trade.exit_time),
            "direction": trade.direction,
            "entry_price": trade.entry_price,
            "exit_price": trade.exit_price,
            "exit_reason": trade.exit_reason,
            "pnl": trade.pnl,
            "bars_held": trade.bars_held,
        }
        for trade in trades.trades
    ]


def _summarize_metrics(metrics: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in metrics:
        if row.get("error"):
            continue
        grouped[(str(row["strategy_key"]), str(row["scenario"]))].append(row)
    summary: list[dict[str, Any]] = []
    for (strategy_key, scenario), rows in sorted(grouped.items()):
        summary.append(
            {
                "strategy_key": strategy_key,
                "scenario": scenario,
                "row_count": len(rows),
                "total_trades": sum(int(row.get("trade_count") or 0) for row in rows),
                "total_signals": sum(int(row.get("signal_count") or 0) for row in rows),
                "avg_expectancy": _mean(row.get("expectancy") for row in rows),
                "avg_delta_expectancy_vs_baseline": _mean(row.get("delta_expectancy_vs_baseline") for row in rows),
                "avg_entry_overlap_vs_baseline": _mean(row.get("entry_overlap_rate_vs_baseline") for row in rows),
                "avg_trade_overlap_vs_baseline": _mean(row.get("trade_overlap_rate_vs_baseline") for row in rows),
            }
        )
    return summary


def _write_report(
    path: Path,
    *,
    started_at: datetime,
    args: argparse.Namespace,
    metrics: list[dict[str, Any]],
    summary: list[dict[str, Any]],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    worst = sorted(
        [row for row in metrics if row.get("scenario") != "polygon_baseline" and not row.get("error")],
        key=lambda row: float(row.get("delta_expectancy_vs_baseline") or 0),
    )[:15]
    lines = [
        "# Catalog Volume Sensitivity Report",
        "",
        f"- generated_at: `{datetime.now(UTC).isoformat()}`",
        f"- started_at: `{started_at.isoformat()}`",
        f"- period: `{args.start}` to `{args.end}`",
        f"- scenarios: `{', '.join(args.scenario or DEFAULT_SCENARIOS)}`",
        f"- row_count: `{len({row['catalog_key'] for row in metrics})}`",
        "",
        "## Summary By Strategy",
        "",
        "| strategy | scenario | rows | trades | avg expectancy | avg delta | entry overlap |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in summary:
        lines.append(
            "| {strategy_key} | {scenario} | {row_count} | {total_trades} | {avg_expectancy} | "
            "{avg_delta_expectancy_vs_baseline} | {avg_entry_overlap_vs_baseline} |".format(**row)
        )
    lines.extend(["", "## Largest Expectancy Degrades", ""])
    lines.append("| catalog_key | symbol | scenario | baseline | delta | overlap |")
    lines.append("|---|---:|---:|---:|---:|---:|")
    for row in worst:
        lines.append(
            f"| {row['catalog_key']} | {row['symbol']} | {row['scenario']} | "
            f"{row.get('baseline_expectancy')} | {row.get('delta_expectancy_vs_baseline')} | "
            f"{row.get('entry_overlap_rate_vs_baseline')} |"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _parse_json_obj(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        parsed = json.loads(str(value))
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _read_csv(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


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


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, default=str, sort_keys=True) + "\n", encoding="utf-8")


def _median(values: list[float]) -> float:
    if not values:
        return 1.0
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2


def _mean(values: Any) -> float | None:
    finite = [float(value) for value in values if value not in (None, "") and math.isfinite(float(value))]
    return round(sum(finite) / len(finite), 6) if finite else None


def _round_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return number
    return round(number, 6)


if __name__ == "__main__":
    raise SystemExit(main())
