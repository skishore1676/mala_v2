"""Bounded optimization/OOS experiment for rectangle definition version 2."""

from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import date, datetime, timezone
import hashlib
import json
from pathlib import Path
import subprocess
from typing import Any, Iterable, Sequence

import numpy as np
import polars as pl

from src.oracle.rectangle_range_expansion_simulator import (
    RangeExpansionTradeResult,
    simulate_daily_range_expansion_trade,
)
from src.oracle.rectangle_trade_simulator import simulate_rectangle_trade
from src.research.classical_patterns.contracts import (
    RectangleSignal,
    TradeResult,
    load_rectangle_config,
)
from src.research.classical_patterns.rectangle import enumerate_rectangles


DEFAULT_V1_CONFIG = Path("config/classical_patterns/rectangle_daily_v1.yaml")
DEFAULT_V2_CONFIG = Path("config/classical_patterns/rectangle_daily_v2.yaml")
DEFAULT_SOURCE_RUN = Path(
    "research/results/playbooks/classical_pattern_lab/public_validation_round_1/"
    "economic_public_43_v1"
)
OPTIMIZATION_END = date(2024, 12, 31)
BOOTSTRAP_DRAWS = 10_000
BOOTSTRAP_SEED = 20260718


def run_exit_experiment(
    *,
    source_run_dir: Path,
    output_dir: Path,
    v1_config_path: Path = DEFAULT_V1_CONFIG,
    v2_config_path: Path = DEFAULT_V2_CONFIG,
    require_clean_git: bool = True,
) -> dict[str, Any]:
    source_run_dir = source_run_dir.expanduser().resolve()
    output_dir = output_dir.expanduser().resolve()
    git = _git_state()
    if require_clean_git and git["dirty"]:
        raise ValueError("Final exit experiment requires a clean Git tree.")
    if output_dir.exists() and any(output_dir.iterdir()):
        raise ValueError("Exit experiment output directory must be absent or empty.")
    output_dir.mkdir(parents=True, exist_ok=True)

    v1_config = load_rectangle_config(v1_config_path)
    v2_config = load_rectangle_config(v2_config_path)
    source_receipt_path = source_run_dir / "receipt.json"
    source_receipt = json.loads(source_receipt_path.read_text(encoding="utf-8"))
    daily_path = source_run_dir / "daily_bars.parquet"
    source_signals_path = source_run_dir / "signals.csv"
    _verify_source_artifact(source_receipt, source_run_dir, "daily_bars.parquet")
    _verify_source_artifact(source_receipt, source_run_dir, "signals.csv")
    daily = pl.read_parquet(daily_path)
    quality = _profile_daily_bars(daily)
    if quality["status"] != "ready":
        raise ValueError(f"Source daily bars failed quality checks: {quality['failed_checks']}")

    daily_by_symbol = {
        str(symbol): daily.filter(pl.col("symbol") == symbol).sort("session_date")
        for symbol in daily.get_column("symbol").unique().sort().to_list()
    }
    v1_signals: list[RectangleSignal] = []
    v2_signals: list[RectangleSignal] = []
    v2_candidate_count = 0
    v2_scanned_window_count = 0
    for symbol in sorted(daily_by_symbol):
        frame = daily_by_symbol[symbol]
        v1_result = enumerate_rectangles(frame, v1_config)
        v2_result = enumerate_rectangles(frame, v2_config)
        v1_signals.extend(v1_result.signals)
        v2_signals.extend(v2_result.signals)
        v2_candidate_count += len(v2_result.candidates)
        v2_scanned_window_count += v2_result.scanned_window_count

    source_signal_ids = set(pl.read_csv(source_signals_path).get_column("signal_id").to_list())
    replayed_v1_signal_ids = {signal.signal_id for signal in v1_signals}
    if source_signal_ids != replayed_v1_signal_ids:
        raise ValueError("Version-1 signal replay does not match the source economic run.")

    baseline_by_event = {_event_key(signal): signal for signal in v1_signals}
    v2_by_event = {_event_key(signal): signal for signal in v2_signals}
    missing_baseline = sorted(set(baseline_by_event) - set(v2_by_event))
    rewritten_baseline = sorted(
        key
        for key in set(baseline_by_event) & set(v2_by_event)
        if baseline_by_event[key].candidate.candidate_id
        != v2_by_event[key].candidate.candidate_id
    )
    if missing_baseline or rewritten_baseline:
        raise ValueError(
            "Version 2 failed baseline-preservation: "
            f"missing={len(missing_baseline)}, rewritten={len(rewritten_baseline)}"
        )
    incremental_keys = set(v2_by_event) - set(baseline_by_event)
    invalid_incremental = sorted(
        key
        for key in incremental_keys
        if v2_by_event[key].candidate.lookback_sessions != 80
    )
    if invalid_incremental:
        raise ValueError("Every incremental version-2 event must be an 80-session rectangle.")

    trade_rows: list[dict[str, Any]] = []
    signal_rows: list[dict[str, Any]] = []
    for signal in sorted(
        v2_signals,
        key=lambda item: (
            item.candidate.breakout_date,
            item.candidate.symbol,
            item.candidate.direction.value,
        ),
    ):
        candidate = signal.candidate
        population = "80_only" if _event_key(signal) in incremental_keys else "baseline_20_40_60"
        signal_rows.append(
            {
                "signal_id": signal.signal_id,
                "symbol": candidate.symbol,
                "direction": candidate.direction.value,
                "breakout_date": candidate.breakout_date.isoformat(),
                "lookback_sessions": candidate.lookback_sessions,
                "population": population,
                "source_split": candidate.split,
                "base_stop": candidate.base_stop,
                "structural_negation": candidate.structural_negation,
                "rectangle_objective": candidate.objective,
            }
        )
        frame = daily_by_symbol[candidate.symbol]
        for stop_buffer in v2_config.definition.lfd_stop_buffer_atr:
            baseline_trade = simulate_rectangle_trade(
                signal,
                frame,
                stop_buffer_atr=stop_buffer,
                config=v2_config,
            )
            trade_rows.append(
                _baseline_trade_row(
                    baseline_trade,
                    signal=signal,
                    stop_buffer_atr=stop_buffer,
                    population=population,
                )
            )
            profile_trade = simulate_daily_range_expansion_trade(
                signal,
                frame,
                stop_buffer_atr=stop_buffer,
                config=v2_config,
            )
            trade_rows.append(
                _profile_trade_row(
                    profile_trade,
                    signal=signal,
                    stop_buffer_atr=stop_buffer,
                    population=population,
                )
            )

    _assign_analysis_periods(trade_rows)
    _verify_trade_population(trade_rows, signal_rows)
    scorecard = _build_scorecard(trade_rows)
    optimization_rows = [row for row in scorecard if row["analysis_period"] == "optimization"]
    if len(optimization_rows) != 4:
        raise ValueError("Optimization scorecard must contain exactly four variants.")
    selected = max(
        optimization_rows,
        key=lambda row: (
            row["mean_net_r_per_signal"],
            row["symbol_cluster_ci95_lower"],
            row["max_drawdown_r"],
            row["variant_id"],
        ),
    )
    selected_variant = str(selected["variant_id"])
    selected_oos = next(
        row
        for row in scorecard
        if row["analysis_period"] == "out_of_sample"
        and row["variant_id"] == selected_variant
    )
    verdict = _edge_verdict(selected_oos)

    by_slice = _build_slice_scorecard(trade_rows)
    comparison = _paired_comparisons(trade_rows)
    artifacts = {
        "signals_v2.csv": _write_csv(signal_rows, output_dir / "signals_v2.csv"),
        "exit_trades.csv": _write_csv(trade_rows, output_dir / "exit_trades.csv"),
        "exit_scorecard.csv": _write_csv(scorecard, output_dir / "exit_scorecard.csv"),
        "exit_slice_scorecard.csv": _write_csv(
            by_slice, output_dir / "exit_slice_scorecard.csv"
        ),
        "paired_exit_comparisons.csv": _write_csv(
            comparison, output_dir / "paired_exit_comparisons.csv"
        ),
    }
    receipt: dict[str, Any] = {
        "schema_version": "RectangleExitExperimentV2",
        "status": "complete",
        "executable": False,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "git": git,
        "question": "Does the frozen daily rectangle entry have net underlying edge under a bounded exit comparison?",
        "source": {
            "run_dir": str(source_run_dir),
            "receipt_hash": _hash(source_receipt_path),
            "daily_bars_hash": _hash(daily_path),
            "dataset_manifest_hash": source_receipt["data"].get("dataset_manifest_hash"),
            "universe_status": source_receipt["data"].get("universe_status"),
            "provider_adjustment_provenance": source_receipt["data"].get(
                "provider_adjustment_provenance"
            ),
        },
        "configs": {
            "v1_path": str(v1_config.source_path),
            "v1_hash": v1_config.source_hash,
            "v2_path": str(v2_config.source_path),
            "v2_hash": v2_config.source_hash,
        },
        "design": {
            "optimization_end": OPTIMIZATION_END.isoformat(),
            "out_of_sample_start": "2025-01-01",
            "primary_metric": "mean_net_r_per_signal",
            "selection_tiebreakers": [
                "symbol_cluster_ci95_lower",
                "max_drawdown_r",
                "variant_id",
            ],
            "variant_count": 4,
            "maximum_trade_sessions": v2_config.definition.maximum_trade_sessions,
            "boundary_purge": True,
            "no_trade_signal_r": 0.0,
            "bootstrap": {
                "method": "symbol_cluster_resample",
                "draws": BOOTSTRAP_DRAWS,
                "seed": BOOTSTRAP_SEED,
            },
        },
        "population": {
            "v1_signals": len(v1_signals),
            "v2_signals": len(v2_signals),
            "incremental_80_only_signals": len(incremental_keys),
            "v2_candidates": v2_candidate_count,
            "v2_scanned_windows": v2_scanned_window_count,
            "optimization_signals": len(
                {
                    row["signal_id"]
                    for row in trade_rows
                    if row["analysis_period"] == "optimization"
                }
            ),
            "out_of_sample_signals": len(
                {
                    row["signal_id"]
                    for row in trade_rows
                    if row["analysis_period"] == "out_of_sample"
                }
            ),
            "purged_signals": len(
                {
                    row["signal_id"]
                    for row in trade_rows
                    if row["analysis_period"] == "purged_boundary"
                }
            ),
        },
        "quality": quality,
        "identity_checks": {
            "source_v1_replay_exact": True,
            "v1_events_preserved_in_v2": True,
            "v1_representatives_preserved_in_v2": True,
            "incremental_events_are_80_only": True,
            "trade_variant_population_complete": True,
        },
        "selection": {
            "selected_variant": selected_variant,
            "optimization_metrics": selected,
            "out_of_sample_metrics": selected_oos,
        },
        "verdict": verdict,
        "limitations": [
            "The 43-symbol universe is a frozen current-symbol cohort, not a point-in-time market universe.",
            "Public provider adjustment continuity was checked empirically but its policy is undocumented.",
            "The 2025+ period was inspected in rectangle v1, so it is secondary OOS evidence rather than a pristine program-level holdout.",
            "The Range Expansion policy is a daily underlying analogue, not the minute-level option-premium profile.",
            "No result authorizes shadow or live trading.",
        ],
        "artifacts": artifacts,
    }
    report_path = output_dir / "REPORT.md"
    report_path.write_text(_render_report(receipt, scorecard, comparison), encoding="utf-8")
    receipt["artifacts"]["REPORT.md"] = _artifact_metadata(report_path, row_count=1)
    receipt_path = output_dir / "experiment_receipt.json"
    receipt_path.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return receipt


def _verify_source_artifact(receipt: dict[str, Any], root: Path, name: str) -> None:
    metadata = receipt.get("artifacts", {}).get(name)
    if not isinstance(metadata, dict) or not metadata.get("content_hash"):
        raise ValueError(f"Source receipt does not bind required artifact: {name}")
    path = root / str(metadata["path"])
    if _hash(path) != metadata["content_hash"]:
        raise ValueError(f"Source artifact hash mismatch: {name}")


def _profile_daily_bars(frame: pl.DataFrame) -> dict[str, Any]:
    required = {"session_date", "visible_at", "symbol", "open", "high", "low", "close", "volume"}
    failed: list[str] = []
    if not required.issubset(frame.columns):
        failed.append("required_columns")
        return {"status": "failed", "failed_checks": failed}
    null_cells = sum(frame.get_column(column).null_count() for column in required)
    if null_cells:
        failed.append("required_nulls")
    duplicate_keys = frame.select(["symbol", "session_date"]).is_duplicated().sum()
    if duplicate_keys:
        failed.append("duplicate_symbol_dates")
    invalid_ohlc = frame.filter(
        (pl.col("high") < pl.max_horizontal("open", "close", "low"))
        | (pl.col("low") > pl.min_horizontal("open", "close", "high"))
        | (pl.col("low") <= 0)
    ).height
    if invalid_ohlc:
        failed.append("invalid_ohlc")
    nonfinite = 0
    for column in ("open", "high", "low", "close", "volume"):
        nonfinite += frame.filter(~pl.col(column).is_finite()).height
    if nonfinite:
        failed.append("nonfinite_numeric")
    counts = frame.group_by("symbol").len().sort("symbol")
    return {
        "status": "ready" if not failed else "failed",
        "failed_checks": failed,
        "row_count": frame.height,
        "column_count": len(frame.columns),
        "symbol_count": counts.height,
        "sessions_per_symbol_min": int(counts.get_column("len").min()),
        "sessions_per_symbol_max": int(counts.get_column("len").max()),
        "first_session": str(frame.get_column("session_date").min()),
        "last_session": str(frame.get_column("session_date").max()),
        "required_null_cells": int(null_cells),
        "duplicate_symbol_dates": int(duplicate_keys),
        "invalid_ohlc_rows": int(invalid_ohlc),
        "nonfinite_numeric_cells": int(nonfinite),
    }


def _event_key(signal: RectangleSignal) -> tuple[str, date, str]:
    candidate = signal.candidate
    return candidate.symbol, candidate.breakout_date, candidate.direction.value


def _common_trade_fields(
    signal: RectangleSignal,
    *,
    variant_id: str,
    family: str,
    stop_buffer_atr: float,
    population: str,
) -> dict[str, Any]:
    candidate = signal.candidate
    return {
        "signal_id": signal.signal_id,
        "variant_id": variant_id,
        "exit_family": family,
        "stop_buffer_atr": stop_buffer_atr,
        "symbol": candidate.symbol,
        "direction": candidate.direction.value,
        "breakout_date": candidate.breakout_date.isoformat(),
        "lookback_sessions": candidate.lookback_sessions,
        "population": population,
        "source_split": candidate.split,
    }


def _baseline_trade_row(
    trade: TradeResult,
    *,
    signal: RectangleSignal,
    stop_buffer_atr: float,
    population: str,
) -> dict[str, Any]:
    row = _common_trade_fields(
        signal,
        variant_id=f"rectangle_height_lfd_buffer_{stop_buffer_atr:.2f}atr".replace(".", "p"),
        family="rectangle_height",
        stop_buffer_atr=stop_buffer_atr,
        population=population,
    )
    row.update(
        status=trade.status,
        entry_date=_iso(trade.entry_date),
        entry_price=trade.entry_price,
        stop_price=trade.stop_price,
        target_1_price=None,
        target_2_price=trade.target_price,
        target_1_date=None,
        target_1_quantity=0.0,
        exit_date=_iso(trade.exit_date),
        exit_price=trade.exit_price,
        exit_reason=trade.exit_reason,
        bars_held=trade.bars_held,
        gross_pnl=trade.gross_pnl,
        net_pnl=trade.net_pnl,
        net_return=trade.net_return,
        net_r=trade.net_r,
        event_net_r=_event_net_r(trade.status, trade.net_r),
        mfe=trade.mfe,
        mae=trade.mae,
    )
    return row


def _profile_trade_row(
    trade: RangeExpansionTradeResult,
    *,
    signal: RectangleSignal,
    stop_buffer_atr: float,
    population: str,
) -> dict[str, Any]:
    row = _common_trade_fields(
        signal,
        variant_id=trade.variant_id,
        family="daily_range_expansion_analogue",
        stop_buffer_atr=stop_buffer_atr,
        population=population,
    )
    row.update(
        status=trade.status,
        entry_date=_iso(trade.entry_date),
        entry_price=trade.entry_price,
        stop_price=trade.stop_price,
        target_1_price=trade.target_1_price,
        target_2_price=trade.target_2_price,
        target_1_date=_iso(trade.target_1_date),
        target_1_quantity=trade.target_1_quantity,
        exit_date=_iso(trade.exit_date),
        exit_price=trade.exit_price,
        exit_reason=trade.exit_reason,
        bars_held=trade.bars_held,
        gross_pnl=trade.gross_pnl,
        net_pnl=trade.net_pnl,
        net_return=trade.net_return,
        net_r=trade.net_r,
        event_net_r=_event_net_r(trade.status, trade.net_r),
        mfe=trade.mfe,
        mae=trade.mae,
    )
    return row


def _event_net_r(status: str, net_r: float | None) -> float | None:
    if status == "closed":
        return float(net_r) if net_r is not None else None
    if status in {"no_trade", "no_fill"}:
        return 0.0
    return None


def _assign_analysis_periods(rows: list[dict[str, Any]]) -> None:
    flags: dict[str, str] = {}
    by_signal: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_signal[str(row["signal_id"])].append(row)
    for signal_id, cohort in by_signal.items():
        breakout = date.fromisoformat(str(cohort[0]["breakout_date"]))
        if any(row["status"] == "censored" for row in cohort):
            flags[signal_id] = "incomplete_data"
        elif breakout <= OPTIMIZATION_END and any(
            row["exit_date"] and date.fromisoformat(str(row["exit_date"])) > OPTIMIZATION_END
            for row in cohort
        ):
            flags[signal_id] = "purged_boundary"
        elif breakout <= OPTIMIZATION_END:
            flags[signal_id] = "optimization"
        else:
            flags[signal_id] = "out_of_sample"
    for row in rows:
        row["analysis_period"] = flags[str(row["signal_id"])]


def _verify_trade_population(
    trade_rows: list[dict[str, Any]], signal_rows: list[dict[str, Any]]
) -> None:
    expected_variants = {
        "rectangle_height_lfd_buffer_0p00atr",
        "rectangle_height_lfd_buffer_0p10atr",
        "range_expansion_lfd_buffer_0p00atr",
        "range_expansion_lfd_buffer_0p10atr",
    }
    signal_ids = {str(row["signal_id"]) for row in signal_rows}
    if len(trade_rows) != len(signal_ids) * len(expected_variants):
        raise ValueError("Trade population row count does not match signal x variant contract.")
    identities = {(str(row["signal_id"]), str(row["variant_id"])) for row in trade_rows}
    if len(identities) != len(trade_rows):
        raise ValueError("Trade population contains duplicate signal/variant identities.")
    for signal_id in signal_ids:
        variants = {
            str(row["variant_id"])
            for row in trade_rows
            if row["signal_id"] == signal_id
        }
        if variants != expected_variants:
            raise ValueError(f"Incomplete trade variants for signal {signal_id}")


def _build_scorecard(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    variants = sorted({str(row["variant_id"]) for row in rows})
    for period in ("optimization", "out_of_sample"):
        for variant in variants:
            cohort = [
                row
                for row in rows
                if row["analysis_period"] == period and row["variant_id"] == variant
            ]
            result.append(_score_rows(cohort, analysis_period=period, variant_id=variant))
    return result


def _score_rows(
    rows: list[dict[str, Any]], *, analysis_period: str, variant_id: str
) -> dict[str, Any]:
    values = np.array([float(row["event_net_r"]) for row in rows], dtype=float)
    closed_values = np.array(
        [float(row["net_r"]) for row in rows if row["status"] == "closed"],
        dtype=float,
    )
    if len(values) == 0:
        raise ValueError(f"No rows available for {analysis_period}/{variant_id}")
    positives = values[values > 0]
    negatives = values[values < 0]
    profit_factor = (
        float(positives.sum() / abs(negatives.sum()))
        if len(negatives)
        else float("inf")
    )
    ordered = sorted(rows, key=lambda row: (row["breakout_date"], row["symbol"]))
    curve = np.cumsum([float(row["event_net_r"]) for row in ordered])
    peaks = np.maximum.accumulate(np.concatenate(([0.0], curve)))
    drawdowns = np.concatenate(([0.0], curve)) - peaks
    lower, upper = _symbol_cluster_interval(rows)
    positive_years, observed_years = _year_stability(rows)
    return {
        "analysis_period": analysis_period,
        "variant_id": variant_id,
        "exit_family": rows[0]["exit_family"],
        "stop_buffer_atr": rows[0]["stop_buffer_atr"],
        "signal_count": len(rows),
        "closed_trade_count": int(sum(row["status"] == "closed" for row in rows)),
        "no_trade_count": int(sum(row["status"] in {"no_trade", "no_fill"} for row in rows)),
        "mean_net_r_per_signal": float(values.mean()),
        "median_net_r_per_signal": float(np.median(values)),
        "mean_net_r_per_closed_trade": float(closed_values.mean()) if len(closed_values) else None,
        "win_rate_per_signal": float((values > 0).mean()),
        "profit_factor": profit_factor,
        "max_drawdown_r": float(drawdowns.min()),
        "symbol_cluster_ci95_lower": lower,
        "symbol_cluster_ci95_upper": upper,
        "positive_year_count": positive_years,
        "observed_year_count": observed_years,
    }


def _symbol_cluster_interval(rows: list[dict[str, Any]]) -> tuple[float, float]:
    by_symbol: dict[str, list[float]] = defaultdict(list)
    for row in rows:
        by_symbol[str(row["symbol"])].append(float(row["event_net_r"]))
    symbols = sorted(by_symbol)
    rng = np.random.default_rng(BOOTSTRAP_SEED)
    draws = np.empty(BOOTSTRAP_DRAWS, dtype=float)
    for index in range(BOOTSTRAP_DRAWS):
        sampled = rng.choice(symbols, size=len(symbols), replace=True)
        values = [value for symbol in sampled for value in by_symbol[str(symbol)]]
        draws[index] = float(np.mean(values))
    return float(np.quantile(draws, 0.025)), float(np.quantile(draws, 0.975))


def _year_stability(rows: list[dict[str, Any]]) -> tuple[int, int]:
    by_year: dict[int, list[float]] = defaultdict(list)
    for row in rows:
        by_year[date.fromisoformat(str(row["breakout_date"])).year].append(
            float(row["event_net_r"])
        )
    return sum(float(np.mean(values)) > 0 for values in by_year.values()), len(by_year)


def _build_slice_scorecard(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for period in ("optimization", "out_of_sample"):
        for variant in sorted({str(row["variant_id"]) for row in rows}):
            base = [
                row
                for row in rows
                if row["analysis_period"] == period and row["variant_id"] == variant
            ]
            for dimension in ("direction", "population"):
                for value in sorted({str(row[dimension]) for row in base}):
                    cohort = [row for row in base if row[dimension] == value]
                    values = [float(row["event_net_r"]) for row in cohort]
                    result.append(
                        {
                            "analysis_period": period,
                            "variant_id": variant,
                            "slice_dimension": dimension,
                            "slice_value": value,
                            "signal_count": len(cohort),
                            "mean_net_r_per_signal": float(np.mean(values)),
                            "profit_factor": _profit_factor(values),
                        }
                    )
    return result


def _paired_comparisons(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for period in ("optimization", "out_of_sample"):
        cohort = [row for row in rows if row["analysis_period"] == period]
        for buffer in (0.0, 0.1):
            baseline_id = f"rectangle_height_lfd_buffer_{buffer:.2f}atr".replace(".", "p")
            profile_id = f"range_expansion_lfd_buffer_{buffer:.2f}atr".replace(".", "p")
            baseline = {
                str(row["signal_id"]): float(row["event_net_r"])
                for row in cohort
                if row["variant_id"] == baseline_id
            }
            profile = {
                str(row["signal_id"]): float(row["event_net_r"])
                for row in cohort
                if row["variant_id"] == profile_id
            }
            if baseline.keys() != profile.keys():
                raise ValueError("Paired exit comparison signal identities do not match.")
            differences = np.array(
                [profile[key] - baseline[key] for key in sorted(baseline)], dtype=float
            )
            result.append(
                {
                    "analysis_period": period,
                    "stop_buffer_atr": buffer,
                    "signal_count": len(differences),
                    "mean_profile_minus_baseline_r": float(differences.mean()),
                    "median_profile_minus_baseline_r": float(np.median(differences)),
                    "profile_better_count": int((differences > 0).sum()),
                    "baseline_better_count": int((differences < 0).sum()),
                    "tie_count": int((differences == 0).sum()),
                }
            )
    return result


def _edge_verdict(oos: dict[str, Any]) -> dict[str, Any]:
    count = int(oos["signal_count"])
    mean_r = float(oos["mean_net_r_per_signal"])
    profit_factor = float(oos["profit_factor"])
    lower = float(oos["symbol_cluster_ci95_lower"])
    if count < 20:
        code = "inconclusive_too_few_signals"
        action = "Do not claim edge; retain only as an unproven discretionary signal candidate."
    elif mean_r <= 0 or profit_factor <= 1.0:
        code = "no_out_of_sample_edge"
        action = "Move on from autonomous rectangle trading; optional discretionary alerts require prospective logging."
    elif lower > 0:
        code = "candidate_edge_requires_fresh_confirmation"
        action = "Treat as a candidate signal only until a genuinely fresh external or forward holdout confirms it."
    else:
        code = "positive_but_inconclusive"
        action = "Do not claim edge; prospective signal-only observation is reasonable."
    return {
        "code": code,
        "selected_oos_mean_net_r_per_signal": mean_r,
        "selected_oos_profit_factor": profit_factor,
        "selected_oos_symbol_cluster_ci95_lower": lower,
        "action": action,
    }


def _render_report(
    receipt: dict[str, Any],
    scorecard: list[dict[str, Any]],
    comparison: list[dict[str, Any]],
) -> str:
    selected = receipt["selection"]
    verdict = receipt["verdict"]
    lines = [
        "# Rectangle Exit Experiment V2",
        "",
        "## Decision",
        "",
        f"**{verdict['code']}** — {verdict['action']}",
        "",
        f"Optimization selected `{selected['selected_variant']}`. Its OOS mean was "
        f"`{selected['out_of_sample_metrics']['mean_net_r_per_signal']:+.3f}R` per signal "
        f"with profit factor `{selected['out_of_sample_metrics']['profit_factor']:.2f}` and "
        f"symbol-clustered 95% interval "
        f"`[{selected['out_of_sample_metrics']['symbol_cluster_ci95_lower']:+.3f}, "
        f"{selected['out_of_sample_metrics']['symbol_cluster_ci95_upper']:+.3f}]`.",
        "",
        "## Design",
        "",
        "- Frozen 20/40/60 signals plus 80-only new events; no 80-session replacement of prior representatives.",
        "- Optimization: through 2024-12-31. OOS: 2025 onward. Boundary-crossing events are purged.",
        "- Primary selection metric: mean net R per emitted signal; no-trades count as zero.",
        "- Four variants: rectangle-height and daily Range Expansion analogue, each with raw and 0.10 ATR LFD stops.",
        "- The selected procedure is evaluated once on OOS.",
        "",
        "## Scorecard",
        "",
        "| Period | Variant | Signals | Mean R/signal | PF | 95% symbol CI | Max DD R |",
        "|---|---|---:|---:|---:|---:|---:|",
    ]
    for row in scorecard:
        lines.append(
            f"| {row['analysis_period']} | {row['variant_id']} | {row['signal_count']} | "
            f"{row['mean_net_r_per_signal']:+.3f} | {row['profit_factor']:.2f} | "
            f"[{row['symbol_cluster_ci95_lower']:+.3f}, {row['symbol_cluster_ci95_upper']:+.3f}] | "
            f"{row['max_drawdown_r']:+.2f} |"
        )
    lines.extend(
        [
            "",
            "## Paired Range Expansion Difference",
            "",
            "| Period | Buffer ATR | Signals | Mean profile minus baseline R | Profile better | Baseline better |",
            "|---|---:|---:|---:|---:|---:|",
        ]
    )
    for row in comparison:
        lines.append(
            f"| {row['analysis_period']} | {row['stop_buffer_atr']:.2f} | "
            f"{row['signal_count']} | {row['mean_profile_minus_baseline_r']:+.3f} | "
            f"{row['profile_better_count']} | {row['baseline_better_count']} |"
        )
    lines.extend(["", "## Limitations", ""])
    lines.extend(f"- {item}" for item in receipt["limitations"])
    lines.extend(
        [
            "",
            "This artifact is local historical research. It is not a trading recommendation, shadow authorization, or live approval.",
            "",
        ]
    )
    return "\n".join(lines)


def _write_csv(rows: list[dict[str, Any]], path: Path) -> dict[str, Any]:
    frame = pl.DataFrame(rows) if rows else pl.DataFrame()
    frame.write_csv(path)
    return _artifact_metadata(path, row_count=frame.height)


def _artifact_metadata(path: Path, *, row_count: int) -> dict[str, Any]:
    return {"path": path.name, "content_hash": _hash(path), "row_count": row_count}


def _profit_factor(values: Iterable[float]) -> float:
    numbers = list(values)
    positive = sum(value for value in numbers if value > 0)
    negative = abs(sum(value for value in numbers if value < 0))
    return positive / negative if negative else float("inf")


def _iso(value: date | None) -> str | None:
    return value.isoformat() if value is not None else None


def _hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _git_state() -> dict[str, Any]:
    def run(*args: str) -> str:
        return subprocess.run(
            ["git", *args],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()

    status = run("status", "--short")
    return {
        "commit": run("rev-parse", "HEAD"),
        "branch": run("rev-parse", "--abbrev-ref", "HEAD"),
        "dirty": bool(status),
        "status": status.splitlines(),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-run-dir", type=Path, default=DEFAULT_SOURCE_RUN)
    parser.add_argument("--v1-config", type=Path, default=DEFAULT_V1_CONFIG)
    parser.add_argument("--v2-config", type=Path, default=DEFAULT_V2_CONFIG)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--allow-dirty", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    receipt = run_exit_experiment(
        source_run_dir=args.source_run_dir,
        output_dir=args.output_dir,
        v1_config_path=args.v1_config,
        v2_config_path=args.v2_config,
        require_clean_git=not args.allow_dirty,
    )
    print("STATUS=complete")
    print(f"VERDICT={receipt['verdict']['code']}")
    print(f"SELECTED_VARIANT={receipt['selection']['selected_variant']}")
    print(f"OUTPUT_DIR={args.output_dir.expanduser().resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
