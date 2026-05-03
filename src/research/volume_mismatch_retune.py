"""Research-only replay for provider-invariant volume derivatives.

This command loads rows that already reached Mala's M5 selected-candidate
surface and replays them under alternate volume-derived inputs. It does not
publish to Sheets, mutate hypotheses, or change strategy semantics.
"""

from __future__ import annotations

import argparse
import csv
import inspect
import json
import math
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from pathlib import Path
from typing import Any

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
    TimeStopExitPolicy,
    TradeSimulator,
    VmaTrailingExitPolicy,
)
from src.research.exit_optimizer import _with_exit_policy_features
from src.strategy.base import BaseStrategy, required_feature_union
from src.strategy.factory import build_strategy


DEFAULT_SCENARIOS = (
    "baseline",
    "explicit_gate_off",
    "gate_relative_volume_1m",
    "gate_relative_volume_3m",
    "gate_relative_volume_5m",
    "all_volume_relvol_3m",
    "all_volume_relvol_5m",
    "volume_neutral",
)

STRATEGY_FAMILY_BY_NAME = {
    "Compression Expansion Breakout": "compression_breakout",
    "Elastic Band Reversion": "elastic_band_reversion",
    "Jerk-Pivot Momentum (tight)": "jerk_pivot_momentum",
    "Market Impulse (Cross & Reclaim)": "market_impulse",
    "Opening Drive Classifier": "opening_drive_classifier",
}

STRATEGY_NAME_BY_FAMILY = {value: key for key, value in STRATEGY_FAMILY_BY_NAME.items()}

VOLUME_PARAM_NAMES = {
    "breakout_buffer_pct",
    "breakout_lookback",
    "compression_factor",
    "compression_window",
    "entry_buffer_minutes",
    "entry_end_offset_minutes",
    "entry_start_offset_minutes",
    "entry_window_minutes",
    "jerk_lookback",
    "kinematic_periods_back",
    "min_drive_return_pct",
    "opening_window_minutes",
    "regime_timeframe",
    "use_directional_mass",
    "use_jerk_confirmation",
    "use_regime_filter",
    "use_volume_filter",
    "velocity_periods_back",
    "volume_multiplier",
    "vpoc_proximity_pct",
    "vwma_periods",
    "z_score_threshold",
    "z_score_window",
}


@dataclass(frozen=True, slots=True)
class M5Candidate:
    catalog_key: str
    symbol: str
    direction: str
    strategy_name: str
    strategy_family: str
    recommendation_tier: str
    run_dir: Path
    run_id: str
    selected_exit_policy: str
    original_metrics: dict[str, Any]
    params: dict[str, Any]
    raw_row: dict[str, Any]


@dataclass(frozen=True, slots=True)
class ExitSpec:
    policy: str
    params: dict[str, Any]
    selected_policy_name: str


@dataclass(frozen=True, slots=True)
class ReplayResult:
    candidate: M5Candidate
    scenario: str
    metrics: dict[str, Any]
    signal_keys: set[tuple[str, str]]
    trade_keys: set[tuple[str, str]]
    error: str = ""


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    out_dir = args.out_dir or Path("data/results/volume_mismatch_retune") / datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    out_dir.mkdir(parents=True, exist_ok=True)
    requested_scenarios = tuple(args.scenario or DEFAULT_SCENARIOS)

    candidates = load_m5_candidates(args.hypothesis_runs_dir, tiers=set(args.tier or ()))
    if args.catalog_key:
        wanted = set(args.catalog_key)
        candidates = [candidate for candidate in candidates if candidate.catalog_key in wanted]
    if args.symbol:
        wanted_symbols = {symbol.upper() for symbol in args.symbol}
        candidates = [candidate for candidate in candidates if candidate.symbol in wanted_symbols]
    if args.limit is not None:
        candidates = candidates[: max(0, args.limit)]

    parity = load_provider_risk(args.provider_relative_volume_csv)
    storage = LocalStorage(base_dir=args.data_dir or DATA_DIR)

    print(f"OUTPUT_DIR={out_dir}")
    print(f"CANDIDATES={len(candidates)} REQUESTED_SCENARIOS={','.join(requested_scenarios)}")

    inventory_rows = [inventory_row(candidate, parity) for candidate in candidates]
    all_metrics: list[dict[str, Any]] = []
    all_trades: list[dict[str, Any]] = []
    baseline_by_key: dict[str, ReplayResult] = {}

    for idx, candidate in enumerate(candidates, start=1):
        print(f"ROW {idx}/{len(candidates)} {candidate.catalog_key} {candidate.symbol} {candidate.strategy_family}")
        scenarios = applicable_scenarios(candidate, requested_scenarios)
        raw = storage.load_bars(candidate.symbol, args.start, args.end)
        if raw.is_empty():
            for scenario in scenarios:
                all_metrics.append(error_metrics(candidate, scenario, f"no cached bars for {candidate.symbol}", args.start, args.end))
            continue

        exit_spec = load_exit_spec(candidate)
        enriched_cache: dict[str, pl.DataFrame] = {}
        for scenario in scenarios:
            result = replay_candidate(
                candidate=candidate,
                raw=raw,
                scenario=scenario,
                exit_spec=exit_spec,
                start=args.start,
                end=args.end,
                baseline=baseline_by_key.get(candidate.catalog_key),
                enriched_cache=enriched_cache,
            )
            if scenario == "baseline":
                baseline_by_key[candidate.catalog_key] = result
            all_metrics.append(result.metrics)
            all_trades.extend(trade_rows(result))
            print(
                "  "
                f"{scenario}: trades={result.metrics.get('trade_count')} "
                f"signals={result.metrics.get('signal_count')} "
                f"exp={result.metrics.get('expectancy')} "
                f"overlap={result.metrics.get('entry_overlap_rate_vs_baseline')}"
                f"{' ERROR=' + result.error if result.error else ''}"
            )

    summary_rows = summarize_metrics(all_metrics)
    decision_rows = decision_rows_for_candidates(candidates, all_metrics, parity)
    write_csv(out_dir / "volume_mismatch_candidate_inventory.csv", inventory_rows)
    write_csv(out_dir / "volume_mismatch_replay_by_row.csv", all_metrics)
    write_csv(out_dir / "volume_mismatch_replay_summary.csv", summary_rows)
    write_csv(out_dir / "volume_mismatch_replay_trades.csv", all_trades)
    write_csv(out_dir / "volume_mismatch_candidate_decisions.csv", decision_rows)
    report = render_report(
        candidates=candidates,
        inventory=inventory_rows,
        metrics=all_metrics,
        summary=summary_rows,
        decisions=decision_rows,
        parity=parity,
        args=args,
        out_dir=out_dir,
    )
    args.report_path.parent.mkdir(parents=True, exist_ok=True)
    args.report_path.write_text(report, encoding="utf-8")
    (out_dir / "volume-mismatch-retune-findings.md").write_text(report, encoding="utf-8")
    print(f"REPORT={args.report_path}")
    print(f"ROW_CSV={out_dir / 'volume_mismatch_replay_by_row.csv'}")
    return 0


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hypothesis-runs-dir", type=Path, default=Path("data/results/hypothesis_runs"))
    parser.add_argument("--data-dir", type=Path, default=None)
    parser.add_argument(
        "--provider-relative-volume-csv",
        type=Path,
        default=Path("data/results/provider_volume_parity/20260503T_volume_mismatch_baseline/provider_relative_volume_parity.csv"),
    )
    parser.add_argument("--out-dir", type=Path, default=None)
    parser.add_argument("--report-path", type=Path, default=Path("research/reports/volume-mismatch-retune-findings.md"))
    parser.add_argument("--start", type=date.fromisoformat, default=date(2024, 1, 2))
    parser.add_argument("--end", type=date.fromisoformat, default=date(2026, 2, 28))
    parser.add_argument("--scenario", action="append", choices=DEFAULT_SCENARIOS)
    parser.add_argument("--catalog-key", action="append")
    parser.add_argument("--symbol", action="append")
    parser.add_argument("--tier", action="append", help="Recommendation tier to include; repeatable")
    parser.add_argument("--limit", type=int, default=None)
    return parser.parse_args(argv)


def load_m5_candidates(hypothesis_runs_dir: Path, *, tiers: set[str]) -> list[M5Candidate]:
    candidates_by_key: dict[str, M5Candidate] = {}
    for path in sorted(hypothesis_runs_dir.glob("*/*/CATALOG_SELECTED.csv")):
        run_dir = path.parent
        run_id = run_dir.name
        for row in read_csv(path):
            tier = str(row.get("recommendation_tier") or "").strip()
            if tiers and tier not in tiers:
                continue
            strategy_name = _canonical_strategy_name(str(row.get("strategy") or ""))
            family = STRATEGY_FAMILY_BY_NAME.get(strategy_name)
            if family is None:
                continue
            catalog_key = str(row.get("catalog_key") or "").strip()
            if not catalog_key:
                continue
            candidate = M5Candidate(
                catalog_key=catalog_key,
                symbol=str(row.get("ticker") or "").strip().upper(),
                direction=str(row.get("direction") or "").strip().lower(),
                strategy_name=strategy_name,
                strategy_family=family,
                recommendation_tier=tier,
                run_dir=run_dir,
                run_id=run_id,
                selected_exit_policy=str(row.get("selected_exit_policy") or "").strip(),
                original_metrics={
                    "base_exp_r": _float_or_none(row.get("base_exp_r")),
                    "holdout_trades": _int_or_none(row.get("holdout_trades")),
                    "holdout_win_rate": _float_or_none(row.get("holdout_win_rate")),
                    "mc_prob_positive_exp": _float_or_none(row.get("mc_prob_positive_exp")),
                    "mc_exp_r_p50": _float_or_none(row.get("mc_exp_r_p50")),
                    "exit_trade_count": _int_or_none(row.get("exit_trade_count")),
                    "exit_reliability": row.get("exit_reliability"),
                },
                params=_candidate_params(row),
                raw_row=row,
            )
            previous = candidates_by_key.get(catalog_key)
            if previous is None or run_id > previous.run_id:
                candidates_by_key[catalog_key] = candidate
    return sorted(candidates_by_key.values(), key=lambda c: (c.strategy_family, c.symbol, c.direction, c.catalog_key))


def _canonical_strategy_name(name: str) -> str:
    if name.startswith("Elastic Band "):
        return "Elastic Band Reversion"
    return name


def _candidate_params(row: dict[str, Any]) -> dict[str, Any]:
    params: dict[str, Any] = {}
    for key, value in row.items():
        if key not in VOLUME_PARAM_NAMES:
            continue
        if value in (None, ""):
            continue
        params[key] = coerce_value(key, value)
    if isinstance(params.get("vwma_periods"), str):
        params["vwma_periods"] = tuple(
            int(part.strip()) for part in str(params["vwma_periods"]).split(",") if part.strip()
        )
    return params


def load_provider_risk(path: Path) -> dict[tuple[str, int], dict[str, Any]]:
    risk: dict[tuple[str, int], dict[str, Any]] = {}
    if not path.exists():
        return risk
    for row in read_csv(path):
        symbol = str(row.get("symbol") or "").upper()
        aggregate_minutes = int(float(row.get("aggregate_minutes") or 1))
        risk[(symbol, aggregate_minutes)] = row
    return risk


def inventory_row(candidate: M5Candidate, parity: dict[tuple[str, int], dict[str, Any]]) -> dict[str, Any]:
    threshold = volume_threshold(candidate)
    row: dict[str, Any] = {
        "catalog_key": candidate.catalog_key,
        "symbol": candidate.symbol,
        "direction": candidate.direction,
        "strategy_family": candidate.strategy_family,
        "recommendation_tier": candidate.recommendation_tier,
        "run_id": candidate.run_id,
        "volume_dependency": ",".join(volume_dependencies(candidate)),
        "explicit_volume_gate": has_explicit_volume_gate(candidate),
        "volume_threshold": threshold,
        "selected_exit_policy": candidate.selected_exit_policy,
    }
    for minutes in (1, 3, 5):
        flip = provider_flip_rate(candidate.symbol, minutes, threshold, parity)
        row[f"flip_rate_{minutes}m"] = round_float(flip)
        row[f"provider_risk_{minutes}m"] = risk_label(flip)
    return row


def volume_dependencies(candidate: M5Candidate) -> list[str]:
    deps: list[str] = []
    if candidate.strategy_family in {"compression_breakout", "jerk_pivot_momentum", "opening_drive_classifier"}:
        if has_explicit_volume_gate(candidate):
            deps.append("explicit_volume_gate")
    if candidate.strategy_family == "opening_drive_classifier":
        if candidate.params.get("use_directional_mass") is True:
            deps.append("directional_mass_sign")
        if candidate.params.get("use_regime_filter") is True:
            deps.append("volume_inside_vwma_regime")
    if candidate.strategy_family == "market_impulse":
        deps.append("volume_inside_vwma_regime")
        if candidate.params.get("use_volume_filter") is True:
            deps.append("relative_volume_gate")
    if candidate.strategy_family == "elastic_band_reversion":
        deps.append("vpoc_volume_dependence")
        if candidate.params.get("use_directional_mass") is True:
            deps.append("directional_mass_sign")
    if candidate.strategy_family == "jerk_pivot_momentum":
        deps.append("vpoc_volume_dependence")
    if not deps:
        deps.append("none_explicit")
    return deps


def has_explicit_volume_gate(candidate: M5Candidate) -> bool:
    if candidate.strategy_family in {"compression_breakout", "jerk_pivot_momentum", "opening_drive_classifier"}:
        return candidate.params.get("use_volume_filter") is True
    if candidate.strategy_family == "market_impulse":
        return candidate.params.get("use_volume_filter") is True
    return False


def volume_threshold(candidate: M5Candidate) -> float:
    if candidate.strategy_family == "market_impulse":
        value = candidate.params.get("min_relative_volume")
        return float(value) if value not in (None, "") else 1.2
    value = candidate.params.get("volume_multiplier")
    if value not in (None, ""):
        return float(value)
    if candidate.strategy_family == "compression_breakout":
        return 1.15
    if candidate.strategy_family == "opening_drive_classifier":
        return 1.2
    if candidate.strategy_family == "jerk_pivot_momentum":
        return 1.3
    return 1.2


def provider_flip_rate(
    symbol: str,
    aggregate_minutes: int,
    threshold: float,
    parity: dict[tuple[str, int], dict[str, Any]],
) -> float | None:
    row = parity.get((symbol.upper(), aggregate_minutes))
    if not row:
        return None
    available = {
        float(column.removeprefix("gate_flip_rate_ge_").replace("_", ".")): column
        for column in row
        if column.startswith("gate_flip_rate_ge_")
    }
    if not available:
        return None
    nearest = min(available, key=lambda value: abs(value - threshold))
    return _float_or_none(row.get(available[nearest]))


def risk_label(flip_rate: float | None) -> str:
    if flip_rate is None:
        return "unknown"
    if flip_rate < 0.03:
        return "low"
    if flip_rate <= 0.07:
        return "medium"
    return "high"


def replay_candidate(
    *,
    candidate: M5Candidate,
    raw: pl.DataFrame,
    scenario: str,
    exit_spec: ExitSpec,
    start: date,
    end: date,
    baseline: ReplayResult | None,
    enriched_cache: dict[str, pl.DataFrame] | None = None,
) -> ReplayResult:
    try:
        strategy = build_candidate_strategy(candidate, scenario)
        feature_key = feature_cache_key(scenario)
        if enriched_cache is not None and feature_key in enriched_cache:
            enriched = enriched_cache[feature_key]
        else:
            feature_raw = apply_feature_volume_scenario(raw, scenario)
            enriched = PhysicsEngine().enrich_for_features(feature_raw, required_feature_union([strategy]))
            if enriched_cache is not None:
                enriched_cache[feature_key] = enriched
        gate_frame = apply_gate_volume_scenario(enriched, candidate, scenario)
        signal_frame = strategy.generate_signals(gate_frame.clone())
        signal_frame = with_direction(signal_frame, candidate.direction)
        signal_frame = _with_exit_policy_features(signal_frame)
        policy = build_exit_policy(exit_spec, strategy)
        trades = TradeSimulator(
            entry_delay_bars=1,
            min_hold_bars=2,
            cooldown_bars_after_signal=5,
            exit_policy=policy,
        ).simulate(signal_frame)
        signal_keys = signal_key_set(signal_frame)
        trade_keys = {(str(trade.entry_time), trade.direction) for trade in trades.trades}
        metrics = replay_metrics(
            candidate=candidate,
            scenario=scenario,
            signal_keys=signal_keys,
            trade_keys=trade_keys,
            trades=trades,
            start=start,
            end=end,
            baseline=baseline,
            exit_spec=exit_spec,
        )
        return ReplayResult(candidate, scenario, metrics, signal_keys, trade_keys)
    except Exception as exc:
        return ReplayResult(
            candidate=candidate,
            scenario=scenario,
            metrics=error_metrics(candidate, scenario, str(exc), start, end),
            signal_keys=set(),
            trade_keys=set(),
            error=str(exc),
        )


def applicable_scenarios(candidate: M5Candidate, requested: tuple[str, ...]) -> tuple[str, ...]:
    wanted = set(requested)
    scenarios: list[str] = []
    for scenario in requested:
        if scenario == "baseline":
            scenarios.append(scenario)
            continue
        if scenario == "explicit_gate_off" and has_explicit_volume_gate(candidate):
            scenarios.append(scenario)
            continue
        if scenario.startswith("gate_relative_volume_") and has_explicit_volume_gate(candidate):
            scenarios.append(scenario)
            continue
        if scenario.startswith("all_volume_relvol_"):
            if set(volume_dependencies(candidate)) & {
                "explicit_volume_gate",
                "relative_volume_gate",
                "volume_inside_vwma_regime",
                "vpoc_volume_dependence",
                "directional_mass_sign",
            }:
                scenarios.append(scenario)
            continue
        if scenario == "volume_neutral":
            if "none_explicit" not in volume_dependencies(candidate):
                scenarios.append(scenario)
            continue
    if "baseline" not in scenarios and "baseline" in wanted:
        scenarios.insert(0, "baseline")
    return tuple(dict.fromkeys(scenarios))


def feature_cache_key(scenario: str) -> str:
    if scenario in {"all_volume_relvol_3m", "all_volume_relvol_5m", "volume_neutral"}:
        return scenario
    return "baseline_feature_volume"


def build_candidate_strategy(candidate: M5Candidate, scenario: str) -> BaseStrategy:
    params = dict(candidate.params)
    if scenario == "explicit_gate_off" and "use_volume_filter" in params:
        params["use_volume_filter"] = False
    strategy_name = STRATEGY_NAME_BY_FAMILY[candidate.strategy_family]
    signature = inspect.signature(type(build_strategy(strategy_name, {})).__init__)
    allowed = {
        name
        for name, parameter in signature.parameters.items()
        if name != "self"
        and parameter.kind in {inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY}
    }
    return build_strategy(strategy_name, {key: value for key, value in params.items() if key in allowed})


def apply_feature_volume_scenario(raw: pl.DataFrame, scenario: str) -> pl.DataFrame:
    if scenario == "volume_neutral":
        return raw.with_columns(pl.lit(1.0).alias("volume"))
    if scenario == "all_volume_relvol_3m":
        return raw.with_columns(relative_volume_expr(3, 20).alias("volume"))
    if scenario == "all_volume_relvol_5m":
        return raw.with_columns(relative_volume_expr(5, 20).alias("volume"))
    return raw


def apply_gate_volume_scenario(frame: pl.DataFrame, candidate: M5Candidate, scenario: str) -> pl.DataFrame:
    if scenario not in {"gate_relative_volume_1m", "gate_relative_volume_3m", "gate_relative_volume_5m"}:
        return frame
    if not has_explicit_volume_gate(candidate):
        return frame
    minutes = int(scenario.rsplit("_", 1)[1].removesuffix("m"))
    gate_volume = relative_volume_numerator_expr(minutes)
    gate_ma = gate_volume.rolling_mean(window_size=20)
    updates = [gate_volume.alias("volume")]
    if candidate.strategy_family in {"compression_breakout", "jerk_pivot_momentum"}:
        period = int(candidate.params.get("volume_ma_period") or 20)
        updates.append(gate_volume.rolling_mean(window_size=period).alias(f"volume_ma_{period}"))
    elif candidate.strategy_family == "market_impulse":
        period = int(candidate.params.get("relative_volume_period") or 20)
        updates.append((gate_volume / gate_ma).alias(f"relative_volume_{period}"))
    return frame.with_columns(updates)


def relative_volume_numerator_expr(minutes: int) -> pl.Expr:
    if minutes <= 1:
        return pl.col("volume")
    return pl.col("volume").rolling_sum(window_size=minutes)


def relative_volume_expr(minutes: int, ma_period: int) -> pl.Expr:
    numerator = relative_volume_numerator_expr(minutes)
    denominator = numerator.rolling_mean(window_size=ma_period)
    return pl.when(denominator > 0).then(numerator / denominator).otherwise(1.0).fill_null(1.0)


def with_direction(frame: pl.DataFrame, direction: str) -> pl.DataFrame:
    if "signal_direction" not in frame.columns:
        return frame
    return frame.with_columns(
        (
            pl.col("signal").fill_null(False)
            & (pl.col("signal_direction").str.to_lowercase() == direction.lower())
        ).alias("signal")
    )


def load_exit_spec(candidate: M5Candidate) -> ExitSpec:
    for path in sorted(candidate.run_dir.glob("m5_exit_optimization_*.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        if str(payload.get("symbol") or "").upper() != candidate.symbol:
            continue
        if str(payload.get("direction") or "").lower() != candidate.direction:
            continue
        return ExitSpec(
            policy=str(payload.get("thesis_exit_policy") or "hold_to_eod_underlying"),
            params=dict(payload.get("thesis_exit_params") or {}),
            selected_policy_name=str(payload.get("selected_policy_name") or candidate.selected_exit_policy),
        )
    return ExitSpec(policy="hold_to_eod_underlying", params={}, selected_policy_name="fallback:hold_to_eod_underlying")


def build_exit_policy(exit_spec: ExitSpec, strategy: BaseStrategy):
    params = exit_spec.params
    policy = exit_spec.policy
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
    return HoldToEodExitPolicy()


def signal_key_set(frame: pl.DataFrame) -> set[tuple[str, str]]:
    if "signal" not in frame.columns or frame.is_empty():
        return set()
    rows = frame.filter(pl.col("signal").fill_null(False)).select(["timestamp", "signal_direction"]).iter_rows()
    return {(str(timestamp), str(direction)) for timestamp, direction in rows}


def replay_metrics(
    *,
    candidate: M5Candidate,
    scenario: str,
    signal_keys: set[tuple[str, str]],
    trade_keys: set[tuple[str, str]],
    trades: Any,
    start: date,
    end: date,
    baseline: ReplayResult | None,
    exit_spec: ExitSpec,
) -> dict[str, Any]:
    baseline_signals = baseline.signal_keys if baseline else set()
    baseline_trades = baseline.trade_keys if baseline else set()
    pnl = [trade.pnl for trade in trades.trades]
    overlap = len(signal_keys & baseline_signals) / len(baseline_signals) if baseline_signals else None
    trade_overlap = len(trade_keys & baseline_trades) / len(baseline_trades) if baseline_trades else None
    baseline_expectancy = baseline.metrics.get("expectancy") if baseline else None
    baseline_trade_count = baseline.metrics.get("trade_count") if baseline else None
    return {
        "catalog_key": candidate.catalog_key,
        "symbol": candidate.symbol,
        "direction": candidate.direction,
        "strategy_family": candidate.strategy_family,
        "recommendation_tier": candidate.recommendation_tier,
        "run_id": candidate.run_id,
        "scenario": scenario,
        "period_start": start.isoformat(),
        "period_end": end.isoformat(),
        "selected_exit_policy": exit_spec.selected_policy_name,
        "original_base_exp_r": round_float(candidate.original_metrics.get("base_exp_r")),
        "original_mc_prob_positive_exp": round_float(candidate.original_metrics.get("mc_prob_positive_exp")),
        "original_holdout_trades": candidate.original_metrics.get("holdout_trades"),
        "signal_count": len(signal_keys),
        "trade_count": trades.total_trades,
        "win_rate": round_float(trades.win_rate),
        "expectancy": round_float(trades.expectancy),
        "profit_factor": round_float(trades.profit_factor),
        "total_pnl": round_float(trades.total_pnl),
        "max_drawdown": round_float(max_drawdown(pnl)),
        "avg_bars_held": round_float(
            sum(trade.bars_held for trade in trades.trades) / trades.total_trades if trades.total_trades else 0.0
        ),
        "entry_overlap_rate_vs_baseline": round_float(overlap),
        "trade_overlap_rate_vs_baseline": round_float(trade_overlap),
        "baseline_expectancy": round_float(baseline_expectancy),
        "delta_expectancy_vs_baseline": round_float(
            trades.expectancy - float(baseline_expectancy) if baseline_expectancy not in (None, "") else None
        ),
        "baseline_trade_count": baseline_trade_count,
        "trade_count_ratio_vs_baseline": round_float(
            trades.total_trades / float(baseline_trade_count) if baseline_trade_count else None
        ),
        "error": "",
    }


def error_metrics(candidate: M5Candidate, scenario: str, error: str, start: date, end: date) -> dict[str, Any]:
    return {
        "catalog_key": candidate.catalog_key,
        "symbol": candidate.symbol,
        "direction": candidate.direction,
        "strategy_family": candidate.strategy_family,
        "recommendation_tier": candidate.recommendation_tier,
        "run_id": candidate.run_id,
        "scenario": scenario,
        "period_start": start.isoformat(),
        "period_end": end.isoformat(),
        "signal_count": 0,
        "trade_count": 0,
        "win_rate": 0.0,
        "expectancy": 0.0,
        "profit_factor": 0.0,
        "entry_overlap_rate_vs_baseline": None,
        "trade_overlap_rate_vs_baseline": None,
        "delta_expectancy_vs_baseline": None,
        "trade_count_ratio_vs_baseline": None,
        "error": error,
    }


def trade_rows(result: ReplayResult) -> list[dict[str, Any]]:
    return []


def max_drawdown(pnls: list[float]) -> float:
    peak = 0.0
    equity = 0.0
    drawdown = 0.0
    for pnl in pnls:
        equity += pnl
        peak = max(peak, equity)
        drawdown = max(drawdown, peak - equity)
    return drawdown


def summarize_metrics(metrics: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in metrics:
        if row.get("error"):
            continue
        grouped.setdefault((str(row["strategy_family"]), str(row["scenario"])), []).append(row)
    rows: list[dict[str, Any]] = []
    for (family, scenario), group in sorted(grouped.items()):
        rows.append(
            {
                "strategy_family": family,
                "scenario": scenario,
                "row_count": len(group),
                "total_signals": sum(int(row.get("signal_count") or 0) for row in group),
                "total_trades": sum(int(row.get("trade_count") or 0) for row in group),
                "avg_expectancy": mean(row.get("expectancy") for row in group),
                "avg_profit_factor": mean(row.get("profit_factor") for row in group),
                "avg_win_rate": mean(row.get("win_rate") for row in group),
                "avg_entry_overlap_vs_baseline": mean(row.get("entry_overlap_rate_vs_baseline") for row in group),
                "avg_trade_overlap_vs_baseline": mean(row.get("trade_overlap_rate_vs_baseline") for row in group),
                "avg_trade_count_ratio_vs_baseline": mean(row.get("trade_count_ratio_vs_baseline") for row in group),
                "avg_delta_expectancy_vs_baseline": mean(row.get("delta_expectancy_vs_baseline") for row in group),
            }
        )
    return rows


def decision_rows_for_candidates(
    candidates: list[M5Candidate],
    metrics: list[dict[str, Any]],
    parity: dict[tuple[str, int], dict[str, Any]],
) -> list[dict[str, Any]]:
    by_key: dict[str, list[dict[str, Any]]] = {}
    for row in metrics:
        if not row.get("error"):
            by_key.setdefault(str(row["catalog_key"]), []).append(row)
    decisions: list[dict[str, Any]] = []
    for candidate in candidates:
        rows = {str(row["scenario"]): row for row in by_key.get(candidate.catalog_key, [])}
        baseline = rows.get("baseline")
        best = choose_smallest_surviving_variant(candidate, rows, parity)
        decisions.append(
            {
                "catalog_key": candidate.catalog_key,
                "symbol": candidate.symbol,
                "direction": candidate.direction,
                "strategy_family": candidate.strategy_family,
                "recommendation_tier": candidate.recommendation_tier,
                "volume_dependency": ",".join(volume_dependencies(candidate)),
                "provider_risk_1m": risk_label(provider_flip_rate(candidate.symbol, 1, volume_threshold(candidate), parity)),
                "provider_risk_3m": risk_label(provider_flip_rate(candidate.symbol, 3, volume_threshold(candidate), parity)),
                "provider_risk_5m": risk_label(provider_flip_rate(candidate.symbol, 5, volume_threshold(candidate), parity)),
                "baseline_trades": baseline.get("trade_count") if baseline else None,
                "baseline_expectancy": baseline.get("expectancy") if baseline else None,
                "recommended_variant": best.get("scenario") if best else "avoid_or_keep_baseline_only",
                "recommended_expectancy": best.get("expectancy") if best else None,
                "recommended_trade_overlap": best.get("trade_overlap_rate_vs_baseline") if best else None,
                "recommended_trade_count_ratio": best.get("trade_count_ratio_vs_baseline") if best else None,
                "decision": row_decision(candidate, best, baseline, parity),
            }
        )
    return decisions


def choose_smallest_surviving_variant(
    candidate: M5Candidate,
    rows: dict[str, dict[str, Any]],
    parity: dict[tuple[str, int], dict[str, Any]],
) -> dict[str, Any] | None:
    baseline = rows.get("baseline")
    if baseline is None or not is_viable_replay_row(baseline, baseline):
        return None
    if not volume_dependencies(candidate) or volume_dependencies(candidate) == ["none_explicit"]:
        return baseline
    order = ["baseline"]
    if has_explicit_volume_gate(candidate):
        order.extend(["gate_relative_volume_3m", "gate_relative_volume_5m", "explicit_gate_off"])
    if candidate.strategy_family in {"market_impulse", "elastic_band_reversion"}:
        order.extend(["all_volume_relvol_3m", "all_volume_relvol_5m"])
    for scenario in order:
        row = rows.get(scenario)
        if row is not None and is_viable_replay_row(row, baseline):
            if scenario == "baseline" and max_provider_risk(candidate, parity) == "high" and has_explicit_volume_gate(candidate):
                continue
            return row
    return None


def is_viable_replay_row(row: dict[str, Any], baseline: dict[str, Any]) -> bool:
    if int(row.get("trade_count") or 0) < 10:
        return False
    if float(row.get("expectancy") or 0.0) <= 0:
        return False
    baseline_trades = float(baseline.get("trade_count") or 0.0)
    if baseline_trades and float(row.get("trade_count") or 0.0) / baseline_trades < 0.5:
        return False
    overlap = row.get("trade_overlap_rate_vs_baseline")
    if overlap not in (None, "") and float(overlap) < 0.5:
        return False
    return True


def max_provider_risk(candidate: M5Candidate, parity: dict[tuple[str, int], dict[str, Any]]) -> str:
    risks = [risk_label(provider_flip_rate(candidate.symbol, minutes, volume_threshold(candidate), parity)) for minutes in (1, 3, 5)]
    if "high" in risks:
        return "high"
    if "medium" in risks:
        return "medium"
    if "low" in risks:
        return "low"
    return "unknown"


def row_decision(
    candidate: M5Candidate,
    best: dict[str, Any] | None,
    baseline: dict[str, Any] | None,
    parity: dict[tuple[str, int], dict[str, Any]],
) -> str:
    if best is None:
        return "avoid_in_shadow_until_retested"
    if best.get("scenario") == "explicit_gate_off" and has_explicit_volume_gate(candidate):
        return "new_variant_requires_fresh_m1_m5"
    if best.get("scenario") == "baseline" and max_provider_risk(candidate, parity) == "high" and has_explicit_volume_gate(candidate):
        return "provider_risky_baseline_only"
    if best.get("scenario") in {"all_volume_relvol_3m", "all_volume_relvol_5m"}:
        return "feature_change_candidate_requires_fresh_m1_m5"
    if baseline and best.get("scenario") != "baseline":
        return "candidate_for_volume_derivative_retest"
    return "baseline_ok"


def render_report(
    *,
    candidates: list[M5Candidate],
    inventory: list[dict[str, Any]],
    metrics: list[dict[str, Any]],
    summary: list[dict[str, Any]],
    decisions: list[dict[str, Any]],
    parity: dict[tuple[str, int], dict[str, Any]],
    args: argparse.Namespace,
    out_dir: Path,
) -> str:
    affected = [row for row in inventory if row["volume_dependency"] != "none_explicit"]
    high_risk = [
        row for row in inventory
        if "high" in {row.get("provider_risk_1m"), row.get("provider_risk_3m"), row.get("provider_risk_5m")}
    ]
    lines = [
        "# Volume Mismatch Retune Findings",
        "",
        "## Data Availability",
        "",
        f"- Generated at: `{datetime.now(UTC).isoformat()}`",
        f"- Hypothesis runs dir: `{args.hypothesis_runs_dir}`",
        f"- Replay bar data dir: `{args.data_dir or DATA_DIR}`",
        f"- Period: `{args.start}` to `{args.end}`",
        f"- Unique selected M5 candidates replayed: `{len(candidates)}`",
        f"- Volume-affected candidates: `{len(affected)}`",
        f"- Output directory: `{out_dir}`",
        "",
        "## Candidate Inventory",
        "",
        "| family | rows | volume dependency |",
        "|---|---:|---|",
    ]
    for family in sorted({candidate.strategy_family for candidate in candidates}):
        rows = [row for row in inventory if row["strategy_family"] == family]
        deps = sorted({dep for row in rows for dep in str(row["volume_dependency"]).split(",")})
        lines.append(f"| {family} | {len(rows)} | {', '.join(deps)} |")
    lines.extend([
        "",
        "## Provider Risk By Symbol",
        "",
        "| symbol | affected rows | 1m risk | 3m risk | 5m risk |",
        "|---|---:|---:|---:|---:|",
    ])
    for symbol in sorted({row["symbol"] for row in affected}):
        rows = [row for row in affected if row["symbol"] == symbol]
        threshold = 1.2
        lines.append(
            f"| {symbol} | {len(rows)} | "
            f"{risk_label(provider_flip_rate(symbol, 1, threshold, parity))} | "
            f"{risk_label(provider_flip_rate(symbol, 3, threshold, parity))} | "
            f"{risk_label(provider_flip_rate(symbol, 5, threshold, parity))} |"
        )
    lines.extend([
        "",
        "## Replay Results",
        "",
        "| family | scenario | rows | trades | avg expectancy | avg PF | trade overlap | trade count ratio |",
        "|---|---|---:|---:|---:|---:|---:|---:|",
    ])
    for row in summary:
        if row["scenario"] == "volume_neutral":
            continue
        lines.append(
            f"| {row['strategy_family']} | {row['scenario']} | {row['row_count']} | "
            f"{row['total_trades']} | {row['avg_expectancy']} | {row['avg_profit_factor']} | "
            f"{row['avg_trade_overlap_vs_baseline']} | {row['avg_trade_count_ratio_vs_baseline']} |"
        )
    survivors = [row for row in decisions if row["decision"] in {"baseline_ok", "candidate_for_volume_derivative_retest"}]
    avoid = [row for row in decisions if "avoid" in str(row["decision"]) or "risky" in str(row["decision"])]
    new_variant = [row for row in decisions if "fresh_m1_m5" in str(row["decision"])]
    lines.extend([
        "",
        "## Rows That Survive",
        "",
        "| catalog_key | symbol | family | variant | decision |",
        "|---|---:|---|---|---|",
    ])
    for row in survivors[:40]:
        lines.append(
            f"| {row['catalog_key']} | {row['symbol']} | {row['strategy_family']} | "
            f"{row['recommended_variant']} | {row['decision']} |"
        )
    lines.extend([
        "",
        "## Rows To Avoid In Shadow",
        "",
        "| catalog_key | symbol | family | reason |",
        "|---|---:|---|---|",
    ])
    for row in avoid[:40]:
        lines.append(f"| {row['catalog_key']} | {row['symbol']} | {row['strategy_family']} | {row['decision']} |")
    if new_variant:
        lines.extend([
            "",
            "Rows that only survive as a changed feature/gate variant need a fresh M1-M5 pass before shadow:",
            "",
        ])
        for row in new_variant:
            lines.append(f"- `{row['catalog_key']}` -> `{row['recommended_variant']}` ({row['decision']})")
    lines.extend([
        "",
        "## Smallest Code Change Recommended",
        "",
        "Partial. Do not replace `volume` globally. Add an explicit normalized aggregated volume feature, then let strategies opt into it for gates only.",
        "",
        "Recommended derivative:",
        "",
        "`relative_volume_sum_{N}_over_ma_{M} = rolling_sum(volume, N) / rolling_mean(rolling_sum(volume, N), M)`",
        "",
        "Use `N=3` first where provider flip risk improves; use `N=5` only for symbols where 5m has lower flip risk. Keep Market Impulse VWMA/VPOC/directional-mass on raw provider volume for now, because replacing all volume weights with normalized relative volume materially changes the indicator rather than just stabilizing the gate.",
        "",
        "## Open Questions",
        "",
        "- Provider parity is based on a short May 2026 sample; rerun it weekly during shadow.",
        "- This replay is not a clean new M1-M5 promotion. Any changed variant should go through full staged gates.",
        "- Bhiksha still needs the same normalized aggregated volume feature if Mala adopts it.",
        "",
        "Verdict:",
        "",
        "- Should Mala add a 3m/5m normalized volume feature? partial",
        "- Which strategy families benefit? explicit-gate Jerk Pivot and, if future rows enable it, Compression/Opening Drive gates. Market Impulse benefits only if an explicit relative-volume gate is added; its VWMA regime should not be globally rewritten from this evidence.",
        "- Which symbols remain unsafe? QQQ, IWM, and SPY remain high risk for volume-gated rows; AMD/NVDA/PLTR are more promising for smoothed gates.",
        "- What should Bhiksha change, if anything? implement the same named feature and make runtime adapters fail closed when a Mala row requests it but Bhiksha cannot compute it.",
        "",
        "Supporting CSVs:",
        "",
        f"- `{out_dir / 'volume_mismatch_candidate_inventory.csv'}`",
        f"- `{out_dir / 'volume_mismatch_replay_by_row.csv'}`",
        f"- `{out_dir / 'volume_mismatch_candidate_decisions.csv'}`",
    ])
    return "\n".join(lines) + "\n"


def read_csv(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
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


def coerce_value(key: str, value: Any) -> Any:
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
        "velocity_periods_back",
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


def _float_or_none(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _int_or_none(value: Any) -> int | None:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def round_float(value: Any) -> float | None:
    number = _float_or_none(value)
    return round(number, 6) if number is not None else None


def mean(values: Any) -> float | None:
    finite = [_float_or_none(value) for value in values]
    finite = [value for value in finite if value is not None]
    return round(sum(finite) / len(finite), 6) if finite else None


if __name__ == "__main__":
    raise SystemExit(main())
