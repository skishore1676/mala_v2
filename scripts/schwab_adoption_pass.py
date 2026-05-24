"""Run a bounded Schwab adoption replay for Mala evidence rows.

This script is intended to run on oldmac from the Bhiksha virtualenv:

    cd /Users/sunny/Documents/bhiksha
    ./.venv/bin/python /Users/sunny/Documents/mala_v2/scripts/schwab_adoption_pass.py \
      --artifact-dir /Users/sunny/Documents/mala_v2/data/results/research_ops/schwab_adoption/manual_YYYYMMDD

It consumes evidence_rows.json and operator_defaults_rows.json exported by
Mala, compiles those rows through Bhiksha's active-plan compiler, fetches
Schwab 1-minute bars, and writes adoption CSV/Markdown artifacts. It exits
non-zero on Schwab auth expiry so auth failures do not get published as
strategy failures.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import math
import re
import statistics
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any

import polars as pl
import yaml

from bhiksha.active_plan.compiler import (
    compile_active_plan_from_rows,
    load_operator_defaults_sheet_rows,
    load_rows_from_sheet_records_with_report,
    load_strategy_catalog_sheet_rows_with_report,
    _atomic_yaml_write,
    _google_catalog_entry_payload,
    _validate_google_catalog_exit_contract,
)
from bhiksha.app.replay import (
    ReplaySignalEvaluator,
    ReplayTrade,
    _deployment_hard_flat_time,
    _hard_flat_exit_decision,
    _replay_underlying_entry_price,
)
from bhiksha.domain.enums import SignalDirection
from bhiksha.domain.models import OptionSelectionRequest
from bhiksha.execution.brokers.public.client import PublicApiClient
from bhiksha.execution.thesis_exit import evaluate_underlying_thesis_exit
from bhiksha.market_data.adapters.schwab import SchwabBarSource
from bhiksha.market_data.feature_service import FeatureService
from bhiksha.market_data.session import as_et_time
from bhiksha.options.public_chain import PublicOptionChainService
from bhiksha.options.selectors import SingleLegOptionSelector
from bhiksha.state.position_tracker import TrackedPosition
from bhiksha.strategy.registry import default_strategy_registry


AUTH_EXPIRED_TEXT = "refresh token has expired"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact-dir", required=True)
    parser.add_argument("--lookback-days", type=int, default=63)
    parser.add_argument("--max-entry-window-minutes", type=int, default=150)
    parser.add_argument("--max-premium-usd", default="1000")
    parser.add_argument(
        "--public-option-smoke-limit",
        type=int,
        default=5,
        help="Fetch Public option-chain and option-bar smoke evidence for this many adoption-pass rows. Use 0 to disable.",
    )
    return parser.parse_args()


def frame_from_bars(bars: list[Any]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "symbol": [bar.symbol for bar in bars],
            "timestamp": [bar.timestamp for bar in bars],
            "open": [bar.open for bar in bars],
            "high": [bar.high for bar in bars],
            "low": [bar.low for bar in bars],
            "close": [bar.close for bar in bars],
            "volume": [bar.volume for bar in bars],
        }
    )


def parse_hhmm(value: Any, default: time) -> time:
    text = str(value or "").strip()
    if not text:
        return default
    hour, minute = text.split(":")[:2]
    return time(int(hour), int(minute))


def strategy_scan_window(deployment: Any) -> tuple[time, time, str]:
    params = deployment.strategy.params
    key = deployment.strategy.key
    if key == "market_impulse":
        open_hour = int(params.get("market_open_hour", 9))
        open_minute = int(params.get("market_open_minute", 30))
        buffer_minutes = int(params.get("entry_buffer_minutes", 3))
        window_minutes = int(params.get("entry_window_minutes", 60))
        start_total = open_hour * 60 + open_minute + buffer_minutes
        end_total = open_hour * 60 + open_minute + window_minutes
        return time(start_total // 60, start_total % 60), time(end_total // 60, end_total % 60), "strategy_params"
    if key == "opening_drive_classifier":
        open_total = 9 * 60 + 30
        start_total = open_total + int(params.get("entry_start_offset_minutes", 25))
        end_total = open_total + int(params.get("entry_end_offset_minutes", 120))
        return time(start_total // 60, start_total % 60), time(end_total // 60, end_total % 60), "strategy_params"
    if key == "jerk_pivot_momentum":
        return parse_hhmm(params.get("session_start"), time(9, 35)), parse_hhmm(
            params.get("session_end"), time(15, 30)
        ), "strategy_params"
    return parse_hhmm(getattr(deployment.execution, "entry_window_start_et", None), time(9, 30)), parse_hhmm(
        getattr(deployment.execution, "entry_window_end_et", None), time(16, 0)
    ), "execution_defaults"


def window_width_minutes(start: time, end: time) -> int:
    return (end.hour * 60 + end.minute) - (start.hour * 60 + start.minute)


def scan_bounded(
    evaluator: ReplaySignalEvaluator,
    deployment: Any,
    enriched: pl.DataFrame,
    *,
    max_entry_window_minutes: int,
) -> tuple[list[ReplayTrade] | None, int, str]:
    strategy = evaluator.strategy_registry.get(deployment.strategy.key)
    start_t, end_t, window_source = strategy_scan_window(deployment)
    width = window_width_minutes(start_t, end_t)
    if width > max_entry_window_minutes:
        return None, width, window_source

    trades: list[ReplayTrade] = []
    active_trade: ReplayTrade | None = None
    active_position: TrackedPosition | None = None
    hard_flat_time = _deployment_hard_flat_time(deployment)
    for index in range(enriched.height):
        latest = enriched.row(index, named=True)
        bar_t = as_et_time(latest["timestamp"])
        if active_trade is None:
            if not (start_t <= bar_t <= end_t):
                continue
            frame_slice = enriched.head(index + 1)
            entry = strategy.evaluate_entry(frame_slice, deployment.deployment_id, deployment.strategy.params)
            if not entry.signal:
                continue
            active_trade = ReplayTrade(entry_index=index, entry_decision=entry)
            active_position = TrackedPosition(
                symbol=deployment.symbol,
                deployment_id=deployment.deployment_id,
                option_symbol="REPLAY_OPTION_PLACEHOLDER",
                underlying_entry_price=_replay_underlying_entry_price(entry),
                entry_timestamp=entry.timestamp,
            )
            continue

        time_exit = _hard_flat_exit_decision(deployment, latest, hard_flat_time)
        if time_exit is not None:
            trades.append(ReplayTrade(active_trade.entry_index, active_trade.entry_decision, index, time_exit, "time_exit"))
            active_trade = None
            active_position = None
            continue

        frame_slice = enriched.head(index + 1)
        thesis_decision = evaluate_underlying_thesis_exit(deployment, frame_slice, active_position) if active_position else None
        if thesis_decision is not None and thesis_decision.exit:
            trades.append(
                ReplayTrade(active_trade.entry_index, active_trade.entry_decision, index, thesis_decision, "thesis_exit")
            )
            active_trade = None
            active_position = None
            continue

        if deployment.exit.use_algorithmic_exit and active_position is not None:
            decision = strategy.evaluate_exit(frame_slice, deployment.deployment_id, deployment.strategy.params, active_position)
            if decision.exit:
                trades.append(ReplayTrade(active_trade.entry_index, active_trade.entry_decision, index, decision, "strategy_exit"))
                active_trade = None
                active_position = None
    if active_trade is not None:
        trades.append(active_trade)
    return trades, width, window_source


def as_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        number = float(value)
        return number if math.isfinite(number) else None
    except (TypeError, ValueError):
        return None


def signed_move(trade: ReplayTrade, enriched: pl.DataFrame) -> float | None:
    if trade.exit_index is None or trade.exit_decision is None:
        return None
    entry = enriched.row(trade.entry_index, named=True)
    exit_row = enriched.row(trade.exit_index, named=True)
    entry_close = as_float(entry.get("close"))
    exit_close = as_float(exit_row.get("close"))
    if not entry_close or exit_close is None:
        return None
    direction = trade.entry_decision.direction.value if trade.entry_decision.direction else ""
    raw = (exit_close - entry_close) / entry_close
    return -raw if direction == "short" else raw


def minutes_held(trade: ReplayTrade) -> float | None:
    if trade.exit_decision is None:
        return None
    return (trade.exit_decision.timestamp - trade.entry_decision.timestamp).total_seconds() / 60


def row_id_for_catalog_key(catalog_key: str) -> str:
    return "adoption_" + re.sub(r"[^A-Za-z0-9_]+", "_", catalog_key)[:180]


def option_trade_ready(row: dict[str, Any]) -> bool:
    return str(row.get("option_trade_ready") or row.get("mala_option_trade_ready") or "").strip().lower() in {"true", "1", "yes", "y"}


def materialization_base(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "catalog_key": str(row.get("catalog_key") or "").strip(),
        "symbol": str(row.get("symbol") or "").strip().upper(),
        "direction": str(row.get("direction") or "").strip().lower(),
        "strategy_key": str(row.get("strategy_key") or "").strip().lower(),
        "mala_expectancy": str(row.get("expectancy") or ""),
        "mala_option_adjusted_expectancy_pct": str(row.get("option_adjusted_expectancy_pct") or ""),
        "mala_signal_count": str(row.get("signal_count") or ""),
        "mala_bhiksha_capability_status": str(row.get("bhiksha_capability_status") or ""),
        "mala_bhiksha_capability_reason": str(row.get("bhiksha_capability_reason") or ""),
        "mala_option_trade_ready": str(row.get("option_trade_ready") or ""),
    }


def materialize_strategy_catalog(
    *,
    artifact_dir: Path,
    evidence: list[dict[str, Any]],
    catalog_entries: list[Any],
    operator_defaults: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    """Write a temporary adoption catalog and classify every evidence row.

    Bhiksha's normal Google catalog sync intentionally writes only promotable
    rows. For adoption we need a stronger audit: every Mala_Evidence_v1 row gets
    a materialization verdict, and supported/watch rows may still be replayed.
    """

    catalog_root = artifact_dir / "strategy_catalog"
    generated_root = catalog_root / "m6_full_adoption"
    generated_root.mkdir(parents=True, exist_ok=True)
    supported_keys = set(default_strategy_registry()._strategies)
    entries_by_key = {entry.catalog_key: entry for entry in catalog_entries}
    verdicts: dict[str, dict[str, Any]] = {}
    expected_paths: set[Path] = set()

    for row in evidence:
        base = materialization_base(row)
        catalog_key = base["catalog_key"]
        if not catalog_key:
            continue
        verdict = {
            **base,
            "materialization_row_id": row_id_for_catalog_key(catalog_key),
            "materialization_status": "materialized",
            "materialization_reason": "temporary_runtime_contract_written",
        }
        entry = entries_by_key.get(catalog_key)
        if entry is None:
            verdict.update(
                {
                    "materialization_status": "missing_strategy_catalog_contract",
                    "materialization_reason": "catalog_row_invalid_or_missing",
                }
            )
            verdicts[catalog_key] = verdict
            continue
        strategy_key = str(entry.strategy_key or "").strip().lower()
        if strategy_key not in supported_keys:
            verdict.update(
                {
                    "materialization_status": "runtime_adapter_missing",
                    "materialization_reason": f"strategy_key_not_in_bhiksha_registry:{strategy_key}",
                }
            )
            verdicts[catalog_key] = verdict
            continue
        capability_status = str(entry.bhiksha_capability_status or "").strip().lower()
        if capability_status and capability_status != "supported":
            verdict.update(
                {
                    "materialization_status": "runtime_adapter_missing",
                    "materialization_reason": str(entry.bhiksha_capability_reason or "bhiksha_capability_not_supported"),
                }
            )
            verdicts[catalog_key] = verdict
            continue
        if not entry.symbol or not entry.direction or not isinstance(entry.strategy_params_json, dict):
            verdict.update(
                {
                    "materialization_status": "missing_entry_params",
                    "materialization_reason": "symbol_direction_or_strategy_params_missing",
                }
            )
            verdicts[catalog_key] = verdict
            continue
        try:
            _validate_google_catalog_exit_contract(catalog_key, entry)
            payload = _google_catalog_entry_payload(entry, operator_defaults=operator_defaults)
        except Exception as exc:  # noqa: BLE001 - row-level materialization evidence.
            verdict.update(
                {
                    "materialization_status": "missing_exit_contract",
                    "materialization_reason": f"{type(exc).__name__}:{str(exc)[:240]}",
                }
            )
            verdicts[catalog_key] = verdict
            continue

        output_path = generated_root / f"{catalog_key}.yaml"
        _atomic_yaml_write(output_path, yaml.safe_dump(payload, sort_keys=False))
        expected_paths.add(output_path)
        verdict["materialized_catalog_path"] = str(output_path)
        verdicts[catalog_key] = verdict

    for stale_file in sorted(generated_root.glob("*.yaml")):
        if stale_file not in expected_paths:
            stale_file.unlink()
    return verdicts


def compile_plan(artifact_dir: Path, *, max_premium_usd: str) -> Any:
    evidence = json.loads((artifact_dir / "evidence_rows.json").read_text(encoding="utf-8"))
    defaults_rows = json.loads((artifact_dir / "operator_defaults_rows.json").read_text(encoding="utf-8"))
    operator_defaults = load_operator_defaults_sheet_rows(defaults_rows)
    catalog_validation = load_strategy_catalog_sheet_rows_with_report(evidence, sheet_name="Mala_Evidence_v1")
    materialization = materialize_strategy_catalog(
        artifact_dir=artifact_dir,
        evidence=evidence,
        catalog_entries=catalog_validation.rows,
        operator_defaults=operator_defaults,
    )
    active_payloads = []
    for row in evidence:
        key = str(row.get("catalog_key") or "").strip()
        if not key or materialization.get(key, {}).get("materialization_status") != "materialized":
            continue
        active_payloads.append(
            {
                "row_id": row_id_for_catalog_key(key),
                "row_type": "strategy",
                "enabled": "TRUE",
                "authorization_mode": "shadow",
                "strategy_id": key,
                "max_trade_premium_usd": max_premium_usd,
                "notes": "Temporary Schwab adoption pass row",
            }
        )
    row_validation = load_rows_from_sheet_records_with_report(
        active_payloads,
        row_type="strategy",
        sheet_name="schwab_adoption_temp",
    )
    compiled = compile_active_plan_from_rows(
        rows=row_validation.rows,
        strategy_catalog_path=artifact_dir / "strategy_catalog",
        active_plan_id=f"schwab_adoption_{datetime.now(UTC).strftime('%Y%m%d')}",
        trading_date=datetime.now(UTC).date().isoformat(),
        source_name="schwab_adoption_pass",
        source_details={"evidence_rows": str(artifact_dir / "evidence_rows.json")},
        suppressed=[
            *catalog_validation.suppressed,
            *row_validation.suppressed,
            *[
                {
                    "action": "suppressed",
                    "row_id": row_id_for_catalog_key(key),
                    "row_type": "strategy",
                    "sheet_name": "m6_full_adoption_materializer",
                    "reason": f"{row['materialization_status']}:{row['materialization_reason']}",
                }
                for key, row in materialization.items()
                if row.get("materialization_status") != "materialized"
            ],
        ],
    )
    (artifact_dir / "active_plan_all_evidence.json").write_text(
        json.dumps(compiled.plan.model_dump(mode="json"), indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (artifact_dir / "m6_materialization_by_row.csv").write_text(
        csv_text(materialization.values()),
        encoding="utf-8",
    )
    print(f"compiled_deployments={len(compiled.plan.deployments)} suppressed={len(compiled.plan.suppressed)}")
    return compiled, evidence, materialization


async def fetch_schwab_bars(symbols: set[str], *, lookback_days: int) -> tuple[dict[str, list[Any]], dict[str, str]]:
    source = SchwabBarSource()
    bars_by_symbol: dict[str, list[Any]] = {}
    errors: dict[str, str] = {}
    try:
        end = datetime.now(UTC)
        start = end - timedelta(days=lookback_days)
        for symbol in sorted(symbols):
            try:
                bars = await source.warm_start(symbol, start, end)
                bars_by_symbol[symbol] = bars
                first = bars[0].timestamp if bars else ""
                last = bars[-1].timestamp if bars else ""
                print(f"bars {symbol} count={len(bars)} first={first} last={last}")
            except Exception as exc:  # noqa: BLE001 - diagnostic artifact needs the provider message.
                message = f"{type(exc).__name__}: {str(exc)[:240]}"
                errors[symbol] = message
                bars_by_symbol[symbol] = []
                print(f"bars_error {symbol} {message}")
    finally:
        await source.close()
    return bars_by_symbol, errors


def csv_text(rows: Any) -> str:
    materialized = list(rows)
    fieldnames: list[str] = []
    for row in materialized:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    if not fieldnames:
        return ""
    import io

    handle = io.StringIO()
    writer = csv.DictWriter(handle, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(materialized)
    return handle.getvalue()


def suppressed_by_row_id(compiled: Any) -> dict[str, dict[str, Any]]:
    mapped: dict[str, dict[str, Any]] = {}
    for row in compiled.plan.suppressed:
        row_id = str(row.get("row_id") or "")
        if row_id:
            mapped[row_id] = row
    return mapped


def adoption_status_for_unreplayed(base: dict[str, Any]) -> tuple[str, str]:
    status = str(base.get("materialization_status") or "")
    reason = str(base.get("materialization_reason") or "")
    if status in {"runtime_adapter_missing", "missing_entry_params", "missing_exit_contract", "missing_strategy_catalog_contract"}:
        return "adoption_blocked", status
    if status == "compile_suppressed":
        return "adoption_blocked", f"compile_suppressed:{reason[:180]}"
    if not option_trade_ready(base):
        return "adoption_blocked", "option_not_ready"
    return "adoption_blocked", reason or "not_replayed"


def build_adoption_rows(
    *,
    compiled: Any,
    evidence: list[dict[str, Any]],
    materialization: dict[str, dict[str, Any]],
    bars_by_symbol: dict[str, list[Any]],
    max_entry_window_minutes: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    evaluator = ReplaySignalEvaluator(FeatureService(), default_strategy_registry())
    deployments_by_key: dict[str, Any] = {}
    for deployment in compiled.plan.deployments:
        metadata = (deployment.source.metadata if deployment.source else {}) or {}
        catalog_key = str(metadata.get("strategy_id") or metadata.get("catalog_key") or deployment.deployment_id.replace("adoption_", ""))
        deployments_by_key[catalog_key] = deployment
    suppressed = suppressed_by_row_id(compiled)
    rows: list[dict[str, Any]] = []
    trade_rows: list[dict[str, Any]] = []
    for evidence_row in evidence:
        catalog_key = str(evidence_row.get("catalog_key") or "").strip()
        if not catalog_key:
            continue
        deployment = deployments_by_key.get(catalog_key)
        mat = materialization.get(catalog_key, materialization_base(evidence_row))
        base = {
            **materialization_base(evidence_row),
            **{
                key: value
                for key, value in mat.items()
                if key.startswith("materialization") or key == "materialized_catalog_path"
            },
            "catalog_key": catalog_key,
            "deployment_id": deployment.deployment_id if deployment is not None else "",
        }
        if deployment is None:
            suppressed_row = suppressed.get(row_id_for_catalog_key(catalog_key))
            if suppressed_row is not None:
                base["materialization_status"] = "compile_suppressed"
                base["materialization_reason"] = str(suppressed_row.get("reason") or "")
            status, reason = adoption_status_for_unreplayed(base)
            rows.append({**base, "adoption_status": status, "adoption_reason": reason, "schwab_bar_count": ""})
            continue

        bars = bars_by_symbol.get(deployment.symbol, [])
        base.update(
            {
                "symbol": deployment.symbol,
                "strategy_key": deployment.strategy.key,
                "schwab_bar_count": len(bars),
            }
        )
        if not bars:
            rows.append({**base, "adoption_status": "adoption_blocked", "adoption_reason": "no_schwab_bars"})
            continue
        try:
            enriched = evaluator.prepare_enriched_frames(frame_from_bars(bars), [deployment])[deployment.deployment_id]
            trades, window_width, window_source = scan_bounded(
                evaluator,
                deployment,
                enriched,
                max_entry_window_minutes=max_entry_window_minutes,
            )
        except Exception as exc:  # noqa: BLE001 - row-level replay errors are adoption evidence.
            rows.append(
                {
                    **base,
                    "adoption_status": "adoption_blocked",
                    "adoption_reason": f"replay_error:{type(exc).__name__}",
                    "error": str(exc)[:500],
                }
            )
            print(f"error {deployment.deployment_id} {type(exc).__name__}: {str(exc)[:200]}")
            continue
        if trades is None:
            rows.append(
                {
                    **base,
                    "adoption_status": "adoption_watch",
                    "adoption_reason": "wide_signal_window_needs_overnight_or_narrowing",
                    "entry_window_minutes": window_width,
                    "entry_window_source": window_source,
                }
            )
            print(f"skip_wide {deployment.deployment_id} width={window_width}")
            continue

        moves: list[float] = []
        holds: list[float] = []
        wins = 0
        for trade in trades:
            move = signed_move(trade, enriched)
            hold = minutes_held(trade)
            if move is not None:
                moves.append(move)
                if move > 0:
                    wins += 1
            if hold is not None:
                holds.append(hold)
            trade_rows.append(
                {
                    "catalog_key": catalog_key,
                    "deployment_id": deployment.deployment_id,
                    "symbol": deployment.symbol,
                    "entry_ts": trade.entry_decision.timestamp.isoformat(),
                    "exit_ts": trade.exit_decision.timestamp.isoformat() if trade.exit_decision else "",
                    "direction": trade.entry_decision.direction.value if trade.entry_decision.direction else "",
                    "exit_category": trade.exit_category,
                    "signed_underlying_move_pct": "" if move is None else round(move * 100, 6),
                    "minutes_held": "" if hold is None else round(hold, 2),
                }
            )
        avg = sum(moves) / len(moves) if moves else None
        median = statistics.median(moves) if moves else None
        win_rate = wins / len(moves) if moves else None
        avg_hold = sum(holds) / len(holds) if holds else None
        median_hold = statistics.median(holds) if holds else None
        if not option_trade_ready(base):
            status, reason = "adoption_blocked", "option_not_ready"
        elif len(trades) < 3:
            status, reason = "adoption_watch", "thin_schwab_trade_count"
        elif avg is None or avg <= 0:
            status, reason = "adoption_blocked", "non_positive_schwab_expectancy"
        elif win_rate is not None and win_rate < 0.4:
            status, reason = "adoption_watch", "low_schwab_win_rate"
        else:
            status, reason = "adoption_pass", "positive_schwab_replay"
        rows.append(
            {
                **base,
                "adoption_status": status,
                "adoption_reason": reason,
                "entry_window_minutes": window_width,
                "entry_window_source": window_source,
                "schwab_trade_count": len(trades),
                "schwab_closed_trade_count": len(moves),
                "schwab_win_rate": "" if win_rate is None else round(win_rate, 4),
                "schwab_avg_signed_move_pct": "" if avg is None else round(avg * 100, 6),
                "schwab_median_signed_move_pct": "" if median is None else round(median * 100, 6),
                "schwab_avg_minutes_held": "" if avg_hold is None else round(avg_hold, 2),
                "schwab_median_minutes_held": "" if median_hold is None else round(median_hold, 2),
                "first_bar": bars[0].timestamp.isoformat(),
                "last_bar": bars[-1].timestamp.isoformat(),
            }
        )
        print(f"done {deployment.deployment_id} {status} trades={len(trades)} avg_move_pct={'' if avg is None else round(avg * 100, 6)}")
    return rows, trade_rows


def deployment_direction(deployment: Any) -> SignalDirection:
    params = deployment.strategy.params or {}
    direction = str(params.get("direction") or (deployment.source.metadata or {}).get("direction") or "").lower()
    return SignalDirection.SHORT if direction == "short" else SignalDirection.LONG


async def public_option_bars_smoke(
    *,
    compiled: Any,
    adoption_rows: list[dict[str, Any]],
    limit: int,
) -> list[dict[str, Any]]:
    if limit <= 0:
        return []
    deployments_by_key: dict[str, Any] = {}
    for deployment in compiled.plan.deployments:
        metadata = (deployment.source.metadata if deployment.source else {}) or {}
        catalog_key = str(metadata.get("strategy_id") or metadata.get("catalog_key") or deployment.deployment_id.replace("adoption_", ""))
        deployments_by_key[catalog_key] = deployment

    selected = [
        row
        for row in adoption_rows
        if row.get("adoption_status") == "adoption_pass" and row.get("catalog_key") in deployments_by_key
    ][:limit]
    if not selected:
        return []

    chain_service = PublicOptionChainService()
    public_client = PublicApiClient()
    selector = SingleLegOptionSelector()
    smoke_rows: list[dict[str, Any]] = []
    try:
        for row in selected:
            catalog_key = str(row["catalog_key"])
            deployment = deployments_by_key[catalog_key]
            base = {
                "catalog_key": catalog_key,
                "deployment_id": deployment.deployment_id,
                "symbol": deployment.symbol,
                "strategy_key": deployment.strategy.key,
                "public_option_smoke_status": "blocked",
                "public_option_smoke_reason": "",
            }
            try:
                direction = deployment_direction(deployment)
                today = date.today()
                contracts = await chain_service.get_chain(
                    deployment.symbol,
                    contract_type="ALL",
                    from_date=today + timedelta(days=max(deployment.execution.dte_min, 0)),
                    to_date=today + timedelta(days=max(deployment.execution.dte_max, deployment.execution.dte_min) + 1),
                    strike_count=200,
                )
                request = OptionSelectionRequest(
                    deployment_id=deployment.deployment_id,
                    symbol=deployment.symbol,
                    direction=direction,
                    signal_timestamp=datetime.now(UTC),
                    execution_profile=deployment.execution.profile,
                    execution_params={
                        **deployment.execution.model_dump(),
                        "long_signal_contract_type": deployment.execution.option_mapping.get("long_signal", "CALL"),
                        "short_signal_contract_type": deployment.execution.option_mapping.get("short_signal", "PUT"),
                    },
                )
                selection = selector.select(request, contracts)
                endpoint_results = []
                for label, endpoint in [
                    ("day_1m", f"/userapigateway/historicdata/OPTION/{selection.option_symbol}/DAY/ONE_MINUTE"),
                    ("week_5m", f"/userapigateway/historicdata/OPTION/{selection.option_symbol}/WEEK/FIVE_MINUTES"),
                    ("year_1d", f"/userapigateway/historicdata/OPTION/{selection.option_symbol}/YEAR/ONE_DAY"),
                ]:
                    try:
                        payload = await public_client.get(endpoint)
                        bars = (payload.get("regularMarket") or {}).get("bars") or []
                        endpoint_results.append((label, bars, "ok", ""))
                    except Exception as exc:  # noqa: BLE001 - provider smoke evidence.
                        endpoint_results.append((label, [], "error", f"{type(exc).__name__}:{str(exc)[:160]}"))

                output = {
                    **base,
                    "public_option_smoke_status": "pass",
                    "public_option_smoke_reason": "public_chain_and_option_bars_available",
                    "public_option_symbol": selection.option_symbol,
                    "public_option_contract_type": selection.contract_type,
                    "public_option_dte": selection.dte,
                    "public_option_abs_delta": "" if selection.abs_delta is None else round(selection.abs_delta, 4),
                    "public_option_bid": "" if selection.bid is None else selection.bid,
                    "public_option_ask": "" if selection.ask is None else selection.ask,
                    "public_option_strike": "" if selection.strike is None else selection.strike,
                    "public_option_estimated_entry": "" if selection.estimated_entry_price is None else selection.estimated_entry_price,
                }
                for label, bars, status, error in endpoint_results:
                    output[f"public_option_{label}_status"] = status
                    output[f"public_option_{label}_bars"] = len(bars)
                    output[f"public_option_{label}_error"] = error
                    if bars:
                        output[f"public_option_{label}_first_ts"] = bars[0].get("timestamp", "")
                        output[f"public_option_{label}_last_ts"] = bars[-1].get("timestamp", "")
                        output[f"public_option_{label}_last_close"] = bars[-1].get("close", "")
                if not any(len(bars) for _, bars, status, _ in endpoint_results if status == "ok"):
                    output["public_option_smoke_status"] = "watch"
                    output["public_option_smoke_reason"] = "chain_available_but_no_option_bars_in_checked_periods"
                smoke_rows.append(output)
                print(f"public_option_smoke {catalog_key} {output['public_option_smoke_status']} {selection.option_symbol}")
            except Exception as exc:  # noqa: BLE001 - row-level provider smoke evidence.
                smoke_rows.append(
                    {
                        **base,
                        "public_option_smoke_status": "blocked",
                        "public_option_smoke_reason": f"{type(exc).__name__}:{str(exc)[:240]}",
                    }
                )
                print(f"public_option_smoke_error {catalog_key} {type(exc).__name__}: {str(exc)[:200]}")
    finally:
        await chain_service.close()
        await public_client.close()
    return smoke_rows


def merge_public_smoke(rows: list[dict[str, Any]], smoke_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    smoke_by_key = {str(row.get("catalog_key")): row for row in smoke_rows}
    merged: list[dict[str, Any]] = []
    for row in rows:
        smoke = smoke_by_key.get(str(row.get("catalog_key")))
        merged.append({**row, **(smoke or {})})
    return merged


def write_outputs(
    artifact_dir: Path,
    rows: list[dict[str, Any]],
    trade_rows: list[dict[str, Any]],
    public_smoke_rows: list[dict[str, Any]],
    evidence_count: int,
    deployment_count: int,
) -> None:
    fieldnames: list[str] = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    with (artifact_dir / "schwab_adoption_by_row.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    trade_fields = [
        "catalog_key",
        "deployment_id",
        "symbol",
        "entry_ts",
        "exit_ts",
        "direction",
        "exit_category",
        "signed_underlying_move_pct",
        "minutes_held",
    ]
    with (artifact_dir / "schwab_adoption_trades.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=trade_fields)
        writer.writeheader()
        writer.writerows(trade_rows)
    if public_smoke_rows:
        (artifact_dir / "public_option_bars_smoke.csv").write_text(csv_text(public_smoke_rows), encoding="utf-8")
    counts: dict[str, int] = {}
    for row in rows:
        counts[str(row["adoption_status"])] = counts.get(str(row["adoption_status"]), 0) + 1
    materialization_counts: dict[str, int] = {}
    for row in rows:
        status = str(row.get("materialization_status") or "unknown")
        materialization_counts[status] = materialization_counts.get(status, 0) + 1
    public_counts: dict[str, int] = {}
    for row in public_smoke_rows:
        status = str(row.get("public_option_smoke_status") or "unknown")
        public_counts[status] = public_counts.get(status, 0) + 1
    lines = [
        "# Schwab Adoption Pass",
        "",
        f"- generated_at: `{datetime.now(UTC).isoformat()}`",
        f"- evidence_rows: `{evidence_count}`",
        f"- compiled_deployments: `{deployment_count}`",
        f"- status_counts: `{json.dumps(counts, sort_keys=True)}`",
        f"- materialization_counts: `{json.dumps(materialization_counts, sort_keys=True)}`",
        f"- public_option_smoke_counts: `{json.dumps(public_counts, sort_keys=True)}`",
        "",
        "| status | catalog_key | symbol | strategy | materialization | trades | win | avg move % | public option | reason |",
        "|---|---|---|---|---|---:|---:|---:|---|---|",
    ]
    order = {"adoption_pass": 0, "adoption_watch": 1, "adoption_blocked": 2}
    for row in sorted(rows, key=lambda item: (order.get(str(item.get("adoption_status", "")), 9), str(item.get("catalog_key", "")))):
        lines.append(
            "| {status} | {catalog_key} | {symbol} | {strategy_key} | {materialization} | {trades} | {win} | {avg} | {public} | {reason} |".format(
                status=row.get("adoption_status", ""),
                catalog_key=row.get("catalog_key", ""),
                symbol=row.get("symbol", ""),
                strategy_key=row.get("strategy_key", ""),
                materialization=row.get("materialization_status", ""),
                trades=row.get("schwab_trade_count", ""),
                win=row.get("schwab_win_rate", ""),
                avg=row.get("schwab_avg_signed_move_pct", ""),
                public=row.get("public_option_smoke_status", ""),
                reason=row.get("adoption_reason", ""),
            )
        )
    (artifact_dir / "SCHWAB_ADOPTION_PASS.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"ADOPTION_CSV={artifact_dir / 'schwab_adoption_by_row.csv'}")
    print(f"ADOPTION_MD={artifact_dir / 'SCHWAB_ADOPTION_PASS.md'}")
    print(f"COUNTS={json.dumps(counts, sort_keys=True)}")


async def async_main() -> int:
    args = parse_args()
    artifact_dir = Path(args.artifact_dir)
    compiled, evidence, materialization = compile_plan(artifact_dir, max_premium_usd=str(args.max_premium_usd))
    bars_by_symbol, errors = await fetch_schwab_bars(
        {deployment.symbol for deployment in compiled.plan.deployments},
        lookback_days=args.lookback_days,
    )
    if errors and not any(bars_by_symbol.values()) and any(AUTH_EXPIRED_TEXT in message for message in errors.values()):
        blocker = artifact_dir / "SCHWAB_ADOPTION_AUTH_REQUIRED.md"
        blocker.write_text(
            "# Schwab Adoption Blocked\n\n"
            f"- generated_at: `{datetime.now(UTC).isoformat()}`\n"
            "- reason: `schwab_refresh_token_expired`\n"
            "- action: refresh Schwab OAuth on oldmac, then rerun this adoption pass.\n",
            encoding="utf-8",
        )
        print(f"SCHWAB_ADOPTION_AUTH_REQUIRED={blocker}")
        return 2

    rows, trade_rows = build_adoption_rows(
        compiled=compiled,
        evidence=evidence,
        materialization=materialization,
        bars_by_symbol=bars_by_symbol,
        max_entry_window_minutes=args.max_entry_window_minutes,
    )
    public_smoke_rows = await public_option_bars_smoke(
        compiled=compiled,
        adoption_rows=rows,
        limit=args.public_option_smoke_limit,
    )
    rows = merge_public_smoke(rows, public_smoke_rows)
    write_outputs(artifact_dir, rows, trade_rows, public_smoke_rows, len(evidence), len(compiled.plan.deployments))
    return 0


def main() -> int:
    return asyncio.run(async_main())


if __name__ == "__main__":
    raise SystemExit(main())
