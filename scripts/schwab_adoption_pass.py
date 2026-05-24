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
from datetime import UTC, datetime, time, timedelta
from pathlib import Path
from typing import Any

import polars as pl

from bhiksha.active_plan.compiler import (
    compile_active_plan_from_rows,
    load_operator_defaults_sheet_rows,
    load_rows_from_sheet_records_with_report,
    load_strategy_catalog_sheet_rows_with_report,
)
from bhiksha.app.replay import (
    ReplaySignalEvaluator,
    ReplayTrade,
    _deployment_hard_flat_time,
    _hard_flat_exit_decision,
    _replay_underlying_entry_price,
)
from bhiksha.execution.thesis_exit import evaluate_underlying_thesis_exit
from bhiksha.market_data.adapters.schwab import SchwabBarSource
from bhiksha.market_data.feature_service import FeatureService
from bhiksha.market_data.session import as_et_time
from bhiksha.state.position_tracker import TrackedPosition
from bhiksha.strategy.registry import default_strategy_registry


AUTH_EXPIRED_TEXT = "refresh token has expired"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact-dir", required=True)
    parser.add_argument("--lookback-days", type=int, default=63)
    parser.add_argument("--max-entry-window-minutes", type=int, default=150)
    parser.add_argument("--max-premium-usd", default="1000")
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


def compile_plan(artifact_dir: Path, *, max_premium_usd: str) -> Any:
    evidence = json.loads((artifact_dir / "evidence_rows.json").read_text(encoding="utf-8"))
    defaults_rows = json.loads((artifact_dir / "operator_defaults_rows.json").read_text(encoding="utf-8"))
    operator_defaults = load_operator_defaults_sheet_rows(defaults_rows)
    catalog_validation = load_strategy_catalog_sheet_rows_with_report(evidence, sheet_name="Mala_Evidence_v1")
    active_payloads = []
    for row in evidence:
        key = str(row.get("catalog_key") or "").strip()
        if not key:
            continue
        active_payloads.append(
            {
                "row_id": "adoption_" + re.sub(r"[^A-Za-z0-9_]+", "_", key)[:180],
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
        google_strategy_catalog=catalog_validation.rows,
        operator_defaults=operator_defaults,
        suppressed=[*catalog_validation.suppressed, *row_validation.suppressed],
    )
    (artifact_dir / "active_plan_all_evidence.json").write_text(
        json.dumps(compiled.plan.model_dump(mode="json"), indent=2, sort_keys=True),
        encoding="utf-8",
    )
    print(f"compiled_deployments={len(compiled.plan.deployments)} suppressed={len(compiled.plan.summary.get('suppressed', []))}")
    return compiled, evidence


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


def build_adoption_rows(
    *,
    compiled: Any,
    evidence: list[dict[str, Any]],
    bars_by_symbol: dict[str, list[Any]],
    max_entry_window_minutes: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    evaluator = ReplaySignalEvaluator(FeatureService(), default_strategy_registry())
    evidence_by_key = {str(row.get("catalog_key")): row for row in evidence}
    rows: list[dict[str, Any]] = []
    trade_rows: list[dict[str, Any]] = []
    for deployment in compiled.plan.deployments:
        metadata = (deployment.source.metadata if deployment.source else {}) or {}
        catalog_key = str(metadata.get("strategy_id") or metadata.get("catalog_key") or deployment.deployment_id.replace("adoption_", ""))
        evidence_row = evidence_by_key.get(catalog_key, {})
        bars = bars_by_symbol.get(deployment.symbol, [])
        base = {
            "catalog_key": catalog_key,
            "deployment_id": deployment.deployment_id,
            "symbol": deployment.symbol,
            "direction": str(evidence_row.get("direction") or ""),
            "strategy_key": deployment.strategy.key,
            "mala_expectancy": str(evidence_row.get("expectancy") or ""),
            "mala_option_adjusted_expectancy_pct": str(evidence_row.get("option_adjusted_expectancy_pct") or ""),
            "mala_signal_count": str(evidence_row.get("signal_count") or ""),
            "schwab_bar_count": len(bars),
        }
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
        if len(trades) < 3:
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


def write_outputs(artifact_dir: Path, rows: list[dict[str, Any]], trade_rows: list[dict[str, Any]], evidence_count: int, deployment_count: int) -> None:
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
    counts: dict[str, int] = {}
    for row in rows:
        counts[str(row["adoption_status"])] = counts.get(str(row["adoption_status"]), 0) + 1
    lines = [
        "# Schwab Adoption Pass",
        "",
        f"- generated_at: `{datetime.now(UTC).isoformat()}`",
        f"- evidence_rows: `{evidence_count}`",
        f"- compiled_deployments: `{deployment_count}`",
        f"- status_counts: `{json.dumps(counts, sort_keys=True)}`",
        "",
        "| status | catalog_key | symbol | strategy | trades | win | avg move % | reason |",
        "|---|---|---|---|---:|---:|---:|---|",
    ]
    order = {"adoption_pass": 0, "adoption_watch": 1, "adoption_blocked": 2}
    for row in sorted(rows, key=lambda item: (order.get(str(item.get("adoption_status", "")), 9), str(item.get("catalog_key", "")))):
        lines.append(
            "| {status} | {catalog_key} | {symbol} | {strategy_key} | {trades} | {win} | {avg} | {reason} |".format(
                status=row.get("adoption_status", ""),
                catalog_key=row.get("catalog_key", ""),
                symbol=row.get("symbol", ""),
                strategy_key=row.get("strategy_key", ""),
                trades=row.get("schwab_trade_count", ""),
                win=row.get("schwab_win_rate", ""),
                avg=row.get("schwab_avg_signed_move_pct", ""),
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
    compiled, evidence = compile_plan(artifact_dir, max_premium_usd=str(args.max_premium_usd))
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
        bars_by_symbol=bars_by_symbol,
        max_entry_window_minutes=args.max_entry_window_minutes,
    )
    write_outputs(artifact_dir, rows, trade_rows, len(evidence), len(compiled.plan.deployments))
    return 0


def main() -> int:
    return asyncio.run(async_main())


if __name__ == "__main__":
    raise SystemExit(main())
