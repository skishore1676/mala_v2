"""Evaluate personal replay anchors against Mala strategies and oracle bars."""

from __future__ import annotations

import csv
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from statistics import mean, median
from typing import Any

import polars as pl

from src.chronos.storage import LocalStorage
from src.newton.engine import PhysicsEngine
from src.strategy.base import BaseStrategy, required_feature_union
from src.strategy.factory import available_strategy_names, build_strategy_by_name


@dataclass(slots=True)
class ReplayAnchor:
    entry_timestamp_et: datetime
    exit_timestamp_et: datetime | None
    source: str
    account_alias: str
    underlying: str
    symbol: str
    option_right: str
    strike: str
    expiration: str
    dte_at_entry: int | None
    opening_side: str
    quantity: float
    entry_price: float | None
    exit_price: float | None
    holding_minutes: float | None
    pnl: float
    return_on_entry_cost: float | None
    label: str

    @property
    def trade_date(self) -> date:
        return self.entry_timestamp_et.date()

    @property
    def underlying_direction(self) -> str:
        if (self.option_right == "C" and self.opening_side == "LONG") or (
            self.option_right == "P" and self.opening_side == "SHORT"
        ):
            return "long"
        if (self.option_right == "P" and self.opening_side == "LONG") or (
            self.option_right == "C" and self.opening_side == "SHORT"
        ):
            return "short"
        return "unknown"


def load_replay_anchors(path: Path) -> list[ReplayAnchor]:
    with path.open(newline="") as handle:
        return [_anchor_from_row(row) for row in csv.DictReader(handle)]


def evaluate_replay_anchors(
    anchors: list[ReplayAnchor],
    *,
    storage: LocalStorage | None = None,
    strategy_names: list[str] | None = None,
    signal_tolerance_minutes: int = 5,
    oracle_windows: tuple[int, ...] = (15, 30, 60),
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    storage = storage or LocalStorage()
    strategies = [build_strategy_by_name(name) for name in (strategy_names or list(available_strategy_names()))]
    frames = load_required_frames(anchors, strategies, storage=storage)
    day_frame_cache: dict[tuple[str, date], pl.DataFrame] = {}
    strategy_signal_cache: dict[tuple[str, str, date], dict[str, object]] = {}
    rows: list[dict[str, object]] = []

    for anchor in anchors:
        frame = frames.get(anchor.underlying)
        if frame is None or frame.is_empty():
            rows.append(_missing_row(anchor, reason="missing_bars"))
            continue
        day_cache_key = (anchor.underlying, anchor.trade_date)
        day_frame = day_frame_cache.get(day_cache_key)
        if day_frame is None:
            day_frame = _same_et_date(frame, anchor.trade_date)
            day_frame_cache[day_cache_key] = day_frame
        rows.append(
            evaluate_anchor(
                anchor,
                day_frame,
                strategies=strategies,
                signal_tolerance_minutes=signal_tolerance_minutes,
                oracle_windows=oracle_windows,
                strategy_signal_cache=strategy_signal_cache,
            )
        )

    return rows, build_summary_rows(rows, strategies)


def load_required_frames(
    anchors: list[ReplayAnchor],
    strategies: list[BaseStrategy],
    *,
    storage: LocalStorage,
) -> dict[str, pl.DataFrame]:
    physics = PhysicsEngine()
    required = required_feature_union(strategies)
    frames: dict[str, pl.DataFrame] = {}
    by_ticker: dict[str, list[ReplayAnchor]] = defaultdict(list)
    for anchor in anchors:
        by_ticker[anchor.underlying].append(anchor)

    for ticker, ticker_anchors in by_ticker.items():
        start = min(anchor.trade_date for anchor in ticker_anchors) - timedelta(days=2)
        end = max(anchor.trade_date for anchor in ticker_anchors) + timedelta(days=2)
        raw = storage.load_bars(ticker, start, end)
        if raw.is_empty():
            continue
        frames[ticker] = physics.enrich_for_features(raw, required)
    return frames


def evaluate_anchor(
    anchor: ReplayAnchor,
    frame: pl.DataFrame,
    *,
    strategies: list[BaseStrategy],
    signal_tolerance_minutes: int,
    oracle_windows: tuple[int, ...],
    strategy_signal_cache: dict[tuple[str, str, date], dict[str, object]] | None = None,
) -> dict[str, object]:
    day_frame = frame
    if not day_frame.is_empty():
        first_ts = day_frame["timestamp"][0]
        if first_ts.astimezone(anchor.entry_timestamp_et.tzinfo).date() != anchor.trade_date:
            day_frame = _same_et_date(frame, anchor.trade_date)

    anchor_bar = _bar_at_or_before(day_frame, anchor.entry_timestamp_et)
    if anchor_bar is None:
        return _missing_row(anchor, reason="missing_entry_bar")

    anchor_idx, anchor_values = anchor_bar
    direction = anchor.underlying_direction
    row: dict[str, object] = {
        **_anchor_base_row(anchor),
        "status": "evaluated",
        "entry_bar_timestamp": anchor_values["timestamp"],
        "underlying_entry_close": anchor_values["close"],
    }
    row.update(_oracle_metrics(day_frame, anchor_idx, direction, oracle_windows))

    matching_names: list[str] = []
    direction_matching_names: list[str] = []
    for strategy in strategies:
        strategy_result = _evaluate_strategy_near_anchor(
            strategy,
            day_frame,
            ticker=anchor.underlying,
            trade_date=anchor.trade_date,
            anchor_idx=anchor_idx,
            anchor_direction=direction,
            tolerance_minutes=signal_tolerance_minutes,
            strategy_signal_cache=strategy_signal_cache,
        )
        signal_key = _strategy_key(strategy.name)
        row[f"{signal_key}_near_signal"] = strategy_result["near_signal"]
        row[f"{signal_key}_direction_match"] = strategy_result["direction_match"]
        row[f"{signal_key}_nearest_signal_delta_min"] = strategy_result["nearest_signal_delta_min"]
        if strategy_result["near_signal"]:
            matching_names.append(strategy.name)
        if strategy_result["direction_match"]:
            direction_matching_names.append(strategy.name)

    row["any_strategy_near_signal"] = bool(matching_names)
    row["any_strategy_direction_match"] = bool(direction_matching_names)
    row["matching_strategies"] = "; ".join(matching_names)
    row["direction_matching_strategies"] = "; ".join(direction_matching_names)
    return row


def build_summary_rows(rows: list[dict[str, object]], strategies: list[BaseStrategy]) -> list[dict[str, object]]:
    evaluated = [row for row in rows if row.get("status") == "evaluated"]
    output = [_summary_row("all", evaluated)]

    by_underlying: dict[str, list[dict[str, object]]] = defaultdict(list)
    by_source: dict[str, list[dict[str, object]]] = defaultdict(list)
    by_direction: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in evaluated:
        by_underlying[str(row.get("underlying") or "UNKNOWN")].append(row)
        by_source[str(row.get("source") or "UNKNOWN")].append(row)
        by_direction[str(row.get("underlying_direction") or "UNKNOWN")].append(row)

    for key, grouped in sorted(by_underlying.items(), key=lambda item: len(item[1]), reverse=True):
        output.append(_summary_row(f"underlying:{key}", grouped))
    for key, grouped in sorted(by_source.items(), key=lambda item: len(item[1]), reverse=True):
        output.append(_summary_row(f"source:{key}", grouped))
    for key, grouped in sorted(by_direction.items(), key=lambda item: len(item[1]), reverse=True):
        output.append(_summary_row(f"direction:{key}", grouped))

    for strategy in strategies:
        signal_key = _strategy_key(strategy.name)
        near_rows = [row for row in evaluated if row.get(f"{signal_key}_near_signal")]
        direction_rows = [row for row in evaluated if row.get(f"{signal_key}_direction_match")]
        base = _summary_row(f"strategy_near:{strategy.name}", near_rows)
        base["evaluated_trades"] = len(evaluated)
        base["coverage_rate"] = round(len(near_rows) / len(evaluated), 4) if evaluated else 0.0
        base["direction_match_rate"] = round(len(direction_rows) / len(evaluated), 4) if evaluated else 0.0
        output.append(base)
    return output


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames: list[str] = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def build_markdown_report(
    *,
    rows: list[dict[str, object]],
    summary_rows: list[dict[str, object]],
    detail_path: Path,
    summary_path: Path,
) -> str:
    evaluated = [row for row in rows if row.get("status") == "evaluated"]
    missing = [row for row in rows if row.get("status") != "evaluated"]
    lines = [
        "# Personal Replay Evaluation",
        "",
        "Research-only comparison of personal opening-window trades against cached Mala bars, strategy signals, and forward oracle movement.",
        "",
        "## Coverage",
        "",
        f"- Replay anchors: {len(rows):,}",
        f"- Evaluated with bars: {len(evaluated):,}",
        f"- Missing bars/entry bar: {len(missing):,}",
        "",
        "## Outputs",
        "",
        f"- `{detail_path}`",
        f"- `{summary_path}`",
        "",
        "## Top Summaries",
        "",
    ]
    lines.extend(_markdown_table(summary_rows[:16]))
    strategy_rows = [row for row in summary_rows if str(row.get("segment", "")).startswith("strategy_near:")]
    if strategy_rows:
        lines += [
            "",
            "## Strategy Overlap",
            "",
        ]
        lines.extend(
            _markdown_table(
                [
                    {
                        "strategy": str(row["segment"]).replace("strategy_near:", ""),
                        "matched_trades": row["trades"],
                        "coverage_rate": row.get("coverage_rate", ""),
                        "direction_match_rate": row.get("direction_match_rate", ""),
                        "matched_pnl": row["pnl"],
                        "matched_avg_pnl": row["avg_pnl"],
                        "matched_median_oracle_r_30m": row["median_oracle_r_30m"],
                    }
                    for row in strategy_rows
                ]
            )
        )
    lines += [
        "",
        "## Interpretation",
        "",
        "1. High `coverage_rate` for a strategy means it often fired near your actual entry timestamp.",
        "2. High `direction_match_rate` is stronger: the strategy fired near the entry and agreed with the option-implied underlying direction.",
        "3. Positive oracle medians with weak realized P&L suggest exit/sizing may matter more than entry.",
        "4. Strong realized P&L with weak strategy coverage suggests Mala does not yet encode the human pattern.",
        "5. The next dataset should add matched `NO_TRADE` rows from the same symbols, dates, and 09:30-10:15 ET window.",
        "",
    ]
    if missing:
        reasons = Counter(str(row.get("reason")) for row in missing)
        lines += ["## Missing Data", ""]
        lines.extend(_markdown_table([{"reason": key, "anchors": value} for key, value in reasons.most_common()]))
        lines.append("")
    return "\n".join(lines)


def _evaluate_strategy_near_anchor(
    strategy: BaseStrategy,
    day_frame: pl.DataFrame,
    *,
    ticker: str,
    trade_date: date,
    anchor_idx: int,
    anchor_direction: str,
    tolerance_minutes: int,
    strategy_signal_cache: dict[tuple[str, str, date], dict[str, object]] | None = None,
) -> dict[str, object]:
    cache_key = (strategy.name, ticker, trade_date)
    cached = strategy_signal_cache.get(cache_key) if strategy_signal_cache is not None else None
    if cached is None:
        try:
            cached = {"frame": strategy.generate_signals(day_frame.clone()), "error": ""}
        except Exception as exc:
            cached = {"frame": None, "error": str(exc)}
        if strategy_signal_cache is not None:
            strategy_signal_cache[cache_key] = cached
    if cached.get("error"):
        return {"near_signal": False, "direction_match": False, "nearest_signal_delta_min": "", "error": cached["error"]}
    signaled = cached.get("frame")
    assert isinstance(signaled, pl.DataFrame)
    if "signal" not in signaled.columns:
        return {"near_signal": False, "direction_match": False, "nearest_signal_delta_min": "", "error": "missing_signal_col"}

    start_idx = max(0, anchor_idx - tolerance_minutes)
    end_idx = min(signaled.height - 1, anchor_idx + tolerance_minutes)
    window = signaled.slice(start_idx, end_idx - start_idx + 1).with_row_index("_window_idx", offset=start_idx)
    signals = window.filter(pl.col("signal"))
    if signals.is_empty():
        return {"near_signal": False, "direction_match": False, "nearest_signal_delta_min": "", "error": ""}

    offsets = [abs(int(idx) - anchor_idx) for idx in signals["_window_idx"].to_list()]
    nearest_delta = min(offsets)
    direction_match = False
    if "signal_direction" in signals.columns and anchor_direction in {"long", "short"}:
        direction_match = anchor_direction in set(str(value) for value in signals["signal_direction"].drop_nulls().to_list())
    return {
        "near_signal": True,
        "direction_match": direction_match,
        "nearest_signal_delta_min": nearest_delta,
        "error": "",
    }


def _oracle_metrics(
    day_frame: pl.DataFrame,
    anchor_idx: int,
    direction: str,
    windows: tuple[int, ...],
) -> dict[str, object]:
    close = day_frame["close"].to_list()
    high = day_frame["high"].to_list()
    low = day_frame["low"].to_list()
    entry = float(close[anchor_idx])
    output: dict[str, object] = {}
    for window in windows:
        end_idx = min(anchor_idx + window, len(close) - 1)
        if end_idx <= anchor_idx:
            output[f"oracle_mfe_{window}m"] = ""
            output[f"oracle_mae_{window}m"] = ""
            output[f"oracle_r_{window}m"] = ""
            continue
        future_high = max(float(value) for value in high[anchor_idx + 1 : end_idx + 1])
        future_low = min(float(value) for value in low[anchor_idx + 1 : end_idx + 1])
        if direction == "short":
            mfe = entry - future_low
            mae = future_high - entry
        else:
            mfe = future_high - entry
            mae = entry - future_low
        output[f"oracle_mfe_{window}m"] = round(mfe, 4)
        output[f"oracle_mae_{window}m"] = round(mae, 4)
        output[f"oracle_r_{window}m"] = round(mfe / mae, 4) if mae > 0 else ""
    return output


def _same_et_date(frame: pl.DataFrame, trade_date: date) -> pl.DataFrame:
    return frame.filter(pl.col("timestamp").dt.convert_time_zone("America/New_York").dt.date() == trade_date)


def _bar_at_or_before(frame: pl.DataFrame, timestamp_et: datetime) -> tuple[int, dict[str, Any]] | None:
    if frame.is_empty():
        return None
    timestamps = frame["timestamp"].to_list()
    timestamp_utc = timestamp_et.astimezone(UTC)
    best: tuple[int, dict[str, Any]] | None = None
    for idx, ts in enumerate(timestamps):
        if ts <= timestamp_utc:
            best = (idx, frame.row(idx, named=True))
        else:
            break
    return best


def _anchor_from_row(row: dict[str, str]) -> ReplayAnchor:
    return ReplayAnchor(
        entry_timestamp_et=datetime.fromisoformat(row["entry_timestamp_et"]),
        exit_timestamp_et=_datetime_or_none(row.get("exit_timestamp_et")),
        source=row.get("source", ""),
        account_alias=row.get("account_alias", ""),
        underlying=row.get("underlying", "").upper(),
        symbol=row.get("symbol", ""),
        option_right=row.get("option_right", ""),
        strike=row.get("strike", ""),
        expiration=row.get("expiration", ""),
        dte_at_entry=_int_or_none(row.get("dte_at_entry")),
        opening_side=row.get("opening_side", ""),
        quantity=_float_or_zero(row.get("quantity")),
        entry_price=_float_or_none(row.get("entry_price")),
        exit_price=_float_or_none(row.get("exit_price")),
        holding_minutes=_float_or_none(row.get("holding_minutes")),
        pnl=_float_or_zero(row.get("pnl")),
        return_on_entry_cost=_float_or_none(row.get("return_on_entry_cost")),
        label=row.get("label", ""),
    )


def _anchor_base_row(anchor: ReplayAnchor) -> dict[str, object]:
    return {
        "entry_timestamp_et": anchor.entry_timestamp_et.isoformat(),
        "exit_timestamp_et": anchor.exit_timestamp_et.isoformat() if anchor.exit_timestamp_et else "",
        "source": anchor.source,
        "account_alias": anchor.account_alias,
        "underlying": anchor.underlying,
        "symbol": anchor.symbol,
        "option_right": anchor.option_right,
        "strike": anchor.strike,
        "expiration": anchor.expiration,
        "dte_at_entry": anchor.dte_at_entry,
        "opening_side": anchor.opening_side,
        "underlying_direction": anchor.underlying_direction,
        "quantity": anchor.quantity,
        "entry_price": anchor.entry_price,
        "exit_price": anchor.exit_price,
        "holding_minutes": anchor.holding_minutes,
        "pnl": anchor.pnl,
        "return_on_entry_cost": anchor.return_on_entry_cost,
        "label": anchor.label,
    }


def _missing_row(anchor: ReplayAnchor, *, reason: str) -> dict[str, object]:
    return {**_anchor_base_row(anchor), "status": "missing", "reason": reason}


def _summary_row(name: str, rows: list[dict[str, object]]) -> dict[str, object]:
    pnls = [float(row.get("pnl") or 0.0) for row in rows]
    wins = [value for value in pnls if value > 0]
    oracle_r = [
        float(row["oracle_r_30m"])
        for row in rows
        if row.get("oracle_r_30m") not in ("", None)
    ]
    near = [row for row in rows if row.get("any_strategy_near_signal")]
    direction = [row for row in rows if row.get("any_strategy_direction_match")]
    return {
        "segment": name,
        "trades": len(rows),
        "pnl": round(sum(pnls), 2) if rows else 0.0,
        "avg_pnl": round(mean(pnls), 2) if rows else 0.0,
        "median_pnl": round(median(pnls), 2) if rows else 0.0,
        "win_rate": round(len(wins) / len(rows), 4) if rows else 0.0,
        "median_oracle_r_30m": round(median(oracle_r), 4) if oracle_r else "",
        "any_strategy_near_rate": round(len(near) / len(rows), 4) if rows else 0.0,
        "any_strategy_direction_match_rate": round(len(direction) / len(rows), 4) if rows else 0.0,
    }


def _strategy_key(name: str) -> str:
    return "".join(char.lower() if char.isalnum() else "_" for char in name).strip("_")


def _markdown_table(rows: list[dict[str, object]]) -> list[str]:
    if not rows:
        return ["No rows."]
    headers = list(rows[0].keys())
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(str(row.get(header, "")) for header in headers) + " |")
    return lines


def _datetime_or_none(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None


def _float_or_none(value: str | None) -> float | None:
    if value in (None, ""):
        return None
    return float(value)


def _float_or_zero(value: str | None) -> float:
    parsed = _float_or_none(value)
    return parsed if parsed is not None else 0.0


def _int_or_none(value: str | None) -> int | None:
    if value in (None, ""):
        return None
    return int(float(value))
