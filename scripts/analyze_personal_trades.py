#!/usr/bin/env python3
"""Build a first personal-trade diagnostic dataset for Mala."""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from statistics import mean, median

from src.personal.trade_ledger import (
    build_round_trips,
    fills_to_dicts,
    load_all_fills,
    round_trips_to_dicts,
    write_dict_csv,
)


DEFAULT_RAW_DIR = Path("data/personal_imports/raw_exports")
DEFAULT_OUT_DIR = Path("data/personal_imports/processed")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Normalize and diagnose personal broker history exports.")
    parser.add_argument("--raw-dir", type=Path, default=DEFAULT_RAW_DIR)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = args.out_dir / stamp
    run_dir.mkdir(parents=True, exist_ok=True)

    fills = load_all_fills(args.raw_dir)
    round_trips = build_round_trips(fills)
    fill_rows = fills_to_dicts(fills)
    round_trip_rows = round_trips_to_dicts(round_trips)

    fills_path = run_dir / "normalized_fills.csv"
    round_trips_path = run_dir / "round_trips.csv"
    replay_anchors_path = run_dir / "mala_replay_anchors.csv"
    report_path = run_dir / "personal_trade_report.md"
    manifest_path = run_dir / "manifest.json"

    write_dict_csv(fills_path, fill_rows)
    write_dict_csv(round_trips_path, round_trip_rows)
    replay_anchor_rows = mala_replay_anchor_rows(round_trip_rows)
    write_dict_csv(replay_anchors_path, replay_anchor_rows)
    report = build_report(fill_rows, round_trip_rows, replay_anchor_rows, fills_path, round_trips_path, replay_anchors_path)
    report_path.write_text(report, encoding="utf-8")
    manifest_path.write_text(
        json.dumps(
            {
                "raw_dir": str(args.raw_dir),
                "fills": len(fill_rows),
                "round_trips": len(round_trip_rows),
                "outputs": {
                    "normalized_fills": str(fills_path),
                    "round_trips": str(round_trips_path),
                    "mala_replay_anchors": str(replay_anchors_path),
                    "report": str(report_path),
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    print(f"fills: {len(fill_rows)}")
    print(f"round_trips: {len(round_trip_rows)}")
    print(f"mala_replay_anchors: {len(replay_anchor_rows)}")
    print(f"run_dir: {run_dir}")
    print(f"report: {report_path}")


def build_report(
    fills: list[dict[str, object]],
    round_trips: list[dict[str, object]],
    replay_anchors: list[dict[str, object]],
    fills_path: Path,
    round_trips_path: Path,
    replay_anchors_path: Path,
) -> str:
    option_fills = [row for row in fills if row.get("security_type") == "OPTION"]
    timed_fills = [row for row in option_fills if row.get("has_intraday_time")]
    date_only_fills = [row for row in option_fills if not row.get("has_intraday_time")]
    completed_timed = [row for row in round_trips if row.get("has_intraday_time")]

    lines = [
        "# Personal Trade Diagnostic",
        "",
        "Research-only diagnostic for Mala. This is not a live-trading signal and should not be used to route orders.",
        "",
        "## Data Coverage",
        "",
        f"- Normalized fills: {len(fills):,}",
        f"- Option fills: {len(option_fills):,}",
        f"- Intraday-timestamped option fills: {len(timed_fills):,}",
        f"- Date-only option fills: {len(date_only_fills):,}",
        f"- Completed FIFO round trips: {len(round_trips):,}",
        f"- Completed round trips with intraday timing: {len(completed_timed):,}",
        f"- Mala replay anchors in 09:30-10:15 ET window: {len(replay_anchors):,}",
        "",
        "## Outputs",
        "",
        f"- `{fills_path}`",
        f"- `{round_trips_path}`",
        f"- `{replay_anchors_path}`",
        "",
        "## Source Mix",
        "",
    ]
    lines.extend(_markdown_table(counter_rows(Counter(str(row.get("source")) for row in fills), "source", "fills")))
    lines += [
        "",
        "## Data Quality",
        "",
    ]
    lines.extend(_markdown_table(data_quality_rows(fills)))
    lines += [
        "",
        "- Public API and thinkorswim statement rows are replay-grade because they include execution timestamps.",
        "- Schwab transaction CSV rows remain useful for broad P&L and symbol coverage, but are date-only in this export.",
        "- thinkorswim trade-history rows are leg-level and timestamped; per-leg fees are not present there, so the replay-anchor P&L is before that fee allocation.",
    ]

    if round_trips:
        lines += [
            "",
            "## Round Trip P&L By Source",
            "",
        ]
        lines.extend(_markdown_table(pnl_group_rows(round_trips, "source")))
        lines += [
            "",
            "## Round Trip P&L By Underlying",
            "",
        ]
        lines.extend(_markdown_table(pnl_group_rows(round_trips, "underlying", limit=15)))
        lines += [
            "",
            "## Entry Time Buckets (Timestamped Trades Only)",
            "",
        ]
        lines.extend(_markdown_table(time_bucket_rows(completed_timed)))
        lines += [
            "",
            "## Opening Window Summary (Timestamped Trades Only)",
            "",
        ]
        lines.extend(_markdown_table(opening_window_rows(completed_timed)))
        lines += [
            "",
            "## Opening Window By Underlying (Replay Anchors)",
            "",
        ]
        lines.extend(_markdown_table(pnl_group_rows(replay_anchors, "underlying", limit=15)))
        lines += [
            "",
            "## DTE Buckets",
            "",
        ]
        lines.extend(_markdown_table(dte_bucket_rows(round_trips)))

    lines += [
        "",
        "## Actionable Mala Uses",
        "",
        "1. Use `normalized_fills.csv` as the personal ledger: every actual broker fill becomes a replay anchor.",
        "2. Use `mala_replay_anchors.csv` as the first supervised evaluation set: these are completed, timestamped round trips whose entries were inside 09:30-10:15 ET.",
        "3. Use timestamped Public and thinkorswim fills to build candidate moments: entry timestamp, underlying, option direction, DTE, size, and realized result.",
        "4. In backtests, sample non-trade moments from the same underlyings, dates, and 9:30-10:15 ET window to create the missing `do not trade` class.",
        "5. Compare actual fills against Mala strategy signals by asking: did a strategy fire within +/- N minutes of the human entry, and what was the forward MFE/MAE?",
        "",
        "## Immediate Research Questions",
        "",
        "1. Which entry-time bucket has positive expectancy after fees?",
        "2. Which underlyings dominate both profits and losses?",
        "3. Does performance decay after the first 15, 30, and 45 minutes from market open?",
        "4. Are 0DTE/1DTE trades behaving differently from longer DTE trades?",
        "5. Which losing trades had favorable MFE first, meaning exit policy may matter more than entry policy?",
        "",
    ]
    return "\n".join(lines)


def counter_rows(counter: Counter[str], label: str, count_label: str) -> list[dict[str, object]]:
    return [{label: key, count_label: value} for key, value in counter.most_common()]


def data_quality_rows(fills: list[dict[str, object]]) -> list[dict[str, object]]:
    grouped: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in fills:
        grouped[str(row.get("broker") or "UNKNOWN")].append(row)
    output = []
    for broker, rows in grouped.items():
        timed = [row for row in rows if row.get("has_intraday_time")]
        option_rows = [row for row in rows if row.get("security_type") == "OPTION"]
        output.append({
            "broker": broker,
            "fills": len(rows),
            "option_fills": len(option_rows),
            "timestamped_fills": len(timed),
            "date_only_fills": len(rows) - len(timed),
        })
    output.sort(key=lambda row: int(row["fills"]), reverse=True)
    return output


def mala_replay_anchor_rows(round_trips: list[dict[str, object]]) -> list[dict[str, object]]:
    rows = []
    for row in round_trips:
        if not row.get("has_intraday_time"):
            continue
        minutes = _minutes_from_time(str(row.get("entry_time_et") or ""))
        if minutes is None or not 570 <= minutes <= 615:
            continue
        rows.append({
            "entry_timestamp_et": row.get("opened_at_et"),
            "exit_timestamp_et": row.get("closed_at_et"),
            "source": row.get("source"),
            "account_alias": row.get("account_alias"),
            "underlying": row.get("underlying"),
            "symbol": row.get("symbol"),
            "option_right": row.get("option_right"),
            "strike": row.get("strike"),
            "expiration": row.get("expiration"),
            "dte_at_entry": row.get("dte_at_entry"),
            "opening_side": row.get("opening_side"),
            "quantity": row.get("quantity"),
            "entry_price": row.get("entry_price"),
            "exit_price": row.get("exit_price"),
            "holding_minutes": row.get("holding_minutes"),
            "pnl": row.get("pnl"),
            "return_on_entry_cost": row.get("return_on_entry_cost"),
            "label": "TRADED",
        })
    return rows


def pnl_group_rows(rows: list[dict[str, object]], key: str, limit: int | None = None) -> list[dict[str, object]]:
    grouped: dict[str, list[float]] = defaultdict(list)
    for row in rows:
        group = str(row.get(key) or "UNKNOWN")
        grouped[group].append(float(row.get("pnl") or 0.0))
    output = []
    for group, pnls in grouped.items():
        wins = [pnl for pnl in pnls if pnl > 0]
        output.append({
            key: group,
            "trades": len(pnls),
            "pnl": round(sum(pnls), 2),
            "avg_pnl": round(mean(pnls), 2),
            "median_pnl": round(median(pnls), 2),
            "win_rate": round(len(wins) / len(pnls), 3),
        })
    output.sort(key=lambda row: abs(float(row["pnl"])), reverse=True)
    return output[:limit] if limit else output


def time_bucket_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    grouped: dict[str, list[float]] = defaultdict(list)
    for row in rows:
        bucket = _entry_bucket(str(row.get("entry_time_et") or ""))
        grouped[bucket].append(float(row.get("pnl") or 0.0))
    order = ["09:30-09:44", "09:45-09:59", "10:00-10:14", "10:15-10:44", "after_10:45", "unknown"]
    return _bucket_rows(grouped, order, "entry_bucket")


def opening_window_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    grouped: dict[str, list[float]] = defaultdict(list)
    for row in rows:
        minutes = _minutes_from_time(str(row.get("entry_time_et") or ""))
        if minutes is None:
            bucket = "unknown"
        elif 570 <= minutes <= 615:
            bucket = "09:30-10:15"
        else:
            bucket = "outside_window"
        grouped[bucket].append(float(row.get("pnl") or 0.0))
    return _bucket_rows(grouped, ["09:30-10:15", "outside_window", "unknown"], "entry_window")


def dte_bucket_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    grouped: dict[str, list[float]] = defaultdict(list)
    for row in rows:
        dte = row.get("dte_at_entry")
        if dte in ("", None):
            bucket = "unknown"
        else:
            value = int(dte)
            if value <= 0:
                bucket = "0DTE"
            elif value == 1:
                bucket = "1DTE"
            elif value <= 5:
                bucket = "2-5DTE"
            elif value <= 14:
                bucket = "6-14DTE"
            else:
                bucket = "15+DTE"
        grouped[bucket].append(float(row.get("pnl") or 0.0))
    return _bucket_rows(grouped, ["0DTE", "1DTE", "2-5DTE", "6-14DTE", "15+DTE", "unknown"], "dte_bucket")


def _bucket_rows(grouped: dict[str, list[float]], order: list[str], label: str) -> list[dict[str, object]]:
    output = []
    for bucket in order:
        pnls = grouped.get(bucket, [])
        if not pnls:
            continue
        wins = [pnl for pnl in pnls if pnl > 0]
        output.append({
            label: bucket,
            "trades": len(pnls),
            "pnl": round(sum(pnls), 2),
            "avg_pnl": round(mean(pnls), 2),
            "win_rate": round(len(wins) / len(pnls), 3),
        })
    return output


def _entry_bucket(time_text: str) -> str:
    total = _minutes_from_time(time_text)
    if total is None:
        return "unknown"
    if 570 <= total <= 584:
        return "09:30-09:44"
    if 585 <= total <= 599:
        return "09:45-09:59"
    if 600 <= total <= 614:
        return "10:00-10:14"
    if 615 <= total <= 644:
        return "10:15-10:44"
    return "after_10:45"


def _minutes_from_time(time_text: str) -> int | None:
    if not time_text:
        return None
    hour, minute, *_ = [int(part) for part in time_text.split(":")]
    return hour * 60 + minute


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


if __name__ == "__main__":
    main()
