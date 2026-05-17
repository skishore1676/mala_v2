"""Prepare TradingView MCP review queues for playbook sample events."""

from __future__ import annotations

import argparse
import csv
import math
import shlex
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo


ET = ZoneInfo("America/New_York")
DEFAULT_TRADINGVIEW_MCP_ROOT = "/Users/suman/code/openclaw-core/workspace-main/external/tradingview-mcp"
OLDMAC_TRADINGVIEW_MCP_ROOT = "/Users/sunny/.openclaw/workspace-main/external/tradingview-mcp"

QUEUE_COLUMNS = [
    "rank",
    "symbol",
    "tv_symbol",
    "direction",
    "event_timestamp_utc",
    "event_timestamp_et",
    "timeframe",
    "visible_from_et",
    "visible_to_et",
    "visible_from_unix",
    "visible_to_unix",
    "screenshot_name",
    "screenshot_region",
    "variant_count",
    "pnl_r_min",
    "pnl_r_max",
    "mfe_r_max",
    "mae_r_max",
    "gap_states",
    "exit_families",
    "outcomes",
    "extension_summaries",
]


@dataclass(frozen=True, slots=True)
class TradingViewReviewResult:
    out_dir: Path
    queue_count: int
    queue_csv: Path
    command_file: Path
    receipt: Path


@dataclass(slots=True)
class _EventGroup:
    symbol: str
    direction: str
    timestamp: datetime
    rows: list[dict[str, str]]

    @property
    def key(self) -> tuple[str, str, datetime]:
        return (self.symbol, self.direction, self.timestamp)


def build_tradingview_review(
    run_dir: Path,
    *,
    out_dir: Path | None = None,
    max_events: int = 12,
    timeframe: str = "5",
    minutes_before: int = 20,
    minutes_after: int = 90,
    screenshot_region: str = "full",
    tradingview_mcp_root: str = DEFAULT_TRADINGVIEW_MCP_ROOT,
    tv_symbol_overrides: dict[str, str] | None = None,
) -> TradingViewReviewResult:
    """Build a review queue and TradingView MCP command file from sample events."""

    sample_events = run_dir / "sample_events.csv"
    if not sample_events.exists():
        raise FileNotFoundError(f"sample_events.csv not found under {run_dir}")
    if max_events <= 0:
        raise ValueError("max_events must be positive")
    if minutes_before < 0 or minutes_after <= 0:
        raise ValueError("minutes_before must be >= 0 and minutes_after must be > 0")

    review_dir = out_dir or run_dir / "tradingview_review"
    review_dir.mkdir(parents=True, exist_ok=True)

    groups = _load_groups(sample_events)
    selected = sorted(groups, key=lambda group: group.timestamp, reverse=True)[:max_events]
    rows = [
        _queue_row(
            rank=rank,
            group=group,
            timeframe=timeframe,
            minutes_before=minutes_before,
            minutes_after=minutes_after,
            screenshot_region=screenshot_region,
            tv_symbol_overrides=tv_symbol_overrides or {},
        )
        for rank, group in enumerate(selected, start=1)
    ]

    queue_csv = review_dir / "tradingview_review_queue.csv"
    _write_csv(queue_csv, rows, QUEUE_COLUMNS)
    command_file = review_dir / "tradingview_mcp_commands.sh"
    _write_command_file(command_file, rows)
    receipt = review_dir / "TRADINGVIEW_MCP_REVIEW.md"
    _write_receipt(
        receipt,
        run_dir=run_dir,
        rows=rows,
        command_file=command_file,
        queue_csv=queue_csv,
        tradingview_mcp_root=tradingview_mcp_root,
    )

    return TradingViewReviewResult(
        out_dir=review_dir,
        queue_count=len(rows),
        queue_csv=queue_csv,
        command_file=command_file,
        receipt=receipt,
    )


def _load_groups(sample_events: Path) -> list[_EventGroup]:
    groups: dict[tuple[str, str, datetime], list[dict[str, str]]] = {}
    with sample_events.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            symbol = row.get("symbol", "").strip().upper()
            direction = row.get("direction", "").strip().lower()
            raw_ts = row.get("event_timestamp", "").strip()
            if not symbol or not direction or not raw_ts:
                continue
            timestamp = _parse_timestamp(raw_ts)
            groups.setdefault((symbol, direction, timestamp), []).append(row)

    return [
        _EventGroup(symbol=symbol, direction=direction, timestamp=timestamp, rows=rows)
        for (symbol, direction, timestamp), rows in groups.items()
    ]


def _queue_row(
    *,
    rank: int,
    group: _EventGroup,
    timeframe: str,
    minutes_before: int,
    minutes_after: int,
    screenshot_region: str,
    tv_symbol_overrides: dict[str, str],
) -> dict[str, str]:
    event_et = group.timestamp.astimezone(ET)
    visible_from = event_et - timedelta(minutes=minutes_before)
    visible_to = event_et + timedelta(minutes=minutes_after)
    tv_symbol = tv_symbol_overrides.get(group.symbol, group.symbol)
    screenshot_name = _screenshot_name(group.symbol, group.direction, event_et, timeframe)
    pnl_values = [_safe_float(row.get("pnl_r")) for row in group.rows]
    mfe_values = [_safe_float(row.get("max_favorable_excursion_r")) for row in group.rows]
    mae_values = [_safe_float(row.get("max_adverse_excursion_r")) for row in group.rows]
    return {
        "rank": str(rank),
        "symbol": group.symbol,
        "tv_symbol": tv_symbol,
        "direction": group.direction,
        "event_timestamp_utc": group.timestamp.astimezone(UTC).isoformat(timespec="seconds"),
        "event_timestamp_et": event_et.isoformat(timespec="seconds"),
        "timeframe": timeframe,
        "visible_from_et": visible_from.isoformat(timespec="seconds"),
        "visible_to_et": visible_to.isoformat(timespec="seconds"),
        "visible_from_unix": str(int(visible_from.timestamp())),
        "visible_to_unix": str(int(visible_to.timestamp())),
        "screenshot_name": screenshot_name,
        "screenshot_region": screenshot_region,
        "variant_count": str(len(group.rows)),
        "pnl_r_min": _format_float(_min(pnl_values)),
        "pnl_r_max": _format_float(_max(pnl_values)),
        "mfe_r_max": _format_float(_max(mfe_values)),
        "mae_r_max": _format_float(_max(mae_values)),
        "gap_states": _join_unique(row.get("gap_state", "") for row in group.rows),
        "exit_families": _join_unique(row.get("exit_family", "") for row in group.rows),
        "outcomes": _join_unique(row.get("outcome_label", "") for row in group.rows),
        "extension_summaries": _join_unique(row.get("extension_summary", "") for row in group.rows),
    }


def _write_command_file(path: Path, rows: list[dict[str, str]]) -> None:
    lines = [
        "#!/usr/bin/env bash",
        "set -euo pipefail",
        "",
        ': "${TRADINGVIEW_CDP_HOST:=127.0.0.1}"',
        ': "${TRADINGVIEW_CDP_PORT:=9223}"',
        ': "${TRADINGVIEW_MCP_ROOT:?set TRADINGVIEW_MCP_ROOT to the tradingview-mcp checkout}"',
        "",
        'cd "$TRADINGVIEW_MCP_ROOT"',
        "export TRADINGVIEW_CDP_HOST TRADINGVIEW_CDP_PORT",
        'npm run -s tv -- status',
        "",
    ]
    for row in rows:
        event = row["event_timestamp_et"]
        screenshot = row["screenshot_name"]
        lines.extend(
            [
                f"# rank {row['rank']}: {row['symbol']} {row['direction']} {event}",
                f"npm run -s tv -- symbol {_quote(row['tv_symbol'])}",
                f"npm run -s tv -- timeframe {_quote(row['timeframe'])}",
                "npm run -s tv -- type Candles",
                f"npm run -s tv -- scroll {_quote(event)}",
                (
                    "npm run -s tv -- range "
                    f"--from {_quote(row['visible_from_unix'])} --to {_quote(row['visible_to_unix'])}"
                ),
                f"npm run -s tv -- screenshot -r {_quote(row['screenshot_region'])} -o {_quote(screenshot)}",
                "",
            ]
        )
    path.write_text("\n".join(lines), encoding="utf-8")
    path.chmod(0o755)


def _write_receipt(
    path: Path,
    *,
    run_dir: Path,
    rows: list[dict[str, str]],
    command_file: Path,
    queue_csv: Path,
    tradingview_mcp_root: str,
) -> None:
    lines = [
        "# TradingView MCP Review Queue",
        "",
        f"- source run: `{run_dir}`",
        f"- queue csv: `{queue_csv}`",
        f"- command file: `{command_file}`",
        f"- queued events: `{len(rows)}`",
        "",
        "This is a visual-review bridge only. Mala remains the source of research",
        "events and outcomes; TradingView MCP is used to navigate the trader chart",
        "surface and capture screenshots for human review.",
        "",
        "## Run",
        "",
        "From this repo checkout, against the local TradingView Desktop:",
        "",
        "```bash",
        f"export TRADINGVIEW_MCP_ROOT={tradingview_mcp_root}",
        "export TRADINGVIEW_CDP_HOST=127.0.0.1",
        "export TRADINGVIEW_CDP_PORT=9223",
        f"{command_file}",
        "```",
        "",
        "Oldmac fallback, if you intentionally want to drive that machine:",
        "",
        "```bash",
        f"ssh oldmac 'TRADINGVIEW_MCP_ROOT={OLDMAC_TRADINGVIEW_MCP_ROOT} TRADINGVIEW_CDP_HOST=127.0.0.1 TRADINGVIEW_CDP_PORT=9223 bash -s' \\",
        f"  < {command_file}",
        "```",
        "",
        "If TradingView has not loaded older intraday bars yet, the MCP range command",
        "may stay inside the currently loaded chart history. If screenshots show a",
        "blank chart canvas, repair or reload the TradingView layout first; the queue",
        "row is still the review target, but the captured image is not evidence.",
        "",
        "## Events",
        "",
    ]
    for row in rows:
        lines.append(
            "- rank {rank}: {symbol} {direction} {event} "
            "variants={variant_count}, pnl_r={pnl_r_min}..{pnl_r_max}, "
            "mfe_max={mfe_r_max}, mae_max={mae_r_max}, exits={exit_families}".format(
                rank=row["rank"],
                symbol=row["symbol"],
                direction=row["direction"],
                event=row["event_timestamp_et"],
                variant_count=row["variant_count"],
                pnl_r_min=row["pnl_r_min"],
                pnl_r_max=row["pnl_r_max"],
                mfe_r_max=row["mfe_r_max"],
                mae_r_max=row["mae_r_max"],
                exit_families=row["exit_families"],
            )
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_csv(path: Path, rows: list[dict[str, str]], columns: list[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def _parse_timestamp(raw: str) -> datetime:
    timestamp = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    if timestamp.tzinfo is None:
        return timestamp.replace(tzinfo=UTC)
    return timestamp.astimezone(UTC)


def _safe_float(raw: str | None) -> float | None:
    if raw in (None, ""):
        return None
    try:
        value = float(raw)
    except ValueError:
        return None
    if math.isnan(value) or math.isinf(value):
        return None
    return value


def _min(values: list[float | None]) -> float | None:
    cleaned = [value for value in values if value is not None]
    return min(cleaned) if cleaned else None


def _max(values: list[float | None]) -> float | None:
    cleaned = [value for value in values if value is not None]
    return max(cleaned) if cleaned else None


def _format_float(value: float | None) -> str:
    if value is None:
        return ""
    return f"{value:.4f}".rstrip("0").rstrip(".")


def _join_unique(values: object) -> str:
    unique = sorted({str(value).strip() for value in values if str(value).strip()})
    return " | ".join(unique)


def _screenshot_name(symbol: str, direction: str, event_et: datetime, timeframe: str) -> str:
    stamp = event_et.strftime("%Y-%m-%d_%H%M_et")
    safe_timeframe = "".join(ch for ch in timeframe if ch.isalnum())
    return f"mala_playbook_{symbol.lower()}_{direction}_{stamp}_{safe_timeframe}m"


def _quote(value: str) -> str:
    return shlex.quote(value)


def _parse_symbol_overrides(raw_values: list[str]) -> dict[str, str]:
    overrides: dict[str, str] = {}
    for raw in raw_values:
        if "=" not in raw:
            raise ValueError(f"Symbol override must be SYMBOL=TV_SYMBOL, got {raw!r}")
        symbol, tv_symbol = raw.split("=", 1)
        overrides[symbol.strip().upper()] = tv_symbol.strip()
    return overrides


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", required=True, type=Path, help="Playbook surface run directory")
    parser.add_argument("--out-dir", type=Path, help="Optional output directory")
    parser.add_argument("--max-events", type=int, default=12)
    parser.add_argument("--timeframe", default="5", help="TradingView timeframe, e.g. 5, 15, 60")
    parser.add_argument("--minutes-before", type=int, default=20)
    parser.add_argument("--minutes-after", type=int, default=90)
    parser.add_argument(
        "--tradingview-mcp-root",
        default=DEFAULT_TRADINGVIEW_MCP_ROOT,
        help="Local tradingview-mcp checkout used in the generated receipt.",
    )
    parser.add_argument(
        "--screenshot-region",
        default="full",
        choices=["full", "chart", "strategy_tester"],
        help="TradingView MCP screenshot region. full is the safest current default.",
    )
    parser.add_argument(
        "--tv-symbol",
        action="append",
        default=[],
        help="Optional mapping like IWM=NYSEARCA:IWM. Repeatable.",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    result = build_tradingview_review(
        args.run_dir,
        out_dir=args.out_dir,
        max_events=args.max_events,
        timeframe=args.timeframe,
        minutes_before=args.minutes_before,
        minutes_after=args.minutes_after,
        screenshot_region=args.screenshot_region,
        tradingview_mcp_root=args.tradingview_mcp_root,
        tv_symbol_overrides=_parse_symbol_overrides(args.tv_symbol),
    )
    print(f"OUT_DIR={result.out_dir}")
    print(f"QUEUE_EVENTS={result.queue_count}")
    print(f"QUEUE_CSV={result.queue_csv}")
    print(f"COMMAND_FILE={result.command_file}")
    print(f"RECEIPT={result.receipt}")


if __name__ == "__main__":
    main()
