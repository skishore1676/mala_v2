"""Render lightweight chart-review artifacts for playbook sample events."""

from __future__ import annotations

import argparse
import csv
import html
import math
from dataclasses import dataclass
from datetime import date, datetime, time
from pathlib import Path
from statistics import mean
from typing import Any
from zoneinfo import ZoneInfo

import polars as pl

from src.config import DATA_DIR


ET = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")
WIDTH = 1100
HEIGHT = 560
LEFT = 70
RIGHT = 24
TOP = 44
BOTTOM = 68


@dataclass(frozen=True, slots=True)
class ReviewEvent:
    symbol: str
    direction: str
    event_ts_utc: datetime
    event_date_et: date
    event_time_et: time
    pnl_min: float
    pnl_max: float
    pnl_avg: float
    mfe_max: float
    mae_max: float
    variants: int
    gap_state: str
    extension_summary: str
    exit_families: str
    stop_prices: tuple[float, ...]
    entry_prices: tuple[float, ...]
    exit_prices: tuple[float, ...]


def render_chart_review(
    *,
    sample_events_csv: Path,
    out_dir: Path,
    data_dir: Path = DATA_DIR,
    max_charts: int = 8,
) -> list[Path]:
    events = _load_events(sample_events_csv)
    grouped = _group_events(events)
    selected_keys = _select_day_symbol_keys(grouped, max_charts=max_charts)
    charts_dir = out_dir / "charts"
    charts_dir.mkdir(parents=True, exist_ok=True)

    rendered: list[Path] = []
    for key in selected_keys:
        symbol, event_date = key
        bars = _load_day_bars(data_dir=data_dir, symbol=symbol, event_date=event_date)
        if bars.is_empty():
            continue
        chart_events = sorted(grouped[key], key=lambda event: event.event_ts_utc)
        path = charts_dir / f"{event_date.isoformat()}_{symbol}.svg"
        path.write_text(_render_svg(symbol, event_date, bars, chart_events), encoding="utf-8")
        rendered.append(path)

    _write_index(out_dir / "REVIEW_INDEX.md", rendered, grouped)
    return rendered


def _load_events(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _group_events(rows: list[dict[str, str]]) -> dict[tuple[str, date], list[ReviewEvent]]:
    raw: dict[tuple[str, str, str], list[dict[str, str]]] = {}
    for row in rows:
        symbol = str(row.get("symbol", "")).strip().upper()
        direction = str(row.get("direction", "")).strip().lower()
        timestamp = str(row.get("event_timestamp", "")).strip()
        if not symbol or direction not in {"long", "short"} or not timestamp:
            continue
        raw.setdefault((symbol, direction, timestamp), []).append(row)

    grouped: dict[tuple[str, date], list[ReviewEvent]] = {}
    for (symbol, direction, timestamp), items in raw.items():
        event_ts = _parse_ts(timestamp)
        event_et = event_ts.astimezone(ET)
        pnls = [_float(item.get("pnl_r")) for item in items]
        mfes = [_float(item.get("max_favorable_excursion_r")) for item in items]
        maes = [_float(item.get("max_adverse_excursion_r")) for item in items]
        entries = sorted({_float(item.get("entry_reference_price")) for item in items})
        stops = sorted({_float(item.get("stop_reference_price")) for item in items})
        exits = sorted({_float(item.get("exit_reference_price")) for item in items})
        clean_pnls = [value for value in pnls if value is not None]
        event = ReviewEvent(
            symbol=symbol,
            direction=direction,
            event_ts_utc=event_ts,
            event_date_et=event_et.date(),
            event_time_et=event_et.time().replace(second=0, microsecond=0),
            pnl_min=min(clean_pnls) if clean_pnls else 0.0,
            pnl_max=max(clean_pnls) if clean_pnls else 0.0,
            pnl_avg=mean(clean_pnls) if clean_pnls else 0.0,
            mfe_max=max([value for value in mfes if value is not None], default=0.0),
            mae_max=max([value for value in maes if value is not None], default=0.0),
            variants=len(items),
            gap_state=next((str(item.get("gap_state", "")) for item in items if item.get("gap_state")), ""),
            extension_summary=" | ".join(
                sorted({str(item.get("extension_summary", "")) for item in items if item.get("extension_summary")})
            ),
            exit_families=",".join(
                sorted({str(item.get("exit_family", "")) for item in items if item.get("exit_family")})
            ),
            stop_prices=tuple(value for value in stops if value is not None),
            entry_prices=tuple(value for value in entries if value is not None),
            exit_prices=tuple(value for value in exits if value is not None),
        )
        grouped.setdefault((symbol, event.event_date_et), []).append(event)
    return grouped


def _select_day_symbol_keys(
    grouped: dict[tuple[str, date], list[ReviewEvent]],
    *,
    max_charts: int,
) -> list[tuple[str, date]]:
    scored: list[tuple[tuple[str, date], float]] = []
    for key, events in grouped.items():
        latest = max(event.event_ts_utc for event in events).timestamp()
        variant_count = sum(event.variants for event in events)
        magnitude = sum(abs(event.pnl_avg) + event.mfe_max for event in events)
        score = latest + (variant_count * 1000.0) + (magnitude * 100.0)
        scored.append((key, score))
    return [key for key, _ in sorted(scored, key=lambda item: item[1], reverse=True)[:max_charts]]


def _load_day_bars(*, data_dir: Path, symbol: str, event_date: date) -> pl.DataFrame:
    path = data_dir / symbol / f"{event_date.isoformat()}.parquet"
    if not path.exists():
        return pl.DataFrame()
    rows: list[dict[str, Any]] = []
    cumulative_pv = 0.0
    cumulative_volume = 0.0
    for row in pl.read_parquet(path).sort("timestamp").to_dicts():
        ts = row["timestamp"]
        ts_et = ts.astimezone(ET) if hasattr(ts, "astimezone") else _parse_ts(str(ts)).astimezone(ET)
        if ts_et.date() != event_date:
            continue
        if not (time(9, 25) <= ts_et.time() <= time(10, 45)):
            continue
        volume = _float(row.get("volume")) or 0.0
        close = _float(row.get("close")) or 0.0
        if ts_et.time() >= time(9, 30):
            cumulative_pv += close * volume
            cumulative_volume += volume
        opening_vwap = cumulative_pv / cumulative_volume if cumulative_volume > 0 else None
        rows.append(
            {
                "timestamp_et": ts_et,
                "time_label": ts_et.strftime("%H:%M"),
                "open": _float(row.get("open")),
                "high": _float(row.get("high")),
                "low": _float(row.get("low")),
                "close": close,
                "opening_vwap": opening_vwap,
            }
        )
    return pl.DataFrame(rows)


def _render_svg(symbol: str, event_date: date, bars: pl.DataFrame, events: list[ReviewEvent]) -> str:
    plot_w = WIDTH - LEFT - RIGHT
    plot_h = HEIGHT - TOP - BOTTOM
    bar_rows = bars.to_dicts()
    prices: list[float] = []
    for row in bar_rows:
        for column in ("open", "high", "low", "close", "opening_vwap"):
            value = _float(row.get(column))
            if value is not None:
                prices.append(value)
    for event in events:
        prices.extend(event.entry_prices)
        prices.extend(event.stop_prices)
        prices.extend(event.exit_prices)
    min_price = min(prices)
    max_price = max(prices)
    pad = max((max_price - min_price) * 0.08, 0.05)
    min_price -= pad
    max_price += pad

    def x_for_index(index: int) -> float:
        if len(bar_rows) <= 1:
            return LEFT + (plot_w / 2)
        return LEFT + (index / (len(bar_rows) - 1)) * plot_w

    def y_for_price(price: float) -> float:
        return TOP + ((max_price - price) / (max_price - min_price)) * plot_h

    time_to_index = {row["time_label"]: idx for idx, row in enumerate(bar_rows)}
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{WIDTH}" height="{HEIGHT}" viewBox="0 0 {WIDTH} {HEIGHT}">',
        '<rect width="100%" height="100%" fill="#fbfaf7"/>',
        f'<text x="{LEFT}" y="26" font-family="Arial" font-size="20" font-weight="700" fill="#1b1b1b">{html.escape(symbol)} {event_date.isoformat()} Opening Reversion Review</text>',
        f'<rect x="{LEFT}" y="{TOP}" width="{plot_w}" height="{plot_h}" fill="#ffffff" stroke="#d8d2c4"/>',
    ]
    for tick in _price_ticks(min_price, max_price, 6):
        y = y_for_price(tick)
        parts.append(f'<line x1="{LEFT}" x2="{WIDTH - RIGHT}" y1="{y:.2f}" y2="{y:.2f}" stroke="#eee8dc"/>')
        parts.append(
            f'<text x="{LEFT - 8}" y="{y + 4:.2f}" text-anchor="end" font-family="Arial" font-size="11" fill="#6b665b">{tick:.2f}</text>'
        )

    candle_w = max(3.0, min(9.0, plot_w / max(len(bar_rows), 1) * 0.55))
    for idx, row in enumerate(bar_rows):
        open_price = _float(row.get("open"))
        high = _float(row.get("high"))
        low = _float(row.get("low"))
        close = _float(row.get("close"))
        if None in (open_price, high, low, close):
            continue
        x = x_for_index(idx)
        color = "#197a5b" if close >= open_price else "#a43f3f"
        parts.append(
            f'<line x1="{x:.2f}" x2="{x:.2f}" y1="{y_for_price(high):.2f}" y2="{y_for_price(low):.2f}" stroke="{color}" stroke-width="1.3"/>'
        )
        y_open = y_for_price(open_price)
        y_close = y_for_price(close)
        rect_y = min(y_open, y_close)
        rect_h = max(abs(y_close - y_open), 1.5)
        parts.append(
            f'<rect x="{x - candle_w / 2:.2f}" y="{rect_y:.2f}" width="{candle_w:.2f}" height="{rect_h:.2f}" fill="{color}" opacity="0.82"/>'
        )

    vwap_points = []
    for idx, row in enumerate(bar_rows):
        value = _float(row.get("opening_vwap"))
        if value is not None:
            vwap_points.append(f"{x_for_index(idx):.2f},{y_for_price(value):.2f}")
    if vwap_points:
        parts.append(
            f'<polyline points="{" ".join(vwap_points)}" fill="none" stroke="#276fbf" stroke-width="2.2" opacity="0.9"/>'
        )
        parts.append(f'<text x="{WIDTH - RIGHT - 88}" y="{TOP + 18}" font-family="Arial" font-size="12" fill="#276fbf">opening VWAP</text>')

    for event in events:
        label = event.event_time_et.strftime("%H:%M")
        idx = time_to_index.get(label)
        if idx is None:
            continue
        x = x_for_index(idx)
        color = "#0b7a55" if event.direction == "long" else "#b33838"
        parts.append(f'<line x1="{x:.2f}" x2="{x:.2f}" y1="{TOP}" y2="{TOP + plot_h}" stroke="{color}" stroke-width="1.8" stroke-dasharray="5 4"/>')
        y_marker = TOP + 18 + (events.index(event) % 4) * 18
        parts.append(
            f'<text x="{x + 5:.2f}" y="{y_marker:.2f}" font-family="Arial" font-size="11" fill="{color}">{event.direction} {label} R {event.pnl_min:.1f}..{event.pnl_max:.1f}</text>'
        )
        for stop in event.stop_prices[:3]:
            y_stop = y_for_price(stop)
            parts.append(f'<line x1="{max(LEFT, x - 22):.2f}" x2="{min(WIDTH - RIGHT, x + 52):.2f}" y1="{y_stop:.2f}" y2="{y_stop:.2f}" stroke="{color}" stroke-width="1" stroke-dasharray="2 2" opacity="0.65"/>')

    for label in ("09:30", "09:45", "10:00", "10:15", "10:30", "10:45"):
        idx = time_to_index.get(label)
        if idx is not None:
            x = x_for_index(idx)
            parts.append(f'<line x1="{x:.2f}" x2="{x:.2f}" y1="{TOP + plot_h}" y2="{TOP + plot_h + 5}" stroke="#918a7c"/>')
            parts.append(f'<text x="{x:.2f}" y="{TOP + plot_h + 22}" text-anchor="middle" font-family="Arial" font-size="11" fill="#6b665b">{label}</text>')

    legend_y = HEIGHT - 24
    parts.append(
        f'<text x="{LEFT}" y="{legend_y}" font-family="Arial" font-size="12" fill="#333">Events: {len(events)}. Vertical lines mark candidate entries; short dashed horizontal marks show sampled stops.</text>'
    )
    parts.append("</svg>")
    return "\n".join(parts)


def _price_ticks(min_price: float, max_price: float, count: int) -> list[float]:
    if count <= 1 or min_price == max_price:
        return [min_price]
    step = (max_price - min_price) / (count - 1)
    return [min_price + step * idx for idx in range(count)]


def _write_index(
    path: Path,
    rendered: list[Path],
    grouped: dict[tuple[str, date], list[ReviewEvent]],
) -> None:
    lines = [
        "# Playbook Chart Review",
        "",
        "These SVGs are generated from local minute bars, not from TOS or TradingView.",
        "Use them as a fast first pass before opening the same dates in your charting tool.",
        "",
    ]
    for chart in rendered:
        stem = chart.stem
        date_text, symbol = stem.split("_", 1)
        key = (symbol, date.fromisoformat(date_text))
        events = grouped.get(key, [])
        lines.append(f"## {symbol} {date_text}")
        lines.append("")
        lines.append(f"![{symbol} {date_text}]({chart.relative_to(path.parent)})")
        lines.append("")
        for event in sorted(events, key=lambda item: item.event_ts_utc):
            lines.append(
                "- {time} ET {direction}, variants={variants}, pnl_r={pnl_min:.2f}..{pnl_max:.2f}, "
                "mfe={mfe:.2f}, mae={mae:.2f}, stretch={stretch}".format(
                    time=event.event_time_et.strftime("%H:%M"),
                    direction=event.direction,
                    variants=event.variants,
                    pnl_min=event.pnl_min,
                    pnl_max=event.pnl_max,
                    mfe=event.mfe_max,
                    mae=event.mae_max,
                    stretch=event.extension_summary,
                )
            )
        lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def _parse_ts(value: str) -> datetime:
    normalized = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(result) or math.isinf(result):
        return None
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sample-events", required=True, type=Path)
    parser.add_argument("--out-dir", required=True, type=Path)
    parser.add_argument("--data-dir", type=Path, default=DATA_DIR)
    parser.add_argument("--max-charts", type=int, default=8)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    rendered = render_chart_review(
        sample_events_csv=args.sample_events,
        out_dir=args.out_dir,
        data_dir=args.data_dir,
        max_charts=max(1, args.max_charts),
    )
    print(f"CHART_REVIEW_INDEX={args.out_dir / 'REVIEW_INDEX.md'}")
    print(f"CHARTS={len(rendered)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
