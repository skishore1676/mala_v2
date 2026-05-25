"""Write the Mala-owned structural-break feed consumed by Kamandal."""

from __future__ import annotations

import argparse
import json
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import polars as pl

from src.chronos.storage import LocalStorage
from src.config import DATA_DIR, RESEARCH_RESULTS_DIR
from src.research.structural_breaks import daily_ohlcv_from_minutes, latest_structural_break

DEFAULT_UNIVERSE = ["SPY", "QQQ", "IWM", "NVDA", "TSLA", "AMD", "MU", "SMH", "PANW", "TLT", "XLE", "AAPL"]


def build_feed_payload(
    *,
    symbols: list[str],
    trade_date: date,
    data_dir: Path = DATA_DIR,
    lookback_days: int = 120,
) -> dict[str, Any]:
    storage = LocalStorage(data_dir)
    rows: list[dict[str, Any]] = []
    start = trade_date - timedelta(days=lookback_days)
    for symbol in symbols:
        minutes = _load_minutes_robust(storage, symbol, start=start, end=trade_date)
        if minutes.is_empty():
            rows.append(_missing_row(symbol, trade_date, "no cached bars"))
            continue
        daily = daily_ohlcv_from_minutes(minutes)
        signal = latest_structural_break(daily, symbol=symbol, minute_bars=minutes, as_of=trade_date)
        if signal is None:
            rows.append(_missing_row(symbol, trade_date, "insufficient bars"))
        elif signal.date != trade_date.isoformat():
            rows.append(_missing_row(symbol, trade_date, f"latest cached bar is {signal.date}"))
        else:
            rows.append(signal.to_feed_row())
    return {
        "date": trade_date.isoformat(),
        "schema_version": 1,
        "source": "mala_v2.structural_breaks_feed",
        "symbols": rows,
    }


def write_feed(
    *,
    symbols: list[str],
    trade_date: date,
    output_dir: Path,
    data_dir: Path = DATA_DIR,
    lookback_days: int = 120,
) -> Path:
    payload = build_feed_payload(symbols=symbols, trade_date=trade_date, data_dir=data_dir, lookback_days=lookback_days)
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"{trade_date.isoformat()}.json"
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _missing_row(symbol: str, trade_date: date, reason: str) -> dict[str, Any]:
    return {
        "symbol": symbol.upper(),
        "date": trade_date.isoformat(),
        "weekly_reclaim_long": False,
        "weekly_reclaim_short": False,
        "daily_range_expansion": False,
        "intraday_volume_acceptance": False,
        "confluence_score": 0,
        "confluence_score_long": 0,
        "confluence_score_short": 0,
        "notes": reason,
    }


def _load_minutes_robust(storage: LocalStorage, symbol: str, *, start: date, end: date) -> pl.DataFrame:
    try:
        return storage.load_bars(symbol, start=start, end=end)
    except pl.exceptions.SchemaError:
        ticker_dir = storage.base_dir / symbol.upper()
        if not ticker_dir.exists():
            return pl.DataFrame()
        frames = []
        for path in sorted(ticker_dir.glob("*.parquet")):
            if not storage._file_in_range(path, start, end):
                continue
            frame = pl.read_parquet(path)
            if "timestamp" in frame.columns:
                frame = frame.with_columns(
                    pl.col("timestamp").cast(pl.Int64).cast(pl.Datetime("us", time_zone="UTC")).alias("timestamp")
                )
            casts = []
            for column in ("open", "high", "low", "close", "volume", "vwap"):
                if column in frame.columns:
                    casts.append(pl.col(column).cast(pl.Float64).alias(column))
            if casts:
                frame = frame.with_columns(casts)
            frames.append(frame)
        return pl.concat(frames, how="diagonal_relaxed").sort("timestamp") if frames else pl.DataFrame()


def main() -> int:
    parser = argparse.ArgumentParser(description="Write the daily structural-break feed.")
    parser.add_argument("--date", default=date.today().isoformat(), help="Trade date to write, YYYY-MM-DD.")
    parser.add_argument("--symbols", default=",".join(DEFAULT_UNIVERSE), help="Comma-separated symbols.")
    parser.add_argument("--output-dir", default=str(RESEARCH_RESULTS_DIR / "structural_breaks"))
    parser.add_argument("--data-dir", default=str(DATA_DIR))
    parser.add_argument("--lookback-days", type=int, default=120)
    args = parser.parse_args()

    path = write_feed(
        symbols=[item.strip().upper() for item in args.symbols.split(",") if item.strip()],
        trade_date=date.fromisoformat(args.date),
        output_dir=Path(args.output_dir),
        data_dir=Path(args.data_dir),
        lookback_days=args.lookback_days,
    )
    print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
