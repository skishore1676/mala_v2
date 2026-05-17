#!/usr/bin/env python
"""Top up the local Polygon minute-bar cache within current entitlement limits.

This is intentionally incremental. It discovers cached symbols, asks Polygon
only for missing recent trading days, and records entitlement/rate-limit blocks
without treating old blocked history as a hard failure.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.chronos.storage import LocalStorage
from src.config import DATA_DIR, settings


BASE_URL = "https://api.polygon.io"
AGGS_ENDPOINT = "/v2/aggs/ticker/{symbol}/range/1/minute/{day}/{day}"


@dataclass(slots=True)
class DayResult:
    symbol: str
    day: str
    status: str
    bars: int = 0
    files_written: int = 0
    http_status: int | None = None
    message: str = ""


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    data_dir = Path(args.data_dir).expanduser().resolve()
    storage = LocalStorage(data_dir)
    symbols = _resolve_symbols(
        data_dir=data_dir,
        symbols_arg=args.symbols,
        active_plan=Path(args.active_plan).expanduser() if args.active_plan else None,
        include_cache_symbols=args.include_cache_symbols,
    )
    if args.max_symbols:
        symbols = symbols[: args.max_symbols]
    if not symbols:
        raise SystemExit("No symbols found. Pass --symbols or point --data-dir at an existing cache.")

    end = _parse_date(args.end) if args.end else date.today()
    default_start = end - timedelta(days=args.lookback_days)
    session = requests.Session()
    api_key = args.api_key or settings.polygon_api_key
    if not api_key:
        raise SystemExit("POLYGON_API_KEY is not set.")
    session.params = {"apiKey": api_key}

    results: list[DayResult] = []
    for symbol in symbols:
        start = _start_for_symbol(storage, symbol, default_start, args.start)
        missing = storage.missing_dates(symbol, start, end)
        print(f"POLYGON_TOPUP_SYMBOL symbol={symbol} missing_days={len(missing)} start={start} end={end}")
        if args.dry_run:
            results.extend(
                DayResult(symbol=symbol, day=day.isoformat(), status="dry_run")
                for day in missing
            )
            continue
        for day in missing:
            result = _fetch_and_save_day(
                session=session,
                storage=storage,
                symbol=symbol,
                day=day,
                max_retries=args.max_retries,
                retry_delay_seconds=args.retry_delay_seconds,
            )
            results.append(result)
            print(
                "POLYGON_TOPUP_DAY "
                f"symbol={result.symbol} day={result.day} status={result.status} "
                f"bars={result.bars} files_written={result.files_written} "
                f"http_status={result.http_status or ''} message={result.message}"
            )
            if args.request_delay_seconds > 0:
                time.sleep(args.request_delay_seconds)

    summary = _build_summary(results, data_dir=data_dir, symbols=symbols, dry_run=args.dry_run)
    summary_path = Path(args.summary_path).expanduser() if args.summary_path else _default_summary_path()
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(
        "POLYGON_TOPUP_SUMMARY "
        f"symbols={len(symbols)} days={len(results)} written={summary['files_written']} "
        f"entitlement_blocked={summary['status_counts'].get('entitlement_blocked', 0)} "
        f"errors={summary['status_counts'].get('error', 0)} summary={summary_path}"
    )
    return 1 if summary["status_counts"].get("error", 0) else 0


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", default=str(DATA_DIR), help="Mala cache directory. Defaults to repo data/.")
    parser.add_argument("--symbols", default="", help="Comma-separated symbols. Defaults to active-plan symbols, or cached symbols when no active plan is supplied.")
    parser.add_argument("--active-plan", default="", help="Optional Bhiksha active_plan.json whose symbols should be included.")
    parser.add_argument("--include-cache-symbols", action="store_true", help="Also include every symbol already present in --data-dir.")
    parser.add_argument("--lookback-days", type=int, default=21, help="Recent calendar-day window to inspect when --start is omitted.")
    parser.add_argument("--start", default="", help="Optional YYYY-MM-DD lower bound.")
    parser.add_argument("--end", default="", help="Optional YYYY-MM-DD upper bound. Defaults to today.")
    parser.add_argument("--max-symbols", type=int, default=0, help="Optional cap for smoke tests.")
    parser.add_argument("--request-delay-seconds", type=float, default=12.0, help="Polite delay between day requests.")
    parser.add_argument("--retry-delay-seconds", type=float, default=65.0, help="Fallback wait after HTTP 429.")
    parser.add_argument("--max-retries", type=int, default=2, help="Retries for HTTP 429/5xx.")
    parser.add_argument("--summary-path", default="", help="Optional JSON summary path.")
    parser.add_argument("--dry-run", action="store_true", help="Report missing days without downloading.")
    parser.add_argument("--api-key", default="", help=argparse.SUPPRESS)
    return parser.parse_args(argv)


def _resolve_symbols(
    *,
    data_dir: Path,
    symbols_arg: str,
    active_plan: Path | None,
    include_cache_symbols: bool,
) -> list[str]:
    symbols: set[str] = set()
    if symbols_arg.strip():
        symbols.update(part.strip().upper() for part in symbols_arg.split(",") if part.strip())
    if active_plan and active_plan.exists():
        payload = json.loads(active_plan.read_text(encoding="utf-8"))
        symbols.update(
            str(deployment.get("symbol", "")).upper()
            for deployment in payload.get("deployments", [])
            if deployment.get("enabled", True) and deployment.get("symbol")
        )
    if not symbols or include_cache_symbols:
        symbols.update(
            path.name.upper()
            for path in data_dir.iterdir()
            if path.is_dir() and any(path.glob("*.parquet"))
        )
    return sorted(symbol for symbol in symbols if symbol)


def _start_for_symbol(storage: LocalStorage, symbol: str, default_start: date, start_arg: str) -> date:
    del storage, symbol
    if start_arg:
        return _parse_date(start_arg)
    return default_start


def _fetch_and_save_day(
    *,
    session: requests.Session,
    storage: LocalStorage,
    symbol: str,
    day: date,
    max_retries: int,
    retry_delay_seconds: float,
) -> DayResult:
    url = BASE_URL + AGGS_ENDPOINT.format(symbol=symbol, day=day.isoformat())
    params = {"adjusted": "true", "sort": "asc", "limit": 50000}
    for attempt in range(max_retries + 1):
        try:
            response = session.get(url, params=params, timeout=30)
        except requests.RequestException as exc:
            if attempt < max_retries:
                time.sleep(min(retry_delay_seconds, 5.0 * (attempt + 1)))
                continue
            return DayResult(symbol=symbol, day=day.isoformat(), status="error", message=type(exc).__name__)
        if response.status_code == 429 and attempt < max_retries:
            time.sleep(_retry_after_seconds(response) or retry_delay_seconds)
            continue
        if response.status_code == 403:
            return DayResult(
                symbol=symbol,
                day=day.isoformat(),
                status="entitlement_blocked",
                http_status=response.status_code,
                message=_response_message(response),
            )
        if response.status_code >= 500 and attempt < max_retries:
            time.sleep(min(retry_delay_seconds, 5.0 * (attempt + 1)))
            continue
        if response.status_code >= 400:
            return DayResult(
                symbol=symbol,
                day=day.isoformat(),
                status="error",
                http_status=response.status_code,
                message=_response_message(response),
            )
        payload = response.json()
        bars = payload.get("results") or []
        written = storage.save_bars(symbol, bars) if bars else 0
        return DayResult(
            symbol=symbol,
            day=day.isoformat(),
            status="written" if written else "no_bars",
            bars=len(bars),
            files_written=written,
            http_status=response.status_code,
        )
    return DayResult(symbol=symbol, day=day.isoformat(), status="error", message="retry_exhausted")


def _build_summary(results: list[DayResult], *, data_dir: Path, symbols: list[str], dry_run: bool) -> dict[str, Any]:
    status_counts: dict[str, int] = {}
    for result in results:
        status_counts[result.status] = status_counts.get(result.status, 0) + 1
    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "data_dir": str(data_dir),
        "dry_run": dry_run,
        "symbols": symbols,
        "day_count": len(results),
        "files_written": sum(result.files_written for result in results),
        "bars_written": sum(result.bars for result in results),
        "status_counts": status_counts,
        "results": [asdict(result) for result in results],
    }


def _default_summary_path() -> Path:
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    return Path("data/results/cache_topup") / f"polygon_cache_topup_{stamp}.json"


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def _response_message(response: requests.Response) -> str:
    try:
        payload = response.json()
    except ValueError:
        return response.reason[:160]
    return str(payload.get("error") or payload.get("message") or response.reason)[:160]


def _retry_after_seconds(response: requests.Response) -> float | None:
    raw = response.headers.get("Retry-After")
    if not raw:
        return None
    try:
        return max(float(raw), 0.0)
    except ValueError:
        return None


if __name__ == "__main__":
    raise SystemExit(main())
