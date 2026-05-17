#!/usr/bin/env python3
"""
Single-shot pipeline: Chronos → Newton → Strategy → Oracle.

Usage:
    uv run python main.py --tickers SPY --start 2024-01-01 --end 2024-12-31
    uv run python main.py --tickers SPY QQQ IWM --strategy "Market Impulse (Cross & Reclaim)"
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(REPO_ROOT))

from src.chronos.client import PolygonClient
from src.chronos.storage import LocalStorage
from src.config import DATA_DIR, settings
from src.newton.engine import PhysicsEngine
from src.oracle.metrics import MetricsCalculator
from src.strategy.base import required_feature_union
from src.strategy.factory import available_strategy_names, build_strategy_by_name


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tickers", nargs="+", default=settings.default_tickers)
    parser.add_argument("--start", type=date.fromisoformat, default=date(2024, 1, 1))
    parser.add_argument("--end", type=date.fromisoformat, default=date.today())
    parser.add_argument(
        "--strategy",
        default="Opening Drive Classifier",
        help=f"Strategy name. Available: {available_strategy_names()}",
    )
    parser.add_argument(
        "--out-dir",
        default=str(DATA_DIR / "results"),
        help="Directory for output JSON/CSV",
    )
    parser.add_argument(
        "--fetch", action="store_true",
        help="Force re-fetch from Polygon even if cached",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    out_root = Path(args.out_dir)
    run_ts = datetime.now().strftime("%Y%m%dT%H%M%S")

    storage = LocalStorage()
    client = PolygonClient(api_key=settings.polygon_api_key)
    physics = PhysicsEngine()
    metrics = MetricsCalculator()

    strategy = build_strategy_by_name(args.strategy)
    print(f"Strategy: {args.strategy}")
    print(f"Tickers:  {args.tickers}")
    print(f"Period:   {args.start} → {args.end}")

    for ticker in args.tickers:
        print(f"\n── {ticker} ──")

        if args.fetch:
            bars = client.fetch_aggs(ticker, args.start, args.end)
            storage.save_bars(ticker, bars)

        raw = storage.load_bars(ticker, args.start, args.end)
        if raw.is_empty():
            print(f"  No data — run with --fetch to download from Polygon")
            continue

        df = physics.enrich_for_features(raw, required_feature_union([strategy]))
        df = strategy.generate_signals(df)          # adds 'signal' boolean column
        df = metrics.add_forward_metrics(df)        # adds MFE/MAE columns

        summary_df = metrics.summarise_signals(df)  # returns DataFrame
        if summary_df.is_empty():
            print("  no signals in this period")
            continue
        result = summary_df.row(0, named=True)

        print(f"  signals={result.get('total_signals', 0)}")
        print(f"  confidence={result.get('confidence_score', 0):.1%}")
        print(f"  avg_mfe={result.get('avg_mfe', 0):+.4f}")
        print(f"  avg_mae={result.get('avg_mae', 0):+.4f}")

        out_dir = out_root / f"{run_ts}_{ticker}_{args.strategy.replace(' ', '_')}"
        out_dir.mkdir(parents=True, exist_ok=True)
        result_path = out_dir / "result.json"
        result_path.write_text(json.dumps(result, indent=2, default=str))
        print(f"  saved → {result_path}")


if __name__ == "__main__":
    main()
