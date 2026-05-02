#!/usr/bin/env python3
"""Compare personal replay anchors against Mala strategies and oracle bars."""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

from loguru import logger

from src.chronos.storage import LocalStorage
from src.config import DATA_DIR
from src.personal.replay_evaluator import (
    build_markdown_report,
    evaluate_replay_anchors,
    load_replay_anchors,
    write_csv,
)
from src.strategy.factory import available_strategy_names


DEFAULT_ANCHORS = Path("data/personal_imports/processed/20260429_232737/mala_replay_anchors.csv")
DEFAULT_OUT_DIR = DATA_DIR / "personal_imports" / "replay_evaluations"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--anchors", type=Path, default=DEFAULT_ANCHORS)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument("--data-dir", type=Path, default=DATA_DIR)
    parser.add_argument(
        "--strategy",
        action="append",
        choices=available_strategy_names(),
        help="Strategy to evaluate. Repeat for multiple. Defaults to all registered strategies.",
    )
    parser.add_argument("--signal-tolerance-minutes", type=int, default=5)
    parser.add_argument("--oracle-window", type=int, action="append", default=[15, 30, 60])
    parser.add_argument("--verbose", action="store_true", help="Keep strategy/Newton INFO logs enabled.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.verbose:
        logger.remove()
        logger.add(sys.stderr, level="WARNING")

    anchors = load_replay_anchors(args.anchors)
    rows, summary_rows = evaluate_replay_anchors(
        anchors,
        storage=LocalStorage(args.data_dir),
        strategy_names=args.strategy,
        signal_tolerance_minutes=args.signal_tolerance_minutes,
        oracle_windows=tuple(args.oracle_window),
    )

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = args.out_dir / stamp
    detail_path = run_dir / "personal_replay_detail.csv"
    summary_path = run_dir / "personal_replay_summary.csv"
    report_path = run_dir / "personal_replay_report.md"
    write_csv(detail_path, rows)
    write_csv(summary_path, summary_rows)
    report_path.write_text(
        build_markdown_report(
            rows=rows,
            summary_rows=summary_rows,
            detail_path=detail_path,
            summary_path=summary_path,
        ),
        encoding="utf-8",
    )

    evaluated = sum(1 for row in rows if row.get("status") == "evaluated")
    print(f"anchors: {len(rows)}")
    print(f"evaluated: {evaluated}")
    print(f"missing: {len(rows) - evaluated}")
    print(f"run_dir: {run_dir}")
    print(f"report: {report_path}")


if __name__ == "__main__":
    main()
