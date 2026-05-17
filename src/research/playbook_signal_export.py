"""Export Mala playbook signal events for Bhiksha parity comparison."""

from __future__ import annotations

import argparse
import csv
import json
from datetime import date
from pathlib import Path
from typing import Any

import polars as pl

from src.chronos.storage import LocalStorage
from src.config import DATA_DIR
from src.newton.engine import PhysicsEngine
from src.research.playbook_surface import _entry_signal_cache_key
from src.strategy.base import required_feature_union
from src.strategy.factory import build_strategy
from src.strategy.intraday_mean_reversion import PLAYBOOK_ID, STRATEGY_NAME


def export_playbook_signal_events(
    run_dir: Path,
    *,
    out_path: Path,
    data_dir: Path | None = None,
    symbols: list[str] | None = None,
) -> Path:
    config_path = run_dir / "config.json"
    if not config_path.exists():
        raise FileNotFoundError(f"config.json not found under {run_dir}")
    run_config = json.loads(config_path.read_text(encoding="utf-8"))
    if run_config.get("playbook_id") != PLAYBOOK_ID:
        raise ValueError(f"unsupported playbook {run_config.get('playbook_id')!r}")

    start = date.fromisoformat(str(run_config["start"]))
    end = date.fromisoformat(str(run_config["end"]))
    configured_symbols = [str(symbol).upper() for symbol in run_config.get("symbols", [])]
    active_symbols = [symbol.upper() for symbol in (symbols or configured_symbols)]
    configs_by_id = {
        str(config_id): dict(config)
        for config_id, config in dict(run_config.get("configs", {})).items()
    }
    storage = LocalStorage(base_dir=data_dir or DATA_DIR)

    rows: list[dict[str, Any]] = []
    for symbol in active_symbols:
        bars = storage.load_bars(symbol, start=start, end=end)
        if bars.is_empty():
            continue
        symbol_col = "symbol" if "symbol" in bars.columns else "ticker"
        if symbol_col == "ticker":
            bars = bars.rename({"ticker": "symbol"})

        enriched_by_feature_set: dict[frozenset[str], pl.DataFrame] = {}
        signals_by_entry_key: dict[str, pl.DataFrame] = {}
        for config_id, config in configs_by_id.items():
            strategy = build_strategy(STRATEGY_NAME, config)
            features = frozenset(required_feature_union([strategy]))
            if features not in enriched_by_feature_set:
                enriched_by_feature_set[features] = PhysicsEngine().enrich_for_features(
                    bars,
                    set(features),
                )
            entry_key = _entry_signal_cache_key(config)
            if entry_key not in signals_by_entry_key:
                signals_by_entry_key[entry_key] = strategy.generate_signals(
                    enriched_by_feature_set[features]
                )
            signals = signals_by_entry_key[entry_key]
            for row in signals.filter(pl.col("signal")).to_dicts():
                direction = row.get("signal_direction")
                if not direction:
                    continue
                timestamp = row["timestamp"]
                rows.append(
                    {
                        "config_id": config_id,
                        "symbol": symbol,
                        "direction": direction,
                        "event_timestamp": timestamp.isoformat()
                        if hasattr(timestamp, "isoformat")
                        else str(timestamp),
                        "event_type": "entry",
                        "policy_id": config_id,
                        "exit_family": str(config.get("exit_family", "")),
                    }
                )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "config_id",
                "symbol",
                "direction",
                "event_timestamp",
                "event_type",
                "policy_id",
                "exit_family",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)
    return out_path


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--data-dir", type=Path)
    parser.add_argument("--symbols", nargs="*")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    print(
        export_playbook_signal_events(
            args.run_dir,
            out_path=args.out,
            data_dir=args.data_dir,
            symbols=args.symbols,
        )
    )


if __name__ == "__main__":
    main()
