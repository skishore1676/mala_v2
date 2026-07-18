"""Local-only runner for deterministic daily rectangle research."""

from __future__ import annotations

import argparse
from collections import Counter
from dataclasses import asdict
from datetime import date, datetime, timezone
import hashlib
import json
from pathlib import Path
import platform
import subprocess
import sys
from typing import Any, Sequence

import polars as pl

from src.chronos.storage import LocalStorage
from src.config import DATA_DIR
from src.oracle.rectangle_trade_simulator import simulate_rectangle_trade

from .contracts import (
    OutcomeResult,
    RectangleResearchConfig,
    TradeResult,
    contract_dict,
    load_rectangle_config,
)
from .daily_bars import build_rth_daily_bars, hash_daily_bars, normalize_daily_input
from .lifecycle import derive_lifecycle
from .readiness import audit_local_cache, load_readiness_report, write_readiness_report
from .public_daily import (
    acquire_public_daily_dataset,
    load_public_daily_dataset,
    load_public_validation_universe,
    verify_public_daily_dataset_against_universe,
    verify_semantic_freeze_for_public_run,
)
from .rectangle import EnumerationResult, enumerate_rectangles
from .review import (
    build_semantic_calibration_batch_v2,
    build_semantic_review_batch,
    ingest_calibration_review_responses_v2,
    ingest_review_responses,
    render_calibration_obsidian_gate_v2,
    render_obsidian_review_card,
    verify_semantic_calibration_batch_v2,
    verify_semantic_batch,
)
from .source_fidelity import (
    freeze_mala_rectangle_semantic_spec_v1,
    ingest_source_fidelity_responses_v3,
    initialize_source_fidelity_review_v3,
    verify_source_fidelity_review_v3,
)


DEFAULT_CONFIG = Path("config/classical_patterns/rectangle_daily_v1.yaml")
DEFAULT_PUBLIC_UNIVERSE = Path(
    "config/classical_patterns/public_validation_universe_v1.json"
)


def run_research(
    daily_by_symbol: dict[str, pl.DataFrame],
    *,
    config: RectangleResearchConfig,
    output_dir: Path,
    run_id: str,
    mode: str,
    argv: Sequence[str] | None = None,
    phase: str = "deterministic implementation fixture shadow",
    readiness: str = "fixture_shadow",
    data_context: dict[str, Any] | None = None,
    warnings: Sequence[str] | None = None,
) -> dict[str, Any]:
    """Run the complete deterministic population and write reviewable artifacts."""

    output_dir = output_dir.expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    enumeration_rows: list[dict[str, Any]] = []
    candidate_rows: list[dict[str, Any]] = []
    signal_rows: list[dict[str, Any]] = []
    lifecycle_rows: list[dict[str, Any]] = []
    outcome_rows: list[dict[str, Any]] = []
    trade_rows: list[dict[str, Any]] = []
    data_hashes: dict[str, str] = {}
    daily_frames: list[pl.DataFrame] = []
    rejection_reasons: Counter[str] = Counter()
    no_trade_reasons: Counter[str] = Counter()
    outcome_counts: Counter[str] = Counter()
    population_counts: Counter[str] = Counter()

    for symbol in sorted(daily_by_symbol):
        daily = daily_by_symbol[symbol].sort("session_date")
        if daily.is_empty():
            continue
        daily_frames.append(daily)
        data_hashes[symbol] = hash_daily_bars(daily)
        enumeration = enumerate_rectangles(daily, config)
        _accumulate_population(population_counts, enumeration)
        for record in enumeration.records:
            row = contract_dict(record)
            enumeration_rows.append(row)
            if record.status == "rejected":
                rejection_reasons[record.reason] += 1
        candidate_rows.extend(contract_dict(candidate) for candidate in enumeration.candidates)
        signal_rows.extend(_signal_row(signal) for signal in enumeration.signals)

        for signal in enumeration.signals:
            events, outcome = derive_lifecycle(signal, daily, config)
            lifecycle_rows.extend(contract_dict(event) for event in events)
            outcome_row = contract_dict(outcome)
            outcome_row.update(
                symbol=signal.candidate.symbol,
                direction=signal.candidate.direction.value,
                split=signal.candidate.split,
                breakout_date=signal.candidate.breakout_date.isoformat(),
            )
            outcome_rows.append(outcome_row)
            outcome_counts[outcome.outcome.value] += 1

            for stop_buffer in config.definition.lfd_stop_buffer_atr:
                trade = simulate_rectangle_trade(
                    signal,
                    daily,
                    stop_buffer_atr=stop_buffer,
                    config=config,
                )
                trade_row = _trade_row(trade, outcome, signal.candidate.symbol, signal.candidate.split)
                trade_rows.append(trade_row)
                if trade.status != "closed":
                    no_trade_reasons[trade.exit_reason] += 1

    combined_daily = pl.concat(daily_frames).sort(["symbol", "session_date"]) if daily_frames else pl.DataFrame()
    frames = {
        "daily_bars.parquet": combined_daily,
        "enumeration_audit.csv": _frame(enumeration_rows),
        "candidates.csv": _frame(candidate_rows),
        "signals.csv": _frame(signal_rows),
        "lifecycle_events.csv": _frame(lifecycle_rows),
        "outcomes.csv": _frame(outcome_rows),
        "trades.csv": _frame(trade_rows),
        "economic_scorecard.csv": _economic_scorecard(trade_rows),
    }
    artifacts = _write_frames(frames, output_dir)

    variant_ids = [f"lfd_buffer_{value:.2f}atr".replace(".", "p") for value in config.definition.lfd_stop_buffer_atr]
    population_checks = _population_checks(
        population_counts,
        signal_rows=signal_rows,
        trade_rows=trade_rows,
        variant_ids=variant_ids,
    )
    if not all(population_checks.values()):
        raise RuntimeError(f"Population accounting failed: {population_checks}")
    git = _git_state()
    receipt: dict[str, Any] = {
        "schema_version": "BacktestRunReceiptV1",
        "phase": phase,
        "run_id": run_id,
        "mode": mode,
        "status": "complete",
        "readiness": readiness,
        "executable": False,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "git": git,
        "environment": {
            "python": platform.python_version(),
            "polars": pl.__version__,
            "argv": list(argv or sys.argv),
        },
        "config": {
            "path": str(config.source_path),
            "hash": config.source_hash,
            "playbook_id": config.playbook_id,
            "version": config.version,
            "status": config.status,
        },
        "data": {
            "adjustment_policy": config.session.adjustment_policy,
            "symbol_hashes": data_hashes,
            "symbols": sorted(data_hashes),
            **(data_context or {}),
        },
        "policies": {
            "entry": config.execution.entry_timing,
            "trade_same_bar": config.execution.same_bar_trade_ordering,
            "outcome_same_bar": config.execution.same_bar_outcome_ordering,
            "human_review_filters_economics": config.population.human_review_may_filter_economics,
            "representative": config.population.representative_policy,
        },
        "variants": {
            "ids": variant_ids,
            "count": len(variant_ids),
            "tested_hypothesis_count": len(variant_ids),
            "maximum_reentries": config.definition.maximum_reentries,
        },
        "population": dict(sorted(population_counts.items())),
        "population_checks": population_checks,
        "rejection_reasons": dict(sorted(rejection_reasons.items())),
        "no_trade_reasons": dict(sorted(no_trade_reasons.items())),
        "outcome_counts": dict(sorted(outcome_counts.items())),
        "lookahead_checks": {
            "breakout_bar_excluded_from_geometry": True,
            "atr_ends_before_breakout": True,
            "entry_is_next_session_open": True,
            "outcomes_are_not_detector_inputs": True,
            "human_review_cannot_filter_population": True,
        },
        "artifacts": artifacts,
        "warnings": list(warnings) if warnings is not None else [
            "Fixture-shadow evidence is not a live or packet-promotion claim.",
            "Provider adjustment provenance must be independently verified before historical holdout claims.",
        ],
    }
    receipt_path = output_dir / "receipt.json"
    receipt_path.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    _write_receipt_markdown(receipt, output_dir / "RECEIPT.md")
    _write_report(receipt, frames["economic_scorecard.csv"], output_dir / "REPORT.md")
    receipt["artifacts"].update(
        {
            "receipt.json": {
                "path": "receipt.json",
                "row_count": 1,
                "content_hash": None,
                "schema_hash": None,
                "note": "Authoritative receipt is not self-hashed.",
            },
            "RECEIPT.md": _artifact_meta(output_dir / "RECEIPT.md", row_count=1),
            "REPORT.md": _artifact_meta(output_dir / "REPORT.md", row_count=1),
        }
    )
    # Rewrite once so the authoritative JSON includes its rendered companions.
    receipt_path.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return receipt


def _accumulate_population(counts: Counter[str], result: EnumerationResult) -> None:
    counts["scanned_windows"] += result.scanned_window_count
    counts["rejected_windows"] += result.rejected_window_count
    counts["valid_non_signal_windows"] += result.valid_non_signal_count
    counts["qualifying_candidates"] += len(result.candidates)
    counts["representative_signals"] += len(result.signals)
    counts["cluster_duplicates"] += result.cluster_duplicate_count


def _population_checks(
    counts: Counter[str],
    *,
    signal_rows: Sequence[dict[str, Any]],
    trade_rows: Sequence[dict[str, Any]],
    variant_ids: Sequence[str],
) -> dict[str, bool]:
    """Fail-closed accounting identities for the complete enumerated population."""

    signal_ids = {str(row["signal_id"]) for row in signal_rows}
    economic_signal_ids = {str(row["signal_id"]) for row in trade_rows}
    expected_signal_variants = {
        (signal_id, variant_id)
        for signal_id in signal_ids
        for variant_id in variant_ids
    }
    actual_signal_variants = [
        (str(row["signal_id"]), str(row["variant_id"]))
        for row in trade_rows
    ]
    return {
        "windows_reconcile": counts["scanned_windows"]
        == counts["rejected_windows"]
        + counts["valid_non_signal_windows"]
        + counts["qualifying_candidates"],
        "candidate_clusters_reconcile": counts["qualifying_candidates"]
        == counts["representative_signals"] + counts["cluster_duplicates"],
        "economic_signal_ids_match_enumerator": economic_signal_ids == signal_ids,
        "economic_signal_variants_match_contract": set(actual_signal_variants)
        == expected_signal_variants,
        "economic_signal_variants_have_no_duplicates": len(actual_signal_variants)
        == len(set(actual_signal_variants)),
    }


def _signal_row(signal: Any) -> dict[str, Any]:
    row = contract_dict(signal.candidate)
    row.update(
        signal_id=signal.signal_id,
        cluster_candidate_count=signal.cluster_candidate_count,
    )
    return row


def _trade_row(trade: TradeResult, outcome: OutcomeResult, symbol: str, split: str) -> dict[str, Any]:
    row = contract_dict(trade)
    row.update(
        symbol=symbol,
        split=split,
        breakout_outcome=outcome.outcome.value,
        outcome_terminal_reason=outcome.terminal_reason,
    )
    return row


def _economic_scorecard(trades: list[dict[str, Any]]) -> pl.DataFrame:
    if not trades:
        return pl.DataFrame()
    frame = pl.DataFrame(trades)
    dimensions = ["split", "direction", "variant_id"]
    rows: list[dict[str, Any]] = []
    for keys, group in frame.group_by(dimensions, maintain_order=True):
        key_values = keys if isinstance(keys, tuple) else (keys,)
        closed = group.filter(pl.col("status") == "closed")
        net_r_values = closed.get_column("net_r").drop_nulls().to_list() if "net_r" in closed.columns else []
        positive = [float(value) for value in net_r_values if float(value) > 0]
        negative = [float(value) for value in net_r_values if float(value) < 0]
        rows.append(
            {
                **dict(zip(dimensions, key_values)),
                "result_rows": len(group),
                "closed_trades": len(closed),
                "no_trade_or_censored": len(group) - len(closed),
                "win_rate": (len(positive) / len(net_r_values)) if net_r_values else None,
                "average_net_r": (sum(float(value) for value in net_r_values) / len(net_r_values)) if net_r_values else None,
                "median_net_r": float(np_median(net_r_values)) if net_r_values else None,
                "profit_factor": (
                    sum(positive) / abs(sum(negative))
                    if negative
                    else float("inf")
                    if positive
                    else None
                ),
            }
        )
    return pl.DataFrame(rows).sort(dimensions)


def np_median(values: Sequence[float]) -> float:
    ordered = sorted(float(value) for value in values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / 2.0


def _frame(rows: list[dict[str, Any]]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame()
    # CSV is deliberately the review surface. Encode nested contract fields as
    # stable JSON instead of allowing Polars to create non-serializable List or
    # Struct columns.
    normalized = [
        {
            key: json.dumps(value, sort_keys=True, separators=(",", ":"))
            if isinstance(value, (list, tuple, dict))
            else value
            for key, value in row.items()
        }
        for row in rows
    ]
    # Economic populations can exceed Polars' default inference sample. A
    # nullable audit field may be empty in the first 100 rows and populated
    # later, so infer across the complete deterministic population.
    return pl.DataFrame(normalized, infer_schema_length=None)


def _write_frames(frames: dict[str, pl.DataFrame], output_dir: Path) -> dict[str, Any]:
    artifacts: dict[str, Any] = {}
    for name, frame in frames.items():
        path = output_dir / name
        if name.endswith(".parquet"):
            frame.write_parquet(path)
        else:
            frame.write_csv(path)
        artifacts[name] = _artifact_meta(path, row_count=len(frame), frame=frame)
    return artifacts


def _artifact_meta(path: Path, *, row_count: int, frame: pl.DataFrame | None = None) -> dict[str, Any]:
    content_hash = hashlib.sha256(path.read_bytes()).hexdigest()
    schema_hash = (
        hashlib.sha256(json.dumps({name: str(dtype) for name, dtype in frame.schema.items()}, sort_keys=True).encode("utf-8")).hexdigest()
        if frame is not None
        else None
    )
    return {
        "path": path.name,
        "row_count": row_count,
        "content_hash": content_hash,
        "schema_hash": schema_hash,
    }


def _git_state() -> dict[str, Any]:
    def run(*args: str) -> str:
        completed = subprocess.run(
            ["git", *args],
            check=False,
            capture_output=True,
            text=True,
        )
        return completed.stdout.strip()

    dirty_paths = [line for line in run("status", "--short").splitlines() if line]
    return {
        "commit": run("rev-parse", "HEAD"),
        "branch": run("branch", "--show-current"),
        "dirty": bool(dirty_paths),
        "dirty_paths": dirty_paths,
    }


def _write_receipt_markdown(receipt: dict[str, Any], path: Path) -> None:
    population = receipt["population"]
    lines = [
        f"# Classical Pattern Lab Receipt — {receipt['run_id']}",
        "",
        f"- Status: `{receipt['status']}`",
        f"- Readiness: `{receipt['readiness']}`",
        f"- Executable: `{str(receipt['executable']).lower()}`",
        f"- Config: `{receipt['config']['playbook_id']}@{receipt['config']['version']}`",
        f"- Git commit: `{receipt['git']['commit']}`",
        f"- Dirty tree: `{str(receipt['git']['dirty']).lower()}`",
        "",
        "## Complete Population",
        "",
    ]
    lines.extend(f"- {key}: `{value}`" for key, value in sorted(population.items()))
    lines.extend(
        [
            "",
            "## Outcomes",
            "",
            *[f"- {key}: `{value}`" for key, value in sorted(receipt["outcome_counts"].items())],
            "",
            "## Boundary",
            "",
            f"This is {receipt['phase']}. It is not an execution packet, shadow",
            "authorization, live approval, or trading recommendation.",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def _write_report(receipt: dict[str, Any], scorecard: pl.DataFrame, path: Path) -> None:
    if receipt["readiness"] == "fixture_shadow":
        verdict = (
            "Fixture-shadow implementation proof only. Economic results below are not\n"
            "promotion evidence until the detector, universe, data provenance, and holdout are frozen."
        )
    else:
        verdict = (
            "Frozen-cohort historical research only. Validation and holdout are untouched by\n"
            "parameter tuning, but current-symbol selection and provider limitations prevent a\n"
            "population-alpha claim."
        )
    lines = [
        f"# Classical Rectangle Breakout Report — {receipt['run_id']}",
        "",
        "## Verdict",
        "",
        verdict,
        "",
        "## Population",
        "",
        "```json",
        json.dumps(receipt["population"], indent=2, sort_keys=True),
        "```",
        "",
        "## Scorecard",
        "",
        "```text",
        str(scorecard),
        "```",
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def _load_daily_csv(path: Path) -> dict[str, pl.DataFrame]:
    frame = pl.read_csv(path, try_parse_dates=True)
    if "symbol" not in frame.columns:
        raise ValueError("Daily CSV requires a symbol column.")
    result: dict[str, pl.DataFrame] = {}
    for symbol in frame.get_column("symbol").unique().sort().to_list():
        result[str(symbol)] = normalize_daily_input(
            frame.filter(pl.col("symbol") == symbol),
            symbol=str(symbol),
        )
    return result


def _load_cache(
    symbols: Sequence[str],
    *,
    start: date,
    end: date,
    data_dir: Path | None,
    config: RectangleResearchConfig,
) -> dict[str, pl.DataFrame]:
    storage = LocalStorage(base_dir=data_dir)
    result: dict[str, pl.DataFrame] = {}
    for symbol in symbols:
        source = storage.load_bars(symbol, start=start, end=end)
        result[symbol.upper()] = build_rth_daily_bars(
            source,
            symbol=symbol,
            session=config.session,
            require_complete=True,
        )
    return result


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    validate = subparsers.add_parser("validate-config")
    validate.add_argument("--config", type=Path, default=DEFAULT_CONFIG)

    audit = subparsers.add_parser("audit-cache")
    audit.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    audit.add_argument("--symbols", required=True)
    audit.add_argument("--start", type=_parse_date)
    audit.add_argument("--end", type=_parse_date)
    audit.add_argument("--data-dir", type=Path, default=DATA_DIR)
    audit.add_argument("--output-dir", type=Path, required=True)

    semantic = subparsers.add_parser("semantic-batch")
    semantic.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    semantic.add_argument("--symbols", required=True)
    semantic.add_argument("--start", required=True, type=_parse_date)
    semantic.add_argument("--end", required=True, type=_parse_date)
    semantic.add_argument("--data-dir", type=Path)
    semantic.add_argument("--readiness-json", type=Path, required=True)
    semantic.add_argument("--batch-id", required=True)
    semantic.add_argument("--batch-size", type=int, default=12)
    semantic.add_argument("--eligibility-start", type=_parse_date)
    semantic.add_argument("--eligibility-end", type=_parse_date)
    semantic.add_argument("--output-dir", type=Path, required=True)

    calibration_v2 = subparsers.add_parser("semantic-calibration-batch-v2")
    calibration_v2.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    calibration_v2.add_argument("--symbols", required=True)
    calibration_v2.add_argument("--start", required=True, type=_parse_date)
    calibration_v2.add_argument("--end", required=True, type=_parse_date)
    calibration_v2.add_argument("--data-dir", type=Path)
    calibration_v2.add_argument("--readiness-json", type=Path, required=True)
    calibration_v2.add_argument("--batch-id", required=True)
    calibration_v2.add_argument("--confirmed-signal-count", type=int, default=6)
    calibration_v2.add_argument("--qualified-no-trigger-count", type=int, default=6)
    calibration_v2.add_argument("--rejected-geometry-count", type=int, default=6)
    calibration_v2.add_argument("--eligibility-start", type=_parse_date)
    calibration_v2.add_argument("--eligibility-end", type=_parse_date)
    calibration_v2.add_argument("--exclude-manifest", type=Path, action="append", default=[])
    calibration_v2.add_argument("--output-dir", type=Path, required=True)

    verify = subparsers.add_parser("verify-semantic-batch")
    verify.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    verify.add_argument("--batch-dir", type=Path, required=True)

    verify_calibration_v2 = subparsers.add_parser("verify-semantic-calibration-batch-v2")
    verify_calibration_v2.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    verify_calibration_v2.add_argument("--batch-dir", type=Path, required=True)

    ingest_calibration_v2 = subparsers.add_parser("ingest-semantic-calibration-responses-v2")
    ingest_calibration_v2.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    ingest_calibration_v2.add_argument("--batch-dir", type=Path, required=True)
    ingest_calibration_v2.add_argument("--responses-csv", type=Path)

    render_calibration_gate_v2 = subparsers.add_parser("render-semantic-calibration-gate-v2")
    render_calibration_gate_v2.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    render_calibration_gate_v2.add_argument("--batch-dir", type=Path, required=True)
    render_calibration_gate_v2.add_argument("--card-id", action="append", required=True)
    render_calibration_gate_v2.add_argument("--output", type=Path, required=True)

    init_source_fidelity_v3 = subparsers.add_parser("init-source-fidelity-review-v3")
    init_source_fidelity_v3.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    init_source_fidelity_v3.add_argument("--batch-dir", type=Path, required=True)
    init_source_fidelity_v3.add_argument("--rubric", type=Path, required=True)

    verify_source_fidelity_v3 = subparsers.add_parser("verify-source-fidelity-review-v3")
    verify_source_fidelity_v3.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    verify_source_fidelity_v3.add_argument("--batch-dir", type=Path, required=True)

    ingest_source_fidelity_v3 = subparsers.add_parser("ingest-source-fidelity-responses-v3")
    ingest_source_fidelity_v3.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    ingest_source_fidelity_v3.add_argument("--batch-dir", type=Path, required=True)
    ingest_source_fidelity_v3.add_argument("--responses-csv", type=Path)

    freeze_source_fidelity_v3 = subparsers.add_parser("freeze-source-fidelity-v3")
    freeze_source_fidelity_v3.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    freeze_source_fidelity_v3.add_argument("--batch-dir", type=Path, required=True)
    freeze_source_fidelity_v3.add_argument("--detector-git-commit", required=True)

    ingest = subparsers.add_parser("ingest-semantic-responses")
    ingest.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    ingest.add_argument("--batch-dir", type=Path, required=True)
    ingest.add_argument("--responses-csv", type=Path)

    obsidian = subparsers.add_parser("render-obsidian-review")
    obsidian.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    obsidian.add_argument("--batch-dir", type=Path, required=True)
    obsidian.add_argument("--output", type=Path, required=True)

    fixture = subparsers.add_parser("fixture-shadow")
    fixture.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    fixture.add_argument("--daily-csv", type=Path, required=True)
    fixture.add_argument("--run-id", required=True)
    fixture.add_argument("--output-dir", type=Path, required=True)

    cache = subparsers.add_parser("run-cache")
    cache.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    cache.add_argument("--symbols", required=True)
    cache.add_argument("--start", required=True, type=_parse_date)
    cache.add_argument("--end", required=True, type=_parse_date)
    cache.add_argument("--data-dir", type=Path)
    cache.add_argument("--run-id", required=True)
    cache.add_argument("--output-dir", type=Path, required=True)

    acquire_public = subparsers.add_parser("acquire-public-daily")
    acquire_public.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    acquire_public.add_argument(
        "--universe", type=Path, default=DEFAULT_PUBLIC_UNIVERSE
    )
    acquire_public.add_argument("--output-dir", type=Path, required=True)
    acquire_public.add_argument("--request-delay-seconds", type=float, default=0.12)

    run_public = subparsers.add_parser("run-public-daily")
    run_public.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    run_public.add_argument(
        "--universe", type=Path, default=DEFAULT_PUBLIC_UNIVERSE
    )
    run_public.add_argument("--dataset-dir", type=Path, required=True)
    run_public.add_argument("--semantic-freeze", type=Path, required=True)
    run_public.add_argument("--run-id", required=True)
    run_public.add_argument("--output-dir", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = load_rectangle_config(args.config)
    if args.command == "validate-config":
        print("STATUS=valid")
        print(f"PLAYBOOK_ID={config.playbook_id}")
        print(f"CONFIG_HASH={config.source_hash}")
        return 0
    if args.command == "audit-cache":
        symbols = [value.strip().upper() for value in args.symbols.split(",") if value.strip()]
        report = audit_local_cache(
            symbols=symbols,
            config=config,
            data_dir=args.data_dir,
            start=args.start,
            end=args.end,
        )
        paths = write_readiness_report(report, args.output_dir)
        print(f"STATUS={report.semantic_review_status}")
        print(f"ECONOMIC_STATUS={report.economic_research_status}")
        print(f"REPORT_HASH={report.report_hash}")
        print(f"REPORT_JSON={paths['json'].resolve()}")
        return 0
    if args.command == "acquire-public-daily":
        universe = load_public_validation_universe(args.universe)
        if universe["config_hash"] != config.source_hash:
            raise ValueError("Public universe uses a stale rectangle config hash.")
        if _git_state()["dirty"]:
            raise ValueError("Public dataset acquisition requires a clean Git tree.")
        result = acquire_public_daily_dataset(
            universe_path=args.universe,
            output_dir=args.output_dir,
            request_delay_seconds=args.request_delay_seconds,
        )
        print(f"STATUS={result.quality_status}")
        print(f"DATASET_ID={result.dataset_id}")
        print(f"CANONICAL_HASH={result.canonical_hash}")
        print(f"MANIFEST={result.manifest_path}")
        print(f"QUALITY_REPORT={result.report_path}")
        return 0
    if args.command == "verify-semantic-batch":
        receipt = verify_semantic_batch(args.batch_dir)
        if receipt.get("config_hash") != config.source_hash:
            raise ValueError("Semantic batch uses a stale config hash.")
        print("STATUS=valid")
        print(f"BATCH_ID={receipt['batch_id']}")
        print(f"CANONICAL_HASH={receipt['canonical_hash']}")
        return 0
    if args.command == "verify-semantic-calibration-batch-v2":
        receipt = verify_semantic_calibration_batch_v2(args.batch_dir)
        if receipt.get("config_hash") != config.source_hash:
            raise ValueError("Semantic calibration batch uses a stale config hash.")
        print("STATUS=valid")
        print(f"BATCH_ID={receipt['batch_id']}")
        print(f"CANONICAL_HASH={receipt['canonical_hash']}")
        return 0
    if args.command == "ingest-semantic-calibration-responses-v2":
        receipt = verify_semantic_calibration_batch_v2(args.batch_dir)
        if receipt.get("config_hash") != config.source_hash:
            raise ValueError("Semantic calibration batch uses a stale config hash.")
        result = ingest_calibration_review_responses_v2(
            batch_dir=args.batch_dir,
            responses_csv=args.responses_csv,
        )
        print(f"STATUS={result.status}")
        print(f"REVIEWED={result.reviewed_count}")
        print(f"SCORECARD={result.scorecard_path}")
        return 0
    if args.command == "render-semantic-calibration-gate-v2":
        receipt = verify_semantic_calibration_batch_v2(args.batch_dir)
        if receipt.get("config_hash") != config.source_hash:
            raise ValueError("Semantic calibration batch uses a stale config hash.")
        output = render_calibration_obsidian_gate_v2(
            batch_dir=args.batch_dir,
            output_path=args.output,
            card_ids=args.card_id,
        )
        print("STATUS=complete")
        print(f"OUTPUT={output}")
        return 0
    if args.command == "init-source-fidelity-review-v3":
        base_receipt = verify_semantic_calibration_batch_v2(args.batch_dir)
        if base_receipt.get("config_hash") != config.source_hash:
            raise ValueError("Source-fidelity base batch uses a stale config hash.")
        result = initialize_source_fidelity_review_v3(
            batch_dir=args.batch_dir,
            rubric_path=args.rubric,
        )
        print("STATUS=complete")
        print(f"BATCH_ID={result.batch_id}")
        print(f"CANONICAL_HASH={result.canonical_hash}")
        print(f"REVIEW_DIR={result.review_dir}")
        return 0
    if args.command == "verify-source-fidelity-review-v3":
        base_receipt = verify_semantic_calibration_batch_v2(args.batch_dir)
        if base_receipt.get("config_hash") != config.source_hash:
            raise ValueError("Source-fidelity base batch uses a stale config hash.")
        receipt = verify_source_fidelity_review_v3(args.batch_dir)
        print("STATUS=valid")
        print(f"BATCH_ID={receipt['batch_id']}")
        print(f"CANONICAL_HASH={receipt['canonical_hash']}")
        return 0
    if args.command == "ingest-source-fidelity-responses-v3":
        base_receipt = verify_semantic_calibration_batch_v2(args.batch_dir)
        if base_receipt.get("config_hash") != config.source_hash:
            raise ValueError("Source-fidelity base batch uses a stale config hash.")
        result = ingest_source_fidelity_responses_v3(
            batch_dir=args.batch_dir,
            responses_csv=args.responses_csv,
        )
        print("STATUS=complete")
        print(f"REVIEWED={result.reviewed_count}")
        print(f"COMPLETE_PASSES={result.complete_review_pass_count}")
        print(f"SCORECARD={result.scorecard_path}")
        return 0
    if args.command == "freeze-source-fidelity-v3":
        base_receipt = verify_semantic_calibration_batch_v2(args.batch_dir)
        if base_receipt.get("config_hash") != config.source_hash:
            raise ValueError("Source-fidelity base batch uses a stale config hash.")
        git = _git_state()
        if git["dirty"]:
            raise ValueError("Semantic freeze requires a clean Git tree.")
        if git["commit"] != args.detector_git_commit:
            raise ValueError("Semantic freeze detector commit does not match HEAD.")
        freeze_path = freeze_mala_rectangle_semantic_spec_v1(
            batch_dir=args.batch_dir,
            detector_git_commit=args.detector_git_commit,
        )
        payload = json.loads(freeze_path.read_text(encoding="utf-8"))
        print(f"STATUS={payload['status']}")
        print(f"FREEZE={freeze_path}")
        print(f"CANONICAL_HASH={payload['canonical_hash']}")
        return 0
    if args.command == "ingest-semantic-responses":
        receipt = verify_semantic_batch(args.batch_dir)
        if receipt.get("config_hash") != config.source_hash:
            raise ValueError("Semantic batch uses a stale config hash.")
        result = ingest_review_responses(
            batch_dir=args.batch_dir,
            responses_csv=args.responses_csv,
        )
        print(f"STATUS={result.status}")
        print(f"REVIEWED={result.reviewed_count}")
        print(f"TOTAL={result.total_count}")
        print(f"SCORECARD={result.scorecard_path}")
        return 0
    if args.command == "render-obsidian-review":
        receipt = verify_semantic_batch(args.batch_dir)
        if receipt.get("config_hash") != config.source_hash:
            raise ValueError("Semantic batch uses a stale config hash.")
        output = render_obsidian_review_card(
            batch_dir=args.batch_dir,
            output_path=args.output,
        )
        print("STATUS=complete")
        print(f"BATCH_ID={receipt['batch_id']}")
        print(f"OBSIDIAN_SOURCE={output}")
        return 0
    if args.command == "semantic-batch":
        symbols = [value.strip().upper() for value in args.symbols.split(",") if value.strip()]
        daily = _load_cache(
            symbols,
            start=args.start,
            end=args.end,
            data_dir=args.data_dir,
            config=config,
        )
        result = build_semantic_review_batch(
            daily,
            config=config,
            readiness=load_readiness_report(args.readiness_json),
            output_dir=args.output_dir,
            batch_id=args.batch_id,
            batch_size=args.batch_size,
            eligibility_start=args.eligibility_start,
            eligibility_end=args.eligibility_end,
        )
        print("STATUS=complete")
        print("READINESS=semantic_pilot")
        print(f"BATCH_ID={result.batch_id}")
        print(f"SELECTED={result.selected_signal_count}")
        print(f"CANONICAL_HASH={result.canonical_hash}")
        print(f"REVIEW_INDEX={result.review_index_path}")
        return 0
    if args.command == "semantic-calibration-batch-v2":
        symbols = [value.strip().upper() for value in args.symbols.split(",") if value.strip()]
        daily = _load_cache(
            symbols,
            start=args.start,
            end=args.end,
            data_dir=args.data_dir,
            config=config,
        )
        result = build_semantic_calibration_batch_v2(
            daily,
            config=config,
            readiness=load_readiness_report(args.readiness_json),
            output_dir=args.output_dir,
            batch_id=args.batch_id,
            confirmed_signal_count=args.confirmed_signal_count,
            qualified_no_trigger_count=args.qualified_no_trigger_count,
            rejected_geometry_count=args.rejected_geometry_count,
            eligibility_start=args.eligibility_start,
            eligibility_end=args.eligibility_end,
            exclude_manifests=args.exclude_manifest,
        )
        print("STATUS=complete")
        print("READINESS=semantic_calibration")
        print(f"BATCH_ID={result.batch_id}")
        print(f"SELECTED={result.selected_count}")
        print(f"CANONICAL_HASH={result.canonical_hash}")
        print(f"REVIEW_INDEX={result.review_index_path}")
        return 0
    if args.command == "fixture-shadow":
        daily = _load_daily_csv(args.daily_csv)
        mode = "fixture_shadow"
    elif args.command == "run-public-daily":
        if _git_state()["dirty"]:
            raise ValueError("Public validation requires a clean Git tree.")
        verify_public_daily_dataset_against_universe(
            output_dir=args.dataset_dir,
            universe_path=args.universe,
            config_hash=config.source_hash,
        )
        daily, dataset = load_public_daily_dataset(args.dataset_dir)
        if dataset["config_hash"] != config.source_hash:
            raise ValueError("Public dataset uses a stale rectangle config hash.")
        freeze = verify_semantic_freeze_for_public_run(
            freeze_path=args.semantic_freeze,
            config_hash=config.source_hash,
        )
        receipt = run_research(
            daily,
            config=config,
            output_dir=args.output_dir,
            run_id=args.run_id,
            mode="public_daily_research",
            argv=argv,
            phase="deterministic Public frozen-cohort validation and holdout",
            readiness="public_best_effort_frozen_cohort_validation",
            data_context={
                "dataset_id": dataset["dataset_id"],
                "dataset_manifest_hash": dataset["canonical_hash"],
                "dataset_quality_status": dataset["quality_status"],
                "economic_research_grade": dataset["economic_research_grade"],
                "universe_hash": dataset["universe_hash"],
                "universe_status": dataset["universe_status"],
                "provider_adjustment_provenance": dataset["adjustment_provenance"],
                "semantic_freeze_hash": freeze["canonical_hash"],
            },
            warnings=[
                "This is a frozen current-symbol cohort, not a point-in-time market universe or population-alpha claim.",
                "Public split continuity was checked empirically; the provider adjustment policy remains undocumented.",
                "This receipt is non-executable and does not authorize shadow or live trading.",
            ],
        )
        print(f"STATUS={receipt['status']}")
        print(f"RUN_ID={receipt['run_id']}")
        print(f"RUN_DIR={args.output_dir.expanduser().resolve()}")
        print(f"RECEIPT_JSON={(args.output_dir / 'receipt.json').expanduser().resolve()}")
        print(f"SIGNALS={receipt['population'].get('representative_signals', 0)}")
        return 0
    else:
        symbols = [value.strip().upper() for value in args.symbols.split(",") if value.strip()]
        daily = _load_cache(
            symbols,
            start=args.start,
            end=args.end,
            data_dir=args.data_dir,
            config=config,
        )
        mode = "local_cache_research"
    receipt = run_research(
        daily,
        config=config,
        output_dir=args.output_dir,
        run_id=args.run_id,
        mode=mode,
        argv=argv,
    )
    print(f"STATUS={receipt['status']}")
    print(f"RUN_ID={receipt['run_id']}")
    print(f"RUN_DIR={args.output_dir.expanduser().resolve()}")
    print(f"RECEIPT_JSON={(args.output_dir / 'receipt.json').expanduser().resolve()}")
    print(f"SIGNALS={receipt['population'].get('representative_signals', 0)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
