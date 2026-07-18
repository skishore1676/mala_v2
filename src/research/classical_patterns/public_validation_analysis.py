"""Deterministic robustness analysis for a frozen Public rectangle run."""

from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import random
from statistics import median
import subprocess
from typing import Any, Sequence

import polars as pl


BOOTSTRAP_ITERATIONS = 10_000
BOOTSTRAP_SEED = 20_260_717
MINIMUM_DIRECTIONAL_CLOSED_TRADES = 20


def analyze_public_validation(
    *, run_dir: Path, output_dir: Path | None = None
) -> dict[str, Any]:
    """Write uncertainty, replication, concentration, and Obsidian artifacts."""

    run_dir = run_dir.expanduser().resolve()
    output_dir = (output_dir or run_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    receipt_path = run_dir / "receipt.json"
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    if (
        receipt.get("mode") != "public_daily_research"
        or receipt.get("executable") is not False
        or receipt.get("status") != "complete"
    ):
        raise ValueError("Analysis requires a complete, non-executable Public daily run.")
    _verify_receipt_artifact(run_dir, receipt, "trades.csv")
    _verify_receipt_artifact(run_dir, receipt, "economic_scorecard.csv")

    trades = pl.read_csv(run_dir / "trades.csv", try_parse_dates=True)
    required = {"split", "direction", "variant_id", "symbol", "status", "net_r"}
    if not required <= set(trades.columns):
        raise ValueError("Public trades artifact is missing analysis fields.")
    closed = trades.filter(
        (pl.col("status") == "closed") & pl.col("net_r").is_not_null()
    )
    if closed.is_empty():
        raise ValueError("Public run has no closed trades to analyze.")

    scorecard_rows = _robustness_rows(closed)
    replication_rows = _replication_rows(scorecard_rows)
    concentration_rows = _concentration_rows(closed)
    replicated = [
        row for row in replication_rows if row["replication_status"] == "replicated_positive"
    ]
    verdict = (
        "candidate_replicated_alpha_requires_broader_universe"
        if replicated
        else "no_replicated_alpha"
    )

    robustness_path = output_dir / "robustness_scorecard.csv"
    replication_path = output_dir / "replication_scorecard.csv"
    concentration_path = output_dir / "symbol_concentration.csv"
    pl.DataFrame(scorecard_rows).write_csv(robustness_path)
    pl.DataFrame(replication_rows).write_csv(replication_path)
    pl.DataFrame(concentration_rows).write_csv(concentration_path)

    analysis: dict[str, Any] = {
        "schema_version": "PublicRectangleValidationAnalysisV1",
        "run_id": receipt["run_id"],
        "created_at": datetime.now(timezone.utc).isoformat(),
        "verdict": verdict,
        "recommended_action": (
            "record_negative_result_and_do_not_retune_on_holdout"
            if not replicated
            else "replicate_without_retuning_on_broader_point_in_time_data"
        ),
        "executable": False,
        "source": {
            "receipt_path": "receipt.json",
            "receipt_file_hash": _sha256_path(receipt_path),
            "dataset_manifest_hash": receipt["data"]["dataset_manifest_hash"],
            "semantic_freeze_hash": receipt["data"]["semantic_freeze_hash"],
            "config_hash": receipt["config"]["hash"],
        },
        "method": {
            "bootstrap_iterations": BOOTSTRAP_ITERATIONS,
            "bootstrap_seed": BOOTSTRAP_SEED,
            "confidence_interval": "nonparametric_trade_level_percentile_95",
            "minimum_directional_closed_trades": MINIMUM_DIRECTIONAL_CLOSED_TRADES,
            "minimum_sample_rule_status": (
                "post_run_descriptive_evidence_floor_not_original_protocol_gate"
            ),
            "replicated_positive_rule": (
                "validation and holdout average net R are positive, both 95% lower bounds "
                "are above zero, and both samples have at least 20 closed trades"
            ),
        },
        "counts": {
            "signals": receipt["population"]["representative_signals"],
            "symbols_with_signals": int(trades.get_column("symbol").n_unique()),
            "closed_trade_rows": len(closed),
            "variants": receipt["variants"]["count"],
            "replicated_positive_cells": len(replicated),
        },
        "artifacts": {
            "robustness_scorecard.csv": _file_meta(robustness_path, len(scorecard_rows)),
            "replication_scorecard.csv": _file_meta(replication_path, len(replication_rows)),
            "symbol_concentration.csv": _file_meta(concentration_path, len(concentration_rows)),
        },
        "git": _git_state(),
        "limitations": [
            "Trade-level bootstrap does not remove cross-symbol or market-regime dependence.",
            "The two stop variants share signals and are not independent strategies.",
            "The current-symbol cohort is not a point-in-time historical universe.",
            "Public corporate-action adjustment policy is empirically checked but undocumented.",
        ],
    }
    analysis["canonical_hash"] = _hash_json(analysis)
    analysis_path = output_dir / "validation_analysis.json"
    _write_json(analysis_path, analysis)
    obsidian_path = output_dir / "OBSIDIAN_REVIEW.md"
    obsidian_path.write_text(
        _render_obsidian_card(
            receipt=receipt,
            analysis=analysis,
            scorecard_rows=scorecard_rows,
            replication_rows=replication_rows,
            concentration_rows=concentration_rows,
        ),
        encoding="utf-8",
    )
    _write_report_artifact(
        output_dir=output_dir,
        receipt=receipt,
        analysis=analysis,
        scorecard_rows=scorecard_rows,
        replication_rows=replication_rows,
        concentration_rows=concentration_rows,
    )
    return analysis


def _robustness_rows(closed: pl.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for scope, dimensions in (
        ("directional", ["split", "direction", "variant_id"]),
        ("combined", ["split", "variant_id"]),
    ):
        for keys, group in closed.group_by(dimensions, maintain_order=True):
            key_values = keys if isinstance(keys, tuple) else (keys,)
            identity = dict(zip(dimensions, key_values))
            values = [float(value) for value in group.get_column("net_r").to_list()]
            lower, upper, probability_positive = _bootstrap_mean(values, identity)
            wins = [value for value in values if value > 0]
            losses = [value for value in values if value < 0]
            rows.append(
                {
                    "scope": scope,
                    "split": identity["split"],
                    "direction": identity.get("direction", "all"),
                    "variant_id": identity["variant_id"],
                    "closed_trades": len(values),
                    "symbol_count": int(group.get_column("symbol").n_unique()),
                    "win_rate": len(wins) / len(values),
                    "average_net_r": sum(values) / len(values),
                    "median_net_r": float(median(values)),
                    "total_net_r": sum(values),
                    "profit_factor": (
                        sum(wins) / abs(sum(losses))
                        if losses
                        else float("inf") if wins else None
                    ),
                    "mean_net_r_ci95_lower": lower,
                    "mean_net_r_ci95_upper": upper,
                    "bootstrap_probability_mean_positive": probability_positive,
                }
            )
    return sorted(
        rows,
        key=lambda row: (
            row["scope"], row["split"], row["direction"], row["variant_id"]
        ),
    )


def _replication_rows(scorecard_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    directional = {
        (row["split"], row["direction"], row["variant_id"]): row
        for row in scorecard_rows
        if row["scope"] == "directional"
    }
    pairs = sorted(
        {
            (row["direction"], row["variant_id"])
            for row in scorecard_rows
            if row["scope"] == "directional"
        }
    )
    rows: list[dict[str, Any]] = []
    for direction, variant_id in pairs:
        validation = directional.get(("validation", direction, variant_id))
        holdout = directional.get(("holdout", direction, variant_id))
        if validation is None or holdout is None:
            status = "missing_required_split"
        elif (
            validation["closed_trades"] >= MINIMUM_DIRECTIONAL_CLOSED_TRADES
            and holdout["closed_trades"] >= MINIMUM_DIRECTIONAL_CLOSED_TRADES
            and validation["mean_net_r_ci95_lower"] > 0
            and holdout["mean_net_r_ci95_lower"] > 0
        ):
            status = "replicated_positive"
        elif validation["average_net_r"] > 0 and holdout["average_net_r"] > 0:
            status = "same_sign_positive_but_uncertain"
        else:
            status = "not_replicated"
        rows.append(
            {
                "direction": direction,
                "variant_id": variant_id,
                "validation_closed_trades": validation["closed_trades"] if validation else 0,
                "validation_average_net_r": validation["average_net_r"] if validation else None,
                "validation_ci95_lower": validation["mean_net_r_ci95_lower"] if validation else None,
                "validation_ci95_upper": validation["mean_net_r_ci95_upper"] if validation else None,
                "holdout_closed_trades": holdout["closed_trades"] if holdout else 0,
                "holdout_average_net_r": holdout["average_net_r"] if holdout else None,
                "holdout_ci95_lower": holdout["mean_net_r_ci95_lower"] if holdout else None,
                "holdout_ci95_upper": holdout["mean_net_r_ci95_upper"] if holdout else None,
                "replication_status": status,
            }
        )
    return rows


def _concentration_rows(closed: pl.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for keys, group in closed.group_by(["split", "variant_id"], maintain_order=True):
        split, variant_id = keys
        totals: defaultdict[str, float] = defaultdict(float)
        for symbol, net_r in group.select("symbol", "net_r").iter_rows():
            totals[str(symbol)] += float(net_r)
        ranked = sorted(totals.items(), key=lambda item: abs(item[1]), reverse=True)
        denominator = sum(abs(value) for value in totals.values())
        rows.append(
            {
                "split": split,
                "variant_id": variant_id,
                "symbol_count": len(totals),
                "largest_absolute_contributor": ranked[0][0],
                "largest_contributor_total_net_r": ranked[0][1],
                "largest_absolute_contribution_share": (
                    abs(ranked[0][1]) / denominator if denominator else 0.0
                ),
                "top_five_absolute_contribution_share": (
                    sum(abs(value) for _, value in ranked[:5]) / denominator
                    if denominator
                    else 0.0
                ),
            }
        )
    return sorted(rows, key=lambda row: (row["split"], row["variant_id"]))


def _bootstrap_mean(
    values: list[float], identity: dict[str, Any]
) -> tuple[float, float, float]:
    identity_seed = int(
        hashlib.sha256(
            json.dumps(identity, sort_keys=True).encode("utf-8")
        ).hexdigest()[:12],
        16,
    )
    rng = random.Random(BOOTSTRAP_SEED + identity_seed)
    count = len(values)
    means = sorted(
        sum(values[rng.randrange(count)] for _ in range(count)) / count
        for _ in range(BOOTSTRAP_ITERATIONS)
    )
    return (
        _percentile(means, 0.025),
        _percentile(means, 0.975),
        sum(value > 0 for value in means) / len(means),
    )


def _percentile(ordered: list[float], quantile: float) -> float:
    position = quantile * (len(ordered) - 1)
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    weight = position - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def _verify_receipt_artifact(
    run_dir: Path, receipt: dict[str, Any], name: str
) -> None:
    metadata = receipt.get("artifacts", {}).get(name)
    if not metadata:
        raise ValueError(f"Public receipt does not declare {name}.")
    path = run_dir / metadata["path"]
    if _sha256_path(path) != metadata["content_hash"]:
        raise ValueError(f"Public run artifact hash mismatch: {name}")


def _render_obsidian_card(
    *,
    receipt: dict[str, Any],
    analysis: dict[str, Any],
    scorecard_rows: list[dict[str, Any]],
    replication_rows: list[dict[str, Any]],
    concentration_rows: list[dict[str, Any]],
) -> str:
    combined = [row for row in scorecard_rows if row["scope"] == "combined"]
    lines = [
        f"# Rectangle Public Validation — {receipt['run_id']}",
        "",
        "## Verdict",
        "",
        "**No replicated alpha in this frozen run. Do not retune from the holdout.**",
        "",
        "Validation shorts were positive, but both short variants reversed to materially",
        "negative average net R in holdout. Validation longs were negative; holdout longs",
        "were approximately flat. Every directional confidence interval crosses zero, and",
        "each directional split has fewer than this report's descriptive 20-trade evidence floor.",
        "",
        "<Suman comment: leave blank if you agree with recording this as a negative result.>",
        "",
        "## What Was Tested",
        "",
        f"- 43 frozen current symbols; {receipt['population']['representative_signals']} complete-population signals.",
        f"- {analysis['counts']['closed_trade_rows']} closed variant rows across two predeclared LFD stop buffers.",
        "- Calibration, validation, and holdout boundaries were unchanged.",
        "- Semantic review had no authority to include, remove, or weight economic events.",
        "- Dataset coverage was 100% for all symbols and three known split checks passed.",
        "",
        "## Validation → Holdout Replication",
        "",
        "| Direction | Stop buffer | Validation avg R (95% CI) | n | Holdout avg R (95% CI) | n | Status |",
        "|---|---|---:|---:|---:|---:|---|",
    ]
    for row in replication_rows:
        lines.append(
            f"| {row['direction']} | {_variant_label(row['variant_id'])} | "
            f"{row['validation_average_net_r']:+.3f} "
            f"[{row['validation_ci95_lower']:+.3f}, {row['validation_ci95_upper']:+.3f}] | "
            f"{row['validation_closed_trades']} | {row['holdout_average_net_r']:+.3f} "
            f"[{row['holdout_ci95_lower']:+.3f}, {row['holdout_ci95_upper']:+.3f}] | "
            f"{row['holdout_closed_trades']} | `{row['replication_status']}` |"
        )
    lines.extend(
        [
            "",
            "## Combined Direction Readout",
            "",
            "| Split | Stop buffer | Avg net R | 95% CI | Closed | Win rate | Profit factor |",
            "|---|---|---:|---:|---:|---:|---:|",
        ]
    )
    for row in sorted(combined, key=lambda item: (item["split"], item["variant_id"])):
        lines.append(
            f"| {row['split']} | {_variant_label(row['variant_id'])} | "
            f"{row['average_net_r']:+.3f} | [{row['mean_net_r_ci95_lower']:+.3f}, "
            f"{row['mean_net_r_ci95_upper']:+.3f}] | {row['closed_trades']} | "
            f"{row['win_rate']:.1%} | {row['profit_factor']:.2f} |"
        )
    lines.extend(
        [
            "",
            "## Stability and Data Caveats",
            "",
        ]
    )
    for row in concentration_rows:
        if row["split"] == "holdout":
            lines.append(
                f"- Holdout {_variant_label(row['variant_id'])}: largest absolute symbol "
                f"contributor `{row['largest_absolute_contributor']}` accounts for "
                f"{row['largest_absolute_contribution_share']:.1%}; top five account for "
                f"{row['top_five_absolute_contribution_share']:.1%}."
            )
    lines.extend(
        [
            "- Bootstrap intervals treat trades as exchangeable; common market-regime dependence remains.",
            "- The two variants share the same signals, so agreement between them is not independent replication.",
            "- This is a current-symbol frozen cohort, not a point-in-time market universe.",
            "- Public adjustment behavior passed known-split continuity checks but remains undocumented.",
            "",
            "## Recommended Next Step",
            "",
            "Record rectangle v1 as a clean negative/insufficient economic result on this cohort.",
            "Do not use the holdout to alter thresholds. The next autonomous lane should test a",
            "new, separately versioned Brandt pattern hypothesis or obtain broader point-in-time",
            "data only if we want to replicate this exact unchanged rectangle rule.",
            "",
            "<Suman comment: add only if you want to override the recommended next lane.>",
            "",
            "## Evidence Identity",
            "",
            f"- Dataset manifest: `{receipt['data']['dataset_manifest_hash']}`",
            f"- Semantic freeze: `{receipt['data']['semantic_freeze_hash']}`",
            f"- Detector config: `{receipt['config']['hash']}`",
            f"- Run commit: `{receipt['git']['commit']}`",
            f"- Analysis: `{analysis['canonical_hash']}`",
            "- Executable: `false`",
            "",
        ]
    )
    return "\n".join(lines)


def _variant_label(variant_id: str) -> str:
    return variant_id.removeprefix("lfd_buffer_").replace("p", ".").replace("atr", " ATR")


def _write_report_artifact(
    *,
    output_dir: Path,
    receipt: dict[str, Any],
    analysis: dict[str, Any],
    scorecard_rows: list[dict[str, Any]],
    replication_rows: list[dict[str, Any]],
    concentration_rows: list[dict[str, Any]],
) -> None:
    directional = [row for row in scorecard_rows if row["scope"] == "directional"]
    combined = [row for row in scorecard_rows if row["scope"] == "combined"]
    variants = sorted({row["variant_id"] for row in scorecard_rows})
    direction_chart_rows: list[dict[str, Any]] = []
    for split in ("calibration", "validation", "holdout"):
        for direction in ("long", "short"):
            row: dict[str, Any] = {
                "cell": f"{split.title()} {direction}",
                "split": split,
                "direction": direction,
            }
            for variant in variants:
                match = next(
                    item
                    for item in directional
                    if item["split"] == split
                    and item["direction"] == direction
                    and item["variant_id"] == variant
                )
                row[_variant_field(variant)] = match["average_net_r"]
                row[_variant_field(variant) + "_closed"] = match["closed_trades"]
                row[_variant_field(variant) + "_ci_lower"] = match[
                    "mean_net_r_ci95_lower"
                ]
                row[_variant_field(variant) + "_ci_upper"] = match[
                    "mean_net_r_ci95_upper"
                ]
            direction_chart_rows.append(row)
    combined_chart_rows: list[dict[str, Any]] = []
    for split in ("calibration", "validation", "holdout"):
        row = {"split": split.title()}
        for variant in variants:
            match = next(
                item
                for item in combined
                if item["split"] == split and item["variant_id"] == variant
            )
            row[_variant_field(variant)] = match["average_net_r"]
            row[_variant_field(variant) + "_closed"] = match["closed_trades"]
            row[_variant_field(variant) + "_ci_lower"] = match[
                "mean_net_r_ci95_lower"
            ]
            row[_variant_field(variant) + "_ci_upper"] = match[
                "mean_net_r_ci95_upper"
            ]
        combined_chart_rows.append(row)
    base_holdout = next(
        row
        for row in combined
        if row["split"] == "holdout" and row["variant_id"] == variants[0]
    )
    headline_rows = [
        {
            "signals": analysis["counts"]["signals"],
            "closed_variant_rows": analysis["counts"]["closed_trade_rows"],
            "replicated_positive_cells": analysis["counts"]["replicated_positive_cells"],
            "holdout_average_net_r": base_holdout["average_net_r"],
        }
    ]
    sources = [
        {
            "id": "run-source",
            "label": "Frozen Public rectangle run",
            "path": "trades.csv",
            "query": {
                "description": "Complete closed-trade population from the hash-bound Public run.",
                "language": "python",
                "query": "python -m src.research.classical_patterns.public_validation_analysis --run-dir <run_dir>",
                "filters": [
                    "status = closed for expectancy metrics",
                    "all frozen cohort symbols",
                    "all predeclared variants",
                ],
                "metric_definitions": [
                    "Average net R is the arithmetic mean of closed-trade net_r after configured costs.",
                    "Closed variant rows count one result per signal and stop-buffer variant.",
                ],
            },
        },
        {
            "id": "analysis-source",
            "label": "Deterministic robustness analysis",
            "path": "validation_analysis.json",
            "query": {
                "description": "Seeded trade-level percentile bootstrap and split replication checks.",
                "language": "python",
                "query": "python -m src.research.classical_patterns.public_validation_analysis --run-dir <run_dir>",
                "metric_definitions": [
                    "95% intervals are 2.5th and 97.5th percentiles from 10,000 seeded trade-level bootstrap means.",
                    "Replicated positive requires positive validation and holdout lower bounds plus at least 20 closed trades in each split; the sample floor is descriptive and was not an original protocol gate.",
                ],
            },
        },
    ]
    artifact = {
        "surface": "report",
        "manifest": {
            "version": 1,
            "surface": "report",
            "title": "Classical Rectangle Public Validation",
            "description": "Frozen-cohort economic validation and robustness report.",
            "generatedAt": analysis["created_at"],
            "sources": sources,
            "cards": [
                {
                    "id": "signals-card",
                    "dataset": "headline",
                    "sourceId": "run-source",
                    "description": "Complete causal signals emitted across the 43-symbol cohort.",
                    "metrics": [{"label": "Signals", "field": "signals", "format": "number"}],
                },
                {
                    "id": "closed-card",
                    "dataset": "headline",
                    "sourceId": "run-source",
                    "description": "Closed result rows; each signal contributes one row per stop variant.",
                    "metrics": [
                        {"label": "Closed variant rows", "field": "closed_variant_rows", "format": "number"}
                    ],
                },
                {
                    "id": "replicated-card",
                    "dataset": "headline",
                    "sourceId": "analysis-source",
                    "description": "Directional variant cells meeting the robustness report's replication rule.",
                    "metrics": [
                        {"label": "Replicated positive cells", "field": "replicated_positive_cells", "format": "number"}
                    ],
                },
                {
                    "id": "holdout-card",
                    "dataset": "headline",
                    "sourceId": "analysis-source",
                    "description": "Combined-direction holdout mean for the 0.00 ATR stop-buffer variant.",
                    "metrics": [
                        {"label": "Holdout average net R", "field": "holdout_average_net_r", "format": "number", "signed": True}
                    ],
                },
            ],
            "charts": [
                {
                    "id": "direction-chart",
                    "title": "Average net R by split and direction",
                    "subtitle": "43-symbol frozen cohort; closed trades only; grouped by the two predeclared LFD stop buffers",
                    "type": "bar",
                    "intent": "comparison",
                    "dataset": "direction_chart",
                    "sourceId": "analysis-source",
                    "encodings": {
                        "x": {"field": "cell", "type": "nominal", "label": "Split and direction"},
                        "y": {
                            "fields": [_variant_field(variant) for variant in variants],
                            "type": "quantitative",
                            "format": "number",
                            "label": "Average net R",
                        },
                    },
                    "xAxisTitle": "Split and direction",
                    "yAxisTitle": "Average net R",
                    "valueFormat": "number",
                    "layout": "full",
                    "referenceLines": [{"axis": "y", "value": 0, "label": "Break-even", "color": "neutral"}],
                    "settings": {"groupMode": "grouped", "showValues": True, "categoryLabelPolicy": "wrap"},
                    "labels": {"values": "auto"},
                    "legend": {"position": "bottom", "sort": "spec", "title": "Stop buffer"},
                    "palette": {"kind": "categorical", "name": "blue-orange"},
                    "surface": {"viewMode": "both", "showControls": True},
                },
                {
                    "id": "combined-chart",
                    "title": "Combined-direction average net R by split",
                    "subtitle": "Validation was slightly positive, but calibration and holdout were negative for both variants",
                    "type": "bar",
                    "intent": "comparison",
                    "dataset": "combined_chart",
                    "sourceId": "analysis-source",
                    "encodings": {
                        "x": {"field": "split", "type": "ordinal", "label": "Research split"},
                        "y": {
                            "fields": [_variant_field(variant) for variant in variants],
                            "type": "quantitative",
                            "format": "number",
                            "label": "Average net R",
                        },
                    },
                    "xAxisTitle": "Research split",
                    "yAxisTitle": "Average net R",
                    "valueFormat": "number",
                    "layout": "full",
                    "referenceLines": [{"axis": "y", "value": 0, "label": "Break-even", "color": "neutral"}],
                    "settings": {"groupMode": "grouped", "showValues": True},
                    "labels": {"values": "all"},
                    "legend": {"position": "bottom", "sort": "spec", "title": "Stop buffer"},
                    "palette": {"kind": "categorical", "name": "blue-orange"},
                    "surface": {"viewMode": "both", "showControls": True},
                },
            ],
            "tables": [
                {
                    "id": "replication-table",
                    "title": "Validation-to-holdout replication detail",
                    "subtitle": "Average net R and seeded 95% bootstrap intervals by direction and stop buffer",
                    "dataset": "replication",
                    "sourceId": "analysis-source",
                    "layout": "full",
                    "density": "dense",
                    "defaultSort": {"field": "direction", "direction": "asc"},
                    "columns": [
                        {"field": "direction", "label": "Direction", "type": "text"},
                        {"field": "stop_buffer", "label": "Stop buffer", "type": "text"},
                        {"field": "validation_average_net_r", "label": "Validation avg R", "format": "number"},
                        {"field": "validation_ci", "label": "Validation 95% CI", "type": "text"},
                        {"field": "validation_closed_trades", "label": "Validation n", "format": "number"},
                        {"field": "holdout_average_net_r", "label": "Holdout avg R", "format": "number"},
                        {"field": "holdout_ci", "label": "Holdout 95% CI", "type": "text"},
                        {"field": "holdout_closed_trades", "label": "Holdout n", "format": "number"},
                        {"field": "replication_status", "label": "Status", "type": "text"},
                    ],
                },
                {
                    "id": "concentration-table",
                    "title": "Symbol concentration by split and stop buffer",
                    "subtitle": "Absolute net-R contribution shares; concentration is descriptive, not a rescue filter",
                    "dataset": "concentration",
                    "sourceId": "analysis-source",
                    "layout": "full",
                    "density": "dense",
                    "defaultSort": {"field": "split", "direction": "asc"},
                    "columns": [
                        {"field": "split", "label": "Split", "type": "text"},
                        {"field": "stop_buffer", "label": "Stop buffer", "type": "text"},
                        {"field": "symbol_count", "label": "Symbols", "format": "number"},
                        {"field": "largest_absolute_contributor", "label": "Largest contributor", "type": "text"},
                        {"field": "largest_absolute_contribution_share", "label": "Largest share", "format": "percent"},
                        {"field": "top_five_absolute_contribution_share", "label": "Top-five share", "format": "percent"},
                    ],
                },
            ],
            "blocks": [
                {"id": "title", "type": "markdown", "body": "# Classical Rectangle Public Validation"},
                {
                    "id": "summary",
                    "type": "markdown",
                    "sourceId": "analysis-source",
                    "body": (
                        "## No replicated alpha in the frozen rectangle run\n\n"
                        "The validation short signal reversed in holdout, while validation longs were negative and holdout longs were approximately flat. Both variants produced negative combined holdout expectancy. This is a clean negative or insufficient result—not a basis for tuning on holdout."
                    ),
                },
                {"id": "metrics", "type": "metric-strip", "cardIds": ["signals-card", "closed-card", "replicated-card", "holdout-card"]},
                {
                    "id": "direction-heading",
                    "type": "markdown",
                    "body": "## Directional effects did not survive validation to holdout\n\nThe charts show average net R; the following table preserves sample sizes and uncertainty intervals.",
                },
                {"id": "direction-visual", "type": "chart", "chartId": "direction-chart"},
                {"id": "replication-detail", "type": "table", "tableId": "replication-table"},
                {
                    "id": "combined-heading",
                    "type": "markdown",
                    "body": "## The complete population was negative in calibration and holdout",
                },
                {"id": "combined-visual", "type": "chart", "chartId": "combined-chart"},
                {
                    "id": "scope-method",
                    "type": "markdown",
                    "sourceId": "run-source",
                    "body": (
                        "## Scope and method\n\n"
                        "The unchanged rectangle v1 detector scanned 43 frozen current symbols from 2021-07-19 through 2026-07-16. The run retained all causal signals and both predeclared Last Full Day stop-buffer variants. Calibration ends 2022-12-31, validation ends 2024-12-31, and the remaining history is holdout. Average net R includes configured costs and uses closed trades only."
                    ),
                },
                {
                    "id": "uncertainty",
                    "type": "markdown",
                    "sourceId": "analysis-source",
                    "body": (
                        "## Uncertainty remains wide\n\n"
                        "All validation and holdout directional 95% bootstrap intervals cross zero. The intervals use 10,000 seeded trade-level resamples and do not remove common market-regime dependence. The 20-trade sample floor is a post-run descriptive evidence floor, not an original protocol gate."
                    ),
                },
                {"id": "concentration-detail", "type": "table", "tableId": "concentration-table"},
                {
                    "id": "limitations",
                    "type": "markdown",
                    "body": (
                        "## What this result does not establish\n\n"
                        "The cohort is not a point-in-time market universe, Public's adjustment policy is undocumented despite passing three known-split continuity checks, and the two variants share the same signals. The report therefore cannot make a population-alpha or independent-replication claim."
                    ),
                },
                {
                    "id": "next",
                    "type": "markdown",
                    "body": (
                        "## Recommended next step\n\n"
                        "Record rectangle v1 as a negative or insufficient economic result and do not retune from holdout. Continue with a separately versioned Brandt pattern hypothesis, or replicate this exact unchanged rule only after obtaining broader point-in-time data. No artifact here authorizes shadow or live trading.\n\n"
                        "## Further questions\n\n"
                        "Would another classical pattern family have a more testable deterministic definition? If exact rectangle replication is desired, which point-in-time universe source can preserve delisted and renamed securities?"
                    ),
                },
            ],
        },
        "snapshot": {
            "version": 1,
            "generatedAt": analysis["created_at"],
            "status": "ready",
            "datasets": {
                "headline": headline_rows,
                "direction_chart": direction_chart_rows,
                "combined_chart": combined_chart_rows,
                "replication": [
                    {
                        **row,
                        "stop_buffer": _variant_label(row["variant_id"]),
                        "validation_ci": f"[{row['validation_ci95_lower']:+.3f}, {row['validation_ci95_upper']:+.3f}]",
                        "holdout_ci": f"[{row['holdout_ci95_lower']:+.3f}, {row['holdout_ci95_upper']:+.3f}]",
                    }
                    for row in replication_rows
                ],
                "concentration": [
                    {**row, "stop_buffer": _variant_label(row["variant_id"])}
                    for row in concentration_rows
                ],
            },
        },
        "sources": sources,
    }
    _write_json(output_dir / "artifact.json", artifact)


def _variant_field(variant_id: str) -> str:
    return variant_id.removeprefix("lfd_buffer_").removesuffix("atr") + "_average_net_r"


def _file_meta(path: Path, row_count: int) -> dict[str, Any]:
    return {"path": path.name, "row_count": row_count, "content_hash": _sha256_path(path)}


def _git_state() -> dict[str, Any]:
    def run(*args: str) -> str:
        return subprocess.run(
            ["git", *args], check=False, capture_output=True, text=True
        ).stdout.strip()

    dirty = [line for line in run("status", "--short").splitlines() if line]
    return {
        "commit": run("rev-parse", "HEAD"),
        "branch": run("branch", "--show-current"),
        "dirty": bool(dirty),
        "dirty_paths": dirty,
    }


def _sha256_path(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _hash_json(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    analysis = analyze_public_validation(run_dir=args.run_dir, output_dir=args.output_dir)
    target = (args.output_dir or args.run_dir).expanduser().resolve()
    print(f"VERDICT={analysis['verdict']}")
    print(f"CANONICAL_HASH={analysis['canonical_hash']}")
    print(f"ANALYSIS_JSON={target / 'validation_analysis.json'}")
    print(f"OBSIDIAN_REVIEW={target / 'OBSIDIAN_REVIEW.md'}")
    print(f"REPORT_ARTIFACT={target / 'artifact.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
