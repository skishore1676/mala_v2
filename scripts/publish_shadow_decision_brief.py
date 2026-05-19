#!/usr/bin/env python3
"""Publish a human-readable Mala/Bhiksha shadow decision brief."""

from __future__ import annotations

import csv
import re
import shutil
from collections import Counter
from datetime import datetime
from pathlib import Path


DEFAULT_VAULT = Path(
    "/Users/sunny/Documents/northstar"
)
DEFAULT_OUTPUT_DIR = "03 Agent Org/research_lab/Mala/Shadow"


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mala-root", default="/Users/sunny/Documents/mala_v2")
    parser.add_argument("--vault-root", default=str(DEFAULT_VAULT))
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--trading-date", default=datetime.now().date().isoformat())
    parser.add_argument("--daily-report", default="")
    parser.add_argument("--signal-ev-report", default="")
    args = parser.parse_args()

    mala_root = Path(args.mala_root).expanduser().resolve()
    vault_root = Path(args.vault_root).expanduser()
    publish_dir = vault_root / args.output_dir
    attachments_dir = publish_dir / "attachments" / args.trading_date
    publish_dir.mkdir(parents=True, exist_ok=True)
    attachments_dir.mkdir(parents=True, exist_ok=True)

    daily_report = _resolve_latest(
        args.daily_report,
        mala_root / "research/results/research_ops/shadow_campaign/daily",
        "shadow_daily_report_*.md",
    )
    signal_report = _resolve_latest(
        args.signal_ev_report,
        mala_root / "research/results/research_ops/bhiksha_signal_ev",
        "BHIKSHA_SIGNAL_EV_REPORT.md",
    )
    signal_dir = signal_report.parent if signal_report else None
    deployment_rows = _read_csv(signal_dir / "bhiksha_signal_ev_deployments.csv") if signal_dir else []
    counterfactual_rows = _read_csv(signal_dir / "bhiksha_signal_counterfactual.csv") if signal_dir else []

    report_metrics = _parse_report_metrics(signal_report)
    daily_metrics = _parse_report_metrics(daily_report)
    root_causes = Counter(row.get("root_cause", "") for row in counterfactual_rows if row.get("root_cause"))
    same_bar = report_metrics.get("same_bar_status", {})
    total_same_bar = sum(same_bar.values())
    match_rate = (same_bar.get("match", 0) / total_same_bar) if total_same_bar else None

    copied = []
    for source in [daily_report, signal_report]:
        if source and source.exists():
            target = attachments_dir / source.name
            shutil.copy2(source, target)
            copied.append((source, target))
    for name in [
        "bhiksha_signal_ev_deployments.csv",
        "bhiksha_signal_ev_trades.csv",
        "bhiksha_signal_events.csv",
        "bhiksha_signal_counterfactual.csv",
        "bhiksha_signal_counterfactual_summary.csv",
    ]:
        source = signal_dir / name if signal_dir else None
        if source and source.exists():
            target = attachments_dir / name
            shutil.copy2(source, target)
            copied.append((source, target))

    output = publish_dir / f"{args.trading_date}.md"
    output.write_text(
        _render_brief(
            trading_date=args.trading_date,
            daily_report=daily_report,
            signal_report=signal_report,
            report_metrics=report_metrics,
            daily_metrics=daily_metrics,
            deployment_rows=deployment_rows,
            root_causes=root_causes,
            match_rate=match_rate,
            copied=copied,
        ),
        encoding="utf-8",
    )
    print(f"SHADOW_DECISION_BRIEF={output}")
    return 0


def _resolve_latest(explicit: str, root: Path, pattern: str) -> Path | None:
    if explicit:
        path = Path(explicit).expanduser()
        return path if path.is_absolute() else path.resolve()
    if not root.exists():
        return None
    matches = sorted(root.glob(f"**/{pattern}"), key=lambda path: path.stat().st_mtime)
    return matches[-1] if matches else None


def _read_csv(path: Path | None) -> list[dict[str, str]]:
    if path is None or not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _parse_report_metrics(path: Path | None) -> dict:
    metrics: dict = {"same_bar_status": {}}
    if path is None or not path.exists():
        return metrics
    lines = path.read_text(encoding="utf-8").splitlines()
    section = ""
    for line in lines:
        if line.startswith("## "):
            section = line.removeprefix("## ").strip()
            continue
        bullet = re.match(r"- ([a-zA-Z0-9_]+): `?([^`]+)`?", line)
        if bullet:
            key, value = bullet.groups()
            metrics[key] = _coerce_number(value.strip())
            continue
        if section == "Same-Bar Mala Replay" and line.startswith("|") and "---" not in line and "Status" not in line:
            parts = [part.strip() for part in line.strip("|").split("|")]
            if len(parts) >= 2:
                metrics["same_bar_status"][parts[0]] = int(float(parts[1]))
    return metrics


def _coerce_number(value: str):
    try:
        if "." in value:
            return float(value)
        return int(value)
    except ValueError:
        return value


def _render_brief(
    *,
    trading_date: str,
    daily_report: Path | None,
    signal_report: Path | None,
    report_metrics: dict,
    daily_metrics: dict,
    deployment_rows: list[dict[str, str]],
    root_causes: Counter,
    match_rate: float | None,
    copied: list[tuple[Path, Path]],
) -> str:
    verdict = _overall_verdict(report_metrics, daily_metrics, root_causes, match_rate)
    lines = [
        f"# Mala Shadow Decision Brief - {trading_date}",
        "",
        f"- generated_at: `{datetime.now().replace(microsecond=0).isoformat()}`",
        f"- decision_verdict: **{verdict}**",
        f"- daily_report: `{daily_report or ''}`",
        f"- signal_ev_report: `{signal_report or ''}`",
        "",
        "## Plain-English Read",
        "",
        "- Signal Expected Value asks whether Bhiksha's Mala-sourced signals matched the intended strategy, became trade plans, and produced option results that support or weaken the expected edge.",
        "- The daily shadow report checks the session plumbing: deployments, signals, blocks, replay status, and runtime issues.",
        "",
        "## Next-Week Decision Questions",
        "",
        "1. Is Bhiksha adopting Mala correctly enough that strategy evidence is trustworthy?",
        "2. Is provider/broker divergence large enough that we must change the live data path or broker setup?",
        "3. When Mala and Bhiksha agree on the signal, does the options result support Mala expectancy?",
        "",
        "## Hard Decision Gates",
        "",
        "- Plumbing gate: same-bar match should be at least 95%, counterfactual misses/extras must have explained causes, and lifecycle/runtime issues should be zero or explicitly fixed.",
        "- Provider gate: repeated provider_feature_mismatch rows on traded deployments means move live feature computation to Polygon or pause that feature family.",
        "- Strategy gate: after at least 20 clean matched closed trades, negative average realized option R means stop polishing plumbing and pivot strategy or exit design.",
        "- Broker gate: broker change is only justified if quote/fill/lifecycle evidence is the blocker after the data-provider contract is clean.",
        "",
        "## Current Evidence",
        "",
        f"- true_signal_events: `{report_metrics.get('true_signal_events', '')}` - strategy conditions that fired in Bhiksha.",
        f"- trade_plans: `{report_metrics.get('trade_plans', '')}` - true signals that reached option trade planning.",
        f"- closed_trades_with_realized_signal_expected_value: `{report_metrics.get('closed_trades_with_realized_ev', '')}` - closed option trades with computed realized PnL/stop-R.",
        f"- total_realized_pnl_usd: `{report_metrics.get('total_realized_pnl_usd', '')}` - realized option PnL across closed trades in the lookback.",
        f"- positive_trades_vs_evidence: `{report_metrics.get('positive_trades_vs_evidence', '')}` - closed trades that supported the Mala edge.",
        f"- adverse_trades_vs_evidence: `{report_metrics.get('adverse_trades_vs_evidence', '')}` - closed trades that worked against the Mala edge.",
        f"- same_bar_match_rate: `{'' if match_rate is None else round(match_rate, 4)}` - share of Bhiksha signals also seen by same-bar Mala replay.",
        f"- daily_runtime_issue_rows: `{daily_metrics.get('runtime_issue_rows', '')}` - deployments with runtime/replay issues in the daily shadow report.",
        "",
        "## Root-Cause Pressure",
        "",
        "| Root Cause | Count |",
        "|---|---:|",
    ]
    for cause, count in root_causes.most_common(10):
        lines.append(f"| {cause or 'unknown'} | {count} |")
    if not root_causes:
        lines.append("| none | 0 |")

    lines.extend(
        [
            "",
            "## Deployment Focus",
            "",
            "| Deployment | Signals | Closed | PnL | Avg R | Mala Exp R | Verdict |",
            "|---|---:|---:|---:|---:|---:|---|",
        ]
    )
    for row in deployment_rows[:20]:
        lines.append(
            "| {deployment_id} | {signal_count} | {closed_trade_count} | {total_realized_pnl_usd} | "
            "{avg_realized_stop_r} | {mala_expected_r_used} | {operator_verdict} |".format(**row)
        )

    lines.extend(
        [
            "",
            "## Recommendation",
            "",
            _recommendation(report_metrics, daily_metrics, root_causes, match_rate),
            "",
            "## Artifact Copies",
            "",
        ]
    )
    for source, target in copied:
        lines.append(f"- `{target}` from `{source}`")
    return "\n".join(lines) + "\n"


def _overall_verdict(report_metrics: dict, daily_metrics: dict, root_causes: Counter, match_rate: float | None) -> str:
    runtime_issues = int(daily_metrics.get("runtime_issue_rows") or 0)
    if runtime_issues:
        return "YELLOW - plumbing fixes required"
    if match_rate is not None and match_rate < 0.95:
        return "YELLOW - signal/provider concordance not clean"
    closed = int(report_metrics.get("closed_trades_with_realized_ev") or 0)
    pnl = float(report_metrics.get("total_realized_pnl_usd") or 0.0)
    if closed >= 20 and pnl < 0:
        return "RED/YELLOW - clean strategy expectancy under pressure"
    if closed:
        return "YELLOW - observe until sample is decision-sized"
    return "YELLOW - insufficient closed-trade sample"


def _recommendation(report_metrics: dict, daily_metrics: dict, root_causes: Counter, match_rate: float | None) -> str:
    runtime_issues = int(daily_metrics.get("runtime_issue_rows") or 0)
    provider_mismatch = sum(count for cause, count in root_causes.items() if cause.startswith("provider_feature_mismatch"))
    closed = int(report_metrics.get("closed_trades_with_realized_ev") or 0)
    pnl = float(report_metrics.get("total_realized_pnl_usd") or 0.0)
    adverse = int(report_metrics.get("adverse_trades_vs_evidence") or 0)
    positive = int(report_metrics.get("positive_trades_vs_evidence") or 0)
    if runtime_issues:
        return "- Owner: Bhiksha plumbing. Fix runtime/lifecycle issues before making strategy decisions."
    if match_rate is not None and match_rate < 0.95:
        return "- Owner: data/adoption. Investigate mismatched same-bar replay before judging alpha."
    if provider_mismatch >= 3:
        return "- Owner: provider contract. Prefer Polygon live features or pause provider-sensitive rows if this repeats next session."
    if closed >= 20 and pnl < 0 and adverse > max(positive * 2, positive + 3):
        return "- Owner: strategy/exit. Stop extending the same experiment; pivot exit thesis or strategy family."
    return "- Owner: Research Ops. Continue daily evidence capture; no promotion until gates are met."


if __name__ == "__main__":
    raise SystemExit(main())
