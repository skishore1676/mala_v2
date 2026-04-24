"""Catalog steward advisory workflow.

Reads Strategy_Catalog plus active_strategy, writes local recommendation
artifacts by default, and can optionally push compact advisory fields back to
Strategy_Catalog.
"""

from __future__ import annotations

import argparse
import csv
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.config import settings
from src.research.google_sheets import GoogleSheetTableClient


STEWARD_COLUMNS = ["steward_recommendation", "steward_notes"]
RECOMMENDATIONS = {"live", "shadow", "hold", "pause"}


@dataclass(slots=True)
class StewardRecommendation:
    catalog_key: str
    recommendation: str
    rank: int
    reason: str
    steward_notes: str
    symbol: str
    direction: str
    strategy_key: str
    active_enabled: bool
    active_mode: str
    expectancy: float | None
    confidence: float | None
    signal_count: int | None
    execution_robustness: float | None
    mc_total_r_p05: float | None

    def artifact_row(self) -> dict[str, Any]:
        return {
            "rank": self.rank,
            "catalog_key": self.catalog_key,
            "steward_recommendation": self.recommendation,
            "steward_notes": self.steward_notes,
            "symbol": self.symbol,
            "direction": self.direction,
            "strategy_key": self.strategy_key,
            "active_enabled": self.active_enabled,
            "active_mode": self.active_mode,
            "expectancy": self.expectancy,
            "confidence": self.confidence,
            "signal_count": self.signal_count,
            "execution_robustness": self.execution_robustness,
            "mc_total_r_p05": self.mc_total_r_p05,
            "reason": self.reason,
        }


def build_recommendations(
    *,
    catalog_rows: list[dict[str, Any]],
    active_rows: list[dict[str, Any]],
    reviewed_at: str,
    report_path: str | None = None,
) -> list[StewardRecommendation]:
    active_by_key = {
        str(row.get("strategy", "")).strip(): row
        for row in active_rows
        if str(row.get("strategy", "")).strip()
    }

    scored: list[dict[str, Any]] = []
    for row in catalog_rows:
        catalog_key = str(row.get("catalog_key", "")).strip()
        if not catalog_key:
            continue

        summary = _parse_summary(row.get("playbook_summary_json"))
        mc_metrics = summary.get("mc_metrics") if isinstance(summary.get("mc_metrics"), dict) else {}
        active = active_by_key.get(catalog_key, {})
        active_enabled = _truthy(active.get("enabled"))
        active_mode = str(active.get("mode", "") or "").strip().lower()

        metrics = {
            "expectancy": _to_float(row.get("expectancy")),
            "confidence": _to_float(row.get("confidence")),
            "signal_count": _to_int(row.get("signal_count")),
            "execution_robustness": _to_float(row.get("execution_robustness")),
            "mc_total_r_p05": _to_float(mc_metrics.get("mc_total_r_p05")),
        }
        recommendation, reason = _recommend(row, metrics, active_enabled)
        scored.append(
            {
                "row": row,
                "active_enabled": active_enabled,
                "active_mode": active_mode,
                "metrics": metrics,
                "recommendation": recommendation,
                "reason": reason,
                "sort_key": _sort_key(recommendation, metrics, active_enabled),
            }
        )

    _demote_duplicate_live_lanes(scored)
    for item in scored:
        item["sort_key"] = _sort_key(item["recommendation"], item["metrics"], item["active_enabled"])
    scored.sort(key=lambda item: item["sort_key"])
    recommendations: list[StewardRecommendation] = []
    for rank, item in enumerate(scored, start=1):
        row = item["row"]
        metrics = item["metrics"]
        notes = {
            "rank": rank,
            "reason": item["reason"],
            "reviewed_at": reviewed_at,
        }
        if report_path:
            notes["report"] = report_path
        recommendations.append(
            StewardRecommendation(
                catalog_key=str(row.get("catalog_key", "")).strip(),
                recommendation=item["recommendation"],
                rank=rank,
                reason=item["reason"],
                steward_notes=json.dumps(notes, separators=(",", ":"), sort_keys=True),
                symbol=str(row.get("symbol", "") or "").strip().upper(),
                direction=str(row.get("direction", "") or "").strip().lower(),
                strategy_key=str(row.get("strategy_key", "") or "").strip().lower(),
                active_enabled=item["active_enabled"],
                active_mode=item["active_mode"],
                expectancy=metrics["expectancy"],
                confidence=metrics["confidence"],
                signal_count=metrics["signal_count"],
                execution_robustness=metrics["execution_robustness"],
                mc_total_r_p05=metrics["mc_total_r_p05"],
            )
        )
    return recommendations


def write_recommendation_artifacts(
    *,
    recommendations: list[StewardRecommendation],
    out_dir: Path,
    reviewed_at: str,
) -> tuple[Path, Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    md_path, csv_path = recommendation_artifact_paths(out_dir=out_dir, reviewed_at=reviewed_at)

    fieldnames = [
        "rank",
        "catalog_key",
        "steward_recommendation",
        "symbol",
        "direction",
        "strategy_key",
        "active_enabled",
        "active_mode",
        "expectancy",
        "confidence",
        "signal_count",
        "execution_robustness",
        "mc_total_r_p05",
        "reason",
        "steward_notes",
    ]
    with csv_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for recommendation in recommendations:
            writer.writerow({key: recommendation.artifact_row().get(key, "") for key in fieldnames})

    counts = {name: 0 for name in ("live", "shadow", "hold", "pause")}
    for recommendation in recommendations:
        counts[recommendation.recommendation] += 1
    lines = [
        "# Catalog Steward Recommendations",
        "",
        f"- reviewed_at: `{reviewed_at}`",
        f"- counts: live={counts['live']}, shadow={counts['shadow']}, hold={counts['hold']}, pause={counts['pause']}",
        "",
        "| Rank | Action | Catalog Key | Symbol | Direction | Strategy | Reason |",
        "|---:|---|---|---|---|---|---|",
    ]
    for recommendation in recommendations:
        lines.append(
            "| {rank} | `{action}` | `{key}` | {symbol} | {direction} | `{strategy}` | {reason} |".format(
                rank=recommendation.rank,
                action=recommendation.recommendation,
                key=recommendation.catalog_key,
                symbol=recommendation.symbol,
                direction=recommendation.direction,
                strategy=recommendation.strategy_key,
                reason=recommendation.reason.replace("|", "/"),
            )
        )
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return md_path, csv_path


def recommendation_artifact_paths(*, out_dir: Path, reviewed_at: str) -> tuple[Path, Path]:
    stamp = reviewed_at.replace(":", "").replace("-", "").replace("+", "Z")
    return (
        out_dir / f"catalog-steward-{stamp}.md",
        out_dir / f"catalog-steward-{stamp}.csv",
    )


def push_recommendations_to_sheet(
    *,
    client: GoogleSheetTableClient,
    recommendations: list[StewardRecommendation],
) -> int:
    client.ensure_columns(STEWARD_COLUMNS)
    rows = client.read_rows()
    by_key = {recommendation.catalog_key: recommendation for recommendation in recommendations}
    updates: list[dict[str, Any]] = []
    for row in rows:
        recommendation = by_key.get(str(row.get("catalog_key", "")).strip())
        if recommendation is None:
            continue
        next_row = dict(row)
        next_row["steward_recommendation"] = recommendation.recommendation
        next_row["steward_notes"] = recommendation.steward_notes
        updates.append(next_row)
    client.batch_update_rows(rows=updates, columns=STEWARD_COLUMNS)
    return len(updates)


def _recommend(row: dict[str, Any], metrics: dict[str, Any], active_enabled: bool) -> tuple[str, str]:
    ready = _truthy(row.get("bhiksha_ready"))
    lifecycle_status = str(row.get("lifecycle_status", "") or "").strip().lower()
    expectancy = metrics["expectancy"]
    signal_count = metrics["signal_count"] or 0
    robustness = metrics["execution_robustness"]
    mc_p05 = metrics["mc_total_r_p05"]

    if active_enabled and (not ready or (expectancy is not None and expectancy < 0.10) or (robustness is not None and robustness < 0.40)):
        return "pause", "active row is weak or no longer ready"
    if not ready:
        return "hold", "not bhiksha_ready"
    if lifecycle_status not in {"active", "candidate"}:
        return "hold", f"lifecycle_status={lifecycle_status or 'blank'}"
    if expectancy is None or expectancy <= 0:
        return "hold", "non-positive or missing expectancy"
    if signal_count < 20:
        return "hold", f"thin sample: signal_count={signal_count}"
    if (
        expectancy >= 0.50
        and signal_count >= 50
        and robustness is not None
        and robustness >= 0.95
        and (mc_p05 is None or mc_p05 > 0)
    ):
        return "live", "strong candidate for active_strategy live review"
    return "shadow", "validated catalog row; collect Bhiksha shadow evidence"


def _sort_key(recommendation: str, metrics: dict[str, Any], active_enabled: bool) -> tuple[Any, ...]:
    action_priority = {"live": 0, "shadow": 1, "pause": 2, "hold": 3}
    return (
        action_priority[recommendation],
        0 if active_enabled else 1,
        -float(metrics["expectancy"] or -999),
        -float(metrics["execution_robustness"] or -999),
        -int(metrics["signal_count"] or 0),
    )


def _demote_duplicate_live_lanes(scored: list[dict[str, Any]]) -> None:
    live_items = [item for item in scored if item["recommendation"] == "live"]
    live_items.sort(
        key=lambda item: (
            -float(item["metrics"]["expectancy"] or -999),
            -float(item["metrics"]["execution_robustness"] or -999),
            -int(item["metrics"]["signal_count"] or 0),
        )
    )
    seen: set[tuple[str, str, str]] = set()
    for item in live_items:
        row = item["row"]
        lane = (
            str(row.get("symbol", "") or "").strip().upper(),
            str(row.get("direction", "") or "").strip().lower(),
            str(row.get("strategy_key", "") or "").strip().lower(),
        )
        if lane in seen:
            item["recommendation"] = "shadow"
            item["reason"] = "duplicate live lane; shadow behind stronger sibling"
            continue
        seen.add(lane)


def _parse_summary(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str) and raw.strip():
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _truthy(value: Any) -> bool:
    return str(value).strip().lower() in {"true", "1", "yes", "y", "live", "shadow"}


def _to_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value: Any) -> int | None:
    numeric = _to_float(value)
    return int(numeric) if numeric is not None else None


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--google-credentials", default=settings.google_api_credentials_path)
    parser.add_argument("--catalog-sheet-id", default=settings.strategy_catalog_sheet_id)
    parser.add_argument("--catalog-sheet-name", default=settings.strategy_catalog_sheet_name)
    parser.add_argument("--active-sheet-name", default="active_strategy")
    parser.add_argument("--out-dir", default="research/reports/catalog_steward")
    parser.add_argument("--push-sheet", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    if not args.catalog_sheet_id:
        raise SystemExit("--catalog-sheet-id or STRATEGY_CATALOG_SHEET_ID is required")
    if not args.google_credentials:
        raise SystemExit("--google-credentials or GOOGLE_API_CREDENTIALS_PATH is required")

    credentials = Path(args.google_credentials)
    catalog_client = GoogleSheetTableClient(
        spreadsheet_id=args.catalog_sheet_id,
        sheet_name=args.catalog_sheet_name,
        credentials_path=credentials,
    )
    active_client = GoogleSheetTableClient(
        spreadsheet_id=args.catalog_sheet_id,
        sheet_name=args.active_sheet_name,
        credentials_path=credentials,
    )
    reviewed_at = datetime.now(UTC).replace(microsecond=0).isoformat()
    out_dir = Path(args.out_dir)
    md_path, csv_path = recommendation_artifact_paths(out_dir=out_dir, reviewed_at=reviewed_at)
    recommendations = build_recommendations(
        catalog_rows=catalog_client.read_rows(),
        active_rows=active_client.read_rows(),
        reviewed_at=reviewed_at,
        report_path=str(md_path),
    )
    write_recommendation_artifacts(
        recommendations=recommendations,
        out_dir=out_dir,
        reviewed_at=reviewed_at,
    )

    updated = 0
    if args.push_sheet:
        updated = push_recommendations_to_sheet(client=catalog_client, recommendations=recommendations)

    print(f"STEWARD_REPORT={md_path}")
    print(f"STEWARD_CSV={csv_path}")
    print(f"STEWARD_RECOMMENDATIONS={len(recommendations)}")
    print(f"STEWARD_SHEET_UPDATED={updated}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
