"""Build trader-facing review packs from playbook surface artifacts."""

from __future__ import annotations

import argparse
import csv
import html
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any


CANDIDATE_COLUMNS = [
    "rank",
    "candidate_type",
    "review_priority",
    "config_id",
    "symbol",
    "direction",
    "extension_family",
    "extension_bin",
    "stage_filter",
    "gap_state_filter",
    "reversal_range_minutes",
    "volume_confirmation_filter",
    "stop_family",
    "exit_family",
    "sample_count",
    "calibration_count",
    "holdout_count",
    "calibration_expectancy_r",
    "holdout_expectancy_r",
    "calibration_win_rate",
    "holdout_win_rate",
    "expectancy_drift_r",
    "score",
    "match_grade",
    "criteria_failed_count",
    "criteria_failed",
    "evidence_note",
    "trader_note",
]

CHART_REVIEW_COLUMNS = [
    "candidate_rank",
    "candidate_type",
    "config_id",
    "symbol",
    "direction",
    "event_timestamp",
    "entry_reference_price",
    "extension_summary",
    "stage_summary",
    "gap_state",
    "trigger_summary",
    "volume_confirmation_summary",
    "stop_reference_price",
    "exit_reference_price",
    "exit_family",
    "outcome_label",
    "pnl_r",
    "max_favorable_excursion_r",
    "max_adverse_excursion_r",
]

SURFACE_COLUMNS_FOR_HEATMAP = [
    "symbol",
    "direction",
    "extension_family",
    "extension_bin",
    "exit_family",
    "sample_count",
    "holdout_expectancy_r",
]


@dataclass(frozen=True, slots=True)
class SurfaceReviewResult:
    out_dir: Path
    candidate_count: int
    review_md: Path
    candidate_csv: Path
    chart_review_csv: Path
    scorecard_svg: Path
    heatmap_svg: Path


def build_surface_review(
    run_dir: Path,
    *,
    out_dir: Path | None = None,
    max_candidates: int = 16,
    events_per_candidate: int = 3,
) -> SurfaceReviewResult:
    """Build a compact trader review pack from a playbook surface run."""

    surface_csv = run_dir / "conditional_surface_by_symbol.csv"
    sample_events_csv = run_dir / "sample_events.csv"
    if not surface_csv.exists():
        raise FileNotFoundError(f"conditional_surface_by_symbol.csv not found under {run_dir}")
    if not sample_events_csv.exists():
        raise FileNotFoundError(f"sample_events.csv not found under {run_dir}")
    if max_candidates <= 0:
        raise ValueError("max_candidates must be positive")
    if events_per_candidate <= 0:
        raise ValueError("events_per_candidate must be positive")

    review_dir = out_dir or run_dir / "surface_review"
    review_dir.mkdir(parents=True, exist_ok=True)

    surface_rows = _read_csv(surface_csv)
    sample_events = _read_csv(sample_events_csv)
    candidates = _rank_candidates(surface_rows, max_candidates=max_candidates)
    chart_rows = _chart_review_rows(
        candidates,
        sample_events,
        events_per_candidate=events_per_candidate,
    )

    candidate_csv = review_dir / "candidate_regions.csv"
    chart_review_csv = review_dir / "chart_review_events.csv"
    scorecard_svg = review_dir / "candidate_scorecard.svg"
    heatmap_svg = review_dir / "holdout_heatmap.svg"
    review_md = review_dir / "SURFACE_REVIEW.md"

    _write_csv(candidate_csv, candidates, CANDIDATE_COLUMNS)
    _write_csv(chart_review_csv, chart_rows, CHART_REVIEW_COLUMNS)
    _write_scorecard_svg(scorecard_svg, candidates)
    _write_heatmap_svg(heatmap_svg, surface_rows)
    _write_review_md(
        review_md,
        run_dir=run_dir,
        candidate_csv=candidate_csv,
        chart_review_csv=chart_review_csv,
        scorecard_svg=scorecard_svg,
        heatmap_svg=heatmap_svg,
        surface_rows=surface_rows,
        candidates=candidates,
        chart_rows=chart_rows,
    )

    return SurfaceReviewResult(
        out_dir=review_dir,
        candidate_count=len(candidates),
        review_md=review_md,
        candidate_csv=candidate_csv,
        chart_review_csv=chart_review_csv,
        scorecard_svg=scorecard_svg,
        heatmap_svg=heatmap_svg,
    )


def _rank_candidates(rows: list[dict[str, str]], *, max_candidates: int) -> list[dict[str, str]]:
    scored: list[dict[str, Any]] = []
    for row in rows:
        sample_count = _safe_int(row.get("sample_count"))
        holdout_count = _safe_int(row.get("holdout_count"))
        calibration_exp = _safe_float(row.get("calibration_expectancy_r"))
        holdout_exp = _safe_float(row.get("holdout_expectancy_r"))
        calibration_win = _safe_float(row.get("calibration_win_rate"))
        holdout_win = _safe_float(row.get("holdout_win_rate"))
        if sample_count < 50 or holdout_count < 10 or holdout_exp is None or holdout_exp <= 0:
            continue
        if row.get("match_grade") not in {"near_favorable", "partial", "favorable"}:
            continue

        drift = None if calibration_exp is None else holdout_exp - calibration_exp
        candidate_type, review_priority, trader_note = _candidate_read(
            row,
            calibration_exp=calibration_exp,
            holdout_exp=holdout_exp,
            calibration_win=calibration_win,
            holdout_win=holdout_win,
            drift=drift,
        )
        score = _candidate_score(
            row,
            calibration_exp=calibration_exp,
            holdout_exp=holdout_exp,
            calibration_win=calibration_win,
            holdout_win=holdout_win,
            drift=drift,
        )
        scored.append(
            {
                **row,
                "candidate_type": candidate_type,
                "review_priority": review_priority,
                "expectancy_drift_r": _format_float(drift),
                "score": _format_float(score),
                "trader_note": trader_note,
                "_sort": (
                    _priority_sort(review_priority),
                    -score,
                    -holdout_exp,
                    -sample_count,
                ),
            }
        )

    selected = sorted(scored, key=lambda row: row["_sort"])[:max_candidates]
    output: list[dict[str, str]] = []
    for rank, row in enumerate(selected, start=1):
        output.append(
            {
                "rank": str(rank),
                **{
                    column: str(row.get(column, ""))
                    for column in CANDIDATE_COLUMNS
                    if column != "rank"
                },
            }
        )
    return output


def _candidate_read(
    row: dict[str, str],
    *,
    calibration_exp: float | None,
    holdout_exp: float,
    calibration_win: float | None,
    holdout_win: float | None,
    drift: float | None,
) -> tuple[str, str, str]:
    exit_family = row.get("exit_family", "")
    match_grade = row.get("match_grade", "")
    if exit_family == "time_stop" or (holdout_win is not None and holdout_win < 0.45):
        return (
            "tail_payoff_review",
            "low",
            "Positive holdout expectancy comes with low hit rate or time-stop behavior; chart review should test whether this matches the intended discretionary feel.",
        )
    if calibration_exp is None or calibration_exp < 0:
        return (
            "holdout_only_suspect",
            "medium",
            "Holdout improved but calibration was weak or negative; useful lead, not evidence.",
        )
    if (
        match_grade in {"favorable", "near_favorable"}
        and
        holdout_exp >= 0.10
        and calibration_exp >= 0.0
        and holdout_win is not None
        and holdout_win >= 0.55
        and (drift is None or abs(drift) <= 0.20)
    ):
        return (
            "clean_reversion_candidate",
            "high",
            "Closest to a tradable reversion shape; inspect recent charts before tightening entry or exits.",
        )
    if holdout_win is not None and holdout_win >= 0.55:
        return (
            "weak_positive_reversion",
            "medium",
            "Hit rate is acceptable but expectancy stability is weak; likely needs tighter context or trigger rules.",
        )
    return (
        "positive_but_messy",
        "low",
        "Positive holdout result is not clean enough yet; use only as a clue for feature refinement.",
    )


def _candidate_score(
    row: dict[str, str],
    *,
    calibration_exp: float | None,
    holdout_exp: float,
    calibration_win: float | None,
    holdout_win: float | None,
    drift: float | None,
) -> float:
    score = holdout_exp
    if calibration_exp is not None:
        score += 0.35 * calibration_exp
    if holdout_win is not None:
        score += 0.25 * (holdout_win - 0.5)
    if calibration_win is not None and holdout_win is not None:
        score -= 0.15 * abs(holdout_win - calibration_win)
    if drift is not None:
        score -= 0.20 * max(0.0, abs(drift) - 0.05)
    if row.get("exit_family") == "time_stop":
        score -= 0.10
    if row.get("stop_family") == "immediate_entry_bar_failure":
        score -= 0.05
    return score


def _chart_review_rows(
    candidates: list[dict[str, str]],
    sample_events: list[dict[str, str]],
    *,
    events_per_candidate: int,
) -> list[dict[str, str]]:
    rows_by_config: dict[tuple[str, str, str], list[dict[str, str]]] = {}
    for row in sample_events:
        rows_by_config.setdefault(
            (
                row.get("config_id", ""),
                row.get("symbol", ""),
                row.get("direction", ""),
            ),
            [],
        ).append(row)

    output: list[dict[str, str]] = []
    for candidate in candidates:
        key = (
            candidate["config_id"],
            candidate["symbol"],
            candidate["direction"],
        )
        candidate_events = sorted(
            rows_by_config.get(key, []),
            key=lambda row: row.get("event_timestamp", ""),
            reverse=True,
        )[:events_per_candidate]
        for event in candidate_events:
            output.append(
                {
                    "candidate_rank": candidate["rank"],
                    "candidate_type": candidate["candidate_type"],
                    **{
                        column: event.get(column, "")
                        for column in CHART_REVIEW_COLUMNS
                        if column not in {"candidate_rank", "candidate_type"}
                    },
                }
            )
    return output


def _write_scorecard_svg(path: Path, candidates: list[dict[str, str]]) -> None:
    width = 1080
    row_h = 34
    top = 72
    left = 360
    right = 80
    rows = candidates[:12]
    height = top + max(1, len(rows)) * row_h + 52
    values = [
        value
        for row in rows
        for value in (
            abs(_safe_float(row.get("calibration_expectancy_r")) or 0.0),
            abs(_safe_float(row.get("holdout_expectancy_r")) or 0.0),
        )
    ]
    scale = max(values or [1.0], default=1.0)
    bar_w = width - left - right
    lines = [
        _svg_header(width, height),
        '<rect width="100%" height="100%" fill="#fbfaf7"/>',
        '<text x="24" y="32" font-size="22" font-family="Arial" fill="#1f2933">Candidate Scorecard</text>',
        '<text x="24" y="54" font-size="13" font-family="Arial" fill="#52606d">Calibration vs holdout expectancy in R. Blue = calibration, green = holdout.</text>',
    ]
    zero_x = left + (bar_w * 0.35)
    max_positive_w = bar_w * 0.65
    max_negative_w = bar_w * 0.30
    lines.append(f'<line x1="{zero_x:.1f}" y1="64" x2="{zero_x:.1f}" y2="{height - 24}" stroke="#9aa5b1" stroke-width="1"/>')
    for index, row in enumerate(rows):
        y = top + index * row_h
        label = (
            f"#{row['rank']} {row['symbol']} {row['direction']} "
            f"{row['extension_family']}>{row['extension_bin']} {row['exit_family']}"
        )
        cal = _safe_float(row.get("calibration_expectancy_r")) or 0.0
        hold = _safe_float(row.get("holdout_expectancy_r")) or 0.0
        lines.append(f'<text x="24" y="{y + 18}" font-size="12" font-family="Arial" fill="#243b53">{html.escape(label)}</text>')
        lines.append(_bar(zero_x, y + 7, cal, scale, max_positive_w, max_negative_w, "#486581"))
        lines.append(_bar(zero_x, y + 19, hold, scale, max_positive_w, max_negative_w, "#2f855a"))
        lines.append(
            f'<text x="{width - 72}" y="{y + 18}" font-size="11" font-family="Arial" fill="#334e68">'
            f'{html.escape(row["candidate_type"])}</text>'
        )
    lines.append("</svg>")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _bar(
    zero_x: float,
    y: float,
    value: float,
    scale: float,
    max_positive_w: float,
    max_negative_w: float,
    fill: str,
) -> str:
    if value >= 0:
        width = max(1.0, (value / scale) * max_positive_w)
        return f'<rect x="{zero_x:.1f}" y="{y:.1f}" width="{width:.1f}" height="9" rx="2" fill="{fill}"/>'
    width = max(1.0, (abs(value) / scale) * max_negative_w)
    return f'<rect x="{zero_x - width:.1f}" y="{y:.1f}" width="{width:.1f}" height="9" rx="2" fill="#c05621"/>'


def _write_heatmap_svg(path: Path, rows: list[dict[str, str]]) -> None:
    filtered = [
        row
        for row in rows
        if _safe_int(row.get("sample_count")) >= 50
        and _safe_float(row.get("holdout_expectancy_r")) is not None
    ]
    symbols = sorted({row.get("symbol", "") for row in filtered if row.get("symbol")})
    directions = [direction for direction in ("long", "short") if any(row.get("direction") == direction for row in filtered)]
    panels = [(symbol, direction) for symbol in symbols for direction in directions]
    cell_w = 112
    cell_h = 26
    panel_w = 760
    panel_h = 260
    width = 1600
    height = 86 + math.ceil(max(1, len(panels)) / 2) * panel_h
    lines = [
        _svg_header(width, height),
        '<rect width="100%" height="100%" fill="#fbfaf7"/>',
        '<text x="24" y="32" font-size="22" font-family="Arial" fill="#1f2933">Holdout Expectancy Heatmap</text>',
        '<text x="24" y="54" font-size="13" font-family="Arial" fill="#52606d">Cells show best holdout expectancy by stretch bin and exit family, sample_count >= 50.</text>',
    ]
    exit_families = sorted({row.get("exit_family", "") for row in filtered if row.get("exit_family")})
    for panel_index, (symbol, direction) in enumerate(panels):
        px = 24 + (panel_index % 2) * panel_w
        py = 86 + (panel_index // 2) * panel_h
        panel_rows = [row for row in filtered if row.get("symbol") == symbol and row.get("direction") == direction]
        stretch_bins = sorted(
            {
                f"{row.get('extension_family')}>{row.get('extension_bin')}"
                for row in panel_rows
            },
            key=_stretch_sort_key,
        )[:8]
        lines.append(f'<text x="{px}" y="{py - 12}" font-size="16" font-family="Arial" fill="#243b53">{symbol} {direction}</text>')
        for col, exit_family in enumerate(exit_families):
            x = px + 170 + col * cell_w
            lines.append(
                f'<text x="{x}" y="{py + 10}" font-size="10" font-family="Arial" fill="#52606d" transform="rotate(-25 {x} {py + 10})">{html.escape(exit_family)}</text>'
            )
        for row_index, stretch_bin in enumerate(stretch_bins):
            y = py + 34 + row_index * cell_h
            lines.append(f'<text x="{px}" y="{y + 17}" font-size="11" font-family="Arial" fill="#243b53">{html.escape(stretch_bin)}</text>')
            for col, exit_family in enumerate(exit_families):
                x = px + 170 + col * cell_w
                value = _best_holdout(panel_rows, stretch_bin, exit_family)
                fill = _heat_color(value)
                label = "" if value is None else _format_float(value)
                lines.append(f'<rect x="{x}" y="{y}" width="{cell_w - 4}" height="{cell_h - 4}" rx="3" fill="{fill}" stroke="#d9e2ec"/>')
                if label:
                    lines.append(f'<text x="{x + 8}" y="{y + 15}" font-size="10" font-family="Arial" fill="#102a43">{label}</text>')
    lines.append("</svg>")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_review_md(
    path: Path,
    *,
    run_dir: Path,
    candidate_csv: Path,
    chart_review_csv: Path,
    scorecard_svg: Path,
    heatmap_svg: Path,
    surface_rows: list[dict[str, str]],
    candidates: list[dict[str, str]],
    chart_rows: list[dict[str, str]],
) -> None:
    grade_counts = _count_by(surface_rows, "match_grade")
    type_counts = _count_by(candidates, "candidate_type")
    stage_lines = _stage_proxy_review(surface_rows, candidates)
    lines = [
        "# Intraday Mean Reversion Surface Review",
        "",
        f"- source run: `{run_dir}`",
        f"- candidate regions: `{candidate_csv}`",
        f"- chart review events: `{chart_review_csv}`",
        f"- scorecard svg: `{scorecard_svg}`",
        f"- heatmap svg: `{heatmap_svg}`",
        f"- surface rows: `{len(surface_rows)}`",
        f"- candidates selected: `{len(candidates)}`",
        f"- chart review rows: `{len(chart_rows)}`",
        "",
        "## Trader Read",
        "",
        _trader_read(candidates),
        "",
        "## Grade Counts",
        "",
    ]
    for grade, count in sorted(grade_counts.items()):
        lines.append(f"- {grade}: `{count}`")
    lines.extend(["", "## Candidate Types", ""])
    for candidate_type, count in sorted(type_counts.items()):
        lines.append(f"- {candidate_type}: `{count}`")
    lines.extend(["", "## Stage Read", ""])
    lines.extend(stage_lines)
    lines.extend(["", "## Top Candidates", ""])
    for row in candidates[:10]:
        lines.append(
            "- #{rank} `{candidate_type}` {symbol} {direction} {extension_family}>{extension_bin} "
            "stage={stage_filter} gap={gap_state_filter} stop={stop_family} exit={exit_family} "
            "n={sample_count}, cal={calibration_expectancy_r}R, hold={holdout_expectancy_r}R, "
            "hold_win={holdout_win_rate}, grade={match_grade}, failed={criteria_failed_count} "
            "({criteria_failed}), note={trader_note}".format(**row)
        )
    lines.extend(
        [
            "",
            "## Review Guidance",
            "",
            "- Treat this as a map for chart review, not a trade permission slip.",
            "- `near_favorable` means exactly one strict criterion missed; it is the preferred chart-review cohort when no strict `favorable` rows exist.",
            "- Favor candidates whose chart events visually match the intended opening-drive reversion.",
            "- Be skeptical of `tail_payoff_review`: positive expectancy with low hit rate may not match the discretionary play.",
            "- Stage currently means `market_pulse_stage`: bullish, accumulation, distribution, or bearish from the 1m MarketPulse VWMA 8/21/34 plus VMA location.",
            "- Multiple-comparisons correction is not optional; it becomes a literal pre-promotion gate once a packet is locked.",
            "- Stop and thesis invalidation are still conflated in this slice. Split them before expanding the playbook beyond review leads.",
            "- If the top chart events look late or random, tighten trigger/context before any M2/M5 stress tests.",
            "- After a candidate survives chart review, create a locked execution packet and only then run cost/Monte Carlo stress.",
            "",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _trader_read(candidates: list[dict[str, str]]) -> str:
    if not candidates:
        return "No candidate regions survived the review filters. Do not chart-chase this surface yet."
    favorable = [row for row in candidates if row.get("match_grade") == "favorable"]
    if favorable:
        return (
            "One or more candidates passed strict surface gates, but this is still not a promotion. "
            "Run visual review first, then apply the locked-packet multiple-comparisons and stress gates."
        )
    high = [row for row in candidates if row.get("review_priority") == "high"]
    if high:
        return (
            "There are near-favorable leads to chart review, but no validated playbook yet. "
            "The next gate is visual confirmation on recent events, not more parameter mining."
        )
    medium = [row for row in candidates if row.get("review_priority") == "medium"]
    if medium:
        return (
            "There are leads, but they are not clean yet. Use chart review to decide whether "
            "the surface is finding your play or just finding unstable holdout pockets."
        )
    return (
        "The surface has positive pockets, but they are messy. Use this as feature-refinement "
        "input before thinking about an execution packet."
    )


def _stage_proxy_review(
    surface_rows: list[dict[str, str]], candidates: list[dict[str, str]]
) -> list[str]:
    stage_counts = _count_by(surface_rows, "stage_filter")
    candidate_stage_counts = _count_by(candidates, "stage_filter")
    near_or_better_stage_counts = _count_by(
        [row for row in surface_rows if row.get("match_grade") in {"favorable", "near_favorable"}],
        "stage_filter",
    )
    lines = [
        "- implemented feature: `market_pulse_stage`.",
        "- source logic: MarketPulse VWMA 8/21/34 stack plus close relative to VMA.",
        f"- surface rows by stage: `{_format_counts(stage_counts)}`",
        f"- candidate rows by stage: `{_format_counts(candidate_stage_counts)}`",
        f"- favorable/near-favorable rows by stage: `{_format_counts(near_or_better_stage_counts)}`",
    ]
    if not near_or_better_stage_counts:
        lines.append("- read: no MarketPulse stage level lifted a row into the strict or near-favorable bucket.")
    elif set(near_or_better_stage_counts) == {"no_filter"}:
        lines.append("- read: current leads come from no-filter rows; MarketPulse stage did not improve the promoted review cohort.")
    else:
        lines.append("- read: inspect whether the MarketPulse-stage rows visually match the intended trader-stage language.")
    return lines


def _best_holdout(rows: list[dict[str, str]], stretch_bin: str, exit_family: str) -> float | None:
    values = [
        _safe_float(row.get("holdout_expectancy_r"))
        for row in rows
        if f"{row.get('extension_family')}>{row.get('extension_bin')}" == stretch_bin
        and row.get("exit_family") == exit_family
    ]
    cleaned = [value for value in values if value is not None]
    return max(cleaned) if cleaned else None


def _heat_color(value: float | None) -> str:
    if value is None:
        return "#edf2f7"
    clipped = max(-0.5, min(0.5, value))
    if clipped >= 0:
        intensity = clipped / 0.5
        r = int(226 - 120 * intensity)
        g = int(244 - 52 * intensity)
        b = int(234 - 99 * intensity)
    else:
        intensity = abs(clipped) / 0.5
        r = int(255 - 38 * intensity)
        g = int(237 - 98 * intensity)
        b = int(213 - 133 * intensity)
    return f"#{r:02x}{g:02x}{b:02x}"


def _stretch_sort_key(label: str) -> tuple[str, float]:
    family, _, threshold = label.partition(">")
    return (family, _safe_float(threshold) or 0.0)


def _priority_sort(priority: str) -> int:
    return {"high": 0, "medium": 1, "low": 2}.get(priority, 3)


def _count_by(rows: list[dict[str, str]], key: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        value = row.get(key, "") or "blank"
        counts[value] = counts.get(value, 0) + 1
    return counts


def _format_counts(counts: dict[str, int]) -> str:
    if not counts:
        return "none"
    return ", ".join(f"{key}={value}" for key, value in sorted(counts.items()))


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _write_csv(path: Path, rows: list[dict[str, str]], columns: list[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _safe_float(raw: Any) -> float | None:
    if raw in (None, ""):
        return None
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return None
    if math.isnan(value) or math.isinf(value):
        return None
    return value


def _safe_int(raw: Any) -> int:
    value = _safe_float(raw)
    return int(value) if value is not None else 0


def _format_float(value: float | None, *, digits: int = 4) -> str:
    if value is None:
        return ""
    return f"{value:.{digits}f}".rstrip("0").rstrip(".")


def _svg_header(width: int, height: int) -> str:
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}">'
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", required=True, type=Path, help="Playbook surface run directory")
    parser.add_argument("--out-dir", type=Path, help="Optional output directory")
    parser.add_argument("--max-candidates", type=int, default=16)
    parser.add_argument("--events-per-candidate", type=int, default=3)
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    result = build_surface_review(
        args.run_dir,
        out_dir=args.out_dir,
        max_candidates=args.max_candidates,
        events_per_candidate=args.events_per_candidate,
    )
    print(f"OUT_DIR={result.out_dir}")
    print(f"CANDIDATES={result.candidate_count}")
    print(f"REVIEW_MD={result.review_md}")
    print(f"CANDIDATE_CSV={result.candidate_csv}")
    print(f"CHART_REVIEW_CSV={result.chart_review_csv}")
    print(f"SCORECARD_SVG={result.scorecard_svg}")
    print(f"HEATMAP_SVG={result.heatmap_svg}")


if __name__ == "__main__":
    main()
