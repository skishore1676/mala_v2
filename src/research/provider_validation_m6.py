"""M6 provider validation artifacts for post-M5 Mala candidates."""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
import re
from typing import Any, Iterable

from src.research.strategy_keys import to_strategy_key


M6_PROVIDER_VALIDATION_CSV = "M6_provider_validation.csv"
M6_FEATURE_PARITY_CSV = "M6_feature_parity.csv"
M6_PROVIDER_REVIEW_MD = "M6_PROVIDER_REVIEW.md"

RISK_ORDER = {"green": 0, "yellow": 1, "red": 2}


@dataclass(slots=True, frozen=True)
class ProviderValidationArtifacts:
    run_dirs: list[Path]
    provider_validation_csvs: list[Path]
    feature_parity_csvs: list[Path]
    review_markdowns: list[Path]


@dataclass(slots=True, frozen=True)
class M6ProviderValidation:
    provider_validation_status: str = "provider_unknown"
    provider_feature_risk: str = ""
    provider_signal_overlap: str = ""
    provider_validation_report: str = ""


def build_m6_provider_validation(
    *,
    run_dirs: Iterable[str | Path],
    provider_relative_volume_csv: str | Path | None = None,
    provider_feature_parity_csv: str | Path | None = None,
    provider_replay_csv: str | Path | None = None,
) -> ProviderValidationArtifacts:
    """Write M6 provider-validation artifacts into each supplied M5 run dir."""

    relative_rows = _read_optional_csv(provider_relative_volume_csv)
    feature_rows = _read_optional_csv(provider_feature_parity_csv)
    replay_rows = _read_optional_csv(provider_replay_csv)
    relative_by_symbol = _relative_rows_by_symbol(relative_rows)
    feature_by_symbol = _feature_rows_by_symbol(feature_rows)
    replay_by_catalog = _provider_replay_by_catalog(replay_rows)

    validation_paths: list[Path] = []
    feature_paths: list[Path] = []
    review_paths: list[Path] = []
    processed: list[Path] = []

    for raw_run_dir in run_dirs:
        run_dir = Path(raw_run_dir)
        selected_rows = _read_csv(run_dir / "CATALOG_SELECTED.csv")
        if not selected_rows or not (run_dir / "M5_execution.csv").exists():
            continue
        m5_rows = _read_csv(run_dir / "M5_execution.csv")
        validation_rows: list[dict[str, Any]] = []
        feature_output_rows: list[dict[str, Any]] = []
        report_path = run_dir / M6_PROVIDER_REVIEW_MD
        for selected in selected_rows:
            catalog_key = str(selected.get("catalog_key") or "").strip()
            if not catalog_key:
                continue
            m5_row = _matching_m5_row(selected, m5_rows) or {}
            params = _strategy_params_from_rows(selected=selected, m5_row=m5_row)
            strategy_name = str(m5_row.get("strategy") or selected.get("strategy") or "")
            strategy_key = to_strategy_key(strategy_name)
            symbol = str(m5_row.get("ticker") or selected.get("ticker") or "").upper()
            feature_findings = classify_feature_parity(
                strategy_key=strategy_key,
                strategy_name=strategy_name,
                strategy_params=params,
                symbol=symbol,
                provider_relative_rows=relative_by_symbol.get(symbol, []),
                provider_feature_rows=feature_by_symbol.get(symbol, []),
            )
            replay = replay_by_catalog.get(catalog_key, {})
            validation_row = provider_validation_row(
                catalog_key=catalog_key,
                selected=selected,
                m5_row=m5_row,
                strategy_key=strategy_key,
                strategy_name=strategy_name,
                symbol=symbol,
                feature_findings=feature_findings,
                replay_row=replay,
                report_path=report_path,
            )
            validation_rows.append(validation_row)
            feature_output_rows.extend(feature_findings)

        validation_csv = run_dir / M6_PROVIDER_VALIDATION_CSV
        feature_csv = run_dir / M6_FEATURE_PARITY_CSV
        _write_csv(validation_csv, validation_rows)
        _write_csv(feature_csv, feature_output_rows)
        report_path.write_text(
            render_m6_provider_review(
                run_dir=run_dir,
                validation_rows=validation_rows,
                feature_rows=feature_output_rows,
                provider_relative_volume_csv=provider_relative_volume_csv,
                provider_feature_parity_csv=provider_feature_parity_csv,
                provider_replay_csv=provider_replay_csv,
            ),
            encoding="utf-8",
        )
        processed.append(run_dir)
        validation_paths.append(validation_csv)
        feature_paths.append(feature_csv)
        review_paths.append(report_path)

    return ProviderValidationArtifacts(
        run_dirs=processed,
        provider_validation_csvs=validation_paths,
        feature_parity_csvs=feature_paths,
        review_markdowns=review_paths,
    )


def discover_latest_m5_run_dirs(runs_dir: str | Path) -> list[Path]:
    """Return run dirs that contain the latest selected row for each catalog key."""

    candidates: dict[str, Path] = {}
    for selected_csv in sorted(Path(runs_dir).glob("*/*/CATALOG_SELECTED.csv")):
        run_dir = selected_csv.parent
        if not (run_dir / "M5_execution.csv").exists():
            continue
        for row in _read_csv(selected_csv):
            catalog_key = str(row.get("catalog_key") or "").strip()
            if not catalog_key:
                continue
            previous = candidates.get(catalog_key)
            if previous is None or _run_sort_token(run_dir) >= _run_sort_token(previous):
                candidates[catalog_key] = run_dir
    return sorted(set(candidates.values()), key=_run_sort_token)


def load_m6_provider_validation(run_dir: str | Path) -> dict[str, M6ProviderValidation]:
    path = Path(run_dir) / M6_PROVIDER_VALIDATION_CSV
    rows = _read_csv(path)
    output: dict[str, M6ProviderValidation] = {}
    for row in rows:
        catalog_key = str(row.get("catalog_key") or "").strip()
        if not catalog_key:
            continue
        output[catalog_key] = M6ProviderValidation(
            provider_validation_status=str(row.get("provider_validation_status") or "provider_unknown"),
            provider_feature_risk=str(row.get("provider_feature_risk") or ""),
            provider_signal_overlap=str(row.get("provider_signal_overlap") or ""),
            provider_validation_report=str(row.get("provider_validation_report") or ""),
        )
    return output


def classify_feature_parity(
    *,
    strategy_key: str,
    strategy_name: str,
    strategy_params: dict[str, Any],
    symbol: str,
    provider_relative_rows: list[dict[str, str]] | None = None,
    provider_feature_rows: list[dict[str, str]] | None = None,
) -> list[dict[str, Any]]:
    """Classify provider feature risk for one M5 candidate."""

    catalog_key = str(strategy_params.get("catalog_key") or "")
    rows: list[dict[str, Any]] = [
        _feature_row(
            catalog_key=catalog_key,
            symbol=symbol,
            strategy_key=strategy_key,
            feature="price_time_range_core",
            risk="green",
            reason="price/time/range features are provider-stable relative to volume-derived features",
        )
    ]
    mode = str(strategy_params.get("volume_filter_mode") or "").strip().lower()
    use_volume_filter = _truthy(strategy_params.get("use_volume_filter"))
    has_relative_filter = _has_any_value(strategy_params, ("min_relative_volume", "max_panic_relative_volume"))

    if strategy_key == "market_impulse":
        rows.append(
            _feature_row(
                catalog_key=catalog_key,
                symbol=symbol,
                strategy_key=strategy_key,
                feature="vwma_regime",
                risk="yellow",
                reason="VWMA regime is part of the thesis and uses provider volume weights",
            )
        )
        if use_volume_filter or (has_relative_filter and not _explicit_false(strategy_params.get("use_volume_filter"))):
            rows.append(
                _feature_row(
                    catalog_key=catalog_key,
                    symbol=symbol,
                    strategy_key=strategy_key,
                    feature="runtime_relative_volume_filter",
                    risk="red",
                    reason="explicit Market Impulse volume filters are not supported by the current Bhiksha adapter",
                    **_relative_provider_metric(
                        provider_relative_rows or [],
                        aggregate_minutes=_int_or_default(strategy_params.get("volume_sum_window"), 1),
                        threshold=_float_or_default(strategy_params.get("min_relative_volume"), 1.2),
                    ),
                )
            )
    elif strategy_key == "elastic_band_reversion":
        rows.append(
            _feature_row(
                catalog_key=catalog_key,
                symbol=symbol,
                strategy_key=strategy_key,
                feature="vpoc_4h",
                risk="yellow",
                reason="VPOC is the value anchor and is volume-derived",
                **_feature_provider_metric("vpoc_4h_pct", provider_feature_rows or []),
            )
        )
        if _truthy(strategy_params.get("use_directional_mass")):
            rows.append(
                _feature_row(
                    catalog_key=catalog_key,
                    symbol=symbol,
                    strategy_key=strategy_key,
                    feature="directional_mass_sign",
                    risk="yellow",
                    reason="directional-mass sign is provider-sensitive but less brittle than magnitude thresholds",
                    **_feature_provider_metric("directional_mass_pct", provider_feature_rows or []),
                )
            )
        if _has_directional_mass_magnitude_threshold(strategy_params):
            rows.append(
                _feature_row(
                    catalog_key=catalog_key,
                    symbol=symbol,
                    strategy_key=strategy_key,
                    feature="directional_mass_magnitude",
                    risk="red",
                    reason="directional-mass magnitude thresholds are provider-sensitive",
                    **_feature_provider_metric("directional_mass_pct", provider_feature_rows or []),
                )
            )
    elif strategy_key == "jerk_pivot_momentum":
        rows.append(
            _feature_row(
                catalog_key=catalog_key,
                symbol=symbol,
                strategy_key=strategy_key,
                feature="vpoc_4h",
                risk="yellow",
                reason="VPOC is volume-derived",
                **_feature_provider_metric("vpoc_4h_pct", provider_feature_rows or []),
            )
        )

    if strategy_key == "opening_drive_classifier":
        if _truthy(strategy_params.get("use_directional_mass")):
            rows.append(
                _feature_row(
                    catalog_key=catalog_key,
                    symbol=symbol,
                    strategy_key=strategy_key,
                    feature="directional_mass_sign",
                    risk="yellow",
                    reason="directional-mass sign is provider-sensitive",
                    **_feature_provider_metric("directional_mass_pct", provider_feature_rows or []),
                )
            )
        if _truthy(strategy_params.get("use_regime_filter")):
            rows.append(
                _feature_row(
                    catalog_key=catalog_key,
                    symbol=symbol,
                    strategy_key=strategy_key,
                    feature="vwma_regime",
                    risk="yellow",
                    reason="impulse regime confirmation uses VWMA provider volume weights",
                )
            )

    if mode == "aggregated_relative_volume" or _has_aggregated_relative_volume_param(strategy_params):
        rows.append(
            _feature_row(
                catalog_key=catalog_key,
                symbol=symbol,
                strategy_key=strategy_key,
                feature="aggregated_relative_volume",
                risk=_relative_provider_risk(
                    provider_relative_rows or [],
                    aggregate_minutes=_int_or_default(strategy_params.get("volume_sum_window"), 3),
                    threshold=_float_or_default(
                        strategy_params.get("relative_volume_threshold")
                        or strategy_params.get("min_relative_volume"),
                        1.2,
                    ),
                    default="yellow",
                ),
                reason="aggregated relative volume is preferred but still needs symbol-level provider parity",
                **_relative_provider_metric(
                    provider_relative_rows or [],
                    aggregate_minutes=_int_or_default(strategy_params.get("volume_sum_window"), 3),
                    threshold=_float_or_default(
                        strategy_params.get("relative_volume_threshold")
                        or strategy_params.get("min_relative_volume"),
                        1.2,
                    ),
                ),
            )
        )
    elif use_volume_filter or mode == "raw_volume_ma":
        rows.append(
            _feature_row(
                catalog_key=catalog_key,
                symbol=symbol,
                strategy_key=strategy_key,
                feature="raw_1m_volume_gate",
                risk=_relative_provider_risk(
                    provider_relative_rows or [],
                    aggregate_minutes=1,
                    threshold=_float_or_default(
                        strategy_params.get("volume_multiplier")
                        or strategy_params.get("min_relative_volume"),
                        1.2,
                    ),
                    default="red",
                    raw_gate=True,
                ),
                reason="raw 1-minute volume gates are provider-fragile",
                **_relative_provider_metric(
                    provider_relative_rows or [],
                    aggregate_minutes=1,
                    threshold=_float_or_default(
                        strategy_params.get("volume_multiplier")
                        or strategy_params.get("min_relative_volume"),
                        1.2,
                    ),
                ),
            )
        )

    return rows


def provider_validation_row(
    *,
    catalog_key: str,
    selected: dict[str, str],
    m5_row: dict[str, str],
    strategy_key: str,
    strategy_name: str,
    symbol: str,
    feature_findings: list[dict[str, Any]],
    replay_row: dict[str, str],
    report_path: Path,
) -> dict[str, Any]:
    feature_risk = max_feature_risk(feature_findings)
    signal_overlap = _provider_signal_overlap(replay_row)
    trade_count_ratio = replay_row.get("trade_count_ratio_vs_baseline") or ""
    expectancy_ratio = _expectancy_ratio(replay_row)
    status = classify_provider_validation_status(
        feature_risk=feature_risk,
        provider_signal_overlap=signal_overlap,
        has_provider_evidence=_is_provider_replay_evidence(replay_row) or _feature_rows_have_provider_evidence(feature_findings),
    )
    notes = sorted({str(row.get("reason") or "") for row in feature_findings if row.get("reason")})
    return {
        "catalog_key": catalog_key,
        "symbol": symbol,
        "direction": str(m5_row.get("direction") or selected.get("direction") or "").lower(),
        "strategy_key": strategy_key,
        "strategy_name": strategy_name,
        "provider_validation_status": status,
        "provider_feature_risk": feature_risk,
        "provider_signal_overlap": _format_optional(signal_overlap),
        "provider_trade_count_ratio": _format_optional(trade_count_ratio),
        "provider_expectancy_ratio": _format_optional(expectancy_ratio),
        "provider_validation_report": _display_path(report_path),
        "provider_feature_notes": " | ".join(notes),
        "provider_evidence_source": _provider_evidence_source(replay_row, feature_findings),
        "generated_at": datetime.now(UTC).isoformat(),
    }


def classify_provider_validation_status(
    *,
    feature_risk: str,
    provider_signal_overlap: float | None,
    has_provider_evidence: bool,
) -> str:
    if feature_risk == "red":
        return "provider_blocked"
    if not has_provider_evidence:
        return "provider_unknown"
    if provider_signal_overlap is not None:
        if provider_signal_overlap < 0.50:
            return "provider_blocked"
        if provider_signal_overlap < 0.80:
            return "provider_watch"
    if feature_risk == "yellow":
        return "provider_watch"
    return "provider_pass"


def max_feature_risk(feature_findings: list[dict[str, Any]]) -> str:
    risk = "green"
    for row in feature_findings:
        candidate = str(row.get("feature_risk") or "green")
        if RISK_ORDER.get(candidate, 0) > RISK_ORDER.get(risk, 0):
            risk = candidate
    return risk


def render_m6_provider_review(
    *,
    run_dir: Path,
    validation_rows: list[dict[str, Any]],
    feature_rows: list[dict[str, Any]],
    provider_relative_volume_csv: str | Path | None,
    provider_feature_parity_csv: str | Path | None,
    provider_replay_csv: str | Path | None,
) -> str:
    counts: dict[str, int] = {}
    for row in validation_rows:
        status = str(row.get("provider_validation_status") or "provider_unknown")
        counts[status] = counts.get(status, 0) + 1
    lines = [
        "# M6 Provider Review",
        "",
        f"- generated_at: `{datetime.now(UTC).isoformat()}`",
        f"- run_dir: `{run_dir}`",
        f"- rows: `{len(validation_rows)}`",
        f"- status_counts: `{json.dumps(counts, sort_keys=True)}`",
        f"- provider_relative_volume_csv: `{provider_relative_volume_csv or ''}`",
        f"- provider_feature_parity_csv: `{provider_feature_parity_csv or ''}`",
        f"- provider_replay_csv: `{provider_replay_csv or ''}`",
        "",
        "M6 is advisory. It validates provider/runtime similarity and does not replace M1-M5 alpha evidence.",
        "",
        "## Candidates",
        "",
        "| catalog_key | symbol | strategy | status | feature risk | signal overlap |",
        "|---|---|---|---|---|---:|",
    ]
    for row in validation_rows:
        lines.append(
            "| {catalog_key} | {symbol} | {strategy_key} | {provider_validation_status} | "
            "{provider_feature_risk} | {provider_signal_overlap} |".format(**row)
        )
    lines.extend(
        [
            "",
            "## Feature Findings",
            "",
            "| catalog_key | feature | risk | provider metric | provider value | reason |",
            "|---|---|---|---|---:|---|",
        ]
    )
    for row in feature_rows:
        lines.append(
            "| {catalog_key} | {feature} | {feature_risk} | {provider_metric} | {provider_value} | {reason} |".format(
                **row
            )
        )
    lines.append("")
    return "\n".join(lines)


def _feature_row(
    *,
    catalog_key: str,
    symbol: str,
    strategy_key: str,
    feature: str,
    risk: str,
    reason: str,
    provider_metric: str = "",
    provider_value: Any = "",
    provider_evidence_status: str = "",
) -> dict[str, Any]:
    return {
        "catalog_key": catalog_key,
        "symbol": symbol,
        "strategy_key": strategy_key,
        "feature": feature,
        "feature_risk": risk,
        "reason": reason,
        "provider_metric": provider_metric,
        "provider_value": _format_optional(provider_value),
        "provider_evidence_status": provider_evidence_status,
    }


def _relative_provider_metric(
    rows: list[dict[str, str]],
    *,
    aggregate_minutes: int,
    threshold: float,
) -> dict[str, Any]:
    row = _best_relative_row(rows, aggregate_minutes=aggregate_minutes)
    if not row:
        return {"provider_metric": "", "provider_value": "", "provider_evidence_status": "missing"}
    key = _threshold_column(threshold)
    if key not in row:
        key = "gate_flip_rate_ge_1_2"
    return {
        "provider_metric": f"aggregate_{aggregate_minutes}m:{key}",
        "provider_value": row.get(key, ""),
        "provider_evidence_status": "present",
    }


def _relative_provider_risk(
    rows: list[dict[str, str]],
    *,
    aggregate_minutes: int,
    threshold: float,
    default: str,
    raw_gate: bool = False,
) -> str:
    if raw_gate:
        return "red"
    row = _best_relative_row(rows, aggregate_minutes=aggregate_minutes)
    if not row:
        return default
    key = _threshold_column(threshold)
    value = _float_or_none(row.get(key))
    if value is None and key != "gate_flip_rate_ge_1_2":
        value = _float_or_none(row.get("gate_flip_rate_ge_1_2"))
    if value is None:
        return default
    if value >= 0.20:
        return "red"
    if value >= 0.10:
        return "yellow" if not raw_gate else "red"
    return "yellow" if raw_gate else "green"


def _feature_provider_metric(feature_pct_column: str, rows: list[dict[str, str]]) -> dict[str, Any]:
    for row in rows:
        if str(row.get("feature_pct_column") or "") == feature_pct_column:
            return {
                "provider_metric": f"{feature_pct_column}:p90_pct_diff",
                "provider_value": row.get("p90_pct_diff", ""),
                "provider_evidence_status": "present",
            }
    return {"provider_metric": "", "provider_value": "", "provider_evidence_status": "missing"}


def _best_relative_row(rows: list[dict[str, str]], *, aggregate_minutes: int) -> dict[str, str] | None:
    exact = [
        row for row in rows
        if _int_or_none(row.get("aggregate_minutes")) == aggregate_minutes
    ]
    if exact:
        return exact[0]
    return rows[0] if rows else None


def _threshold_column(threshold: float) -> str:
    label = str(float(threshold)).rstrip("0").rstrip(".").replace(".", "_")
    return f"gate_flip_rate_ge_{label}"


def _matching_m5_row(selected: dict[str, str], m5_rows: list[dict[str, str]]) -> dict[str, str] | None:
    for row in m5_rows:
        if _selected_matches_m5(selected, row):
            return row
    return None


def _selected_matches_m5(selected: dict[str, str], row: dict[str, str]) -> bool:
    ignored = {
        "catalog_key",
        "recommendation_tier",
        "exit_reliability",
        "exit_trade_count",
        "selected_exit_policy",
        "mc_prob_positive_exp",
        "mc_exp_r_p50",
        "base_exp_r",
        "holdout_trades",
        "holdout_win_rate",
    }
    for key, value in selected.items():
        if key in ignored or value in ("", None):
            continue
        if key not in row:
            continue
        if str(row.get(key, "")).strip() != str(value).strip():
            return False
    return True


def _strategy_params_from_rows(*, selected: dict[str, str], m5_row: dict[str, str]) -> dict[str, Any]:
    excluded = {
        "ticker",
        "strategy",
        "direction",
        "execution_profile",
        "stress_profile",
        "recommendation_tier",
        "recommendation_tier_reason",
        "recommendation_checks_json",
        "exit_reliability",
        "exit_trade_count",
        "selected_exit_policy",
        "mc_prob_positive_exp",
        "mc_exp_r_p50",
        "base_exp_r",
        "holdout_trades",
        "holdout_win_rate",
    }
    params = {"catalog_key": selected.get("catalog_key", "")}
    for source in (m5_row, selected):
        for key, value in source.items():
            if key in excluded or value in ("", None):
                continue
            params[key] = _parse_scalar(value)
    return params


def _relative_rows_by_symbol(rows: list[dict[str, str]]) -> dict[str, list[dict[str, str]]]:
    output: dict[str, list[dict[str, str]]] = {}
    for row in rows:
        symbol = str(row.get("symbol") or "").upper()
        if symbol:
            output.setdefault(symbol, []).append(row)
    return output


def _feature_rows_by_symbol(rows: list[dict[str, str]]) -> dict[str, list[dict[str, str]]]:
    output: dict[str, list[dict[str, str]]] = {}
    for row in rows:
        symbol = str(row.get("symbol") or "").upper()
        if symbol:
            output.setdefault(symbol, []).append(row)
    return output


def _provider_replay_by_catalog(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    output: dict[str, dict[str, str]] = {}
    for row in rows:
        catalog_key = str(row.get("catalog_key") or "").strip()
        if not catalog_key or row.get("error"):
            continue
        scenario = str(row.get("scenario") or "").strip()
        previous = output.get(catalog_key)
        if previous is None or _replay_scenario_rank(scenario) < _replay_scenario_rank(str(previous.get("scenario") or "")):
            output[catalog_key] = row
    return output


def _replay_scenario_rank(scenario: str) -> int:
    order = {
        "volume_provider_like": 0,
        "provider_like": 0,
        "gate_relative_volume_3m": 1,
        "gate_relative_volume_5m": 2,
        "baseline_feature_volume": 3,
        "baseline": 4,
        "polygon_baseline": 4,
    }
    return order.get(scenario, 10)


def _provider_signal_overlap(replay_row: dict[str, str]) -> float | None:
    for key in (
        "entry_overlap_rate_vs_baseline",
        "trade_overlap_rate_vs_baseline",
        "recommended_trade_overlap",
    ):
        value = _float_or_none(replay_row.get(key))
        if value is not None:
            return value
    return None


def _expectancy_ratio(replay_row: dict[str, str]) -> float | None:
    expectancy = _float_or_none(replay_row.get("expectancy") or replay_row.get("recommended_expectancy"))
    baseline = _float_or_none(replay_row.get("baseline_expectancy"))
    if expectancy is None or baseline in (None, 0.0):
        return None
    return expectancy / baseline


def _feature_rows_have_provider_evidence(feature_findings: list[dict[str, Any]]) -> bool:
    return any(row.get("provider_evidence_status") == "present" for row in feature_findings)


def _provider_evidence_source(replay_row: dict[str, str], feature_findings: list[dict[str, Any]]) -> str:
    sources: list[str] = []
    if _is_provider_replay_evidence(replay_row):
        sources.append(f"provider_replay:{replay_row.get('scenario', '')}")
    if _feature_rows_have_provider_evidence(feature_findings):
        sources.append("provider_parity")
    return "|".join(source for source in sources if source)


def _is_provider_replay_evidence(replay_row: dict[str, str]) -> bool:
    scenario = str(replay_row.get("scenario") or "")
    return _replay_scenario_rank(scenario) < _replay_scenario_rank("baseline")


def _has_directional_mass_magnitude_threshold(params: dict[str, Any]) -> bool:
    for key, value in params.items():
        lowered = key.lower()
        if "directional_mass" not in lowered:
            continue
        if lowered in {"use_directional_mass", "require_directional_mass"}:
            continue
        if _has_value(value):
            return True
    return False


def _has_aggregated_relative_volume_param(params: dict[str, Any]) -> bool:
    for key, value in params.items():
        if re.match(r"relative_volume_sum_\d+_over_ma_\d+", key):
            return _has_value(value)
    return False


def _has_any_value(params: dict[str, Any], keys: Iterable[str]) -> bool:
    return any(_has_value(params.get(key)) for key in keys)


def _read_optional_csv(path: str | Path | None) -> list[dict[str, str]]:
    if path is None or str(path).strip() == "":
        return []
    return _read_csv(Path(path))


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames: list[str] = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    with path.open("w", encoding="utf-8", newline="") as handle:
        if not fieldnames:
            return
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _run_sort_token(run_dir: Path) -> tuple[str, str]:
    return (run_dir.parent.name, run_dir.name)


def _display_path(path: Path) -> str:
    resolved = path.resolve()
    repo_root = Path(__file__).resolve().parents[2]
    try:
        return str(resolved.relative_to(repo_root))
    except ValueError:
        return str(resolved)


def _parse_scalar(value: Any) -> Any:
    if value is None:
        return None
    text = str(value).strip()
    if text == "":
        return ""
    lowered = text.lower()
    if lowered in {"true", "false"}:
        return lowered == "true"
    if "," in text:
        parts = [part.strip() for part in text.split(",") if part.strip()]
        if parts and all(re.fullmatch(r"-?\d+", part) for part in parts):
            return [int(part) for part in parts]
        return text
    if re.fullmatch(r"-?\d+", text):
        return int(text)
    if re.fullmatch(r"-?\d+\.\d+", text):
        return float(text)
    return text


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _explicit_false(value: Any) -> bool:
    if isinstance(value, bool):
        return value is False
    return str(value).strip().lower() in {"0", "false", "no", "n"}


def _has_value(value: Any) -> bool:
    if value in (None, ""):
        return False
    return str(value).strip().lower() not in {"none", "null", "nan"}


def _float_or_none(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _int_or_none(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _int_or_default(value: Any, default: int) -> int:
    parsed = _int_or_none(value)
    return default if parsed is None else parsed


def _float_or_default(value: Any, default: float) -> float:
    parsed = _float_or_none(value)
    return default if parsed is None else parsed


def _format_optional(value: Any) -> str:
    if value in (None, ""):
        return ""
    parsed = _float_or_none(value)
    if parsed is not None:
        return f"{parsed:.6g}"
    return str(value)


__all__ = [
    "M6_FEATURE_PARITY_CSV",
    "M6_PROVIDER_REVIEW_MD",
    "M6_PROVIDER_VALIDATION_CSV",
    "M6ProviderValidation",
    "ProviderValidationArtifacts",
    "build_m6_provider_validation",
    "classify_feature_parity",
    "classify_provider_validation_status",
    "discover_latest_m5_run_dirs",
    "load_m6_provider_validation",
    "max_feature_risk",
]
