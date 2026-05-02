"""
Strategy_Catalog writer.

Upserts a single row into the Strategy_Catalog Google Sheet tab.
Called when a hypothesis reaches `promote` (M5 pass).
The row lands with lifecycle_status=candidate; a human flips it to approved.

Column schema matches the live sheet exactly:
    catalog_key, playbook_id, symbol, bias_template, strategy_key,
    strategy_family, direction, lifecycle_status, operator_status_override,
    operator_notes, bhiksha_ready, first_validated_date, last_validated_date,
    validation_count, expectancy, confidence, signal_count,
    execution_robustness, thesis_exit_policy, playbook_summary_json

Source for each field (from M5 run):
    expectancy          ← m5_best["base_exp_r"]
    confidence          ← m5_best["holdout_win_rate"]
    signal_count        ← m5_best["holdout_trades"]
    execution_robustness← m5_best["mc_prob_positive_exp"]
    thesis_exit_policy  ← optimized thesis exit policy
    bias_template       ← _BIAS_TEMPLATE_MAP[(strategy_key, direction)]
    playbook_summary_json ← restricted Mala evidence only; runtime fields are
                            marked operator_required
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

from src.research.google_sheets import GoogleSheetTableClient
from src.research.mala_handoff import MALA_HANDOFF_VERSION, derive_signal_window
from src.research.strategy_keys import to_strategy_key

# Must match the column order in the live Google Sheet exactly.
STRATEGY_CATALOG_HEADERS = [
    "catalog_key",
    "playbook_id",
    "symbol",
    "bias_template",
    "strategy_key",
    "strategy_family",
    "direction",
    "lifecycle_status",
    "operator_status_override",
    "operator_notes",
    "bhiksha_ready",
    "first_validated_date",
    "last_validated_date",
    "validation_count",
    "expectancy",
    "confidence",
    "signal_count",
    "execution_robustness",
    "thesis_exit_policy",
    "playbook_summary_json",
]

# Columns that are M5 metadata, not strategy entry params.
_M5_NON_PARAM_COLS = {
    "ticker", "strategy", "direction",
    "execution_profile", "stress_profile",
    "selected_ratio", "evaluation_window",
    "holdout_trades", "holdout_win_rate", "base_exp_r", "trades",
    "passes_all_cost_gates", "passes_holdout",
    "mc_prob_positive_exp", "mc_exp_r_mean",
    "mc_exp_r_p05", "mc_exp_r_p50", "mc_exp_r_p95",
    "mc_total_r_p05", "mc_total_r_p50", "mc_total_r_p95",
    "mc_max_dd_p50", "mc_max_drawdown_p50",
    "structure", "dte", "delta_plan",
    "entry_window_et", "profit_take", "risk_rule",
}

# strategy_key → direction → bias_template
_BIAS_TEMPLATE_MAP: dict[tuple[str, str], str] = {
    ("elastic_band_reversion",          "long"):  "bullish_mean_reversion_intraday",
    ("elastic_band_reversion",          "short"): "bearish_mean_reversion_intraday",
    ("compression_expansion_breakout",  "long"):  "bullish_mean_reversion_intraday",
    ("compression_expansion_breakout",  "short"): "bearish_mean_reversion_intraday",
    ("market_impulse",                  "long"):  "bullish_trend_intraday",
    ("market_impulse",                  "short"): "bearish_trend_intraday",
    ("opening_drive_classifier",        "long"):  "bullish_trend_intraday",
    ("opening_drive_classifier",        "short"): "bearish_trend_intraday",
    ("opening_drive_v2",                "long"):  "bullish_trend_intraday",
    ("opening_drive_v2",                "short"): "bearish_trend_intraday",
    ("jerk_pivot_momentum",             "long"):  "bullish_trend_intraday",
    ("jerk_pivot_momentum",             "short"): "bearish_trend_intraday",
    ("kinematic_ladder",                "long"):  "bullish_trend_intraday",
    ("kinematic_ladder",                "short"): "bearish_trend_intraday",
    ("regime_router",                   "long"):  "bullish_trend_intraday",
    ("regime_router",                   "short"): "bearish_trend_intraday",
}

# ---------------------------------------------------------------------------
# Bhiksha capability registry
# ---------------------------------------------------------------------------
# Keep these in sync with the bhiksha repo whenever you add a new strategy
# class (src/bhiksha/strategy/*.py) or a new exit policy handler
# (src/bhiksha/execution/thesis_exit.py).
#
# Update rule:
#   • New strategy class in bhiksha  → add its `key` value to _BHIKSHA_STRATEGY_KEYS
#   • New exit handler in bhiksha    → add the policy string to _BHIKSHA_EXIT_POLICIES
# ---------------------------------------------------------------------------
_BHIKSHA_STRATEGY_KEYS: frozenset[str] = frozenset({
    "elastic_band_reversion",
    "jerk_pivot_momentum",
    "manual_breakout",
    "manual_trigger",
    "market_impulse",
    "opening_drive_classifier",
})

_BHIKSHA_EXIT_POLICIES: frozenset[str] = frozenset({
    "fixed_rr_underlying",
    "trailing_vma_underlying",
    "time_stop_underlying",
    "hold_to_eod_underlying",
    "ma_trailing_underlying",
    "ma_crossover_underlying",
    "atr_trailing_underlying",
})


def _is_bhiksha_ready(strategy_key: str, thesis_exit_policy: str | None) -> bool:
    """Return True when both the strategy and exit policy are implemented in Bhiksha."""
    if strategy_key not in _BHIKSHA_STRATEGY_KEYS:
        return False
    if thesis_exit_policy and thesis_exit_policy not in _BHIKSHA_EXIT_POLICIES:
        return False
    return True


def _can_compile_after_operator_review(strategy_key: str, thesis_exit_policy: str | None) -> bool:
    """Return True when Bhiksha has code support after operator runtime config is supplied."""
    return _is_bhiksha_ready(strategy_key, thesis_exit_policy)


def _to_strategy_key(strategy_display_name: str) -> str:
    """Backward-compatible wrapper for older catalog tests/callers."""
    return to_strategy_key(strategy_display_name)


def _bias_template(strategy_key: str, direction: str) -> str:
    return _BIAS_TEMPLATE_MAP.get(
        (strategy_key, direction),
        f"{'bullish' if direction == 'long' else 'bearish'}_trend_intraday",
    )


def _build_playbook_summary(
    m5_best: dict[str, Any],
    exit_opt: dict[str, Any] | None = None,
    strategy_key: str = "",
) -> str:
    """Build restricted Mala-owned evidence for Strategy_Catalog.

    Mala may publish strategy evidence and optimized underlying thesis exits.
    Runtime option vehicle, execution window, premium budget, option stops and
    targets, live/shadow mode, and conflict policy are operator/Bhiksha owned.
    """
    if not exit_opt:
        raise ValueError("Strategy_Catalog publish requires a tested thesis exit artifact.")
    entry_params = {
        k: v for k, v in m5_best.items()
        if k not in _M5_NON_PARAM_COLS and v not in (None, "")
    }
    signal_start, signal_end, derivation = derive_signal_window(strategy_key, entry_params)

    mc_metrics = {
        k: m5_best.get(k)
        for k in (
            "mc_prob_positive_exp",
            "mc_exp_r_mean", "mc_exp_r_p50", "mc_exp_r_p95",
            "mc_total_r_p05", "mc_total_r_p50", "mc_total_r_p95",
        )
    }

    thesis_exit_policy = str(exit_opt.get("thesis_exit_policy") or "")
    can_compile_after_review = _can_compile_after_operator_review(strategy_key, thesis_exit_policy)
    warnings = [
        f"legacy_m5_execution_mapping_ignored:{field}"
        for field in ("structure", "dte", "delta_plan", "entry_window_et", "profit_take", "risk_rule")
        if m5_best.get(field) not in (None, "")
    ]
    if not signal_start and not signal_end:
        warnings.append("strategy_signal_window_not_declared")

    blob: dict[str, Any] = {
        "mala_handoff_version": MALA_HANDOFF_VERSION,
        "bhiksha_compatibility": {
            "bhiksha_ready": False,
            "can_compile_after_operator_review": can_compile_after_review,
            "has_optimized_thesis_exit": True,
            "supported_strategy_and_exit": can_compile_after_review,
            "note": (
                "Mala evidence only; operator runtime bridge config is required before Bhiksha live consumption."
            ),
        },
        "strategy": {
            "strategy_key": strategy_key,
            "params": entry_params,
            "signal_window_start_et": signal_start,
            "signal_window_end_et": signal_end,
            "signal_window_derivation": derivation,
        },
        "runtime_requirements": {
            "option_vehicle": "operator_required",
            "execution_window": "operator_required",
            "max_premium_usd": "operator_required",
            "option_premium_stop": "operator_required",
            "option_premium_target": "operator_required",
            "live_or_shadow": "operator_required",
            "conflict_policy": "operator_required",
        },
        "mc_metrics": mc_metrics,
        "m5_evidence": {
            "execution_profile_tested": m5_best.get("execution_profile", ""),
            "stress_profile_tested": m5_best.get("stress_profile", ""),
            "expectancy": m5_best.get("base_exp_r"),
            "confidence": m5_best.get("holdout_win_rate"),
            "signal_count": m5_best.get("holdout_trades"),
            "execution_robustness": m5_best.get("mc_prob_positive_exp"),
            "note": "M5 evidence is research/backtest evidence, not live option execution authorization.",
        },
        "validation": {
            "status": "needs_operator_runtime_config",
            "warnings": warnings,
        },
        "thesis_exit": {
            "tested": True,
            "anchor": exit_opt.get("thesis_exit_anchor", "underlying"),
            "policy": thesis_exit_policy,
            "policy_name": exit_opt.get("selected_policy_name", ""),
            "params": exit_opt.get("thesis_exit_params", {}),
            "metrics": exit_opt.get("selected_metrics", {}),
            "catastrophe_exit_anchor": exit_opt.get("catastrophe_exit_anchor", "option_premium"),
            "catastrophe_exit_params": exit_opt.get("catastrophe_exit_params", {}),
            "note": "Underlying thesis exit was selected by Mala exit optimization; option premium handling remains operator/Bhiksha owned.",
        },
    }
    blob["thesis_exit_params"] = exit_opt.get("thesis_exit_params", {})
    blob["catastrophe_exit_params"] = exit_opt.get("catastrophe_exit_params", {})
    blob["exit_candidate_policies"] = exit_opt.get("candidate_policies", [])
    blob["exit_controls"] = {
        "use_algorithmic_exit": False,
        "native_strategy_exit_policy": None,
        "exit_stack": "thesis_exit_then_catastrophe",
        "note": "Mala thesis exit is authoritative; native exits require explicit opt-in.",
    }

    return json.dumps(blob, default=str)


def upsert_strategy_catalog(
    *,
    catalog_key: str,
    symbol: str,
    strategy: str,
    m5_best: dict[str, Any],
    spreadsheet_id: str,
    credentials_path: str | Path,
    sheet_name: str = "Strategy_Catalog",
    exit_opt: dict[str, Any] | None = None,
) -> None:
    """Write or update one row in Strategy_Catalog from M5 results.

    Matches on `catalog_key`. If not found, appends a new row.
    Credentials must be a service-account JSON path.
    """
    client = GoogleSheetTableClient(
        spreadsheet_id=spreadsheet_id,
        sheet_name=sheet_name,
        credentials_path=Path(credentials_path),
    )
    client.ensure_sheet_exists()

    today = date.today().isoformat()
    strategy_key = _to_strategy_key(strategy)
    direction = str(m5_best.get("direction", "long"))
    if not exit_opt:
        raise ValueError(f"Refusing to publish {catalog_key}: tested thesis exit artifact is required.")
    thesis_exit_policy = str(exit_opt.get("thesis_exit_policy") or "")

    row: dict[str, Any] = {
        "catalog_key":              catalog_key,
        "playbook_id":              catalog_key,
        "symbol":                   symbol,
        "bias_template":            _bias_template(strategy_key, direction),
        "strategy_key":             strategy_key,
        "strategy_family":          strategy_key,
        "direction":                direction,
        "lifecycle_status":         "candidate",
        "operator_status_override": "",
        "operator_notes":           "operator runtime bridge config required",
        "bhiksha_ready":             "false",
        "first_validated_date":     today,
        "last_validated_date":      today,
        "validation_count":         1,
        "expectancy":               round(float(m5_best.get("base_exp_r") or 0), 4),
        "confidence":               round(float(m5_best.get("holdout_win_rate") or 0), 4),
        "signal_count":             int(m5_best.get("holdout_trades") or 0),
        "execution_robustness":     round(float(m5_best.get("mc_prob_positive_exp") or 0), 5),
        "thesis_exit_policy":       thesis_exit_policy,
        "playbook_summary_json":    _build_playbook_summary(m5_best, exit_opt, strategy_key),
    }

    existing = client.read_rows()

    for ex in existing:
        if ex.get("catalog_key") == catalog_key:
            ex.update(row)
            client.batch_update_rows(rows=[ex], columns=list(row.keys()))
            return

    if not existing:
        client.overwrite_table(headers=STRATEGY_CATALOG_HEADERS, rows=[row])
        return

    values = [[row.get(h, "") for h in STRATEGY_CATALOG_HEADERS]]
    client.service.spreadsheets().values().append(
        spreadsheetId=client.spreadsheet_id,
        range=f"{sheet_name}!A1",
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body={"values": values},
    ).execute()
