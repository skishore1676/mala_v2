from __future__ import annotations

import polars as pl

from hypothesis_agent import (
    HypothesisState,
    _best_m5_row,
    _catalog_candidate_rows,
    _matching_promoted_candidate,
    parse_hypothesis,
    run_m1,
    write_run_summary,
)


def test_matching_promoted_candidate_uses_same_row_as_best_m5_selection() -> None:
    promoted = pl.DataFrame(
        [
            {
                "ticker": "SPY",
                "strategy": "Market Impulse (Cross & Reclaim)",
                "direction": "long",
                "entry_window_minutes": 45,
            },
            {
                "ticker": "SPY",
                "strategy": "Market Impulse (Cross & Reclaim)",
                "direction": "short",
                "entry_window_minutes": 60,
            },
        ]
    )
    m5 = pl.DataFrame(
        [
            {
                "ticker": "SPY",
                "strategy": "Market Impulse (Cross & Reclaim)",
                "direction": "long",
                "entry_window_minutes": 45,
                "execution_profile": "debit_spread_default",
                "mc_prob_positive_exp": 0.55,
            },
            {
                "ticker": "SPY",
                "strategy": "Market Impulse (Cross & Reclaim)",
                "direction": "short",
                "entry_window_minutes": 60,
                "execution_profile": "debit_spread_default",
                "mc_prob_positive_exp": 0.71,
            },
        ]
    )

    best = _best_m5_row(m5)
    assert best is not None
    matched = _matching_promoted_candidate(promoted, best, ["entry_window_minutes"])

    assert matched is not None
    assert matched["direction"] == "short"
    assert matched["entry_window_minutes"] == 60


def test_best_m5_row_prefers_single_option_profile_for_bhiksha() -> None:
    m5 = pl.DataFrame(
        [
            {
                "ticker": "AMD",
                "strategy": "Market Impulse",
                "direction": "short",
                "execution_profile": "debit_spread_default",
                "mc_prob_positive_exp": 0.99,
            },
            {
                "ticker": "AMD",
                "strategy": "Market Impulse",
                "direction": "short",
                "execution_profile": "single_option",
                "mc_prob_positive_exp": 0.71,
            },
        ]
    )

    best = _best_m5_row(m5)

    assert best is not None
    assert best["execution_profile"] == "single_option"


def test_catalog_candidate_rows_pick_one_target_profile_per_ticker_direction() -> None:
    m5 = pl.DataFrame(
        [
            {
                "ticker": "AMD",
                "strategy": "Market Impulse",
                "direction": "short",
                "execution_profile": "single_option",
                "mc_prob_positive_exp": 0.75,
            },
            {
                "ticker": "AMD",
                "strategy": "Market Impulse",
                "direction": "short",
                "execution_profile": "debit_spread_default",
                "mc_prob_positive_exp": 0.99,
            },
            {
                "ticker": "QQQ",
                "strategy": "Market Impulse",
                "direction": "short",
                "execution_profile": "single_option",
                "mc_prob_positive_exp": 0.80,
            },
        ]
    )

    selected = _catalog_candidate_rows(m5)

    assert [(r["ticker"], r["execution_profile"]) for r in selected] == [
        ("AMD", "single_option"),
        ("QQQ", "single_option"),
    ]
    assert [r["ticker"] for r in _catalog_candidate_rows(m5, min_mc_prob=0.78)] == ["QQQ"]


def test_run_summary_preserves_market_impulse_params(tmp_path) -> None:
    pl.DataFrame(
        [
            {
                "ticker": "AMD",
                "strategy": "Market Impulse",
                "direction": "short",
                "entry_buffer_minutes": 5,
                "entry_window_minutes": 90,
                "regime_timeframe": "30m",
                "vwma_periods": "5,13,21",
            },
        ]
    ).write_csv(tmp_path / "M2_promoted.csv")
    pl.DataFrame(
        [
            {
                "ticker": "AMD",
                "strategy": "Market Impulse",
                "direction": "short",
                "entry_buffer_minutes": 5,
                "entry_window_minutes": 90,
                "regime_timeframe": "30m",
                "vwma_periods": "5,13,21",
                "holdout_signals": 62,
                "holdout_exp_r": 0.4,
                "passes_cost_gate": True,
            },
        ]
    ).write_csv(tmp_path / "M4_holdout.csv")
    pl.DataFrame(
        [
            {
                "ticker": "AMD",
                "strategy": "Market Impulse",
                "direction": "short",
                "entry_buffer_minutes": 5,
                "entry_window_minutes": 90,
                "regime_timeframe": "30m",
                "vwma_periods": "5,13,21",
                "execution_profile": "single_option",
                "selected_ratio": 2.0,
                "base_exp_r": 0.3,
                "mc_exp_r_p50": 0.2,
                "mc_prob_positive_exp": 0.8,
                "mc_max_dd_p50": -1.0,
            },
        ]
    ).write_csv(tmp_path / "M5_execution.csv")
    hypothesis = HypothesisState(
        path=tmp_path / "idea.md",
        id="idea",
        state="pending",
        decision="",
        tickers=["AMD"],
        strategy="Market Impulse",
        max_stage="M5",
        search_mode="discovery",
        directions=["short"],
        max_configs=8,
    )

    path = write_run_summary(
        out_dir=tmp_path,
        hypothesis=hypothesis,
        stages_run=["M1", "M2", "M4", "M5"],
        decision="promote",
        notes=[],
    )

    text = path.read_text()
    assert "entry_buffer_minutes" in text
    assert "entry_window_minutes" in text
    assert "regime_timeframe" in text
    assert "vwma_periods" in text


def test_parse_hypothesis_reads_max_configs(tmp_path) -> None:
    path = tmp_path / "idea.md"
    path.write_text(
        "\n".join(
            [
                "# Hypothesis",
                "- id: `idea`",
                "- state: `pending`",
                "- decision: ``",
                "- symbol_scope: `SPY, QQQ`",
                "- strategy: `Elastic Band Reversion`",
                "- max_stage: `M4`",
                "- search_mode: `fixed`",
                "- direction_scope: `long`",
                "- max_configs: `128`",
            ]
        )
    )

    hypothesis = parse_hypothesis(path)

    assert hypothesis.max_configs == 128
    assert hypothesis.search_mode == "fixed"
    assert hypothesis.directions == ["long"]


def test_run_m1_honors_direction_scope(monkeypatch) -> None:
    def fake_run_walk_forward_for_strategies(**_kwargs):
        return [
            {
                "ticker": "NVDA",
                "strategy": "Elastic Band z=3.0/w=120+dm",
                "direction": "short",
                "oos_windows": 4,
                "pct_positive_oos_windows": 1.0,
                "avg_test_exp_r": 0.5,
                "oos_signals": 100,
            },
            {
                "ticker": "NVDA",
                "strategy": "Elastic Band z=3.0/w=120+dm",
                "direction": "long",
                "oos_windows": 4,
                "pct_positive_oos_windows": 1.0,
                "avg_test_exp_r": 0.1,
                "oos_signals": 100,
            },
        ]

    monkeypatch.setattr("hypothesis_agent.run_walk_forward_for_strategies", fake_run_walk_forward_for_strategies)
    monkeypatch.setattr("hypothesis_agent.aggregate_walk_forward", lambda rows: pl.DataFrame(rows))

    _detail, _aggregate, top = run_m1(
        strategy_name="Elastic Band z=3.0/w=120+dm",
        frames={"NVDA": pl.DataFrame({"close": [1.0]})},
        windows=[],
        configs=[
            {
                "z_score_threshold": 3.0,
                "z_score_window": 120,
                "kinematic_periods_back": 1,
                "use_directional_mass": True,
                "use_jerk_confirmation": True,
            }
        ],
        metrics=object(),
        top_per_ticker=2,
        directions=["long"],
    )

    assert top.height == 1
    assert top.row(0, named=True)["direction"] == "long"
