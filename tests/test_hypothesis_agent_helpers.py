from __future__ import annotations

import polars as pl

from hypothesis_agent import _best_m5_row, _matching_promoted_candidate, parse_hypothesis, run_m1


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
