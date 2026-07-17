from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, time, timedelta, timezone
import hashlib
from pathlib import Path

import polars as pl
import pytest
import yaml

from src.oracle.rectangle_trade_simulator import simulate_rectangle_trade
from src.research.classical_patterns.contracts import (
    BreakoutDirection,
    BreakoutOutcome,
    RectangleCandidate,
    RectangleSignal,
    SessionDefinition,
    load_rectangle_config,
    validate_rectangle_config,
)
from src.research.classical_patterns.daily_bars import (
    build_rth_daily_bars,
    normalize_daily_input,
)
from src.research.classical_patterns.lifecycle import derive_lifecycle
from src.research.classical_patterns.rectangle import enumerate_rectangles
from src.research.classical_patterns.runner import run_research


CONFIG_PATH = Path("config/classical_patterns/rectangle_daily_v1.yaml")


def _config():
    return load_rectangle_config(CONFIG_PATH)


def _rectangle_frame(*, future_delta: float = 0.0) -> pl.DataFrame:
    """Synthetic, causal rectangle with a close-confirmed breakout at index 60."""

    start = date(2021, 1, 1)
    rows: list[dict[str, object]] = []
    for index in range(80):
        high, low, close = 102.0, 98.0, 100.0
        if index % 8 in {1, 5}:
            high = 105.0
            close = 101.0
        elif index % 8 in {3, 7}:
            low = 95.0
            close = 99.0
        if index == 60:
            high, low, close = 109.0, 104.0, 108.0
        elif index > 60:
            high += future_delta
            low -= future_delta
            close += future_delta
        session_date = start + timedelta(days=index)
        rows.append(
            {
                "session_date": session_date,
                "visible_at": datetime.combine(
                    session_date, time(21), tzinfo=timezone.utc
                ),
                "symbol": "TEST",
                "open": close,
                "high": high,
                "low": low,
                "close": close,
                "volume": 1_000_000.0,
                "source_bar_count": 390,
                "complete_session": True,
            }
        )
    return pl.DataFrame(rows)


def _candidate(
    *,
    direction: BreakoutDirection = BreakoutDirection.LONG,
    breakout_index: int = 0,
) -> RectangleCandidate:
    long = direction is BreakoutDirection.LONG
    return RectangleCandidate(
        candidate_id=f"candidate-{direction.value}",
        symbol="TEST",
        direction=direction,
        breakout_index=breakout_index,
        breakout_date=date(2024, 1, 2),
        breakout_time=datetime(2024, 1, 2, 21, tzinfo=timezone.utc),
        breakout_close=106.0 if long else 94.0,
        pattern_start_date=date(2023, 12, 1),
        pattern_end_date=date(2024, 1, 1),
        lookback_sessions=20,
        upper_boundary=105.0,
        lower_boundary=95.0,
        upper_edge=105.0,
        lower_edge=95.0,
        boundary_tolerance=0.0,
        breakout_boundary=105.0 if long else 95.0,
        atr=2.0,
        height=10.0,
        height_atr=5.0,
        close_drift_fraction=0.0,
        touch_alternations=3,
        center_close_containment=1.0,
        boundary_dispersion=0.0,
        latest_touch_age_sessions=1,
        upper_touch_indices=(2, 10),
        lower_touch_indices=(6, 14),
        lfd_index=max(0, breakout_index - 1),
        lfd_date=date(2024, 1, 1),
        lfd_high=104.0,
        lfd_low=100.0,
        base_stop=100.0 if long else 100.0,
        structural_negation=95.0 if long else 105.0,
        objective=115.0 if long else 85.0,
        split="validation",
        tradeable=True,
        breakout_bar_diagnostic_codes=(),
    )


def _signal(**kwargs) -> RectangleSignal:
    candidate = _candidate(**kwargs)
    return RectangleSignal(
        signal_id=f"signal:{candidate.candidate_id}",
        candidate=candidate,
        cluster_candidate_count=1,
    )


def _daily_rows(values: list[tuple[float, float, float, float]]) -> pl.DataFrame:
    start = date(2024, 1, 2)
    return normalize_daily_input(
        pl.DataFrame(
            {
                "session_date": [start + timedelta(days=i) for i in range(len(values))],
                "open": [row[0] for row in values],
                "high": [row[1] for row in values],
                "low": [row[2] for row in values],
                "close": [row[3] for row in values],
                "volume": [1_000.0] * len(values),
            }
        ),
        symbol="TEST",
    )


def test_config_is_strict_and_fixture_shadow_is_non_executable(tmp_path: Path) -> None:
    config = _config()
    assert config.status == "fixture_shadow"
    assert config.definition.maximum_reentries == 0
    assert config.population.human_review_may_filter_economics is False

    payload = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8"))
    payload["unexpected"] = True
    invalid = tmp_path / "invalid.yaml"
    invalid.write_text(yaml.safe_dump(payload), encoding="utf-8")
    with pytest.raises(ValueError, match=r"unknown=\['unexpected'\]"):
        load_rectangle_config(invalid)

    with pytest.raises(ValueError, match="maximum_reentries=0"):
        validate_rectangle_config(
            replace(config, definition=replace(config.definition, maximum_reentries=1))
        )
    with pytest.raises(ValueError, match="version=1"):
        validate_rectangle_config(replace(config, version=2))


def test_daily_input_rejects_null_and_nonfinite_prices() -> None:
    base = {
        "session_date": [date(2024, 1, 2)],
        "open": [100.0],
        "high": [101.0],
        "low": [99.0],
        "close": [100.0],
        "volume": [1_000.0],
    }
    for field, invalid in (("close", None), ("high", float("inf"))):
        payload = {key: list(values) for key, values in base.items()}
        payload[field] = [invalid]
        with pytest.raises(ValueError, match="null|required|non-finite"):
            normalize_daily_input(pl.DataFrame(payload), symbol="TEST")


def test_daily_builder_uses_new_york_session_across_dst_and_filters_incomplete() -> None:
    session = SessionDefinition(
        timezone="America/New_York",
        market_open=time(9, 30),
        market_close=time(16),
        minimum_source_bars=2,
        adjustment_policy="provider_adjusted",
    )
    source = pl.DataFrame(
        {
            "timestamp": [
                datetime(2024, 1, 8, 14, 30, tzinfo=timezone.utc),
                datetime(2024, 1, 8, 20, 59, tzinfo=timezone.utc),
                datetime(2024, 7, 8, 13, 30, tzinfo=timezone.utc),
            ],
            "open": [100.0, 101.0, 200.0],
            "high": [101.0, 102.0, 201.0],
            "low": [99.0, 100.0, 199.0],
            "close": [100.5, 101.5, 200.5],
            "volume": [10.0, 20.0, 30.0],
        }
    )
    all_days = build_rth_daily_bars(
        source, symbol="dst", session=session, require_complete=False
    )
    assert all_days.get_column("session_date").to_list() == [
        date(2024, 1, 8),
        date(2024, 7, 8),
    ]
    assert all_days.get_column("source_bar_count").to_list() == [2, 1]
    complete = build_rth_daily_bars(
        source, symbol="dst", session=session, require_complete=True
    )
    assert complete.get_column("session_date").to_list() == [date(2024, 1, 8)]


def test_enumerator_is_prefix_invariant_and_future_poison_safe() -> None:
    config = _config()
    prefix = _rectangle_frame().head(61)
    normal = enumerate_rectangles(prefix, config)
    poisoned = enumerate_rectangles(_rectangle_frame(future_delta=50.0), config)
    prefix_ids = {candidate.candidate_id for candidate in normal.candidates}
    poisoned_prefix_ids = {
        candidate.candidate_id
        for candidate in poisoned.candidates
        if candidate.breakout_index <= 60
    }
    assert prefix_ids
    assert prefix_ids == poisoned_prefix_ids
    assert all(candidate.breakout_index == 60 for candidate in normal.candidates)
    assert len(normal.signals) == 1


def test_representative_tie_prefers_shorter_lookback_then_stable_id() -> None:
    base = _candidate()
    shorter = replace(base, candidate_id="z-short", lookback_sessions=20)
    longer = replace(base, candidate_id="a-long", lookback_sessions=40)
    lexical = replace(base, candidate_id="a-short", lookback_sessions=20)
    assert shorter.representative_key() < longer.representative_key()
    assert lexical.representative_key() < shorter.representative_key()


def test_enumerator_has_long_short_price_mirror_symmetry() -> None:
    config = _config()
    long_frame = _rectangle_frame().head(61)
    mirrored = long_frame.with_columns(
        (pl.lit(200.0) - pl.col("open")).alias("open"),
        (pl.lit(200.0) - pl.col("low")).alias("high"),
        (pl.lit(200.0) - pl.col("high")).alias("low"),
        (pl.lit(200.0) - pl.col("close")).alias("close"),
    )
    long_result = enumerate_rectangles(long_frame, config)
    short_result = enumerate_rectangles(mirrored, config)
    assert len(long_result.candidates) == len(short_result.candidates)
    assert len(long_result.signals) == len(short_result.signals) == 1
    long = long_result.signals[0].candidate
    short = short_result.signals[0].candidate
    assert long.direction is BreakoutDirection.LONG
    assert short.direction is BreakoutDirection.SHORT
    assert short.lower_boundary == pytest.approx(200.0 - long.upper_boundary)
    assert short.upper_boundary == pytest.approx(200.0 - long.lower_boundary)


@pytest.mark.parametrize(
    ("continuation", "expected"),
    [
        ([(106.0, 116.0, 106.0, 115.0)], BreakoutOutcome.TYPE_1),
        (
            [(106.0, 110.0, 104.0, 108.0), (109.0, 116.0, 106.0, 115.0)],
            BreakoutOutcome.TYPE_2,
        ),
        (
            [(106.0, 110.0, 99.0, 108.0), (109.0, 116.0, 106.0, 115.0)],
            BreakoutOutcome.TYPE_3,
        ),
        ([(106.0, 110.0, 94.0, 96.0)], BreakoutOutcome.TYPE_4),
        ([(106.0, 116.0, 104.0, 110.0)], BreakoutOutcome.UNRESOLVED),
    ],
)
def test_lifecycle_type_taxonomy_and_same_bar_ambiguity(
    continuation: list[tuple[float, float, float, float]],
    expected: BreakoutOutcome,
) -> None:
    breakout = (106.0, 109.0, 106.0, 108.0)
    _, outcome = derive_lifecycle(
        _signal(), _daily_rows([breakout, *continuation]), _config()
    )
    assert outcome.outcome is expected


def test_trade_simulator_enters_next_open_and_resolves_same_bar_stop_first() -> None:
    bars = _daily_rows(
        [
            (106.0, 109.0, 106.0, 108.0),
            (103.0, 116.0, 99.0, 110.0),
        ]
    )
    result = simulate_rectangle_trade(
        _signal(), bars, stop_buffer_atr=0.0, config=_config()
    )
    assert result.status == "closed"
    assert result.entry_date == date(2024, 1, 3)
    assert result.exit_reason == "same_bar_stop_first"
    assert result.net_r is not None and result.net_r < 0


def test_breakout_bar_level_touches_are_diagnostic_not_post_signal_outcomes() -> None:
    frame = _rectangle_frame().with_columns(
        pl.when(pl.int_range(pl.len()) == 60)
        .then(pl.lit(94.0))
        .otherwise(pl.col("low"))
        .alias("low")
    )
    result = enumerate_rectangles(frame.head(61), _config())
    assert result.signals
    candidate = result.signals[0].candidate
    assert candidate.tradeable is True
    assert "breakout_bar_spans_lfd" in candidate.breakout_bar_diagnostic_codes


def test_trade_simulator_mirrors_long_and_short_next_open_gap_rules() -> None:
    long_result = simulate_rectangle_trade(
        _signal(),
        _daily_rows([(106.0, 109.0, 106.0, 108.0), (99.0, 104.0, 98.0, 100.0)]),
        stop_buffer_atr=0.0,
        config=_config(),
    )
    short_result = simulate_rectangle_trade(
        _signal(direction=BreakoutDirection.SHORT),
        _daily_rows([(94.0, 94.0, 91.0, 92.0), (101.0, 102.0, 96.0, 100.0)]),
        stop_buffer_atr=0.0,
        config=_config(),
    )
    assert long_result.exit_reason == "entry_open_through_stop"
    assert short_result.exit_reason == "entry_open_through_stop"


def test_later_gap_exit_does_not_use_post_exit_intraday_excursions() -> None:
    result = simulate_rectangle_trade(
        _signal(),
        _daily_rows(
            [
                (106.0, 109.0, 106.0, 108.0),
                (103.0, 108.0, 101.0, 106.0),
                (99.0, 130.0, 80.0, 120.0),
            ]
        ),
        stop_buffer_atr=0.0,
        config=_config(),
    )
    assert result.exit_reason == "gap_stop"
    assert result.mfe == pytest.approx(108.0 - result.entry_price)
    assert result.mae == pytest.approx(result.entry_price - 101.0)


def test_fixture_shadow_runner_writes_reconciled_non_executable_receipt(
    tmp_path: Path,
) -> None:
    output = tmp_path / "run"
    receipt = run_research(
        {"TEST": _rectangle_frame()},
        config=_config(),
        output_dir=output,
        run_id="fixture-proof",
        mode="fixture_shadow",
        argv=["fixture-shadow"],
    )
    assert receipt["executable"] is False
    assert receipt["readiness"] == "fixture_shadow"
    assert all(receipt["population_checks"].values())
    assert receipt["population"]["representative_signals"] >= 1
    assert (output / "receipt.json").exists()
    assert (output / "candidates.csv").exists()
    candidates = pl.read_csv(output / "candidates.csv")
    assert candidates.get_column("upper_touch_indices")[0].startswith("[")
    for name, metadata in receipt["artifacts"].items():
        if name == "receipt.json":
            continue
        assert metadata["content_hash"] == hashlib.sha256(
            (output / name).read_bytes()
        ).hexdigest()
