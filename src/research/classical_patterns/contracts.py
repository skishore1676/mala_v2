"""Versioned contracts for deterministic classical-pattern research.

These contracts deliberately keep model/human chart observations separate from
the authoritative mechanics.  Breakout, Last Full Day, negation, objective,
lifecycle, and fills are derived by deterministic code from frozen config and
OHLCV bars.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime, time
from enum import Enum
import hashlib
import json
from pathlib import Path
from typing import Any, Iterable

import yaml


class BreakoutDirection(str, Enum):
    LONG = "long"
    SHORT = "short"


class LifecycleState(str, Enum):
    BREAKOUT = "breakout"
    BOUNDARY_RETEST = "boundary_retest"
    LFD_VIOLATED = "lfd_violated"
    OBJECTIVE_HIT = "objective_hit"
    NEGATED = "negated"
    EXPIRED = "expired"
    CENSORED = "censored"
    UNRESOLVED = "unresolved"


class BreakoutOutcome(str, Enum):
    TYPE_1 = "type_1"
    TYPE_2 = "type_2"
    TYPE_3 = "type_3"
    TYPE_4 = "type_4"
    CENSORED = "censored"
    UNRESOLVED = "unresolved"


@dataclass(frozen=True, slots=True)
class SessionDefinition:
    timezone: str
    market_open: time
    market_close: time
    minimum_source_bars: int
    adjustment_policy: str


@dataclass(frozen=True, slots=True)
class PatternDefinition:
    lookback_sessions: tuple[int, ...]
    atr_lookback_sessions: int
    pivot_span_sessions: int
    minimum_boundary_touches: int
    minimum_touch_separation_sessions: int
    minimum_touch_alternations: int
    boundary_tolerance_atr: float
    breakout_buffer_atr: float
    lfd_stop_buffer_atr: tuple[float, ...]
    negation_buffer_atr: float
    minimum_height_atr: float
    maximum_height_atr: float
    maximum_close_drift_height_fraction: float
    minimum_center_close_containment: float
    maximum_latest_touch_age_fraction: float
    objective_height_multiple: float
    maximum_lifecycle_sessions: int
    maximum_trade_sessions: int
    maximum_reentries: int


@dataclass(frozen=True, slots=True)
class ExecutionAssumptions:
    entry_timing: str
    same_bar_trade_ordering: str
    same_bar_outcome_ordering: str
    slippage_bps_each_side: float
    round_trip_cost_bps: float


@dataclass(frozen=True, slots=True)
class SplitDefinition:
    calibration_end: date
    validation_end: date
    holdout_end: date | None

    def label(self, event_date: date) -> str:
        if event_date <= self.calibration_end:
            return "calibration"
        if event_date <= self.validation_end:
            return "validation"
        if self.holdout_end is None or event_date <= self.holdout_end:
            return "holdout"
        return "post_holdout"


@dataclass(frozen=True, slots=True)
class PopulationDefinition:
    representative_policy: str
    include_all_representative_signals: bool
    human_review_may_filter_economics: bool


@dataclass(frozen=True, slots=True)
class RectangleResearchConfig:
    playbook_id: str
    version: int
    status: str
    timeframe: str
    session: SessionDefinition
    definition: PatternDefinition
    execution: ExecutionAssumptions
    splits: SplitDefinition
    population: PopulationDefinition
    source_path: Path
    source_hash: str


@dataclass(frozen=True, slots=True)
class RectangleCandidate:
    candidate_id: str
    symbol: str
    direction: BreakoutDirection
    breakout_index: int
    breakout_date: date
    breakout_time: datetime
    breakout_close: float
    pattern_start_date: date
    pattern_end_date: date
    lookback_sessions: int
    upper_boundary: float
    lower_boundary: float
    upper_edge: float
    lower_edge: float
    boundary_tolerance: float
    breakout_boundary: float
    atr: float
    height: float
    height_atr: float
    close_drift_fraction: float
    touch_alternations: int
    center_close_containment: float
    boundary_dispersion: float
    latest_touch_age_sessions: int
    upper_touch_indices: tuple[int, ...]
    lower_touch_indices: tuple[int, ...]
    lfd_index: int
    lfd_date: date
    lfd_high: float
    lfd_low: float
    base_stop: float
    structural_negation: float
    objective: float
    split: str
    tradeable: bool
    breakout_bar_diagnostic_codes: tuple[str, ...]

    @property
    def minimum_touch_count(self) -> int:
        return min(len(self.upper_touch_indices), len(self.lower_touch_indices))

    @property
    def total_touch_count(self) -> int:
        return len(self.upper_touch_indices) + len(self.lower_touch_indices)

    def representative_key(self) -> tuple[float | int | str, ...]:
        """Outcome-blind ordering for one breakout cluster.

        Geometry is selected before looking at the breakout close. Higher
        minimum-side touches, alternations, and containment win; then lower
        dispersion, fresher touches, and the shorter lookback. The id is a
        stable final tie-breaker.
        """

        return (
            -self.minimum_touch_count,
            -self.touch_alternations,
            -self.center_close_containment,
            self.boundary_dispersion,
            self.latest_touch_age_sessions,
            self.lookback_sessions,
            self.candidate_id,
        )


@dataclass(frozen=True, slots=True)
class RectangleSignal:
    signal_id: str
    candidate: RectangleCandidate
    cluster_candidate_count: int


@dataclass(frozen=True, slots=True)
class EnumerationRecord:
    record_id: str
    symbol: str
    breakout_index: int
    breakout_date: date
    lookback_sessions: int
    direction: BreakoutDirection | None
    status: str
    reason: str
    candidate_id: str | None
    qualifying_geometry_count: int


@dataclass(frozen=True, slots=True)
class LifecycleEvent:
    signal_id: str
    event_index: int
    event_date: date
    state: LifecycleState
    price: float | None
    detail: str


@dataclass(frozen=True, slots=True)
class OutcomeResult:
    signal_id: str
    outcome: BreakoutOutcome
    terminal_date: date
    sessions_observed: int
    boundary_retested: bool
    lfd_violated: bool
    terminal_reason: str


@dataclass(frozen=True, slots=True)
class TradeResult:
    signal_id: str
    variant_id: str
    direction: BreakoutDirection
    status: str
    entry_date: date | None
    entry_price: float | None
    stop_price: float
    target_price: float
    exit_date: date | None
    exit_price: float | None
    exit_reason: str
    bars_held: int
    gross_pnl: float | None
    net_pnl: float | None
    net_return: float | None
    net_r: float | None
    mfe: float | None
    mae: float | None


def load_rectangle_config(path: Path | str) -> RectangleResearchConfig:
    resolved = Path(path).expanduser().resolve()
    raw_bytes = resolved.read_bytes()
    payload = yaml.safe_load(raw_bytes) or {}
    if not isinstance(payload, dict):
        raise ValueError("Rectangle config must contain a YAML mapping.")

    _assert_exact_keys(
        payload,
        {
            "playbook_id",
            "version",
            "status",
            "timeframe",
            "session",
            "definition",
            "execution_assumptions",
            "splits",
            "population",
            "deferred",
        },
        "root",
    )

    session = _require_mapping(payload, "session")
    definition = _require_mapping(payload, "definition")
    execution = _require_mapping(payload, "execution_assumptions")
    splits = _require_mapping(payload, "splits")
    population = _require_mapping(payload, "population")
    deferred = _require_mapping(payload, "deferred")
    _assert_exact_keys(
        session,
        {"timezone", "market_open", "market_close", "minimum_source_bars", "adjustment_policy"},
        "session",
    )
    _assert_exact_keys(
        definition,
        {
            "lookback_sessions",
            "atr_lookback_sessions",
            "pivot_span_sessions",
            "minimum_boundary_touches",
            "minimum_touch_separation_sessions",
            "minimum_touch_alternations",
            "boundary_tolerance_atr",
            "breakout_buffer_atr",
            "lfd_stop_buffer_atr",
            "negation_buffer_atr",
            "minimum_height_atr",
            "maximum_height_atr",
            "maximum_close_drift_height_fraction",
            "minimum_center_close_containment",
            "maximum_latest_touch_age_fraction",
            "objective_height_multiple",
            "maximum_lifecycle_sessions",
            "maximum_trade_sessions",
            "maximum_reentries",
        },
        "definition",
    )
    _assert_exact_keys(
        execution,
        {
            "entry_timing",
            "same_bar_trade_ordering",
            "same_bar_outcome_ordering",
            "slippage_bps_each_side",
            "round_trip_cost_bps",
        },
        "execution_assumptions",
    )
    _assert_exact_keys(splits, {"calibration_end", "validation_end", "holdout_end"}, "splits")
    _assert_exact_keys(
        population,
        {
            "representative_policy",
            "include_all_representative_signals",
            "human_review_may_filter_economics",
        },
        "population",
    )
    _assert_exact_keys(
        deferred,
        {
            "agent_model_in_signal_path",
            "consultation",
            "playbook_packet",
            "bhiksha_runtime",
            "options_overlay",
        },
        "deferred",
    )
    invalid_deferred = {
        key: value for key, value in deferred.items() if not isinstance(value, bool) or value
    }
    if invalid_deferred:
        raise ValueError(
            "v1 fixture shadow requires every deferred integration to be false: "
            + ", ".join(sorted(invalid_deferred))
        )
    for section_name, section, fields in (
        (
            "population",
            population,
            {"include_all_representative_signals", "human_review_may_filter_economics"},
        ),
    ):
        invalid_booleans = [field for field in fields if not isinstance(section[field], bool)]
        if invalid_booleans:
            raise ValueError(
                f"Invalid boolean fields in {section_name}: {sorted(invalid_booleans)}"
            )

    config = RectangleResearchConfig(
        playbook_id=str(payload.get("playbook_id", "")),
        version=int(payload.get("version", 0)),
        status=str(payload.get("status", "")),
        timeframe=str(payload.get("timeframe", "")),
        session=SessionDefinition(
            timezone=str(session.get("timezone", "")),
            market_open=_parse_time(session.get("market_open")),
            market_close=_parse_time(session.get("market_close")),
            minimum_source_bars=int(session.get("minimum_source_bars", 0)),
            adjustment_policy=str(session.get("adjustment_policy", "")),
        ),
        definition=PatternDefinition(
            lookback_sessions=tuple(int(v) for v in definition.get("lookback_sessions", [])),
            atr_lookback_sessions=int(definition.get("atr_lookback_sessions", 0)),
            pivot_span_sessions=int(definition.get("pivot_span_sessions", 0)),
            minimum_boundary_touches=int(definition.get("minimum_boundary_touches", 0)),
            minimum_touch_separation_sessions=int(definition.get("minimum_touch_separation_sessions", 0)),
            minimum_touch_alternations=int(definition.get("minimum_touch_alternations", 0)),
            boundary_tolerance_atr=float(definition.get("boundary_tolerance_atr", 0.0)),
            breakout_buffer_atr=float(definition.get("breakout_buffer_atr", 0.0)),
            lfd_stop_buffer_atr=tuple(float(v) for v in definition.get("lfd_stop_buffer_atr", [])),
            negation_buffer_atr=float(definition.get("negation_buffer_atr", 0.0)),
            minimum_height_atr=float(definition.get("minimum_height_atr", 0.0)),
            maximum_height_atr=float(definition.get("maximum_height_atr", 0.0)),
            maximum_close_drift_height_fraction=float(definition.get("maximum_close_drift_height_fraction", 0.0)),
            minimum_center_close_containment=float(definition.get("minimum_center_close_containment", 0.0)),
            maximum_latest_touch_age_fraction=float(definition.get("maximum_latest_touch_age_fraction", 0.0)),
            objective_height_multiple=float(definition.get("objective_height_multiple", 0.0)),
            maximum_lifecycle_sessions=int(definition.get("maximum_lifecycle_sessions", 0)),
            maximum_trade_sessions=int(definition.get("maximum_trade_sessions", 0)),
            maximum_reentries=int(definition.get("maximum_reentries", 0)),
        ),
        execution=ExecutionAssumptions(
            entry_timing=str(execution.get("entry_timing", "")),
            same_bar_trade_ordering=str(execution.get("same_bar_trade_ordering", "")),
            same_bar_outcome_ordering=str(execution.get("same_bar_outcome_ordering", "")),
            slippage_bps_each_side=float(execution.get("slippage_bps_each_side", 0.0)),
            round_trip_cost_bps=float(execution.get("round_trip_cost_bps", 0.0)),
        ),
        splits=SplitDefinition(
            calibration_end=_parse_date(splits.get("calibration_end"), "calibration_end"),
            validation_end=_parse_date(splits.get("validation_end"), "validation_end"),
            holdout_end=_parse_optional_date(splits.get("holdout_end"), "holdout_end"),
        ),
        population=PopulationDefinition(
            representative_policy=str(population.get("representative_policy", "")),
            include_all_representative_signals=bool(population.get("include_all_representative_signals", False)),
            human_review_may_filter_economics=bool(population.get("human_review_may_filter_economics", True)),
        ),
        source_path=resolved,
        source_hash=hashlib.sha256(raw_bytes).hexdigest(),
    )
    validate_rectangle_config(config)
    return config


def validate_rectangle_config(config: RectangleResearchConfig) -> None:
    errors: list[str] = []
    if config.playbook_id != "classical-rectangle-breakout-daily":
        errors.append("unsupported playbook_id")
    if config.version != 1:
        errors.append("v1 implementation requires version=1")
    if config.status not in {"draft", "fixture_shadow", "frozen", "retired"}:
        errors.append("status must be draft, fixture_shadow, frozen, or retired")
    if config.timeframe != "1d":
        errors.append("v1 only supports timeframe=1d")
    if config.session.timezone != "America/New_York":
        errors.append("v1 requires America/New_York session grouping")
    if config.session.market_open >= config.session.market_close:
        errors.append("market_open must be before market_close")
    if config.session.minimum_source_bars <= 0:
        errors.append("minimum_source_bars must be positive")
    if config.session.adjustment_policy not in {"provider_adjusted", "split_adjusted"}:
        errors.append("unsupported adjustment_policy")

    definition = config.definition
    if not definition.lookback_sessions or any(v < 5 for v in definition.lookback_sessions):
        errors.append("lookback_sessions must contain values >= 5")
    if tuple(sorted(set(definition.lookback_sessions))) != definition.lookback_sessions:
        errors.append("lookback_sessions must be sorted and unique")
    if definition.atr_lookback_sessions < 2:
        errors.append("atr_lookback_sessions must be >= 2")
    if definition.pivot_span_sessions < 1:
        errors.append("pivot_span_sessions must be >= 1")
    if definition.minimum_boundary_touches < 2:
        errors.append("minimum_boundary_touches must be >= 2")
    if definition.minimum_touch_separation_sessions < 1:
        errors.append("minimum_touch_separation_sessions must be >= 1")
    if definition.minimum_touch_alternations < 1:
        errors.append("minimum_touch_alternations must be >= 1")
    for name, value in (
        ("boundary_tolerance_atr", definition.boundary_tolerance_atr),
        ("breakout_buffer_atr", definition.breakout_buffer_atr),
        ("negation_buffer_atr", definition.negation_buffer_atr),
        ("maximum_close_drift_height_fraction", definition.maximum_close_drift_height_fraction),
    ):
        if value < 0:
            errors.append(f"{name} must be non-negative")
    if not definition.lfd_stop_buffer_atr:
        errors.append("at least one lfd_stop_buffer_atr variant is required")
    if any(v < 0 for v in definition.lfd_stop_buffer_atr):
        errors.append("lfd_stop_buffer_atr values must be non-negative")
    if definition.minimum_height_atr <= 0:
        errors.append("minimum_height_atr must be positive")
    if definition.maximum_height_atr <= definition.minimum_height_atr:
        errors.append("maximum_height_atr must exceed minimum_height_atr")
    if not 0 < definition.minimum_center_close_containment <= 1:
        errors.append("minimum_center_close_containment must be in (0, 1]")
    if not 0 < definition.maximum_latest_touch_age_fraction <= 1:
        errors.append("maximum_latest_touch_age_fraction must be in (0, 1]")
    if definition.objective_height_multiple <= 0:
        errors.append("objective_height_multiple must be positive")
    if definition.maximum_trade_sessions <= 0:
        errors.append("maximum_trade_sessions must be positive")
    if definition.maximum_lifecycle_sessions < definition.maximum_trade_sessions:
        errors.append("maximum_lifecycle_sessions must be >= maximum_trade_sessions")
    if definition.maximum_reentries != 0:
        errors.append("v1 fixture shadow requires maximum_reentries=0")

    if config.execution.entry_timing != "next_session_open":
        errors.append("daily close confirmation requires entry_timing=next_session_open")
    if config.execution.same_bar_trade_ordering != "stop_first":
        errors.append("v1 requires conservative same_bar_trade_ordering=stop_first")
    if config.execution.same_bar_outcome_ordering != "unresolved":
        errors.append("v1 requires same_bar_outcome_ordering=unresolved")
    if config.execution.slippage_bps_each_side < 0:
        errors.append("slippage_bps_each_side must be non-negative")
    if config.execution.round_trip_cost_bps < 0:
        errors.append("round_trip_cost_bps must be non-negative")
    if config.splits.validation_end <= config.splits.calibration_end:
        errors.append("validation_end must follow calibration_end")
    if config.splits.holdout_end and config.splits.holdout_end <= config.splits.validation_end:
        errors.append("holdout_end must follow validation_end")
    if not config.population.include_all_representative_signals:
        errors.append("economic population must include all representative signals")
    if config.population.human_review_may_filter_economics:
        errors.append("human review may not filter the economic population")
    if config.population.representative_policy != "causal_geometry_quality":
        errors.append("unsupported representative_policy")

    if errors:
        raise ValueError("Invalid rectangle config: " + "; ".join(errors))


def contract_dict(value: Any) -> dict[str, Any]:
    """Convert a contract dataclass to a JSON-safe dictionary."""

    return _json_safe(asdict(value))


def stable_contract_hash(values: Iterable[Any]) -> str:
    payload = [_json_safe(asdict(value)) for value in values]
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _json_safe(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, (date, datetime, time)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return value


def _require_mapping(payload: dict[str, Any], key: str) -> dict[str, Any]:
    value = payload.get(key)
    if not isinstance(value, dict):
        raise ValueError(f"Rectangle config section {key!r} must be a mapping.")
    return value


def _assert_exact_keys(payload: dict[str, Any], allowed: set[str], section: str) -> None:
    unknown = set(payload) - allowed
    missing = allowed - set(payload)
    if unknown or missing:
        details: list[str] = []
        if unknown:
            details.append(f"unknown={sorted(unknown)}")
        if missing:
            details.append(f"missing={sorted(missing)}")
        raise ValueError(f"Invalid rectangle config keys in {section}: " + ", ".join(details))


def _parse_time(value: Any) -> time:
    try:
        return time.fromisoformat(str(value))
    except ValueError as exc:
        raise ValueError(f"Invalid session time: {value!r}") from exc


def _parse_date(value: Any, field: str) -> date:
    try:
        return date.fromisoformat(str(value))
    except ValueError as exc:
        raise ValueError(f"Invalid {field}: {value!r}") from exc


def _parse_optional_date(value: Any, field: str) -> date | None:
    if value in (None, ""):
        return None
    return _parse_date(value, field)
