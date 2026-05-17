"""Load versioned operator policies for playbook consultation artifacts."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from src.strategy.intraday_mean_reversion import PLAYBOOK_ID


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_POLICY_PATHS = {
    PLAYBOOK_ID: REPO_ROOT
    / "research/playbooks/operator_policies/mean_reversion_intraday_operator_v1.yaml"
}
CONFIDENCE_RANK = {"low": 0, "light": 1, "moderate": 2, "high": 3}


@dataclass(frozen=True, slots=True)
class OperatorPolicy:
    """Versioned thresholds used by the operator consultation lane."""

    data: dict[str, Any]
    source_path: Path | None = None

    @property
    def policy_id(self) -> str:
        return str(self.data.get("policy_id", ""))

    @property
    def policy_version(self) -> str:
        return str(self.data.get("policy_version", ""))

    @property
    def playbook_id(self) -> str:
        return str(self.data.get("playbook_id", ""))

    @property
    def rule_id(self) -> str:
        return f"{self.policy_id}:{self.policy_version}"

    @property
    def min_forward_n(self) -> int:
        return int(self.data.get("cohort", {}).get("min_forward_n", 15))

    @property
    def decision_window(self) -> str:
        return str(self.data.get("read_thresholds", {}).get("decision_window", "15"))

    @property
    def mixed_band(self) -> float:
        return _pct_points(self.data.get("read_thresholds", {}).get("mixed_band_pp", 10))

    @property
    def strong_reversion_edge(self) -> float:
        return _pct_points(
            self.data.get("read_thresholds", {}).get("strong_reversion_edge_pp", 20)
        )

    @property
    def strong_continuation_edge(self) -> float:
        return _pct_points(
            self.data.get("read_thresholds", {}).get("strong_continuation_edge_pp", 20)
        )

    @property
    def take_verdicts(self) -> set[str]:
        values = self.data.get("take_policy", {}).get("take_verdicts", [])
        return {str(value) for value in values}

    @property
    def min_confidence(self) -> str:
        return str(self.data.get("take_policy", {}).get("min_confidence", "moderate"))

    @property
    def min_cohort_n(self) -> int:
        return int(self.data.get("take_policy", {}).get("min_cohort_n", 60))

    @property
    def min_exit_survived(self) -> float:
        return _pct_points(self.data.get("take_policy", {}).get("min_exit_survived_pct", 45))

    @property
    def min_target_atr_fraction(self) -> float:
        return float(self.data.get("management", {}).get("min_target_atr_fraction", 0.10))

    @property
    def min_target_price_fraction(self) -> float:
        return float(self.data.get("management", {}).get("min_target_price_fraction", 0.0010))

    @property
    def exit_selection(self) -> str:
        return str(
            self.data.get("management", {}).get(
                "exit_selection",
                "max_survived_then_target_move",
            )
        )

    def confidence_for_count(self, count: int) -> str:
        thresholds = self.data.get("cohort", {}).get("confidence_min_counts", {})
        ordered = sorted(
            ((str(label), int(min_count)) for label, min_count in thresholds.items()),
            key=lambda item: item[1],
            reverse=True,
        )
        for label, min_count in ordered:
            if count >= min_count:
                return label
        return "low"

    def confidence_at_least(self, actual: str, minimum: str | None = None) -> bool:
        expected = minimum or self.min_confidence
        return CONFIDENCE_RANK.get(actual, -1) >= CONFIDENCE_RANK.get(expected, -1)

    def to_payload(self) -> dict[str, Any]:
        return {
            "policy_id": self.policy_id,
            "policy_version": self.policy_version,
            "rule_id": self.rule_id,
            "source_path": str(self.source_path) if self.source_path else "embedded",
            "config": self.data,
        }


def load_operator_policy(
    *,
    playbook_id: str = PLAYBOOK_ID,
    path: Path | None = None,
) -> OperatorPolicy:
    config_path = path or DEFAULT_POLICY_PATHS.get(playbook_id)
    if config_path is None:
        raise ValueError(f"No default operator policy registered for playbook {playbook_id!r}")
    data = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Operator policy {config_path} must contain a mapping")
    policy = OperatorPolicy(data=data, source_path=config_path)
    if policy.playbook_id and policy.playbook_id != playbook_id:
        raise ValueError(
            f"Operator policy {config_path} is for {policy.playbook_id!r}, not {playbook_id!r}"
        )
    return policy


def operator_policy_from_payload(
    payload: dict[str, Any],
    *,
    override_path: Path | None = None,
) -> OperatorPolicy:
    playbook_id = str(payload.get("playbook_id") or PLAYBOOK_ID)
    if override_path is not None:
        return load_operator_policy(playbook_id=playbook_id, path=override_path)
    embedded = payload.get("operator_policy")
    if isinstance(embedded, dict) and isinstance(embedded.get("config"), dict):
        return OperatorPolicy(
            data=dict(embedded["config"]),
            source_path=Path(str(embedded.get("source_path", "")))
            if embedded.get("source_path")
            else None,
        )
    return load_operator_policy(playbook_id=playbook_id)


def _pct_points(raw: Any) -> float:
    value = float(raw)
    return value / 100.0 if value > 1 else value
