"""Search-space helpers for the hypothesis workbench."""

from __future__ import annotations

from itertools import product
from typing import Any

from src.research.models import ParameterSpec, StrategySearchSpec
from src.strategy.factory import build_strategy_by_name


def build_search_configs(
    strategy_name: str,
    *,
    mode: str,
    max_configs: int = 32,
) -> list[dict[str, Any]]:
    """Return bounded configs from the strategy's own declared search surface."""
    strategy = build_strategy_by_name(strategy_name)
    if mode == "fixed":
        return [strategy.search_config()]
    search_spec = strategy.search_spec
    if search_spec is not None:
        space = _space_from_spec(search_spec, mode=mode)
        return _valid_configs(search_spec, space, max_configs=max_configs)

    space = strategy.parameter_space
    if not space:
        return [{}]
    if mode == "retune":
        space = {key: values[:1] for key, values in space.items()}
    return _bounded_grid(space, max_configs=max_configs)


def search_param_keys(strategy_name: str) -> list[str]:
    """Return stable parameter keys used by the selected strategy surface."""
    strategy = build_strategy_by_name(strategy_name)
    if strategy.search_spec is not None:
        return sorted(strategy.search_spec.parameter_map())
    return sorted(strategy.parameter_space)


def _space_from_spec(search_spec: StrategySearchSpec, *, mode: str) -> dict[str, list[Any]]:
    if mode == "retune":
        return {
            parameter.name: _retune_values(parameter)
            for parameter in search_spec.parameters
        }
    return search_spec.parameter_space()


def _retune_values(parameter: ParameterSpec) -> list[Any]:
    legal = parameter.legal_values()
    anchors: list[Any] = []
    for value in (parameter.prior_center, parameter.default):
        if value is not None and value in legal and value not in anchors:
            anchors.append(value)

    if parameter.type == "categorical":
        return anchors or legal[:1]

    if not anchors:
        return legal[:1]
    anchor = anchors[0]
    try:
        index = legal.index(anchor)
    except ValueError:
        return anchors
    neighbor_indexes = [index - 1, index, index + 1]
    values: list[Any] = []
    for item_index in neighbor_indexes:
        if item_index < 0 or item_index >= len(legal):
            continue
        value = legal[item_index]
        if value not in values:
            values.append(value)
    for value in anchors:
        if value not in values:
            values.append(value)
    return values


def _valid_configs(
    search_spec: StrategySearchSpec,
    space: dict[str, list[Any]],
    *,
    max_configs: int,
) -> list[dict[str, Any]]:
    configs = _bounded_grid(space, max_configs=max_configs * 4)
    valid: list[dict[str, Any]] = []
    seen: set[str] = set()
    for config in configs:
        normalized = search_spec.normalize_config(config)
        if normalized.errors:
            continue
        key = repr(sorted(normalized.config.items()))
        if key in seen:
            continue
        seen.add(key)
        valid.append(normalized.config)
    if len(valid) <= max_configs:
        return valid or [search_spec.prior_config()]
    return _sample_evenly(valid, max_configs=max_configs)


def _bounded_grid(space: dict[str, list[Any]], *, max_configs: int) -> list[dict[str, Any]]:
    keys = sorted(space)
    if not keys:
        return [{}]
    values = [space[key] for key in keys]
    all_configs = [dict(zip(keys, combo, strict=True)) for combo in product(*values)]
    unique: list[dict[str, Any]] = []
    seen: set[str] = set()
    for config in all_configs:
        key = repr(sorted(config.items()))
        if key in seen:
            continue
        seen.add(key)
        unique.append(config)
    if len(unique) <= max_configs:
        return unique
    return _sample_evenly(unique, max_configs=max_configs)


def _sample_evenly(configs: list[dict[str, Any]], *, max_configs: int) -> list[dict[str, Any]]:
    if max_configs <= 1:
        return configs[:1]
    if len(configs) <= max_configs:
        return configs
    indexes = sorted({
        round(index * (len(configs) - 1) / (max_configs - 1))
        for index in range(max_configs)
    })
    return [configs[index] for index in indexes]


__all__ = [
    "build_search_configs",
    "search_param_keys",
]
