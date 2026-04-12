"""Strategy package.

Keep this module lightweight so importing ``src.strategy.factory`` does not
eagerly import every strategy class and reintroduce package-level cycles.
"""

from __future__ import annotations

from typing import Any

from src.strategy.base import BaseStrategy, coerce_time, required_feature_union


def __getattr__(name: str) -> Any:
    if name in {"available_strategy_names", "build_strategy", "build_strategy_by_name"}:
        from src.strategy import factory

        return getattr(factory, name)
    raise AttributeError(f"module 'src.strategy' has no attribute {name!r}")


__all__ = [
    "BaseStrategy",
    "available_strategy_names",
    "build_strategy",
    "build_strategy_by_name",
    "coerce_time",
    "required_feature_union",
]
