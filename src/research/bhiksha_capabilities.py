"""Bhiksha capability manifest reader for Mala handoff evidence."""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
from typing import Any

import yaml


CAPABILITY_MANIFEST_ENV = "BHIKSHA_CAPABILITIES_PATH"
_LOCAL_DEFAULT = Path("/Users/suman/code/bhiksha/config/capabilities/bhiksha_capabilities_v1.yaml")
_OLDMAC_DEFAULT = Path.home() / "Documents" / "bhiksha" / "config" / "capabilities" / "bhiksha_capabilities_v1.yaml"
_LOCAL_RUNTIME_DEFAULT = Path("/Users/suman/code/bhiksha/artifacts/capabilities/bhiksha_runtime_capabilities_v2.json")
_OLDMAC_RUNTIME_DEFAULT = (
    Path.home() / "Documents" / "bhiksha" / "artifacts" / "capabilities" / "bhiksha_runtime_capabilities_v2.json"
)

_MARKET_IMPULSE_NAME_VARIANTS = {
    "market impulse (cross & reclaim)": "cross_reclaim",
    "mi high close reclaim": "close_location_reclaim",
    "mi second touch": "delayed_reclaim",
    "mi shallow spring": "same_bar_shallow_reclaim",
    "mi push through": "continuation_confirmation",
}


@dataclass(frozen=True)
class BhikshaCapabilityResult:
    strategy_variant: str
    status: str
    reason: str
    manifest_version: int | None
    bhiksha_ready: bool


def default_capability_manifest_path() -> Path:
    configured = os.getenv(CAPABILITY_MANIFEST_ENV)
    if configured:
        return Path(configured).expanduser()
    if _LOCAL_RUNTIME_DEFAULT.exists():
        return _LOCAL_RUNTIME_DEFAULT
    if _OLDMAC_RUNTIME_DEFAULT.exists():
        return _OLDMAC_RUNTIME_DEFAULT
    if _LOCAL_DEFAULT.exists():
        return _LOCAL_DEFAULT
    return _OLDMAC_DEFAULT


def load_capability_manifest(path: str | Path | None = None) -> dict[str, Any] | None:
    resolved = Path(path).expanduser() if path is not None else default_capability_manifest_path()
    try:
        raw = resolved.read_text(encoding="utf-8")
        payload = json.loads(raw) if resolved.suffix.lower() == ".json" else yaml.safe_load(raw) or {}
    except (FileNotFoundError, OSError, json.JSONDecodeError, yaml.YAMLError):
        return None
    if not isinstance(payload, dict):
        return None
    if int(payload.get("version") or 0) < 1:
        return None
    if not isinstance(payload.get("strategies"), dict):
        return None
    return payload


def derive_strategy_variant(
    *,
    strategy_key: str,
    strategy_name: str,
    strategy_params: dict[str, Any],
    manifest: dict[str, Any] | None = None,
) -> str:
    normalized_key = strategy_key.strip().lower()
    if normalized_key == "market_impulse":
        entry_mode = str(strategy_params.get("entry_mode") or "").strip().lower()
        if entry_mode:
            return entry_mode
        name_variant = _MARKET_IMPULSE_NAME_VARIANTS.get(strategy_name.strip().lower())
        if name_variant:
            return name_variant
    if manifest:
        strategies = manifest.get("strategies")
        if isinstance(strategies, dict):
            strategy_config = strategies.get(normalized_key)
            if isinstance(strategy_config, dict) and strategy_config.get("default_variant"):
                return str(strategy_config["default_variant"])
    return "default"


def evaluate_bhiksha_capability(
    *,
    strategy_key: str,
    strategy_name: str,
    strategy_params: dict[str, Any],
    thesis_exit_policy: str | None,
    thesis_exit_tested: bool,
    recommendation_tier: str,
    manifest: dict[str, Any] | None,
) -> BhikshaCapabilityResult:
    variant = derive_strategy_variant(
        strategy_key=strategy_key,
        strategy_name=strategy_name,
        strategy_params=strategy_params,
        manifest=manifest,
    )
    if manifest is None:
        return BhikshaCapabilityResult(
            strategy_variant=variant,
            status="unknown_manifest",
            reason="bhiksha_capability_manifest_missing",
            manifest_version=None,
            bhiksha_ready=False,
        )

    version = int(manifest.get("version") or 0)
    strategies = manifest.get("strategies")
    strategy_config = strategies.get(strategy_key) if isinstance(strategies, dict) else None
    if not isinstance(strategy_config, dict):
        return _blocked(variant, version, "strategy_key_not_in_bhiksha_capability_manifest")
    variants = strategy_config.get("variants")
    variant_config = variants.get(variant) if isinstance(variants, dict) else None
    if not isinstance(variant_config, dict):
        return _blocked(variant, version, "strategy_variant_not_in_bhiksha_capability_manifest")

    status = str(variant_config.get("status") or "unsupported").strip().lower()
    reason = str(variant_config.get("reason") or "").strip()
    parameter_gap = _runtime_parameter_gap(
        strategy_key=strategy_key,
        strategy_variant=variant,
        strategy_params=strategy_params,
    )
    if parameter_gap:
        status = "unsupported"
        reason = parameter_gap
    if thesis_exit_policy:
        supported_policies = {
            str(policy).strip()
            for policy in (manifest.get("supported_thesis_exit_policies") or [])
            if str(policy).strip()
        }
        if thesis_exit_policy not in supported_policies:
            status = "unsupported"
            reason = f"unsupported_thesis_exit_policy:{thesis_exit_policy}"

    # Runtime readiness is intentionally narrower than Mala promotion quality.
    # Evidence quality, watch-only state, and activation decisions are published
    # as separate Mala_Evidence_v1 gates.
    bhiksha_ready = status == "supported"
    return BhikshaCapabilityResult(
        strategy_variant=variant,
        status=status,
        reason=reason or ("supported" if status == "supported" else "unsupported"),
        manifest_version=version,
        bhiksha_ready=bhiksha_ready,
    )


def _blocked(variant: str, version: int | None, reason: str) -> BhikshaCapabilityResult:
    return BhikshaCapabilityResult(
        strategy_variant=variant,
        status="unsupported",
        reason=reason,
        manifest_version=version,
        bhiksha_ready=False,
    )


def _runtime_parameter_gap(
    *,
    strategy_key: str,
    strategy_variant: str,
    strategy_params: dict[str, Any],
) -> str:
    if strategy_key == "market_impulse" and strategy_variant == "cross_reclaim":
        if _truthy(strategy_params.get("use_volume_filter")):
            return "unsupported_runtime_param:use_volume_filter"
        has_relative_threshold = any(
            _has_value(strategy_params.get(key))
            for key in ("min_relative_volume", "max_panic_relative_volume")
        )
        if has_relative_threshold and not _explicit_false(strategy_params.get("use_volume_filter")):
            return "unsupported_runtime_param:relative_volume_filter"
    return ""


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    return text in {"1", "true", "yes", "y"}


def _has_value(value: Any) -> bool:
    if value in (None, ""):
        return False
    return str(value).strip().lower() not in {"none", "null", "nan"}


def _explicit_false(value: Any) -> bool:
    if isinstance(value, bool):
        return value is False
    return str(value).strip().lower() in {"0", "false", "no", "n"}
