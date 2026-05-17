# Next Deletion Pass - 2026-05-17

## Removed From Active Source

- `hypothesis_agent.py` no longer imports or writes legacy `Strategy_Catalog`.
- `reoptimize_exits.py` no longer imports or writes legacy `Strategy_Catalog`; Sheet publication is only through `src.research.mala_handoff`.
- Strategy factory no longer registers:
  - `Kinematic Ladder`
  - `Regime Router (Kinematic + Compression)`
  - `Opening Drive v2 (Short Continue)`
- Legacy strategy-key and exit-policy grid entries for retired factory strategies were removed.

## Archived

- `src/research/catalog.py` -> `research/reports/archive/scripts/legacy_strategy_catalog_20260517/catalog.py`
- `src/research/catalog_volume_sensitivity.py` -> `research/reports/archive/scripts/legacy_forensics_20260517/catalog_volume_sensitivity.py`
- `src/research/volume_mismatch_retune.py` -> `research/reports/archive/scripts/legacy_forensics_20260517/volume_mismatch_retune.py`
- `src/strategy/ema_momentum.py` -> `research/reports/archive/strategies/retired_20260517/ema_momentum.py`
- `src/strategy/kinematic_ladder.py` -> `research/reports/archive/strategies/retired_20260517/kinematic_ladder.py`
- `src/strategy/regime_router.py` -> `research/reports/archive/strategies/retired_20260517/regime_router.py`

Archived tests were renamed so pytest will not collect them accidentally.

## Kept

- `src/research/catalog_steward.py` remains active because it is an advisory `Mala_Evidence_v1` / `active_strategy` review tool, not a legacy publisher.
- `Strategy_Catalog` read-only language remains in guardrail docs and tests where it protects against accidental legacy publication.

## Re-entry Rule

Retired strategies do not come back through factory compatibility aliases. They must return as new playbook packets with current hypothesis docs, M5.5 option-exit evidence, publication fields, and Bhiksha shadow activation review.
