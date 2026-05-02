# Hypothesis: MI High Close Reclaim semiconductor M1

## Config
- id: `mi-desc-high-close-semiconductors-m1`
- state: `completed`
- decision: `promote`
- symbol_scope: `MU,AMD,SMH,SOXX`
- strategy: `MI High Close Reclaim`
- max_stage: `M1`
- search_mode: `fixed`
- direction_scope: `long,short`
- max_configs: `1`
- last_run: `2026-05-02T04:57:06+0000`

## Thesis
A high close-location reclaim should distinguish decisive VMA defense from weak recrosses inside the Market Impulse regime.

## Rules
- Run fixed named-mode config only: entry_mode=close_location_reclaim; no RVOL, gap, sector, or ATR-excursion variants.
- Evaluate both long and short directions on MU, AMD, SMH, SOXX at M1 only.

## Notes
- Overnight Market Impulse descendant launch; feasibility=config-only against registered MI High Close Reclaim.

## Agent Report
### Run
`2026-05-01T235642` — strategy: `MI High Close Reclaim`

### Stages Executed
`M2 → M3 → M4 → M5`

### Notes
- M2: 2 candidates promoted
- M3: 30 detail rows
- M4: 1 promoted
- M5: 4 execution mappings

### Decision
`promote`

### Artifacts
`/Users/sunny/Documents/mala_v2/data/results/hypothesis_runs/mi-desc-high-close-semiconductors-m1/2026-05-01T235642`
