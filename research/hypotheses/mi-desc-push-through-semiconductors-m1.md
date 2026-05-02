# Hypothesis: MI Push Through semiconductor M1

## Config
- id: `mi-desc-push-through-semiconductors-m1`
- state: `completed`
- decision: `promote`
- symbol_scope: `MU,AMD,SMH,SOXX`
- strategy: `MI Push Through`
- max_stage: `M1`
- search_mode: `fixed`
- direction_scope: `long,short`
- max_configs: `1`
- last_run: `2026-05-02T04:57:42+0000`

## Thesis
A continuation-confirmation bar after reclaim should filter fake VMA reclaims and keep only follow-through in the Market Impulse regime.

## Rules
- Run fixed named-mode config only: entry_mode=continuation_confirmation; no RVOL, gap, sector, or ATR-excursion variants.
- Evaluate both long and short directions on MU, AMD, SMH, SOXX at M1 only.

## Notes
- Overnight Market Impulse descendant launch; feasibility=config-only against registered MI Push Through.

## Agent Report
### Run
`2026-05-01T235708` — strategy: `MI Push Through`

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
`/Users/sunny/Documents/mala_v2/data/results/hypothesis_runs/mi-desc-push-through-semiconductors-m1/2026-05-01T235708`
