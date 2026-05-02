# Hypothesis: MI Shallow Spring semiconductor M1

## Config
- id: `mi-desc-shallow-spring-semiconductors-m1`
- state: `completed`
- decision: `promote`
- symbol_scope: `MU,AMD,SMH,SOXX`
- strategy: `MI Shallow Spring`
- max_stage: `M1`
- search_mode: `fixed`
- direction_scope: `long,short`
- max_configs: `1`
- last_run: `2026-05-02T03:05:07+0000`

## Thesis
A shallow same-bar VMA reclaim in an aligned Market Impulse regime should preserve the original MI edge while excluding deep ambiguous pierces across liquid semiconductor names.

## Rules
- Run fixed named-mode config only: entry_mode=same_bar_shallow_reclaim; no RVOL, gap, sector, or ATR-excursion variants.
- Evaluate both long and short directions on MU, AMD, SMH, SOXX at M1 only.

## Notes
- Overnight Market Impulse descendant launch; feasibility=config-only against registered MI Shallow Spring.

## Agent Report
### Run
`2026-05-01T220441` — strategy: `MI Shallow Spring`

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
`/Users/sunny/Documents/mala_v2/data/results/hypothesis_runs/mi-desc-shallow-spring-semiconductors-m1/2026-05-01T220441`
