# Hypothesis: MI Second Touch semiconductor M1

## Config
- id: `mi-desc-second-touch-semiconductors-m1`
- state: `completed`
- decision: `promote`
- symbol_scope: `MU,AMD,SMH,SOXX`
- strategy: `MI Second Touch`
- max_stage: `M1`
- search_mode: `fixed`
- direction_scope: `long,short`
- max_configs: `1`
- last_run: `2026-05-02T04:56:39+0000`

## Thesis
A delayed VMA reclaim after an initial pierce should reduce same-bar noise while retaining Market Impulse regime alignment across semiconductor names.

## Rules
- Run fixed named-mode config only: entry_mode=delayed_reclaim; no RVOL, gap, sector, or ATR-excursion variants.
- Evaluate both long and short directions on MU, AMD, SMH, SOXX at M1 only.

## Notes
- Overnight Market Impulse descendant launch; feasibility=config-only against registered MI Second Touch.

## Agent Report
### Run
`2026-05-01T235604` — strategy: `MI Second Touch`

### Stages Executed
`M2 → M3 → M4 → M5`

### Notes
- M2: 1 candidates promoted
- M3: 15 detail rows
- M4: 1 promoted
- M5: 4 execution mappings
- exit_opt: 1 catalog candidates optimized

### Decision
`promote`

### Artifacts
`/Users/sunny/Documents/mala_v2/data/results/hypothesis_runs/mi-desc-second-touch-semiconductors-m1/2026-05-01T235604`
