# Hypothesis: Jerk-Pivot Momentum Current Basket Discovery

## Config
- id: `jerk-pivot-current-basket-discovery`
- state: `completed`
- decision: `promote`
- symbol_scope: `SPY, QQQ, IWM, AAPL, AMD, META, NVDA, PLTR, TSLA`
- strategy: `Jerk-Pivot Momentum (tight)`
- max_stage: `M5`
- max_configs: `48`
- last_run: `2026-04-13T01:37:01+0000`

## Thesis
Jerk inflections near VPOC can identify moments where intraday momentum is re-accelerating from a high-participation reference level. If velocity, acceleration, and jerk align near VPOC, continuation should be strong enough to survive cost and execution stress.

## Rules
- Entry: jerk sign-change confirms velocity and acceleration alignment near VPOC.
- Direction: long and short are both evaluated by the strategy signal direction.
- Filters: use the strategy search surface for VPOC proximity, jerk lookback, kinematic lookback, volume filter, and volume multiplier.
- Exit: use MFE/MAE reward-risk gates through M1-M5, then evaluate per-candidate thesis exits after M5.

## Notes
- Feasibility tag: `config-only`
- Prior v1 discoveries around jerk/kinematics are leads only; this run must pass v2 gates on its own.
- Market regime columns are observational evidence, not a gate.
- Use `CATALOG_SELECTED.csv` as the concise selected-candidate readout after M5.

## Agent Report
### Run
`2026-04-12T203208` — strategy: `Jerk-Pivot Momentum (tight)`

### Stages Executed
`M1 → M2 → M3 → M4 → M5`

### Notes
- M1 PASS: pct_pos=100%  exp_r=+0.3855  signals=119  windows=5
- M2: 13 candidates promoted
- M3: 190 detail rows
- M4: 6 promoted
- M5: 24 execution mappings
- exit_opt: 2 catalog candidates optimized

### Decision
`promote`

### Artifacts
`/Users/suman/kg_env/projects/mala_v2/data/results/hypothesis_runs/jerk-pivot-current-basket-discovery/2026-04-12T203208`
