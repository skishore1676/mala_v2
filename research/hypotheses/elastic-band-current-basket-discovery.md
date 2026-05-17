# Hypothesis: Elastic Band Current Basket Discovery

## Config
- id: `elastic-band-current-basket-discovery`
- state: `completed`
- decision: `promote`
- symbol_scope: `SPY, QQQ, IWM, AAPL, AMD, META, NVDA, PLTR, TSLA`
- strategy: `Elastic Band Reversion`
- max_stage: `M5`
- last_run: `2026-05-17T12:06:38+0000`

## Thesis
Large intraday stretches away from 4-hour VPOC should mean-revert when short-term kinematics show exhaustion. The goal is to let mala_v2 rediscover which symbols, directions, and Elastic Band configurations survive the staged gates across the current trading basket.

## Rules
- Entry: price stretches far enough from VPOC to clear the strategy z-score threshold.
- Direction: long below value and short above value, based on the strategy signal direction.
- Filters: use the strategy search surface for z-score threshold, z-score window, directional mass, jerk confirmation, and kinematic lookback.
- Exit: use MFE/MAE reward-risk gates through M1-M5, then write an M5 thesis-exit optimization artifact for promoted candidates.

## Notes
- Feasibility tag: `config-only`
- Prior v1 evidence suggested Elastic Band had useful survivors, especially IWM short and NVDA long, but v1 results are leads only. This run must pass the v2 workbench gates on its own.
- Market regime columns are observational evidence, not a gate.
- Strategy_Catalog should only be considered after an M5 promote.

## Agent Report
### Run
`2026-05-17T070215` — strategy: `Elastic Band Reversion`

### Stages Executed
`M1 → M2 → M3 → M4 → M5`

### Notes
- M1 PASS: pct_pos=80%  exp_r=+0.2300  signals=1275  windows=5
- M2: 24 candidates promoted
- M3: 298 detail rows
- M4: 3 promoted
- M5: 12 execution mappings
- exit_opt: 2 catalog candidates optimized

### Decision
`promote`

### Artifacts
`/Users/suman/code/mala_v2/data/results/hypothesis_runs/elastic-band-current-basket-discovery/2026-05-17T070215`
