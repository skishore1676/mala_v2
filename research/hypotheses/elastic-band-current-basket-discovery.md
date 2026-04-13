# Hypothesis: Elastic Band Current Basket Discovery

## Config
- id: `elastic-band-current-basket-discovery`
- state: `completed`
- decision: `promote`
- symbol_scope: `SPY, QQQ, IWM, AAPL, AMD, META, NVDA, PLTR, TSLA`
- strategy: `Elastic Band Reversion`
- max_stage: `M5`
- last_run: `2026-04-13T00:11:21+0000`

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
`2026-04-12T191121` — strategy: `Elastic Band Reversion`

### Stages Executed
`M1 → M2 → M3 → M4 → M5`

### Notes
- M1 PASS
- M2: 24 candidates promoted
- M3: walk-forward detail
- M4: 1 promoted (NVDA short, z=1.75, w=360+dm, kin_lb=5)
- M5: 4 execution profiles; stock_like mc_prob=0.956 (promote); options paths marginal/kill
- Note: exit optimization step errored (exit code 1); no exit opt artifact written

### Decision
`promote`

### Artifacts
`/Users/suman/kg_env/projects/mala_v2/data/results/hypothesis_runs/elastic-band-current-basket-discovery/2026-04-12T191121`
